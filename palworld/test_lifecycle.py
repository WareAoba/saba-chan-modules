"""Contract tests for Palworld lifecycle module.

핵심 목적: module.toml ↔ lifecycle.py 사이의 필드명 매핑이 틀어지면
프로덕션에서 설정이 조용히 무시되는 사고가 발생한다. 이 테스트는
"필드 이름 한 글자라도 drift하면 즉시 실패"하도록 강제한다.

검증 축:
1. module.toml 자체 파싱 & BOM 안전성
2. [credential_map] 타겟이 실제 INI 키여야 함
3. module.toml file_key 값 ⊆ DEFAULT_PALWORLD_SETTINGS.keys()
4. settings.fields[].name (saba-chan 전용 그룹 제외)마다 file_key가 있다
5. lifecycle.py 공개 API가 잘못된 config에 대해 우아하게 실패하는지
"""
import json
import os
import subprocess
import sys
import tomllib
from unittest import mock

import pytest

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
MODULE_TOML = os.path.join(MODULE_DIR, "module.toml")

# 데몬이 호출하는 lifecycle 함수 전체 (src/supervisor/mod.rs, src/ipc/handlers/*)
DAEMON_FUNCTIONS = {
    "get_launch_command", "stop", "status", "command", "validate", "configure",
    "read_properties", "reset_properties", "reset_server", "import_settings",
    "get_credentials", "accept_agreements", "diagnose_log", "list_versions",
    "get_version_details", "install_server", "get_installed_version",
}


# ─── 1. module.toml 자체 무결성 ─────────────────────────────

def _load_module_toml():
    """TOML을 UTF-8-BOM 허용하여 로드 (데몬 측 로더와 동일 정책)."""
    with open(MODULE_TOML, "rb") as f:
        raw = f.read()
    # BOM 수동 제거 (TOML은 BOM을 허용하지 않는 파서가 많음)
    if raw.startswith(b"\xef\xbb\xbf"):
        raw = raw[3:]
    return tomllib.loads(raw.decode("utf-8"))


def _file_keys():
    """설정 이름 → PalWorldSettings.ini 키 (module.toml file_key)"""
    from daemon_rcon import setting_file_keys
    return setting_file_keys(os.path.dirname(MODULE_TOML))


def test_module_toml_parses():
    cfg = _load_module_toml()
    assert cfg["module"]["name"] == "palworld"
    assert cfg["module"]["entry"] == "lifecycle.py"
    assert "settings" in cfg


def test_module_toml_required_sections_present():
    cfg = _load_module_toml()
    for section in ("module", "protocols", "credential_map", "config", "install", "settings"):
        assert section in cfg, f"missing [{section}] section"


# ─── 2. credential_map 타겟 유효성 ──────────────────────────

def test_credential_map_targets_are_real_ini_keys():
    """credential_map 우측 값은 실제 PalWorldSettings.ini 키여야 한다.
    예: rest_password = "AdminPassword" → AdminPassword가 _PALWORLD_KEY_MAP의 값이거나
        DEFAULT_PALWORLD_SETTINGS의 키여야 한다."""
    from palworld.lifecycle import DEFAULT_PALWORLD_SETTINGS
    _PALWORLD_KEY_MAP = _file_keys()

    cfg = _load_module_toml()
    cred_map = cfg.get("credential_map", {})
    assert cred_map, "credential_map must not be empty for Palworld (REST 인증 필수)"

    known_ini_keys = set(_PALWORLD_KEY_MAP.values()) | set(DEFAULT_PALWORLD_SETTINGS.keys())
    for daemon_key, ini_key in cred_map.items():
        assert ini_key in known_ini_keys, (
            f"credential_map.{daemon_key} = {ini_key!r} references unknown INI key. "
            f"Must appear in _PALWORLD_KEY_MAP.values() or DEFAULT_PALWORLD_SETTINGS.keys()."
        )


def test_credential_map_rest_password_binds_admin_password():
    """Palworld의 REST API 인증은 AdminPassword를 사용. 이 계약이 깨지면
    데몬의 rest_password 동기화가 침묵한 채 실패한다."""
    cfg = _load_module_toml()
    assert cfg["credential_map"].get("rest_password") == "AdminPassword"


# ─── 3. KEY_MAP ↔ DEFAULT 일관성 ────────────────────────────

def test_key_map_values_subset_of_defaults():
    """_PALWORLD_KEY_MAP의 모든 INI 키는 DEFAULT_PALWORLD_SETTINGS에 존재해야 한다.
    이 대응이 깨지면 '설정을 저장했는데 서버에는 반영 안됨' 버그가 된다."""
    from palworld.lifecycle import DEFAULT_PALWORLD_SETTINGS
    _PALWORLD_KEY_MAP = _file_keys()

    missing = [
        (friendly, ini_key)
        for friendly, ini_key in _PALWORLD_KEY_MAP.items()
        if ini_key not in DEFAULT_PALWORLD_SETTINGS
    ]
    assert not missing, (
        f"{len(missing)}개 INI 키가 DEFAULT_PALWORLD_SETTINGS에 누락됨 "
        f"(오타 혹은 누락): {missing[:5]}..."
    )


def test_key_map_no_duplicate_targets():
    """두 개 이상의 friendly name이 같은 INI 키에 매핑되면
    configure() 호출 시 덮어쓰기 순서에 따라 값이 소실된다."""
    _PALWORLD_KEY_MAP = _file_keys()

    reverse = {}
    for friendly, ini_key in _PALWORLD_KEY_MAP.items():
        reverse.setdefault(ini_key, []).append(friendly)
    dupes = {k: v for k, v in reverse.items() if len(v) > 1}
    assert not dupes, f"중복 매핑된 INI 키: {dupes}"


# ─── 4. settings.fields ↔ KEY_MAP 계약 ──────────────────────

def _ini_bound_field_names(cfg):
    """module.toml에서 INI에 기록되어야 하는 필드만 추출.
    group="saba-chan"은 데몬 내부 전용(예: rest_host, rest_port)이므로 제외."""
    result = []
    for f in cfg.get("settings", {}).get("fields", []):
        if f.get("group") == "saba-chan":
            continue
        result.append(f["name"])
    return result


def test_ini_bound_fields_are_mapped():
    """INI에 써야 하는 모든 UI 필드가 _PALWORLD_KEY_MAP에 등록되어 있어야 한다.
    누락되면 사용자가 GUI에서 값을 바꿔도 서버에 반영되지 않는다."""
    _PALWORLD_KEY_MAP = _file_keys()

    cfg = _load_module_toml()
    unmapped = [n for n in _ini_bound_field_names(cfg) if n not in _PALWORLD_KEY_MAP]
    assert not unmapped, (
        f"{len(unmapped)}개 UI 필드가 _PALWORLD_KEY_MAP에 없음 "
        f"(GUI 설정이 서버에 반영되지 않음): {unmapped}"
    )


def test_ini_bound_field_types_sane():
    """숫자·불리언·셀렉트 필드가 `type`을 명시했는지 확인."""
    cfg = _load_module_toml()
    valid_types = {"text", "password", "number", "boolean", "select", "textarea"}
    for f in cfg.get("settings", {}).get("fields", []):
        assert f.get("type") in valid_types, f"{f.get('name')}: invalid type {f.get('type')!r}"


# ─── 4b. 최신 업데이트(DefaultPalWorldSettings.ini) 완전성 ───
# 목적: 게임 업데이트로 INI 옵션이 추가/제거될 때 (1) DEFAULT dict,
# (2) _PALWORLD_KEY_MAP, (3) module.toml GUI 노출 세 곳 중 하나라도
# drift하면 즉시 실패시킨다. "모든 옵션을 GUI에 노출" 요구사항의 회귀 가드.

# RCON은 개발사가 폐지 예정 선언 + REST API는 saba-chan이 항상 강제 활성화하므로
# (_enforce_rest_policy) 사용자가 GUI에서 건드릴 수 없게 의도적으로 숨긴다.
_EXPECTED_INTERNAL_ONLY_INI_KEYS = {"RCONEnabled", "RCONPort", "RESTAPIEnabled"}


def _load_default_ini_props():
    from extensions.ue4_ini import parse_option_settings
    ini_path = os.path.join(MODULE_DIR, "test_data", "DefaultPalWorldSettings.ini")
    return parse_option_settings(ini_path)


def test_default_settings_matches_sample_ini():
    """DEFAULT_PALWORLD_SETTINGS는 DefaultPalWorldSettings.ini(최신 게임 업데이트 샘플)와
    키 집합이 완전히 일치해야 한다. drift하면 '설정 초기화' 시 최신 옵션이 누락되거나
    이미 삭제된 옵션이 계속 기록된다."""
    from palworld.lifecycle import DEFAULT_PALWORLD_SETTINGS

    ini_keys = set(_load_default_ini_props().keys())
    default_keys = set(DEFAULT_PALWORLD_SETTINGS.keys())

    missing_in_dict = ini_keys - default_keys
    extra_in_dict = default_keys - ini_keys
    assert not missing_in_dict, f"DEFAULT_PALWORLD_SETTINGS에 누락된 최신 INI 키: {sorted(missing_in_dict)}"
    assert not extra_in_dict, f"DEFAULT_PALWORLD_SETTINGS에만 있고 샘플 INI엔 없는 키(오타 의심): {sorted(extra_in_dict)}"


def test_key_map_covers_all_ini_keys_except_internal():
    """_PALWORLD_KEY_MAP은 RCON/REST 강제관리 키를 제외한 모든 INI 키를 커버해야 한다.
    최신 업데이트로 추가된 옵션이 매핑 없이 방치되는 사고를 방지한다."""
    _PALWORLD_KEY_MAP = _file_keys()

    ini_keys = set(_load_default_ini_props().keys())
    mapped_keys = set(_PALWORLD_KEY_MAP.values())
    unmapped = ini_keys - mapped_keys - _EXPECTED_INTERNAL_ONLY_INI_KEYS
    assert not unmapped, f"_PALWORLD_KEY_MAP에 매핑되지 않은 INI 키: {sorted(unmapped)}"


def test_all_ini_keys_exposed_in_gui_except_internal():
    """요구사항: '기본적으로 모든 옵션을 GUI에 노출'. RCON/REST 강제관리 키를 제외한
    모든 INI 키가 module.toml의 settings.fields에 노출되어야 한다."""
    _PALWORLD_KEY_MAP = _file_keys()

    cfg = _load_module_toml()
    ui_field_names = {f["name"] for f in cfg.get("settings", {}).get("fields", [])}
    ini_keys = set(_load_default_ini_props().keys())
    reverse_map = {v: k for k, v in _PALWORLD_KEY_MAP.items()}

    missing = []
    for ini_key in ini_keys - _EXPECTED_INTERNAL_ONLY_INI_KEYS:
        friendly = reverse_map.get(ini_key)
        if friendly is None or friendly not in ui_field_names:
            missing.append(ini_key)
    assert not missing, f"GUI에 노출되지 않은 INI 옵션: {sorted(missing)}"


def test_settings_groups_are_known():
    """settings.fields의 group은 saba-chan/None/communication/advanced 중 하나여야 한다."""
    cfg = _load_module_toml()
    known_groups = {None, "saba-chan", "communication", "advanced"}
    for f in cfg.get("settings", {}).get("fields", []):
        assert f.get("group") in known_groups, f"{f['name']}: unknown group {f.get('group')!r}"


def test_communication_and_advanced_groups_non_empty():
    """통신 탭/고급 탭이 실제로 접을 수 있는 그룹으로 분리되어 있는지 확인."""
    cfg = _load_module_toml()
    fields = cfg.get("settings", {}).get("fields", [])
    assert any(f.get("group") == "communication" for f in fields), "communication 그룹이 비어있음"
    assert any(f.get("group") == "advanced" for f in fields), "advanced 그룹이 비어있음"


# ─── 5. 공개 API 방어 동작 ─────────────────────────────────

def test_get_launch_command_missing_executable():
    from palworld.lifecycle import get_launch_command
    result = get_launch_command({"port": 8211})
    assert result.get("success") is False


def test_get_launch_command_nonexistent_executable():
    from palworld.lifecycle import get_launch_command
    result = get_launch_command({"server_executable": "/nonexistent/PalServer.exe"})
    assert result.get("success") is False


def test_validate_empty_config_fails():
    from palworld.lifecycle import validate
    result = validate({})
    # empty config should not succeed silently
    assert result.get("success") in (False, None) or "message" in result


def test_status_returns_stopped_when_no_executable():
    from palworld.lifecycle import status
    result = status({})
    assert result.get("success") is True
    assert result.get("status") == "stopped"


def test_backup_paths_skip_the_games_own_backups(tmp_path):
    """백업은 월드를 살리는 최소 구성만: 게임이 월드 폴더 안에 쌓는 자동 백업(backup/)은 넣지 않는다"""
    from palworld.lifecycle import backup_paths
    world = tmp_path / "Pal" / "Saved" / "SaveGames" / "0" / "ABCDEF0123456789"
    (world / "Players").mkdir(parents=True)
    (world / "backup" / "world").mkdir(parents=True)
    for name in ("Level.sav", "LevelMeta.sav"):
        (world / name).write_bytes(b"x")
    result = backup_paths({"working_dir": str(tmp_path)})
    assert result["success"] is True, result
    paths = set(result["paths"])
    assert str(world / "backup") not in paths
    assert not any(p.startswith(str(world / "backup")) for p in paths)
    assert {str(world / "Level.sav"), str(world / "LevelMeta.sav"), str(world / "Players")} <= paths


def test_backup_snapshots_list_the_games_world_backups_newest_first(tmp_path):
    """게임이 bIsUseBackupSaveData로 쌓는 `backup/world/<시각>/`을 스냅샷으로 알려 준다 (사바쨩이 보관·복원).
    스냅샷 안의 항목은 월드 폴더의 같은 이름 자리로 돌아간다"""
    from palworld.lifecycle import backup_snapshots
    world = tmp_path / "Pal" / "Saved" / "SaveGames" / "0" / "ABCDEF0123456789"
    for stamp in ("2026.10.24-10.00.00", "2026.10.24-10.30.00"):
        snap = world / "backup" / "world" / stamp
        (snap / "Players").mkdir(parents=True)
        (snap / "Level.sav").write_bytes(b"x")
    (world / "backup" / "local" / "2026.10.24-10.30.00").mkdir(parents=True)
    result = backup_snapshots({"working_dir": str(tmp_path)})
    assert result["success"] is True, result
    ids = [s["id"] for s in result["snapshots"]]
    assert ids == ["0/ABCDEF0123456789/2026.10.24-10.30.00", "0/ABCDEF0123456789/2026.10.24-10.00.00"]
    newest = result["snapshots"][0]
    # 시각은 폴더 이름(게임이 적은 시각)에서: 복사·압축 해제로 바뀐 수정 시각을 쓰지 않는다
    import time
    assert newest["created"] == int(time.mktime(time.strptime("2026.10.24-10.30.00", "%Y.%m.%d-%H.%M.%S")))
    os.utime(world / "backup" / "world" / "2026.10.24-10.00.00", (2_000_000_000, 2_000_000_000))
    again = backup_snapshots({"working_dir": str(tmp_path)})["snapshots"]
    assert again[0]["id"].endswith("10.30.00") and again[0]["created"] > again[1]["created"]
    snap = world / "backup" / "world" / "2026.10.24-10.30.00"
    assert sorted((p["from"], p["to"]) for p in newest["paths"]) == sorted([
        (str(snap / "Level.sav"), str(world / "Level.sav")),
        (str(snap / "Players"), str(world / "Players")),
    ])
    assert backup_snapshots({"working_dir": str(tmp_path / "none")}) == {"success": True, "snapshots": []}


def test_backup_is_declared_as_hook():
    cfg = _load_module_toml()
    assert cfg["backup"].get("hook") is True
    assert not any("SaveGames" in p for p in cfg["backup"].get("paths", [])), "SaveGames 통째는 게임 자체 백업까지 넣는다"
    assert "Pal/Saved/Config" in cfg["backup"]["paths"]


# ─── 6. 인코딩 계약 (BOM 처리) ──────────────────────────────

def test_module_toml_bom_roundtrip(tmp_path):
    """TOML 저장 시 BOM을 붙여도 로드가 동작해야 한다 (프로덕션 파일이 BOM으로
    오염되는 사고 방지)."""
    raw = open(MODULE_TOML, "rb").read()
    if raw.startswith(b"\xef\xbb\xbf"):
        raw = raw[3:]
    # BOM 재부착하여 저장
    p = tmp_path / "module.toml"
    p.write_bytes(b"\xef\xbb\xbf" + raw)
    # 재로드
    with open(p, "rb") as f:
        data = f.read()
    if data.startswith(b"\xef\xbb\xbf"):
        data = data[3:]
    parsed = tomllib.loads(data.decode("utf-8"))
    assert parsed["module"]["name"] == "palworld"


# ─── 7. 명령 응답 포매팅 (디스코드 표시 문구 복원) ──────────

def _runtime_env(include_extensions):
    import daemon_rcon
    import i18n
    paths = {os.path.dirname(daemon_rcon.__file__), os.path.dirname(i18n.__file__)}
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    if include_extensions:
        paths.add(os.path.dirname(os.path.dirname(MODULE_DIR)))
    env["PYTHONPATH"] = os.pathsep.join(paths)
    return env


def _run_cli(function, config, include_extensions=False):
    proc = subprocess.run([sys.executable, "lifecycle.py", function], cwd=MODULE_DIR,
                          input=json.dumps(config), capture_output=True, text=True,
                          encoding="utf-8", timeout=30, env=_runtime_env(include_extensions))
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout.strip().splitlines()[-1])


def test_status_and_stop_work_without_ue4_extension():
    """ue4-ini 익스텐션이 없어도 status/stop은 동작해야 한다 (지연 import)."""
    assert _run_cli("status", {})["status"] == "stopped"
    # REST 비밀번호가 없어 저장 후 종료를 요청할 수 없다: 죽이지 않고 알린다 (MK10)
    assert _run_cli("stop", {"pid": 2 ** 22 + 12345})["error_code"] == "graceful_stop_unavailable"
    assert _run_cli("stop", {"pid": 2 ** 22 + 12345, "force": True})["success"] is True
    assert _run_cli("nope", {})["success"] is False


def test_validate_accepts_daemon_server_jar_key(tmp_path):
    from palworld.lifecycle import validate
    exe = tmp_path / "PalServer.exe"
    exe.write_text("x")
    codes = {i["code"] for i in validate({"server_jar": str(exe), "port": 1})["issues"]}
    assert "NO_EXECUTABLE" not in codes and "EXECUTABLE_NOT_FOUND" not in codes


def test_status_uses_pid_liveness():
    from palworld import lifecycle
    with mock.patch.object(lifecycle, "is_process_alive", return_value=True):
        assert lifecycle.status({"pid": 77})["status"] == "running"
    with mock.patch.object(lifecycle, "is_process_alive", return_value=False):
        assert lifecycle.status({"pid": 77})["status"] == "stopped"


def test_import_settings_keeps_string_fields(tmp_path):
    from palworld.lifecycle import import_settings
    platform_dir = "WindowsServer" if sys.platform == "win32" else "LinuxServer"
    ini_dir = tmp_path / "Pal" / "Saved" / "Config" / platform_dir
    ini_dir.mkdir(parents=True)
    (ini_dir / "PalWorldSettings.ini").write_text(
        "[/Script/Pal.PalGameWorldSettings]\n"
        'OptionSettings=(ServerName="12345",ServerPlayerMaxNum=32,ExpRate=1.500000,'
        'bIsPvP=True,AdminPassword="0042")\n', encoding="utf-8")
    settings = import_settings({"working_dir": str(tmp_path)})["settings"]
    assert settings["server_name"] == "12345"
    assert settings["admin_password"] == "0042"
    assert settings["max_players"] == 32
    assert settings["exp_rate"] == 1.5
    assert settings["is_pvp"] is True


def test_deny_technology_list_is_written_as_a_quoted_list(tmp_path):
    """Palworld wants DenyTechnologyList=("PALBOX","RepairBench"), not one string"""
    from palworld import lifecycle
    written = {}
    with mock.patch.object(lifecycle, "_parse_option_settings", return_value={}), \
         mock.patch.object(lifecycle, "_write_option_settings", side_effect=lambda path, props: written.update(props) or True):
        lifecycle.configure({"working_dir": str(tmp_path), "settings": {"deny_technology_list": "PALBOX, RepairBench"}})
    assert written["DenyTechnologyList"] == '("PALBOX","RepairBench")'
    assert lifecycle._to_ini_list("", '"') == ""
    assert lifecycle._to_ini_list('("A")', '"') == '("A")'
    assert lifecycle._from_ini_list('("PALBOX","RepairBench")') == "PALBOX,RepairBench"


def test_rcon_stays_off_and_has_no_command_path(tmp_path):
    """RCON은 보안 정책으로 막는다: 설정으로 켤 수 없고, 손으로 켠 INI도 저장 때 꺼지며, 명령은 REST로만 간다"""
    from palworld import lifecycle
    # 설정으로 RCON을 켜거나 REST를 끌 수 없다
    assert not {"RCONEnabled", "RESTAPIEnabled"} & set(_file_keys().values())
    assert not hasattr(lifecycle, "execute_command_via_rcon")
    written = {}
    with mock.patch.object(lifecycle, "_parse_option_settings", return_value={"RCONEnabled": "True"}), \
         mock.patch.object(lifecycle, "_write_option_settings", side_effect=lambda path, props: written.update(props) or True):
        lifecycle.configure({"working_dir": str(tmp_path), "settings": {"rcon_enabled": True, "server_name": "x"}})
    assert written["RCONEnabled"] == "False"
    with mock.patch.object(lifecycle, "execute_command_via_rest", return_value={"success": True}) as rest:
        lifecycle.command({"command": "info", "instance_id": "i1", "protocol_mode": "rcon"})
    rest.assert_called_once()
    assert "rcon_password" not in lifecycle.get_credentials({"working_dir": str(tmp_path)})


def test_player_count_reads_rest_metrics():
    from palworld import lifecycle
    client = mock.Mock()
    client.metrics.return_value = {"currentplayernum": 2, "maxplayernum": 32, "serverfps": 60}
    with mock.patch.object(lifecycle, "_rest_client", return_value=client):
        assert lifecycle.player_count({}) == {"success": True, "online": 2, "max": 32}
    client.players.assert_not_called()
    client.metrics.side_effect = RuntimeError("HTTP 401")
    with mock.patch.object(lifecycle, "_rest_client", return_value=client):
        assert lifecycle.player_count({})["success"] is False
    with mock.patch.object(lifecycle, "_rest_client", return_value=None):
        assert lifecycle.player_count({})["success"] is False



def test_player_count_names_the_players_from_rest():
    """플레이어 이름은 공식 REST GET /players의 name(게임 안 이름)이고, 비면 accountName이다.
    IP·ID는 내지 않는다. 이름을 못 읽어도 수는 그대로 답한다. 데몬이 목록을 물을 때(players_detail)만 읽는다
    (검수 13, docs.palworldgame.com/api/rest-api/players, 계약 06 §2)"""
    from palworld import lifecycle
    client = mock.Mock()
    client.metrics.return_value = {"currentplayernum": 2, "maxplayernum": 32}
    client.players.return_value = {"players": [
        {"name": "Pal", "accountName": "acc1", "playerId": "p1", "userId": "steam_1", "ip": "10.0.0.5"},
        {"name": "", "accountName": "acc2", "playerId": "p2", "userId": "steam_2", "ip": "10.0.0.6"},
    ]}
    with mock.patch.object(lifecycle, "_rest_client", return_value=client):
        assert lifecycle.player_count({"players_detail": True})["players"] == [{"name": "Pal"}, {"name": "acc2"}]
    client.players.side_effect = RuntimeError("HTTP 500")
    with mock.patch.object(lifecycle, "_rest_client", return_value=client):
        assert lifecycle.player_count({"players_detail": True}) == {"success": True, "online": 2, "max": 32}
    # 아무도 없으면 목록을 묻지 않는다
    client.reset_mock(side_effect=True)
    client.metrics.return_value = {"currentplayernum": 0, "maxplayernum": 32}
    with mock.patch.object(lifecycle, "_rest_client", return_value=client):
        assert lifecycle.player_count({"players_detail": True}) == {"success": True, "online": 0, "max": 32, "players": []}
    client.players.assert_not_called()

def _daemon_rest_response(response, method, endpoint, body=None):
    return {"success": True, "data": {
        "status": 200, "url": f"http://127.0.0.1:8212{endpoint}",
        "method": method, "body": body, "response": response,
        "response_text": json.dumps(response),
    }}


def test_rest_client_leaves_host_and_credentials_to_the_daemon(monkeypatch):
    from palworld import lifecycle
    rest = mock.Mock(return_value=_daemon_rest_response({"name": "Pal"}, "GET", "/v1/api/info"))
    monkeypatch.setattr(lifecycle, "daemon_rest", rest, raising=False)
    with mock.patch.object(lifecycle, "_read_admin_password_from_ini", side_effect=AssertionError("로컬 자격증명 조회")):
        client = lifecycle._rest_client({"instance_id": "i1", "rest_host": "10.0.0.5", "rest_port": 8300})
        assert client.info() == {"name": "Pal"}
    rest.assert_called_once_with("i1", "GET", "/v1/api/info", body=None, timeout=5)


def test_a_sandboxed_run_reaches_its_rest_api_through_the_daemon(monkeypatch, tmp_path):
    """HOST 문맥에서는 로컬 비밀번호 없이 접속자 조회와 정지를 데몬에 맡긴다."""
    from palworld import lifecycle
    import daemon_rcon
    calls = []

    def host(op, args, timeout):
        calls.append((op, args))
        if args["path"].endswith("/players"):
            data = {"players": [{"name": "Pal", "accountName": "acc", "ip": "10.0.0.5"}]}
        else:
            data = {"currentplayernum": 2, "maxplayernum": 32}
        return _daemon_rest_response(data, args["method"], args["path"], args.get("body"))
    monkeypatch.setenv("SABA_HOST_DIR", str(tmp_path))
    monkeypatch.setattr(daemon_rcon, "_host_request", host)
    with mock.patch.object(lifecycle, "_read_admin_password_from_ini", return_value=None):
        assert lifecycle.player_count({"players_detail": True}) == {"success": True, "online": 2, "max": 32, "players": [{"name": "Pal"}]}
        assert lifecycle.stop({"pid": 4321, "managed": True})["success"] is True
    assert [(op, args["method"], args["path"]) for op, args in calls] == [
        ("instance.rest", "GET", "/v1/api/metrics"),
        ("instance.rest", "GET", "/v1/api/players"),
        ("instance.rest", "POST", "/v1/api/save"),
        ("instance.rest", "POST", "/v1/api/shutdown"),
    ]
    assert calls[2][1]["body"] == {}


def test_reinstalling_for_an_update_keeps_the_users_settings(tmp_path, monkeypatch):
    """서버 업데이트는 install_server를 다시 부른다. 채워진 PalWorldSettings.ini를 기본값으로 덮으면
    서버 이름·비밀번호가 초기화되어 비공개 서버가 공개가 된다"""
    from palworld import lifecycle


    install = str(tmp_path / "server")
    with mock.patch("extensions.steamcmd.install_for_module", return_value={"success": True}):
        assert lifecycle.install_server({"install_dir": install})["success"]
        props = lifecycle.read_properties({"working_dir": install})["properties"]
        assert props["RESTAPIEnabled"] == "True", "a fresh install still gets the REST-enabled defaults"
        lifecycle.configure({"working_dir": install, "settings": {"server_name": "우리서버", "server_password": "secret"}})
        assert lifecycle.install_server({"install_dir": install})["success"]
    props = lifecycle.read_properties({"working_dir": install})["properties"]
    assert props["ServerName"] == "우리서버"
    assert props["ServerPassword"] == "secret"


def test_a_new_server_starts_with_the_settings_chosen_when_creating_it(tmp_path, monkeypatch):
    """만들 때 고른 이름·비밀번호·인원은 첫 설치의 PalWorldSettings.ini에 들어간다 (비밀번호 없이 열리지 않게)"""
    from palworld import lifecycle


    install = str(tmp_path / "server")
    settings = {"server_name": "우리서버", "server_password": "secret", "max_players": 8, "managed_start": True}
    with mock.patch("extensions.steamcmd.install_for_module", return_value={"success": True}):
        assert lifecycle.install_server({"install_dir": install, "settings": settings})["success"]
    props = lifecycle.read_properties({"working_dir": install})["properties"]
    assert props["ServerName"] == "우리서버"
    assert props["ServerPassword"] == "secret"
    assert props["ServerPlayerMaxNum"] == "8"
    assert props["RESTAPIEnabled"] == "True"
    assert props["RCONEnabled"] == "False"


def test_backup_paths_fail_when_a_save_folder_cannot_be_read(tmp_path):
    """읽지 못하는 세이브 폴더를 빈 세이브로 보고 성공하면, 설정만 든 백업이 성공으로 남는다"""
    from palworld import lifecycle
    (tmp_path / "Pal" / "Saved" / "SaveGames" / "0").mkdir(parents=True)
    with mock.patch.object(lifecycle.os, "listdir", side_effect=PermissionError("denied")):
        result = lifecycle.backup_paths({"working_dir": str(tmp_path)})
    assert result["success"] is False and result.get("message"), result
    assert lifecycle.backup_paths({"working_dir": str(tmp_path / "none")}) == {"success": True, "paths": []}


def test_commands_that_change_access_or_grant_things_are_danger():
    """권한·밴 해제·계정·서버 옵션을 바꾸거나 아이템·경험치·무적을 주는 명령은 danger.
    caution은 말하는 사바쨩이 확인 없이 실행할 수 있다(L48)"""
    with open(os.path.join(os.path.dirname(__file__), "module.toml"), "rb") as f:
        risks = {c["name"]: c.get("risk") for c in tomllib.load(f)["commands"]["fields"]}
    for name in ('unban', 'ban'):
        assert risks[name] == "danger", name


def test_a_failed_rest_policy_write_does_not_sync_an_unwritten_password(tmp_path):
    """INI 저장이 실패하면(예: Windows ACL 교체 실패) 바뀌었다고 하지 않고, INI에 없는 비밀번호를 데몬에 넘기지 않는다"""
    from palworld import lifecycle
    ini = tmp_path / "PalWorldSettings.ini"
    ini.write_text("", encoding="utf-8")
    with mock.patch.object(lifecycle, "_get_settings_ini_path", return_value=str(ini)), \
         mock.patch.object(lifecycle, "_parse_option_settings", return_value={}), \
         mock.patch.object(lifecycle, "_write_option_settings", return_value=False):
        result = lifecycle._enforce_rest_policy(str(tmp_path), {"rest_password": "fromdaemon"})
        assert result["changed"] is False and result["write_failed"] is True
        assert not result.get("admin_password")
        result = lifecycle._enforce_rest_policy(str(tmp_path), {})
        assert result["changed"] is False and not result.get("admin_password")


def test_get_launch_command_refuses_to_start_when_the_rest_policy_cannot_be_saved(tmp_path):
    """REST 설정을 INI에 저장하지 못하면 데몬이 관리할 수 없는 서버를 띄우지 않고, 이유를 알린다"""
    from palworld import lifecycle
    exe = tmp_path / "PalServer.exe"
    exe.write_text("", encoding="utf-8")
    with mock.patch.object(lifecycle, "_enforce_rest_policy",
                           return_value={"changed": False, "write_failed": True, "admin_password": None}):
        result = lifecycle.get_launch_command({"server_executable": str(exe), "working_dir": str(tmp_path)})
    assert result["success"] is False, result
    # 키가 locale에 없으면 i18n.t가 키를 그대로 돌려주므로 실제 문구인지도 본다
    assert result["message"] == lifecycle.i18n.t("errors.rest_policy_write_failed") != "errors.rest_policy_write_failed"
    assert "program" not in result


@pytest.mark.parametrize("command,args,method,endpoint,body", [
    ("info", {}, "GET", "/v1/api/info", None),
    ("players", {}, "GET", "/v1/api/players", None),
    ("metrics", {}, "GET", "/v1/api/metrics", None),
    ("settings", {}, "GET", "/v1/api/settings", None),
    ("save", {}, "POST", "/v1/api/save", {}),
    ("announce", {"message": "공지"}, "POST", "/v1/api/announce", {"message": "공지"}),
    ("shutdown", {"waittime": 1, "message": "종료"}, "POST", "/v1/api/shutdown", {"waittime": 1, "message": "종료"}),
    ("kick", {"userid": "steam_1", "message": "추방"}, "POST", "/v1/api/kick", {"userid": "steam_1", "message": "추방"}),
    ("ban", {"userid": "steam_1", "message": "차단"}, "POST", "/v1/api/ban", {"userid": "steam_1", "message": "차단"}),
    ("unban", {"userid": "steam_1"}, "POST", "/v1/api/unban", {"userid": "steam_1"}),
])
def test_rest_commands_keep_method_and_endpoint_at_the_sdk_boundary(monkeypatch, command, args, method, endpoint, body):
    from palworld import lifecycle
    monkeypatch.setattr(lifecycle, "_urlopen", mock.Mock(side_effect=AssertionError("직접 REST 호출")), raising=False)
    reply = _daemon_rest_response({"ok": True}, method, endpoint, body)
    rest = mock.Mock(return_value=reply)
    monkeypatch.setattr(lifecycle, "daemon_rest", rest, raising=False)
    result = lifecycle.command({"instance_id": "i1", "command": command, "args": args})
    assert result["success"] is True, result
    assert result["data"] == reply["data"]
    rest.assert_called_once_with("i1", method, endpoint, body=body, timeout=5)


def test_nickname_resolution_uses_the_sdk_player_list(monkeypatch):
    from palworld import lifecycle
    monkeypatch.setattr(lifecycle, "_urlopen", mock.Mock(side_effect=AssertionError("직접 REST 호출")), raising=False)
    rest = mock.Mock(return_value=_daemon_rest_response({"players": [{"name": "Pal", "userId": "steam_1"}]}, "GET", "/v1/api/players"))
    monkeypatch.setattr(lifecycle, "daemon_rest", rest, raising=False)
    assert lifecycle.resolve_player_id("i1", "Pal", {}) == (True, "steam_1")
    rest.assert_called_once_with("i1", "GET", "/v1/api/players", body=None, timeout=5)


def test_rest_failure_keeps_the_daemon_error_code(monkeypatch):
    from palworld import lifecycle
    error = ConnectionError("요청 거부")
    error.error_code = "no_instance"
    monkeypatch.setattr(lifecycle, "daemon_rest", mock.Mock(side_effect=error), raising=False)
    result = lifecycle.execute_rest_via_daemon("i1", "/v1/api/save", {}, "save")
    assert result["success"] is False
    assert result["error_code"] == "no_instance"


@pytest.mark.parametrize("failure", [False, OSError("쓰기 거부")])
def test_first_install_fails_when_the_chosen_settings_cannot_be_written(tmp_path, monkeypatch, failure):
    from palworld import lifecycle
    with mock.patch("extensions.steamcmd.install_for_module", return_value={"success": True}), \
         mock.patch.object(lifecycle, "_write_option_settings", side_effect=failure if isinstance(failure, Exception) else None, return_value=failure):
        result = lifecycle.install_server({"install_dir": str(tmp_path / "server"), "settings": {"server_password": "private"}})
    assert result["success"] is False, result
    assert result["error_code"] == "settings_write_failed"
    assert result["message"] == lifecycle.i18n.t("errors.settings_write_failed")
    assert "executable_path" not in result


@pytest.mark.parametrize("failure", [False, OSError("쓰기 거부")])
def test_configure_reports_the_settings_write_failure_code(tmp_path, failure):
    from palworld import lifecycle
    with mock.patch.object(lifecycle, "_parse_option_settings", return_value={}), \
         mock.patch.object(lifecycle, "_write_option_settings", side_effect=failure if isinstance(failure, Exception) else None, return_value=failure):
        result = lifecycle.configure({"working_dir": str(tmp_path), "settings": {"server_name": "Pal"}})
    assert result["success"] is False
    assert result["error_code"] == "settings_write_failed"


def test_backup_rejects_a_name_outside_the_save_root(tmp_path, monkeypatch):
    from daemon_rcon import PathOutside
    from palworld import lifecycle
    monkeypatch.setattr(lifecycle, "_entries", lambda path: ["../outside"])
    with pytest.raises(PathOutside):
        lifecycle.backup_paths({"working_dir": str(tmp_path)})


@pytest.mark.parametrize("function", ["get_credentials", "stop", "install_server", "command"])
def test_path_boundary_errors_reach_the_shared_runner(tmp_path, monkeypatch, function):
    from daemon_rcon import PathOutside
    from palworld import lifecycle
    if function == "install_server":
        with mock.patch("extensions.steamcmd.install_for_module", return_value={"success": True}), \
             mock.patch.object(lifecycle, "_write_option_settings", side_effect=PathOutside(tmp_path / "outside")):
            with pytest.raises(PathOutside):
                lifecycle.install_server({"install_dir": str(tmp_path)})
    elif function == "command":
        with mock.patch.object(lifecycle, "execute_command_via_rest", side_effect=PathOutside(tmp_path / "outside")):
            with pytest.raises(PathOutside):
                lifecycle.command({"command": "info", "instance_id": "i1"})
    elif function == "stop":
        with mock.patch.object(lifecycle, "_rest_shutdown", side_effect=PathOutside(tmp_path / "outside")):
            with pytest.raises(PathOutside):
                lifecycle.stop({"pid": 4321, "instance_id": "i1"})
    else:
        with mock.patch.object(lifecycle, "_get_settings_ini_path", side_effect=PathOutside(tmp_path / "outside")):
            with pytest.raises(PathOutside):
                getattr(lifecycle, function)({"pid": 4321, "instance_id": "i1"})


def test_line_start_splits_rest_logs_written_without_newlines():
    """[launch] line_start: 팰월드는 REST 접근 로그를 줄바꿈 없이 이어 쓴다 (2026-10-25 PC 실측 원문).
    줄 머리는 시각까지 포함해야 한다: `[LOG]`로만 나누면 시각이 앞 줄에 떨어져 남는다."""
    import re

    with open(os.path.join(MODULE_DIR, "module.toml"), "rb") as f:
        pattern = tomllib.load(f)["launch"]["line_start"]
    head = re.compile(pattern)
    assert not head.match("")
    joined = (
        "[2026-10-26 01:21:33] [LOG] REST accessed endpoint /v1/api/metrics OK"
        "[2026-10-26 01:21:33] [LOG] REST accessed endpoint /v1/api/save OK"
        "[2026-10-26 01:21:33] [LOG] REST accessed endpoint /v1/api/shutdown OK"
    )
    starts = [m.start() for m in head.finditer(joined) if m.start() > 0]
    parts = [joined[a:b] for a, b in zip([0, *starts], [*starts, len(joined)])]
    assert parts == [
        "[2026-10-26 01:21:33] [LOG] REST accessed endpoint /v1/api/metrics OK",
        "[2026-10-26 01:21:33] [LOG] REST accessed endpoint /v1/api/save OK",
        "[2026-10-26 01:21:33] [LOG] REST accessed endpoint /v1/api/shutdown OK",
    ]
    # 한 줄로 온 로그는 나누지 않는다
    single = "[2026-10-26 01:18:32] [LOG] REST accessed endpoint /v1/api/metrics OK"
    assert [m.start() for m in head.finditer(single) if m.start() > 0] == []


def test_config_files_lists_the_settings_ini(tmp_path):
    """가져오기를 해제할 때 되돌릴 파일: 사바쨩이 REST 정책·설정을 쓰는 PalWorldSettings.ini"""
    from palworld.lifecycle import FUNCTIONS, _get_settings_ini_path
    config = {"working_dir": str(tmp_path)}
    assert FUNCTIONS["config_files"](config) == {"success": True, "files": [_get_settings_ini_path(config)]}


def test_validate_leaves_the_port_check_to_the_daemon(tmp_path):
    """포트 사용 검사는 데몬이 한다. 팰월드 게임 포트는 UDP라 TCP로 확인하던 모듈 검사는 아무것도 잡지 못했다"""
    import socket
    from palworld import lifecycle
    exe = tmp_path / "PalServer.exe"
    exe.write_text("x")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        s.listen(1)
        port = s.getsockname()[1]
        result = lifecycle.validate({"server_jar": str(exe), "port": port})
    assert "PORT_IN_USE" not in [i["code"] for i in result.get("issues", [])]


def test_stop_uses_the_shared_procedure_with_a_rest_shutdown():
    """정지 절차(기다림·강제 종료·managed·요청 불가)는 공용 graceful_stop이 맡는다. Palworld는 REST로만 요청한다"""
    from palworld import lifecycle
    with mock.patch.object(lifecycle, "graceful_stop", side_effect=lambda config, request, i18n, module_dir: request()) as shared, \
         mock.patch.object(lifecycle, "_rest_shutdown", return_value=False) as rest:
        assert lifecycle.stop({"pid": 4321}) is False
    rest.assert_called_once_with({"pid": 4321})
    assert shared.call_args.kwargs["module_dir"] == lifecycle.MODULE_DIR


def test_diagnose_rules_come_from_module_toml_with_their_phrases(tmp_path):
    """진단 규칙은 module.toml [[diagnose]] 하나에 둔다(공용 diagnose_lines). 문구는 validate.log_<code>(_hint)"""
    from daemon_rcon import load_diagnose_rules
    from palworld import lifecycle
    rules = load_diagnose_rules(os.path.dirname(MODULE_TOML))
    with open(os.path.join(os.path.dirname(MODULE_TOML), "locales", "en.json"), encoding="utf-8") as f:
        phrases = json.load(f)["validate"]
    for rule in rules:
        assert f"log_{rule['code'].lower()}" in phrases and f"log_{rule['code'].lower()}_hint" in phrases, rule["code"]
    issues = lifecycle.diagnose_log({"log_lines": ["LogNet: Error: Address already in use"]})["issues"]
    assert [(i["code"], i["severity"], i["message"]) for i in issues] == [
        ("PORT_IN_USE", "critical", phrases["log_port_in_use"])]


def test_install_reads_the_steam_app_from_module_toml_and_asks_for_a_missing_extension(tmp_path, monkeypatch):
    """Steam app_id는 module.toml [install] 한 곳에 둔다(steamcmd install_for_module). 익스텐션이 없거나 꺼져 있으면
    (import 가드의 ModuleNotFoundError) 데몬이 켜기를 안내하도록 extension_required로 답한다"""
    import extensions.steamcmd
    from palworld import lifecycle
    with mock.patch("extensions.steamcmd.install_for_module", return_value={"success": False}) as install:
        lifecycle.install_server({"install_dir": str(tmp_path / "server")})
    install.assert_called_once_with(lifecycle.MODULE_DIR, str(tmp_path / "server"))
    assert _load_module_toml()["install"]["app_id"] == 2394010
    monkeypatch.delattr(extensions.steamcmd, "install_for_module")
    result = lifecycle.install_server({"install_dir": str(tmp_path / "server")})
    assert result["error_code"] == "extension_required" and result["missing_extensions"] == ["steamcmd"]
