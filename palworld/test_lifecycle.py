"""Contract tests for Palworld lifecycle module.

핵심 목적: module.toml ↔ lifecycle.py 사이의 필드명 매핑이 틀어지면
프로덕션에서 설정이 조용히 무시되는 사고가 발생한다. 이 테스트는
"필드 이름 한 글자라도 drift하면 즉시 실패"하도록 강제한다.

검증 축:
1. module.toml 자체 파싱 & BOM 안전성
2. [credential_map] 타겟이 실제 INI 키여야 함
3. _PALWORLD_KEY_MAP.values() ⊆ DEFAULT_PALWORLD_SETTINGS.keys()
4. settings.fields[].name (saba-chan 전용 그룹 제외) ⊆ _PALWORLD_KEY_MAP.keys()
5. lifecycle.py 공개 API가 잘못된 config에 대해 우아하게 실패하는지
"""
import json
import os
import subprocess
import sys
import tomllib
from unittest import mock

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
MODULE_TOML = os.path.join(MODULE_DIR, "module.toml")

# 데몬이 호출하는 lifecycle 함수 전체 (src/supervisor/mod.rs, src/ipc/handlers/*)
DAEMON_FUNCTIONS = {
    "get_launch_command", "stop", "status", "command", "validate", "configure",
    "read_properties", "reset_properties", "reset_server", "import_settings",
    "get_credentials", "accept_eula", "diagnose_log", "list_versions",
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
    from palworld.lifecycle import _PALWORLD_KEY_MAP, DEFAULT_PALWORLD_SETTINGS

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
    from palworld.lifecycle import _PALWORLD_KEY_MAP, DEFAULT_PALWORLD_SETTINGS

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
    from palworld.lifecycle import _PALWORLD_KEY_MAP

    reverse = {}
    for friendly, ini_key in _PALWORLD_KEY_MAP.items():
        reverse.setdefault(ini_key, []).append(friendly)
    dupes = {k: v for k, v in reverse.items() if len(v) > 1}
    assert not dupes, f"중복 매핑된 INI 키: {dupes}"


def test_key_map_no_duplicate_friendly_names():
    from palworld.lifecycle import _PALWORLD_KEY_MAP
    # dict이므로 구조적으로 중복 불가능하지만, 동치성은 강제한다.
    assert len(_PALWORLD_KEY_MAP) == len(set(_PALWORLD_KEY_MAP.keys()))


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
    from palworld.lifecycle import _PALWORLD_KEY_MAP

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
    from palworld.lifecycle import _PALWORLD_KEY_MAP

    ini_keys = set(_load_default_ini_props().keys())
    mapped_keys = set(_PALWORLD_KEY_MAP.values())
    unmapped = ini_keys - mapped_keys - _EXPECTED_INTERNAL_ONLY_INI_KEYS
    assert not unmapped, f"_PALWORLD_KEY_MAP에 매핑되지 않은 INI 키: {sorted(unmapped)}"


def test_all_ini_keys_exposed_in_gui_except_internal():
    """요구사항: '기본적으로 모든 옵션을 GUI에 노출'. RCON/REST 강제관리 키를 제외한
    모든 INI 키가 module.toml의 settings.fields에 노출되어야 한다."""
    from palworld.lifecycle import _PALWORLD_KEY_MAP

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
    assert _run_cli("stop", {"pid": 2 ** 22 + 12345})["success"] is True
    assert _run_cli("nope", {})["success"] is False


def test_validate_accepts_daemon_server_jar_key(tmp_path):
    from palworld.lifecycle import validate
    exe = tmp_path / "PalServer.exe"
    exe.write_text("x")
    codes = {i["code"] for i in validate({"server_jar": str(exe), "port": 1})["issues"]}
    assert "NO_EXECUTABLE" not in codes and "EXECUTABLE_NOT_FOUND" not in codes


def test_stop_kills_only_given_pid():
    from palworld import lifecycle
    with mock.patch.object(lifecycle, "_kill") as kill, \
         mock.patch.object(lifecycle, "_rest_shutdown", return_value=False):
        result = lifecycle.stop({"pid": 4321, "server_executable": "C:/x/PalServer.exe"})
    assert result["success"] is True
    kill.assert_called_once_with(4321)


def test_status_uses_pid_liveness():
    from palworld import lifecycle
    with mock.patch.object(lifecycle, "_pid_running", return_value=True):
        assert lifecycle.status({"pid": 77})["status"] == "running"
    with mock.patch.object(lifecycle, "_pid_running", return_value=False):
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
