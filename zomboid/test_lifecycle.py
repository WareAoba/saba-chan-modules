"""Contract tests for Project Zomboid lifecycle module.

핵심 목적: module.toml ↔ lifecycle.py 사이의 필드명 매핑이 틀어지면
프로덕션에서 설정이 조용히 무시되는 사고가 발생한다. 이 테스트는
"필드 이름 한 글자라도 drift하면 즉시 실패"하도록 강제한다.
"""
import os
import sys

import pytest
from unittest import mock

try:
    import tomllib
except ImportError:  # pragma: no cover
    import tomli as tomllib  # type: ignore

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
MODULE_TOML = os.path.join(MODULE_DIR, "module.toml")


def _load_module_toml():
    with open(MODULE_TOML, "rb") as f:
        raw = f.read()
    if raw.startswith(b"\xef\xbb\xbf"):
        raw = raw[3:]
    return tomllib.loads(raw.decode("utf-8"))


# ─── 1. module.toml 자체 무결성 ─────────────────────────────

def test_module_toml_parses():
    cfg = _load_module_toml()
    assert cfg["module"]["name"] == "zomboid"
    assert cfg["module"]["entry"] == "lifecycle.py"


def test_module_toml_required_sections_present():
    cfg = _load_module_toml()
    for section in ("module", "protocols", "config", "install", "settings"):
        assert section in cfg, f"missing [{section}] section"


def test_only_measured_os_is_declared():
    """리눅스 데몬에서 만들기·시작을 막는다: 공식 start-server.sh는 java 줄 없이 ./ProjectZomboid64를 띄워
    실행 스크립트 해석이 실패하고, 대체 경로는 LD_LIBRARY_PATH(linux64)·LD_PRELOAD를 주지 않는다. 리눅스는 실측한 적이 없다"""
    assert _load_module_toml()["extensions"]["saba-core"]["platforms"] == ["windows"]


# ─── 2. KEY_MAP ↔ DEFAULT 일관성 ────────────────────────────

def test_defaults_for_a_new_server_come_from_module_toml(tmp_path):
    """servertest.ini가 아직 없을 때 보이는 값은 module.toml 설정 기본값이다 (기본값을 코드에 따로 두지 않는다)"""
    from zomboid import lifecycle
    with mock.patch.object(lifecycle.IniManager, "get_ini_path", return_value=str(tmp_path / "servertest.ini")):
        props = lifecycle.read_properties({})["properties"]
    assert props["max_players"] == "16"
    assert props["public_name"] == "My PZ Server"
    assert props["pvp"] == "true"
    assert props["speed_limit"] == "70"


def test_ini_key_map_no_duplicate_targets():
    """두 friendly name이 같은 INI 키에 매핑되면 설정이 소실된다."""
    from zomboid.lifecycle import MODULE_DIR
    from daemon_rcon import setting_file_keys
    _INI_KEY_MAP = setting_file_keys(MODULE_DIR)

    reverse = {}
    for friendly, ini_key in _INI_KEY_MAP.items():
        reverse.setdefault(ini_key, []).append(friendly)
    dupes = {k: v for k, v in reverse.items() if len(v) > 1}
    assert not dupes, f"중복 매핑된 INI 키: {dupes}"



# ─── 3. settings.fields ↔ KEY_MAP 계약 ──────────────────────

def _ini_bound_field_names(cfg):
    """group='saba-chan'은 데몬 내부 전용(ram, server_name 등). INI와 무관."""
    result = []
    for f in cfg.get("settings", {}).get("fields", []):
        # cache_dir는 INI가 아니라 실행 인자(-cachedir)다
        if f.get("group") == "saba-chan" or f["name"] == "cache_dir":
            continue
        result.append(f["name"])
    return result


def test_ini_bound_fields_are_mapped():
    """INI에 쓰여야 하는 UI 필드가 모두 file_key를 가져야 한다.
    누락되면 GUI에서 바꾼 값이 서버에 반영되지 않는다. (RCON 칸은 없다: 보안상 늘 꺼 둔다)"""
    from zomboid.lifecycle import MODULE_DIR
    from daemon_rcon import setting_file_keys
    _INI_KEY_MAP = setting_file_keys(MODULE_DIR)

    cfg = _load_module_toml()
    unmapped = [n for n in _ini_bound_field_names(cfg) if n not in _INI_KEY_MAP]
    assert not unmapped, (
        f"{len(unmapped)}개 UI 필드가 _INI_KEY_MAP에 없음 "
        f"(GUI 설정이 서버에 반영되지 않음): {unmapped}"
    )


def test_ui_field_types_sane():
    cfg = _load_module_toml()
    valid_types = {"text", "password", "number", "boolean", "select", "textarea"}
    for f in cfg.get("settings", {}).get("fields", []):
        assert f.get("type") in valid_types, f"{f.get('name')}: invalid type {f.get('type')!r}"


# ─── 4. IniManager 기본 I/O 라운드트립 ─────────────────────

def test_ini_manager_write_and_read_roundtrip(tmp_path):
    from zomboid.lifecycle import IniManager
    ini_path = str(tmp_path / "servertest.ini")
    IniManager.write(ini_path, {"PublicName": "T", "MaxPlayers": "8", "PVP": "false"})
    assert os.path.exists(ini_path)
    result = IniManager.read(ini_path)
    assert result["PublicName"] == "T"
    assert result["MaxPlayers"] == "8"
    assert result["PVP"] == "false"


def test_a_failed_ini_save_keeps_the_old_file(tmp_path, monkeypatch):
    """저장이 중간에 실패해도 서버 ini가 비거나 잘리지 않는다"""
    from zomboid.lifecycle import IniManager
    ini_path = str(tmp_path / "servertest.ini")
    IniManager.write(ini_path, {"PublicName": "Original", "Password": "pw"})
    before = open(ini_path, "rb").read()

    def broken_replace(*_):
        raise OSError("disk full")
    # 바꾸는 단계에서 실패시킨다 (Windows는 기존 파일을 os.replace가 아니라 ReplaceFileW로 바꾼다)
    import daemon_rcon
    monkeypatch.setattr(daemon_rcon, "_replace_keeping_access", broken_replace)

    with pytest.raises(OSError):
        IniManager.write(ini_path, {"PublicName": "Changed"})
    assert open(ini_path, "rb").read() == before
    assert [n for n in os.listdir(tmp_path) if n.endswith(".tmp")] == []


def test_ini_manager_update_preserves_existing(tmp_path):
    from zomboid.lifecycle import IniManager
    ini_path = str(tmp_path / "servertest.ini")
    IniManager.write(ini_path, {"PublicName": "Original", "MaxPlayers": "4"})
    IniManager.update(ini_path, {"MaxPlayers": "8"})
    result = IniManager.read(ini_path)
    assert result["PublicName"] == "Original", "기존 값이 업데이트로 인해 소실됨"
    assert result["MaxPlayers"] == "8"


def test_ini_manager_get_ini_path_uses_server_name():
    from zomboid.lifecycle import IniManager
    # config_dir 명시 시 그 디렉토리 사용
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        path = IniManager.get_ini_path({"server_name": "myserver", "config_dir": td})
        assert path.endswith("myserver.ini")
        # 경로는 실제 경로로 풀린다 (Windows 임시 폴더의 8.3 짧은 이름 RUNNER~1 등)
        assert os.path.realpath(td) in path


# ─── 5. 공개 API 방어 동작 ─────────────────────────────────

def test_validate_empty_config_fails():
    from zomboid.lifecycle import validate
    result = validate({})
    assert result.get("success") in (False, None) or "message" in result


def test_missing_server_folder_blocks_the_start(tmp_path):
    from zomboid.lifecycle import validate
    issues = validate({"working_dir": str(tmp_path / "nowhere")})["issues"]
    assert [i["code"] for i in issues if i["severity"] == "critical"] == ["no_server_dir"]
    assert all(i.get("solution") for i in issues)


def test_missing_start_script_blocks_the_start(tmp_path):
    """시작 스크립트가 없으면 켜도 실패하므로 점검이 막는다 (실행 경로와 같은 파일을 찾는다)"""
    from zomboid.lifecycle import validate
    server = tmp_path / "server"
    server.mkdir()
    config = {"working_dir": str(server), "cache_dir": str(tmp_path / "home"), "server_name": "s1", "admin_password": "pw12"}
    critical = [i for i in validate(config)["issues"] if i["severity"] == "critical"]
    assert [i["code"] for i in critical] == ["executable_not_found"] and critical[0]["solution"]
    # 실행 경로가 받아 주는 다른 스크립트 이름이나, 스크립트 없이 JRE와 jar만 있어도 막지 않는다
    other = server / ("StartServer.bat" if sys.platform == "win32" else "StartServer.sh")
    other.write_text("")
    assert not any(i["severity"] == "critical" for i in validate(config)["issues"])
    other.unlink()
    java = server / "jre64" / "bin" / ("java.exe" if sys.platform == "win32" else "java")
    java.parent.mkdir(parents=True)
    java.write_text("")
    (server / "java").mkdir()
    (server / "java" / "projectzomboid.jar").write_text("")
    assert not any(i["severity"] == "critical" for i in validate(config)["issues"])


def test_first_launch_without_admin_password_is_blocked(tmp_path):
    from zomboid.lifecycle import validate
    server = tmp_path / "server"
    server.mkdir()
    (server / "StartServer64.bat").write_text("")
    (server / "start-server.sh").write_text("")
    home = tmp_path / "home"
    config = {"working_dir": str(server), "cache_dir": str(home), "server_name": "s1"}
    codes = [i["code"] for i in validate(config)["issues"] if i["severity"] == "critical"]
    assert codes == ["admin_password_required"]
    assert not any(i["severity"] == "critical" for i in validate({**config, "admin_password": "pw12"})["issues"])
    # 빈 DB나 계정 없는 DB(첫 실행이 비밀번호 전에 멈춤)는 여전히 막는다
    import sqlite3
    (home / "db").mkdir(parents=True)
    db = home / "db" / "s1.db"
    db.write_text("")
    assert any(i["code"] == "admin_password_required" for i in validate(config)["issues"])
    db.unlink()
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE whitelist (id INTEGER PRIMARY KEY, username TEXT, admin BOOLEAN)")
    conn.commit()
    assert any(i["code"] == "admin_password_required" for i in validate(config)["issues"])
    # 계정이 있으면(이미 한 번 켠 서버) 비밀번호 없이도 된다
    conn.execute("INSERT INTO whitelist (username, admin) VALUES ('admin', 1)")
    conn.commit()
    conn.close()
    assert not any(i["severity"] == "critical" for i in validate(config)["issues"])


def test_get_launch_command_missing_executable():
    from zomboid.lifecycle import get_launch_command
    result = get_launch_command({"port": 16261})
    assert result.get("success") is False


def test_backup_paths_follow_cache_dir(tmp_path):
    """서버가 만드는 설정 파일 넷은 아직 없어도 경로로 낸다: 지운 파일도 복원할 수 있어야 한다(데몬은 훅 경로 안만 복원한다, MA3)"""
    from zomboid.lifecycle import backup_paths
    result = backup_paths({"cache_dir": str(tmp_path), "server_name": "myserver"})
    assert result["success"] is True
    assert result["paths"] == [
        os.path.join(str(tmp_path), "Saves", "Multiplayer", "myserver"),
        os.path.join(str(tmp_path), "db", "myserver.db"),
        os.path.join(str(tmp_path), "Server", "myserver.ini"),
        os.path.join(str(tmp_path), "Server", "myserver_SandboxVars.lua"),
        os.path.join(str(tmp_path), "Server", "myserver_spawnpoints.lua"),
        os.path.join(str(tmp_path), "Server", "myserver_spawnregions.lua"),
    ]



def _pz_backup(path, server, world=True, when="2026-10-26 at 01:27:36 JST"):
    """PZ 42.21 서버가 만든 백업 zip과 같은 모양 (test-zomboid 실측, 2026-10-26)"""
    import zipfile
    path.parent.mkdir(parents=True, exist_ok=True)
    version = "249" if world else "World isn't exist"
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("readme.txt", f"Backup time: {when}\nServerName: {server}\nCurrent server version:42.21\n"
                                 f"Current world version:249\nWorld version in this backup is:{version}\n")
        z.writestr("options.ini", "x")
        z.writestr(f"db/{server}.db", "db")
        for name in (f"{server}.ini", f"{server}_SandboxVars.lua", f"{server}_spawnpoints.lua", f"{server}_spawnregions.lua"):
            z.writestr(f"Server/{name}", "cfg")
        if world:
            z.writestr(f"Saves/Multiplayer/{server}/map_meta.bin", "w")
            z.writestr(f"Saves/Multiplayer/{server}/players.db", "p")


def test_backup_snapshots_list_the_servers_own_backup_zips(tmp_path):
    """BackupsOnStart·BackupsOnVersionChange가 만든 `backups/<종류>/backup_N.zip`(1이 가장 새것)을 zip 스냅샷으로 알려 준다.
    zip 안의 이 서버 세이브·DB·설정만 데이터 폴더의 같은 자리로 돌아가고, 월드가 없던 때의 백업과 다른 서버 이름의 백업은 뺀다"""
    import time
    from zomboid.lifecycle import backup_snapshots
    home = tmp_path
    _pz_backup(home / "backups" / "startup" / "backup_1.zip", "myserver")
    _pz_backup(home / "backups" / "startup" / "backup_2.zip", "myserver", world=False, when="2026-10-26 at 01:25:16 JST")
    _pz_backup(home / "backups" / "version" / "backup_1.zip", "other")
    old = time.time() - 3600
    os.utime(home / "backups" / "startup" / "backup_1.zip", (old, old))
    result = backup_snapshots({"cache_dir": str(home), "server_name": "myserver"})
    assert result["success"] is True, result
    assert [s["id"] for s in result["snapshots"]] == ["startup/backup_1"]
    snap = result["snapshots"][0]
    assert snap["archive"] == str(home / "backups" / "startup" / "backup_1.zip")
    assert snap["created"] == int(old)
    assert sorted((p["from"], p["to"]) for p in snap["paths"]) == sorted([
        ("Saves/Multiplayer/myserver", os.path.join(str(home), "Saves", "Multiplayer", "myserver")),
        ("db/myserver.db", os.path.join(str(home), "db", "myserver.db")),
        ("Server/myserver.ini", os.path.join(str(home), "Server", "myserver.ini")),
        ("Server/myserver_SandboxVars.lua", os.path.join(str(home), "Server", "myserver_SandboxVars.lua")),
        ("Server/myserver_spawnpoints.lua", os.path.join(str(home), "Server", "myserver_spawnpoints.lua")),
        ("Server/myserver_spawnregions.lua", os.path.join(str(home), "Server", "myserver_spawnregions.lua")),
    ])
    assert backup_snapshots({"cache_dir": str(tmp_path / "none"), "server_name": "myserver"}) == {"success": True, "snapshots": []}


def test_backup_snapshots_are_declared():
    from zomboid.lifecycle import FUNCTIONS
    assert _load_module_toml()["backup"].get("snapshots") is True
    assert "backup_snapshots" in FUNCTIONS


def test_backup_paths_let_older_whole_server_folder_backups_restore(tmp_path):
    """0.1.8 이전 백업은 Server/ 폴더를 통째로 담았다. 그 폴더를 restore_roots로 내야 데몬이 그 백업을 복원한다.
    restore_roots는 백업 대상이 아니므로 paths에는 여전히 이 서버 것만 든다 (MA3)"""
    from zomboid.lifecycle import backup_paths
    result = backup_paths({"cache_dir": str(tmp_path), "server_name": "myserver"})
    assert result["restore_roots"] == [os.path.join(str(tmp_path), "Server")]
    assert os.path.join(str(tmp_path), "Server") not in result["paths"]

def test_backup_paths_take_only_this_servers_files(tmp_path):
    """Server/ 폴더에는 서버 이름마다 <이름>.ini와 <이름>_*.lua가 있다. 다른 서버 것은 넣지 않는다"""
    from zomboid.lifecycle import backup_paths
    server = tmp_path / "Server"
    server.mkdir()
    ours = ["myserver.ini", "myserver_SandboxVars.lua", "myserver_spawnregions.lua", "myserver_spawnpoints.lua"]
    others = ["other.ini", "other_SandboxVars.lua", "myserver_2.ini", "myserver_2_SandboxVars.lua"]
    for name in ours + others:
        (server / name).write_text("x")
    result = backup_paths({"cache_dir": str(tmp_path), "server_name": "myserver"})
    taken = sorted(os.path.basename(p) for p in result["paths"] if os.path.dirname(p) == str(server))
    assert taken == sorted(ours), taken

# ─── 6. 인코딩 계약 ────────────────────────────────────────

def test_module_toml_bom_roundtrip(tmp_path):
    raw = open(MODULE_TOML, "rb").read()
    if raw.startswith(b"\xef\xbb\xbf"):
        raw = raw[3:]
    p = tmp_path / "module.toml"
    p.write_bytes(b"\xef\xbb\xbf" + raw)
    with open(p, "rb") as f:
        data = f.read()
    if data.startswith(b"\xef\xbb\xbf"):
        data = data[3:]
    parsed = tomllib.loads(data.decode("utf-8"))
    assert parsed["module"]["name"] == "zomboid"


# ─── Regression: unknown function must not crash the plugin ────
# 데몬은 plugin 종료 코드 != 0 을 HTTP 500 으로 변환한다.
# minecraft/palworld와 같이 그레이스풀 JSON + exit 0 으로 통일.
def test_unknown_function_returns_graceful_json_with_zero_exit():
    import json as _json
    import subprocess
    import sys

    # 하위 프로세스는 conftest의 sys.path를 물려받지 않는다. conftest가 찾은 공용 런타임을 PYTHONPATH로 넣는다
    runtime_dir = os.environ.get("SABA_RUNTIME_DIR", "")
    if not os.path.isdir(runtime_dir):
        import pytest
        pytest.skip("daemon runtime modules dir not available")
    env = os.environ.copy()
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = runtime_dir + (os.pathsep + existing if existing else "")

    proc = subprocess.run(
        [sys.executable, "lifecycle.py", "definitely_not_a_real_function"],
        cwd=MODULE_DIR,
        input="{}",
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=15,
        env=env,
    )
    assert proc.returncode == 0, (
        f"unknown function should not exit non-zero (was {proc.returncode}); "
        f"stderr={proc.stderr!r}"
    )
    payload = _json.loads(proc.stdout.strip().splitlines()[-1])
    assert payload["success"] is False
    assert "Unknown function" in payload["message"]
    assert "available" in payload


def test_managed_launch_turns_rcon_off(tmp_path):
    """관리형 시작은 콘솔(stdin)로 명령을 보낸다: 빈 RCONPassword로 RCON을 끈다."""
    from zomboid.lifecycle import _enforce_rcon_policy, IniManager
    ini = tmp_path / "servertest.ini"
    ini.write_text("RCONPassword=secret\nRCONPort=27015\n", encoding="utf-8")
    assert _enforce_rcon_policy(str(ini), {"managed": True, "rcon_password": "daemon-pw", "rcon_port": 27020}) == ""
    assert IniManager.read(str(ini))["RCONPassword"] == ""


def test_unmanaged_launch_turns_rcon_on_with_the_daemon_access(tmp_path):
    """관리형 시작을 끄면 콘솔이 없다: 데몬이 만든 포트·비밀번호로 RCON을 켜고, 접속 정보가 없으면 끈다."""
    from zomboid.lifecycle import _enforce_rcon_policy, IniManager
    ini = tmp_path / "servertest.ini"
    ini.write_text("RCONPassword=\nRCONPort=27015\n", encoding="utf-8")
    assert _enforce_rcon_policy(str(ini), {"managed": False, "rcon_password": "daemon-pw", "rcon_port": 27020}) == "daemon-pw"
    props = IniManager.read(str(ini))
    assert (props["RCONPassword"], props["RCONPort"]) == ("daemon-pw", "27020")
    assert _enforce_rcon_policy(str(ini), {"managed": False}) == ""
    assert IniManager.read(str(ini))["RCONPassword"] == ""

    # 첫 시작이라 설정 파일이 아직 없어도 RCON을 켠 채로 만든다
    fresh = tmp_path / "Server" / "first.ini"
    assert _enforce_rcon_policy(str(fresh), {"managed": False, "rcon_password": "daemon-pw", "rcon_port": 27021}) == "daemon-pw"
    props = IniManager.read(str(fresh))
    assert (props["RCONPassword"], props["RCONPort"]) == ("daemon-pw", "27021")
    assert props["DefaultPort"] == "16261"


def test_commands_go_to_the_console():
    from unittest import mock
    from zomboid import lifecycle
    with mock.patch.object(lifecycle, "daemon_stdin_command", return_value={"data": {}}) as stdin:
        result = lifecycle.command({"command_text": "players", "instance_id": "z"})
    assert result["success"] is True
    stdin.assert_called_once_with("z", "players")


def test_kick_and_ban_show_what_the_server_answered():
    """데몬은 module 명령의 답에서 message만 호출자에게 돌려준다 (saba-chan command.rs run_module_command).
    없는 사용자를 추방해도 성공 문구만 보이지 않게, 서버가 콘솔에 찍은 답을 message로 준다"""
    from zomboid import lifecycle
    answered = {"success": True, "data": {"response": "User ghost doesn't exist.", "executed": 'kick "ghost"'}}
    with mock.patch.object(lifecycle, "daemon_stdin_command", return_value=answered):
        result = lifecycle.command({"command": "kick", "args": {"player": "ghost"}, "instance_id": "z"})
    assert (result["success"], result["message"]) == (True, "User ghost doesn't exist.")
    # 서버가 아무 줄도 찍지 않으면(data가 null) 보냈다는 문구
    with mock.patch.object(lifecycle, "daemon_stdin_command", return_value={"success": True, "data": None}):
        result = lifecycle.command({"command": "banuser", "args": {"player": "rj"}, "instance_id": "z"})
    assert 'banuser "rj"' in result["message"]


def test_a_quote_in_kick_input_is_refused_as_an_invalid_argument():
    """콘솔 템플릿이었을 때 데몬이 주던 거절과 같은 코드·칸·규칙: 화면·디스코드가 어느 칸이 왜 틀렸는지 알린다"""
    from zomboid import lifecycle
    with mock.patch.object(lifecycle, "daemon_stdin_command") as stdin:
        refused = lifecycle.command({"command": "kick", "args": {"player": "rj", "reason": 'x" -r "y'}, "instance_id": "z"})
    stdin.assert_not_called()
    assert refused["success"] is False
    assert (refused["error_code"], refused["field"], refused["rule"]) == ("invalid_argument", "reason", "quote")
    assert refused["params"] == {"value": 'x" -r "y'}


def test_kick_and_ban_without_a_reason_send_no_empty_reason():
    """서버는 빈 인자를 받지 않는다(CommandBase 인자 규칙 `(.+)`): `kick "이름" -r ""`는 명령 전체가 거절된다"""
    from zomboid import lifecycle
    sent = []
    with mock.patch.object(lifecycle, "daemon_stdin_command", side_effect=lambda i, line: sent.append(line) or {}):
        for name in ("kick", "banuser"):
            assert lifecycle.command({"command": name, "args": {"player": "Rick Grimes"}, "instance_id": "z"})["success"]
            assert lifecycle.command({"command": name, "args": {"player": "rj", "reason": "spawn kill"}, "instance_id": "z"})["success"]
        # 따옴표는 서버가 인자를 가르는 글자다: 값에 있으면 보내지 않는다
        refused = lifecycle.command({"command": "kick", "args": {"player": "rj", "reason": 'x" -r "y'}, "instance_id": "z"})
    assert refused["success"] is False
    assert sent == ['kick "Rick Grimes"', 'kick "rj" -r "spawn kill"', 'banuser "Rick Grimes"', 'banuser "rj" -r "spawn kill"']


def test_console_templates_leave_no_empty_quotes_for_a_blank_input():
    """데몬은 빈 선택 입력의 `{자리}`만 지운다: 따옴표로 감싼 자리는 `""`가 남아 서버가 빈 인자로 읽는다.
    createhorde는 서버 콘솔에서 사용자명이 필수다 (pzwiki Admin commands)"""
    blank = []
    for field in _load_module_toml()["commands"]["fields"]:
        for item in field.get("inputs", []):
            optional = not item.get("required") and "default" not in item
            if optional and '"{%s}"' % item["name"] in field.get("console_template", ""):
                blank.append((field["name"], item["name"]))
    assert blank == []


# ─── 콘솔 하이라이팅 규칙 (GUI syntaxHighlight.js와 같은 우선순위 방식) ───

def _highlighted(line, rules):
    """priority 큰 규칙부터, 이미 칠한 칸은 덮지 않는다. [(조각, color)]"""
    import re
    styles = [None] * len(line)
    for rule in sorted(rules, key=lambda r: -r.get("priority", 0)):
        for m in re.finditer(rule["pattern"], line):
            s, e = m.span("hl") if "hl" in m.re.groupindex and m.group("hl") is not None else m.span()
            for i in range(s, e):
                if styles[i] is None:
                    styles[i] = rule["color"]
    out, start = [], 0
    for i in range(1, len(line) + 1):
        if i == len(line) or styles[i] != styles[start]:
            if styles[start] is not None:
                out.append((line[start:i], styles[start]))
            start = i
    return out


B41_LINE = "LOG  : General     , 1703936123456> SERVER STARTED"
B42_LINE = "LOG  : Network      f:0, t:1765669004670, st:214 957 503> Server exited"


@pytest.mark.parametrize("line", [B41_LINE, B42_LINE])
def test_highlight_marks_category_and_header_for_b41_and_b42(line):
    parts = _highlighted(line, _load_module_toml()["syntax_highlight"]["rules"])
    assert ("LOG", "info") in parts
    assert any(c == "keyword" and t in ("General", "Network") for t, c in parts)
    assert any(c == "dim" and ("1703936123456" in t or "t:1765669004670" in t) for t, c in parts)


def test_highlight_marks_server_exited_as_shutdown():
    parts = _highlighted(B42_LINE, _load_module_toml()["syntax_highlight"]["rules"])
    assert ("Server exited", "warn") in parts


@pytest.mark.parametrize("line, level", [
    (B41_LINE, "LOG"),
    ("ERROR: General      f:0, t:1765669004675, st:1> boom", "ERROR"),
    ("LOG  : General      f:0, t:1765669004675, st:1> mod says ERROR: nope", "LOG"),
])
def test_log_pattern_reads_level_prefix_only(line, level):
    import re
    m = re.search(_load_module_toml()["module"]["log_pattern"], line)
    assert m and m.group("level") == level


def test_log_pattern_skips_unprefixed_stack_lines():
    import re
    assert re.search(_load_module_toml()["module"]["log_pattern"], "\tat zombie.network.ServerMap.SaveAll(ServerMap.java:75)") is None


def test_highlight_marks_unix_path_after_space_only():
    rules = _load_module_toml()["syntax_highlight"]["rules"]
    assert ("/home/pz/Zomboid/Server", "path") in _highlighted("LOG  : General      f:0, t:1765669004675, st:1> loading /home/pz/Zomboid/Server", rules)
    assert not any(c == "path" for _, c in _highlighted("LOG  : General      f:0, t:1765669004675, st:1> see a.b/c/d", rules))


def test_saving_one_setting_keeps_other_values_in_a_locale_encoded_ini(tmp_path, monkeypatch):
    """PZ나 메모장이 cp949로 쓴 ini에서 설정 하나를 저장하면 다른 키의 한글 값이 U+FFFD로 영구히 깨졌다.
    영문 Windows(cp1252)에서도 한글 값을 깨진 글자로 읽지 않는다"""
    from zomboid import lifecycle
    from zomboid.lifecycle import IniManager
    monkeypatch.setattr(__import__("locale"), "getpreferredencoding", lambda do_setlocale=True: "cp1252")
    ini = tmp_path / "servertest.ini"
    ini.write_bytes("PublicName=우리 서버\nMaxPlayers=8\n".encode("cp949"))
    assert IniManager.read(str(ini))["PublicName"] == "우리 서버"
    IniManager.write(str(ini), {"MaxPlayers": "16"})
    raw = ini.read_bytes()
    assert "PublicName=우리 서버".encode("cp949") in raw
    assert b"MaxPlayers=16" in raw


def test_reset_server_reports_a_data_folder_it_cannot_reach(tmp_path, monkeypatch):
    """데이터 폴더에 접근할 수 없으면 아무것도 지우지 않고 '초기화 완료'를 돌려주던 것을 실패로 알린다"""
    from zomboid import lifecycle
    real_stat = os.lstat

    def denied(path, *a, **kw):
        if str(path).startswith(str(tmp_path)):
            raise PermissionError(13, "Access is denied", str(path))
        return real_stat(path, *a, **kw)

    monkeypatch.setattr(lifecycle.os, "lstat", denied)
    assert lifecycle.reset_server({"cache_dir": str(tmp_path), "server_name": "servertest"})["success"] is False
    monkeypatch.setattr(lifecycle.os, "lstat", real_stat)
    assert lifecycle.reset_server({"cache_dir": str(tmp_path / "none"), "server_name": "servertest"})["success"] is True


def test_the_assigned_port_reaches_the_server(tmp_path):
    """두 번째 인스턴스는 데몬이 옮긴 포트로 떠야 한다: 실행 인자 -port로 넘긴다"""
    from unittest import mock
    from zomboid import lifecycle
    parsed = {"java_exe_abs": "java", "classpath": "a.jar", "jvm_flags": ["-Xmx2g"], "main_class": "zombie.network.GameServer", "extra_args": []}
    with mock.patch.object(lifecycle, "_find_server_dir", return_value=str(tmp_path)), \
         mock.patch.object(lifecycle, "_parse_launch_info", return_value=parsed):
        args = lifecycle.get_launch_command({"port": 16300, "preview": True})["args"]
        default = lifecycle.get_launch_command({"preview": True})["args"]
    assert args[args.index("-port") + 1] == "16300"
    # 두 번째 포트(UDPPort, 기본 port+1)도 함께 옮긴다: ini 값(16262)이 남으면 첫 서버와 겹친다
    assert args[args.index("-udpport") + 1] == "16301"
    assert "-port" not in default and "-udpport" not in default


def test_the_declared_udp_port_reaches_the_server(tmp_path):
    """두 번째 포트는 udp_port 설정(데몬이 다른 인스턴스와 함께 옮기고 충돌을 검사하는 포트)에서 넘긴다 (saba-chan 리뷰 289)"""
    from unittest import mock
    from zomboid import lifecycle
    parsed = {"java_exe_abs": "java", "classpath": "a.jar", "jvm_flags": [], "main_class": "zombie.network.GameServer", "extra_args": []}
    with mock.patch.object(lifecycle, "_find_server_dir", return_value=str(tmp_path)), \
         mock.patch.object(lifecycle, "_parse_launch_info", return_value=parsed):
        args = lifecycle.get_launch_command({"port": 16271, "udp_port": 16300, "preview": True})["args"]
        top = lifecycle.get_launch_command({"port": 65535, "preview": True})["args"]
    assert args[args.index("-udpport") + 1] == "16300"
    # 마지막 포트도 -port로 넘긴다. port+1이 없으니 -udpport만 뺀다
    assert top[top.index("-port") + 1] == "65535" and "-udpport" not in top
    from daemon_rcon import setting_file_keys
    assert setting_file_keys(lifecycle.MODULE_DIR)["udp_port"] == "UDPPort"


@pytest.mark.parametrize(("has_db", "include_password"), [(False, True), (True, False)])
def test_admin_password_is_only_passed_before_the_server_database_exists(tmp_path, monkeypatch, has_db, include_password):
    from unittest import mock
    from zomboid import lifecycle

    parsed = {"java_exe_abs": "java", "classpath": "a.jar", "jvm_flags": [],
              "main_class": "zombie.network.GameServer", "extra_args": []}
    monkeypatch.setattr(lifecycle, "_has_server_db", lambda _config: has_db)
    with mock.patch.object(lifecycle, "_find_server_dir", return_value=str(tmp_path)), \
         mock.patch.object(lifecycle, "_parse_launch_info", return_value=parsed):
        args = lifecycle.get_launch_command({"admin_password": "private", "preview": True})["args"]

    assert ("-adminpassword" in args) is include_password
    assert ("private" in args) is include_password


def test_steamcmd_extension_import_failure_does_not_search_for_an_executable(monkeypatch):
    from zomboid import lifecycle
    import extensions.steamcmd

    # 익스텐션이 없거나 꺼져 있으면(import 가드) 데몬이 켜기를 안내하도록 extension_required로 답한다
    monkeypatch.delattr(extensions.steamcmd, "install_for_module", raising=False)
    monkeypatch.setattr(lifecycle.subprocess, "run", lambda *args, **kwargs: pytest.fail("unexpected executable search"))

    result = lifecycle.install_server({"install_dir": "server", "settings": {}})

    assert result["success"] is False
    assert result["message"]
    assert result["error_code"] == "extension_required" and result["missing_extensions"] == ["steamcmd"]


def test_a_new_server_starts_with_the_password_and_size_chosen_when_creating_it(tmp_path, monkeypatch):
    """만들 때 고른 접속 암호·인원·포트는 첫 설치에서 서버 ini에 들어간다 (암호 없이 열리지 않게). 업데이트는 덮지 않는다"""
    from unittest import mock
    from zomboid import lifecycle

    home = tmp_path / "data"
    settings = {"cache_dir": str(home), "server_name": "srv", "password": "pw", "max_players": 4, "port": 16300, "ram": 4}
    with mock.patch("extensions.steamcmd.install_for_module", return_value={"success": True}):
        assert lifecycle.install_server({"install_dir": str(tmp_path / "server"), "settings": settings})["success"]
        ini = home / "Server" / "srv.ini"
        values = lifecycle.IniManager.read(str(ini))
        assert (values["Password"], values["MaxPlayers"], values["DefaultPort"]) == ("pw", "4", "16300")
        lifecycle.IniManager.update(str(ini), {"Password": "changed"})
        assert lifecycle.install_server({"install_dir": str(tmp_path / "server"), "settings": settings})["success"]
    assert lifecycle.IniManager.read(str(ini))["Password"] == "changed"


def test_backup_paths_fail_when_the_server_folder_cannot_be_read(tmp_path):
    """Server/를 읽지 못하면 SandboxVars·spawn 설정이 조용히 빠지므로 백업 훅을 실패시킨다"""
    from unittest import mock
    from zomboid import lifecycle
    (tmp_path / "Server").mkdir()
    with mock.patch.object(lifecycle.os, "listdir", side_effect=PermissionError("denied")):
        result = lifecycle.backup_paths({"cache_dir": str(tmp_path), "server_name": "myserver"})
    assert result["success"] is False and result.get("message"), result


def test_command_risks_follow_user_rule():
    """위험도 기준(사용자 지정 2026-11-06): 안전 = 조회·단순 변경·영향이 작은 게임 안 변화,
    주의 = 위험할 수도 있는 게임 안 명령·PC 자원을 많이 쓰는 명령,
    위험 = 특정 유저에게 큰 영향을 주는 게임 안 명령·설정/로그 파일을 직접 바꾸는 명령.
    게임에 직접 끄라고 보내는 명령은 사바쨩 [정지]와 같은 일이라 위험으로 두고 디스코드 목록에서 뺀다"""
    with open(os.path.join(os.path.dirname(__file__), "module.toml"), "rb") as f:
        fields = {c["name"]: c for c in tomllib.load(f)["commands"]["fields"]}
    risks = {n: c.get("risk") for n, c in fields.items()}
    expected = {"safe": ("servermsg", "players", "save", "showoptions", "startrain", "stoprain"),
                "caution": ("teleport", "teleportto", "chopper", "gunshot", "additem", "addxp", "godmod", "invisible"),
                "danger": ("kick", "banuser", "unbanuser", "adduser", "setaccesslevel", "createhorde",
                           "reloadoptions", "changeoption", "quit")}
    for risk, names in expected.items():
        for name in names:
            assert risks[name] == risk, name
    assert fields["quit"]["remote"] is False


def _worlds(home, *names):
    for n in names:
        world = home / "Saves" / "Multiplayer" / n
        world.mkdir(parents=True)
        (world / "map.bin").write_text("x")


def test_reset_with_an_empty_server_name_deletes_only_the_default_world(tmp_path):
    from zomboid.lifecycle import reset_server, backup_paths
    home = tmp_path / "home"
    _worlds(home, "servertest", "other")
    config = {"working_dir": str(tmp_path / "server"), "cache_dir": str(home), "server_name": ""}

    assert reset_server(config)["success"] is True

    assert not (home / "Saves" / "Multiplayer" / "servertest").exists()
    assert (home / "Saves" / "Multiplayer" / "other" / "map.bin").exists()
    assert str(home / "Saves" / "Multiplayer" / "servertest") in backup_paths(config)["paths"]


def test_reset_preview_lists_this_servers_world_without_deleting(tmp_path):
    from zomboid.lifecycle import reset_preview, reset_server
    home = tmp_path / "home"
    _worlds(home, "servertest", "other")
    config = {"working_dir": str(tmp_path / "server"), "cache_dir": str(home), "server_name": "servertest"}

    seen = reset_preview(config)

    assert seen["success"], seen
    [scope] = seen["scopes"]
    [item] = scope["items"]
    world = home / "Saves" / "Multiplayer" / "servertest"
    assert item["paths"] == [str(world)] and item["label"] and scope["note"]
    assert (world / "map.bin").exists()
    assert reset_preview({**config, "server_name": "../other"})["success"] is False
    assert reset_server(config)["success"] is True
    assert not world.exists()


@pytest.mark.parametrize("name", ["..", ".", "a/b", "a\\b", "../other", "C:other", "other ", "other."])
def test_reset_refuses_a_server_name_outside_its_own_world(tmp_path, name):
    from zomboid.lifecycle import reset_server
    home = tmp_path / "home"
    _worlds(home, "other")
    config = {"working_dir": str(tmp_path / "server"), "cache_dir": str(home), "server_name": name}

    assert reset_server(config)["success"] is False

    assert (home / "Saves" / "Multiplayer" / "other" / "map.bin").exists()


@pytest.mark.parametrize("key", ["cache_dir", "config_dir"])
@pytest.mark.parametrize("name", ["../other", "a/b", "a\\b", "C:other"])
def test_ini_path_rejects_a_profile_name_outside_its_folder(tmp_path, key, name):
    from daemon_rcon import PathOutside
    from zomboid.lifecycle import IniManager

    with pytest.raises(PathOutside):
        IniManager.get_ini_path({key: str(tmp_path), "server_name": name})


@pytest.mark.parametrize("layout", ["server", "home", "flat"])
def test_import_records_the_existing_data_root_without_moving_the_ini(tmp_path, layout):
    from zomboid import lifecycle

    home = tmp_path / "data"
    ini_dir = home if layout == "flat" else home / "Server"
    ini_dir.mkdir(parents=True)
    ini = ini_dir / "srv.ini"
    lifecycle.IniManager.write(str(ini), {"PublicName": "Existing", "MaxPlayers": "7"})
    config = {"config_dir": str(ini_dir if layout == "server" else home), "server_name": "srv"}

    imported = lifecycle.import_settings(config)

    assert imported["settings"]["data_home"] == str(home)
    assert imported["settings"]["public_name"] == "Existing"
    assert lifecycle.IniManager.get_ini_path({**config, **imported["settings"]}) == str(ini)
    assert str(home / "Saves" / "Multiplayer" / "srv") in lifecycle.backup_paths({**config, **imported["settings"]})["paths"]


def test_the_compatibility_data_root_can_be_loaded_without_legacy_config(tmp_path):
    from zomboid import lifecycle

    config = {"data_home": str(tmp_path), "server_name": "srv"}
    assert lifecycle.IniManager.get_ini_path(config) == str(tmp_path / "Server" / "srv.ini")
    assert str(tmp_path / "db" / "srv.db") in lifecycle.backup_paths(config)["paths"]


def test_first_install_fails_when_the_selected_settings_cannot_be_written(tmp_path, monkeypatch):
    from zomboid import lifecycle
    import extensions.steamcmd

    monkeypatch.setattr(extensions.steamcmd, "install_for_module", lambda *args, **kwargs: {"success": True})
    def denied(*args, **kwargs):
        raise PermissionError("denied")

    monkeypatch.setattr(lifecycle.IniManager, "write", denied)
    result = lifecycle.install_server({"install_dir": str(tmp_path / "server"), "settings": {"cache_dir": str(tmp_path / "data"), "password": "private"}})

    assert result["success"] is False
    assert result["error_code"] == "settings_write_failed"
    assert result["message"] and not result["message"].startswith("errors.")


def test_install_does_not_hide_a_profile_path_escape(tmp_path, monkeypatch):
    from daemon_rcon import PathOutside
    from zomboid import lifecycle
    import extensions.steamcmd

    monkeypatch.setattr(extensions.steamcmd, "install_for_module", lambda *args, **kwargs: {"success": True})
    with pytest.raises(PathOutside):
        lifecycle.install_server({"install_dir": str(tmp_path / "server"), "settings": {"cache_dir": str(tmp_path / "data"), "server_name": "../other", "password": "private"}})


def test_manifest_declares_the_external_data_roots_and_the_sdk_boundary():
    cfg = _load_module_toml()
    assert cfg["extensions"]["saba-core"]["data_roots"] == ["{cache_dir}", "{data_home}", "{home}/Zomboid"]
    assert cfg["module"]["dependencies"]["saba-core"] == ">=0.1.14-0"
    field = next(field for field in cfg["settings"]["fields"] if field["name"] == "data_home")
    assert field["format"] == "path"


@pytest.mark.parametrize("folder", ["Server", "db", "Saves"])
def test_backup_rejects_a_linked_subfolder_outside_the_data_root(tmp_path, folder):
    import subprocess
    from daemon_rcon import PathOutside
    from zomboid.lifecycle import backup_paths

    home = tmp_path / "data"
    outside = tmp_path / "outside"
    home.mkdir()
    outside.mkdir()
    link = home / folder
    if sys.platform == "win32":
        subprocess.run(["cmd", "/c", "mklink", "/J", str(link), str(outside)], check=True, capture_output=True)
    else:
        link.symlink_to(outside, target_is_directory=True)

    with pytest.raises(PathOutside):
        backup_paths({"cache_dir": str(home), "server_name": "srv"})


def _existing_server(tmp_path, script, monkeypatch=None):
    server = tmp_path / "기존 서버"
    server.mkdir()
    (server / "StartServer64.bat").write_text(script, encoding="utf-8")
    if monkeypatch:
        from zomboid import lifecycle
        monkeypatch.setattr(lifecycle, "_find_launch_script", lambda d: os.path.join(d, "StartServer64.bat"))
    return server


def test_import_reads_the_script_this_os_would_run(tmp_path, monkeypatch):
    """.bat과 .sh가 함께 있으면 실행할 때 고르는 스크립트(_find_launch_script)의 값을 가져온다"""
    from zomboid import lifecycle

    server = _existing_server(tmp_path, '".\\jre64\\bin\\java.exe" -Xmx16g -cp java/ zombie.network.GameServer -servername win\n')
    (server / "start-server.sh").write_text('#!/bin/sh\n./jre64/bin/java -Xmx6g -cp java/ zombie.network.GameServer -servername linux "$@"\n', encoding="utf-8")
    monkeypatch.setattr(lifecycle, "_find_launch_script", lambda d: os.path.join(d, "start-server.sh"))

    found = lifecycle._script_settings(str(server))

    assert found["ram"] == 6
    assert found["server_name"] == "linux"


def test_import_resolves_a_relative_data_folder_against_the_server_folder(tmp_path, monkeypatch):
    """스크립트의 -cachedir=./data는 서버 폴더 기준이다 (lifecycle이 도는 폴더 기준이면 다른 설정을 읽는다)"""
    from zomboid import lifecycle

    server = _existing_server(tmp_path, '".\\jre64\\bin\\java.exe" -Xmx8g -cp java/ zombie.network.GameServer -cachedir=./data\n', monkeypatch)
    monkeypatch.chdir(tmp_path)

    found = lifecycle._script_settings(str(server))

    assert found["data_home"] == os.path.normpath(os.path.join(str(server), "data"))


def test_import_reads_memory_profile_and_data_folder_from_the_start_script(tmp_path, monkeypatch):
    """가져온 서버는 쓰던 StartServer64.bat의 -Xmx·-servername·-cachedir를 그대로 쓴다 (기본값 4GB·servertest로 덮지 않게)"""
    from zomboid import lifecycle

    home = tmp_path / "데이터 폴더"
    (home / "Server").mkdir(parents=True)
    lifecycle.IniManager.write(str(home / "Server" / "pvp.ini"), {"PublicName": "Mine", "MaxPlayers": "12"})
    server = _existing_server(tmp_path, (
        '@setlocal enableextensions\n'
        'SET PZ_CLASSPATH=java/;java/projectzomboid.jar\n'
        f'".\\jre64\\bin\\java.exe" -Djava.awt.headless=true -Xms16g -Xmx16g -cp %PZ_CLASSPATH% zombie.network.GameServer -statistic 0 -servername pvp "-cachedir={home}" %1 %2\n'
        'PAUSE\n'
    ), monkeypatch)

    settings = lifecycle.import_settings({"working_dir": str(server)})["settings"]

    assert settings["ram"] == 16
    assert settings["server_name"] == "pvp"
    assert settings["data_home"] == str(home)
    assert settings["public_name"] == "Mine"
    assert settings["max_players"] == 12


@pytest.mark.parametrize("profiles,expected", [(["pvp"], "pvp"), (["pvp", "servertest"], "servertest"), ([], "servertest")])
def test_import_finds_the_only_existing_profile_when_the_script_names_none(tmp_path, monkeypatch, profiles, expected):
    from zomboid import lifecycle

    home = tmp_path / "Zomboid"
    (home / "Server").mkdir(parents=True)
    for name in profiles:
        lifecycle.IniManager.write(str(home / "Server" / f"{name}.ini"), {"PublicName": name})
        (home / "Server" / f"{name}_SandboxVars.lua").write_text("SandboxVars = {}", encoding="utf-8")
    monkeypatch.setenv("USERPROFILE", str(tmp_path))
    monkeypatch.setenv("HOME", str(tmp_path))
    server = _existing_server(tmp_path, '".\\jre64\\bin\\java.exe" -Xmx8g -cp java/ zombie.network.GameServer -statistic 0\n', monkeypatch)

    settings = lifecycle.import_settings({"working_dir": str(server)})["settings"]

    assert settings.get("server_name", "servertest") == expected
    assert settings["ram"] == 8


def test_launch_does_not_repeat_the_scripts_data_folder(tmp_path, monkeypatch):
    """스크립트에 -cachedir가 있어도 데몬이 정한 데이터 폴더 하나만 넘긴다 (둘이면 어느 쪽을 쓸지 PZ에 맡기게 된다)"""
    from zomboid import lifecycle

    server = _existing_server(tmp_path, '".\\jre64\\bin\\java.exe" -Xmx8g -cp java/ zombie.network.GameServer -cachedir=C:\\old\n')
    monkeypatch.setattr(lifecycle, "_find_launch_script", lambda d: os.path.join(d, "StartServer64.bat"))
    (server / "jre64" / "bin").mkdir(parents=True)
    (server / "jre64" / "bin" / "java.exe").write_text("", encoding="utf-8")
    home = tmp_path / "new home"

    result = lifecycle.get_launch_command({"working_dir": str(server), "data_home": str(home), "preview": True})

    cachedirs = [a for a in result["args"] if a.startswith("-cachedir=")]
    assert cachedirs == [f"-cachedir={home}"]


def test_config_files_lists_the_profile_ini(tmp_path):
    from zomboid.lifecycle import FUNCTIONS, IniManager
    config = {"data_home": str(tmp_path), "server_name": "pvp"}
    assert FUNCTIONS["config_files"](config) == {"success": True, "files": [IniManager.get_ini_path(config)]}


def test_import_does_not_offer_defaults_when_the_server_ini_cannot_be_read(tmp_path, monkeypatch):
    """스크립트의 데이터 폴더 ini를 읽지 못하면(없음·권한 없음) 기본값을 가져오지 않는다 (사용자 설정을 기본값으로 덮지 않게)"""
    from zomboid import lifecycle

    home = tmp_path / "읽을 수 없는 데이터"
    server = _existing_server(tmp_path, f'".\\jre64\\bin\\java.exe" -Xmx8g -cp java/ zombie.network.GameServer -servername pvp "-cachedir={home}"\n', monkeypatch)

    settings = lifecycle.import_settings({"working_dir": str(server)})["settings"]

    assert settings["data_home"] == str(home)
    assert settings["server_name"] == "pvp"
    assert not {"public_name", "max_players", "port", "pvp"} & set(settings)


def test_a_data_folder_that_cannot_be_made_fails_with_a_plain_message(tmp_path):
    """데이터 폴더를 만들 수 없으면 원본 오류(경로·트레이스백) 대신 현지화한 한 줄로 실패한다"""
    from unittest import mock
    from zomboid import lifecycle
    parsed = {"java_exe_abs": "java", "classpath": "a.jar", "jvm_flags": ["-Xmx2g"], "main_class": "zombie.network.GameServer", "extra_args": []}
    with mock.patch.object(lifecycle, "_find_server_dir", return_value=str(tmp_path)), \
         mock.patch.object(lifecycle, "_parse_launch_info", return_value=parsed), \
         mock.patch.object(lifecycle.os, "makedirs", side_effect=PermissionError(5, "Access is denied", "D:\\")):
        result = lifecycle.get_launch_command({"data_home": "D:\\zomboid data"})
    assert result["success"] is False
    assert result["message"] == lifecycle.i18n.t("errors.data_home_unavailable")
    assert "D:" not in result["message"]


def test_stop_uses_the_shared_procedure_with_a_console_quit_then_sigterm():
    """정지 절차(기다림·강제 종료·managed·요청 불가)는 공용 graceful_stop이 맡는다. 모듈은 요청 방법만 정한다:
    콘솔 `quit`(저장하고 끝난다) → (콘솔이 없으면) SIGTERM. quit 앞에 save와 5초 대기를 붙이지 않는다"""
    from unittest import mock
    from zomboid import lifecycle
    with mock.patch.object(lifecycle, "graceful_stop", side_effect=lambda config, request, i18n, module_dir: request()) as shared, \
         mock.patch.object(lifecycle, "request_console", return_value=True) as console, \
         mock.patch.object(lifecycle, "request_terminate") as term:
        assert lifecycle.stop({"pid": 7, "instance_id": "z"}) is True
    assert shared.call_args.kwargs["module_dir"] == lifecycle.MODULE_DIR
    console.assert_called_once_with({"pid": 7, "instance_id": "z"}, "quit")
    term.assert_not_called()
    with mock.patch.object(lifecycle, "graceful_stop", side_effect=lambda config, request, i18n, module_dir: request()), \
         mock.patch.object(lifecycle, "request_console", return_value=False), \
         mock.patch.object(lifecycle, "request_terminate", return_value=True) as term:
        assert lifecycle.stop({"pid": 7}) is True
    term.assert_called_once_with(7)


def test_diagnose_reads_the_daemon_lines_then_the_server_console_but_not_the_client_log(tmp_path):
    """진단 규칙은 module.toml [[diagnose]] (공용 diagnose_lines). 데몬이 준 콘솔 줄 → server-console.txt 순이고,
    같은 PC의 게임 클라이언트 로그(console.txt)는 서버 문제가 아니라 보지 않는다 (MP20)"""
    from zomboid import lifecycle
    issues = lifecycle.diagnose_log({"log_lines": ["java.net.BindException: Address already in use"]})["issues"]
    assert [(i["code"], i["severity"]) for i in issues] == [("PORT_IN_USE", "error")]
    home = tmp_path / "Zomboid"
    home.mkdir()
    (home / "console.txt").write_text("java.lang.OutOfMemoryError\n", encoding="utf-8")
    config = {"cache_dir": str(home)}
    assert lifecycle.diagnose_log(config)["issues"] == []
    (home / "server-console.txt").write_text("LuaException: boom\n", encoding="utf-8")
    assert [i["code"] for i in lifecycle.diagnose_log(config)["issues"]] == ["MOD_ERROR"]


def test_install_reads_the_steam_app_from_module_toml(tmp_path):
    """Steam app_id·브랜치는 module.toml [install] 한 곳에 둔다(steamcmd install_for_module).
    업데이트 확인도 같은 선언(steamcmd_beta)을 쓰므로 모듈이 따로 브랜치를 고르지 않는다"""
    from unittest import mock
    from zomboid import lifecycle
    with mock.patch("extensions.steamcmd.install_for_module", return_value={"success": False}) as install:
        lifecycle.install_server({"install_dir": str(tmp_path / "server"), "use_beta": True})
    install.assert_called_once_with(lifecycle.MODULE_DIR, str(tmp_path / "server"), beta=None)
    assert _load_module_toml()["install"]["app_id"] == 380870


def test_list_versions_offers_the_steam_branches():
    """서버마다 고를 수 있는 Steam 브랜치를 버전 목록으로 내놓는다 (앱의 "서버 버전" 칸이 이 목록을 보인다).
    기본은 정식(public)이고, 이름은 locales에서 온다"""
    import json
    from zomboid import lifecycle
    result = lifecycle.list_versions({})
    assert result["success"] is True
    assert [v["id"] for v in result["versions"]] == ["public", "unstable", "legacy41"]
    assert result["latest"]["release"] == "public"
    assert lifecycle.FUNCTIONS["list_versions"] is lifecycle.list_versions
    locales = os.path.join(lifecycle.MODULE_DIR, "locales")
    for name in os.listdir(locales):
        with open(os.path.join(locales, name), encoding="utf-8") as f:
            names = json.load(f)["versions"]
        assert set(names) == {"public", "unstable", "legacy41"} and all(names.values()), name


def test_install_downloads_the_chosen_branch(tmp_path):
    """고른 브랜치(`version`)를 SteamCMD의 -beta로 넘긴다. 정식으로 돌아갈 때도 `public`을 적어 넘긴다.
    목록에 없는 브랜치는 SteamCMD를 부르지 않고 거절한다"""
    from zomboid import lifecycle
    server = str(tmp_path / "server")
    for branch in ("unstable", "legacy41", "public"):
        with mock.patch("extensions.steamcmd.install_for_module", return_value={"success": False}) as install:
            lifecycle.install_server({"install_dir": server, "version": branch})
        install.assert_called_once_with(lifecycle.MODULE_DIR, server, beta=branch)
    with mock.patch("extensions.steamcmd.install_for_module", return_value={"success": False}) as install:
        refused = lifecycle.install_server({"install_dir": server, "version": "b42 -validate"})
    install.assert_not_called()
    assert refused["success"] is False and "b42 -validate" in refused["message"]


def test_a_setting_with_a_line_break_is_a_write_failure(tmp_path, monkeypatch):
    # 줄바꿈은 ini 한 줄을 깨뜨리므로 공용 settings_to_file이 거부한다. 모듈 예외가 아니라 저장 실패로 답해야 한다
    from zomboid import lifecycle
    ini_path = str(tmp_path / "servertest.ini")
    monkeypatch.setattr(lifecycle.IniManager, "get_ini_path", staticmethod(lambda config: ini_path))
    result = lifecycle.configure({"settings": {"public_name": "a\nb"}})
    assert result["success"] is False and "properties_write_failed" not in result["message"]
    assert lifecycle._write_initial_ini({"settings": {"public_name": "a\nb"}})["error_code"] == "settings_write_failed"


def test_highlight_colors_level_prefix_only():
    """본문에 나온 ERROR: 같은 낱말은 줄 레벨이 아니다 (log_pattern과 같이 줄 머리만)"""
    line = "LOG  : General      f:0, t:1765669004675, st:1> mod says ERROR: nope"
    parts = _highlighted(line, _load_module_toml()["syntax_highlight"]["rules"])
    assert ("LOG", "info") in parts
    assert ("ERROR", "error") not in parts


# 콘솔 실측 B42.21 (2026-10-30, 사용자 PC): 머리에 t: 없이 `f:0 st:116,904,051`, 경고·오류는 ` at 클래스.함수   >`
B42_21_LINES = {
    "info": "LOG  : Network      f:0 st:116,999,001> *** SERVER STARTED ****",
    "caller": "WARN : Script       f:0 st:116,947,061 at VehicleScript.Loaded                > vehicle \"RaceCar58\" extents != physicsChassisShape",
    "listening": "LOG  : General      f:0 st:116,999,010> Server is listening on port 16261 (for Steam connection) and port 16262 (for UDPRakNet connection)",
    "console": "LOG  : General      f:0 st:117,001,200> command entered via server console (System.in): \"servermsg ㅎㅇㅎㅇ\"",
    "exception": "zombie.core.properties.IsoPropertyType$IsoPropertyTypeNotFoundException: Property Name not found: ladderW at IsoPropertyType.lookup(IsoPropertyType.java:273). Message: Property Name not found: ladderW",
    "frame": "zombie.core.properties.IsoPropertyType.lookup(IsoPropertyType.java:273)",
}


@pytest.mark.parametrize("key, part", [
    ("info", ("f:0 st:116,999,001", "dim")),
    ("caller", ("f:0 st:116,947,061", "dim")),
    ("caller", ("VehicleScript.Loaded", "keyword")),
    ("caller", ("Script", "keyword")),
    ("listening", ("Server is listening", "success")),
    ("console", ("servermsg ㅎㅇㅎㅇ", "command")),
    ("exception", ("zombie.core.properties.IsoPropertyType$IsoPropertyTypeNotFoundException", "error")),
    ("frame", ("zombie.core.properties.IsoPropertyType.lookup(IsoPropertyType.java:273)", "dim")),
])
def test_highlight_follows_b42_21_console(key, part):
    assert part in _highlighted(B42_21_LINES[key], _load_module_toml()["syntax_highlight"]["rules"])


# 접속·퇴장 줄: 공개 운영 도구가 콘솔에서 읽는 형식 (CubeCoders AMPTemplates project-zomboid.kvp
# UserJoinRegex·UserLeaveRegex, 2025-12 B42 갱신 60afb4e). B41은 이름이 있고, B42 Multiplayer 줄은 guid만 있다
JOIN_LEAVE_LINES = [
    ('LOG  : Network     , 1703936123456> ConnectionManager: [fully-connected] "" connection: guid=1234 ip=10.0.0.5 '
     'steam-id=76561198000000000 access= username="Bob the Builder" connection-type="UDPRakNet"',
     [("[fully-connected]", "success"), ("Bob the Builder", "player")]),
    ('LOG  : Network     , 1703936123456> Disconnected player "Bob the Builder" 76561198000000000',
     [("Bob the Builder", "warn")]),
    ('LOG  : Multiplayer  f:0 st:117,100,000> connection: guid=1234 [fully-connected] ""', [("[fully-connected]", "success")]),
    ('LOG  : Multiplayer  f:0 st:117,200,000> connection: guid=1234 [disconnect] "receive-disconnect"', [("[disconnect]", "warn")]),
]


@pytest.mark.parametrize("line, parts", JOIN_LEAVE_LINES)
def test_highlight_marks_join_and_leave_lines(line, parts):
    painted = _highlighted(line, _load_module_toml()["syntax_highlight"]["rules"])
    for part in parts:
        assert part in painted, painted


def test_highlight_does_not_guess_player_names_from_words():
    """`3 clients connected` 같은 줄의 낱말을 플레이어로 칠하지 않는다"""
    for line in ("LOG  : General      f:0 st:1> 3 clients connected", "LOG  : Network      f:0 st:1> peer disconnected", "LOG  : General      f:0 st:1> it was kicked"):
        assert not any(c in ("player", "warning", "warn") for _, c in _highlighted(line, _load_module_toml()["syntax_highlight"]["rules"])), line


def test_join_and_leave_patterns_count_b41_names_and_b42_connection_ids():
    """상황판 접속자 수: B41은 이름, B42 Multiplayer 줄은 이름이 없어 접속 번호(guid)로 센다 (사용자 결정 2026-10-30).
    형식 근거는 CubeCoders AMPTemplates project-zomboid.kvp UserJoinRegex·UserLeaveRegex"""
    import re
    saba = _load_module_toml()["extensions"]["saba-core"]

    def name(patterns, line):
        for p in patterns:
            m = re.search(p, line)
            if m:
                return m.group("name")
        return None

    join, leave = saba["player_join_patterns"], saba["player_leave_patterns"]
    assert name(join, JOIN_LEAVE_LINES[0][0]) == "Bob the Builder"
    assert name(leave, JOIN_LEAVE_LINES[1][0]) == "Bob the Builder"
    assert name(join, JOIN_LEAVE_LINES[2][0]) == "1234"
    assert name(leave, JOIN_LEAVE_LINES[3][0]) == "1234"
    assert name(leave, 'LOG  : Multiplayer  f:0 st:1> connection: guid=1234 [disconnect] "connection-lost"') == "1234"
    for line in (B41_LINE, B42_21_LINES["console"], "LOG  : General      f:0 st:1> 3 clients connected",
                 'LOG  : General      f:0 st:1> command entered via server console (System.in): "servermsg Disconnected player \\"x\\""'):
        assert name(join, line) is None and name(leave, line) is None, line



def _launchable_server(tmp_path, monkeypatch, java_version=None):
    server = _existing_server(tmp_path, '".\\jre64\\bin\\java.exe" -Xms16g -Xmx16g -XX:+UseZGC -cp java/ zombie.network.GameServer\n', monkeypatch)
    (server / "jre64" / "bin").mkdir(parents=True)
    (server / "jre64" / "bin" / "java.exe").write_text("", encoding="utf-8")
    if java_version:
        (server / "jre64" / "release").write_text(f'IMPLEMENTOR="Azul Systems, Inc."\nJAVA_VERSION="{java_version}"\n', encoding="utf-8")
    return server


@pytest.mark.parametrize("host_gb, heap", [(None, "8g"), (32, "8g"), (9.5, "8g"), (9, "4g"), (5.5, "4g"), (5, "3584m"), (3, "2g")])
def test_empty_memory_is_sized_for_this_pc(tmp_path, monkeypatch, host_gb, heap):
    """메모리를 비우면 데몬이 넘긴 PC 메모리로 정한다: 8G, PC 몫 1.5GB를 못 남기면 4G, 그것도 안 되면 PC − 1.5GB (2G 아래로는 안 내린다)"""
    from zomboid import lifecycle
    if host_gb is None:
        monkeypatch.delenv("SABA_HOST_MEMORY", raising=False)
    else:
        monkeypatch.setenv("SABA_HOST_MEMORY", str(int(host_gb * 1024 ** 3)))
    server = _launchable_server(tmp_path, monkeypatch)

    args = lifecycle.get_launch_command({"working_dir": str(server), "ram": "", "preview": True})["args"]

    assert f"-Xms{heap}" in args and f"-Xmx{heap}" in args


def test_set_memory_is_used_as_is(tmp_path, monkeypatch):
    from zomboid import lifecycle
    monkeypatch.setenv("SABA_HOST_MEMORY", str(4 * 1024 ** 3))
    server = _launchable_server(tmp_path, monkeypatch)

    args = lifecycle.get_launch_command({"working_dir": str(server), "ram": 12, "preview": True})["args"]

    assert "-Xmx12g" in args


@pytest.mark.parametrize("version, on", [("25.0.1", True), ("27", True), ("17.0.1", False), (None, False)])
def test_compact_object_headers_follow_the_bundled_java(tmp_path, monkeypatch, version, on):
    """동봉 JRE가 Java 25 이상이면 Compact Object Headers(JEP 519)를 켠다. 버전을 모르면 켜지 않는다"""
    from zomboid import lifecycle
    server = _launchable_server(tmp_path, monkeypatch, version)

    args = lifecycle.get_launch_command({"working_dir": str(server), "ram": 4, "preview": True})["args"]

    assert ("-XX:+UseCompactObjectHeaders" in args) is on
    if on:
        assert args.index("-XX:+UseCompactObjectHeaders") < args.index("-cp")


def test_gc_follows_the_script_unless_g1_is_chosen(tmp_path, monkeypatch):
    """기본은 게임 공식 스크립트의 GC(ZGC) 그대로. java_gc = g1이면 스크립트의 GC 옵션을 G1 하나로 바꾼다
    (ZGC를 못 쓰는 오래된 Windows Server 등)"""
    from zomboid import lifecycle
    server = _launchable_server(tmp_path, monkeypatch)

    default = lifecycle.get_launch_command({"working_dir": str(server), "ram": 4, "preview": True})["args"]
    assert "-XX:+UseZGC" in default and "-XX:+UseG1GC" not in default

    g1 = lifecycle.get_launch_command({"working_dir": str(server), "ram": 4, "java_gc": "g1", "preview": True})["args"]
    assert "-XX:+UseG1GC" in g1 and "-XX:+UseZGC" not in g1
    assert g1.index("-XX:+UseG1GC") < g1.index("-cp")


def test_g1_choice_also_applies_without_a_launch_script(tmp_path, monkeypatch):
    from zomboid import lifecycle
    server = tmp_path / "srv"
    (server / "jre64" / "bin").mkdir(parents=True)
    (server / "jre64" / "bin" / ("java.exe" if sys.platform == "win32" else "java")).write_text("", encoding="utf-8")
    (server / "java").mkdir()
    (server / "java" / "projectzomboid.jar").write_text("", encoding="utf-8")
    monkeypatch.setattr(lifecycle, "_find_launch_script", lambda d: None)
    monkeypatch.setattr(lifecycle, "_find_server_dir", lambda c: str(server))

    args = lifecycle.get_launch_command({"working_dir": str(server), "ram": 4, "java_gc": "g1", "preview": True})["args"]
    assert "-XX:+UseG1GC" in args and "-XX:+UseZGC" not in args


# ─── 접속자 목록 ([status] players) ─────────────────────────

def _players_reply(monkeypatch, response):
    from zomboid import lifecycle
    sent = []

    def fake(iid, line, **kw):
        sent.append(line)
        return {"success": True, "data": {"response": response}}
    monkeypatch.setattr(lifecycle, "daemon_stdin_command", fake)
    return lifecycle, sent


def test_player_count_is_the_status_function():
    from zomboid import lifecycle
    assert _load_module_toml()["status"]["players"] == "player_count"
    assert "player_count" in lifecycle.FUNCTIONS


def test_player_count_does_not_write_to_the_console_every_minute(monkeypatch):
    """1분 갱신은 콘솔·RCON에 명령을 보내지 않는다. 수는 데몬이 접속·퇴장 줄로 세고, 모듈은 최대 인원만 준다"""
    lifecycle, sent = _players_reply(monkeypatch, "")
    assert lifecycle.player_count({"instance_id": "inst-1"}) == {"success": True}
    assert lifecycle.player_count({"instance_id": "inst-1", "max_players": 16}) == {"success": True, "max": 16}
    assert sent == []


@pytest.mark.parametrize("response,online,names", [
    ("Players connected (0): ", 0, []),
    ("Players connected (2): \n-Bob\n-사바쨩", 2, ["Bob", "사바쨩"]),
    # 콘솔에는 명령 메아리와 로그 줄이 섞여 온다
    ("players\nLOG  : General     f:0, t:1, st:0> something\nPlayers connected (1): \n- Alice ", 1, ["Alice"]),
])
def test_player_list_reads_the_players_command(monkeypatch, response, online, names):
    lifecycle, sent = _players_reply(monkeypatch, response)
    result = lifecycle.player_count({"instance_id": "inst-1", "players_detail": True, "max_players": 32})
    assert sent == ["players"]
    assert result == {"success": True, "online": online, "max": 32, "players": [{"name": n} for n in names]}


def test_player_list_does_not_guess_from_an_unknown_reply(monkeypatch):
    lifecycle, _ = _players_reply(monkeypatch, "Unknown command players")
    result = lifecycle.player_count({"instance_id": "inst-1", "players_detail": True})
    assert result["success"] is False and result["message"]
    # 수가 이름 줄과 맞지 않으면 이름은 싣지 않고 수만 준다
    lifecycle, _ = _players_reply(monkeypatch, "Players connected (3): \n-Bob")
    assert lifecycle.player_count({"instance_id": "inst-1", "players_detail": True}) == {"success": True, "online": 3}


# ─── 사용자 실행 인자 (자바 모드 로더 등) ─────────────────────────

def test_launch_puts_user_jvm_args_before_the_main_class_and_game_args_after(tmp_path, monkeypatch):
    """-javaagent 같은 JVM 인자는 메인 클래스 뒤에 붙으면 게임 인자가 되어 JVM이 읽지 않는다.
    JVM 인자는 -cp 앞에, 게임 인자는 메인 클래스 뒤에 넣고, 스크립트에 이미 있는 인자는 거듭 넣지 않는다"""
    from zomboid import lifecycle
    server = _existing_server(tmp_path, '".\\jre64\\bin\\java.exe" -Xmx16g -Dzomboid.znetlog=1 -cp java/ zombie.network.GameServer\n', monkeypatch)
    (server / "jre64" / "bin").mkdir(parents=True)
    (server / "jre64" / "bin" / "java.exe").write_text("", encoding="utf-8")

    result = lifecycle.get_launch_command({"working_dir": str(server), "ram": 4, "preview": True,
                                           "extra_args": ["-javaagent:ZombieBuddy.jar", "-Dzomboid.znetlog=1", "-debug"]})

    args = result["args"]
    assert result["extra_args_placed"] is True
    assert args.index("-javaagent:ZombieBuddy.jar") < args.index("-cp")
    assert args.count("-Dzomboid.znetlog=1") == 1
    assert args.index("-debug") > args.index("zombie.network.GameServer")


def test_import_keeps_the_scripts_own_jvm_args_as_extra_args(tmp_path, monkeypatch):
    """Steam 업데이트가 StartServer64.bat을 공식 내용으로 되돌려도, 쓰던 스크립트에 더했던 JVM 인자(자바 모드 로더 등)가
    남도록 가져오기에서 추가 실행 인자로 옮긴다. 공식 스크립트의 인자와 사바쨩이 정하는 메모리·GC·데이터 폴더는 옮기지 않는다"""
    from zomboid import lifecycle
    server = _existing_server(tmp_path, (
        '@setlocal enableextensions\n'
        'SET PZ_CLASSPATH=java/;java/projectzomboid.jar\n'
        '".\\jre64\\bin\\java.exe" -Djava.awt.headless=true -Dzomboid.steam=1 -Dzomboid.znetlog=1 -XX:+UseZGC '
        '-XX:-CreateCoredumpOnCrash -XX:-OmitStackTraceInFastThrow -Xms16g -Xmx16g -Djava.library.path=natives/ '
        '-javaagent:ZombieBuddy.jar=frontend=console -Ddeployment.user.cachedir=D:\\pz '
        '-cp %PZ_CLASSPATH% zombie.network.GameServer -statistic 0 %1 %2\n'
        'PAUSE\n'
    ), monkeypatch)

    result = lifecycle.import_settings({"working_dir": str(server)})

    assert result["extra_args"] == ["-javaagent:ZombieBuddy.jar=frontend=console"]


def test_import_of_an_unchanged_official_script_moves_no_args(tmp_path, monkeypatch):
    from zomboid import lifecycle
    server = _existing_server(tmp_path, (
        '".\\jre64\\bin\\java.exe" -Djava.awt.headless=true -Dzomboid.steam=1 -Dzomboid.znetlog=1 -XX:+UseZGC '
        '-XX:-CreateCoredumpOnCrash -XX:-OmitStackTraceInFastThrow -Xms16g -Xmx16g -Djava.library.path=natives/ '
        '-cp java/ zombie.network.GameServer -statistic 0\n'
    ), monkeypatch)

    assert "extra_args" not in lifecycle.import_settings({"working_dir": str(server)})
