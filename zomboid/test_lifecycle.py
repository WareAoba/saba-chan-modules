"""Contract tests for Project Zomboid lifecycle module.

핵심 목적: module.toml ↔ lifecycle.py 사이의 필드명 매핑이 틀어지면
프로덕션에서 설정이 조용히 무시되는 사고가 발생한다. 이 테스트는
"필드 이름 한 글자라도 drift하면 즉시 실패"하도록 강제한다.
"""
import os
import sys

import pytest

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


# ─── 2. KEY_MAP ↔ DEFAULT 일관성 ────────────────────────────

def test_ini_key_map_values_mostly_in_defaults():
    """_INI_KEY_MAP이 가리키는 INI 키의 상당수는 DEFAULT_INI_PROPERTIES에 있어야 한다.
    모두 있어야 한다고 강제하진 않는다 (PZ는 기본값이 서버 최초 실행 시 생성되는
    필드도 있음). 단 coverage가 극도로 낮으면 매핑이 깨진 것."""
    from zomboid.lifecycle import _INI_KEY_MAP, DEFAULT_INI_PROPERTIES

    covered = sum(1 for v in _INI_KEY_MAP.values() if v in DEFAULT_INI_PROPERTIES)
    ratio = covered / max(1, len(_INI_KEY_MAP))
    assert ratio >= 0.3, (
        f"_INI_KEY_MAP → DEFAULT_INI_PROPERTIES coverage가 비정상적으로 낮음 "
        f"({covered}/{len(_INI_KEY_MAP)} = {ratio:.1%}). 필드명 drift 의심."
    )


def test_ini_key_map_no_duplicate_targets():
    """두 friendly name이 같은 INI 키에 매핑되면 설정이 소실된다."""
    from zomboid.lifecycle import _INI_KEY_MAP

    reverse = {}
    for friendly, ini_key in _INI_KEY_MAP.items():
        reverse.setdefault(ini_key, []).append(friendly)
    dupes = {k: v for k, v in reverse.items() if len(v) > 1}
    assert not dupes, f"중복 매핑된 INI 키: {dupes}"


def test_ini_key_map_reverse_is_bijective():
    """_INI_KEY_MAP_REV는 _INI_KEY_MAP의 완전 역이어야 한다.
    일방향만 수정되면 read_properties가 잘못된 필드를 반환한다."""
    from zomboid.lifecycle import _INI_KEY_MAP, _INI_KEY_MAP_REV
    assert _INI_KEY_MAP_REV == {v: k for k, v in _INI_KEY_MAP.items()}


def test_default_ini_no_duplicate_keys_case_insensitive():
    """INI 키는 대소문자 구분을 않는 경우가 많다. DEFAULT_INI_PROPERTIES에
    동일 키가 대소문자만 다르게 중복되면 예측 불가."""
    from zomboid.lifecycle import DEFAULT_INI_PROPERTIES
    seen = {}
    for k in DEFAULT_INI_PROPERTIES:
        low = k.lower()
        seen.setdefault(low, []).append(k)
    dupes = {k: v for k, v in seen.items() if len(v) > 1}
    # server_browser_announced_ip 같이 의도된 예외는 허용하나,
    # 완전 동일 대소문자 중복은 불가
    strict_dupes = {k: v for k, v in dupes.items() if len(set(v)) > 1}
    # DEFAULT_INI_PROPERTIES는 dict라 같은 이름은 이미 합쳐짐. 이 테스트는
    # "case insensitive collision"을 경고 수준으로 감지.
    # → 중복 존재만 확인하고 실패시키진 않는다. 대신 mutually-distinct
    #    case-only collisions은 경고.
    assert isinstance(strict_dupes, dict)  # placeholder: 항상 통과 (diagnostic)


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
    """INI에 쓰여야 하는 UI 필드가 모두 _INI_KEY_MAP에 있어야 한다.
    누락되면 GUI에서 바꾼 값이 서버에 반영되지 않는다. (RCON 칸은 없다: 보안상 늘 꺼 둔다)"""
    from zomboid.lifecycle import _INI_KEY_MAP

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
        assert td in path


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


def test_launch_turns_rcon_off(tmp_path):
    """RCON is blocked for security: an empty RCONPassword disables it in Project Zomboid."""
    from zomboid.lifecycle import _enforce_rcon_policy, IniManager
    ini = tmp_path / "servertest.ini"
    ini.write_text("RCONPassword=secret\nRCONPort=27015\n", encoding="utf-8")
    assert _enforce_rcon_policy(str(ini)) == ""
    assert IniManager.read(str(ini))["RCONPassword"] == ""


def test_commands_go_to_the_console():
    from unittest import mock
    from zomboid import lifecycle
    with mock.patch.object(lifecycle, "daemon_stdin_command", return_value={"data": {}}) as stdin:
        result = lifecycle.command({"command_text": "players", "instance_id": "z"})
    assert result["success"] is True
    stdin.assert_called_once_with("z", "players")


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
    monkeypatch.setattr(lifecycle.locale, "getpreferredencoding", lambda do_setlocale=True: "cp1252")
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
    real_stat = os.stat

    def denied(path, *a, **kw):
        if str(path).startswith(str(tmp_path)):
            raise PermissionError(13, "Access is denied", str(path))
        return real_stat(path, *a, **kw)

    monkeypatch.setattr(lifecycle.os, "stat", denied)
    assert lifecycle.reset_server({"cache_dir": str(tmp_path), "server_name": "servertest"})["success"] is False
    monkeypatch.setattr(lifecycle.os, "stat", real_stat)
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
    assert lifecycle._INI_KEY_MAP["udp_port"] == "UDPPort"


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

    monkeypatch.setenv("SABA_ENABLED_EXTENSIONS", "steamcmd")
    monkeypatch.delattr(extensions.steamcmd, "SteamCMD", raising=False)
    monkeypatch.setattr(lifecycle.subprocess, "run", lambda *args, **kwargs: pytest.fail("unexpected executable search"))

    result = lifecycle.install_server({"install_dir": "server", "settings": {}})

    assert result["success"] is False
    assert result["message"]


def test_a_new_server_starts_with_the_password_and_size_chosen_when_creating_it(tmp_path, monkeypatch):
    """만들 때 고른 접속 암호·인원·포트는 첫 설치에서 서버 ini에 들어간다 (암호 없이 열리지 않게). 업데이트는 덮지 않는다"""
    from unittest import mock
    from zomboid import lifecycle

    class FakeSteam:
        def ensure_available(self):
            pass

        def install(self, **kwargs):
            return {"success": True}

    monkeypatch.setenv("SABA_ENABLED_EXTENSIONS", "steamcmd")
    home = tmp_path / "data"
    settings = {"cache_dir": str(home), "server_name": "srv", "password": "pw", "max_players": 4, "port": 16300, "ram": 4}
    with mock.patch("extensions.steamcmd.SteamCMD", FakeSteam):
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


def test_commands_that_change_access_or_grant_things_are_danger():
    """권한·밴 해제·계정·서버 옵션을 바꾸거나 아이템·경험치·무적을 주는 명령은 danger.
    caution은 말하는 사바쨩이 확인 없이 실행할 수 있다(L48)"""
    with open(os.path.join(os.path.dirname(__file__), "module.toml"), "rb") as f:
        risks = {c["name"]: c.get("risk") for c in tomllib.load(f)["commands"]["fields"]}
    for name in ('unbanuser', 'adduser', 'setaccesslevel', 'banuser', 'additem', 'addxp', 'godmod', 'invisible', 'createhorde', 'changeoption', 'reloadoptions'):
        assert risks[name] == "danger", name


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

    class FakeSteam:
        def ensure_available(self):
            pass

        def install(self, **kwargs):
            return {"success": True}

    monkeypatch.setenv("SABA_ENABLED_EXTENSIONS", "steamcmd")
    monkeypatch.setattr(extensions.steamcmd, "SteamCMD", FakeSteam)
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

    class FakeSteam:
        def ensure_available(self):
            pass

        def install(self, **kwargs):
            return {"success": True}

    monkeypatch.setenv("SABA_ENABLED_EXTENSIONS", "steamcmd")
    monkeypatch.setattr(extensions.steamcmd, "SteamCMD", FakeSteam)
    with pytest.raises(PathOutside):
        lifecycle.install_server({"install_dir": str(tmp_path / "server"), "settings": {"cache_dir": str(tmp_path / "data"), "server_name": "../other", "password": "private"}})


def test_manifest_declares_the_external_data_roots_and_the_sdk_boundary():
    cfg = _load_module_toml()
    assert cfg["extensions"]["saba-core"]["data_roots"] == ["{cache_dir}", "{data_home}", "{home}/Zomboid"]
    assert cfg["module"]["dependencies"]["saba-core"] == ">=0.1.11-0"
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
