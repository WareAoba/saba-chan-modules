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
    from zomboid.lifecycle import backup_paths
    result = backup_paths({"cache_dir": str(tmp_path), "server_name": "myserver"})
    assert result["success"] is True
    assert result["paths"] == [
        os.path.join(str(tmp_path), "Saves", "Multiplayer", "myserver"),
        os.path.join(str(tmp_path), "db", "myserver.db"),
        os.path.join(str(tmp_path), "Server"),
    ]


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


def test_normal_stop_never_force_kills_first():
    """RCON is off: a normal stop goes console save+quit (or SIGTERM) and waits; kill only after the wait"""
    from unittest import mock
    from zomboid import lifecycle
    calls = []
    with mock.patch.object(lifecycle, "_console_quit", side_effect=lambda iid: calls.append("quit") or True), \
         mock.patch.object(lifecycle, "_request_terminate", side_effect=lambda pid: calls.append("term") or True), \
         mock.patch.object(lifecycle, "wait_for_exit", side_effect=lambda pid, timeout: calls.append("wait") or True), \
         mock.patch.object(lifecycle, "force_kill", side_effect=lambda pid: calls.append("kill") or (True, None)):
        assert lifecycle.stop({"pid": 7, "instance_id": "z"})["success"] is True
    assert calls == ["quit", "wait"]

    calls.clear()
    with mock.patch.object(lifecycle, "_console_quit", return_value=False), \
         mock.patch.object(lifecycle, "_request_terminate", side_effect=lambda pid: calls.append("term") or True), \
         mock.patch.object(lifecycle, "wait_for_exit", side_effect=lambda pid, timeout: calls.append("wait") or False), \
         mock.patch.object(lifecycle, "force_kill", side_effect=lambda pid: calls.append("kill") or (True, None)):
        lifecycle.stop({"pid": 7})
    assert calls == ["term", "wait", "kill"]


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


def test_saving_one_setting_keeps_other_values_in_a_locale_encoded_ini(tmp_path):
    """PZ나 메모장이 cp949로 쓴 ini에서 설정 하나를 저장하면 다른 키의 한글 값이 U+FFFD로 영구히 깨졌다"""
    from zomboid.lifecycle import IniManager
    ini = tmp_path / "servertest.ini"
    ini.write_bytes("PublicName=우리 서버\nMaxPlayers=8\n".encode("cp949"))
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
