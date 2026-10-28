import pytest
"""Tests for Minecraft lifecycle module functions."""
import json
import os
import subprocess
import sys
import tempfile
from unittest import mock

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))

# 데몬이 호출하는 lifecycle 함수 전체 (src/supervisor/mod.rs, src/ipc/handlers/*)
DAEMON_FUNCTIONS = {
    "get_launch_command", "stop", "status", "command", "validate", "configure",
    "read_properties", "reset_properties", "reset_server", "import_settings",
    "accept_agreements", "diagnose_log", "list_versions",
    "get_version_details", "install_server", "get_installed_version", "instance_commands",
}


def test_default_properties_loaded():
    """DEFAULT_PROPERTIES should load from server.properties file."""
    from minecraft.lifecycle import DEFAULT_PROPERTIES
    assert isinstance(DEFAULT_PROPERTIES, dict)
    assert len(DEFAULT_PROPERTIES) > 30, f"Expected 30+ properties, got {len(DEFAULT_PROPERTIES)}"
    # saba-chan override: RCON is blocked
    assert DEFAULT_PROPERTIES.get("enable-rcon") == "false"
    # Vanilla properties should be present
    assert "server-port" in DEFAULT_PROPERTIES
    assert "difficulty" in DEFAULT_PROPERTIES
    assert "gamemode" in DEFAULT_PROPERTIES


def test_default_properties_no_empty_keys():
    """No property key should be empty."""
    from minecraft.lifecycle import DEFAULT_PROPERTIES
    for key in DEFAULT_PROPERTIES:
        assert key.strip() != "", "Found empty key in DEFAULT_PROPERTIES"


def test_property_key_map_coverage():
    """module.toml file_key values should reference valid server.properties keys."""
    from daemon_rcon import setting_file_keys
    for friendly_name, prop_key in setting_file_keys(MODULE_DIR).items():
        # Most mapped keys should exist in DEFAULT_PROPERTIES
        # (some might be saba-chan-specific, so we just check format)
        assert "-" in prop_key or "." in prop_key or prop_key.isalpha(), \
            f"Key map entry '{friendly_name}' -> '{prop_key}' looks invalid"


def test_server_properties_manager_write_and_read():
    """ServerPropertiesManager read/write roundtrip."""
    from minecraft.lifecycle import ServerPropertiesManager
    with tempfile.TemporaryDirectory() as tmpdir:
        mgr = ServerPropertiesManager(tmpdir)
        props = {"server-port": "25565", "gamemode": "creative", "motd": "Test Server"}
        mgr.write(props)
        assert mgr.exists()
        result = mgr.read()
        assert result["server-port"] == "25565"
        assert result["gamemode"] == "creative"
        assert result["motd"] == "Test Server"


def test_server_properties_manager_update():
    """ServerPropertiesManager update merges into existing."""
    from minecraft.lifecycle import ServerPropertiesManager
    with tempfile.TemporaryDirectory() as tmpdir:
        mgr = ServerPropertiesManager(tmpdir)
        mgr.write({"server-port": "25565", "gamemode": "survival"})
        mgr.update({"gamemode": "creative"})
        result = mgr.read()
        assert result["server-port"] == "25565"
        assert result["gamemode"] == "creative"


def test_a_failed_properties_save_keeps_the_old_file(monkeypatch):
    """저장이 중간에 실패해도 server.properties가 비거나 잘리지 않는다 (화이트리스트 등 접속 설정이 기본값으로 돌아가지 않게)"""
    from minecraft.lifecycle import ServerPropertiesManager
    with tempfile.TemporaryDirectory() as tmpdir:
        mgr = ServerPropertiesManager(tmpdir)
        mgr.write({"white-list": "true", "online-mode": "true"})
        before = open(mgr.path, "rb").read()

        def broken_replace(*_):
            raise OSError("disk full")
        # 바꾸는 단계에서 실패시킨다 (Windows는 기존 파일을 os.replace가 아니라 ReplaceFileW로 바꾼다)
        import daemon_rcon
        monkeypatch.setattr(daemon_rcon, "_replace_keeping_access", broken_replace)

        assert mgr.update({"white-list": "false"}) is False
        assert mgr.write({"white-list": "false"}) is False
        assert open(mgr.path, "rb").read() == before
        assert [n for n in os.listdir(tmpdir) if n.endswith(".tmp")] == []


def test_server_properties_manager_update_new_file():
    """ServerPropertiesManager update creates file from defaults if missing."""
    from minecraft.lifecycle import ServerPropertiesManager
    with tempfile.TemporaryDirectory() as tmpdir:
        mgr = ServerPropertiesManager(tmpdir)
        assert not mgr.exists()
        mgr.update({"motd": "Hello World"})
        assert mgr.exists()
        result = mgr.read()
        assert result["motd"] == "Hello World"
        # Defaults should also be present
        assert "server-port" in result


def test_server_properties_manager_get_defaults():
    """get_defaults returns a copy of DEFAULT_PROPERTIES."""
    from minecraft.lifecycle import ServerPropertiesManager, DEFAULT_PROPERTIES
    with tempfile.TemporaryDirectory() as tmpdir:
        mgr = ServerPropertiesManager(tmpdir)
        defaults = mgr.get_defaults()
        assert defaults == DEFAULT_PROPERTIES
        # Should be a copy, not same reference
        defaults["test-key"] = "test-value"
        assert "test-key" not in DEFAULT_PROPERTIES


def _make_fake_jar(tmpdir, name="server.jar"):
    """Create a fake server jar for tests. Returns its path."""
    jar_path = os.path.join(tmpdir, name)
    with open(jar_path, "w") as f:
        f.write("fake")
    return jar_path


# ── get_launch_command tests ─────────────────────────────────

def test_build_launch_info_basic():
    """get_launch_command returns program, args, working_dir on valid config."""
    from minecraft.lifecycle import get_launch_command
    with tempfile.TemporaryDirectory() as tmpdir:
        jar = _make_fake_jar(tmpdir)
        result = get_launch_command({
            "server_jar": jar,
            "working_dir": tmpdir,
            "ram": 2,
        })
        assert result["success"] is True
        assert "program" in result
        assert "-Xmx2G" in result["args"]
        assert "-Xms2G" in result["args"]
        assert "-jar" in result["args"]
        assert result["working_dir"] == os.path.abspath(tmpdir)


def test_build_launch_info_missing_jar():
    """get_launch_command returns action_required when jar missing."""
    from minecraft.lifecycle import get_launch_command
    with tempfile.TemporaryDirectory() as tmpdir:
        result = get_launch_command({
            "working_dir": tmpdir,
            "ram": 2,
        })
        assert result["success"] is False
        assert result.get("action_required") == "executable_not_found"


def test_build_launch_info_nonexistent_jar():
    """get_launch_command returns action_required when jar file does not exist."""
    from minecraft.lifecycle import get_launch_command
    with tempfile.TemporaryDirectory() as tmpdir:
        result = get_launch_command({
            "server_jar": os.path.join(tmpdir, "nonexistent.jar"),
            "working_dir": tmpdir,
            "ram": 2,
        })
        assert result["success"] is False
        assert result.get("action_required") == "executable_not_found"


def test_build_launch_info_fractional_ram():
    """get_launch_command converts fractional GB to MB."""
    from minecraft.lifecycle import get_launch_command
    with tempfile.TemporaryDirectory() as tmpdir:
        jar = _make_fake_jar(tmpdir)
        result = get_launch_command({
            "server_jar": jar,
            "working_dir": tmpdir,
            "ram": 0.5,
        })
        assert result["success"] is True
        assert "-Xmx512M" in result["args"]
        assert "-Xms512M" in result["args"]


def test_memory_size_values_pass_through_and_old_numbers_still_work():
    """ram은 "4G"·"1024M" 형식 그대로, 예전에 저장된 숫자(GB)도 읽는다."""
    from minecraft.lifecycle import get_launch_command
    with tempfile.TemporaryDirectory() as tmpdir:
        jar = _make_fake_jar(tmpdir)
        for ram, want in [("4G", "-Xmx4G"), ("1024M", "-Xmx1024M"), (4, "-Xmx4G"), ("bad", "-Xmx2G")]:
            result = get_launch_command({"server_jar": jar, "working_dir": tmpdir, "ram": ram})
            assert want in result["args"], (ram, result["args"])


def test_saved_settings_still_pass_the_declared_value_rules():
    """예전에 숫자(GB)로 저장된 ram과 꺼짐을 뜻하는 관리 포트 0이 저장 검사에서 막히지 않는다."""
    import re
    fields = {f["name"]: f for f in _module_toml()["settings"]["fields"]}
    ram = fields["ram"]
    assert "format" not in ram
    for ok in ["4G", "1024M", "512k", "4", "0.5"]:
        assert re.fullmatch(ram["pattern"], ok), ok
    for bad in ["4096", "4GB", "-Xmx4G", ""]:
        assert not re.fullmatch(ram["pattern"], bad), bad
    port = fields["management_server_port"]
    assert "format" not in port and port["min"] == 0 and port["default"] == 0


def test_extra_args_go_before_the_jar_and_preview_writes_nothing():
    """사용자 추가 인자는 JVM 자리(-jar 앞)에, 미리보기는 server.properties를 만들지 않는다."""
    from minecraft.lifecycle import get_launch_command
    with tempfile.TemporaryDirectory() as tmpdir:
        jar = _make_fake_jar(tmpdir)
        result = get_launch_command({
            "server_jar": jar,
            "working_dir": tmpdir,
            "ram": "2G",
            "extra_args": ["-XX:+UseZGC"],
            "preview": True,
        })
        assert result["extra_args_placed"] is True
        args = result["args"]
        assert args.index("-XX:+UseZGC") < args.index("-jar")
        assert not os.path.exists(os.path.join(tmpdir, "server.properties"))


def test_build_launch_info_aikar_flags():
    """get_launch_command includes Aikar flags when requested."""
    from minecraft.lifecycle import get_launch_command
    with tempfile.TemporaryDirectory() as tmpdir:
        jar = _make_fake_jar(tmpdir)
        result = get_launch_command({
            "server_jar": jar,
            "working_dir": tmpdir,
            "ram": 2,
            "use_aikar_flags": True,
        })
        assert result["success"] is True
        assert "-XX:+UseG1GC" in result["args"]


def test_build_launch_info_extra_jvm_args_list():
    """get_launch_command appends extra JVM args from list."""
    from minecraft.lifecycle import get_launch_command
    with tempfile.TemporaryDirectory() as tmpdir:
        jar = _make_fake_jar(tmpdir)
        result = get_launch_command({
            "server_jar": jar,
            "working_dir": tmpdir,
            "ram": 2,
            "jvm_args": ["-Dfoo=bar"],
        })
        assert "-Dfoo=bar" in result["args"]


def test_build_launch_info_extra_jvm_args_string():
    """get_launch_command splits extra JVM args from string."""
    from minecraft.lifecycle import get_launch_command
    with tempfile.TemporaryDirectory() as tmpdir:
        jar = _make_fake_jar(tmpdir)
        result = get_launch_command({
            "server_jar": jar,
            "working_dir": tmpdir,
            "ram": 2,
            "jvm_args": "-Dfoo=bar -Dbaz=qux",
        })
        assert "-Dfoo=bar" in result["args"]
        assert "-Dbaz=qux" in result["args"]


def test_build_launch_info_eula_not_accepted_by_default():
    """get_launch_command does NOT auto-accept EULA by default."""
    from minecraft.lifecycle import get_launch_command, EulaManager
    with tempfile.TemporaryDirectory() as tmpdir:
        jar = _make_fake_jar(tmpdir)
        result = get_launch_command({
            "server_jar": jar,
            "working_dir": tmpdir,
            "ram": 2,
        })
        assert result["success"] is True
        accepted, _ = EulaManager.check_eula(tmpdir)
        assert not accepted, "EULA should not be auto-accepted without auto_eula=True"


def test_eula_is_written_only_when_the_user_accepted_it(tmp_path):
    from minecraft.lifecycle import accept_agreements, EulaManager
    assert accept_agreements({"working_dir": str(tmp_path), "accepted_agreements": []})["success"] is True
    assert not EulaManager.check_eula(str(tmp_path))[0]
    assert accept_agreements({"working_dir": str(tmp_path), "accepted_agreements": ["eula"]})["success"] is True
    assert EulaManager.check_eula(str(tmp_path))[0]


@pytest.mark.parametrize("managed", [True, False])
def test_launch_turns_rcon_off_in_every_mode(managed):
    """RCON is blocked for security: every launch turns it off and does not sync the daemon's password/port."""
    from minecraft.lifecycle import get_launch_command, ServerPropertiesManager
    with tempfile.TemporaryDirectory() as tmpdir:
        jar = _make_fake_jar(tmpdir)
        mgr = ServerPropertiesManager(tmpdir)
        mgr.write({"enable-rcon": "true", "rcon.password": "old", "rcon.port": "25575"})

        get_launch_command({
            "server_jar": jar,
            "working_dir": tmpdir,
            "ram": 2,
            "managed": managed,
            "rcon_password": "daemon-pw",
            "rcon_port": 25580,
        })
        props = mgr.read()
        assert props.get("enable-rcon") == "false"
        assert props.get("rcon.password") == "old"
        assert props.get("rcon.port") == "25575"


def test_rcon_cannot_be_turned_on_from_settings():
    from daemon_rcon import setting_file_keys
    keys = setting_file_keys(MODULE_DIR)
    assert not {"enable_rcon", "rcon_password", "rcon_port", "broadcast_rcon_to_ops"} & set(keys)
    assert not {"enable-rcon", "rcon.password", "rcon.port", "broadcast-rcon-to-ops"} & set(keys.values())


def test_build_launch_info_working_dir_from_jar():
    """get_launch_command infers working_dir from jar path when not specified."""
    from minecraft.lifecycle import get_launch_command
    with tempfile.TemporaryDirectory() as tmpdir:
        jar = _make_fake_jar(tmpdir)
        result = get_launch_command({
            "server_jar": jar,
            "ram": 2,
        })
        assert result["success"] is True
        assert result["working_dir"] == os.path.abspath(tmpdir)


# ── get_launch_command delegates to get_launch_command ──

def test_get_launch_command_returns_success():
    """get_launch_command uses get_launch_command and returns its result."""
    from minecraft.lifecycle import get_launch_command
    with tempfile.TemporaryDirectory() as tmpdir:
        jar = _make_fake_jar(tmpdir)
        result = get_launch_command({
            "server_jar": jar,
            "working_dir": tmpdir,
            "ram": 2,
            "auto_eula": True,
        })
        assert result["success"] is True
        assert "program" in result
        assert "args" in result


# ── 데몬 계약 ────────────────────────────────────────────────

def test_every_daemon_function_is_registered():
    from minecraft.lifecycle import FUNCTIONS
    assert DAEMON_FUNCTIONS <= set(FUNCTIONS), sorted(DAEMON_FUNCTIONS - set(FUNCTIONS))


def test_cli_unknown_function_returns_json_with_zero_exit():
    import daemon_rcon
    import i18n
    env = dict(os.environ)
    runtime = {os.path.dirname(daemon_rcon.__file__), os.path.dirname(i18n.__file__)}
    env["PYTHONPATH"] = os.pathsep.join([*runtime, env.get("PYTHONPATH", "")])
    proc = subprocess.run([sys.executable, "lifecycle.py", "nope"], cwd=MODULE_DIR, input="{}",
                          capture_output=True, text=True, encoding="utf-8", timeout=30, env=env)
    assert proc.returncode == 0, proc.stderr
    payload = json.loads(proc.stdout.strip().splitlines()[-1])
    assert payload["success"] is False and "Unknown function" in payload["message"]


def test_configure_writes_only_mapped_keys():
    from minecraft.lifecycle import configure, ServerPropertiesManager
    with tempfile.TemporaryDirectory() as tmpdir:
        ServerPropertiesManager(tmpdir).write({"motd": "old"})
        result = configure({"working_dir": tmpdir, "settings": {
            "motd": "new", "pvp": False, "auto_eula": True, "java_path": "java", "ram": 4,
            "use_aikar_flags": True, "managed_start": True, "graceful_stop": True,
        }})
        assert result["success"] is True
        props = ServerPropertiesManager(tmpdir).read()
        assert props["motd"] == "new" and props["pvp"] == "false"
        for key in ("auto_eula", "java_path", "ram", "use_aikar_flags", "managed_start", "graceful_stop"):
            assert key not in props


def test_import_settings_keeps_string_fields():
    from minecraft.lifecycle import import_settings, ServerPropertiesManager
    with tempfile.TemporaryDirectory() as tmpdir:
        ServerPropertiesManager(tmpdir).write({
            "level-seed": "-4172144997902289642", "motd": "12345", "level-name": "0042",
            "max-players": "20", "pvp": "true", "custom-key": "true",
        })
        settings = import_settings({"working_dir": tmpdir})["settings"]
        assert settings["level_seed"] == "-4172144997902289642"
        assert settings["motd"] == "12345"
        assert settings["level_name"] == "0042"
        assert settings["max_players"] == 20
        assert settings["pvp"] is True
        assert settings["custom-key"] == "true"


def test_reset_server_keeps_jar_eula_and_bundled_java():
    from minecraft.lifecycle import reset_server
    with tempfile.TemporaryDirectory() as tmpdir:
        _make_fake_jar(tmpdir)
        open(os.path.join(tmpdir, "eula.txt"), "w").close()
        os.makedirs(os.path.join(tmpdir, "java", "bin"))
        os.makedirs(os.path.join(tmpdir, "world"))
        open(os.path.join(tmpdir, "server.properties"), "w").close()
        result = reset_server({"working_dir": tmpdir})
        assert result["success"] is True
        assert sorted(os.listdir(tmpdir)) == ["eula.txt", "java", "server.jar"]


def test_parse_major_version_variants():
    from minecraft.lifecycle import JavaDetector
    assert JavaDetector.parse_major_version("17.0.2") == 17
    assert JavaDetector.parse_major_version("1.8.0_362") == 8
    assert JavaDetector.parse_major_version("21-ea") == 21
    assert JavaDetector.parse_major_version("25") == 25
    assert JavaDetector.parse_major_version("garbage") == 0


def test_required_java_by_minecraft_version():
    from minecraft.lifecycle import required_java
    assert required_java("1.21") == 21
    assert required_java("1.21.4") == 21
    assert required_java("1.20.5") == 21
    assert required_java("1.20.6-pre1") == 21
    assert required_java("1.20.4") == 17
    assert required_java("1.18") == 17
    assert required_java("1.17.1") == 16
    assert required_java("1.16.5") == 8
    assert required_java("24w14a") >= 21
    assert required_java("26.1") >= 21
    assert required_java(None) == 17


def test_status_does_not_signal_process():
    from minecraft import lifecycle
    with mock.patch.object(lifecycle.MinecraftPing, "ping", return_value=None), \
         mock.patch.object(lifecycle, "is_process_alive", return_value=True) as alive, \
         mock.patch.object(lifecycle.os, "kill", side_effect=AssertionError("os.kill")):
        result = lifecycle.status({"pid": 1234, "port": 25565})
    assert result["status"] == "starting"
    alive.assert_called_once_with(1234)


def test_ping_closes_socket_on_error():
    from minecraft import lifecycle
    conn = mock.MagicMock()
    conn.__enter__.return_value.sendall.side_effect = OSError("reset")
    with mock.patch.object(lifecycle.socket, "create_connection", return_value=conn):
        assert lifecycle.MinecraftPing.ping("127.0.0.1", 25565) is None
    conn.__exit__.assert_called_once()


# ─── 콘솔 하이라이팅 규칙 (GUI syntaxHighlight.js와 같은 우선순위 방식) ───

def _module_toml():
    import tomllib
    with open(os.path.join(MODULE_DIR, "module.toml"), "rb") as f:
        return tomllib.load(f)


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


@pytest.mark.parametrize("line, level", [
    ("[17:35:00] [Server thread/WARN]: Can't keep up!", "WARN"),          # Vanilla
    ("[17:35:00 WARN]: Can't keep up!", "WARN"),                          # Paper 콘솔
    ("[17:35:00 ERROR]: [LuckPerms] Failed to load", "ERROR"),           # Paper 플러그인 로거
    ("[17:35:00] [Server thread/ERROR] [minecraft/DedicatedServer]: x", "ERROR"),  # Forge
])
def test_log_pattern_parses_level_for_vanilla_paper_forge(line, level):
    import re
    m = re.search(_module_toml()["module"]["log_pattern"], line)
    assert m and m.group("level") == level


def test_log_pattern_ignores_level_word_in_chat():
    import re
    pattern = _module_toml()["module"]["log_pattern"]
    m = re.search(pattern, "[17:25:00 INFO]: <Steve> ERROR] lol")
    assert m.group("level") == "INFO"


def test_highlight_paper_console_line_marks_timestamp_and_level():
    rules = _module_toml()["syntax_highlight"]["rules"]
    parts = _highlighted("[17:35:00 WARN]: Can't keep up! Running 5023ms behind", rules)
    assert ("[17:35:00", "timestamp") in parts
    assert ("WARN", "warn") in parts


def test_highlight_vanilla_console_line_unchanged():
    rules = _module_toml()["syntax_highlight"]["rules"]
    parts = _highlighted("[17:24:15] [Server thread/INFO]: Steve joined the game", rules)
    assert ("[17:24:15]", "timestamp") in parts
    assert ("Server thread/INFO", "dim") in parts


def test_highlight_rules_stay_fast_on_long_tokens():
    """긴 단어(base64 덤프 등) 한 줄에서 규칙이 줄 길이의 제곱으로 느려지지 않는다."""
    import re
    import time
    line = "x" * 5000
    for rule in _module_toml()["syntax_highlight"]["rules"]:
        started = time.perf_counter()
        list(re.finditer(rule["pattern"], line))
        assert time.perf_counter() - started < 0.02, rule["name"]


def test_highlight_marks_adjacent_uuids():
    rules = _module_toml()["syntax_highlight"]["rules"]
    a, b = "069a79f4-44e9-4726-a5be-fca90e38aaf5", "853c80ef-3c37-49fd-aa49-938b674adae6"
    parts = _highlighted(f"[17:24:15 INFO]: {a} {b}", rules)
    assert (a, "uuid") in parts and (b, "uuid") in parts


def test_install_reports_download_progress_to_the_daemon(tmp_path, capsys):
    """서버 jar를 받는 동안 데몬이 읽는 `PROGRESS:{json}` 줄을 낸다 (GUI 진행률이 0%에 머물지 않게)."""
    import hashlib
    import io
    from minecraft.lifecycle import ServerInstaller

    body = b"x" * 300_000

    class Resp(io.BytesIO):
        headers = {"Content-Length": str(len(body))}

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    details = {"has_server": True, "download_url": "https://example.invalid/server.jar", "sha1": hashlib.sha1(body).hexdigest()}
    with mock.patch.object(ServerInstaller, "get_version_details", return_value=details), \
            mock.patch("urllib.request.urlopen", return_value=Resp(body)):
        result = ServerInstaller.install_server("1.21.1", str(tmp_path))
    assert result["success"], result

    lines = [json.loads(l[len("PROGRESS:"):]) for l in capsys.readouterr().err.splitlines() if l.startswith("PROGRESS:")]
    percents = [p["percent"] for p in lines]
    assert percents and percents[-1] == 100 and percents == sorted(percents), percents
    assert len(set(percents)) == len(percents), "같은 퍼센트를 거듭 내지 않는다"
    assert all(p["message"] for p in lines)



def test_a_failed_update_download_keeps_the_running_jar(tmp_path):
    """업데이트 받기가 중간에 끊기거나 체크섬이 틀리면 지금 쓰던 jar를 지우지 않는다. 데몬은 server.jar만 백업하므로
    fabric-server-launch.jar 같은 다른 이름은 모듈이 지켜야 한다 (외부 감사 P1). 받은 것은 검증한 뒤에 바꾼다"""
    import hashlib
    import io
    from minecraft import lifecycle
    from minecraft.lifecycle import ServerInstaller

    body = b"new" * 1000

    class Resp(io.BytesIO):
        headers = {"Content-Length": str(len(body))}

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    class Broken(Resp):
        def read(self, n=-1):
            if self.tell() >= 1000:
                raise ConnectionResetError("reset")
            return super().read(min(n, 1000))

        read1 = read  # 공용 download는 read1로 받는다

    jar = tmp_path / "fabric-server-launch.jar"
    source = {"url": "https://example.invalid/fabric.jar", "sha256": hashlib.sha256(body).hexdigest(), "build": "x"}
    details = {"has_server": True, "download_url": "https://example.invalid/server.jar", "sha1": None}
    cases = [(Broken(body), source), (Resp(body), dict(source, sha256="0" * 64))]
    for resp, src in cases:
        jar.write_bytes(b"old launcher")
        with mock.patch.object(ServerInstaller, "get_version_details", return_value=details), \
                mock.patch.object(lifecycle.server_types, "resolve_download", return_value=src), \
                mock.patch("urllib.request.urlopen", return_value=resp):
            result = ServerInstaller.install_server("1.21.1", str(tmp_path), server_type="fabric")
        assert result["success"] is False, result
        assert jar.read_bytes() == b"old launcher"
        assert sorted(p.name for p in tmp_path.iterdir()) == ["fabric-server-launch.jar"], "받다 만 파일을 남기지 않는다"

    with mock.patch.object(ServerInstaller, "get_version_details", return_value=details), \
            mock.patch.object(lifecycle.server_types, "resolve_download", return_value=source), \
            mock.patch("urllib.request.urlopen", return_value=Resp(body)):
        result = ServerInstaller.install_server("1.21.1", str(tmp_path), server_type="fabric")
    assert result["success"], result
    assert jar.read_bytes() == body
    assert not [p for p in tmp_path.iterdir() if p.name.startswith("fabric-server-launch.jar.")], "임시 파일이 남지 않는다"


def test_an_unhashed_launcher_must_arrive_whole_before_it_replaces_the_old_one(tmp_path):
    """Fabric 런처는 해시가 없다. 연결이 Content-Length보다 일찍 정상 종료되거나 받은 것이 온전한 jar가 아니면
    실패로 알리고 옛 런처를 둔다 (리뷰 561 P2-1). 온전한 jar는 해시 없이도 설치한다"""
    import io
    import zipfile
    from minecraft import lifecycle
    from minecraft.lifecycle import ServerInstaller

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("META-INF/MANIFEST.MF", "Main-Class: x\n" * 200)
    good = buf.getvalue()

    def resp(body, length):
        class Resp(io.BytesIO):
            headers = {"Content-Length": str(length)}

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False
        return Resp(body)

    jar = tmp_path / "fabric-server-launch.jar"
    source = {"url": "https://example.invalid/fabric.jar", "build": "x"}
    details = {"has_server": True, "download_url": "https://example.invalid/server.jar", "sha1": None}

    def install(r):
        with mock.patch.object(ServerInstaller, "get_version_details", return_value=details), \
                mock.patch.object(lifecycle.server_types, "resolve_download", return_value=source), \
                mock.patch("urllib.request.urlopen", return_value=r):
            return ServerInstaller.install_server("1.21.1", str(tmp_path), server_type="fabric")

    for r in (resp(good[: len(good) // 2], len(good)), resp(b"not a jar" * 100, 900)):
        jar.write_bytes(b"old launcher")
        result = install(r)
        assert result["success"] is False, result
        assert jar.read_bytes() == b"old launcher"
        assert sorted(p.name for p in tmp_path.iterdir()) == ["fabric-server-launch.jar"]

    result = install(resp(good, len(good)))
    assert result["success"], result
    assert jar.read_bytes() == good

def test_new_install_writes_the_instance_settings_to_server_properties(tmp_path):
    """새 설치는 인스턴스 설정(프리셋·옮긴 포트)을 server.properties에 쓰고, 사바쨩 설정은 쓰지 않는다."""
    import hashlib
    import io
    from minecraft import lifecycle

    body = b"x" * 1000

    class Resp(io.BytesIO):
        headers = {"Content-Length": str(len(body))}

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    details = {"has_server": True, "download_url": "https://example.invalid/server.jar", "sha1": hashlib.sha1(body).hexdigest()}
    with mock.patch.object(lifecycle.ServerInstaller, "get_version_details", return_value=details), \
            mock.patch("urllib.request.urlopen", return_value=Resp(body)):
        result = lifecycle.install_server({"version": "1.21.1", "install_dir": str(tmp_path),
                                           "settings": {"port": 25570, "difficulty": "hard", "pvp": False,
                                                        "ram": "4G", "server_type": "vanilla"}})
    assert result["success"], result

    props = (tmp_path / "server.properties").read_text(encoding="utf-8")
    assert "server-port=25570" in props and "difficulty=hard" in props and "pvp=false" in props
    assert "ram" not in props and "server_type" not in props


def test_java_download_failure_is_reported_in_the_user_language(tmp_path, monkeypatch):
    """설치 중 Java 받기가 실패하면 영어 고정 문구가 아니라 데몬 언어(ko)로 알린다."""
    from i18n import I18n
    from minecraft import lifecycle
    monkeypatch.setenv("SABA_LANG", "ko")
    monkeypatch.setattr(lifecycle, "i18n", I18n(MODULE_DIR))
    with mock.patch("urllib.request.urlopen", side_effect=OSError("boom")):
        result = lifecycle.JavaDownloader.download_jre(21, str(tmp_path))
    assert result == {"success": False, "message": "Java 다운로드 실패: boom"}


def test_player_count_reads_server_list_ping():
    from minecraft import lifecycle
    ping = {"players": {"online": 3, "max": 20, "sample": []}, "version": {"name": "1.21"}}
    with mock.patch.object(lifecycle.MinecraftPing, "ping", return_value=ping) as p:
        assert lifecycle.player_count({"port": 25570}) == {"success": True, "online": 3, "max": 20}
    p.assert_called_once_with("127.0.0.1", 25570, timeout=3)
    for reply in [None, {"version": {"name": "1.21"}}, {"players": {"online": "many"}}]:
        with mock.patch.object(lifecycle.MinecraftPing, "ping", return_value=reply):
            assert lifecycle.player_count({"port": 25570})["success"] is False, reply



def test_player_count_names_the_players_from_the_console_list():
    """이름은 데몬이 목록을 물을 때(players_detail)만 콘솔 `list`로 읽는다. 1분 갱신은 콘솔에 아무것도 보내지 않는다.
    문구는 서버 jar의 en_us.json "commands.list.players" = "There are %s of a max of %s players online: %s"
    (26.3 server jar에서 확인). 줄 앞에는 러너·서버 로그 머리가 붙는다 (검수 13, 계약 06 §2)"""
    from minecraft import lifecycle
    import daemon_rcon
    ping = {"players": {"online": 2, "max": 20}}
    reply = {"success": True, "data": {"response": "[STDOUT/INFO] [20:07:01] [Server thread/INFO]: There are 2 of a max of 20 players online: Steve, Alex_99"}}
    with mock.patch.object(lifecycle.MinecraftPing, "ping", return_value=ping), \
         mock.patch.object(daemon_rcon, "daemon_stdin_command", return_value=reply) as send:
        assert lifecycle.player_count({"port": 25565, "instance_id": "i1"}) == {"success": True, "online": 2, "max": 20}
        send.assert_not_called()
        result = lifecycle.player_count({"port": 25565, "instance_id": "i1", "players_detail": True})
    send.assert_called_once_with("i1", "list", timeout=5)
    assert result == {"success": True, "online": 2, "max": 20, "players": [{"name": "Steve"}, {"name": "Alex_99"}]}

    # 아무도 없으면 이름 칸이 빈다
    empty = {"success": True, "data": {"response": "There are 0 of a max of 20 players online: "}}
    with mock.patch.object(lifecycle.MinecraftPing, "ping", return_value={"players": {"online": 0, "max": 20}}), \
         mock.patch.object(daemon_rcon, "daemon_stdin_command", return_value=empty):
        assert lifecycle.player_count({"port": 25565, "instance_id": "i1", "players_detail": True})["players"] == []

    # 문구가 다르거나(플러그인이 list를 바꿈), 이름 규칙에 맞지 않거나, 수가 맞지 않거나, 답이 없으면 이름을 짐작하지 않고 수만 답한다
    for response in ["Online (2): Steve Alex", "There are 2 of a max of 20 players online: Steve and Alex",
                     "There are 3 of a max of 20 players online: Steve, Alex", None]:
        bad = {"success": True, "data": {"response": response} if response else None}
        with mock.patch.object(lifecycle.MinecraftPing, "ping", return_value=ping), \
             mock.patch.object(daemon_rcon, "daemon_stdin_command", return_value=bad):
            assert lifecycle.player_count({"port": 25565, "instance_id": "i1", "players_detail": True}) == \
                {"success": True, "online": 2, "max": 20}, response

def test_player_count_without_status_answers_that_the_count_is_unknown(tmp_path):
    """enable-status=false면 핑이 늘 실패한다: 실패로 답하면 데몬이 5분 뒤 응답 없음으로 알린다 (saba-chan 리뷰 282)"""
    from minecraft import lifecycle
    (tmp_path / "server.properties").write_text("enable-status=false\n", encoding="utf-8")
    with mock.patch.object(lifecycle.MinecraftPing, "ping", side_effect=AssertionError("pinged")):
        assert lifecycle.player_count({"working_dir": str(tmp_path), "port": 25565}) == {"success": True}


def test_player_count_pings_the_address_the_server_listens_on(tmp_path):
    """server-ip를 LAN 주소로 묶으면 127.0.0.1로는 닿지 않는다"""
    from minecraft import lifecycle
    (tmp_path / "server.properties").write_text("server-ip=192.168.0.10\n", encoding="utf-8")
    ping = {"players": {"online": 1, "max": 20}}
    with mock.patch.object(lifecycle.MinecraftPing, "ping", return_value=ping) as p:
        assert lifecycle.player_count({"working_dir": str(tmp_path), "port": 25565})["online"] == 1
    p.assert_called_once_with("192.168.0.10", 25565, timeout=3)


def test_a_sandboxed_run_pings_its_server_through_the_daemon(monkeypatch, tmp_path):
    """격리 실행은 127.0.0.1에 닿지 못해 접속자 핑이 늘 실패했다. SABA_LOOPBACK_DIR이 있으면 데몬 중계로 핑한다"""
    import daemon_rcon
    from minecraft import lifecycle
    status = json.dumps({"players": {"online": 1, "max": 20}}).encode()
    body = lifecycle.MinecraftPing._encode_varint(0) + lifecycle.MinecraftPing._encode_varint(len(status)) + status
    sent = []

    def relay(port, data, *, timeout=5, idle_ms=None):
        sent.append((port, data, idle_ms))
        return lifecycle.MinecraftPing._encode_varint(len(body)) + body
    monkeypatch.setenv("SABA_LOOPBACK_DIR", str(tmp_path))
    monkeypatch.setattr(daemon_rcon, "loopback_exchange", relay)
    monkeypatch.setattr(lifecycle.socket, "create_connection", lambda *a, **k: pytest.fail("a direct connection was tried"))
    assert lifecycle.MinecraftPing.ping("127.0.0.1", 25570)["players"]["online"] == 1
    assert sent == [(25570, lifecycle.MinecraftPing._request("127.0.0.1", 25570), 300)]


def test_find_java_picks_a_runtime_new_enough_from_the_daemon_list(monkeypatch):
    """데몬이 넘긴 런타임 중 필요한 버전 이상인 64비트를 고르고, `-version`을 돌리지 않는다"""
    import json as _json
    from minecraft.lifecycle import JavaDetector
    runtimes = [
        {"java": "C:/Program Files (x86)/Java/jre1.8.0_381/bin/java.exe", "version": "1.8.0_381", "major": 8, "arch": "x86"},
        {"java": "C:/Program Files/Java/jdk-25/bin/java.exe", "version": "25", "major": 25, "arch": "amd64"},
        {"java": "C:/Program Files/Eclipse Adoptium/jdk-21/bin/java.exe", "version": "21.0.5", "major": 21, "arch": "amd64"},
    ]
    monkeypatch.setenv("SABA_JAVA_RUNTIMES", _json.dumps(runtimes))
    monkeypatch.setattr(JavaDetector, "get_java_info", staticmethod(lambda p: pytest.fail(f"probed {p}")))
    assert JavaDetector.find_java("java", min_major=21)["major_version"] == 21
    assert JavaDetector.find_java("java", min_major=8)["major_version"] == 21  # 32비트 8보다 64비트가 먼저
    # 맞는 것이 없으면 가장 높은 버전을 돌려줘서 "Java N 이상 필요" 안내가 나온다
    assert JavaDetector.find_java("java", min_major=26)["major_version"] == 25


def test_find_java_without_daemon_list_skips_oracle_shims(monkeypatch, tmp_path):
    from minecraft import lifecycle
    from minecraft.lifecycle import JavaDetector
    # Windows는 java.exe를 찾는다
    exe = "java.exe" if os.name == "nt" else "java"
    shim = tmp_path / "Oracle" / "Java" / "java8path" / exe
    real = tmp_path / "jdk-21" / "bin" / exe
    for p in (shim, real):
        p.parent.mkdir(parents=True)
        p.write_text("")
    monkeypatch.delenv("SABA_JAVA_RUNTIMES", raising=False)
    monkeypatch.delenv("JAVA_HOME", raising=False)
    monkeypatch.setattr(lifecycle.shutil, "which", lambda name: str(shim))
    monkeypatch.setattr(JavaDetector, "WINDOWS_PATHS", [str(tmp_path)])
    monkeypatch.setattr(JavaDetector, "UNIX_PATHS", [str(tmp_path)])
    probed = []

    def info(path):
        probed.append(path)
        return {"path": path, "version": "21.0.5", "major_version": 21, "bits64": True}
    monkeypatch.setattr(JavaDetector, "get_java_info", staticmethod(info))
    found = JavaDetector.find_java(min_major=17)
    assert found["path"] == str(real)
    assert str(shim) not in probed


def test_a_bundled_java_too_old_for_the_server_is_skipped(monkeypatch, tmp_path):
    """예전 버전용으로 받아 둔 java/(JRE 17)를 1.21 서버에 쓰면 시작이 영구히 막힌다.
    번들 Java는 필요한 주 버전 이상일 때만 쓰고, 아니면 데몬이 찾은 시스템 Java를 쓴다"""
    import json as _json
    from minecraft import lifecycle
    bundled = tmp_path / "java" / "bin" / ("java.exe" if os.name == "nt" else "java")
    bundled.parent.mkdir(parents=True)
    bundled.write_text("")
    (tmp_path / "java" / "release").write_text('JAVA_VERSION="17.0.12"\n')
    system = {"java": "C:/Program Files/Eclipse Adoptium/jdk-21/bin/java.exe", "version": "21.0.5", "major": 21, "arch": "amd64"}
    monkeypatch.setenv("SABA_JAVA_RUNTIMES", _json.dumps([system]))
    config = {"working_dir": str(tmp_path), "server_version": "1.21.1"}
    assert lifecycle._launch_java(config, None) == system["java"]
    assert lifecycle._launch_java({"working_dir": str(tmp_path), "server_version": "1.20.4"}, None) == str(bundled)


def test_the_java_path_the_user_set_wins_over_the_bundled_java(monkeypatch, tmp_path):
    """사용자가 java_path를 정했는데도 서버 폴더의 java/가 먼저 쓰였다"""
    from minecraft import lifecycle
    bundled = tmp_path / "java" / "bin" / ("java.exe" if os.name == "nt" else "java")
    bundled.parent.mkdir(parents=True)
    bundled.write_text("")
    (tmp_path / "java" / "release").write_text('JAVA_VERSION="21.0.5"\n')
    mine = "D:/jdk-21/bin/java.exe"
    monkeypatch.setattr(lifecycle.JavaDetector, "get_java_info", staticmethod(
        lambda p: {"path": p, "version": "21.0.5", "major_version": 21, "bits64": True}))
    config = {"working_dir": str(tmp_path), "server_version": "1.21.1", "java_path": mine}
    assert lifecycle._launch_java(config, None) == mine


def test_validate_uses_the_java_the_daemon_found_without_running_it(monkeypatch, tmp_path):
    """격리 안에서는 `java -version`이 돌지 않을 수 있다. 데몬이 넘긴 런타임은 버전을 이미 알므로
    validate가 다시 돌려 보다 실패를 삼키고 JAVA_NOT_FOUND를 내지 않는다"""
    import json as _json
    from minecraft import lifecycle
    system = {"java": "C:/Program Files/Eclipse Adoptium/jdk-21/bin/java.exe", "version": "21.0.5", "major": 21, "arch": "amd64"}
    monkeypatch.setenv("SABA_JAVA_RUNTIMES", _json.dumps([system]))
    monkeypatch.setattr(lifecycle.JavaDetector, "get_java_info", staticmethod(lambda p: None))
    result = lifecycle.validate({"working_dir": str(tmp_path), "server_version": "1.21.1"})
    codes = [i["code"] for i in result.get("issues", [])]
    assert "JAVA_NOT_FOUND" not in codes and "JAVA_VERSION_TOO_OLD" not in codes, codes


def test_a_failed_jre_extraction_leaves_no_half_java(tmp_path, monkeypatch):
    """압축 풀기가 중간에 실패하면 반쯤 풀린 java/가 남아 그 뒤로 계속 골라졌다.
    새 JRE는 임시 폴더에 풀고 다 풀린 뒤에만 java/를 바꾼다"""
    import io
    import zipfile as _zip
    from minecraft import lifecycle
    old = tmp_path / "java" / "bin"
    old.mkdir(parents=True)
    (old / ("java.exe" if os.name == "nt" else "java")).write_text("old")
    buf = io.BytesIO()
    exe = "java.exe" if os.name == "nt" else "java"
    with _zip.ZipFile(buf, "w", _zip.ZIP_STORED) as zf:
        zf.writestr(f"jdk-21-jre/bin/{exe}", "new")
        zf.writestr("jdk-21-jre/lib/modules", "X" * 64)
    data = bytearray(buf.getvalue())
    data[data.index(b"X" * 64)] = ord("Y")  # 두 번째 파일의 CRC가 맞지 않는다
    body = bytes(data)

    class Resp(io.BytesIO):
        headers = {"Content-Length": str(len(body))}

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(lifecycle.JavaDownloader, "_platform_info", staticmethod(lambda: ("windows", "x64")))
    monkeypatch.setattr(lifecycle.JavaDetector, "get_java_info", staticmethod(lambda p: None))
    with mock.patch("urllib.request.urlopen", side_effect=_fake_adoptium(body)):
        result = lifecycle.JavaDownloader.download_jre(21, str(tmp_path))
    assert result["success"] is False
    assert (old / exe).read_text() == "old", "the previous java/ is untouched"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["java"], "no partial folder or archive is left"


def _jre_zip_response(exe):
    import io
    import zipfile as _zip
    buf = io.BytesIO()
    with _zip.ZipFile(buf, "w") as zf:
        zf.writestr(f"jdk-21-jre/bin/{exe}", "new")
    body = buf.getvalue()

    class Resp(io.BytesIO):
        headers = {"Content-Length": str(len(body))}

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False
    return Resp(body)


def _fake_adoptium(archive, checksum=None, seen=None, binaries=None):
    """urlopen 대역: Adoptium assets API에는 고정 판의 JRE 압축 정보(JSON)를, 그 link에는 archive를 준다"""
    import hashlib
    import io
    import urllib.parse

    class Resp(io.BytesIO):
        def __init__(self, body):
            super().__init__(body)
            self.headers = {"Content-Length": str(len(body))}

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def urlopen(req, timeout=None):
        url = req.full_url if hasattr(req, "full_url") else req
        if seen is not None:
            seen.append(url)
        if "/v3/assets/release_name/" in url:
            query = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))
            release = urllib.parse.unquote(urllib.parse.urlsplit(url).path.rsplit("/", 1)[1])
            found = binaries if binaries is not None else [{
                "os": query["os"], "architecture": query["architecture"], "image_type": "jre",
                "package": {"link": f"https://github.com/adoptium/x/releases/download/{release}/jre.zip",
                            "checksum": checksum or hashlib.sha256(archive).hexdigest()}}]
            return Resp(json.dumps({"release_name": release, "binaries": found}).encode())
        return Resp(archive)
    return urlopen


def test_jre_download_is_pinned_and_checked_against_the_release_checksum(tmp_path, monkeypatch):
    """JRE는 그날의 최신 GA(binary/latest)가 아니라 주 버전별로 고정한 판을 받고, 그 판의 SHA-256으로 확인한다.
    해시가 다르면 풀지 않고 멈춘다 (메이커 관문 6, 감사 0.1.11 P2-2)"""
    from i18n import I18n
    from minecraft import lifecycle
    monkeypatch.setenv("SABA_LANG", "en")
    monkeypatch.setattr(lifecycle, "i18n", I18n(MODULE_DIR))
    exe = "java.exe" if os.name == "nt" else "java"
    archive = _jre_zip_response(exe).getvalue()
    monkeypatch.setattr(lifecycle.JavaDownloader, "_platform_info", staticmethod(lambda: ("windows", "x64")))
    monkeypatch.setattr(lifecycle.JavaDetector, "get_java_info", staticmethod(
        lambda p: {"path": p, "version": "21.0.12", "major_version": 21, "bits64": True} if open(p).read() == "new" else None))
    seen = []
    with mock.patch("urllib.request.urlopen", side_effect=_fake_adoptium(archive, checksum="0" * 64, seen=seen)):
        result = lifecycle.JavaDownloader.download_jre(21, str(tmp_path))
    assert result["success"] is False
    assert "SHA256 mismatch" in result["message"], result
    assert not (tmp_path / "java").exists() and sorted(p.name for p in tmp_path.iterdir()) == []
    release = lifecycle.JavaDownloader._RELEASES[21]
    assert "/v3/assets/release_name/eclipse/" in seen[0] and "latest" not in seen[0], seen
    assert release.replace("+", "%2B") in seen[0] and "os=windows" in seen[0] and "architecture=x64" in seen[0]
    assert seen[1] == f"https://github.com/adoptium/x/releases/download/{release}/jre.zip"

    seen.clear()
    with mock.patch("urllib.request.urlopen", side_effect=_fake_adoptium(archive, seen=seen)):
        result = lifecycle.JavaDownloader.download_jre(21, str(tmp_path))
    assert result["success"], result
    assert (tmp_path / "java" / "bin" / exe).read_text() == "new"


def test_jre_major_without_a_pinned_release_uses_the_next_pinned_one(tmp_path, monkeypatch):
    """고정 판이 없는 주 버전(1.17의 16)은 그보다 높은 가장 낮은 고정 판을 받는다. 그런 판이 없거나
    이 플랫폼용 JRE가 없으면 아무것도 받지 않고 사용자 언어로 알린다"""
    from i18n import I18n
    from minecraft import lifecycle
    monkeypatch.setenv("SABA_LANG", "ko")
    monkeypatch.setattr(lifecycle, "i18n", I18n(MODULE_DIR))
    releases = lifecycle.JavaDownloader._RELEASES
    assert lifecycle.JavaDownloader._pinned_release(16) == releases[17]
    assert lifecycle.JavaDownloader._pinned_release(8) == releases[8]
    assert lifecycle.JavaDownloader._pinned_release(max(releases) + 1) is None
    monkeypatch.setattr(lifecycle.JavaDownloader, "_platform_info", staticmethod(lambda: ("mac", "aarch64")))
    monkeypatch.setattr(lifecycle.JavaDetector, "get_java_info", staticmethod(lambda p: None))
    seen = []
    with mock.patch("urllib.request.urlopen", side_effect=_fake_adoptium(b"", seen=seen)):
        result = lifecycle.JavaDownloader.download_jre(max(releases) + 1, str(tmp_path))
    assert result == {"success": False, "message": lifecycle.i18n.t(
        "errors.java_release_unavailable", version=str(max(releases) + 1), platform="mac aarch64")}
    assert seen == []
    with mock.patch("urllib.request.urlopen", side_effect=_fake_adoptium(b"", seen=seen, binaries=[])):
        result = lifecycle.JavaDownloader.download_jre(8, str(tmp_path))
    assert result["success"] is False and "aarch64" in result["message"] and "{" not in result["message"], result
    assert len(seen) == 1, "only the assets API is asked"
    assert sorted(p.name for p in tmp_path.iterdir()) == []


def test_a_new_jre_that_does_not_run_keeps_the_old_java(tmp_path, monkeypatch):
    """받은 JRE가 이 PC에서 돌지 않으면 원래 java/를 지운 뒤에야 알게 되어 돌던 런타임까지 잃었다.
    새 JRE를 먼저 확인하고, 확인된 뒤에만 바꾼다"""
    from minecraft import lifecycle
    exe = "java.exe" if os.name == "nt" else "java"
    old = tmp_path / "java" / "bin"
    old.mkdir(parents=True)
    (old / exe).write_text("old")
    monkeypatch.setattr(lifecycle.JavaDownloader, "_platform_info", staticmethod(lambda: ("windows", "x64")))
    monkeypatch.setattr(lifecycle.JavaDetector, "get_java_info", staticmethod(lambda p: None))
    with mock.patch("urllib.request.urlopen", side_effect=_fake_adoptium(_jre_zip_response(exe).getvalue())):
        result = lifecycle.JavaDownloader.download_jre(21, str(tmp_path))
    assert result["success"] is False
    assert (old / exe).read_text() == "old"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["java"]


def test_a_failed_java_swap_puts_the_old_java_back(tmp_path, monkeypatch):
    """옛 java/를 옆으로 옮긴 뒤 새 것을 들이지 못하면 java/가 없어져 다음 시작이 Java를 찾지 못했다"""
    from minecraft import lifecycle
    exe = "java.exe" if os.name == "nt" else "java"
    old = tmp_path / "java" / "bin"
    old.mkdir(parents=True)
    (old / exe).write_text("old")
    monkeypatch.setattr(lifecycle.JavaDownloader, "_platform_info", staticmethod(lambda: ("windows", "x64")))
    # 새로 받은 JRE만 돈다 (옛 java/가 돌면 받지 않고 그것을 쓴다)
    monkeypatch.setattr(lifecycle.JavaDetector, "get_java_info", staticmethod(
        lambda p: {"path": p, "version": "21.0.5", "major_version": 21, "bits64": True} if open(p).read() == "new" else None))
    real_replace = os.replace

    def replace(src, dst):
        if str(src).endswith(".part"):
            raise PermissionError("in use")
        return real_replace(src, dst)
    monkeypatch.setattr(lifecycle.os, "replace", replace)
    with mock.patch("urllib.request.urlopen", side_effect=_fake_adoptium(_jre_zip_response(exe).getvalue())):
        result = lifecycle.JavaDownloader.download_jre(21, str(tmp_path))
    assert result["success"] is False
    assert (old / exe).read_text() == "old"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["java"]


def test_reset_server_keeps_what_the_loader_needs_to_start(tmp_path):
    """서버 초기화는 월드·설정만 지운다. Forge·NeoForge(1.17+)는 libraries/의 인자 파일과
    user_jvm_args.txt로 뜨므로 지우면 다시 시작할 수 없다"""
    from minecraft import lifecycle
    keep = ["server.jar", "eula.txt", "user_jvm_args.txt", "run.bat", "fabric-server-launcher.properties",
            "libraries/net/minecraftforge/forge/47.3.0/win_args.txt", ".fabric/remappedJars/x.jar", "java/bin/java", "versions/1.21/x.jar"]
    gone = ["world/level.dat", "server.properties", "logs/latest.log", "ops.json"]
    for rel in keep + gone:
        p = tmp_path / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("x")
    assert lifecycle.reset_server({"working_dir": str(tmp_path)})["success"]
    for rel in keep:
        assert (tmp_path / rel).exists(), rel
    for rel in gone:
        assert not (tmp_path / rel).exists(), rel


def test_reset_server_only_deletes_what_the_server_made(tmp_path):
    """서버 폴더에 사용자 파일·데몬 파일이 섞여 있어도 서버가 만든 월드·설정만 지운다.
    예전에는 허용 목록 밖을 모두 지워 바탕화면에 설치한 서버를 초기화하면 바탕화면이 비었다"""
    from minecraft import lifecycle
    gone = ["world/level.dat", "server.properties", "ops.json", "logs/latest.log"]
    kept = ["server.jar", "Documents/report.docx", "notes.txt", "photo.png", "other-server/world/level.dat"]
    for rel in gone + kept:
        p = tmp_path / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("x")
    result = lifecycle.reset_server({"working_dir": str(tmp_path)})
    assert result["success"], result
    for rel in kept:
        assert (tmp_path / rel).exists(), rel
    for rel in gone:
        assert not (tmp_path / rel).exists(), rel


def test_reset_server_keeps_daemon_files_when_the_server_lives_in_the_instance_folder(tmp_path):
    """인스턴스 폴더에 서버를 둔 경우 데몬의 백업·인스턴스 기록·로그를 지우지 않는다"""
    from minecraft import lifecycle
    for rel in ["instance.json", "settings.json", "backups/a.zip", "logs/daemon.log", "rcon.json", "server.properties", "world/level.dat"]:
        p = tmp_path / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("x")
    assert lifecycle.reset_server({"working_dir": str(tmp_path)})["success"]
    for rel in ["instance.json", "settings.json", "backups/a.zip", "logs/daemon.log", "rcon.json"]:
        assert (tmp_path / rel).exists(), rel
    assert not (tmp_path / "world").exists()


def test_reset_server_refuses_a_folder_that_is_not_a_server(tmp_path):
    """server.properties·eula.txt가 없는 폴더는 서버 폴더로 보지 않고 아무것도 지우지 않는다"""
    from minecraft import lifecycle
    for rel in ["server.jar", "world/level.dat", "notes.txt"]:
        p = tmp_path / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("x")
    result = lifecycle.reset_server({"working_dir": str(tmp_path)})
    assert result["success"] is False
    assert sorted(p.name for p in tmp_path.iterdir()) == ["notes.txt", "server.jar", "world"]


def _server_dir(root, props, files=()):
    root.mkdir(parents=True, exist_ok=True)
    (root / "server.properties").write_text(props, encoding="latin-1")
    for rel in files:
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("x")
    return root


def test_reset_server_deletes_the_world_named_in_server_properties(tmp_path):
    """월드 이름은 server.properties의 level-name을 따른다(다른 차원 폴더 포함)"""
    from minecraft import lifecycle
    srv = _server_dir(tmp_path, "level-name=survival\n", ["survival/level.dat", "survival_nether/x", "survival_the_end/x"])
    assert lifecycle.reset_server({"working_dir": str(srv)})["success"]
    assert sorted(p.name for p in srv.iterdir()) == []


def test_reset_server_follows_a_world_in_a_subfolder_and_escaped_names(tmp_path):
    """바닐라는 하위 경로 level-name을 받고, 값은 Java properties 이스케이프로 저장된다"""
    from minecraft import lifecycle
    srv = _server_dir(tmp_path / "a", "level-name=worlds/survival\n", ["worlds/survival/level.dat", "worlds/other/level.dat"])
    assert lifecycle.reset_server({"working_dir": str(srv)})["success"]
    assert not (srv / "worlds" / "survival").exists()
    assert (srv / "worlds" / "other" / "level.dat").exists()

    srv = _server_dir(tmp_path / "b", "level-name=\\uc6d4\\ub4dc\n", ["\uc6d4\ub4dc/level.dat"])
    assert lifecycle.reset_server({"working_dir": str(srv)})["success"]
    assert not (srv / "\uc6d4\ub4dc").exists()


def test_reset_server_fails_without_deleting_when_the_world_name_is_unsafe(tmp_path):
    """폴더 밖·폴더 자신·끝 점·드라이브를 가리키는 level-name이면 아무것도 지우지 않고 실패로 알린다
    (월드를 남긴 채 성공이라고 하면 다음 시작에 옛 월드가 그대로 뜬다)"""
    from minecraft import lifecycle
    (tmp_path / "outside").mkdir()
    for i, name in enumerate(["../outside", ".", "world.", "my\\:world", "/abs"]):
        srv = _server_dir(tmp_path / f"s{i}", f"level-name={name}\n", ["world/level.dat", "ops.json"])
        result = lifecycle.reset_server({"working_dir": str(srv)})
        assert result["success"] is False, name
        assert (srv / "server.properties").exists() and (srv / "ops.json").exists(), name
    assert (tmp_path / "outside").exists()


def test_reset_server_refuses_a_world_name_that_is_a_kept_item(tmp_path):
    """level-name이 남겨야 할 항목(데몬 백업, 실행에 필요한 폴더·파일)과 같으면 지우지 않는다. Windows처럼 대소문자를 가리지 않는다"""
    from minecraft import lifecycle
    for name in ["backups", "Backups", "instance.json", "java", "libraries", "eula.txt", "server.jar"]:
        srv = _server_dir(tmp_path / name, f"level-name={name}\n",
                          ["instance.json", "backups/a.zip", "java/bin/java", "libraries/x", "eula.txt", "server.jar", "BACKUPS_nether/x"])
        before = sorted(p.name for p in srv.iterdir())
        result = lifecycle.reset_server({"working_dir": str(srv)})
        assert result["success"] is False, name
        assert sorted(p.name for p in srv.iterdir()) == before, name


def test_server_properties_saved_in_the_windows_locale_are_read(tmp_path, monkeypatch):
    """ANSI(cp949) 편집기로 한글 MOTD를 저장한 server.properties 때문에 시작이 UnicodeDecodeError로 막혔다.
    영문 Windows(cp1252)에서도 cp949 파일을 깨진 글자로 읽어 MOTD를 망가뜨리지 않는다"""
    import locale
    from minecraft.lifecycle import ServerPropertiesManager
    monkeypatch.setattr(locale, "getpreferredencoding", lambda do_setlocale=True: "cp1252")
    (tmp_path / "server.properties").write_bytes("motd=우리 서버\nserver-port=25565\n".encode("cp949"))
    manager = ServerPropertiesManager(str(tmp_path))
    assert manager.read()["motd"] == "우리 서버"
    assert manager.update({"server-port": "25570"})
    props = manager.read()
    assert (props["motd"], props["server-port"]) == ("우리 서버", "25570")
    # 서버는 UTF-8로 읽으므로 고쳐 쓸 때 UTF-8로 바꾼다
    assert (tmp_path / "server.properties").read_text(encoding="utf-8").startswith("motd=우리 서버")


def _jar_with_version(path, version):
    import zipfile
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("version.json", json.dumps({"id": version, "name": version, "stable": True}))


def test_installed_version_reads_the_same_config_as_start(tmp_path):
    """데몬은 설치된 버전 확인에도 시작·점검과 같은 값(server_executable 전체 경로, working_dir)을 넘긴다.
    server.jar가 아닌 이름(paper 등)도 그 경로의 jar를 읽는다 (MK7)"""
    from minecraft import lifecycle
    jar = tmp_path / "paper-1.21.4.jar"
    _jar_with_version(jar, "1.21.4")
    (tmp_path / "server.jar").write_bytes(b"not a jar")
    result = lifecycle.get_installed_version({"server_executable": str(jar), "working_dir": str(tmp_path)})
    assert result["success"] and result["version"] == "1.21.4", result


def test_offline_status_reads_the_version_from_server_executable(tmp_path):
    from minecraft import lifecycle
    jar = tmp_path / "custom.jar"
    _jar_with_version(jar, "1.20.6")
    with mock.patch.object(lifecycle.MinecraftPing, "ping", return_value=None):
        result = lifecycle.status({"server_executable": str(jar), "working_dir": str(tmp_path)})
    assert result["status"] == "stopped" and result.get("version") == "1.20.6", result


def test_install_server_names_the_jar_from_server_executable(tmp_path):
    """데몬은 설치할 실행 파일 이름을 server_executable로 넘긴다 (예전 이름 jar_name)"""
    import hashlib
    import io
    from minecraft import lifecycle
    body = b"jar"

    class Resp(io.BytesIO):
        headers = {"Content-Length": str(len(body))}

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    details = {"has_server": True, "download_url": "https://example.invalid/server.jar", "sha1": hashlib.sha1(body).hexdigest()}
    with mock.patch.object(lifecycle.ServerInstaller, "get_version_details", return_value=details), \
            mock.patch("urllib.request.urlopen", return_value=Resp(body)):
        result = lifecycle.install_server({"version": "1.21.1", "install_dir": str(tmp_path),
                                           "server_executable": "my-server.jar", "settings": {"server_type": "vanilla"}})
    assert result["success"], result
    assert (tmp_path / "my-server.jar").is_file() and not (tmp_path / "server.jar").exists()


@pytest.mark.parametrize("jar_name", ["../escape.jar", "sub/server.jar", "C:\\escape.jar"])
def test_installer_rejects_a_jar_name_outside_the_install_root(tmp_path, jar_name):
    from daemon_rcon import PathOutside
    from minecraft import lifecycle
    details = {"has_server": True, "download_url": "https://example.invalid/server.jar"}
    with mock.patch.object(lifecycle.ServerInstaller, "get_version_details", return_value=details), \
            mock.patch("tempfile.mkstemp", side_effect=AssertionError("path must be checked before download staging")), \
            mock.patch("urllib.request.urlopen", side_effect=OSError("download should not start")) as download:
        with pytest.raises(PathOutside):
            lifecycle.ServerInstaller.install_server("1.21.1", str(tmp_path), jar_name=jar_name)
    download.assert_not_called()


def test_install_keeps_the_sdk_path_error(tmp_path):
    from daemon_rcon import PathOutside
    from minecraft import lifecycle
    with mock.patch.object(lifecycle.ServerInstaller, "install_server", side_effect=PathOutside(tmp_path / "outside")):
        with pytest.raises(PathOutside):
            lifecycle.install_server({"version": "1.21.1", "install_dir": str(tmp_path),
                                      "settings": {"server_type": "vanilla"}})


def test_diagnose_log_summarizes_an_oversized_matched_line():
    from minecraft import lifecycle
    result = lifecycle.diagnose_log({"log_lines": ["java.lang.OutOfMemoryError: " + "x" * (4 * 1024 * 1024)]})
    assert result["success"] and result["issues"]
    assert len(json.dumps(result).encode("utf-8")) < 64 * 1024
    assert result["issues"][0]["matched_line"].startswith("java.lang.OutOfMemoryError")


def test_backup_keeps_permissions_and_settings():
    """복원하면 권한·밴·화이트리스트·서버 설정도 돌아와야 한다 (PaperMC 문서: 서버 루트의 데이터 파일)"""
    import tomllib
    with open(os.path.join(os.path.dirname(__file__), "module.toml"), "rb") as f:
        paths = tomllib.load(f)["backup"]["paths"]
    for name in ("server.properties", "ops.json", "whitelist.json", "banned-players.json", "banned-ips.json"):
        assert name in paths, name


def test_backup_keeps_plugin_mod_and_server_type_configs():
    """Paper·Spigot·모드 서버는 플러그인·모드와 그 설정이 플레이 데이터다 (MK18, PaperMC 문서 "Configuration").
    사바쨩이 다시 받아 주지 않는 것이라 복원 때 같이 돌아와야 한다. 다시 만들 수 있는 캐시(.paper-remapped, libraries, logs)는 넣지 않는다"""
    import tomllib
    with open(os.path.join(os.path.dirname(__file__), "module.toml"), "rb") as f:
        paths = tomllib.load(f)["backup"]["paths"]
    # plugins/는 훅(backup_paths)이 지도 타일을 빼고 고른다
    for name in ("mods", "config", "bukkit.yml", "spigot.yml", "commands.yml", "permissions.yml", "help.yml",
                 "paper.yml", "purpur.yml", "pufferfish.yml"):
        assert name in paths, name
    for name in (".paper-remapped", "libraries", "logs", "versions", "cache"):
        assert name not in paths, name


def test_commands_that_change_access_or_grant_things_are_danger():
    """권한·밴 해제·허용 목록을 바꾸거나 아이템·게임 모드를 주는 명령은 danger.
    caution은 말하는 사바쨩이 확인 없이 실행할 수 있다(L48)"""
    import tomllib
    with open(os.path.join(os.path.dirname(__file__), "module.toml"), "rb") as f:
        risks = {c["name"]: c.get("risk") for c in tomllib.load(f)["commands"]["fields"]}
    for name in ("give", "gamemode", "pardon", "op", "deop", "whitelist", "ban", "difficulty"):
        assert risks[name] == "danger", name


def test_backup_leaves_out_rendered_map_tiles(tmp_path):
    """plugins/ 안에서 다시 그릴 수 있는 지도 타일(dynmap 기본 web/tiles, dynmap 위키 "Configuration.txt")은 빼고
    나머지 플러그인 데이터는 그대로 넣는다 (MA5)"""
    from minecraft import lifecycle
    import tomllib
    plugins = tmp_path / "plugins"
    (plugins / "dynmap" / "web" / "tiles" / "world").mkdir(parents=True)
    (plugins / "dynmap" / "web" / "index.html").write_text("x")
    (plugins / "dynmap" / "configuration.txt").write_text("x")
    (plugins / "LuckPerms").mkdir()
    (plugins / "LuckPerms.jar").write_text("x")
    result = lifecycle.backup_paths({"working_dir": str(tmp_path)})
    assert result["success"] is True
    got = sorted(os.path.relpath(p, tmp_path).replace(os.sep, "/") for p in result["paths"])
    assert got == ["plugins/LuckPerms", "plugins/LuckPerms.jar",
                   "plugins/dynmap/configuration.txt", "plugins/dynmap/web/index.html"], got
    with open(os.path.join(os.path.dirname(lifecycle.__file__), "module.toml"), "rb") as f:
        backup = tomllib.load(f)["backup"]
    assert backup.get("hook") is True and "plugins" not in backup["paths"]


def test_backup_without_plugins_folder_and_unreadable_plugins(tmp_path):
    from minecraft import lifecycle
    assert lifecycle.backup_paths({"working_dir": str(tmp_path)}) == {"success": True, "paths": []}
    (tmp_path / "plugins").mkdir()
    with mock.patch.object(lifecycle.os, "listdir", side_effect=PermissionError("denied")):
        result = lifecycle.backup_paths({"working_dir": str(tmp_path)})
    assert result["success"] is False and "denied" in result.get("message", ""), result


def test_an_update_keeps_the_jar_mode_and_clears_old_leftover_downloads(tmp_path):
    """임시 파일(mkstemp, 0600)로 받아도 새 jar는 옛 jar의 권한을 따른다. 강제 종료로 남은 오래된 받기 파일은
    다음 설치가 지우고, 방금 시작한 다른 받기 파일은 건드리지 않는다 (리뷰 568 P3)"""
    import hashlib
    import io
    import time
    from minecraft.lifecycle import ServerInstaller

    body = b"new" * 1000

    class Resp(io.BytesIO):
        headers = {"Content-Length": str(len(body))}

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    jar = tmp_path / "server.jar"
    jar.write_bytes(b"old")
    os.chmod(jar, 0o644)
    stale = tmp_path / "server.jar.abc123.download"
    stale.write_bytes(b"x")
    os.utime(stale, (time.time() - 7200, time.time() - 7200))
    fresh = tmp_path / "server.jar.def456.download"
    fresh.write_bytes(b"y")
    details = {"has_server": True, "download_url": "https://example.invalid/server.jar", "sha1": hashlib.sha1(body).hexdigest()}
    with mock.patch.object(ServerInstaller, "get_version_details", return_value=details), \
            mock.patch("urllib.request.urlopen", return_value=Resp(body)):
        result = ServerInstaller.install_server("1.21.1", str(tmp_path))
    assert result["success"], result
    assert jar.read_bytes() == body
    if os.name != "nt":
        assert jar.stat().st_mode & 0o777 == 0o644
    assert not stale.exists()
    assert fresh.exists()


def test_an_update_still_installs_where_file_modes_cannot_be_set(tmp_path):
    """권한을 바꿀 수 없는 파일시스템(일부 네트워크·FAT 드라이브)에서도 권한 맞추기 실패로 설치를 막지 않는다 (리뷰 575 P3)"""
    import hashlib
    import io
    from minecraft.lifecycle import ServerInstaller

    body = b"new" * 1000

    class Resp(io.BytesIO):
        headers = {"Content-Length": str(len(body))}

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    jar = tmp_path / "server.jar"
    jar.write_bytes(b"old")
    details = {"has_server": True, "download_url": "https://example.invalid/server.jar", "sha1": hashlib.sha1(body).hexdigest()}
    with mock.patch.object(ServerInstaller, "get_version_details", return_value=details), \
            mock.patch("urllib.request.urlopen", return_value=Resp(body)), \
            mock.patch("shutil.copymode", side_effect=PermissionError("chmod not supported")):
        result = ServerInstaller.install_server("1.21.1", str(tmp_path))
    assert result["success"], result
    assert jar.read_bytes() == body


def test_ready_patterns_match_the_done_line_only_from_the_server():
    """서버가 접속을 받기 시작하면 데몬이 instance.ready를 낸다(계약 06 §4). 문구는 PC 정규 설치 26.3 서버의 실제 줄이다.
    데몬은 러너 머리를 뗀 게임 stdout 원문을 맞추므로 줄 처음의 로그 머리에 묶어, 채팅(`<이름> Done (...)`)으로는 흉내 내지 못하게 한다.
    0.1.8 데몬은 schema v1의 모르는 키를 거부하므로 검사하지 않는 [extensions.saba-core] 표에 둔다(코어 의존성은 그대로)"""
    import re
    import tomllib
    with open(os.path.join(os.path.dirname(__file__), "module.toml"), "rb") as f:
        toml = tomllib.load(f)
    patterns = [re.compile(p) for p in toml["extensions"]["saba-core"]["ready_patterns"]]
    # 데몬은 러너 머리 없이 게임 stdout 원문과 맞춘다. 바닐라와 Paper 계열(`[시각 INFO]: `) 머리 둘 다
    real = '[20:07:01] [Server thread/INFO]: Done (0.267s)! For help, type "help"'
    assert any(p.search('[20:07:01 INFO]: Done (3.120s)! For help, type "help"') for p in patterns)
    assert any(p.search(real) for p in patterns)
    assert any(p.search('[20:07:01] [Server thread/INFO] [minecraft/DedicatedServer]: Done (9.512s)! For help, type "help"') for p in patterns)
    chat = '[20:08:00] [Server thread/INFO]: <Steve> Done (1.0s)! For help, type "help"'
    assert not any(p.search(chat) for p in patterns)
    # 채팅에 머리 흉내("]: ")를 넣어도 줄 처음의 로그 머리부터 맞춰야 한다 (리뷰 544)
    assert not any(p.search('[20:08:00] [Server thread/INFO]: <Steve> ]: Done (1.0s)! For help, type "help"') for p in patterns)
    assert "ready_patterns" not in toml["detection"]
    # 정지·설정 파일이 공용 도우미(graceful_stop·update_key_values, saba-core 0.1.11부터)를 쓴다
    assert toml["module"]["dependencies"]["saba-core"] == ">=0.1.11-0"


def test_graceful_exit_patterns_match_the_stop_line_only_from_the_server():
    """정상 종료 문구도 줄 처음의 로그 머리부터 맞춘다. 채팅에 "Stopping the server"를 친 뒤 서버가 충돌 신호 없이 죽으면
    "게임 안 정지"로 잘못 갈라 자동 재시작·알림이 빠지지 않게 한다 (EE2)"""
    import re
    import tomllib
    with open(os.path.join(os.path.dirname(__file__), "module.toml"), "rb") as f:
        toml = tomllib.load(f)
    patterns = [re.compile(p) for p in toml["detection"]["graceful_exit_patterns"]]
    assert any(p.search("[20:07:01] [Server thread/INFO]: Stopping the server") for p in patterns)
    assert any(p.search("[20:07:01 INFO]: Stopping the server") for p in patterns)
    assert any(p.search("[20:07:01] [Server thread/INFO] [minecraft/MinecraftServer]: Stopping the server") for p in patterns)
    assert not any(p.search("[20:08:00] [Server thread/INFO]: <Steve> Stopping the server") for p in patterns)
    assert not any(p.search("[20:08:00] [Server thread/INFO]: <Steve> ]: Stopping the server") for p in patterns)


def test_player_patterns_name_who_joined_or_left():
    """접속·퇴장 줄에서 이름만 잡는다(계약 06 §8.3). 문구는 server jar en_us.json(26.3) 그대로이고,
    채팅·/say로 같은 글을 써도 맞지 않는다"""
    import re
    import tomllib
    with open(os.path.join(os.path.dirname(__file__), "module.toml"), "rb") as f:
        table = tomllib.load(f)["extensions"]["saba-core"]
    join = [re.compile(p) for p in table["player_join_patterns"]]
    leave = [re.compile(p) for p in table["player_leave_patterns"]]

    def name(patterns, line):
        found = [m.group("name") for p in patterns if (m := p.search(line))]
        return found[0] if found else None
    head = "[20:08:00] [Server thread/INFO]: "
    assert name(join, head + "Steve joined the game") == "Steve"
    assert name(join, head + "Alex_99 (formerly known as OldAlex) joined the game") == "Alex_99"
    assert name(leave, head + "Steve left the game") == "Steve"
    assert name(join, "[20:08:00 INFO]: Steve joined the game") == "Steve"
    # Forge·NeoForge는 레벨 뒤에 로거 칸을 붙인다 (fmlloader log4j2.xml `[%t/%level] [minecraft/%logger{1}]: `)
    forge = "[20:08:00] [Server thread/INFO] [minecraft/MinecraftServer]: "
    assert name(join, forge + "Steve joined the game") == "Steve"
    assert name(leave, forge + "Steve left the game") == "Steve"
    assert name(join, forge + "<Alex> Steve joined the game") is None
    for spoof in ["<Steve> Alex joined the game", "[Server] Alex joined the game", "<Steve> Alex left the game",
                  "<Steve> ]: Notch joined the game", "<Steve> ]: Notch left the game",
                  "[Not Secure] <Steve> ]: Notch joined the game"]:
        assert name(join, head + spoof) is None and name(leave, head + spoof) is None, spoof


@pytest.mark.parametrize("name,content,expected", [
    ("start.bat", "@echo off\njava -Xms2G -Xmx10G -jar server.jar nogui\npause\n", "10G"),
    ("run.sh", "#!/bin/sh\nexec java -Xmx3072M -jar paper.jar --nogui\n", "3072M"),
    ("user_jvm_args.txt", "# Forge\n-Xmx6G\n", "6G"),
])
def test_import_keeps_the_memory_of_the_existing_start_script(tmp_path, name, content, expected):
    """가져온 서버는 쓰던 실행 스크립트의 -Xmx를 그대로 쓴다 (모듈 기본값 4G로 줄지 않게)"""
    from minecraft.lifecycle import import_settings, ServerPropertiesManager
    ServerPropertiesManager(str(tmp_path)).write({"motd": "kept"})
    (tmp_path / name).write_text(content, encoding="utf-8")

    settings = import_settings({"working_dir": str(tmp_path)})["settings"]

    assert settings["ram"] == expected
    assert settings["motd"] == "kept"


def test_import_without_a_start_script_leaves_the_memory_setting_alone(tmp_path):
    from minecraft.lifecycle import import_settings, ServerPropertiesManager
    ServerPropertiesManager(str(tmp_path)).write({"motd": "kept"})
    assert "ram" not in import_settings({"working_dir": str(tmp_path)})["settings"]


def test_config_files_lists_the_files_saba_chan_may_change(tmp_path):
    """가져오기를 해제할 때 되돌릴 파일: 사바쨩이 쓰는 server.properties와 eula.txt (있든 없든 경로는 알린다)"""
    from minecraft.lifecycle import FUNCTIONS
    files = FUNCTIONS["config_files"]({"working_dir": str(tmp_path)})
    assert files["success"] is True
    assert sorted(files["files"]) == sorted([str(tmp_path / "server.properties"), str(tmp_path / "eula.txt")])


def _portable_java(java_dir, major):
    """다운로드한 것처럼 둔 포터블 JRE (`release` 파일로 주 버전을 읽는다)"""
    exe = "java.exe" if os.name == "nt" else "java"
    os.makedirs(os.path.join(java_dir, "bin"), exist_ok=True)
    open(os.path.join(java_dir, "bin", exe), "w").close()
    with open(os.path.join(java_dir, "release"), "w", encoding="utf-8") as f:
        f.write(f'JAVA_VERSION="{major}.0.1"\n')
    return os.path.join(java_dir, "bin", exe)


def test_import_fetches_a_portable_java_into_the_instance_folder(tmp_path, monkeypatch):
    """가져온 서버는 PC에 깔린 Java(낮을 수 있다)를 쓰지 않고, 필요한 버전의 포터블 JRE를 인스턴스 폴더에 받는다.
    서버 폴더에는 아무것도 더하지 않는다 (해제할 때 흔적이 남지 않게)"""
    from minecraft import lifecycle
    server, instance = tmp_path / "server", tmp_path / "instance"
    server.mkdir()
    instance.mkdir()
    lifecycle.ServerPropertiesManager(str(server)).write({"motd": "kept"})
    monkeypatch.setattr(lifecycle, "_required_java_for", lambda *a, **k: 25)
    monkeypatch.setattr(lifecycle.JavaDetector, "find_java", lambda *a, **k: {"path": "java", "major_version": 25, "version": "25"})
    calls = []
    def fake_download(major, install_dir):
        calls.append((major, install_dir))
        return {"success": True, "java_path": _portable_java(os.path.join(install_dir, "java"), major)}
    monkeypatch.setattr(lifecycle.JavaDownloader, "download_jre", fake_download)

    result = lifecycle.import_settings({"working_dir": str(server), "instance_dir": str(instance), "migration_import": True})

    assert result["success"] is True
    assert calls == [(25, str(instance))]
    assert not (server / "java").exists()
    info = lifecycle._launch_java_info({"working_dir": str(server), "instance_dir": str(instance)}, None)
    assert info["path"].startswith(str(instance)) and info["major_version"] == 25


def test_import_keeps_a_portable_java_the_server_folder_already_has(tmp_path, monkeypatch):
    from minecraft import lifecycle
    server, instance = tmp_path / "server", tmp_path / "instance"
    instance.mkdir()
    _portable_java(str(server / "java"), 25)
    lifecycle.ServerPropertiesManager(str(server)).write({"motd": "kept"})
    monkeypatch.setattr(lifecycle, "_required_java_for", lambda *a, **k: 21)
    monkeypatch.setattr(lifecycle.JavaDownloader, "download_jre", lambda *a: pytest.fail("no download when the folder has one"))

    assert lifecycle.import_settings({"working_dir": str(server), "instance_dir": str(instance), "migration_import": True})["success"] is True


def test_only_the_migration_import_fetches_java(tmp_path, monkeypatch):
    """런타임 받기는 데몬이 가져오기 표시(`migration_import`)를 실은 호출에서만: 설정 창의 실제 설정 읽기(settings/live)
    같은 다른 import_settings 호출은 기본 시간 한도(120초) 안에 끝나야 하고, 열 때마다 받으려 하지 않는다"""
    from minecraft import lifecycle
    server, instance = tmp_path / "server", tmp_path / "instance"
    server.mkdir()
    instance.mkdir()
    lifecycle.ServerPropertiesManager(str(server)).write({"motd": "kept"})
    monkeypatch.setattr(lifecycle, "_required_java_for", lambda *a, **k: 25)
    monkeypatch.setattr(lifecycle.JavaDownloader, "download_jre", lambda *a: pytest.fail("no download outside the migration import"))

    result = lifecycle.import_settings({"working_dir": str(server), "instance_dir": str(instance)})

    assert result["success"] is True and result["settings"]["motd"] == "kept"


def test_a_failed_java_download_does_not_fail_the_import(tmp_path, monkeypatch):
    """받지 못해도 가져오기는 된다 (시작할 때 Java 문제로 안내한다)"""
    from minecraft import lifecycle
    server, instance = tmp_path / "server", tmp_path / "instance"
    server.mkdir()
    instance.mkdir()
    lifecycle.ServerPropertiesManager(str(server)).write({"motd": "kept"})
    monkeypatch.setattr(lifecycle, "_required_java_for", lambda *a, **k: 25)
    monkeypatch.setattr(lifecycle.JavaDownloader, "download_jre", lambda *a: {"success": False, "message": "offline"})

    result = lifecycle.import_settings({"working_dir": str(server), "instance_dir": str(instance), "migration_import": True})

    assert result["success"] is True and result["settings"]["motd"] == "kept"
    assert "offline" in result.get("java_warning", "")


def test_validate_leaves_the_port_check_to_the_daemon(tmp_path):
    """포트 사용 검사는 데몬이 시작마다 한다(다른 인스턴스, 다른 프로그램, TCP·UDP, Windows 예약 범위).
    모듈이 다시 확인하면 검사를 끈 사용자에게도 경고가 남는다"""
    import socket
    from minecraft import lifecycle
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        s.listen(1)
        port = s.getsockname()[1]
        result = lifecycle.validate({"working_dir": str(tmp_path), "port": port})
    assert "PORT_IN_USE" not in [i["code"] for i in result.get("issues", [])]


def test_stop_uses_the_shared_procedure_with_console_then_sigterm():
    """정지 절차(기다림·강제 종료·managed·요청 불가)는 공용 graceful_stop이 맡는다. 모듈은 요청 방법만 정한다:
    콘솔 `stop` → (콘솔이 없으면) SIGTERM"""
    from minecraft import lifecycle
    with mock.patch.object(lifecycle, "graceful_stop", side_effect=lambda config, request, i18n, module_dir: request()) as shared, \
         mock.patch.object(lifecycle, "request_console", return_value=True) as console, \
         mock.patch.object(lifecycle, "request_terminate") as term:
        assert lifecycle.stop({"pid": 99, "instance_id": "abc"}) is True
    assert shared.call_args.kwargs["module_dir"] == lifecycle.MODULE_DIR
    console.assert_called_once_with({"pid": 99, "instance_id": "abc"}, "stop")
    term.assert_not_called()
    with mock.patch.object(lifecycle, "graceful_stop", side_effect=lambda config, request, i18n, module_dir: request()), \
         mock.patch.object(lifecycle, "request_console", return_value=False), \
         mock.patch.object(lifecycle, "request_terminate", return_value=True) as term:
        assert lifecycle.stop({"pid": 99}) is True
    term.assert_called_once_with(99)


def test_diagnose_rules_come_from_module_toml_and_read_the_latest_log(tmp_path):
    """진단 규칙은 module.toml [[diagnose]] 하나에 둔다(공용 diagnose_lines). 규칙마다 문제·해결 문구가 있다"""
    from daemon_rcon import load_diagnose_rules
    from minecraft import lifecycle
    rules = load_diagnose_rules(MODULE_DIR)
    assert {"JAVA_NOT_FOUND", "EULA_NOT_ACCEPTED", "PORT_IN_USE", "SERVER_OVERLOADED"} <= {r["code"] for r in rules}
    with open(os.path.join(MODULE_DIR, "locales", "ko.json"), encoding="utf-8") as f:
        errors = json.load(f)["errors"]
    for rule in rules:
        assert rule["code"].lower() in errors["detect"] and rule["code"].lower() in errors["solutions"], rule["code"]
    (tmp_path / "logs").mkdir()
    (tmp_path / "logs" / "latest.log").write_text("[12:00:00] [main/INFO]: You need to agree to the EULA\n", encoding="utf-8")
    issues = lifecycle.diagnose_log({"working_dir": str(tmp_path)})["issues"]
    assert [(i["code"], i["severity"]) for i in issues] == [("EULA_NOT_ACCEPTED", "critical")]


def test_a_jre_archive_that_reaches_outside_the_java_folder_is_not_extracted(tmp_path):
    """받은 JRE 압축은 공용 safe_extract로 푼다: java/ 밖을 가리키는 항목이 하나라도 있으면 아무것도 쓰지 않는다"""
    import zipfile
    from minecraft import lifecycle

    def fake_download(url, dest, **kwargs):
        with zipfile.ZipFile(dest, "w") as z:
            z.writestr("jdk-21-jre/bin/java", "x")
            z.writestr("jdk-21-jre/../../evil.txt", "x")
        return {}

    with mock.patch.object(lifecycle, "download", side_effect=fake_download), \
         mock.patch.object(lifecycle.JavaDownloader, "_find_bundled_java", return_value=None):
        result = lifecycle.JavaDownloader.download_jre(21, str(tmp_path))
    assert result["success"] is False
    assert not (tmp_path.parent / "evil.txt").exists()
    assert sorted(p.name for p in tmp_path.iterdir()) == []


def test_a_setting_with_a_line_break_is_a_write_failure():
    # 줄바꿈은 server.properties 한 줄을 깨뜨리므로 공용 settings_to_file이 거부한다. 모듈 예외가 아니라 저장 실패로 답해야 한다
    from minecraft.lifecycle import configure, ServerPropertiesManager
    with tempfile.TemporaryDirectory() as tmpdir:
        ServerPropertiesManager(tmpdir).write({"motd": "old"})
        result = configure({"working_dir": tmpdir, "settings": {"motd": "a\nb"}})
        assert result["success"] is False
        assert ServerPropertiesManager(tmpdir).read()["motd"] == "old"


def test_server_properties_values_use_java_escapes():
    """PC 실측: 저장마다 '월드 유형' 형식 경고. 서버는 level-type=minecraft\\:normal로 쓴다(Java Properties)"""
    from minecraft.lifecycle import ServerPropertiesManager, configure, import_settings
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "server.properties")
        with open(path, "w", encoding="utf-8") as f:
            f.write("level-type=minecraft\\:normal\nmotd=a\\\\b\\u00e9\n")
        assert ServerPropertiesManager(tmpdir).read()["level-type"] == "minecraft:normal"
        assert ServerPropertiesManager(tmpdir).read()["motd"] == "a\\bé"
        assert import_settings({"working_dir": tmpdir})["settings"]["level_type"] == "minecraft:normal"
        assert configure({"working_dir": tmpdir, "settings": {"level_type": "minecraft:flat", "motd": "x=y"}})["success"]
        text = open(path, encoding="utf-8").read()
        assert "level-type=minecraft\\:flat\n" in text and "motd=x\\=y\n" in text
        assert ServerPropertiesManager(tmpdir).read()["motd"] == "x=y"
    with tempfile.TemporaryDirectory() as tmpdir:
        ServerPropertiesManager(tmpdir).write(ServerPropertiesManager(tmpdir).get_defaults())
        assert "level-type=minecraft\\:normal\n" in open(os.path.join(tmpdir, "server.properties"), encoding="utf-8").read()


def test_an_escaped_line_break_survives_import_and_save():
    """motd=a\\nb(두 줄 MOTD)는 가져온 값 그대로 다시 저장할 수 있다. 진짜 줄바꿈으로 풀면 공용 도우미가 거부한다"""
    from minecraft.lifecycle import ServerPropertiesManager, configure, import_settings
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "server.properties")
        with open(path, "w", encoding="utf-8") as f:
            f.write("motd=a\\nb\n")
        motd = import_settings({"working_dir": tmpdir})["settings"]["motd"]
        assert configure({"working_dir": tmpdir, "settings": {"motd": motd}})["success"] is True
        with open(path, encoding="utf-8") as f:
            assert "motd=a\\nb\n" in f.read()


# ─── 공식 설치기로 까는 서버 종류 (Quilt·Forge·NeoForge·BuildTools) ─────────────

def _installer_fixture(tmp_path, server_type, source, run, jre=None):
    """설치기 jar를 받고 Java를 갖춘 뒤 run(cmd, cwd, env)으로 설치기를 돌리는 install_server를 부른다"""
    import hashlib
    import io
    from minecraft import lifecycle
    from minecraft.lifecycle import ServerInstaller

    body = _jar_bytes({"META-INF/MANIFEST.MF": "Main-Class: installer.Main\n"})

    class Resp(io.BytesIO):
        headers = {"Content-Length": str(len(body))}

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    source = {"url": "https://example.invalid/installer.jar", "sha256": hashlib.sha256(body).hexdigest(),
              "installer": True, **source}
    details = {"has_server": True, "download_url": "https://example.invalid/server.jar", "sha1": None,
               "java_major_version": 21}
    calls = []

    def fake_run(cmd, cwd, env):
        calls.append({"cmd": cmd, "cwd": cwd, "env": env, "installer": open(cmd[cmd.index("-jar") + 1], "rb").read()})
        return run(cmd, cwd, env)

    def fake_jre(major, folder, image_type="jre", max_major=None):
        return jre(major, folder, image_type, max_major) if jre else \
            {"success": True, "java_path": os.path.join(folder, "java", "bin", image_type), "version": "21"}

    with mock.patch.object(ServerInstaller, "get_version_details", return_value=details), \
            mock.patch.object(lifecycle.server_types, "resolve_download", return_value=source), \
            mock.patch("urllib.request.urlopen", return_value=Resp(body)), \
            mock.patch.object(lifecycle.JavaDownloader, "download_jre", side_effect=fake_jre) as jres, \
            mock.patch.object(ServerInstaller, "_run_installer_process", side_effect=fake_run):
        result = ServerInstaller.install_server("1.21.1", str(tmp_path), server_type=server_type, accept_eula=True)
    return result, calls, jres, body


def _jar_bytes(files):
    import io
    import zipfile
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, content in files.items():
            z.writestr(name, content)
    return buf.getvalue()


def _no_build_dirs(path):
    return [p.name for p in path.iterdir() if p.name.startswith(".saba-install")] == []


def _quilt_run(cmd, cwd, env):
    target = next(a for a in cmd if a.startswith("--install-dir=")).split("=", 1)[1]
    assert cwd == target
    open(os.path.join(cwd, "installer.jar.log"), "w").write("installer log")
    open(os.path.join(target, "quilt-server-launch.jar"), "wb").write(_jar_bytes({
        "META-INF/MANIFEST.MF": "Main-Class: org.quiltmc.loader.impl.launch.server.QuiltServerLauncher\n"}))
    open(os.path.join(target, "server.jar"), "wb").write(b"vanilla 1.21.1")
    os.makedirs(os.path.join(target, "libraries", "org", "quiltmc"))
    open(os.path.join(target, "libraries", "org", "quiltmc", "quilt-loader.jar"), "wb").write(b"loader")
    return 0, []


def test_quilt_is_installed_by_its_official_installer_with_portable_java(tmp_path):
    (tmp_path / "world").mkdir()
    (tmp_path / "world" / "level.dat").write_bytes(b"save")
    result, calls, jre, body = _installer_fixture(tmp_path, "quilt", {"build": "0.30.1"}, _quilt_run)
    assert result["success"], result
    assert result["executable_path"] == str(tmp_path / "quilt-server-launch.jar")
    assert (result["server_type"], result["build"], result["java_path"]) == ("quilt", "0.30.1", calls[0]["cmd"][0])
    # 설치기는 PC의 Java가 아니라 고정 판 포터블 Java로 돈다. 서버 폴더에 더 새 Java가 있으면 고정 판으로 바꾼다
    assert jre.call_args.args == (21, str(tmp_path)) and jre.call_args.kwargs == {"max_major": 21}
    cmd = calls[0]["cmd"]
    assert cmd[cmd.index("-jar") + 2:-1] == ["install", "server", "1.21.1", "0.30.1", "--download-server"]
    assert calls[0]["installer"] == body, "체크섬을 확인한 설치기를 돌린다"
    # 준비 폴더에 만든 서버를 확인한 뒤 서버 폴더로 옮긴다. 월드는 그대로다
    assert os.path.dirname(os.path.dirname(calls[0]["cwd"])) == str(tmp_path)
    assert (tmp_path / "server.jar").read_bytes() == b"vanilla 1.21.1"
    assert (tmp_path / "libraries" / "org" / "quiltmc" / "quilt-loader.jar").read_bytes() == b"loader"
    assert (tmp_path / "world" / "level.dat").read_bytes() == b"save"
    assert not (tmp_path / "installer.jar.log").exists(), "설치기 로그는 서버 폴더로 옮기지 않는다"
    assert _no_build_dirs(tmp_path), "설치기 폴더는 지운다"
    assert (tmp_path / "eula.txt").read_text(encoding="utf-8").strip().endswith("eula=true")


def test_buildtools_builds_spigot_with_a_jdk_inside_the_server_folder(tmp_path):
    (tmp_path / "server.jar").write_bytes(b"old spigot")
    (tmp_path / "paper-1.21.1.jar").write_bytes(_jar_bytes({
        "META-INF/MANIFEST.MF": "Main-Class: io.papermc.paperclip.Main\n",
        "META-INF/versions.list": "abc\t1.21.1\t1.21.1/paper-1.21.1.jar\n"}))
    built = _jar_bytes({"META-INF/MANIFEST.MF": "Main-Class: org.bukkit.craftbukkit.bootstrap.Main\n",
                        "META-INF/versions.list": "d955 *spigot-1.21.1-R0.1-SNAPSHOT.jar\n"})

    def run(cmd, cwd, env):
        out = cmd[cmd.index("--output-dir") + 1]
        assert os.path.dirname(out) == cwd
        open(os.path.join(cwd, "BuildData.txt"), "w").write("work")
        open(os.path.join(out, cmd[cmd.index("--final-name") + 1]), "wb").write(built)
        return 0, []

    result, calls, jre, _ = _installer_fixture(tmp_path, "spigot", {"build": "4344", "java_min": 21, "java_max": 23,
                                                                     "jdk": True}, run)
    assert result["success"], result
    # 서버 폴더에 남은 다른 종류의 jar(paper)는 판정에 끼지 않는다
    assert (tmp_path / "server.jar").read_bytes() == built
    assert result["executable_path"] == str(tmp_path / "server.jar")
    # BuildTools는 컴파일하므로 설치기 폴더에 받은 JDK로 돈다. 서버는 서버 폴더의 JRE로 뜬다
    build = calls[0]["cwd"]
    assert calls[0]["cmd"][0] == os.path.join(build, "java", "bin", "jdk")
    assert result["java_path"] == os.path.join(str(tmp_path), "java", "bin", "jre")
    # 메이븐 저장소·임시 파일도 설치기 폴더 안에 둔다 (사용자 홈의 ~/.m2를 채우지 않는다)
    assert calls[0]["env"]["MAVEN_OPTS"] == f"-Dmaven.repo.local={os.path.join(build, 'm2')}"
    assert f"-Djava.io.tmpdir={os.path.join(build, 'tmp')}" in calls[0]["cmd"]
    assert not (tmp_path / "BuildData.txt").exists() and _no_build_dirs(tmp_path)


def test_a_failed_installer_reports_why_and_keeps_the_old_server(tmp_path):
    (tmp_path / "server.jar").write_bytes(b"old server")
    tail = ["[INFO] BUILD FAILURE", "[ERROR] No compiler is provided in this environment.", "[INFO] done"]

    def half_done(cmd, cwd, env):
        target = next((a.split("=", 1)[1] for a in cmd if a.startswith("--install-dir=")), None)
        open(os.path.join(target, "server.jar"), "wb").write(b"half")
        return 1, tail

    result, _, _, _ = _installer_fixture(tmp_path, "quilt", {"build": "0.30.1"}, half_done)
    assert result["success"] is False and "quilt" in result["message"]
    assert "No compiler is provided" in result["message"]
    assert (tmp_path / "server.jar").read_bytes() == b"old server", "실패한 설치는 서버 폴더를 건드리지 않는다"
    assert _no_build_dirs(tmp_path)

    # 설치기가 성공이라 해도 고른 종류의 서버가 생기지 않았으면 실패다
    result, _, _, _ = _installer_fixture(tmp_path, "quilt", {"build": "0.30.1"}, lambda *a: (0, []))
    assert result["success"] is False and "quilt" in result["message"]
    assert (tmp_path / "server.jar").read_bytes() == b"old server"
    assert _no_build_dirs(tmp_path)


def test_an_installer_needs_its_portable_java(tmp_path):
    from minecraft import lifecycle
    with mock.patch.object(lifecycle.JavaDownloader, "download_jre", return_value={"success": False, "message": "offline"}), \
            mock.patch.object(lifecycle.ServerInstaller, "_run_installer_process") as run, \
            mock.patch.object(lifecycle.ServerInstaller, "get_version_details",
                              return_value={"has_server": True, "download_url": "x", "sha1": None, "java_major_version": 21}), \
            mock.patch.object(lifecycle.server_types, "resolve_download",
                              return_value={"url": "https://example.invalid/i.jar", "installer": True, "build": "1",
                                            "sha256": "ab" * 32}), \
            mock.patch.object(lifecycle, "download", return_value={"sha1": "x"}):
        result = lifecycle.ServerInstaller.install_server("1.21.1", str(tmp_path), server_type="forge")
    assert result["success"] is False and "offline" in result["message"]
    run.assert_not_called()
    assert _no_build_dirs(tmp_path)


def test_an_installer_folder_in_use_is_not_removed_but_a_stale_one_is(tmp_path):
    import time
    from minecraft.lifecycle import ServerInstaller
    fresh = tmp_path / ".saba-install-fresh"
    stale = tmp_path / ".saba-install-stale"
    for d in (fresh, stale):
        (d / "Spigot" / ".git").mkdir(parents=True)
        pack = d / "Spigot" / ".git" / "pack.idx"
        pack.write_bytes(b"x")
        os.chmod(pack, 0o444)  # git 팩 파일은 읽기 전용이다
    old = time.time() - 2 * 3600
    os.utime(stale, (old, old))
    build = ServerInstaller._make_build_dir(str(tmp_path))
    assert fresh.exists() and not stale.exists() and os.path.isdir(build)


def test_bundled_java_newer_than_an_installer_allows_is_replaced(tmp_path):
    """서버 폴더의 java/가 고정 판보다 새것이면 그대로 쓰지 않는다 (옛 Forge·BuildTools는 너무 새 Java를 거절한다)"""
    from minecraft.lifecycle import JavaDownloader
    with mock.patch.object(JavaDownloader, "_find_bundled_java", return_value="/srv/java/bin/java"), \
            mock.patch.object(lifecycle_module().JavaDetector, "get_java_info",
                              return_value={"major_version": 21, "version": "21"}), \
            mock.patch.object(JavaDownloader, "_release_package", side_effect=RuntimeError("download")) as fetch:
        assert JavaDownloader.download_jre(8, str(tmp_path))["already_exists"]
        result = JavaDownloader.download_jre(8, str(tmp_path), max_major=8)
    assert result["success"] is False and fetch.called


def lifecycle_module():
    from minecraft import lifecycle
    return lifecycle
