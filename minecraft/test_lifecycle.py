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
    "accept_eula", "diagnose_log", "list_versions",
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
    """_PROPERTY_KEY_MAP values should reference valid server.properties keys."""
    from minecraft.lifecycle import _PROPERTY_KEY_MAP
    for friendly_name, prop_key in _PROPERTY_KEY_MAP.items():
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
        assert result.get("action_required") == "server_jar_not_found"


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
        assert result.get("action_required") == "server_jar_not_found"


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
    from minecraft.lifecycle import _PROPERTY_KEY_MAP
    assert not {"enable_rcon", "rcon_password", "rcon_port", "broadcast_rcon_to_ops"} & set(_PROPERTY_KEY_MAP)


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


def test_stop_graceful_then_reports_success():
    from minecraft import lifecycle
    with mock.patch.object(lifecycle, "_send_console_stop", return_value=True) as console, \
         mock.patch.object(lifecycle, "wait_for_exit", return_value=True), \
         mock.patch.object(lifecycle, "force_kill") as kill:
        result = lifecycle.stop({"pid": 99, "instance_id": "abc"})
    assert result["success"] is True
    console.assert_called_once_with("abc")
    kill.assert_not_called()


def test_normal_stop_without_console_terminates_before_any_kill():
    """RCON is off and there is no console: a normal stop asks the server to exit (SIGTERM) and waits,
    so it can save; it only force-kills after the wait runs out"""
    from minecraft import lifecycle
    calls = []
    with mock.patch.object(lifecycle, "_send_console_stop", return_value=False), \
         mock.patch.object(lifecycle, "_request_terminate", side_effect=lambda pid: calls.append("term") or True), \
         mock.patch.object(lifecycle, "wait_for_exit", side_effect=lambda pid, timeout: calls.append("wait") or True), \
         mock.patch.object(lifecycle, "force_kill", side_effect=lambda pid: calls.append("kill") or (True, None)):
        result = lifecycle.stop({"pid": 99})
    assert result["success"] is True
    assert calls == ["term", "wait"]

    calls.clear()
    with mock.patch.object(lifecycle, "_send_console_stop", return_value=False), \
         mock.patch.object(lifecycle, "_request_terminate", side_effect=lambda pid: calls.append("term") or True), \
         mock.patch.object(lifecycle, "wait_for_exit", side_effect=lambda pid, timeout: calls.append("wait") or False), \
         mock.patch.object(lifecycle, "force_kill", side_effect=lambda pid: calls.append("kill") or (True, None)):
        lifecycle.stop({"pid": 99})
    assert calls == ["term", "wait", "kill"]


@pytest.mark.skipif(os.name == "nt", reason="SIGTERM is POSIX only")
def test_request_terminate_sends_sigterm_not_sigkill():
    import signal
    from minecraft import lifecycle
    with mock.patch.object(lifecycle.os, "kill") as kill:
        assert lifecycle._request_terminate(4321) is True
    kill.assert_called_once_with(4321, signal.SIGTERM)


def test_force_stop_kills_without_asking():
    from minecraft import lifecycle
    with mock.patch.object(lifecycle, "_send_console_stop") as console, \
         mock.patch.object(lifecycle, "_request_terminate") as term, \
         mock.patch.object(lifecycle, "force_kill", return_value=(True, None)) as kill:
        lifecycle.stop({"pid": 99, "force": True})
    console.assert_not_called()
    term.assert_not_called()
    kill.assert_called_once_with(99)


def test_ping_closes_socket_on_error():
    from minecraft import lifecycle
    conn = mock.MagicMock()
    conn.__enter__.return_value.sendall.side_effect = OSError("reset")
    with mock.patch.object(lifecycle.socket, "create_connection", return_value=conn):
        assert lifecycle.MinecraftPing.ping("127.0.0.1", 25565) is None
    conn.__exit__.assert_called_once()
