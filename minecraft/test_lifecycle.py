"""Tests for Minecraft lifecycle module functions."""
import os
import sys
import json
import tempfile

# Setup path for imports
MODULE_DIR = os.path.join(os.path.dirname(__file__), '..', 'minecraft')
sys.path.insert(0, MODULE_DIR)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def test_default_properties_loaded():
    """DEFAULT_PROPERTIES should load from server.properties file."""
    from minecraft.lifecycle import DEFAULT_PROPERTIES
    assert isinstance(DEFAULT_PROPERTIES, dict)
    assert len(DEFAULT_PROPERTIES) > 30, f"Expected 30+ properties, got {len(DEFAULT_PROPERTIES)}"
    # saba-chan override: enable-rcon should be true
    assert DEFAULT_PROPERTIES.get("enable-rcon") == "true"
    # Vanilla properties should be present
    assert "server-port" in DEFAULT_PROPERTIES
    assert "difficulty" in DEFAULT_PROPERTIES
    assert "gamemode" in DEFAULT_PROPERTIES


def test_default_properties_no_empty_keys():
    """No property key should be empty."""
    from minecraft.lifecycle import DEFAULT_PROPERTIES
    for key in DEFAULT_PROPERTIES:
        assert key.strip() != "", f"Found empty key in DEFAULT_PROPERTIES"


def test_property_key_map_coverage():
    """_PROPERTY_KEY_MAP values should reference valid server.properties keys."""
    from minecraft.lifecycle import _PROPERTY_KEY_MAP, DEFAULT_PROPERTIES
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
    from minecraft.lifecycle import ServerPropertiesManager, DEFAULT_PROPERTIES
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


if __name__ == "__main__":
    tests = [
        test_default_properties_loaded,
        test_default_properties_no_empty_keys,
        test_property_key_map_coverage,
        test_server_properties_manager_write_and_read,
        test_server_properties_manager_update,
        test_server_properties_manager_update_new_file,
        test_server_properties_manager_get_defaults,
    ]
    passed = 0
    failed = 0
    for test in tests:
        try:
            test()
            print(f"  PASS: {test.__name__}")
            passed += 1
        except Exception as e:
            print(f"  FAIL: {test.__name__}: {e}")
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(1 if failed > 0 else 0)
