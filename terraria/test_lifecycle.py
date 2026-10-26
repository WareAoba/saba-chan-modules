"""테라리아 모듈 계약 테스트.

실제 서버 없이 함수에 config를 넣고 답과 파일 변화를 본다.
- module.toml ↔ lifecycle 키 표가 어긋나지 않는다
- serverconfig.txt 왕복(특수 문자 포함), 다른 키·주석 보존, 못 읽으면 쓰지 않음
- 정지: managed는 요청 뒤 반환, 요청할 길이 없으면 죽이지 않음
- 백업: 이 월드·설정·밴 목록만, 게임이 만든 .bak와 실행 파일은 빼고
- 초기화: 이 월드 파일만 지우고 이름 검증
- 설치: 공식 zip에서 서버 폴더만 풀고 기존 설정을 덮지 않음
"""
import importlib.util
import io
import os
import re
import zipfile
from pathlib import Path

import pytest

try:
    import tomllib
except ImportError:  # pragma: no cover
    import tomli as tomllib  # type: ignore

MODULE_DIR = Path(__file__).resolve().parent


@pytest.fixture
def lc():
    spec = importlib.util.spec_from_file_location("terraria_lifecycle", MODULE_DIR / "lifecycle.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    # XNA 설치·점검은 Windows 전용이다. 그 동작을 보는 테스트만 켠다
    module._is_windows = lambda: False
    return module


@pytest.fixture
def toml():
    with open(MODULE_DIR / "module.toml", "rb") as f:
        return tomllib.load(f)


def _server(tmp_path, lc, exe=True):
    server = tmp_path / "server"
    server.mkdir()
    if exe:
        (server / lc._server_exe_name()).write_bytes(b"exe")
    return server


def _config(server, **extra):
    config = {
        "instance_id": "inst-1",
        "working_dir": str(server),
        "server_executable": str(server / "TerrariaServer.exe"),
        "port": 7777,
        "world_name": "Island",
        "world_dir": str(server / "Worlds"),
        "autocreate": "2",
    }
    config.update(extra)
    return config


def _read(path):
    return Path(path).read_text(encoding="utf-8")


# ─── module.toml ↔ lifecycle ────────────────────────────────

def test_every_game_setting_maps_to_a_config_key(lc, toml):
    game = [f["name"] for f in toml["settings"]["fields"] if f.get("group") != "saba-chan"]
    assert sorted(game) == sorted(lc._KEY_MAP), "설정 이름과 serverconfig 키 표가 어긋났다"


def test_launch_only_settings_are_not_written_to_the_game_file(lc, toml):
    launch_only = [f["name"] for f in toml["settings"]["fields"] if f.get("group") == "saba-chan"]
    assert launch_only
    assert not set(launch_only) & set(lc._KEY_MAP)


def test_every_command_declares_risk_and_description(toml):
    for cmd in toml["commands"]["fields"]:
        assert cmd.get("risk") in ("safe", "caution", "danger"), cmd["name"]
        assert cmd.get("description"), cmd["name"]


def test_measured_commands_and_settings_are_declared(toml):
    commands = {item["name"]: item for item in toml["commands"]["fields"]}
    assert not {"death", "pvpdeath", "alldeath", "allpvpdeath"} & commands.keys()
    assert commands["seed"]["risk"] == "safe"
    assert commands["seed"]["console_template"] == "seed"
    settings = {item["name"]: item for item in toml["settings"]["fields"]}
    for name in ("seed_celebration", "seed_theconstant", "seed_notthebees", "seed_notraps",
                 "seed_fortheworthy", "seed_remix", "seed_drunk", "seed_zenith"):
        assert settings[name]["type"] == "boolean" and settings[name]["install"] is True
    assert settings["worldrollbackstokeep"]["type"] == "number"
    assert settings["worldrollbackstokeep"]["min"] == 0
    assert settings["priority"]["default"] == "1"
    assert {"ja-JP", "ko-KR", "zh-Hant"} <= set(settings["language"]["options"])


def test_measured_boolean_settings_roundtrip(lc):
    names = ("seed_celebration", "seed_theconstant", "seed_notthebees", "seed_notraps",
             "seed_fortheworthy", "seed_remix", "seed_drunk", "seed_zenith", "slowliquids")
    values = {name: True for name in names}
    assert lc._file_updates(values) == values
    assert lc._settings_from_file({name: "1" for name in names}) == values
    assert lc._settings_from_file({"worldrollbackstokeep": "0"})["worldrollbackstokeep"] == 0


def test_setting_types_are_supported(toml):
    for field in toml["settings"]["fields"]:
        assert field.get("type", "text") in ("text", "number", "password", "boolean", "select", "file"), field["name"]


def test_functions_named_by_toml_exist(lc, toml):
    if toml.get("backup", {}).get("hook"):
        assert "backup_paths" in lc.FUNCTIONS
    assert "functions" in lc.FUNCTIONS


# ─── serverconfig.txt 문법 ──────────────────────────────────

def test_configure_roundtrip_keeps_special_characters(lc, tmp_path):
    server = _server(tmp_path, lc)
    settings = {
        "motd": '어서 오세요: a=b \\ "quoted" #not-comment',
        "password": "p@ss=word:1\\x",
        "max_players": 16,
        "secure": True,
        "upnp": False,
        "difficulty": "2",
        "language": "de-DE",
        "npcstream": 30,
        "priority": "3",
        "journeypermission_godmode": "0",
        "seed": "for the worthy",
    }
    result = lc.configure(_config(server, settings=settings))
    assert result["success"], result

    imported = lc.import_settings({"working_dir": str(server)})
    assert imported["success"], imported
    for key, value in settings.items():
        assert imported["settings"][key] == value, key


def test_configure_writes_official_spelling(lc, tmp_path):
    server = _server(tmp_path, lc)
    lc.configure(_config(server, settings={"secure": True, "upnp": False, "max_players": 8}))
    text = _read(server / "serverconfig.txt")
    assert "secure=1" in text.splitlines()
    assert "upnp=0" in text.splitlines()
    assert "maxplayers=8" in text.splitlines()


def test_configure_keeps_comments_order_and_unknown_keys(lc, tmp_path):
    server = _server(tmp_path, lc)
    original = "#example config\r\n#maxplayers=8\r\nmotd=old\r\ncustomkey=keep\r\npriority=1\r\n"
    (server / "serverconfig.txt").write_bytes(original.encode("utf-8"))

    assert lc.configure(_config(server, settings={"motd": "new"}))["success"]

    raw = (server / "serverconfig.txt").read_bytes().decode("utf-8")
    assert raw == original.replace("motd=old", "motd=new")


def test_configure_clearing_a_value_removes_the_key(lc, tmp_path):
    server = _server(tmp_path, lc)
    (server / "serverconfig.txt").write_text("password=secret\nmotd=hi\n", encoding="utf-8")
    assert lc.configure(_config(server, settings={"password": ""}))["success"]
    assert _read(server / "serverconfig.txt") == "motd=hi\n"


def test_configure_does_not_write_when_the_file_cannot_be_read(lc, tmp_path):
    server = _server(tmp_path, lc)
    # UTF-8로도 cp1252로도 풀리지 않는 바이트 (0x81은 cp1252에 없다)
    broken = b"motd=\x81\x8d broken\nmaxplayers=4\n"
    (server / "serverconfig.txt").write_bytes(broken)
    result = lc.configure(_config(server, settings={"motd": "new"}))
    assert result["success"] is False
    assert (server / "serverconfig.txt").read_bytes() == broken


def test_cp1252_configure_and_import_preserve_bytes_comments_and_crlf(lc, tmp_path):
    server = _server(tmp_path, lc)
    path = server / "serverconfig.txt"
    original = b"#motd=Please don\x92t cut the purple trees!\r\nport=7777\r\n"
    path.write_bytes(original)

    result = lc.configure(_config(server, settings={"port": 7788}))

    assert result["success"]
    assert path.read_bytes() == original.replace(b"port=7777", b"port=7788")
    assert lc.import_settings({"working_dir": str(server)})["settings"]["port"] == 7788


def test_configure_rejects_line_breaks(lc, tmp_path):
    server = _server(tmp_path, lc)
    result = lc.configure(_config(server, settings={"motd": "a\nworld=C:/other.wld"}))
    assert result["success"] is False
    assert not (server / "serverconfig.txt").exists()


def test_import_reads_world_location_from_the_game_file(lc, tmp_path):
    server = _server(tmp_path, lc)
    worlds = tmp_path / "MyWorlds"
    (server / "serverconfig.txt").write_text(f"world={worlds / 'Old Land.wld'}\nport=7778\n", encoding="utf-8")
    result = lc.import_settings({"working_dir": str(server)})
    assert result["success"]
    assert result["settings"]["world_name"] == "Old Land"
    assert result["settings"]["world_dir"] == str(worlds)
    assert result["settings"]["port"] == 7778
    assert result["executable_path"] == lc._server_exe_name()


def test_import_without_config_file_still_succeeds(lc, tmp_path):
    server = _server(tmp_path, lc)
    result = lc.import_settings({"working_dir": str(server)})
    assert result["success"]
    assert result["settings"] == {}


# ─── 실행 ────────────────────────────────────────────────

def test_launch_without_executable_asks_for_action(lc, tmp_path):
    server = _server(tmp_path, lc, exe=False)
    result = lc.get_launch_command(_config(server))
    assert result["success"] is False
    assert result["action_required"] == "executable_not_found"


def test_launch_uses_config_file_and_pins_world_to_instance(lc, tmp_path):
    server = _server(tmp_path, lc)
    (server / "serverconfig.txt").write_text("motd=keep me\n", encoding="utf-8")
    config = _config(server, server_executable=str(server / lc._server_exe_name()), port=7790)
    result = lc.get_launch_command(config)
    assert result["success"], result
    assert result["program"] == str(server / lc._server_exe_name())
    assert result["args"] == ["-config", "serverconfig.txt"]
    assert result["working_dir"] == str(server)

    props = lc._read_config_file(str(server / "serverconfig.txt"))
    assert props["world"] == str(server / "Worlds" / "Island.wld")
    assert props["worldpath"] == str(server / "Worlds")
    assert props["worldname"] == "Island"
    assert props["port"] == "7790"
    assert props["autocreate"] == "2"
    assert props["motd"] == "keep me"
    assert (server / "Worlds").is_dir()


def test_launch_preview_writes_nothing(lc, tmp_path):
    server = _server(tmp_path, lc)
    result = lc.get_launch_command(_config(server, preview=True))
    assert result["success"]
    assert not (server / "serverconfig.txt").exists()
    assert not (server / "Worlds").exists()


def test_launch_adds_launch_only_arguments(lc, tmp_path):
    server = _server(tmp_path, lc)
    result = lc.get_launch_command(_config(server, preview=True, ip="127.0.0.1", steam=True,
                                           lobby="friends", disable_announcement_box=True,
                                           announcement_box_range=-1))
    assert result["args"] == ["-config", "serverconfig.txt", "-ip", "127.0.0.1", "-steam",
                              "-lobby", "friends", "-disableannouncementbox", "-announcementboxrange", "-1"]


def test_lobby_needs_steam(lc, tmp_path):
    server = _server(tmp_path, lc)
    result = lc.get_launch_command(_config(server, preview=True, lobby="private"))
    assert "-lobby" not in result["args"]


@pytest.mark.parametrize("name", ["..", ".", "a/b", "a\\b", "C:", "trail.", "trail "])
def test_bad_world_names_are_refused(lc, tmp_path, name):
    server = _server(tmp_path, lc)
    result = lc.get_launch_command(_config(server, world_name=name))
    assert result["success"] is False
    assert not (server / "serverconfig.txt").exists()


def test_empty_world_name_falls_back_to_default(lc, tmp_path):
    server = _server(tmp_path, lc)
    world_dir, name, world_file = lc._world(_config(server, world_name=""))
    assert name == lc.DEFAULT_WORLD_NAME
    assert world_file == str(server / "Worlds" / f"{lc.DEFAULT_WORLD_NAME}.wld")


def test_validate_flags_missing_executable(lc, tmp_path):
    server = _server(tmp_path, lc, exe=False)
    result = lc.validate(_config(server))
    assert result["success"] is False
    assert any(i["code"] == "executable_not_found" and i["severity"] == "critical" for i in result["issues"])


def test_validate_flags_bad_world_name(lc, tmp_path):
    server = _server(tmp_path, lc)
    result = lc.validate(_config(server, server_executable=str(server / lc._server_exe_name()), world_name=".."))
    assert result["success"] is False
    assert any(i["code"] == "invalid_world_name" for i in result["issues"])


# ─── 정지 ────────────────────────────────────────────────

def _no_kill(*args, **kwargs):
    pytest.fail("정상 종료 요청 길이 있거나 managed면 죽이지 않는다")


def test_managed_stop_requests_exit_and_returns(lc, monkeypatch):
    sent = []
    monkeypatch.setattr(lc, "daemon_stdin_command", lambda iid, line, **kw: sent.append((iid, line)))
    monkeypatch.setattr(lc, "wait_for_exit", _no_kill)
    monkeypatch.setattr(lc, "force_kill", _no_kill)
    result = lc.stop({"managed": True, "instance_id": "inst-1", "pid": 42})
    assert result["success"] is True
    assert sent == [("inst-1", "exit")]


def test_stop_without_console_does_not_kill(lc, monkeypatch):
    def unavailable(*args, **kwargs):
        raise ConnectionError("409 no_command_path")
    monkeypatch.setattr(lc, "daemon_stdin_command", unavailable)
    monkeypatch.setattr(lc, "wait_for_exit", _no_kill)
    monkeypatch.setattr(lc, "force_kill", _no_kill)
    for managed in (True, False):
        result = lc.stop({"managed": managed, "instance_id": "inst-1", "pid": 42})
        assert result["success"] is False
        assert result["error_code"] == "graceful_stop_unavailable"


def test_nonmanaged_stop_waits_then_kills(lc, monkeypatch):
    waited, killed = [], []
    monkeypatch.setattr(lc, "daemon_stdin_command", lambda *a, **k: None)
    monkeypatch.setattr(lc, "wait_for_exit", lambda pid, timeout: waited.append(timeout) or False)
    monkeypatch.setattr(lc, "force_kill", lambda pid: killed.append(pid) or (True, None))
    result = lc.stop({"managed": False, "instance_id": "inst-1", "pid": 42,
                      "stop_timeout": 50, "stop_call_limit": 80})
    assert waited == [50]
    assert killed == [42]
    assert result["success"] is True


def test_force_stop_kills(lc, monkeypatch):
    killed = []
    monkeypatch.setattr(lc, "daemon_stdin_command", _no_kill)
    monkeypatch.setattr(lc, "force_kill", lambda pid: killed.append(pid) or (True, None))
    assert lc.stop({"managed": False, "pid": 42, "force": True})["success"]
    assert killed == [42]


# ─── 백업 ────────────────────────────────────────────────

def _populated_server(tmp_path, lc):
    server = _server(tmp_path, lc)
    worlds = server / "Worlds"
    worlds.mkdir()
    for name in ("Island.wld", "Island.wld.bak", "Other.wld"):
        (worlds / name).write_bytes(b"w")
    (server / "serverconfig.txt").write_text("motd=x\n", encoding="utf-8")
    (server / "banlist.txt").write_text("", encoding="utf-8")
    (server / "notes.txt").write_text("user file", encoding="utf-8")
    (server / "instance.json").write_text("{}", encoding="utf-8")
    return server


def test_backup_paths_are_this_world_config_and_banlist(lc, tmp_path):
    server = _populated_server(tmp_path, lc)
    result = lc.backup_paths(_config(server))
    assert result["success"], result
    assert sorted(result["paths"]) == sorted([
        str(server / "Worlds" / "Island.wld"),
        str(server / "serverconfig.txt"),
        str(server / "banlist.txt"),
    ])


def test_backup_follows_banlist_setting(lc, tmp_path):
    server = _populated_server(tmp_path, lc)
    result = lc.backup_paths(_config(server, banlist="bans.txt"))
    assert str(server / "bans.txt") in result["paths"]


def test_backup_refuses_bad_banlist(lc, tmp_path):
    server = _populated_server(tmp_path, lc)
    result = lc.backup_paths(_config(server, banlist="../outside.txt"))
    assert result["success"] is False


# ─── 초기화 ──────────────────────────────────────────────

def test_reset_deletes_only_this_world(lc, tmp_path):
    server = _populated_server(tmp_path, lc)
    worlds = server / "Worlds"
    (worlds / "Island.wld.bak2").write_bytes(b"backup")
    (worlds / "Island.wld.bak12").write_bytes(b"backup")
    (worlds / "Other.wld.bak2").write_bytes(b"other backup")
    result = lc.reset_server(_config(server))
    assert result["success"], result
    assert not (worlds / "Island.wld").exists()
    assert not (worlds / "Island.wld.bak").exists()
    assert not (worlds / "Island.wld.bak2").exists()
    assert not (worlds / "Island.wld.bak12").exists()
    assert (worlds / "Other.wld.bak2").exists()
    for kept in (worlds / "Other.wld", server / "serverconfig.txt", server / "banlist.txt",
                 server / "notes.txt", server / "instance.json", server / lc._server_exe_name()):
        assert kept.exists(), kept


@pytest.mark.parametrize("name", ["..", ".", "a/b", "C:", "x."])
def test_reset_refuses_bad_names(lc, tmp_path, name):
    server = _populated_server(tmp_path, lc)
    result = lc.reset_server(_config(server, world_name=name))
    assert result["success"] is False
    assert (server / "Worlds" / "Island.wld").exists()


def test_reset_refuses_folder_without_server(lc, tmp_path):
    folder = tmp_path / "Desktop"
    (folder / "Worlds").mkdir(parents=True)
    (folder / "Worlds" / "Island.wld").write_bytes(b"w")
    result = lc.reset_server(_config(folder))
    assert result["success"] is False
    assert (folder / "Worlds" / "Island.wld").exists()


# ─── 버전·설치 ───────────────────────────────────────────

def test_list_versions_reads_official_names(lc, monkeypatch):
    monkeypatch.setattr(lc, "_fetch_server_names",
                        lambda: ["terraria-server-1458.zip", "terraria-server-1458.zip", "terraria-server-1449.zip"])
    result = lc.list_versions({})
    assert result["success"]
    assert [v["id"] for v in result["versions"]] == ["1.4.5.8", "1.4.4.9"]
    assert result["latest"]["release"] == "1.4.5.8"


def test_list_versions_reports_network_failure(lc, monkeypatch):
    def fail():
        raise ConnectionError("offline")
    monkeypatch.setattr(lc, "_fetch_server_names", fail)
    assert lc.list_versions({})["success"] is False


def _zip_bytes(entries):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, data in entries.items():
            z.writestr(name, data)
    return buf.getvalue()


def _fake_download(monkeypatch, lc, data, seen=None):
    def download(url, dest):
        if seen is not None:
            seen.append(url)
        Path(dest).write_bytes(data)
    monkeypatch.setattr(lc, "_download", download)


def test_install_extracts_only_this_platform(lc, tmp_path, monkeypatch):
    exe = lc._server_exe_name()
    folder = "Windows" if exe.endswith(".exe") else "Linux"
    other = "Linux/TerrariaServer.bin.x86_64" if folder == "Windows" else "Windows/TerrariaServer.exe"
    data = _zip_bytes({
        f"1458/{folder}/{exe}": b"server",
        f"1458/{folder}/serverconfig.txt": b"#motd=Please don\x92t cut the purple trees!\r\npriority=1\r\n",
        f"1458/{folder}/lib/a.dll": b"lib",
        f"1458/{other}": b"other",
    })
    seen = []
    _fake_download(monkeypatch, lc, data, seen)
    install = tmp_path / "inst"
    result = lc.install_server({"version": "1.4.5.8", "install_dir": str(install),
                                "settings": {"max_players": 12, "port": 7781}})
    assert result["success"], result
    assert seen == ["https://terraria.org/api/download/pc-dedicated-server/terraria-server-1458.zip"]
    assert (install / exe).read_bytes() == b"server"
    assert (install / "lib" / "a.dll").exists()
    assert not (install / "Linux").exists() and not (install / "Windows").exists()
    assert result["executable_path"] == str(install / exe)
    props = lc._read_config_file(str(install / "serverconfig.txt"))
    assert props["maxplayers"] == "12"
    assert props["port"] == "7781"
    assert (install / "serverconfig.txt").read_bytes().startswith(
        b"#motd=Please don\x92t cut the purple trees!\r\npriority=1\r\n")
    assert not [p for p in install.iterdir() if p.name.endswith(".download")]


def test_install_keeps_existing_config(lc, tmp_path, monkeypatch):
    exe = lc._server_exe_name()
    folder = "Windows" if exe.endswith(".exe") else "Linux"
    _fake_download(monkeypatch, lc, _zip_bytes({
        f"1458/{folder}/{exe}": b"new",
        f"1458/{folder}/serverconfig.txt": b"#sample\n",
    }))
    install = tmp_path / "inst"
    install.mkdir()
    (install / "serverconfig.txt").write_text("motd=mine\nworld=keep.wld\n", encoding="utf-8")
    (install / "Worlds").mkdir()
    (install / "Worlds" / "Island.wld").write_bytes(b"save")
    assert lc.install_server({"version": "1.4.5.8", "install_dir": str(install)})["success"]
    assert _read(install / "serverconfig.txt") == "motd=mine\nworld=keep.wld\n"
    assert (install / "Worlds" / "Island.wld").read_bytes() == b"save"


def test_install_picks_platform_named_folder_only_and_refuses_ambiguous(lc, tmp_path, monkeypatch):
    entries = {
        "1458/Linux/TerrariaServer.bin.x86_64": b"linux",
        "1458/Linux/mscorlib.dll": b"mono",
        "1458/Windows/TerrariaServer.exe": b"windows",
        "1458/Windows/serverconfig.txt": b"priority=1\r\n",
        "1458/Mac/Terraria Server.app/Contents/Resources/TerrariaServer.exe": b"mac",
    }
    archive = io.BytesIO(_zip_bytes(entries))
    for os_name, exe, expected in (("nt", "TerrariaServer.exe", b"windows"),
                                   ("posix", "TerrariaServer.bin.x86_64", b"linux")):
        install = tmp_path / os_name
        monkeypatch.setattr(lc, "_server_exe_name", lambda exe=exe: exe)
        lc._extract_server(archive, str(install))
        assert (install / exe).read_bytes() == expected
        assert sorted(p.name for p in install.iterdir()) == sorted(
            ["TerrariaServer.exe", "serverconfig.txt"] if os_name == "nt"
            else ["TerrariaServer.bin.x86_64", "mscorlib.dll"])
    ambiguous = _zip_bytes({**entries, "1458/Other/Linux/TerrariaServer.bin.x86_64": b"other"})
    with pytest.raises(ValueError):
        lc._extract_server(io.BytesIO(ambiguous), str(tmp_path / "ambiguous"))


@pytest.fixture
def xna_ext(monkeypatch):
    """XNA 익스텐션(extensions.xna)을 흉내 낸다. present 값과 install 호출을 테스트가 정한다"""
    import sys
    import types

    class XnaError(Exception):
        def __init__(self, code, message):
            super().__init__(message)
            self.code = code

    state = {"present": False, "installed": [], "fail": None}

    def install(target):
        if state["fail"]:
            raise XnaError("checksum_mismatch", state["fail"])
        state["installed"].append(str(target))
        state["present"] = True
        return []

    ext = types.ModuleType("extensions.xna")
    ext.XnaError = XnaError
    ext.present = lambda target: state["present"]
    ext.install = install
    pkg = types.ModuleType("extensions")
    pkg.__path__ = []
    pkg.xna = ext
    monkeypatch.setitem(sys.modules, "extensions", pkg)
    monkeypatch.setitem(sys.modules, "extensions.xna", ext)
    return state


def _no_xna_ext(monkeypatch):
    """익스텐션이 없거나 꺼져 있을 때: 데몬의 import 가드처럼 import가 실패한다"""
    import sys
    monkeypatch.setitem(sys.modules, "extensions.xna", None)


def _windows_server_zip(monkeypatch, lc):
    _fake_download(monkeypatch, lc, _zip_bytes({"1458/Windows/TerrariaServer.exe": b"server"}))
    monkeypatch.setattr(lc, "_server_exe_name", lambda: "TerrariaServer.exe")
    monkeypatch.setattr(lc, "_is_windows", lambda: True)


def test_module_declares_the_xna_extension(toml):
    assert toml["install"]["requires_extensions"] == ["xna"]
    assert toml["module"]["dependencies"]["ext-xna"] == ">=0.1.0"


def test_validate_reports_missing_xna(lc, tmp_path, monkeypatch, xna_ext):
    server = _server(tmp_path, lc)
    monkeypatch.setattr(lc, "_is_windows", lambda: True)
    issue = next(i for i in lc.validate(_config(server))["issues"] if i["code"] == "xna_framework_missing")
    assert issue["severity"] == "critical"
    xna_ext["present"] = True
    assert "xna_framework_missing" not in [i["code"] for i in lc.validate(_config(server))["issues"]]


def test_validate_reports_a_missing_xna_extension(lc, tmp_path, monkeypatch):
    _no_xna_ext(monkeypatch)
    monkeypatch.setattr(lc, "_is_windows", lambda: True)
    issue = next(i for i in lc.validate(_config(_server(tmp_path, lc)))["issues"]
                 if i["code"] == "extension_required")
    assert issue["severity"] == "critical"


def test_install_puts_xna_next_to_the_server_on_windows(lc, tmp_path, monkeypatch, xna_ext):
    _windows_server_zip(monkeypatch, lc)
    install = tmp_path / "inst"
    assert lc.install_server({"version": "1.4.5.8", "install_dir": str(install)})["success"] is True
    assert xna_ext["installed"] == [str(install)]


def test_install_without_the_xna_extension_asks_for_it(lc, tmp_path, monkeypatch):
    _no_xna_ext(monkeypatch)
    _windows_server_zip(monkeypatch, lc)
    result = lc.install_server({"version": "1.4.5.8", "install_dir": str(tmp_path / "inst")})
    assert result["success"] is False
    assert result["error_code"] == "extension_required"
    assert result["missing_extensions"] == ["xna"]


def test_install_reports_an_xna_failure(lc, tmp_path, monkeypatch, xna_ext):
    _windows_server_zip(monkeypatch, lc)
    xna_ext["fail"] = "not the official file"
    result = lc.install_server({"version": "1.4.5.8", "install_dir": str(tmp_path / "inst")})
    assert result["success"] is False
    assert result["error_code"] == "xna_install_failed"
    assert "not the official file" in result["message"]


def test_install_does_not_need_xna_off_windows(lc, tmp_path, monkeypatch):
    _no_xna_ext(monkeypatch)
    _fake_download(monkeypatch, lc, _zip_bytes({"1458/Linux/TerrariaServer.bin.x86_64": b"server"}))
    monkeypatch.setattr(lc, "_server_exe_name", lambda: "TerrariaServer.bin.x86_64")
    assert lc.install_server({"version": "1.4.5.8", "install_dir": str(tmp_path / "inst")})["success"] is True


def test_server_patterns_accept_only_measured_full_lines(toml):
    patterns = toml["extensions"]["saba-core"]
    ready = re.compile(patterns["ready_patterns"][0])
    graceful = re.compile(toml["detection"]["graceful_exit_patterns"][0])
    ready_lines = ("Server started", "Server gestartet", "Server avviato", "Serveur démarré",
                   "Servidor iniciado", "Сервер запущен", "服务器已启动", "Servidor inicializado.",
                   "Serwer uruchomiony", "サーバーが開始しました", "서버 시작", "伺服器已啟動")
    exit_lines = ("Saving before exit...", "Vor dem Verlassen speichern\u00a0...",
                  "Salvataggio prima dell'uscita...", "Enregistrement avant de quitter…",
                  "Guardando antes de salir...", "Сохранение перед выходом...", "正在进行退出前的保存……",
                  "Salvando antes de sair...", "Zapisywanie przed wyjściem...", "セーブ中…",
                  "종료 전에 저장하는 중...", "離開前儲存中……")
    assert all(ready.fullmatch(line) and ready.fullmatch(": " + line) for line in ready_lines)
    assert all(graceful.fullmatch(line) and graceful.fullmatch(": " + line) for line in exit_lines)
    assert not ready.fullmatch("<Server> Server started")


def test_install_refuses_zip_without_server(lc, tmp_path, monkeypatch):
    _fake_download(monkeypatch, lc, _zip_bytes({"1458/readme.txt": b"x"}))
    result = lc.install_server({"version": "1.4.5.8", "install_dir": str(tmp_path / "inst")})
    assert result["success"] is False


def test_install_refuses_escaping_entries(lc, tmp_path, monkeypatch):
    exe = lc._server_exe_name()
    folder = "Windows" if exe.endswith(".exe") else "Linux"
    _fake_download(monkeypatch, lc, _zip_bytes({
        f"1458/{folder}/{exe}": b"x",
        f"1458/{folder}/../../evil.txt": b"x",
    }))
    # run_lifecycle이 PathOutside를 module_path_outside 실패로 바꾼다. 아무것도 풀지 않는다
    with pytest.raises(lc.PathOutside):
        lc.install_server({"version": "1.4.5.8", "install_dir": str(tmp_path / "inst")})
    assert not (tmp_path / "evil.txt").exists()
    assert not (tmp_path / "inst" / exe).exists()


def test_install_refuses_corrupt_zip(lc, tmp_path, monkeypatch):
    _fake_download(monkeypatch, lc, b"not a zip")
    result = lc.install_server({"version": "1.4.5.8", "install_dir": str(tmp_path / "inst")})
    assert result["success"] is False


@pytest.mark.parametrize("version", ["", "latest", "1.4/../x", "1..4"])
def test_install_refuses_bad_version(lc, tmp_path, monkeypatch, version):
    monkeypatch.setattr(lc, "_download", _no_kill)
    result = lc.install_server({"version": version, "install_dir": str(tmp_path / "inst")})
    assert result["success"] is False


def test_install_reports_settings_write_failure(lc, tmp_path, monkeypatch):
    exe = lc._server_exe_name()
    folder = "Windows" if exe.endswith(".exe") else "Linux"
    _fake_download(monkeypatch, lc, _zip_bytes({f"1458/{folder}/{exe}": b"x"}))
    install = tmp_path / "inst"
    install.mkdir()
    (install / "serverconfig.txt").write_bytes(b"motd=\x81\n")
    result = lc.install_server({"version": "1.4.5.8", "install_dir": str(install),
                                "settings": {"max_players": 4}})
    assert result["success"] is False
    assert result["error_code"] == "settings_write_failed"


# ─── 번역 ────────────────────────────────────────────────

def test_locales_have_the_same_keys():
    import json

    def keys(d, prefix=""):
        out = set()
        for k, v in d.items():
            out |= keys(v, f"{prefix}{k}.") if isinstance(v, dict) else {prefix + k}
        return out

    base = keys(json.loads((MODULE_DIR / "locales" / "en.json").read_text(encoding="utf-8")))
    files = sorted((MODULE_DIR / "locales").glob("*.json"))
    assert len(files) == 10
    for path in files:
        assert keys(json.loads(path.read_text(encoding="utf-8"))) == base, path.name


def test_server_console_is_a_pseudo_console(toml):
    # 실측(2026-10-25): Windows 서버는 파이프 stdin을 읽지 않아 명령·exit가 먹지 않았다
    assert toml["launch"]["console"] == "pty"


def test_installed_version_comes_from_the_changelog(lc, tmp_path):
    server = _server(tmp_path, lc)
    # 공식 Windows zip의 changelog.txt 첫 줄 (실측, CRLF)
    (server / "changelog.txt").write_bytes(b"Version 1.4.5.8 Changes ---------------\r\n\r\nI. Content Changes\r\n")
    result = lc.get_installed_version(_config(server))
    assert result["success"] is True
    assert result["version"] == "1.4.5.8"


def test_installed_version_without_a_changelog_is_unknown(lc, tmp_path):
    result = lc.get_installed_version(_config(_server(tmp_path, lc)))
    assert result["success"] is False
