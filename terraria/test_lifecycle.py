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
from daemon_rcon import setting_file_keys

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
    assert sorted(game) == sorted(setting_file_keys(lc.MODULE_DIR)), "게임 설정마다 serverconfig 키(file_key)가 있어야 한다"


def test_launch_only_settings_are_not_written_to_the_game_file(lc, toml):
    launch_only = [f["name"] for f in toml["settings"]["fields"] if f.get("group") == "saba-chan"]
    assert launch_only
    assert not set(launch_only) & set(setting_file_keys(lc.MODULE_DIR))


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
    # 예약 함수 `functions`는 공용 run_lifecycle이 표의 이름으로 답한다
    assert "functions" not in lc.FUNCTIONS


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

def test_stop_uses_the_shared_procedure_with_the_console_exit(lc, monkeypatch):
    """정지 절차(기다림·강제 종료·managed·요청 불가)는 공용 graceful_stop이 맡는다. 테라리아는 콘솔 `exit`로만 요청한다"""
    asked, used = [], {}
    monkeypatch.setattr(lc, "request_console", lambda config, command: asked.append(command) or True)
    monkeypatch.setattr(lc, "graceful_stop", lambda config, request, i18n, module_dir: used.update(module_dir=module_dir) or request())
    assert lc.stop({"instance_id": "inst-1", "pid": 42}) is True
    assert asked == ["exit"]
    assert used["module_dir"] == lc.MODULE_DIR


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
    monkeypatch.setattr(lc, "_download", lambda *a, **k: pytest.fail("잘못된 버전은 받지 않는다"))
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


# PC 실측(2026-10-26) 콘솔 원문 → (토큰, 칠할 글자). 데몬처럼 priority가 큰 규칙부터 본다
_HIGHLIGHT_SAMPLES = [
    ("Server started", "success", "Server started"),
    (": Server started", "success", "Server started"),
    ("서버 시작", "success", "서버 시작"),
    ("Saving before exit...", "warn", "Saving before exit..."),
    ("60.3% - Smoothing the world - 0.2%", "dim", "60.3% - Smoothing the world - 0.2%"),
    ("Resetting game objects 24%", "dim", "Resetting game objects 24%"),
    ("Loading world data: 1%", "dim", "Loading world data: 1%"),
    ("Saving world data: 27%", "dim", "Saving world data: 27%"),
    ("Validating world save: 64%", "dim", "Validating world save: 64%"),
    ("Settling liquids 50%", "dim", "Settling liquids 50%"),
    ("Error Logging Enabled.", "dim", "Error Logging Enabled."),
    ("Terraria Server v1.4.5.8", "keyword", "v1.4.5.8"),
    ("Listening on port 7777", "number", "7777"),
    ("Port: 7777", "number", "7777"),
    ("Player limit: 8", "number", "8"),
    ("Time: 4:30 AM", "timestamp", "4:30 AM"),
    ("World Seed: 0.1.1.0.pumpkinseason|949485760", "string", "0.1.1.0.pumpkinseason|949485760"),
    ("Creating world - Seed: pumpkinseason|949485760, Width: 6400, Height: 1800, Evil: -1, Difficulty: 0",
     "string", "pumpkinseason|949485760"),
    ("<Server> hello from saba", "player", "<Server>"),
    (": time", "command", "time"),
    (":dawn", "command", "dawn"),
    ("Invalid command.", "error", "Invalid command."),
    (": Backing up world file", "info", "Backing up world file"),
    ("No players connected.", "info", "No players connected."),
    ("Forcing water to settle.", "info", "Forcing water to settle."),
    ("Unhandled Exception: System.IO.FileNotFoundException: Could not load file or assembly",
     "error", "System.IO.FileNotFoundException"),
]


def _first_highlight(toml, line):
    """데몬 토큰화의 단순판: priority 큰 규칙(같으면 먼저 선언된 규칙)의 첫 일치와 그 글자"""
    rules = toml["syntax_highlight"]["rules"]
    order = sorted(range(len(rules)), key=lambda i: (-rules[i].get("priority", 0), i))
    for i in order:
        m = re.search(rules[i]["pattern"], line)
        if m:
            text = m.group("hl") if "hl" in m.groupdict() and m.group("hl") is not None else m.group(0)
            return rules[i]["color"], text
    return None, None


@pytest.mark.parametrize("line,token,text", _HIGHLIGHT_SAMPLES)
def test_console_highlight_uses_measured_lines(toml, line, token, text):
    assert _first_highlight(toml, line) == (token, text)


def test_console_highlight_does_not_color_chat_as_server_messages(toml):
    assert _first_highlight(toml, "<Server> Server started") == ("player", "<Server>")


def test_ready_and_exit_highlight_follow_the_detection_patterns(toml):
    """준비·정상 종료 문구는 감지 패턴과 같은 목록이어야 한다 (언어를 더할 때 한쪽만 고치지 않게)"""
    rules = {r["name"]: r["pattern"] for r in toml["syntax_highlight"]["rules"]}
    def alternation(p):
        """`^(?:: )?(?P<hl>A|B)\\s*$`와 `^(?:: )?(?:A|B)\\s*$`에서 A|B"""
        start = p.index("(?P<hl>") + 7 if "(?P<hl>" in p else p.index("(?:", p.index(")?") + 2) + 3
        return p[start:p.rindex(")")]
    assert alternation(rules["server_started"]) == alternation(toml["extensions"]["saba-core"]["ready_patterns"][0])
    assert alternation(rules["saving_before_exit"]) == alternation(toml["detection"]["graceful_exit_patterns"][0])


def test_progress_patterns_fold_only_measured_progress_lines(toml):
    """데몬이 접을 진행 줄 (saba-chan #605 progress_patterns). 실측 진행 줄은 맞고 준비·채팅·저장 알림 줄은 맞지 않는다"""
    patterns = [re.compile(p) for p in toml["extensions"]["saba-core"]["progress_patterns"]]
    folds = lambda line: any(p.search(line) for p in patterns)
    for line in ("60.3% - Smoothing the world - 0.2%", "0.0% - Generating world terrain - 0.3%",
                 "Resetting game objects 24%", "Loading world data: 1%", "Saving world data: 27%",
                 "Validating world save: 64%", "Settling liquids 50%", ": Saving world data: 2%"):
        assert folds(line), line
    for line in ("Server started", ": Backing up world file", "<Server> 50% done", "Time: 4:30 AM"):
        assert not folds(line), line


def test_configure_writes_utf8_when_cp1252_cannot_hold_the_value(lc, tmp_path):
    """공식 cp1252 파일에 cp1252로 못 쓰는 값(한국어 MOTD, 한국어 사용자 이름 아래 월드 폴더)이 오면 UTF-8로 쓴다.
    서버는 UTF-8로 읽는다(PC 실측). 리뷰 P2: 'charmap' codec can't encode로 저장·시작이 실패했다"""
    server = _server(tmp_path, lc)
    (server / "serverconfig.txt").write_bytes(b"#motd=Please don\x92t cut the purple trees!\r\npriority=1\r\n")
    assert lc.configure(_config(server, settings={"motd": "안녕 사바쨩"}))["success"] is True
    raw = (server / "serverconfig.txt").read_bytes()
    text = raw.decode("utf-8")
    assert "motd=안녕 사바쨩\r\n" in text
    assert "#motd=Please don\u2019t cut the purple trees!" in text  # 주석의 0x92도 같은 글자로 옮겨진다
    assert lc.import_settings(_config(server))["settings"]["motd"] == "안녕 사바쨩"


def test_relative_world_path_is_under_the_server_folder(lc, tmp_path):
    """가져온 serverconfig.txt의 상대 world 경로는 서버 폴더 기준이다 (리뷰 P3: 모듈 폴더 기준으로 풀려 초기화가 아무것도 지우지 않았다)"""
    server = _server(tmp_path, lc)
    world_dir, _, world_file = lc._world(_config(server, world_dir="Worlds", world_name="a"))
    assert Path(world_dir) == server / "Worlds"
    assert Path(world_file) == server / "Worlds" / "a.wld"


# ─── 연결 해제 때 되돌릴 설정 파일 (#70 계약) ───────────────────

def test_config_files_is_the_server_config(lc, tmp_path):
    server = _server(tmp_path, lc)
    result = lc.config_files(_config(server))
    assert result == {"success": True, "files": [str(server / "serverconfig.txt")]}


def test_config_files_needs_a_server_folder(lc):
    assert lc.config_files({})["success"] is False


# ─── 실행 인자 모양은 module.toml `arg` 한 곳 ──────────────────

def test_launch_argument_shapes_come_from_module_toml(tmp_path):
    import shutil
    copy = tmp_path / "terraria"
    shutil.copytree(MODULE_DIR, copy, ignore=shutil.ignore_patterns("__pycache__", "test_*"))
    text = (copy / "module.toml").read_text(encoding="utf-8")
    assert "arg = \"-ip {value}\"" in text
    (copy / "module.toml").write_text(text.replace("arg = \"-ip {value}\"", "arg = \"-ip={value}\""),
                                      encoding="utf-8")
    spec = importlib.util.spec_from_file_location("terraria_lifecycle_copy", copy / "lifecycle.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    server = _server(tmp_path, module)
    result = module.get_launch_command(_config(server, preview=True, ip="127.0.0.1"))
    assert result["args"] == ["-config", "serverconfig.txt", "-ip=127.0.0.1"]


# ─── 접속·퇴장 (서버 파일 번역 문자열 LegacyMultiplayer.19·20, 12개 언어) ───

_JOIN_LEAVE = [
    ("{0} has joined.", "{0} has left."),
    ("{0} ist beigetreten.", "{0} ist gegangen."),
    ("{0} si è unito.", "{0} ha abbandonato."),
    ("{0} s'est connecté-e.", "{0} est parti-e."),
    ("{0} se ha unido.", "{0} se ha ido."),
    ("Игрок {0} присоединился.", "Игрок {0} вышел."),
    ("{0}已加入。", "{0}已离开。"),
    ("{0} entrou.", "{0} saiu."),
    ("{0} dołącza.", "{0} odchodzi."),
    ("{0}が参加しました。", "{0}が去りました。"),
    ("{0} 님이 참가했습니다.", "{0} 님이 떠났습니다."),
    ("{0}已加入。", "{0}已離開。"),
]


def _player_name(patterns, line):
    for pattern in patterns:
        m = re.fullmatch(pattern, line)
        if m:
            return m.group("name")
    return None


@pytest.mark.parametrize("name", ["Bob", "사바쨩", "Dr. Who"])
def test_join_and_leave_lines_name_the_player(toml, name):
    saba = toml["extensions"]["saba-core"]
    for join, leave in _JOIN_LEAVE:
        for prefix in ("", ": "):
            assert _player_name(saba["player_join_patterns"], prefix + join.format(name)) == name, join
            assert _player_name(saba["player_leave_patterns"], prefix + leave.format(name)) == name, leave
            assert _player_name(saba["player_leave_patterns"], prefix + join.format(name)) is None


def test_chat_and_team_lines_are_not_joins(toml):
    saba = toml["extensions"]["saba-core"]
    for line in ("<Bob> Alice has joined.", "Bob has joined the red team.", "<Bob> Alice has left.",
                 "Bob has left!", "*Bob has joined.", ": *Bob has left.", ": <Bob> Alice has joined.", ":<Bob> 사바쨩 님이 떠났습니다."):
        assert _player_name(saba["player_join_patterns"] + saba["player_leave_patterns"], line) is None, line


# ─── 접속자 (`playing`, 서버 파일 CLI.NoPlayers·OnePlayerConnected·PlayersConnected) ───

def _playing(monkeypatch, lc, response):
    sent = []

    def fake(iid, line, **kw):
        sent.append(line)
        return {"success": True, "data": {"response": response}}
    monkeypatch.setattr(lc, "daemon_stdin_command", fake)
    return sent


def test_player_count_does_not_write_to_the_console_every_minute(lc, monkeypatch):
    sent = _playing(monkeypatch, lc, "")
    assert lc.player_count({"instance_id": "inst-1"}) == {"success": True}
    # 수는 데몬이 접속·퇴장 줄로 센다. 최대 인원만 알려 준다
    assert lc.player_count({"instance_id": "inst-1", "max_players": 16}) == {"success": True, "max": 16}
    assert sent == []


@pytest.mark.parametrize("response,online,names", [
    (": playing\nNo players connected.", 0, []),
    ("Bob (127.0.0.1:50012)\n1 player connected.", 1, ["Bob"]),
    (": Bob (10.0.0.2:1)\n사바쨩 (10.0.0.3:2)\n2 players connected.", 2, ["Bob", "사바쨩"]),
    ("사바쨩 (10.0.0.3:2)\n플레이어 1명 접속 중.", 1, ["사바쨩"]),
    ("접속 중인 플레이어가 없음.", 0, []),
    ("A (1)\nB (2)\nC (3)\nПодключено игроков: 3.", 3, ["A", "B", "C"]),
])
def test_player_list_reads_playing(lc, monkeypatch, response, online, names):
    sent = _playing(monkeypatch, lc, response)
    result = lc.player_count({"instance_id": "inst-1", "players_detail": True})
    assert sent == ["playing"]
    assert result["success"] is True
    assert result["online"] == online
    assert result["players"] == [{"name": n} for n in names]
    assert "max" not in result


def test_player_list_reports_the_limit_and_keeps_names_that_start_with_a_colon(lc, monkeypatch):
    _playing(monkeypatch, lc, "Other (line)\n: playing\n:3 (1.2.3.4:5)\n1 player connected.")
    result = lc.player_count({"instance_id": "inst-1", "players_detail": True, "max_players": 8})
    assert result == {"success": True, "online": 1, "max": 8, "players": [{"name": ":3"}]}


def test_zero_is_a_launch_argument_value(lc, tmp_path):
    server = _server(tmp_path, lc)
    result = lc.get_launch_command(_config(server, preview=True, announcement_box_range=0))
    assert result["args"] == ["-config", "serverconfig.txt", "-announcementboxrange", "0"]


def test_player_list_without_matching_names_gives_only_the_count(lc, monkeypatch):
    _playing(monkeypatch, lc, "Bob (127.0.0.1:1)\n2 players connected.")
    result = lc.player_count({"instance_id": "inst-1", "players_detail": True})
    assert result == {"success": True, "online": 2}


def test_player_list_unknown_reply_is_a_failure(lc, monkeypatch):
    _playing(monkeypatch, lc, "Invalid command.")
    assert lc.player_count({"instance_id": "inst-1", "players_detail": True})["success"] is False


def test_player_list_console_failure_is_a_failure(lc, monkeypatch):
    def unavailable(*a, **kw):
        raise ConnectionError("409")
    monkeypatch.setattr(lc, "daemon_stdin_command", unavailable)
    assert lc.player_count({"instance_id": "inst-1", "players_detail": True})["success"] is False


def test_player_count_is_the_status_function(lc, toml):
    assert toml["status"]["players"] == "player_count"
    assert "player_count" in lc.FUNCTIONS and "config_files" in lc.FUNCTIONS


def test_module_has_an_icon():
    import struct
    head = (MODULE_DIR / "icon.png").read_bytes()[:24]
    width, height = struct.unpack(">II", head[16:24])
    assert head[:8] == b"\x89PNG\r\n\x1a\n" and width == height >= 256
