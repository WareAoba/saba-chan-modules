"""서버 종류 판정·설치 소스와 플러그인 판정·명령 카탈로그 테스트"""
import glob
import json
import os
import re
import zipfile
from unittest import mock

import pytest

from minecraft import lifecycle

server_types = lifecycle.server_types
plugins = lifecycle.plugins
MODULE_DIR = os.path.dirname(os.path.abspath(__file__))


def make_jar(path, files):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with zipfile.ZipFile(path, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return path


def bundler(path, inner, version="1.21.4", main="io.papermc.paperclip.Main"):
    return make_jar(path, {
        "META-INF/MANIFEST.MF": f"Manifest-Version: 1.0\nMain-Class: {main}\n",
        "META-INF/versions.list": f"abc123\t{version}\t{version}/{inner}\n",
    })


# ─── 서버 종류 판정 ─────────────────────────────────────────────

@pytest.mark.parametrize("inner,expected", [
    ("paper-1.21.4.jar", "paper"),
    ("purpur-1.21.4.jar", "purpur"),
    ("folia-1.21.4.jar", "folia"),
    ("server-1.21.4.jar", "vanilla"),
])
def test_bundled_server_jar_names_decide_the_type_whatever_the_file_is_called(tmp_path, inner, expected):
    bundler(str(tmp_path / "start.jar"), inner)
    found = server_types.detect(str(tmp_path))
    assert (found["type"], found["version"], found["jar"]) == (expected, "1.21.4", "start.jar")


def test_old_paperclip_falls_back_to_the_folder_for_the_fork(tmp_path):
    make_jar(str(tmp_path / "server.jar"), {
        "META-INF/MANIFEST.MF": "Main-Class: io.papermc.paperclip.Paperclip\n",
    })
    (tmp_path / "purpur.yml").write_text("settings: {}\n")
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "paper-global.yml").write_text("x: 1\n")
    assert server_types.detect(str(tmp_path))["type"] == "purpur"


def test_fabric_launcher_wins_over_the_vanilla_jar_it_downloads(tmp_path):
    bundler(str(tmp_path / "server.jar"), "server-1.21.4.jar", main="net.minecraft.bundler.Main")
    make_jar(str(tmp_path / "fabric-server-launch.jar"), {
        "META-INF/MANIFEST.MF": "Main-Class: net.fabricmc.loader.impl.launch.server.FabricServerLauncher\n",
    })
    found = server_types.detect(str(tmp_path), str(tmp_path / "server.jar"))
    assert (found["type"], found["jar"]) == ("fabric", "fabric-server-launch.jar")


def test_modern_forge_runs_from_its_args_file_without_a_jar(tmp_path):
    bundler(str(tmp_path / "server.jar"), "server-1.20.1.jar", main="net.minecraft.bundler.Main")
    args = tmp_path / "libraries" / "net" / "minecraftforge" / "forge" / "1.20.1-47.3.0"
    args.mkdir(parents=True)
    (args / "unix_args.txt").write_text("-cp x\n")
    (args / "win_args.txt").write_text("-cp x\n")
    found = server_types.detect(str(tmp_path))
    assert (found["type"], found["jar"], found["loader_version"]) == ("forge", None, "1.20.1-47.3.0")

    target, _ = server_types.launch_target({"server_type": "forge"}, None, str(tmp_path))
    assert target[0].startswith("@libraries/net/minecraftforge/forge/1.20.1-47.3.0/")
    assert target[-1] == "nogui"

    cmd = lifecycle.get_launch_command({"server_type": "forge", "working_dir": str(tmp_path), "java_path": "java"})
    assert cmd["success"] and "-jar" not in cmd["args"]


@pytest.mark.parametrize("server_type,loader,mc,java", [
    ("forge", "1.20.6-50.1.0", "1.20.6", 21),
    ("forge", "1.18.2-40.2.0", "1.18.2", 17),
    ("neoforge", "21.1.77", "1.21.1", 21),
    ("neoforge", "20.4.237", "1.20.4", 17),
    ("neoforge", "21.0.167", "1.21", 21),
    ("neoforge", "26.1.0.5-beta", "26.1", 25),
])
def test_args_file_install_without_version_needs_the_java_of_its_minecraft(tmp_path, server_type, loader, mc, java):
    """가져온 1.17+ Forge/NeoForge 서버(실행 jar·server_version 없음)는 로더 버전에서 Minecraft 버전을 읽는다"""
    root = {"forge": ("net", "minecraftforge", "forge"), "neoforge": ("net", "neoforged", "neoforge")}[server_type]
    args = tmp_path.joinpath("libraries", *root, loader)
    args.mkdir(parents=True)
    (args / "unix_args.txt").write_text("-cp x\n")
    found = server_types.detect(str(tmp_path))
    assert found["version"] == mc
    config = {"server_type": server_type, "working_dir": str(tmp_path)}
    assert lifecycle._required_java_for(config, lifecycle._resolve_server_jar(config)) == java


def test_folder_markers_alone_are_enough(tmp_path):
    (tmp_path / "spigot.yml").write_text("a: 1\n")
    (tmp_path / "bukkit.yml").write_text("a: 1\n")
    assert server_types.detect(str(tmp_path))["type"] == "spigot"
    assert server_types.detect(str(tmp_path / "missing"))["type"] is None


def test_validate_warns_when_the_chosen_type_is_not_what_is_installed(tmp_path):
    jar = bundler(str(tmp_path / "server.jar"), "paper-1.21.4.jar")
    (tmp_path / "eula.txt").write_text("eula=true\n")
    result = lifecycle.validate({"server_type": "vanilla", "server_jar": jar, "working_dir": str(tmp_path), "port": 0})
    assert "SERVER_TYPE_MISMATCH" in [i["code"] for i in result["issues"]]


@pytest.mark.parametrize("name", [
    "paper-1.21.4-130.jar",
    "purpur-1.21.4-2400.jar",
    "fabric-server-launch.jar",
    "fabric-server-mc.1.21.4-loader.0.16.10-launcher.1.0.1.jar",
    "minecraft_server.1.12.2.jar",
])
def test_server_jar_is_found_by_its_usual_name_when_server_jar_is_missing(tmp_path, name):
    (tmp_path / name).write_bytes(b"")
    config = {"working_dir": str(tmp_path), "server_executable": str(tmp_path / "server.jar")}
    assert lifecycle._resolve_server_jar(config) == str(tmp_path / name)


def test_server_jar_keeps_priority_and_missing_jar_stays_missing(tmp_path):
    (tmp_path / "paper-1.21.4-130.jar").write_bytes(b"")
    (tmp_path / "server.jar").write_bytes(b"")
    config = {"working_dir": str(tmp_path), "server_executable": "server.jar"}
    assert lifecycle._resolve_server_jar(config) == str(tmp_path / "server.jar")

    empty = tmp_path / "empty"
    empty.mkdir()
    (empty / "something-else.jar").write_bytes(b"")
    config = {"working_dir": str(empty), "server_executable": "server.jar"}
    assert lifecycle._resolve_server_jar(config) == str(empty / "server.jar")



def test_server_jar_picks_the_newest_build_and_keeps_an_explicit_name(tmp_path):
    for name in ["paper-1.21.4-99.jar", "paper-1.21.4-130.jar", "paper-1.9.4-200.jar"]:
        (tmp_path / name).write_bytes(b"")
    config = {"working_dir": str(tmp_path), "server_executable": "server.jar"}
    assert lifecycle._resolve_server_jar(config) == str(tmp_path / "paper-1.21.4-130.jar")

    # 사용자가 직접 적은 이름이 없으면 다른 jar로 바꾸지 않는다
    config = {"working_dir": str(tmp_path), "server_executable": "my-server.jar"}
    assert lifecycle._resolve_server_jar(config) == str(tmp_path / "my-server.jar")

# ─── 설치 소스 ──────────────────────────────────────────────────

def test_paper_downloads_the_newest_stable_build():
    builds = [
        {"id": 12, "channel": "ALPHA", "downloads": {"server:default": {"url": "u12"}}},
        {"id": 11, "channel": "STABLE", "downloads": {"server:default": {"url": "u11", "checksums": {"sha256": "ab"}}}},
    ]
    with mock.patch.object(server_types, "_get_json", return_value=builds) as get:
        assert server_types.resolve_download("paper", "1.21.4") == {"url": "u11", "sha256": "ab", "build": 11}
    assert get.call_args[0][0] == "https://fill.papermc.io/v3/projects/paper/versions/1.21.4/builds"


def test_fabric_picks_stable_loader_and_installer():
    answers = {
        "https://meta.fabricmc.net/v2/versions/loader/1.21.4": [
            {"loader": {"version": "0.17.0", "stable": False}}, {"loader": {"version": "0.16.10", "stable": True}}],
        "https://meta.fabricmc.net/v2/versions/installer": [{"version": "1.0.1", "stable": True}],
    }
    with mock.patch.object(server_types, "_get_json", side_effect=lambda url: answers[url]):
        found = server_types.resolve_download("fabric", "1.21.4")
    assert found["url"].endswith("/1.21.4/0.16.10/1.0.1/server/jar")


def test_types_without_a_download_source_say_so(tmp_path):
    with mock.patch.object(lifecycle.ServerInstaller, "get_version_details",
                           return_value={"has_server": True, "download_url": "x", "sha1": None}):
        result = lifecycle.install_server({"version": "1.21.4", "install_dir": str(tmp_path),
                                           "settings": {"server_type": "spigot"}})
    assert result["success"] is False and "spigot" in result["message"]


# ─── plugin.yml 읽기 ────────────────────────────────────────────

PLUGIN_YML = """
name: 'Essentials'
version: "2.21.0"
main: com.earth2me.essentials.Essentials
authors: [Zenexer, ementalo, "snowleo"]
softdepend:
  - Vault
commands:
  # 주석
  afk:
    description: Marks you as away-from-keyboard.
    usage: /<command> [player/message...]
    aliases: [eafk, away]
  balance:
    aliases:
    - bal
    - money
    description: >
      States the current balance
      of a player.
  'kit':
    usage: |
      /<command> [kit] [player]
      /<command> list
permissions:
  essentials.*:
    default: op
"""


def test_plugin_yml_subset_parses_like_yaml():
    parsed = plugins.parse_yaml(PLUGIN_YML)
    assert parsed["name"] == "Essentials" and parsed["version"] == "2.21.0"
    assert parsed["authors"] == ["Zenexer", "ementalo", "snowleo"]
    assert parsed["commands"]["afk"]["aliases"] == ["eafk", "away"]
    assert parsed["commands"]["balance"]["aliases"] == ["bal", "money"]
    assert parsed["commands"]["balance"]["description"] == "States the current balance of a player.\n"
    assert parsed["commands"]["kit"]["usage"].splitlines() == ["/<command> [kit] [player]", "/<command> list"]
    yaml = pytest.importorskip("yaml")
    assert parsed == yaml.safe_load(PLUGIN_YML) | {"version": "2.21.0"}


# ─── 플러그인·모드 판정과 명령 ─────────────────────────────────

def server_with_plugins(tmp_path):
    make_jar(str(tmp_path / "plugins" / "LuckPerms-Bukkit-5.4.jar"), {
        "plugin.yml": "name: LuckPerms\nversion: 5.4.0\nmain: x\ncommands:\n  luckperms:\n    aliases: [lp]\n",
    })
    make_jar(str(tmp_path / "plugins" / "renamed-anything.jar"), {
        "paper-plugin.yml": "name: CoolPaper\nversion: 1.0\nmain: y\n",
        "plugin.yml": "name: CoolPaper\nversion: 1.0\nmain: y\ncommands:\n  cool:\n    usage: /<command> <on|off>\n",
    })
    make_jar(str(tmp_path / "plugins" / "Old.jar.disabled"), {"plugin.yml": "name: Old\nversion: 1\nmain: z\ncommands:\n  old: {}\n"})
    make_jar(str(tmp_path / "plugins" / "library.jar"), {"a.class": ""})
    make_jar(str(tmp_path / "mods" / "spark-fabric.jar"), {"fabric.mod.json": json.dumps({"id": "spark", "name": "spark", "version": "1.10"})})
    make_jar(str(tmp_path / "mods" / "forge-mod.jar"), {
        "META-INF/mods.toml": 'modLoader="javafml"\n[[mods]]\nmodId="examplemod"\nversion="2.0"\ndisplayName="Example Mod"\n[[dependencies.examplemod]]\nmodId="forge"\n',
    })
    return tmp_path


def test_forge_picks_the_highest_build_by_number(tmp_path):
    for build in ("1.20.1-47.9.0", "1.20.1-47.10.0"):
        args = tmp_path / "libraries" / "net" / "minecraftforge" / "forge" / build
        args.mkdir(parents=True)
        (args / "unix_args.txt").write_text("-cp x\n")
    assert server_types.detect(str(tmp_path))["loader_version"] == "1.20.1-47.10.0"


def test_server_folder_with_brackets_is_still_read(tmp_path):
    work = tmp_path / "srv [paper]"
    bundler(str(work / "paper-1.21.4-130.jar"), "paper-1.21.4.jar")
    make_jar(str(work / "plugins" / "lp.jar"), {"plugin.yml": "name: LuckPerms\nversion: 5.4\n"})
    found = server_types.detect(str(work))
    assert (found["type"], found["jar"]) == ("paper", "paper-1.21.4-130.jar")
    assert [p["id"] for p in plugins.scan(str(work))] == ["LuckPerms"]


def test_one_broken_plugin_is_skipped_not_fatal(tmp_path):
    make_jar(str(tmp_path / "mods" / "array.jar"), {"quilt.mod.json": "[1, 2]"})
    make_jar(str(tmp_path / "mods" / "num.jar"), {"fabric.mod.json": '{"id": 42}'})
    make_jar(str(tmp_path / "plugins" / "lp.jar"), {"plugin.yml": "name: LuckPerms\nversion: 5.4\n"})
    found = {p["id"] for p in plugins.scan(str(tmp_path))}
    assert "LuckPerms" in found
    assert lifecycle.instance_commands({"working_dir": str(tmp_path)})["success"]


def test_plugins_are_identified_from_their_metadata_not_file_names(tmp_path):
    found = {p["id"]: p for p in plugins.scan(str(server_with_plugins(tmp_path)))}
    assert set(found) == {"LuckPerms", "CoolPaper", "Old", "spark", "examplemod"}
    assert (found["CoolPaper"]["kind"], found["CoolPaper"]["file"]) == ("paper", "plugins/renamed-anything.jar")
    assert found["CoolPaper"]["commands"]["cool"]["usage"] == "/<command> <on|off>"
    assert found["Old"]["enabled"] is False
    assert (found["examplemod"]["kind"], found["examplemod"]["name"]) == ("forge", "Example Mod")


def test_instance_commands_join_the_catalog_and_declared_commands(tmp_path):
    result = lifecycle.instance_commands({"working_dir": str(server_with_plugins(tmp_path))})
    by_name = {c["name"]: c for c in result["commands"]}
    # 공식 문서 기준 정의 (LuckPerms, spark 모드)
    assert by_name["lp-user-permission-set"]["group"] == "LuckPerms"
    assert by_name["lp-user-permission-set"]["console_template"].startswith("lp user {user} permission set")
    assert any(c["group"] == "spark" for c in result["commands"])
    # plugin.yml에만 있는 명령은 자유 입력 한 칸
    assert by_name["cool"]["console_template"] == "cool {args}"
    assert by_name["cool"]["inputs"][0]["placeholder"] == "/cool <on|off>"
    assert by_name["luckperms"]["group"] == "LuckPerms"
    # 꺼진 플러그인의 명령은 없다
    assert "old" not in by_name
    catalogs = {c["id"]: c["catalog"] for c in result["components"]}
    assert catalogs["LuckPerms"] == "luckperms" and catalogs["CoolPaper"] is None


def test_migration_reports_type_jar_and_plugins(tmp_path):
    server_with_plugins(tmp_path)
    bundler(str(tmp_path / "paper-1.21.4-100.jar"), "paper-1.21.4.jar")
    (tmp_path / "server.properties").write_text("motd=hi\nserver-port=25570\n")
    result = lifecycle.import_settings({"working_dir": str(tmp_path)})
    assert result["settings"]["server_type"] == "paper"
    assert result["executable_path"] == "paper-1.21.4-100.jar"
    assert result["detected"]["version"] == "1.21.4"
    assert {c["id"] for c in result["detected"]["components"]} >= {"LuckPerms", "CoolPaper"}


# ─── 카탈로그 데이터 ────────────────────────────────────────────

CATALOG = sorted(glob.glob(os.path.join(MODULE_DIR, "plugin_catalog", "*.json")))
PLACEHOLDER = re.compile(r"\{(\w+)\}")


def module_command_names():
    text = open(os.path.join(MODULE_DIR, "module.toml"), encoding="utf-8").read()
    return set(re.findall(r'^\[\[commands\.fields\]\]\nname = "([^"]+)"', text, re.M))


@pytest.mark.parametrize("path", CATALOG, ids=os.path.basename)
def test_catalog_entries_render_the_way_the_daemon_binds_them(path):
    data = json.load(open(path, encoding="utf-8"))
    assert data["match"] and data["docs"], "match와 공식 문서 출처가 있어야 한다"
    for cmd in data["commands"]:
        where = f"{os.path.basename(path)}:{cmd['name']}"
        assert re.fullmatch(r"[a-z0-9][a-z0-9._-]*", cmd["name"]), where
        assert cmd["risk"] in ("safe", "caution", "danger"), where
        template = cmd["console_template"]
        assert "\n" not in template and not PLACEHOLDER.sub("", template).count("{"), where
        # 데몬은 입력을 순서대로 채우고 비어 있는 {자리}를 앞 공백과 함께 지운다:
        # 자리 순서 = 입력 순서, 선택 입력은 필수 입력 뒤, 선택 자리 뒤에는 글자가 없어야 한다
        names = [i["name"] for i in cmd["inputs"]]
        assert PLACEHOLDER.findall(template) == names, where
        required = [i.get("required") is True for i in cmd["inputs"]]
        assert required == sorted(required, reverse=True), where
        if False in required:
            first_optional = names[required.index(False)]
            assert not PLACEHOLDER.sub("", template[template.index("{" + first_optional + "}"):]).strip(), where
        for item in cmd["inputs"]:
            assert item.get("type", "text") in ("text", "number", "select"), where
            if item.get("type") == "select":
                assert item["options"] and all(isinstance(o, str) for o in item["options"]), where


def test_catalog_names_are_unique_and_never_shadow_module_commands():
    seen, static = {}, module_command_names()
    for path in CATALOG:
        for cmd in json.load(open(path, encoding="utf-8"))["commands"]:
            assert cmd["name"] not in static, cmd["name"]
            assert cmd["name"] not in seen, f"{cmd['name']}: {seen.get(cmd['name'])} / {os.path.basename(path)}"
            seen[cmd["name"]] = os.path.basename(path)


def test_every_catalog_command_has_a_korean_label():
    ko = json.load(open(os.path.join(MODULE_DIR, "plugin_catalog", "locales", "ko.json"), encoding="utf-8"))
    for path in CATALOG:
        slug = os.path.splitext(os.path.basename(path))[0]
        for cmd in json.load(open(path, encoding="utf-8"))["commands"]:
            assert ko.get(slug, {}).get(cmd["name"], {}).get("label"), f"{slug}:{cmd['name']}"


def test_installing_a_version_without_settings_keeps_the_folders_server_type(tmp_path):
    """GUI의 '버전 설치'는 {version, install_dir}만 보낸다. Paper 폴더를 바닐라로 덮으면
    플러그인이 꺼지고 따로 저장된 지옥·엔드가 사라진 것처럼 보인다"""
    bundler(str(tmp_path / "paper-1.21.4.jar"), "paper-1.21.4.jar")
    with mock.patch.object(lifecycle.ServerInstaller, "install_server", return_value={"success": True}) as install:
        lifecycle.install_server({"version": "1.21.5", "install_dir": str(tmp_path)})
    assert install.call_args.kwargs["server_type"] == "paper"
    empty = tmp_path / "new"
    empty.mkdir()
    with mock.patch.object(lifecycle.ServerInstaller, "install_server", return_value={"success": True}) as install:
        lifecycle.install_server({"version": "1.21.5", "install_dir": str(empty)})
    assert install.call_args.kwargs["server_type"] == "vanilla"


def test_a_fabric_launcher_jar_tells_its_minecraft_version(tmp_path):
    """Fabric 런처 jar에는 version.json이 없다. 버전을 모르면 Java 17이 골라져 1.20.5+ 서버가 시작부터 실패한다"""
    jar = make_jar(str(tmp_path / "fabric-server-launch.jar"), {
        "META-INF/MANIFEST.MF": "Manifest-Version: 1.0\nMain-Class: net.fabricmc.loader.impl.launch.server.FabricServerLauncher\n",
        "install.properties": "fabric-loader-version=0.16.9\ngame-version=1.21.1\n",
    })
    info = server_types.inspect_jar(jar)
    assert (info["type"], info["version"]) == ("fabric", "1.21.1")


def test_new_instances_pick_the_newest_version_their_server_type_can_install():
    """만들 때 고르는 최신 버전은 서버 종류가 받을 수 있는 것이다 (Paper·Folia는 안정 빌드가 나와야 한다)"""
    manifest = {"latest": {"release": "1.21.5", "snapshot": "25w01a"},
                "versions": [{"id": v, "type": "release"} for v in ["1.21.5", "1.21.4", "1.21.3"]]}

    def resolve(server_type, version):
        if version == "1.21.5":
            raise ValueError("version_unavailable")
        return {"url": "u"}

    with mock.patch.object(lifecycle.ServerInstaller, "fetch_version_manifest", return_value=manifest), \
            mock.patch.object(server_types, "resolve_download", side_effect=resolve):
        paper = lifecycle.list_versions({"settings": {"server_type": "paper"}})
        vanilla = lifecycle.list_versions({"settings": {"server_type": "vanilla"}})
        plain = lifecycle.list_versions({})
    assert paper["success"] and paper["latest"]["release"] == "1.21.4"
    assert vanilla["latest"]["release"] == "1.21.5"
    assert plain["latest"]["release"] == "1.21.5"


def test_a_version_the_source_does_not_have_yet_is_skipped_not_given_up_on():
    """Folia·Purpur는 아직 없는 버전에 404를 준다: 그 버전을 건너뛰고 다음 릴리스를 본다 (Mojang 최신으로 돌아가지 않는다)"""
    import io
    import urllib.error

    def fake_urlopen(req, timeout=None):
        url = req.full_url
        if "1.21.5" in url:
            raise urllib.error.HTTPError(url, 404, "Not Found", {}, io.BytesIO(b"{}"))
        if "purpur" in url:
            body = {"build": "100", "md5": "x"}
        else:
            body = [{"id": 7, "channel": "STABLE", "downloads": {"server:default": {"url": "u", "checksums": {"sha256": "s"}}}}]
        return io.BytesIO(json.dumps(body).encode())

    with mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
        for server_type in ("paper", "folia", "purpur"):
            assert server_types.latest_installable(server_type, ["1.21.5", "1.21.4"]) == "1.21.4", server_type
