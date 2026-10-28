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
    ("neoforge", "26.1.2.7", "26.1.2", 25),
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
                           return_value={"has_server": True, "download_url": "x", "sha1": None}), \
            mock.patch.dict(server_types.SERVER_TYPES, {"spigot": {"family": "bukkit", "install": None}}):
        result = lifecycle.install_server({"version": "1.21.4", "install_dir": str(tmp_path),
                                           "settings": {"server_type": "spigot"}})
    assert result["success"] is False and "spigot" in result["message"]


def test_every_server_type_has_an_install_source():
    """마인크래프트 서버 종류는 모두 새로 설치할 수 있다 (사용자 지정 10-28)"""
    assert [t for t, info in server_types.SERVER_TYPES.items() if not info["install"]] == []


PUFFERFISH = "https://ci.pufferfish.host/job/Pufferfish-1.21/"


def test_pufferfish_takes_the_newest_successful_build_of_that_exact_version():
    builds = {"builds": [
        {"number": 40, "result": "FAILURE", "artifacts": []},
        {"number": 39, "result": "SUCCESS", "artifacts": [
            {"fileName": "pufferfish-paperclip-1.21.10-R0.1-SNAPSHOT-mojmap.jar",
             "relativePath": "pufferfish-server/build/libs/pufferfish-paperclip-1.21.10-R0.1-SNAPSHOT-mojmap.jar"}]},
        {"number": 21, "result": "SUCCESS", "artifacts": [
            {"fileName": "pufferfish-paperclip-1.21.1-R0.1-SNAPSHOT-mojmap.jar",
             "relativePath": "build/libs/pufferfish-paperclip-1.21.1-R0.1-SNAPSHOT-mojmap.jar"}]},
        {"number": 20, "result": "SUCCESS", "artifacts": [
            {"fileName": "pufferfish-paperclip-1.21.1-R0.1-SNAPSHOT-mojmap.jar",
             "relativePath": "build/libs/pufferfish-paperclip-1.21.1-R0.1-SNAPSHOT-mojmap.jar"}]},
    ]}
    with mock.patch.object(server_types, "_get_json", return_value=builds) as get:
        found = server_types.resolve_download("pufferfish", "1.21.1")
        assert get.call_args[0][0].startswith(PUFFERFISH + "api/json")
        assert found == {"url": PUFFERFISH + "21/artifact/build/libs/pufferfish-paperclip-1.21.1-R0.1-SNAPSHOT-mojmap.jar",
                         "build": 21}
        with pytest.raises(ValueError, match="version_unavailable"):
            server_types.resolve_download("pufferfish", "1.21.4")


def test_quilt_runs_the_official_installer_with_the_newest_stable_loader():
    answers = {
        "https://meta.quiltmc.org/v3/versions/loader/1.21.1": [
            {"loader": {"version": "0.30.0-beta.2"}}, {"loader": {"version": "0.29.2"}}, {"loader": {"version": "0.30.1"}},
            {"loader": {"version": "0.9.9"}}],
        "https://meta.quiltmc.org/v3/versions/installer": [
            {"version": "0.15.1", "url": "https://maven.quiltmc.org/repository/release/org/quiltmc/quilt-installer/0.15.1/quilt-installer-0.15.1.jar"},
            {"version": "0.15.0", "url": "https://maven.quiltmc.org/repository/release/org/quiltmc/quilt-installer/0.15.0/quilt-installer-0.15.0.jar"}],
    }
    texts = {answers["https://meta.quiltmc.org/v3/versions/installer"][0]["url"] + ".sha256": "ab" * 32 + "\n"}
    with mock.patch.object(server_types, "_get_json", side_effect=lambda url: answers[url]), \
            mock.patch.object(server_types, "_get_text", side_effect=lambda url: texts[url]):
        found = server_types.resolve_download("quilt", "1.21.1")
    assert found["url"].endswith("/quilt-installer-0.15.1.jar") and found["sha256"] == "ab" * 32
    assert found["build"] == "0.30.1" and found["installer"]
    assert server_types.installer_command("quilt", "1.21.1", found, "/b/server", "/b", "server.jar") == \
        (["install", "server", "1.21.1", "0.30.1", "--download-server", "--install-dir=/b/server"], "/b/server")


def test_forge_prefers_the_recommended_build_and_checks_the_maven_hash():
    promos = {"promos": {"1.21.1-latest": "52.1.21", "1.21.1-recommended": "52.1.20", "1.21.6-latest": "56.0.9"}}
    base = "https://maven.minecraftforge.net/net/minecraftforge/forge/"
    texts = {base + "1.21.1-52.1.20/forge-1.21.1-52.1.20-installer.jar.sha256": "cd" * 32,
             base + "1.21.6-56.0.9/forge-1.21.6-56.0.9-installer.jar.sha256": "ef" * 32}
    with mock.patch.object(server_types, "_get_json", return_value=promos), \
            mock.patch.object(server_types, "_get_text", side_effect=lambda url: texts[url]):
        found = server_types.resolve_download("forge", "1.21.1")
        assert (found["url"], found["sha256"], found["build"]) == (
            base + "1.21.1-52.1.20/forge-1.21.1-52.1.20-installer.jar", "cd" * 32, "1.21.1-52.1.20")
        assert server_types.resolve_download("forge", "1.21.6")["build"] == "1.21.6-56.0.9"
        with pytest.raises(ValueError, match="version_unavailable"):
            server_types.resolve_download("forge", "1.21.2")
    # 옛 Forge 설치기는 경로 인자 없이 작업 폴더에 깐다: 작업 폴더도 준비 폴더다
    assert server_types.installer_command("forge", "1.21.1", found, "/b/server", "/b", "server.jar") == \
        (["--installServer", "/b/server"], "/b/server")


def test_neoforge_picks_the_newest_stable_build_of_that_minecraft_version():
    versions = {"versions": ["21.1.208", "21.1.209", "21.1.210-beta", "21.10.5-beta", "21.11.1-beta",
                             "26.1.0.4-beta", "26.1.2.7", "26.1.2.12", "26.1.2.9"]}
    base = "https://maven.neoforged.net/releases/net/neoforged/neoforge/"
    with mock.patch.object(server_types, "_get_json", return_value=versions), \
            mock.patch.object(server_types, "_get_text", return_value="12" * 32):
        assert server_types.resolve_download("neoforge", "1.21.1")["url"] == \
            base + "21.1.209/neoforge-21.1.209-installer.jar"
        assert server_types.resolve_download("neoforge", "26.1.2")["build"] == "26.1.2.12"
        # 안정판이 아직 없으면 베타를 쓴다 (NeoForge가 내려받기로 내놓는 판)
        assert server_types.resolve_download("neoforge", "1.21.10")["build"] == "21.10.5-beta"
        assert server_types.resolve_download("neoforge", "26.1")["build"] == "26.1.0.4-beta"
        with pytest.raises(ValueError, match="version_unavailable"):
            server_types.resolve_download("neoforge", "1.21.4")


@pytest.mark.parametrize("server_type", ["spigot", "craftbukkit"])
def test_spigot_and_craftbukkit_are_built_by_buildtools(server_type):
    info = {"name": "4344", "javaVersions": [65, 67]}
    with mock.patch.object(server_types, "_get_json", return_value=info) as get:
        found = server_types.resolve_download(server_type, "1.21.1")
    assert get.call_args[0][0] == "https://hub.spigotmc.org/versions/1.21.1.json"
    assert found["url"] == "https://hub.spigotmc.org/jenkins/job/BuildTools/lastSuccessfulBuild/artifact/target/BuildTools.jar"
    assert found["installer"] and found["jdk"] and (found["java_min"], found["java_max"]) == (21, 23)
    assert found["build"] == "4344"
    # 소스·메이븐 작업물은 설치기 폴더에 두고 완성된 jar만 준비 폴더에 낸다
    assert server_types.installer_command(server_type, "1.21.1", found, "/b/server", "/b", "server.jar") == \
        (["--rev", "1.21.1", "--compile", server_type, "--nogui", "--output-dir", "/b/server", "--final-name", "server.jar"],
         "/b")

    with mock.patch.object(server_types, "_get_json", side_effect=ValueError("version_unavailable")):
        with pytest.raises(ValueError, match="version_unavailable"):
            server_types.resolve_download(server_type, "1.99")


@pytest.mark.parametrize("inner,expected", [("spigot-1.21.1-R0.1-SNAPSHOT.jar", "spigot"),
                                            ("craftbukkit-1.21.1-R0.1-SNAPSHOT.jar", "craftbukkit")])
def test_buildtools_bootstrap_jar_is_read_from_its_versions_list(tmp_path, inner, expected):
    """BuildTools가 만든 jar는 Main-Class가 CraftBukkit bootstrap이고 versions.list가 `해시 *이름` 한 칸 형식이다"""
    jar = make_jar(str(tmp_path / "server.jar"), {
        "META-INF/MANIFEST.MF": "Manifest-Version: 1.0\nMain-Class: org.bukkit.craftbukkit.bootstrap.Main\n",
        "META-INF/versions.list": f"d955c4b938 *{inner}\n",
    })
    info = server_types.inspect_jar(jar)
    assert (info["type"], info["version"]) == (expected, "1.21.1")


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


def _leaf_paths(obj, prefix=()):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from _leaf_paths(v, prefix + (k,))
    else:
        yield prefix, obj


def test_the_catalog_is_translated_into_every_module_language():
    """화면 문구는 10개 언어 모두에 둔다. 한국어만 있으면 다른 언어 사용자는 영어 원문을 본다"""
    catalog_locales = os.path.join(MODULE_DIR, "plugin_catalog", "locales")
    ko = dict(_leaf_paths(json.load(open(os.path.join(catalog_locales, "ko.json"), encoding="utf-8"))))
    for name in sorted(os.listdir(os.path.join(MODULE_DIR, "locales"))):
        if name in ("ko.json", "en.json"):  # 영어는 카탈로그 원문이다
            continue
        path = os.path.join(catalog_locales, name)
        assert os.path.exists(path), name
        other = dict(_leaf_paths(json.load(open(path, encoding="utf-8"))))
        assert other.keys() == ko.keys(), name
        empty = [k for k, v in other.items() if not isinstance(v, str) or not v.strip()]
        assert not empty, (name, empty[:5])


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


# 접근 권한·밴 해제·허용 목록·권한 그룹을 바꾸거나 아이템·돈·경험치·무적·게임 모드를 주는 명령은 danger다.
# caution은 말하는 사바쨩이 확인 없이 실행할 수 있어서, 이런 명령이 caution이면 권한을 바꾸지 않는다는 규칙이 깨진다(L48)
GRANTING_COMMANDS = {
    "essentialsx.json": {"eco-give", "eco-take", "exp-give", "exp-set", "exp-reset", "ess-gamemode", "ess-give",
                         "ess-give-meta", "god", "fly", "kit-give", "unban", "unbanip", "socialspy", "powertool"},
    "citizens.json": {"npc-owner"},
    "multiverse-core.json": {"mv-config"},
    "multiverse-portals.json": {"mvp-config"},
    "floodgate.json": {"fwhitelist-add", "fwhitelist-remove"},
    "griefprevention.json": {"adjustbonusclaimblocks", "adjustbonusclaimblocksall", "setaccruedclaimblocks"},
    "multiverse-inventories.json": {"mvinv-give", "mvinv-config"},
    "worldguard.json": {"wg-god", "ungod", "region-setparent", "region-addmember", "region-addowner",
                        "region-removemember", "region-removeowner", "region-flag", "region-define",
                        "region-setpriority"},
}


def test_commands_that_change_access_or_grant_things_are_danger():
    risks = {}
    for path in CATALOG:
        data = json.load(open(path, encoding="utf-8"))
        for cmd in data["commands"]:
            risks[(os.path.basename(path), cmd["name"])] = cmd["risk"]
    wrong = [key for file, names in GRANTING_COMMANDS.items() for key in ((file, n) for n in names)
             if risks.get(key) != "danger"]
    # 웹 편집기 생성은 권한 데이터를 외부 사이트에 올리므로 최소 caution이다.
    external_editors = {"lp-editor", "lp-user-editor", "lp-group-editor", "lp-track-editor"}
    wrong += [key for key, risk in risks.items()
              if key[0] == "luckperms.json" and risk == "caution" and key[1] not in external_editors]
    assert not wrong, wrong


def test_plugin_commands_without_a_known_risk_are_danger():
    """plugin.yml이 선언한 명령은 무엇을 하는지 모르고 인자도 자유롭다. 카탈로그에 risk가 없을 때도 같다"""
    assert plugins._declared_command("lp", {"usage": "/<command>"})["risk"] == "danger"
    assert plugins._catalog_command("x", {"name": "a", "console_template": "a"})["risk"] == "danger"


def test_player_only_commands_stay_out_of_the_console_list(tmp_path):
    """카탈로그가 플레이어 전용으로 적은 명령은 plugin.yml에 있어도 콘솔 명령으로 내지 않는다.
    EssentialsSpawn의 setspawn은 콘솔에서 쓰면 "플레이어만 쓸 수 있다"로 실패한다 (외부 감사 P2)"""
    make_jar(str(tmp_path / "plugins" / "EssentialsXSpawn.jar"), {
        "plugin.yml": "name: EssentialsSpawn\nversion: 2.21.0\nmain: x\ncommands:\n"
                      "  setspawn:\n    aliases: [esetspawn]\n  spawn:\n    aliases: [espawn]\n",
    })
    names = {c["name"] for c in lifecycle.instance_commands({"working_dir": str(tmp_path)})["commands"]}
    assert "setspawn" not in names
    assert "spawn" in names

    # 하위 명령이 플레이어 전용이면(CoreProtect "co inspect") 자유 입력 루트 "co {args}"로도 내지 않는다.
    # 콘솔에서 되는 하위 명령은 카탈로그 정의(co-lookup 등)로 남는다 (PR #43 리뷰)
    make_jar(str(tmp_path / "plugins" / "CoreProtect.jar"), {
        "plugin.yml": "name: CoreProtect\nversion: 23.0\nmain: x\ncommands:\n  co:\n    aliases: [core]\n",
    })
    names = {c["name"] for c in lifecycle.instance_commands({"working_dir": str(tmp_path)})["commands"]}
    assert "co" not in names
    assert "co-lookup" in names


def test_loader_servers_report_the_minecraft_version_they_run(tmp_path):
    """Quilt 런처 jar에는 버전이 없고 1.17+ Forge·NeoForge에는 실행 jar가 없다: 옆의 바닐라 jar나 로더 버전으로 답한다"""
    quilt = tmp_path / "quilt"
    make_jar(str(quilt / "quilt-server-launch.jar"), {
        "META-INF/MANIFEST.MF": "Main-Class: org.quiltmc.loader.impl.launch.server.QuiltServerLauncher\n"})
    bundler(str(quilt / "server.jar"), "server-1.21.1.jar", version="1.21.1", main="net.minecraft.bundler.Main")
    assert server_types.detect(str(quilt))["version"] == "1.21.1"
    found = lifecycle.get_installed_version({"working_dir": str(quilt),
                                             "server_executable": str(quilt / "quilt-server-launch.jar")})
    assert (found["success"], found["version"], found["server_type"]) == (True, "1.21.1", "quilt")

    neo = tmp_path / "neo"
    args = neo / "libraries" / "net" / "neoforged" / "neoforge" / "21.1.265"
    args.mkdir(parents=True)
    (args / "unix_args.txt").write_text("-cp x\n")
    found = lifecycle.get_installed_version({"working_dir": str(neo), "server_executable": str(neo / "server.jar")})
    assert (found["success"], found["version"], found["server_type"]) == (True, "1.21.1", "neoforge")


def test_year_versioned_forge_reports_its_minecraft_version(tmp_path):
    """26.x Forge의 로더 버전은 `26.3-66.1.0`이다 (클라우드 실측: 설치한 26.3 Forge의 installed-version이 실패했다)"""
    args = tmp_path / "libraries" / "net" / "minecraftforge" / "forge" / "26.3-66.1.0"
    args.mkdir(parents=True)
    (args / "unix_args.txt").write_text("-cp x\n")
    shim = make_jar(str(tmp_path / "forge-26.3-66.1.0-shim.jar"),
                    {"META-INF/MANIFEST.MF": "Main-Class: net.minecraftforge.bootstrap.shim.Main\n"})
    found = lifecycle.get_installed_version({"working_dir": str(tmp_path), "server_executable": shim})
    assert (found["success"], found.get("version"), found.get("server_type")) == (True, "26.3", "forge")
    assert server_types.minecraft_version_of("forge", "26.1.2-64.0.1") == "26.1.2"


def test_the_installer_result_must_be_the_chosen_type_and_build(tmp_path):
    """설치기가 끝난 뒤 준비 폴더에 그 종류·그 판의 서버가 있어야 한다 (서버 폴더에 남은 옛 jar로 판정하지 않는다)"""
    forge = tmp_path / "forge"
    args = forge / "libraries" / "net" / "minecraftforge" / "forge" / "1.21.1-52.1.20"
    args.mkdir(parents=True)
    (args / "unix_args.txt").write_text("-cp x\n")
    make_jar(str(forge / "forge-1.21.1-52.1.20-shim.jar"), {"META-INF/MANIFEST.MF": "Main-Class: net.minecraftforge.bootstrap.shim.Main\n"})
    assert server_types.installed_jar("forge", {"build": "1.21.1-52.1.20"}, str(forge), "server.jar") == "forge-1.21.1-52.1.20-shim.jar"
    assert server_types.installed_jar("forge", {"build": "1.21.1-52.1.21"}, str(forge), "server.jar") is None

    old = tmp_path / "old"
    make_jar(str(old / "forge-1.16.5-36.2.34.jar"), {"META-INF/MANIFEST.MF": "Main-Class: net.minecraftforge.server.ServerMain\n"})
    assert server_types.installed_jar("forge", {"build": "1.16.5-36.2.34"}, str(old), "server.jar") == "forge-1.16.5-36.2.34.jar"

    neo = tmp_path / "neo"
    (neo / "libraries" / "net" / "neoforged" / "neoforge" / "21.1.265").mkdir(parents=True)
    (neo / "libraries" / "net" / "neoforged" / "neoforge" / "21.1.265" / "unix_args.txt").write_text("-cp x\n")
    assert server_types.installed_jar("neoforge", {"build": "21.1.265"}, str(neo), "server.jar") == ""

    spigot = tmp_path / "spigot"
    bundler(str(spigot / "server.jar"), "paper-1.21.1.jar")
    assert server_types.installed_jar("spigot", {"build": "1"}, str(spigot), "server.jar") is None


def test_a_forge_args_file_beats_a_leftover_vanilla_jar_for_the_version(tmp_path):
    args = tmp_path / "libraries" / "net" / "minecraftforge" / "forge" / "1.21.1-52.1.20"
    args.mkdir(parents=True)
    (args / "unix_args.txt").write_text("-cp x\n")
    shim = make_jar(str(tmp_path / "forge-1.21.1-52.1.20-shim.jar"),
                    {"META-INF/MANIFEST.MF": "Main-Class: net.minecraftforge.bootstrap.shim.Main\n"})
    bundler(str(tmp_path / "server.jar"), "server-1.21.4.jar", version="1.21.4", main="net.minecraft.bundler.Main")
    assert server_types.detect(str(tmp_path), shim)["version"] == "1.21.1"


def test_buildtools_craftbukkit_jar_takes_the_server_jar_name(tmp_path):
    """BuildTools는 CraftBukkit에 --final-name을 쓰지 않고 craftbukkit-<버전>.jar로 낸다"""
    make_jar(str(tmp_path / "craftbukkit-1.21.1.jar"), {
        "META-INF/MANIFEST.MF": "Main-Class: org.bukkit.craftbukkit.bootstrap.Main\n",
        "META-INF/versions.list": "d955 *craftbukkit-1.21.1-R0.1-SNAPSHOT.jar\n"})
    assert server_types.installed_jar("craftbukkit", {"build": "1"}, str(tmp_path), "server.jar") == "server.jar"
    assert sorted(os.listdir(tmp_path)) == ["server.jar"]
