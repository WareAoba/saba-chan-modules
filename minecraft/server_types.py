"""Minecraft 서버 종류 (바닐라·페이퍼·패브릭 등) — 판정, 설치 소스, 실행 인자

서버 종류마다 다른 것은 "어떤 jar를 받는가"와 "어떻게 띄우는가" 두 가지다.
나머지(server.properties, 콘솔, EULA)는 모두 같으므로 한 모듈 안의 설정(`server_type`)으로 둔다.

판정은 파일 이름이 아니라 내용으로 한다:
  1. 서버 jar 안의 메타데이터 — 번들러/Paperclip의 `META-INF/versions.list`에 적힌
     실제 서버 jar 이름(`paper-1.21.4.jar`, `purpur-…`, `server-…`), MANIFEST의 Main-Class
  2. 서버 폴더의 흔적 — `config/paper-global.yml`, `purpur.yml`, `spigot.yml`,
     `.fabric/`, `libraries/net/neoforged/neoforge/<버전>/` 등
새 서버 종류는 아래 표(`SERVER_TYPES`, `_VERSIONS_LIST_PREFIXES`, `_MAIN_CLASSES`, `_MARKERS`)에
한 줄씩 더하면 된다. 판정 코드는 표만 읽는다.
"""

import glob
import json
import os
import re
import sys
import zipfile

USER_AGENT = "saba-chan/1.0 (https://github.com/WareAoba/saba-chan)"

# id → 성격. family: 플러그인/모드 폴더 판정에 쓴다 (bukkit = plugins/, mod = mods/)
# install: install_server가 받을 수 있는 소스 (없으면 직접 설치한 서버를 가져와야 한다).
# quilt·forge·neoforge·buildtools는 받은 jar가 서버가 아니라 공식 설치기다 (`installer_args`로 돌린다)
SERVER_TYPES = {
    "vanilla":    {"family": None,     "install": "mojang"},
    "paper":      {"family": "bukkit", "install": "fill"},
    "folia":      {"family": "bukkit", "install": "fill"},
    "purpur":     {"family": "bukkit", "install": "purpur"},
    "pufferfish": {"family": "bukkit", "install": "pufferfish"},
    "spigot":     {"family": "bukkit", "install": "buildtools"},
    "craftbukkit": {"family": "bukkit", "install": "buildtools"},
    "fabric":     {"family": "mod",    "install": "fabric"},
    "quilt":      {"family": "mod",    "install": "quilt"},
    "forge":      {"family": "mod",    "install": "forge"},
    "neoforge":   {"family": "mod",    "install": "neoforge"},
}

# versions.list 세 번째 칸(번들된 서버 jar 경로)의 파일 이름 접두사 → 서버 종류
_VERSIONS_LIST_PREFIXES = [
    ("purpur-", "purpur"),
    ("pufferfish-", "pufferfish"),
    ("folia-", "folia"),
    ("paper-", "paper"),
    ("spigot-", "spigot"),
    ("craftbukkit-", "craftbukkit"),
    ("server-", "vanilla"),
]

# MANIFEST Main-Class → 서버 종류 (번들러가 아닌 예전 jar·런처)
_MAIN_CLASSES = [
    ("net.fabricmc.", "fabric"),
    ("org.quiltmc.", "quilt"),
    ("net.minecraftforge.", "forge"),
    ("cpw.mods.", "forge"),
    ("net.neoforged.", "neoforge"),
    ("org.bukkit.craftbukkit.", "craftbukkit"),
    ("net.minecraft.server.", "vanilla"),
]

# 서버 폴더 흔적 → 서버 종류. 더 구체적인 것이 먼저다 (퍼퍼 폴더에도 paper-global.yml이 있다)
_MARKERS = [
    ("purpur.yml", "purpur"),
    ("pufferfish.yml", "pufferfish"),
    ("config/folia-global.yml", "folia"),
    ("config/paper-global.yml", "paper"),
    ("paper.yml", "paper"),
    ("spigot.yml", "spigot"),
    ("bukkit.yml", "craftbukkit"),
    ("libraries/net/neoforged/neoforge", "neoforge"),
    ("libraries/net/minecraftforge/forge", "forge"),
    (".quilt", "quilt"),
    ("quilt-server-launcher.properties", "quilt"),
    (".fabric", "fabric"),
    ("fabric-server-launcher.properties", "fabric"),
]

# server.jar가 없을 때 서버 폴더에서 찾는 흔한 서버 jar 이름 (앞의 것이 먼저다)
_JAR_NAMES = [
    "server.jar",
    "fabric-server-launch.jar",
    "fabric-server-mc.*.jar",
    "paper-*.jar",
    "purpur-*.jar",
    "minecraft_server.*.jar",
]

# Paperclip 계열은 같은 진입점을 쓰므로 jar 안의 versions.list가 결정한다
_PAPERCLIP_MAIN = ("io.papermc.paperclip.", "com.destroystokyo.paperclip.")
_MC_VERSION = re.compile(r"^(\d+\.\d+(?:\.\d+)?|\d{2}w\d{2}[a-z])")


def _read_manifest(zf):
    try:
        text = zf.read("META-INF/MANIFEST.MF").decode("utf-8", "replace")
    except KeyError:
        return {}
    out = {}
    key = None
    for line in text.splitlines():
        if line.startswith(" ") and key:  # 72바이트 넘는 값의 이어지는 줄
            out[key] += line[1:]
        elif ":" in line:
            key, _, value = line.partition(":")
            key = key.strip()
            out[key] = value.strip()
    return out


def inspect_jar(path):
    """jar 하나를 열어 서버 종류와 마인크래프트 버전을 알아낸다. 서버 jar가 아니면 None.

    돌려주는 값: {"type", "version"(없을 수 있음), "evidence"}
    """
    try:
        zf = zipfile.ZipFile(path)
    except (OSError, zipfile.BadZipFile):
        return None
    with zf:
        names = set(zf.namelist())
        manifest = _read_manifest(zf)
        main = manifest.get("Main-Class", "")

        # 1.18+ 번들러와 Paperclip: 번들된 실제 서버 jar 이름이 종류와 버전을 알려 준다
        # (BuildTools의 Spigot·CraftBukkit bootstrap은 `해시 *이름` 한 칸이고 버전은 이름에 있다)
        if "META-INF/versions.list" in names:
            listing = zf.read("META-INF/versions.list").decode("utf-8", "replace")
            for line in listing.splitlines():
                parts = line.split("\t")
                if len(parts) >= 3:
                    file_name, version = os.path.basename(parts[2]), parts[1] or None
                elif line.split() and line.split()[-1].startswith("*"):
                    file_name, version = os.path.basename(line.split()[-1][1:]), None
                else:
                    continue
                for prefix, server_type in _VERSIONS_LIST_PREFIXES:
                    if file_name.startswith(prefix):
                        if version is None:
                            named = _MC_VERSION.match(file_name[len(prefix):])
                            version = named.group(1) if named else None
                        return {"type": server_type, "version": version,
                                "evidence": f"{os.path.basename(path)}: META-INF/versions.list → {file_name}"}

        # 바닐라 jar의 version.json
        version = None
        if "version.json" in names:
            try:
                version = json.loads(zf.read("version.json").decode("utf-8")).get("id")
            except (ValueError, KeyError):
                version = None

        # Fabric 서버 런처 jar는 version.json 대신 install.properties에 게임 버전을 적는다
        if version is None and "install.properties" in names:
            props = zf.read("install.properties").decode("utf-8", "replace")
            m = re.search(r"^game-version=(\S+)", props, re.M)
            version = m.group(1) if m else None

        # 예전 Paperclip(1.17 이하)은 versions.list가 없다 — 폴더 흔적으로 포크를 가린다
        if main.startswith(_PAPERCLIP_MAIN):
            return {"type": "paper", "version": version, "paper_family": True,
                    "evidence": f"{os.path.basename(path)}: Main-Class {main}"}

        for prefix, server_type in _MAIN_CLASSES:
            if main.startswith(prefix):
                # CraftBukkit 클래스를 쓰면서 Spigot 클래스가 들어 있으면 Spigot이다
                if server_type == "craftbukkit" and any(n.startswith("org/spigotmc/") for n in names):
                    server_type = "spigot"
                return {"type": server_type, "version": version,
                        "evidence": f"{os.path.basename(path)}: Main-Class {main}"}

        if "net/minecraft/server/MinecraftServer.class" in names:
            return {"type": "vanilla", "version": version,
                    "evidence": f"{os.path.basename(path)}: net/minecraft/server/MinecraftServer"}
    return None


def _version_key(path):
    """이름 속 숫자를 수로 비교한다: paper-1.21.4-130.jar가 -99.jar보다, 1.21이 1.9보다 뒤다"""
    return [int(t) if t.isdigit() else t for t in re.split(r"(\d+)", os.path.basename(path).lower())]


def find_server_jar(working_dir):
    """서버 폴더에서 `_JAR_NAMES` 순서로 서버 jar를 찾는다. 같은 무늬가 여럿이면 버전이 가장 높은 것 (없으면 None)"""
    if not working_dir or not os.path.isdir(working_dir):
        return None
    for pattern in _JAR_NAMES:
        found = [p for p in glob.glob(os.path.join(glob.escape(working_dir), pattern)) if os.path.isfile(p)]
        if found:
            return max(found, key=_version_key)
    return None


def _marker_type(working_dir):
    for rel, server_type in _MARKERS:
        if os.path.exists(os.path.join(working_dir, *rel.split("/"))):
            return server_type, rel
    return None, None


def _modern_forge_version(working_dir, server_type):
    """1.17+ Forge/NeoForge는 실행 jar 대신 libraries/…/<버전>/unix_args.txt로 뜬다."""
    root = {"forge": ("net", "minecraftforge", "forge"),
            "neoforge": ("net", "neoforged", "neoforge")}.get(server_type)
    if not root:
        return None
    base = os.path.join(working_dir, "libraries", *root)
    candidates = glob.glob(os.path.join(glob.escape(base), "*", "unix_args.txt"))
    if not candidates:
        return None
    # 47.10.0이 47.9.0보다 뒤다 — 글자 순서가 아니라 버전 숫자로 고른다
    return os.path.basename(os.path.dirname(max(candidates, key=lambda p: _version_key(os.path.dirname(p)))))


def detect(working_dir, server_jar=None):
    """서버 폴더(와 지정된 jar)를 보고 서버 종류를 정한다.

    돌려주는 값: {"type", "version", "jar"(작업 폴더 기준 상대 경로, 없을 수 있음), "evidence": [...]}
    판정할 수 없으면 type이 None이다.
    """
    result = {"type": None, "version": None, "jar": None, "evidence": []}
    if not working_dir or not os.path.isdir(working_dir):
        return result

    jars = []
    if server_jar and os.path.isfile(server_jar):
        jars.append(server_jar)
    jars.extend(p for p in sorted(glob.glob(os.path.join(glob.escape(working_dir), "*.jar"))) if p not in jars)

    infos = [(jar, info) for jar in jars for info in [inspect_jar(jar)] if info]
    marker, marker_rel = _marker_type(working_dir)

    found = infos[0] if infos else None
    if found and found[1]["type"] == "vanilla":
        # 로더(Fabric·Forge)나 포크 jar가 바닐라 jar 옆에 있으면 그쪽이 서버를 띄운다
        found = next((x for x in infos if x[1]["type"] != "vanilla"), found)
    if found:
        jar, info = found
        server_type = info["type"]
        # 예전 Paperclip jar는 paper/purpur/pufferfish가 같아 보인다 — 폴더 흔적이 더 구체적이면 그것
        if info.get("paper_family") and marker in ("purpur", "pufferfish", "folia"):
            server_type = marker
            result["evidence"].append(f"{marker_rel} (folder)")
        # 바닐라 jar만 있는데 Forge/NeoForge/Fabric 설치 흔적이 있으면 그 로더가 띄운다 (1.17+ Forge는 jar가 없다)
        if server_type == "vanilla" and family(marker) == "mod":
            server_type = marker
            jar = None
            result["evidence"].append(f"{marker_rel} (folder)")
        result.update(type=server_type, version=info.get("version"),
                      jar=os.path.relpath(jar, working_dir) if jar else None)
        result["evidence"].insert(0, info["evidence"])
    elif marker:
        result["type"] = marker
        result["evidence"].append(f"{marker_rel} (folder)")

    # 로더 런처 jar(Quilt)에 버전이 없으면 그 로더가 띄우는 옆의 바닐라 jar의 버전이다
    if result["version"] is None and family(result["type"]) == "mod":
        result["version"] = next((info.get("version") for _, info in infos
                                  if info["type"] == "vanilla" and info.get("version")), None)

    if result["type"] in ("forge", "neoforge"):
        args_version = _modern_forge_version(working_dir, result["type"])
        if args_version:
            result["loader_version"] = args_version
            result["version"] = result["version"] or minecraft_version_of(result["type"], args_version)
            result["evidence"].append(f"libraries/…/{args_version}/unix_args.txt")
    return result


def minecraft_version_of(server_type, loader_version):
    """로더 버전에 담긴 Minecraft 버전. Forge는 `1.20.1-47.3.0`, NeoForge는 `21.1.77`(→1.21.1)·`26.1.0.5`(→26.1)"""
    text = str(loader_version or "")
    if server_type == "forge":
        mc = text.split("-")[0]
        return mc if re.fullmatch(r"1\.\d+(\.\d+)?", mc) else None
    if server_type == "neoforge":
        numbers = re.match(r"(\d+)\.(\d+)", text)
        if not numbers:
            return None
        major, minor = int(numbers.group(1)), int(numbers.group(2))
        if major >= 26:  # 연도 기반 Minecraft 버전: 26.1.0.5 → 26.1, 26.1.2.7 → 26.1.2
            patch = re.match(r"\d+\.\d+\.(\d+)\.", text)
            return f"{major}.{minor}" + (f".{patch.group(1)}" if patch and patch.group(1) != "0" else "")
        return f"1.{major}" if minor == 0 else f"1.{major}.{minor}"
    return None


def family(server_type):
    return SERVER_TYPES.get(server_type or "vanilla", {}).get("family")


def launch_target(config, server_jar, working_dir):
    """JVM 인자 뒤에 붙일 실행 대상. (args, None) 또는 (None, 실패 응답 dict)

    1.17+ Forge/NeoForge는 실행 jar가 없고 설치기가 만든 인자 파일(@…/win_args.txt)로 뜬다.
    그 밖의 종류는 모두 `-jar <서버 jar> nogui`다.
    """
    server_type = config.get("server_type") or "vanilla"
    if server_type in ("forge", "neoforge"):
        version = _modern_forge_version(working_dir, server_type) if working_dir else None
        if version:  # 인자 파일이 있으면 그것이 정식 실행 방법이다 (옛 Forge는 jar로 뜬다)
            root = {"forge": "libraries/net/minecraftforge/forge",
                    "neoforge": "libraries/net/neoforged/neoforge"}[server_type]
            args_file = "win_args.txt" if sys.platform == "win32" else "unix_args.txt"
            return [f"@{root}/{version}/{args_file}", "nogui"], None
    if not server_jar or not os.path.isfile(server_jar):
        return None, None  # 호출한 쪽이 jar 없음 오류를 만든다
    return ["-jar", os.path.abspath(server_jar), "nogui"], None


# ─── 설치 소스 ──────────────────────────────────────────────────

def _get_json(url, timeout=15):
    """받을 곳의 JSON. 그 버전이 없으면(404: Folia·Purpur·등록 초기의 Paper) ValueError("version_unavailable")"""
    import urllib.error
    import urllib.request
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        if e.code == 404:
            raise ValueError("version_unavailable") from e
        raise


def _get_text(url, timeout=15):
    """받을 곳의 짧은 글 (maven의 `.sha256` 같은 해시 파일). 없으면(404) ValueError("version_unavailable")"""
    import urllib.error
    import urllib.request
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read(4096).decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            raise ValueError("version_unavailable") from e
        raise


def _maven_sha256(url):
    """maven 저장소가 jar 옆에 두는 `.sha256`. 형식이 아니면 ValueError"""
    text = _get_text(url + ".sha256").strip().split()
    if not text or not re.fullmatch(r"[0-9a-fA-F]{64}", text[0]):
        raise ValueError("version_unavailable")
    return text[0].lower()


def _stable(version):
    """`-beta`·`-alpha`·`+…` 같은 꼬리가 없는 판"""
    return re.fullmatch(r"[\d.]+", version) is not None


_BUILDTOOLS_URL = "https://hub.spigotmc.org/jenkins/job/BuildTools/lastSuccessfulBuild/artifact/target/BuildTools.jar"


def resolve_download(server_type, mc_version):
    """서버 종류와 마인크래프트 버전으로 받을 jar를 정한다.

    돌려주는 값: {"url", "sha256"?, "sha1"?, "md5"?, "build"?} — 받을 수 없으면 ValueError
    (메시지는 install_server가 사용자 문구로 바꾼다). 공식 설치기를 받는 종류는 `installer: True`와
    설치기를 돌릴 최소 Java(`java_min`, 있을 때)를 더한다
    """
    source = SERVER_TYPES.get(server_type, {}).get("install")
    if source == "fill":
        # PaperMC Fill v3: 빌드 목록은 최신순, 안정 채널의 server:default를 받는다
        builds = _get_json(f"https://fill.papermc.io/v3/projects/{server_type}/versions/{mc_version}/builds")
        if isinstance(builds, dict):  # {"ok": false, "message": ...}
            raise ValueError("version_unavailable")
        for build in builds:
            if build.get("channel") == "STABLE":
                dl = (build.get("downloads") or {}).get("server:default") or {}
                if dl.get("url"):
                    return {"url": dl["url"], "sha256": (dl.get("checksums") or {}).get("sha256"),
                            "build": build.get("id")}
        raise ValueError("version_unavailable")
    if source == "purpur":
        info = _get_json(f"https://api.purpurmc.org/v2/purpur/{mc_version}/latest")
        if not info.get("build"):
            raise ValueError("version_unavailable")
        return {"url": f"https://api.purpurmc.org/v2/purpur/{mc_version}/latest/download",
                "md5": info.get("md5"), "build": info.get("build")}
    if source == "fabric":
        loaders = _get_json(f"https://meta.fabricmc.net/v2/versions/loader/{mc_version}")
        stable = [entry["loader"]["version"] for entry in loaders if entry.get("loader", {}).get("stable")]
        if not stable:
            raise ValueError("version_unavailable")
        installers = _get_json("https://meta.fabricmc.net/v2/versions/installer")
        installer = next((i["version"] for i in installers if i.get("stable")), None)
        if not installer:
            raise ValueError("version_unavailable")
        return {"url": f"https://meta.fabricmc.net/v2/versions/loader/{mc_version}/{stable[0]}/{installer}/server/jar",
                "build": stable[0]}
    if source == "pufferfish":
        # Pufferfish는 마인크래프트 부 버전마다 Jenkins 작업 하나(Pufferfish-1.21)를 두고, 빌드마다 jar 하나를 낸다.
        # 공식 체크섬은 없다 (Jenkins 지문 페이지는 받은 파일의 MD5를 다시 계산할 뿐이다)
        minor = re.match(r"(\d+\.\d+)", mc_version)
        if not minor:
            raise ValueError("version_unavailable")
        job = f"https://ci.pufferfish.host/job/Pufferfish-{minor.group(1)}/"
        want = re.compile(rf"pufferfish-paperclip-{re.escape(mc_version)}-R\d+\.\d+-SNAPSHOT(-\w+)?\.jar")
        listing = _get_json(job + "api/json?tree=builds[number,result,artifacts[fileName,relativePath]]")
        for build in listing.get("builds") or []:
            if build.get("result") != "SUCCESS":
                continue
            for artifact in build.get("artifacts") or []:
                if want.fullmatch(artifact.get("fileName", "")):
                    return {"url": f"{job}{build['number']}/artifact/{artifact['relativePath']}", "build": build["number"]}
        raise ValueError("version_unavailable")
    if source == "quilt":
        # 공식 설치기가 로더 라이브러리와 바닐라 server.jar를 받고 quilt-server-launch.jar를 만든다.
        # 메타 API의 hashes는 maven의 jar와 다를 때가 있어 (2026-10 실측) maven의 .sha256으로 확인한다
        loaders = [entry["loader"]["version"] for entry in
                   _get_json(f"https://meta.quiltmc.org/v3/versions/loader/{mc_version}") if entry.get("loader")]
        stable = [v for v in loaders if _stable(v)]
        if not stable:
            raise ValueError("version_unavailable")
        installers = [i for i in _get_json("https://meta.quiltmc.org/v3/versions/installer")
                      if _stable(i.get("version", "")) and str(i.get("url", "")).startswith("https://maven.quiltmc.org/")]
        if not installers:
            raise ValueError("version_unavailable")
        installer = max(installers, key=lambda i: _version_key(i["version"]))
        return {"url": installer["url"], "sha256": _maven_sha256(installer["url"]),
                "build": max(stable, key=_version_key), "installer": True}
    if source == "forge":
        # 추천 판, 없으면 최신 판 (files.minecraftforge.net이 내려받기로 내놓는 두 판)
        promos = _get_json("https://files.minecraftforge.net/net/minecraftforge/forge/promotions_slim.json").get("promos") or {}
        build = promos.get(f"{mc_version}-recommended") or promos.get(f"{mc_version}-latest")
        if not build:
            raise ValueError("version_unavailable")
        full = f"{mc_version}-{build}"
        url = f"https://maven.minecraftforge.net/net/minecraftforge/forge/{full}/forge-{full}-installer.jar"
        return {"url": url, "sha256": _maven_sha256(url), "build": full, "installer": True}
    if source == "neoforge":
        # 그 마인크래프트 버전의 가장 새 안정판, 안정판이 아직 없으면 가장 새 베타
        versions = _get_json("https://maven.neoforged.net/api/maven/versions/releases/net/neoforged/neoforge").get("versions") or []
        mine = [v for v in versions if minecraft_version_of("neoforge", v) == mc_version]
        if not mine:
            raise ValueError("version_unavailable")
        build = max([v for v in mine if _stable(v)] or mine, key=_version_key)
        url = f"https://maven.neoforged.net/releases/net/neoforged/neoforge/{build}/neoforge-{build}-installer.jar"
        return {"url": url, "sha256": _maven_sha256(url), "build": build, "installer": True}
    if source == "buildtools":
        # Spigot·CraftBukkit은 배포 jar가 없고 공식 BuildTools로 빌드한다. 버전 정보가 없으면 빌드할 수 없는 버전이다.
        # javaVersions는 BuildTools가 받는 Java의 class 파일 버전 범위다 (65 = Java 21). BuildTools.jar에는 공식 체크섬이 없다
        info = _get_json(f"https://hub.spigotmc.org/versions/{mc_version}.json")
        java = [v for v in info.get("javaVersions") or [] if isinstance(v, int)]
        return {"url": _BUILDTOOLS_URL, "build": str(info.get("name") or ""), "installer": True,
                **({"java_min": java[0] - 44} if java else {})}
    raise ValueError("install_unsupported")


def installer_args(server_type, mc_version, source, install_dir, build_dir, jar_name):
    """공식 설치기(`java -jar <설치기>`) 뒤에 붙일 인자. 설치기는 build_dir에서 돈다.

    Quilt·Forge·NeoForge는 install_dir에 서버를 바로 깐다. BuildTools는 build_dir에 빌드해 jar_name으로 낸다
    (호출한 쪽이 완성된 jar를 서버 폴더로 옮긴다)
    """
    kind = SERVER_TYPES[server_type]["install"]
    if kind == "quilt":
        return ["install", "server", mc_version, source["build"], "--download-server", f"--install-dir={install_dir}"]
    if kind in ("forge", "neoforge"):
        return ["--installServer", install_dir]
    if kind == "buildtools":
        return ["--rev", mc_version, "--compile", server_type, "--nogui", "--output-dir", build_dir,
                "--final-name", jar_name]
    raise ValueError("install_unsupported")


def latest_installable(server_type, releases, tries=10, deadline_secs=60):
    """서버 종류가 받을 수 있는 가장 새 릴리스 (`releases`는 새것부터).

    Paper·Folia는 새 마인크래프트 버전이 나온 뒤 안정 빌드가 늦게 나오므로, Mojang 최신이 아직 없을 수 있다.
    받을 곳이 따로 없는 종류(바닐라 등)나 확인하지 못하면(네트워크) None — 호출 쪽이 Mojang 최신을 쓴다
    """
    if not SERVER_TYPES.get(server_type, {}).get("install"):
        return None
    import time
    # 차례로 묻는다: 받을 곳이 느려도 데몬의 list_versions 한도(120초) 안에 끝나게
    deadline = time.monotonic() + deadline_secs
    for version in releases[:tries]:
        if time.monotonic() > deadline:
            return None
        try:
            resolve_download(server_type, version)
            return version
        except ValueError as e:
            if str(e) != "version_unavailable":
                return None
        except Exception:
            return None
    return None
