#!/usr/bin/env python3
"""
Minecraft server lifecycle management module for saba-chan.

Provides complete server management:
  - Java environment detection and validation
  - EULA management
  - server.properties read/write
  - Error detection and diagnosis with solutions
  - Server launch command generation (for Rust managed process)
  - Graceful shutdown (the daemon sends `stop` to the console; this module falls back to SIGTERM)
  - Server List Ping for rich status info

Always outputs JSON to stdout, logs to stderr.
"""

import sys
import json
import locale
import subprocess
import os
import re
import shutil
import socket
import struct
import time
import hashlib
import tempfile
import zipfile
from pathlib import Path
from i18n import I18n

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))  # 같은 폴더의 server_types, plugins

import plugins
import server_types
from daemon_rcon import PathOutside, atomic_write, force_kill, is_process_alive, safe_join, wait_for_exit

# ─── Init ─────────────────────────────────────────────────────

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
i18n = I18n(MODULE_DIR)


def _progress(percent, message):
    """데몬이 읽는 진행률 줄 (stderr `PROGRESS:{json}`, GUI 설치 진행률)"""
    print("PROGRESS:" + json.dumps({"percent": percent, "message": message}, ensure_ascii=True), file=sys.stderr, flush=True)

# ─── Friendly name → server.properties key mapping ───────────
# Used by configure() and install_server() to translate settings.
_PROPERTY_KEY_MAP = {
    "accepts_transfers": "accepts-transfers",
    "allow_flight": "allow-flight",
    "allow_nether": "allow-nether",
    "broadcast_console_to_ops": "broadcast-console-to-ops",
    "bug_report_link": "bug-report-link",
    "difficulty": "difficulty",
    "enable_command_block": "enable-command-block",
    "enable_code_of_conduct": "enable-code-of-conduct",
    "enable_jmx_monitoring": "enable-jmx-monitoring",
    "enable_query": "enable-query",
    "enable_status": "enable-status",
    "enforce_secure_profile": "enforce-secure-profile",
    "enforce_whitelist": "enforce-whitelist",
    "entity_broadcast_range_percentage": "entity-broadcast-range-percentage",
    "force_gamemode": "force-gamemode",
    "function_permission_level": "function-permission-level",
    "gamemode": "gamemode",
    "generate_structures": "generate-structures",
    "generator_settings": "generator-settings",
    "hardcore": "hardcore",
    "hide_online_players": "hide-online-players",
    "initial_disabled_packs": "initial-disabled-packs",
    "initial_enabled_packs": "initial-enabled-packs",
    "level_name": "level-name",
    "level_seed": "level-seed",
    "level_type": "level-type",
    "log_ips": "log-ips",
    "max_chained_neighbor_updates": "max-chained-neighbor-updates",
    "max_players": "max-players",
    "max_tick_time": "max-tick-time",
    "max_world_size": "max-world-size",
    "motd": "motd",
    "network_compression_threshold": "network-compression-threshold",
    "online_mode": "online-mode",
    "op_permission_level": "op-permission-level",
    "player_idle_timeout": "player-idle-timeout",
    "pause_when_empty_seconds": "pause-when-empty-seconds",
    "port": "server-port",
    "prevent_proxy_connections": "prevent-proxy-connections",
    "pvp": "pvp",
    "query_port": "query.port",
    "debug": "debug",
    "management_server_enabled": "management-server-enabled",
    "management_server_host": "management-server-host",
    "management_server_port": "management-server-port",
    "management_server_secret": "management-server-secret",
    "management_server_allowed_origins": "management-server-allowed-origins",
    "management_server_tls_enabled": "management-server-tls-enabled",
    "management_server_tls_keystore": "management-server-tls-keystore",
    "management_server_tls_keystore_password": "management-server-tls-keystore-password",
    "rate_limit": "rate-limit",
    "region_file_compression": "region-file-compression",
    "require_resource_pack": "require-resource-pack",
    "resource_pack": "resource-pack",
    "resource_pack_id": "resource-pack-id",
    "resource_pack_prompt": "resource-pack-prompt",
    "resource_pack_sha1": "resource-pack-sha1",
    "server_ip": "server-ip",
    "simulation_distance": "simulation-distance",
    "spawn_animals": "spawn-animals",
    "spawn_monsters": "spawn-monsters",
    "spawn_npcs": "spawn-npcs",
    "spawn_protection": "spawn-protection",
    "status_heartbeat_interval": "status-heartbeat-interval",
    "sync_chunk_writes": "sync-chunk-writes",
    "text_filtering_config": "text-filtering-config",
    "text_filtering_version": "text-filtering-version",
    "use_native_transport": "use-native-transport",
    "view_distance": "view-distance",
    "white_list": "white-list",
}


# ╔═══════════════════════════════════════════════════════════╗
# ║                    Java Detection                         ║
# ╚═══════════════════════════════════════════════════════════╝

def required_java(mc_version):
    """Minecraft 버전에 필요한 최소 Java 주 버전 (서버 jar에 version.json이 없을 때의 폴백).
      연도 기반(26.1 등), 26년 이후 스냅샷 → 25
      1.20.5+ / 1.21+ / 그 전 스냅샷(24w14a 등) → 21
      1.18 ~ 1.20.4 → 17, 1.17 → 16, 그 이전 → 8, 알 수 없음 → 17
    """
    if not mc_version:
        return 17
    text = str(mc_version).strip()
    snapshot = re.match(r"^(\d{2})w\d{2}[a-z]$", text)
    if snapshot:
        return 25 if int(snapshot.group(1)) >= 26 else 21
    numbers = [int(n) for n in re.findall(r"\d+", text.split("-")[0])]
    if not numbers:
        return 17
    if numbers[0] != 1:
        return 25  # 연도 기반 버전 (예: 26.1)
    minor = numbers[1] if len(numbers) > 1 else 0
    patch = numbers[2] if len(numbers) > 2 else 0
    if minor >= 21 or (minor == 20 and patch >= 5):
        return 21
    if minor >= 18:
        return 17
    if minor == 17:
        return 16
    return 8


class JavaDetector:
    """Detect and validate Java installations."""

    WINDOWS_PATHS = [
        os.path.expandvars(r"%ProgramFiles%\Java"),
        os.path.expandvars(r"%ProgramFiles%\Eclipse Adoptium"),
        os.path.expandvars(r"%ProgramFiles%\Microsoft"),
        os.path.expandvars(r"%ProgramFiles%\Zulu"),
        os.path.expandvars(r"%ProgramFiles%\BellSoft"),
        os.path.expandvars(r"%LOCALAPPDATA%\Programs\Eclipse Adoptium"),
    ]

    UNIX_PATHS = [
        "/usr/bin/java",
        "/usr/lib/jvm",
        "/usr/local/bin/java",
        "/Library/Java/JavaVirtualMachines",
    ]

    # Oracle 설치기가 PATH 앞에 넣는 바로가기 폴더. 실제 런타임이 아니고, 32비트 Java 8을 가리키기도 한다
    SHIM_MARKERS = ("javapath", "java8path")
    # 후보 하나의 `-version` 제한과 전체 탐색 상한 (초)
    PROBE_TIMEOUT = 5
    SEARCH_BUDGET = 20

    @staticmethod
    def is_shim(path):
        parts = re.split(r"[\\/]+", str(path).lower())
        return any(marker in parts for marker in JavaDetector.SHIM_MARKERS)

    @staticmethod
    def daemon_runtimes():
        """데몬이 찾은 Java 런타임 (env `SABA_JAVA_RUNTIMES`, `[{java, home, version, major, arch}]`).
        각 JDK의 `release` 파일에서 읽은 값이라 `-version`을 돌리지 않아도 된다. 없거나 읽을 수 없으면 []"""
        try:
            runtimes = json.loads(os.environ.get("SABA_JAVA_RUNTIMES") or "[]")
        except ValueError:
            return []
        found = []
        for rt in runtimes if isinstance(runtimes, list) else []:
            if not isinstance(rt, dict) or not isinstance(rt.get("java"), str) or not isinstance(rt.get("major"), int):
                continue
            found.append({
                "path": rt["java"],
                "version": str(rt.get("version", rt["major"])),
                "major_version": rt["major"],
                "bits64": str(rt.get("arch", "")).lower() not in ("x86", "i386", "i586", "i686"),
            })
        return found

    @staticmethod
    def _choose(infos, min_major):
        """조건에 맞는 것 중 64비트를, 그중 필요한 버전을 넘는 가장 낮은 주 버전을 고른다.
        맞는 것이 없으면 가장 높은 버전을 돌려줘서 "Java N 이상 필요" 안내가 나오게 한다"""
        if not infos:
            return None
        prefer64 = sorted(infos, key=lambda i: not i.get("bits64", True))
        if min_major:
            enough = [i for i in prefer64 if i["major_version"] >= min_major]
            if enough:
                return min(enough, key=lambda i: (not i.get("bits64", True), i["major_version"]))
            return max(prefer64, key=lambda i: (i.get("bits64", True), i["major_version"]))
        return prefer64[0]

    @staticmethod
    def find_java(preferred_path=None, min_major=None):
        """
        Find a working Java executable.
        `min_major`가 있으면 그 주 버전 이상의 64비트 런타임을 고른다.
        Returns dict with path, version, major_version or None.
        """
        # 1. 사용자가 정한 경로는 그것만 본다
        if preferred_path and preferred_path != "java":
            return JavaDetector.get_java_info(preferred_path)

        # 2. 데몬이 찾은 런타임 (JAVA_HOME, PATH, 알려진 설치 위치)
        found = JavaDetector.daemon_runtimes()
        if found:
            return JavaDetector._choose(found, min_major)

        # 3. 데몬이 목록을 주지 않으면 직접 찾는다 (바로가기는 건너뛴다)
        candidates = []
        java_home = os.environ.get("JAVA_HOME")
        if java_home:
            exe = "java.exe" if os.name == "nt" else "java"
            candidates.append(os.path.join(java_home, "bin", exe))
        on_path = shutil.which("java")
        if on_path:
            candidates.append(on_path)
        if os.name == "nt":
            for base in JavaDetector.WINDOWS_PATHS:
                candidates.extend(JavaDetector._runtimes_under(os.path.expandvars(base), "java.exe"))
        else:
            for path in JavaDetector.UNIX_PATHS:
                if os.path.isfile(path):
                    candidates.append(path)
                else:
                    candidates.extend(JavaDetector._runtimes_under(path, "java"))

        deadline = time.monotonic() + JavaDetector.SEARCH_BUDGET
        seen, infos = set(), []
        for candidate in candidates:
            key = os.path.normcase(os.path.abspath(candidate))
            if key in seen or JavaDetector.is_shim(candidate):
                continue
            seen.add(key)
            if time.monotonic() > deadline:
                break
            info = JavaDetector.get_java_info(candidate)
            if not info:
                continue
            infos.append(info)
            if info.get("bits64", True) and (not min_major or info["major_version"] >= min_major):
                break
        return JavaDetector._choose(infos, min_major)

    @staticmethod
    def _runtimes_under(base, exe):
        """`<base>/<jdk>/bin/<exe>`와 `<base>/<jdk>/Contents/Home/bin/<exe>`(macOS)만 본다"""
        if not os.path.isdir(base):
            return []
        found = []
        try:
            homes = sorted(os.listdir(base))
        except OSError:
            return []
        for name in homes:
            for rel in (("bin", exe), ("Contents", "Home", "bin", exe)):
                path = os.path.join(base, name, *rel)
                if os.path.isfile(path):
                    found.append(path)
                    break
        return found

    @staticmethod
    def get_java_info(java_path):
        """Get Java version information. Returns dict or None."""
        try:
            creationflags = 0x08000000 if os.name == "nt" else 0
            result = subprocess.run(
                [java_path, "-version"],
                capture_output=True, text=True, timeout=JavaDetector.PROBE_TIMEOUT,
                creationflags=creationflags
            )
            output = result.stderr + result.stdout
            version_match = re.search(r'version "([^"]+)"', output)
            if version_match:
                version_str = version_match.group(1)
                major = JavaDetector.parse_major_version(version_str)
                return {
                    "path": java_path,
                    "version": version_str,
                    "major_version": major,
                    # 64비트 JVM은 `-version`에 "64-Bit"를 적는다 (HotSpot, OpenJ9)
                    "bits64": "64-bit" in output.lower(),
                    "raw_output": output.strip()
                }
        except (subprocess.TimeoutExpired, FileNotFoundError, OSError, PermissionError):
            pass
        return None

    @staticmethod
    def parse_major_version(version_str):
        """Extract major version number. '17.0.2' → 17, '1.8.0_362' → 8, '21-ea' → 21, 읽을 수 없으면 0"""
        numbers = re.findall(r"\d+", str(version_str))
        if not numbers:
            return 0
        major = int(numbers[0])
        if major == 1 and len(numbers) > 1:
            major = int(numbers[1])
        return major

    @staticmethod
    def validate_for_minecraft(java_info, mc_version=None, min_java=None):
        """Check if Java version meets Minecraft's minimum requirement.
        `min_java`(서버 jar의 version.json java_version)가 있으면 그 값을 쓴다."""
        issues = []
        if not java_info:
            issues.append({
                "code": "JAVA_NOT_FOUND",
                "severity": "critical",
                "message": i18n.t("errors.detect.java_not_found"),
                "solution": i18n.t("errors.solutions.java_not_found"),
            })
            return issues

        major = java_info.get("major_version", 0)
        if not isinstance(min_java, int) or min_java <= 0:
            min_java = required_java(mc_version)

        if major < min_java:
            issues.append({
                "code": "JAVA_VERSION_TOO_OLD",
                "severity": "critical",
                "message": i18n.t("errors.detect.java_version_too_old",
                                  current=str(major), required=str(min_java)),
                "solution": i18n.t("errors.solutions.java_version_too_old",
                                   required=str(min_java)),
            })
        return issues


# ╔═══════════════════════════════════════════════════════════╗
# ║               Java Auto-Download (Adoptium)               ║
# ╚═══════════════════════════════════════════════════════════╝

class JavaDownloader:
    """Download portable JRE from Eclipse Adoptium (Temurin) API.

    Downloads a JRE into <install_dir>/java/ so each server instance
    can have its own self-contained Java runtime without requiring
    a system-wide installation.
    """

    # Adoptium API v3 — returns the latest Temurin JRE for a given major version
    _API_BASE = "https://api.adoptium.net/v3"

    @staticmethod
    def _platform_info():
        """Detect OS and arch for Adoptium API query."""
        import platform as _platform
        system = _platform.system().lower()
        machine = _platform.machine().lower()

        if system == "windows":
            os_name = "windows"
        elif system == "linux":
            os_name = "linux"
        elif system == "darwin":
            os_name = "mac"
        else:
            os_name = system

        if machine in ("x86_64", "amd64"):
            arch = "x64"
        elif machine in ("aarch64", "arm64"):
            arch = "aarch64"
        else:
            arch = machine

        return os_name, arch

    @staticmethod
    def download_jre(major_version, install_dir):
        """Download and extract a portable JRE into <install_dir>/java/.

        Args:
            major_version: Java major version (e.g., 21, 17)
            install_dir: Server install directory

        Returns:
            dict with success, java_path, java_home, version
        """
        import urllib.request
        import tarfile
        import zipfile

        os_name, arch = JavaDownloader._platform_info()
        java_dir = os.path.join(install_dir, "java")

        # Check if already downloaded
        existing = JavaDownloader._find_bundled_java(java_dir)
        if existing:
            info = JavaDetector.get_java_info(existing)
            if info and info["major_version"] >= major_version:
                return {
                    "success": True,
                    "java_path": existing,
                    "java_home": java_dir,
                    "version": info["version"],
                    "already_exists": True,
                }

        # Determine archive type
        ext = "zip" if os_name == "windows" else "tar.gz"

        # Adoptium API: get latest release binary
        url = (
            f"{JavaDownloader._API_BASE}/binary/latest/{major_version}/ga/"
            f"{os_name}/{arch}/jre/hotspot/normal/eclipse"
            f"?project=jdk"
        )

        print(f"Downloading Java {major_version} JRE from Adoptium...", file=sys.stderr)
        print(f"  URL: {url}", file=sys.stderr)

        tmp_file = os.path.join(install_dir, f"java-jre.{ext}")

        try:
            req = urllib.request.Request(url, headers={"User-Agent": "saba-chan/1.0"})
            with urllib.request.urlopen(req, timeout=300) as resp:
                total = int(resp.headers.get("Content-Length", 0))
                downloaded = 0
                chunk_size = 256 * 1024
                last_pct = -1
                message = i18n.t("messages.downloading_java", version=str(major_version))

                with open(tmp_file, "wb") as f:
                    while True:
                        chunk = resp.read(chunk_size)
                        if not chunk:
                            break
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total > 0:
                            pct = downloaded * 100 // total
                            if pct != last_pct:
                                last_pct = pct
                                _progress(pct, message)

        except Exception as e:
            if os.path.exists(tmp_file):
                os.remove(tmp_file)
            return {"success": False, "message": i18n.t("errors.java_download_failed", error=str(e))}

        # Extract: 임시 폴더에 다 푼 뒤에만 java/를 바꾼다 (중간 실패가 반쯤 풀린 Java를 남기지 않게)
        final_dir = java_dir
        java_dir = final_dir + ".part"
        print(f"  Extracting to {final_dir}...", file=sys.stderr)
        shutil.rmtree(java_dir, ignore_errors=True)
        os.makedirs(java_dir, exist_ok=True)

        try:
            if ext == "zip":
                with zipfile.ZipFile(tmp_file) as zf:
                    # Adoptium zips have a top-level dir like "jdk-21.0.2+13-jre"
                    top_dirs = {name.split("/")[0] for name in zf.namelist() if "/" in name}
                    strip_prefix = top_dirs.pop() + "/" if len(top_dirs) == 1 else ""
                    for member in zf.infolist():
                        if member.is_dir():
                            continue
                        # Strip top-level directory
                        rel_path = member.filename
                        if strip_prefix and rel_path.startswith(strip_prefix):
                            rel_path = rel_path[len(strip_prefix):]
                        if not rel_path:
                            continue
                        dest = os.path.join(java_dir, rel_path)
                        os.makedirs(os.path.dirname(dest), exist_ok=True)
                        with zf.open(member) as src, open(dest, "wb") as dst:
                            dst.write(src.read())
            else:
                with tarfile.open(tmp_file, "r:gz") as tf:
                    # Same stripping for tar.gz
                    members = tf.getmembers()
                    if members:
                        top_dir = members[0].name.split("/")[0]
                        for member in members:
                            if member.name.startswith(top_dir + "/"):
                                member.name = member.name[len(top_dir) + 1:]
                            if member.name:
                                tf.extract(member, java_dir)
        except Exception as e:
            shutil.rmtree(java_dir, ignore_errors=True)
            return {"success": False, "message": i18n.t("errors.java_extract_failed", error=str(e))}
        finally:
            if os.path.exists(tmp_file):
                os.remove(tmp_file)

        # 새 JRE가 이 PC에서 도는지 먼저 본다: 돌지 않으면 원래 java/를 그대로 둔다
        java_exe = JavaDownloader._find_bundled_java(java_dir)
        if java_exe and os.name != "nt":
            import stat
            st = os.stat(java_exe)
            os.chmod(java_exe, st.st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        if not java_exe:
            shutil.rmtree(java_dir, ignore_errors=True)
            return {"success": False, "message": i18n.t("errors.java_executable_missing")}
        if not JavaDetector.get_java_info(java_exe):
            shutil.rmtree(java_dir, ignore_errors=True)
            return {"success": False, "message": i18n.t("errors.java_run_failed")}

        # 옛 java/는 먼저 옆으로 옮긴다: 쓰는 중이라 옮기지 못하면 손대지 않고 실패한다.
        # 새 것을 들이지 못하면 옛 것을 되돌린다
        old_dir = final_dir + ".old"
        try:
            shutil.rmtree(old_dir, ignore_errors=True)
            had_old = os.path.exists(final_dir)
            if had_old:
                os.replace(final_dir, old_dir)
            try:
                os.replace(java_dir, final_dir)
            except OSError:
                if had_old:
                    os.replace(old_dir, final_dir)
                raise
        except OSError as e:
            shutil.rmtree(java_dir, ignore_errors=True)
            return {"success": False, "message": i18n.t("errors.java_extract_failed", error=str(e))}
        shutil.rmtree(old_dir, ignore_errors=True)
        java_dir = final_dir
        java_exe = JavaDownloader._find_bundled_java(java_dir)
        info = JavaDetector.get_java_info(java_exe) if java_exe else None
        if not info:
            return {"success": False, "message": i18n.t("errors.java_run_failed")}

        print(f"  Java {info['version']} ready at {java_exe}", file=sys.stderr)

        return {
            "success": True,
            "java_path": java_exe,
            "java_home": java_dir,
            "version": info["version"],
            "major_version": info["major_version"],
        }

    @staticmethod
    def bundled_major(java_dir, java_exe):
        """번들 Java의 주 버전: JRE의 `release` 파일(실행하지 않는다), 없으면 `-version`. 모르면 0"""
        try:
            with open(os.path.join(java_dir, "release"), encoding="utf-8", errors="replace") as f:
                m = re.search(r'^JAVA_VERSION="([^"]+)"', f.read(), re.M)
            if m:
                return JavaDetector.parse_major_version(m.group(1))
        except OSError:
            pass
        info = JavaDetector.get_java_info(java_exe)
        return info["major_version"] if info else 0

    @staticmethod
    def _find_bundled_java(java_dir):
        """Find java executable inside a bundled JRE directory."""
        if not os.path.isdir(java_dir):
            return None
        exe_name = "java.exe" if os.name == "nt" else "java"
        candidate = os.path.join(java_dir, "bin", exe_name)
        if os.path.isfile(candidate):
            return candidate
        # Search recursively (some extractions may have nested dirs)
        for root, _dirs, files in os.walk(java_dir):
            if exe_name in files:
                return os.path.join(root, exe_name)
        return None


# ╔═══════════════════════════════════════════════════════════╗
# ║                EULA Management                            ║
# ╚═══════════════════════════════════════════════════════════╝

class EulaManager:
    """Handle Minecraft Server EULA."""

    @staticmethod
    def check_eula(working_dir):
        """Check if EULA is accepted. Returns (accepted: bool, path: str)."""
        eula_path = os.path.join(working_dir, "eula.txt")
        if not os.path.exists(eula_path):
            return False, eula_path
        try:
            with open(eula_path, "r", encoding="utf-8") as f:
                content = f.read()
            if re.search(r"eula\s*=\s*true", content, re.IGNORECASE):
                return True, eula_path
        except OSError:
            pass
        return False, eula_path

    @staticmethod
    def accept_eula(working_dir):
        """Write eula=true to eula.txt."""
        eula_path = os.path.join(working_dir, "eula.txt")
        try:
            content = (
                "#By changing the setting below to TRUE you are indicating "
                "your agreement to our EULA (https://aka.ms/MinecraftEULA).\n"
                f"#Generated by saba-chan on {time.strftime('%c')}\n"
                "eula=true\n"
            )
            atomic_write(eula_path, content.encode("utf-8"))
            return True
        except OSError:
            return False


# ╔═══════════════════════════════════════════════════════════╗
# ║             server.properties Manager                     ║
# ╚═══════════════════════════════════════════════════════════╝

def _load_default_properties():
    """Load vanilla defaults from server.properties reference file,
    then apply saba-chan-specific overrides (RCON stays off: commands go through the console)."""
    defaults = {}
    ref_path = os.path.join(MODULE_DIR, "server.properties")
    if os.path.isfile(ref_path):
        try:
            with open(ref_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if "=" in line:
                        key, _, value = line.partition("=")
                        defaults[key.strip()] = value.strip()
        except OSError:
            pass

    # saba-chan overrides: RCON is blocked for security
    defaults["enable-rcon"] = "false"
    defaults.setdefault("pause-when-empty-seconds", "60")
    return defaults


DEFAULT_PROPERTIES = _load_default_properties()


class ServerPropertiesManager:
    """Read/write Minecraft server.properties files."""

    def __init__(self, working_dir):
        self.path = os.path.join(working_dir, "server.properties")

    def exists(self):
        return os.path.isfile(self.path)

    def _lines(self):
        """파일의 줄. UTF-8이 아니면(ANSI 편집기로 저장한 cp949 등) cp949·시스템 로캘·latin-1 순서로 읽는다.
        cp949가 로캘보다 먼저다: 영문 Windows의 cp1252는 거의 모든 바이트를 받아 한글을 깨진 글자로 읽는다"""
        with open(self.path, "rb") as f:
            raw = f.read()
        for encoding in ("utf-8-sig", "cp949", locale.getpreferredencoding(False), "latin-1"):
            try:
                return raw.decode(encoding).splitlines(keepends=True)
            except (UnicodeDecodeError, LookupError):
                continue
        return []

    def read(self):
        """Parse server.properties into a dict."""
        props = {}
        if not self.exists():
            return props
        try:
            for line in self._lines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, _, value = line.partition("=")
                    props[key.strip()] = value.strip()
        except OSError:
            pass
        return props

    def write(self, properties):
        """Write a full properties dict (replaces entire file)."""
        header = (
            "#Minecraft server properties\n"
            f"#Generated by saba-chan on {time.strftime('%c')}\n"
        )
        body = "".join(f"{key}={value}\n" for key, value in sorted(properties.items()))
        try:
            atomic_write(self.path, (header + body).encode("utf-8"))
            return True
        except OSError:
            return False

    def update(self, changes):
        """Merge changes into existing properties (preserves comments)."""
        if not self.exists():
            merged = dict(DEFAULT_PROPERTIES)
            merged.update(changes)
            return self.write(merged)

        try:
            lines = []
            updated_keys = set()
            for line in self._lines():
                stripped = line.strip()
                if stripped and not stripped.startswith("#") and "=" in stripped:
                    key = stripped.partition("=")[0].strip()
                    if key in changes:
                        lines.append(f"{key}={changes[key]}\n")
                        updated_keys.add(key)
                    else:
                        lines.append(line)
                else:
                    lines.append(line)

            for key, value in changes.items():
                if key not in updated_keys:
                    lines.append(f"{key}={value}\n")

            atomic_write(self.path, "".join(lines).encode("utf-8"))
            return True
        except OSError:
            return False

    def get_defaults(self):
        return dict(DEFAULT_PROPERTIES)


# ╔═══════════════════════════════════════════════════════════╗
# ║                Error Detection                            ║
# ╚═══════════════════════════════════════════════════════════╝

ERROR_PATTERNS = [
    {
        "code": "JAVA_NOT_FOUND",
        "patterns": [
            r"'java' is not recognized",
            r"java: not found",
            r"No such file or directory.*java",
            r"The system cannot find the file specified",
        ],
        "severity": "critical",
    },
    {
        "code": "JAVA_VERSION_TOO_OLD",
        "patterns": [
            r"UnsupportedClassVersionError",
            r"class file version \d+\.\d+",
            r"requires Java \d+",
            r"has been compiled by a more recent version",
        ],
        "severity": "critical",
    },
    {
        "code": "EULA_NOT_ACCEPTED",
        "patterns": [
            r"You need to agree to the EULA",
            r"Failed to load eula",
            r"Go to eula\.txt",
        ],
        "severity": "critical",
    },
    {
        "code": "PORT_IN_USE",
        "patterns": [
            r"FAILED TO BIND TO PORT",
            r"Address already in use",
            r"Perhaps a server is already running on that port",
            r"java\.net\.BindException",
        ],
        "severity": "critical",
    },
    {
        "code": "OUT_OF_MEMORY",
        "patterns": [
            r"java\.lang\.OutOfMemoryError",
            r"There is insufficient memory",
            r"Could not reserve enough space",
            r"GC overhead limit exceeded",
        ],
        "severity": "critical",
    },
    {
        "code": "WORLD_CORRUPT",
        "patterns": [
            r"Failed to load.*level\.dat",
            r"Caused by: java\.util\.zip\.ZipException",
            r"Region file is truncated",
        ],
        "severity": "error",
    },
    {
        "code": "INVALID_JAR",
        "patterns": [
            r"Invalid or corrupt jarfile",
            r"Error: Unable to access jarfile",
            r"Could not find or load main class",
        ],
        "severity": "critical",
    },
    {
        "code": "PERMISSION_DENIED",
        "patterns": [
            r"Permission denied",
            r"Access is denied",
        ],
        "severity": "critical",
    },
    {
        "code": "SERVER_OVERLOADED",
        "patterns": [
            r"Can't keep up! Is the server overloaded\?",
        ],
        "severity": "warning",
    },
]


class ErrorDetector:
    """Detect and diagnose common Minecraft server errors in log output."""

    @staticmethod
    def diagnose(log_lines):
        """Analyze log lines for known error patterns. Returns list of issues."""
        issues = []
        seen_codes = set()
        if isinstance(log_lines, str):
            log_lines = log_lines.splitlines()

        for line in log_lines:
            for pdef in ERROR_PATTERNS:
                if pdef["code"] in seen_codes:
                    continue
                for regex in pdef["patterns"]:
                    if re.search(regex, line, re.IGNORECASE):
                        code = pdef["code"]
                        seen_codes.add(code)
                        issues.append({
                            "code": code,
                            "severity": pdef["severity"],
                            "matched_line": line.strip()[:200],
                            "message": i18n.t(f"errors.detect.{code.lower()}"),
                            "solution": i18n.t(f"errors.solutions.{code.lower()}"),
                        })
                        break
        return issues

    @staticmethod
    def diagnose_startup_failure(exit_code, stderr_text, working_dir):
        """Diagnose why a server failed to start."""
        issues = ErrorDetector.diagnose(stderr_text)

        eula_accepted, _ = EulaManager.check_eula(working_dir)
        if not eula_accepted:
            if not any(i["code"] == "EULA_NOT_ACCEPTED" for i in issues):
                issues.append({
                    "code": "EULA_NOT_ACCEPTED",
                    "severity": "critical",
                    "matched_line": "",
                    "message": i18n.t("errors.detect.eula_not_accepted"),
                    "solution": i18n.t("errors.solutions.eula_not_accepted"),
                })

        log_path = os.path.join(working_dir, "logs", "latest.log")
        if os.path.isfile(log_path):
            try:
                with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                    recent = f.readlines()[-200:]
                for issue in ErrorDetector.diagnose(recent):
                    if not any(i["code"] == issue["code"] for i in issues):
                        issues.append(issue)
            except OSError:
                pass

        return issues


# ╔═══════════════════════════════════════════════════════════╗
# ║            Minecraft Server List Ping                     ║
# ╚═══════════════════════════════════════════════════════════╝

class MinecraftPing:
    """Minecraft Server List Ping (SLP) protocol."""

    @staticmethod
    def ping(host="127.0.0.1", port=25565, timeout=3):
        """Ping a server and get status dict (players, version, motd) or None."""
        try:
            # 격리 실행(미확인 모듈)은 127.0.0.1에 닿지 못한다: 데몬이 이 인스턴스 포트로 대신 보낸다
            if os.environ.get("SABA_LOOPBACK_DIR") and host in ("127.0.0.1", "localhost"):
                from daemon_rcon import loopback_exchange
                data = loopback_exchange(port, MinecraftPing._request(host, port), timeout=timeout, idle_ms=300)
                return MinecraftPing._parse_response(data)
            # with 블록이라 오류가 나도 소켓이 닫힌다
            with socket.create_connection((host, port), timeout=timeout) as sock:
                return MinecraftPing._exchange(sock, host, port)
        except (socket.timeout, socket.error, OSError, ImportError, ValueError):
            return None

    @staticmethod
    def _exchange(sock, host, port):
        """핸드셰이크와 상태 요청을 보내고 응답을 읽는다."""
        sock.sendall(MinecraftPing._request(host, port))

        # Read response
        data = b""
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            data += chunk
            try:
                parsed = MinecraftPing._parse_response(data)
                if parsed is not None:
                    return parsed
            except Exception:
                continue

        return None

    @staticmethod
    def _request(host, port):
        """핸드셰이크와 상태 요청 바이트"""
        # Handshake
        protocol_version = MinecraftPing._encode_varint(-1)
        host_bytes = host.encode("utf-8")
        host_len = MinecraftPing._encode_varint(len(host_bytes))
        port_bytes = struct.pack(">H", port)
        next_state = MinecraftPing._encode_varint(1)

        handshake_data = (
            MinecraftPing._encode_varint(0x00)
            + protocol_version + host_len + host_bytes + port_bytes + next_state
        )
        # Status request
        return (MinecraftPing._encode_varint(len(handshake_data)) + handshake_data
                + MinecraftPing._encode_varint(1) + MinecraftPing._encode_varint(0x00))

    @staticmethod
    def _encode_varint(value):
        if value < 0:
            value += 1 << 32
        result = b""
        while True:
            byte = value & 0x7F
            value >>= 7
            if value != 0:
                byte |= 0x80
            result += struct.pack("B", byte)
            if value == 0:
                break
        return result

    @staticmethod
    def _read_varint(data, offset=0):
        result = 0
        shift = 0
        while True:
            if offset >= len(data):
                raise ValueError("VarInt too short")
            byte = data[offset]
            offset += 1
            result |= (byte & 0x7F) << shift
            if not (byte & 0x80):
                break
            shift += 7
        if result & (1 << 31):
            result -= 1 << 32
        return result, offset

    @staticmethod
    def _parse_response(data):
        try:
            pkt_len, offset = MinecraftPing._read_varint(data, 0)
            if len(data) < offset + pkt_len:
                return None
            pkt_id, offset = MinecraftPing._read_varint(data, offset)
            if pkt_id != 0x00:
                return None
            json_len, offset = MinecraftPing._read_varint(data, offset)
            return json.loads(data[offset:offset + json_len].decode("utf-8"))
        except (ValueError, json.JSONDecodeError, UnicodeDecodeError):
            return None


# ╔═══════════════════════════════════════════════════════════╗
# ║                Lifecycle Functions                        ║
# ╚═══════════════════════════════════════════════════════════╝


def _resolve_server_jar(config):
    """Resolve the server jar path from config.

    Checks keys in order: server_executable → executable_path → server_jar (예전 데몬이 넘기던 이름).
    If the path is relative, resolves it against working_dir (if set).
    Returns an absolute path string or None.
    """
    jar = (
        config.get("server_executable")
        or config.get("executable_path")
        or config.get("server_jar")
    )
    if not jar:
        return None

    # If relative, resolve against working_dir
    if not os.path.isabs(jar):
        working_dir = config.get("working_dir", "")
        if working_dir:
            jar = os.path.join(working_dir, jar)

    jar = os.path.abspath(jar)
    if not os.path.isfile(jar) and os.path.basename(jar).lower() == "server.jar":
        # 기본 이름(server.jar)이면 — 페이퍼·퍼퍼·패브릭 등은 흔한 이름으로 폴더에서 찾는다.
        # 사용자가 직접 적은 다른 이름은 없으면 없는 대로 둔다(다른 jar로 바꿔 띄우지 않는다)
        jar = server_types.find_server_jar(os.path.dirname(jar)) or jar
    return jar


def _java_memory(value):
    """ram 설정 → Java 힙 크기. "4G"·"1024M"·"512k"는 그대로, 예전에 저장된 숫자(GB)는 G로
    (소수는 M으로: 0.5 → 512M). 읽을 수 없으면 2G"""
    text = str(value).strip()
    if re.fullmatch(r"\d+[KMGkmg]", text):
        return text
    try:
        gb = float(text)
    except (ValueError, TypeError):
        return "2G"
    if gb <= 0:
        return "2G"
    return f"{int(gb)}G" if gb == int(gb) else f"{int(gb * 1024)}M"


def _required_java_for(config, server_jar, jar_info=None):
    """이 서버에 필요한 Java 주 버전: 서버 jar의 version.json java_version, 없으면 Minecraft 버전으로 추정"""
    if jar_info is None and server_jar:
        jar_info = _read_jar_version(*os.path.split(server_jar))
    min_java = (jar_info or {}).get("java_version")
    if isinstance(min_java, int) and min_java > 0:
        return min_java
    return required_java(_minecraft_version(config, server_jar, jar_info))


def _minecraft_version(config, server_jar, jar_info=None):
    """설정의 server_version, 없으면 서버 jar, 그것도 없으면 서버 폴더(인자 파일로 뜨는 Forge/NeoForge의 로더 버전)"""
    version = config.get("server_version") or (jar_info or {}).get("version")
    if version or not config.get("working_dir"):
        return version
    return server_types.detect(config["working_dir"], server_jar).get("version")


def _launch_java(config, server_jar, jar_info=None):
    """실제로 띄울 Java의 경로 (`_launch_java_info`). 찾지 못하면 설정의 java_path"""
    info = _launch_java_info(config, server_jar, jar_info)
    return info["path"] if info else config.get("java_path", "java")


def _launch_java_info(config, server_jar, jar_info=None):
    """실제로 띄울 Java와 그 버전: 사용자가 정한 java_path, 포터블 Java(<working_dir>/java/, <instance_dir>/java/), 시스템 Java 순이다.
    시스템 Java는 이 서버에 필요한 주 버전 이상인 것을 고른다. 번들과 데몬이 찾은 런타임은 `-version`을 돌리지 않는다
    (격리 안에서는 돌지 않을 수 있다). 찾지 못하면 None"""
    java_path = config.get("java_path") or "java"
    if java_path != "java":
        return JavaDetector.get_java_info(java_path)
    required = _required_java_for(config, server_jar, jar_info)
    bundled = _portable_java(config, server_jar, required)
    if bundled:
        return bundled
    return JavaDetector.find_java(java_path, min_major=required)


def _portable_java(config, server_jar, required):
    """이 서버에 맞는 포터블 Java: <working_dir>/java/(새로 설치한 서버), <instance_dir>/java/(가져온 서버) 순. 없으면 None.
    예전 버전용으로 받아 둔 것이 이 서버에 낮으면 쓰지 않는다"""
    wd = config.get("working_dir") or (os.path.dirname(os.path.abspath(server_jar)) if server_jar else "")
    for base in (wd, config.get("instance_dir")):
        if not base:
            continue
        java_dir = os.path.join(base, "java")
        bundled = JavaDownloader._find_bundled_java(java_dir)
        major = JavaDownloader.bundled_major(java_dir, bundled) if bundled else 0
        if bundled and major >= required:
            return {"path": bundled, "version": str(major), "major_version": major, "bits64": True}
    return None


def _fetch_portable_java(config):
    """가져온 서버에 맞는 포터블 JRE를 인스턴스 폴더에 받는다 (PC에 깔린 Java에 기대지 않고, 서버 폴더에는 더하지 않는다).
    이미 있으면 받지 않는다. 받지 못하면 {java_warning} (가져오기는 계속하고 시작할 때 Java 문제로 안내한다)"""
    instance_dir = config.get("instance_dir")
    if not instance_dir or (config.get("java_path") or "java") != "java":
        return {}
    server_jar = _resolve_server_jar(config)
    required = _required_java_for(config, server_jar)
    if not required or _portable_java(config, server_jar, required):
        return {}
    result = JavaDownloader.download_jre(required, instance_dir)
    if result.get("success"):
        return {}
    return {"java_warning": i18n.t("errors.java_download_warning", version=str(required), error=result.get("message", ""))}


def validate(config):
    """
    Validate all prerequisites before starting.
    Checks: Java, server jar, EULA, working dir, port availability.
    """
    issues = []
    server_jar = _resolve_server_jar(config)
    working_dir = config.get("working_dir", "")

    # Java: 띄울 Java로 검사한다. 필요한 버전은 서버 jar의 version.json(java_version, id)을 먼저 본다
    jar_info = _read_jar_version(*os.path.split(server_jar)) if server_jar else None
    java_info = _launch_java_info(config, server_jar, jar_info)
    mc_version = _minecraft_version(config, server_jar, jar_info)
    issues.extend(JavaDetector.validate_for_minecraft(
        java_info, mc_version, min_java=(jar_info or {}).get("java_version")))

    # Server JAR (인자 파일로 뜨는 Forge/NeoForge는 jar가 없어도 된다)
    launchable, _ = server_types.launch_target(config, server_jar, working_dir)
    if launchable:
        pass
    elif not server_jar:
        issues.append({
            "code": "NO_SERVER_JAR", "severity": "critical",
            "message": i18n.t("errors.server_jar_not_specified"),
            "solution": i18n.t("errors.solutions.server_jar_not_specified"),
        })
    elif not os.path.isfile(server_jar):
        issues.append({
            "code": "JAR_NOT_FOUND", "severity": "critical",
            "message": i18n.t("errors.server_jar_not_found", path=server_jar),
            "solution": i18n.t("errors.solutions.server_jar_not_found"),
        })

    # 고른 서버 종류와 실제 서버 파일이 다르면 알린다 (설정만 바꾸고 다시 설치하지 않은 경우)
    detected = server_types.detect(working_dir, server_jar) if working_dir else {}
    chosen = config.get("server_type") or "vanilla"
    if detected.get("type") and detected["type"] != chosen:
        issues.append({
            "code": "SERVER_TYPE_MISMATCH", "severity": "warning",
            "message": i18n.t("errors.server_type_mismatch", chosen=chosen, detected=detected["type"]),
            "solution": i18n.t("errors.solutions.server_type_mismatch"),
        })

    # Working directory
    if working_dir and not os.path.isdir(working_dir):
        try:
            os.makedirs(working_dir, exist_ok=True)
        except OSError:
            issues.append({
                "code": "WORKING_DIR_ERROR", "severity": "critical",
                "message": i18n.t("errors.working_dir_error", path=working_dir),
                "solution": i18n.t("errors.solutions.working_dir_error"),
            })

    # EULA
    actual_working = working_dir or (os.path.dirname(server_jar) if server_jar else "")
    eula_accepted = False
    if actual_working:
        eula_accepted, _ = EulaManager.check_eula(actual_working)
        if not eula_accepted:
            issues.append({
                "code": "EULA_NOT_ACCEPTED", "severity": "critical",
                "message": i18n.t("errors.detect.eula_not_accepted"),
                "solution": i18n.t("errors.solutions.eula_not_accepted"),
            })

    # Port availability — 이 서버가 이미 켜져 있으면 그 포트는 자기 것이다
    port = config.get("port", 25565)
    if port and not config.get("running"):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(1)
                if s.connect_ex(("127.0.0.1", int(port))) == 0:
                    issues.append({
                        "code": "PORT_IN_USE", "severity": "warning",
                        "message": i18n.t("errors.detect.port_in_use", port=str(port)),
                        "solution": i18n.t("errors.solutions.port_in_use", port=str(port)),
                    })
        except (OSError, ValueError):
            pass

    return {
        "success": len([i for i in issues if i["severity"] == "critical"]) == 0,
        "issues": issues,
        "java_info": java_info,
        "eula_accepted": eula_accepted,
    }


def _enforce_rcon_policy(working_dir, config=None):
    """
    Keep RCON off before every launch, in every mode.

    saba-chan blocks RCON for security: commands reach the server through its console (stdin),
    which saba-runner keeps open across daemon restarts. The daemon's RCON password/port are not synced.

    Returns dict with details of what was changed, or None if server.properties does not exist yet.
    """
    mgr = ServerPropertiesManager(working_dir)
    if not mgr.exists():
        return None  # server.properties will be generated on first run (defaults keep RCON off)

    props = mgr.read()
    if props.get("enable-rcon", "false").lower() == "false":
        return {"changed": False}
    mgr.update({"enable-rcon": "false"})
    print("[Minecraft] RCON turned off (saba-chan sends commands through the console)", file=sys.stderr)
    return {"changed": True, "changes": {"enable-rcon": "false"}}


def get_launch_command(config):
    """
    Build the command for the Rust daemon to spawn as a ManagedProcess.
    Returns { success, program, args, working_dir, env_vars }.
    """
    java_path = config.get("java_path", "java")
    server_jar = _resolve_server_jar(config)
    ram = _java_memory(config.get("ram", "2G"))
    working_dir = config.get("working_dir")
    extra_jvm_args = config.get("jvm_args", [])

    if not working_dir and server_jar:
        working_dir = os.path.dirname(os.path.abspath(server_jar))

    # 실행 대상: 대부분 `-jar <서버 jar> nogui`, 1.17+ Forge/NeoForge는 인자 파일 (server_types)
    target, _ = server_types.launch_target(config, server_jar, working_dir)
    if target is None and not server_jar:
        return {
            "success": False,
            "action_required": "executable_not_found",
            "message": i18n.t("errors.server_jar_not_specified"),
            "options": ["update_path", "install_new"],
        }

    if target is None:
        return {
            "success": False,
            "action_required": "executable_not_found",
            "message": i18n.t("errors.server_jar_not_found", path=server_jar),
            "configured_path": server_jar,
            "options": ["update_path", "install_new"],
        }

    java_path = _launch_java(config, server_jar)

    args = [f"-Xmx{ram}", f"-Xms{ram}"]

    # Aikar's flags (recommended for modern MC servers)
    if config.get("use_aikar_flags", False):
        args.extend([
            "-XX:+UseG1GC", "-XX:+ParallelRefProcEnabled",
            "-XX:MaxGCPauseMillis=200", "-XX:+UnlockExperimentalVMOptions",
            "-XX:+DisableExplicitGC", "-XX:+AlwaysPreTouch",
            "-XX:G1NewSizePercent=30", "-XX:G1MaxNewSizePercent=40",
            "-XX:G1HeapRegionSize=8M", "-XX:G1ReservePercent=20",
            "-XX:G1HeapWastePercent=5", "-XX:G1MixedGCCountTarget=4",
            "-XX:InitiatingHeapOccupancyPercent=15",
            "-XX:G1MixedGCLiveThresholdPercent=90",
            "-XX:G1RSetUpdatingPauseTimePercent=5",
            "-XX:SurvivorRatio=32", "-XX:+PerfDisableSharedMem",
            "-XX:MaxTenuringThreshold=1",
        ])

    if isinstance(extra_jvm_args, list):
        args.extend(extra_jvm_args)
    elif isinstance(extra_jvm_args, str):
        args.extend(extra_jvm_args.split())

    # 사용자가 더한 실행 인자(데몬의 extra_args)는 JVM 인자 자리, 실행 대상(-jar 등) 앞에 넣는다
    extra_args = config.get("extra_args") or []
    if isinstance(extra_args, list):
        args.extend(str(a) for a in extra_args)

    args.extend(target)

    abs_working_dir = os.path.abspath(working_dir)

    # 미리보기(preview)는 아무 파일도 고치지 않는다
    if not config.get("preview"):
        _enforce_rcon_policy(abs_working_dir, config=config)

    return {
        "success": True,
        "program": java_path,
        "args": args,
        "working_dir": abs_working_dir,
        "env_vars": {},
        "extra_args_placed": True,
    }


def configure(config):
    """Apply settings to server.properties."""
    working_dir = config.get("working_dir")
    if not working_dir:
        return {"success": False, "message": i18n.t("errors.no_working_dir")}

    settings = config.get("settings", {})
    if not settings:
        return {"success": False, "message": i18n.t("errors.no_settings")}

    mgr = ServerPropertiesManager(working_dir)

    props_changes = {}
    for key, value in settings.items():
        # server.properties 키가 아닌 것(ram, java_path 같은 사바쨩 설정)은 쓰지 않는다
        prop_key = _PROPERTY_KEY_MAP.get(key)
        if prop_key is None:
            continue
        if isinstance(value, bool):
            value = "true" if value else "false"
        props_changes[prop_key] = str(value)

    success = mgr.update(props_changes)
    return {
        "success": success,
        "message": i18n.t("messages.properties_updated") if success
                   else i18n.t("errors.properties_write_failed"),
        "updated_keys": list(props_changes.keys()),
    }


# 가져온 서버가 쓰던 실행 스크립트 (Forge·NeoForge 설치기는 메모리를 user_jvm_args.txt에 둔다)
_START_SCRIPTS = ("user_jvm_args.txt", "run.bat", "start.bat", "run.sh", "start.sh")


def _script_memory(working_dir):
    """쓰던 실행 스크립트의 -Xmx ("10G", "3072M"). 없으면 None (모듈 기본값 그대로)"""
    for name in _START_SCRIPTS:
        try:
            with open(os.path.join(working_dir, name), encoding="utf-8", errors="replace") as f:
                text = f.read()
        except OSError:
            continue
        for line in text.splitlines():
            if line.lstrip().startswith(("#", "rem ", "REM ", "::")):
                continue
            m = re.search(r"(?<!\S)-Xmx(\d+[KMGkmg])(?!\S)", line)
            if m:
                return m.group(1).upper()
    return None


def import_settings(config):
    """Read server.properties and return settings in saba-chan key format.

    Used during migration to import existing server settings into saba-chan.
    서버 종류(바닐라·페이퍼·패브릭…)와 설치된 플러그인·모드도 jar 안의 메타데이터로 판정해
    `settings.server_type`, `executable_path`(작업 폴더 기준 서버 jar), `detected`로 돌려준다.
    """
    working_dir = config.get("working_dir") or ""
    detected = server_types.detect(working_dir, _resolve_server_jar(config))
    found = {}
    if detected.get("type"):
        found["server_type"] = detected["type"]
    extra = {"detected": {
        "server_type": detected.get("type"),
        "version": detected.get("version"),
        "evidence": detected.get("evidence", []),
        "components": plugins.instance_commands(working_dir)["components"],
    }}
    if detected.get("jar"):
        extra["executable_path"] = detected["jar"]

    ram = _script_memory(working_dir)
    if ram:
        found["ram"] = ram
    extra.update(_fetch_portable_java(config))

    result = read_properties(config)
    if not result.get("success"):
        return result

    raw_props = result.get("properties", {})
    if not raw_props:
        return {"success": True, "settings": found, "message": i18n.t("messages.no_properties_found"), **extra}

    # Build reverse map: server.properties key → saba-chan key
    reverse_map = {v: k for k, v in _PROPERTY_KEY_MAP.items()}

    # 스키마 타입대로만 바꾼다: 숫자 같은 비밀번호("000123")나 이름은 문자열 그대로
    from daemon_rcon import coerce_setting, load_setting_types
    types = load_setting_types(os.path.dirname(os.path.abspath(__file__)))
    settings = {}
    for prop_key, raw_value in raw_props.items():
        saba_key = reverse_map.get(prop_key, prop_key)
        settings[saba_key] = coerce_setting(raw_value, types.get(saba_key))
    settings.update(found)

    return {"success": True, "settings": settings, **extra}


def config_files(config):
    """사바쨩이 고칠 수 있는 게임 설정 파일 (가져온 서버를 해제할 때 데몬이 가져오기 전 내용으로 되돌린다)"""
    working_dir = config.get("working_dir")
    if not working_dir:
        return {"success": False, "message": i18n.t("errors.no_working_dir")}
    return {"success": True, "files": [os.path.join(working_dir, "server.properties"), os.path.join(working_dir, "eula.txt")]}


def instance_commands(config):
    """이 서버에 설치된 플러그인·모드가 콘솔에서 받는 명령 (데몬 `[commands] hook = true`)

    돌려주는 값: {success, commands: [명령 필드 + group], components: [판정한 플러그인·모드]}
    """
    working_dir = config.get("working_dir")
    if not working_dir:
        return {"success": False, "message": i18n.t("errors.no_working_dir")}
    return {"success": True, **plugins.instance_commands(working_dir)}


def read_properties(config):
    """Read current server.properties."""
    working_dir = config.get("working_dir")
    if not working_dir:
        return {"success": False, "message": i18n.t("errors.no_working_dir")}

    mgr = ServerPropertiesManager(working_dir)
    if not mgr.exists():
        return {
            "success": True, "exists": False,
            "properties": mgr.get_defaults(),
            "message": i18n.t("messages.properties_using_defaults"),
        }
    return {"success": True, "exists": True, "properties": mgr.read()}


def reset_properties(config):
    """Reset server.properties to default values."""
    working_dir = config.get("working_dir")
    if not working_dir:
        return {"success": False, "message": i18n.t("errors.no_working_dir")}

    mgr = ServerPropertiesManager(working_dir)
    defaults = mgr.get_defaults()
    success = mgr.write(defaults)
    return {
        "success": success,
        "message": i18n.t("messages.properties_reset") if success
                   else i18n.t("errors.properties_write_failed"),
    }


# 서버 초기화가 지우는 것: 서버(바닐라·Bukkit 계열·모드 로더)가 서버 폴더에 만드는 월드 밖 항목
_RESET_FILES = (
    "server.properties", "ops.json", "whitelist.json", "banned-players.json", "banned-ips.json",
    "usercache.json", "version_history.json", "permissions.yml", "commands.yml", "help.yml",
    "bukkit.yml", "spigot.yml", "paper.yml", "purpur.yml", "wepif.yml", ".console_history",
)
_RESET_DIRS = ("logs", "crash-reports", "plugins", "mods", "config", "cache", "debug")


# 초기화가 지우지 않는 것: 서버를 띄우는 파일·폴더(.jar도)와 인스턴스 폴더의 데몬 파일
_RESET_KEEP = {
    "eula.txt", "user_jvm_args.txt", "run.bat", "run.sh",
    "fabric-server-launcher.properties", "quilt-server-launcher.properties",
    "java", "libraries", "versions", ".fabric", ".quilt",
    "instance.json", "settings.json", "settings.json.tmp", "managed.pid", "run", "backups", "rcon.json", "rcon.json.tmp", "migration",
}


def _unescape_property(value):
    """Java .properties 값의 이스케이프를 푼다 (\\uXXXX, \\:, \\= 등)"""
    def repl(m):
        s = m.group(1)
        if len(s) == 5:
            return chr(int(s[1:], 16))
        return {"t": "\t", "n": "\n", "r": "\r", "f": "\f"}.get(s, s)
    return re.sub(r"\\(u[0-9a-fA-F]{4}|.)", repl, value)


def _reset_world_paths(config, props, working_path, keep):
    """지울 월드 폴더(서버 폴더 기준 상대 경로)와 다른 차원 폴더. level-name이 폴더 안을 가리키지 않거나
    남길 항목과 겹치면 None"""
    name = _unescape_property(props.get("level-name", "")) or str(config.get("level_name") or "") or "world"
    parts = re.split(r"[\\/]", name)
    if os.path.isabs(name) or any(p in ("", ".", "..") or ":" in p or p[-1] in " ." for p in parts):
        return None
    if parts[0].casefold() in keep or parts[0].casefold().endswith(".jar"):
        return None
    base = working_path.resolve()
    world = "/".join(parts)
    paths = [world, f"{world}_nether", f"{world}_the_end"]
    for rel in paths:
        if base not in (working_path / rel).resolve().parents:
            return None
    return paths


def reset_server(config):
    """Full server reset: delete the worlds and settings the server made, keep what starts the server
    (jars, eula.txt, the bundled java/, loader files of Forge/NeoForge/Fabric/Quilt).

    서버 폴더(server.properties나 eula.txt가 있는 폴더)에서 서버가 만든 항목만 지운다.
    사용자가 둔 다른 파일과 데몬 파일(인스턴스 폴더에 서버를 둔 경우)은 남긴다.
    server.properties will be recreated from module defaults on next configure().
    """
    import shutil

    working_dir = config.get("working_dir")
    if not working_dir:
        return {"success": False, "message": i18n.t("errors.no_working_dir")}

    working_path = Path(working_dir)
    if not working_path.is_dir():
        return {"success": False, "message": i18n.t("errors.no_working_dir")}
    if not any((working_path / name).is_file() for name in ("server.properties", "eula.txt")):
        return {"success": False, "message": i18n.t("errors.not_a_server_folder", path=str(working_path))}

    keep = set(_RESET_KEEP)
    targets = list(_RESET_FILES) + list(_RESET_DIRS)
    # 인스턴스 폴더에 서버를 두면 logs/는 데몬 로그와 같은 폴더다
    if (working_path / "instance.json").is_file():
        keep.add("logs")
        targets.remove("logs")
    # 월드를 못 찾았는데 나머지만 지우고 성공이라 하면 다음 시작에 옛 월드가 그대로 뜬다
    props = ServerPropertiesManager(working_dir).read()
    worlds = _reset_world_paths(config, props, working_path, keep)
    if worlds is None:
        name = props.get("level-name") or config.get("level_name") or ""
        return {"success": False, "message": i18n.t("errors.unsafe_world_name", name=str(name))}
    targets += worlds

    deleted = []
    errors = []

    for name in targets:
        item = working_path / name
        if not os.path.lexists(item):
            continue
        try:
            if item.is_dir() and not item.is_symlink():
                shutil.rmtree(item)
            else:
                item.unlink()
            deleted.append(name)
        except OSError as e:
            errors.append(f"{name}: {e}")

    success = len(errors) == 0
    msg_parts = []
    if deleted:
        msg_parts.append(i18n.t("messages.server_reset_deleted", count=len(deleted)))
    if errors:
        msg_parts.append(i18n.t("errors.server_reset_partial", errors="; ".join(errors)))

    return {
        "success": success,
        "deleted": deleted,
        "errors": errors,
        "message": " ".join(msg_parts) if msg_parts
                   else i18n.t("messages.server_reset_complete"),
    }


def accept_agreements(config):
    """사용자가 동의한 약관(module.toml `[[agreements]]`)을 파일에 쓴다. eula가 있으면 eula.txt에 true"""
    if "eula" not in (config.get("accepted_agreements") or []):
        return {"success": True}
    working_dir = config.get("working_dir")
    if not working_dir:
        return {"success": False, "message": i18n.t("errors.no_working_dir")}

    ok = EulaManager.accept_eula(working_dir)
    return {
        "success": ok,
        "message": i18n.t("messages.eula_accepted") if ok
                   else i18n.t("errors.eula_write_failed"),
    }


def diagnose_log(config):
    """Diagnose errors from provided log lines or logs/latest.log."""
    log_lines = config.get("log_lines", [])
    working_dir = config.get("working_dir", "")

    if isinstance(log_lines, str):
        log_lines = log_lines.splitlines()

    if not log_lines and working_dir:
        log_path = os.path.join(working_dir, "logs", "latest.log")
        if os.path.isfile(log_path):
            try:
                with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                    log_lines = f.readlines()[-500:]
            except OSError:
                pass

    return {
        "success": True,
        "issues": ErrorDetector.diagnose(log_lines),
        "lines_analyzed": len(log_lines),
    }


# 데몬이 호출 제한을 알려 주지 않으면(0.1.8 데몬, 모듈 호출 120초) 강제 종료까지 그 안에 끝나도록 90초에서 멈춘다.
# 알려 주면(stop_call_limit, 데몬 #480 이후) 그 30초 앞까지 기다린다
STOP_TIMEOUT_MAX = 90
STOP_CALL_MARGIN = 30


def _stop_timeout(config):
    """정지 대기(초): 인스턴스 stop_timeout → module.toml [config] stop_timeout. 상한은 데몬의 호출 제한 안이다"""
    value = config.get("stop_timeout")
    if value in (None, ""):
        import tomllib
        with open(os.path.join(MODULE_DIR, "module.toml"), "rb") as f:
            value = tomllib.load(f).get("config", {}).get("stop_timeout", 30)
    try:
        value = int(value)
    except (TypeError, ValueError):
        value = 30
    cap = STOP_TIMEOUT_MAX
    try:
        cap = int(config["stop_call_limit"]) - STOP_CALL_MARGIN
    except (KeyError, TypeError, ValueError):
        pass
    return max(1, min(value, cap))


def _send_console_stop(instance_id):
    """데몬을 거쳐 서버 콘솔(stdin)에 `stop`을 보낸다. 콘솔이 없으면 False"""
    if not instance_id:
        return False
    try:
        from daemon_rcon import daemon_stdin_command
        daemon_stdin_command(instance_id, "stop", timeout=5)
        return True
    except Exception as e:  # 콘솔 없음(409), 데몬 없음
        print(f"[Minecraft] console stop unavailable: {e}", file=sys.stderr)
        return False


def _request_terminate(pid):
    """POSIX: SIGTERM (서버가 월드를 저장하고 끝난다). Windows 콘솔 앱에는 보낼 정상 종료 신호가 없어 False"""
    if os.name == "nt":
        return False
    import signal
    try:
        os.kill(int(pid), signal.SIGTERM)
        return True
    except (OSError, ValueError) as e:
        print(f"[Minecraft] SIGTERM failed: {e}", file=sys.stderr)
        return False


def stop(config):
    """서버 정지. 일반 정지는 콘솔 `stop` → SIGTERM 순서로 저장 후 종료를 기다리고,
    force이거나 기다려도 안 끝날 때만 강제 종료한다 (RCON은 보안 정책으로 꺼져 있다).
    둘 다 보낼 수 없으면 끝내지 않고 `graceful_stop_unavailable`로 답한다.

    데몬은 콘솔이 있는 managed 서버를 직접 멈추므로, 이 함수는 콘솔이 없을 때 주로 불린다.
    """
    try:
        pid = config.get("pid")
        if not pid:
            return {"success": False, "message": i18n.t("errors.no_pid_provided")}

        if not config.get("force", False):
            asked = _send_console_stop(config.get("instance_id")) or _request_terminate(pid)
            if not asked:
                # 저장 후 종료를 요청할 길이 없다(콘솔 없음, Windows): 저장 없이 죽이지 않고 데몬이 사용자에게 묻게 한다
                return {"success": False, "error_code": "graceful_stop_unavailable",
                        "message": i18n.t("errors.graceful_stop_unavailable")}
            if config.get("managed"):
                # managed: 데몬이 인스턴스 stop_timeout까지 기다리고 그 뒤에만 강제 종료한다(02 §3.4). 여기서 기다리면 호출 제한에 잘린다
                return {"success": True, "message": i18n.t("messages.shutdown_signal_sent", pid=pid)}
            # Windows에서 os.kill(pid, 0)은 프로세스를 끝내 버린다. 공용 도우미로만 확인한다
            if wait_for_exit(pid, timeout=_stop_timeout(config)):
                return {"success": True,
                        "message": i18n.t("messages.graceful_stop", pid=pid)}
            print(i18n.t("messages.graceful_stop_timeout"), file=sys.stderr)

        ok, error = force_kill(pid)
        if ok:
            return {"success": True, "message": i18n.t("messages.force_killed", pid=pid)}
        return {"success": False, "message": i18n.t("errors.failed_to_kill", error=error)}
    except Exception as e:
        return {"success": False, "message": i18n.t("errors.failed_to_stop", error=str(e))}


def status(config):
    """Enhanced status with Server List Ping."""
    try:
        pid = config.get("pid")
        port = config.get("port", 25565)
        host = config.get("host", "127.0.0.1")

        ping_result = MinecraftPing.ping(host, int(port), timeout=3)
        if ping_result:
            players = ping_result.get("players", {})
            version = ping_result.get("version", {})
            desc = ping_result.get("description", "")
            if isinstance(desc, dict):
                desc = desc.get("text", "")

            return {
                "success": True, "status": "running", "pid": pid,
                "online": True,
                "players_online": players.get("online", 0),
                "players_max": players.get("max", 0),
                "player_list": [p.get("name", "") for p in players.get("sample", [])],
                "version": version.get("name", "unknown"),
                "protocol": version.get("protocol", -1),
                "motd": desc,
                "message": i18n.t("messages.server_online"),
            }

        if pid and is_process_alive(pid):
            return {"success": True, "status": "starting", "pid": pid,
                    "online": False, "message": i18n.t("messages.server_starting_no_response")}

        result = {"success": True, "status": "stopped", "online": False,
                "message": i18n.t("messages.no_process_running")}

        # Attempt to detect installed version from server.jar even when offline
        server_jar = _resolve_server_jar(config)
        ver_info = _read_jar_version(*os.path.split(server_jar)) if server_jar else None
        if ver_info:
            result["version"] = ver_info["version"]
            result["java_version"] = ver_info.get("java_version")
            result["stable"] = ver_info.get("stable")

        return result
    except PathOutside:
        raise
    except Exception as e:
        return {"success": False, "message": i18n.t("errors.status_check_failed", error=str(e))}


def player_count(config):
    """접속자 수 (module.toml [status] players). 데몬이 켜진 서버에 1분마다 부른다 (Server List Ping)"""
    props = ServerPropertiesManager(config.get("working_dir", "")).read() if config.get("working_dir") else {}
    if props.get("enable-status", "").lower() == "false":
        # 핑에 답하지 않는 설정: 수는 모르지만 실패가 아니다 (실패로 답하면 데몬이 응답 없음으로 알린다)
        return {"success": True}
    # server-ip로 한 주소에만 묶었으면 127.0.0.1로는 닿지 않는다
    bound = props.get("server-ip", "")
    host = bound if bound not in ("", "0.0.0.0", "::") else config.get("host", "127.0.0.1")
    ping_result = MinecraftPing.ping(host, int(config.get("port", 25565)), timeout=3)
    if not ping_result:
        return {"success": False, "message": i18n.t("messages.server_starting_no_response")}
    players = ping_result.get("players")
    try:
        result = {"success": True, "online": int(players["online"]), "max": int(players.get("max", 0))}
    except (TypeError, KeyError, ValueError):
        # 모르는 값을 0명으로 보이지 않는다
        return {"success": False, "message": i18n.t("messages.server_starting_no_response")}
    # 이름은 데몬이 목록을 물을 때(players_detail)만 콘솔로 읽는다: 1분 갱신마다 콘솔에 list가 찍히지 않게
    if config.get("players_detail"):
        names = _console_player_names(config.get("instance_id"))
        if names is not None:
            result["players"] = [{"name": n} for n in names]
    return result


# 서버 jar en_us.json "commands.list.players" (26.3 server jar에서 확인). 줄 앞의 로그 머리는 건너뛴다.
# Paper·Spigot도 바닐라 ListPlayersCommand를 그대로 써서 문구가 같다(Paper 소스 ListPlayersCommand.java.patch는
# 플레이어가 보낸 경우의 숨김 필터만 더한다). 플러그인이 표시 이름을 바꾸면 이름 규칙에 걸려 수만 답한다
_LIST_PLAYERS_RE = re.compile(r"There are (\d+) of a max of (\d+) players online: ?(.*)$")
_PLAYER_NAME_RE = re.compile(r"[A-Za-z0-9_]{1,16}")


def _console_player_names(instance_id):
    """콘솔 `list`의 답에서 이름 목록. 문구·이름 규칙·수가 맞지 않으면 짐작하지 않고 None"""
    if not instance_id:
        return None
    from daemon_rcon import daemon_stdin_command
    try:
        reply = daemon_stdin_command(instance_id, "list", timeout=5)
    except Exception as e:
        print(f"[Minecraft] list for player names failed: {e}", file=sys.stderr)
        return None
    response = ((reply or {}).get("data") or {}).get("response") or ""
    for line in response.splitlines():
        m = _LIST_PLAYERS_RE.search(line.strip())
        if not m:
            continue
        names = [n.strip() for n in m.group(3).split(",") if n.strip()]
        if len(names) != int(m.group(1)) or not all(_PLAYER_NAME_RE.fullmatch(n) for n in names):
            return None
        return names
    return None


# ─── JAR version parsing ──────────────────────────────────

def _read_jar_version(install_dir, jar_name="server.jar"):
    """Read version.json from inside a Minecraft server.jar.

    Returns dict with version info or None on failure.
    """
    if not install_dir:
        return None
    jar_path = str(safe_join(install_dir, jar_name))
    if not os.path.isfile(jar_path):
        return None
    try:
        with zipfile.ZipFile(jar_path, "r") as zf:
            if "version.json" not in zf.namelist():
                return None
            with zf.open("version.json") as vf:
                data = json.loads(vf.read().decode("utf-8"))
            return {
                "version": data.get("id", "unknown"),
                "name": data.get("name", "unknown"),
                "protocol_version": data.get("protocol_version"),
                "java_version": data.get("java_version"),
                "stable": data.get("stable", False),
                "build_time": data.get("build_time", ""),
                "world_version": data.get("world_version"),
            }
    except (zipfile.BadZipFile, KeyError, json.JSONDecodeError, OSError):
        return None


def get_installed_version(config):
    """Detect installed Minecraft server version by parsing version.json inside server.jar.

    데몬은 시작·점검과 같은 config(server_executable 전체 경로, working_dir)를 넘긴다.
    예전 데몬은 install_dir·jar_name을 넘겼다.
    """
    jar_path = _resolve_server_jar(config)
    if not jar_path and config.get("install_dir"):
        jar_path = str(safe_join(config["install_dir"], config.get("jar_name", "server.jar")))
    install_dir = config.get("working_dir") or config.get("install_dir") or (os.path.dirname(jar_path) if jar_path else "")

    if not install_dir:
        return {"success": False, "message": i18n.t("errors.no_install_dir")}

    jar_path = jar_path or os.path.join(install_dir, "server.jar")
    install_dir, jar_name = os.path.split(jar_path)
    if not os.path.isfile(jar_path):
        found = server_types.find_server_jar(install_dir)
        if not found:
            return {"success": False, "message": i18n.t("errors.server_jar_not_found", path=jar_path)}
        jar_path, jar_name = found, os.path.basename(found)

    info = _read_jar_version(install_dir, jar_name)
    if info:
        return {"success": True, **info}
    # 페이퍼 등 Paperclip jar는 version.json 대신 versions.list에 버전이 있다
    forked = server_types.inspect_jar(jar_path)
    if forked and forked.get("version"):
        return {"success": True, "version": forked["version"], "server_type": forked["type"]}
    return {"success": False, "message": i18n.t("errors.version_info_unreadable")}


def command(config):
    """Send a named command to the server console through the daemon."""
    try:
        command_text = config.get("command")
        args = config.get("args", {})
        instance_id = config.get("instance_id")

        if not command_text:
            return {"success": False, "message": i18n.t("errors.no_command")}
        if not instance_id:
            return {"success": False, "message": i18n.t("errors.no_instance_id")}

        formatted = _format_command(command_text, args)

        from daemon_rcon import daemon_stdin_command
        result = daemon_stdin_command(instance_id, formatted, timeout=5)
        return {"success": result.get("success", True), "message": formatted}
    except Exception as e:
        return {"success": False, "message": str(e)}


# ─── Helpers ──────────────────────────────────────────────────

def _format_command(cmd, args):
    """Format a named command with its arguments."""
    formatters = {
        "say": lambda a: f"say {a.get('message', '')}",
        "give": lambda a: f"give {a.get('player', '')} {a.get('item', '')} {int(a.get('amount', 1))}",
        "save-all": lambda _: "save-all",
        "list": lambda _: "list",
        "weather": lambda a: f"weather {a.get('type', 'clear')} {int(a.get('duration', 1000))}",
        "difficulty": lambda a: f"difficulty {a.get('level', 'normal')}",
        "whitelist": lambda a: f"whitelist {a.get('action', 'list')} {a.get('player', '')}".strip(),
        "op": lambda a: f"op {a.get('player', '')}",
        "deop": lambda a: f"deop {a.get('player', '')}",
        "ban": lambda a: f"ban {a.get('player', '')} {a.get('reason', '')}".strip(),
        "pardon": lambda a: f"pardon {a.get('player', '')}",
        "kick": lambda a: f"kick {a.get('player', '')} {a.get('reason', '')}".strip(),
        "tp": lambda a: f"tp {a.get('player', '')} {a.get('target', '')}".strip(),
        "time": lambda a: f"time set {a.get('value', 'day')}",
        "gamemode": lambda a: f"gamemode {a.get('mode', 'survival')} {a.get('player', '')}".strip(),
        "seed": lambda _: "seed",
        "stop": lambda _: "stop",
    }
    formatter = formatters.get(cmd)
    return formatter(args) if formatter else cmd


# ╔═══════════════════════════════════════════════════════════╗
# ║              Server Installation / Download               ║
# ╚═══════════════════════════════════════════════════════════╝

VERSION_MANIFEST_URL = "https://piston-meta.mojang.com/mc/game/version_manifest_v2.json"


# 이보다 오래된 받기 파일은 끊긴 설치(강제 종료)가 남긴 것이다. 진행 중인 다른 설치의 파일은 남긴다
_STALE_DOWNLOAD_SECONDS = 3600


def _remove_stale_downloads(install_path, jar_name):
    cutoff = time.time() - _STALE_DOWNLOAD_SECONDS
    try:
        names = os.listdir(install_path)
    except OSError:
        return
    for name in names:
        if name.startswith(jar_name + ".") and name.endswith(".download"):
            path = os.path.join(install_path, name)
            try:
                if os.path.getmtime(path) < cutoff:
                    os.remove(path)
            except OSError:
                pass


def _remove_quietly(path):
    """받다 만 임시 파일을 지운다. 이미 없으면 그만이다"""
    try:
        os.remove(path)
    except OSError:
        pass


class ServerInstaller:
    """Download and install Minecraft server jars from Mojang."""

    @staticmethod
    def fetch_version_manifest():
        """Fetch the version manifest from Mojang.
        Returns dict with 'latest' and 'versions' keys."""
        import urllib.request
        try:
            req = urllib.request.Request(VERSION_MANIFEST_URL, headers={
                "User-Agent": "saba-chan/1.0"
            })
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            raise ConnectionError(f"Failed to fetch version manifest: {e}")

    @staticmethod
    def list_versions(include_snapshots=False, page=1, per_page=25):
        """Return a paginated list of available Minecraft server versions.

        Args:
            include_snapshots: Include snapshot/pre-release versions
            page: Page number (1-based)
            per_page: Results per page

        Returns:
            dict with 'versions', 'latest', 'total', 'page', 'per_page', 'total_pages'
        """
        manifest = ServerInstaller.fetch_version_manifest()
        latest = manifest.get("latest", {})
        all_versions = manifest.get("versions", [])

        # Filter
        if not include_snapshots:
            versions = [v for v in all_versions if v.get("type") == "release"]
        else:
            versions = all_versions  # Already sorted newest-first by Mojang

        total = len(versions)
        total_pages = max(1, (total + per_page - 1) // per_page)
        page = max(1, min(page, total_pages))
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        page_versions = versions[start_idx:end_idx]

        result = []
        for v in page_versions:
            result.append({
                "id": v["id"],
                "type": v["type"],
                "release_time": v.get("releaseTime", ""),
                "url": v.get("url", ""),  # metadata URL, not download URL
            })

        return {
            "versions": result,
            "latest": latest,
            "total": total,
            "page": page,
            "per_page": per_page,
            "total_pages": total_pages,
        }

    @staticmethod
    def get_version_details(version_id):
        """Get detailed info for a specific version including download URL.

        Returns dict with server download URL, SHA1, size, java requirements.
        """
        manifest = ServerInstaller.fetch_version_manifest()
        version_entry = None
        for v in manifest.get("versions", []):
            if v["id"] == version_id:
                version_entry = v
                break

        if not version_entry:
            return None

        # Fetch version-specific metadata
        import urllib.request
        meta_url = version_entry["url"]
        req = urllib.request.Request(meta_url, headers={"User-Agent": "saba-chan/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            meta = json.loads(resp.read().decode("utf-8"))

        server_dl = meta.get("downloads", {}).get("server")
        if not server_dl:
            return {
                "id": version_id,
                "type": version_entry.get("type"),
                "has_server": False,
                "message": i18n.t("errors.version_no_server", version=version_id),
            }

        java_version = meta.get("javaVersion", {})

        return {
            "id": version_id,
            "type": version_entry.get("type"),
            "has_server": True,
            "download_url": server_dl.get("url"),
            "sha1": server_dl.get("sha1"),
            "size": server_dl.get("size"),
            "java_major_version": java_version.get("majorVersion"),
            "java_component": java_version.get("component"),
            "release_time": version_entry.get("releaseTime"),
        }

    @staticmethod
    def install_server(version_id, install_dir, jar_name="server.jar",
                       accept_eula=False, initial_settings=None, server_type="vanilla"):
        """Download and install a Minecraft server.

        Args:
            version_id: Minecraft version (e.g., "1.21.11")
            install_dir: Directory to install into (created if needed)
            jar_name: Name for the server jar file
            accept_eula: Whether to auto-accept the EULA
            initial_settings: Optional dict of server.properties settings

        Returns:
            dict with success, install_path, details
        """
        details = ServerInstaller.get_version_details(version_id)
        if not details:
            return {
                "success": False,
                "message": i18n.t("errors.version_not_found", version=version_id),
            }

        if not details.get("has_server"):
            return {
                "success": False,
                "message": details.get("message") or i18n.t("errors.version_no_server", version=version_id),
            }

        download_url = details["download_url"]
        expected_sha1 = details.get("sha1")
        expected_sha256 = expected_md5 = None
        build = None
        if server_type != "vanilla":
            # 바닐라 외 종류는 그 프로젝트의 빌드를 받는다. Java 요구는 같은 마인크래프트 버전을 따른다
            try:
                source = server_types.resolve_download(server_type, version_id)
            except ValueError as e:
                key = str(e) if str(e) in ("install_unsupported", "version_unavailable") else "version_unavailable"
                return {"success": False,
                        "message": i18n.t(f"errors.server_type_{key}", type=server_type, version=version_id)}
            except Exception as e:
                return {"success": False, "message": i18n.t("errors.download_lookup_failed", error=str(e))}
            download_url = source["url"]
            expected_sha1 = None
            expected_sha256 = source.get("sha256")
            expected_md5 = source.get("md5")
            build = source.get("build")
            # Fabric 런처는 처음 뜰 때 바닐라 server.jar를 같은 폴더에 받는다 — 이름이 겹치면 안 된다
            if server_type == "fabric" and jar_name == "server.jar":
                jar_name = "fabric-server-launch.jar"

        # Create install directory
        install_path = os.path.abspath(install_dir)
        try:
            os.makedirs(install_path, exist_ok=True)
        except OSError as e:
            return {
                "success": False,
                "message": i18n.t("errors.working_dir_error", path=str(e)),
            }

        jar_path = str(safe_join(install_path, jar_name))
        # 받는 동안과 검증이 끝날 때까지는 임시 파일에 쓴다: 실패해도 지금 쓰던 jar가 남는다
        # (데몬은 업데이트 전에 server.jar만 백업한다. fabric-server-launch.jar 같은 다른 이름은 여기서 지킨다).
        # 해시가 있으면 해시로, 없으면(Fabric 런처) 크기와 jar 구조로 확인한다. 이름은 요청마다 다르다
        _remove_stale_downloads(install_path, jar_name)
        try:
            fd, part_path = tempfile.mkstemp(prefix=jar_name + ".", suffix=".download", dir=install_path)
            os.close(fd)
        except OSError as e:
            return {"success": False, "message": i18n.t("errors.download_failed", error=str(e))}

        # Download with progress
        import urllib.request
        print(f"Downloading Minecraft server {version_id}...", file=sys.stderr)
        print(f"  URL: {download_url}", file=sys.stderr)
        print(f"  Destination: {jar_path}", file=sys.stderr)

        try:
            req = urllib.request.Request(download_url, headers={"User-Agent": "saba-chan/1.0"})
            with urllib.request.urlopen(req, timeout=120) as resp:
                total = int(resp.headers.get("Content-Length", 0))
                sha1 = hashlib.sha1()
                sha256 = hashlib.sha256()
                md5 = hashlib.md5()
                downloaded = 0
                chunk_size = 65536
                last_pct = -1
                message = i18n.t("messages.downloading_server", version=version_id)

                with open(part_path, "wb") as f:
                    while True:
                        chunk = resp.read(chunk_size)
                        if not chunk:
                            break
                        f.write(chunk)
                        sha1.update(chunk)
                        sha256.update(chunk)
                        md5.update(chunk)
                        downloaded += len(chunk)
                        if total > 0:
                            pct = downloaded * 100 // total
                            if pct != last_pct:
                                last_pct = pct
                                _progress(pct, message)
                # 연결이 일찍 정상 종료되면 read()는 오류 없이 끝난다
                if total > 0 and downloaded != total:
                    raise IOError(f"incomplete download: {downloaded}/{total} bytes")
            if not (expected_sha1 or expected_sha256 or expected_md5):
                with zipfile.ZipFile(part_path) as z:
                    bad = z.testzip()
                if bad is not None:
                    raise IOError(f"corrupt jar entry: {bad}")

        except Exception as e:
            # Clean up partial download
            _remove_quietly(part_path)
            return {
                "success": False,
                "message": i18n.t("errors.download_failed", error=str(e)),
            }

        # Verify SHA1
        actual_sha1 = sha1.hexdigest()
        if expected_sha1 and actual_sha1 != expected_sha1:
            _remove_quietly(part_path)
            return {
                "success": False,
                "message": i18n.t("errors.checksum_mismatch", algorithm="SHA1",
                                  expected=expected_sha1, actual=actual_sha1),
            }

        for name, expected, digest in (("SHA256", expected_sha256, sha256), ("MD5", expected_md5, md5)):
            if expected and digest.hexdigest() != expected.lower():
                _remove_quietly(part_path)
                return {"success": False,
                        "message": i18n.t("errors.checksum_mismatch", algorithm=name,
                                          expected=expected, actual=digest.hexdigest())}

        # mkstemp는 0600으로 만든다: 옛 jar 권한(없으면 umask 기본값)을 따른다.
        # 권한을 바꿀 수 없는 파일시스템이면 그대로 둔다
        try:
            if os.path.isfile(jar_path):
                shutil.copymode(jar_path, part_path)
            else:
                umask = os.umask(0)
                os.umask(umask)
                os.chmod(part_path, 0o666 & ~umask)
        except OSError:
            pass
        try:
            os.replace(part_path, jar_path)
        except OSError as e:
            # 서버가 jar를 잡고 있으면(Windows) 바꾸지 못한다: 옛 jar는 그대로 둔다
            _remove_quietly(part_path)
            return {"success": False, "message": i18n.t("errors.download_failed", error=str(e))}
        print(f"  Checksum verified: {actual_sha1}", file=sys.stderr)

        # Post-install setup
        result = {
            "success": True,
            "install_path": install_path,
            "jar_path": jar_path,
            "jar_name": jar_name,
            "executable_path": jar_path,
            "server_type": server_type,
            "build": build,
            "version": version_id,
            "sha1": actual_sha1,
            "size": downloaded,
            "java_major_version": details.get("java_major_version"),
        }

        # Accept EULA if requested
        if accept_eula:
            ok = EulaManager.accept_eula(install_path)
            result["eula_accepted"] = ok

        # Apply initial server settings
        if initial_settings and isinstance(initial_settings, dict):
            mgr = ServerPropertiesManager(install_path)
            props = {}
            for k, v in initial_settings.items():
                prop_key = _PROPERTY_KEY_MAP.get(k, k)
                if isinstance(v, bool):
                    v = "true" if v else "false"
                props[prop_key] = str(v)

            # RCON is blocked for security (commands go through the console)
            props["enable-rcon"] = "false"

            if props:
                mgr.update(props)

            result["settings_applied"] = True

        # Download bundled Java if needed
        java_req = details.get("java_major_version")
        if java_req:
            # Check if system Java is already sufficient
            java_info = JavaDetector.find_java(min_major=java_req)
            if java_info and java_info["major_version"] >= java_req:
                result["java_ok"] = True
                result["java_path"] = java_info["path"]
                result["java_version"] = java_info["version"]
            else:
                # Download portable JRE into the server directory
                reason = (
                    f"system Java {java_info['major_version']}" if java_info
                    else "no system Java found"
                )
                print(f"  {reason}, downloading Java {java_req}...", file=sys.stderr)
                jre_result = JavaDownloader.download_jre(java_req, install_path)
                if jre_result.get("success"):
                    result["java_ok"] = True
                    result["java_path"] = jre_result["java_path"]
                    result["java_version"] = jre_result.get("version", "")
                    result["java_bundled"] = True
                else:
                    result["java_warning"] = i18n.t("errors.java_download_warning", version=str(java_req),
                                                    error=jre_result.get("message", ""))

        result["message"] = i18n.t("messages.server_installed", version=version_id, path=install_path)
        return result


def list_versions(config):
    """List available Minecraft server versions.

    Config options:
        include_snapshots (bool): Include snapshots/pre-releases (default: false)
        page (int): Page number, 1-based (default: 1)
        per_page (int): Results per page (default: 25)
        settings (dict): 인스턴스 설정. `server_type`이 있으면 `latest.release`는 그 종류가 받을 수 있는
            가장 새 릴리스다 (데몬이 새 인스턴스의 버전을 고를 때 넘긴다)
    """
    try:
        include_snapshots = config.get("include_snapshots", False)
        page = int(config.get("page", 1))
        per_page = int(config.get("per_page", 25))

        result = ServerInstaller.list_versions(
            include_snapshots=include_snapshots,
            page=page,
            per_page=per_page,
        )
        server_type = (config.get("settings") or {}).get("server_type")
        if server_type and server_type != "vanilla":
            releases = [v["id"] for v in result["versions"] if v["type"] == "release"]
            installable = server_types.latest_installable(server_type, releases)
            if installable:
                result["latest"] = {**result.get("latest", {}), "release": installable}
        return {"success": True, **result}
    except Exception as e:
        return {"success": False, "message": str(e)}


def get_version_details(config):
    """Get detailed info for a specific version.

    Config options:
        version (str): Version ID (e.g., "1.21.11")
    """
    try:
        version_id = config.get("version")
        if not version_id:
            return {"success": False, "message": i18n.t("errors.no_version")}

        details = ServerInstaller.get_version_details(version_id)
        if not details:
            return {"success": False, "message": i18n.t("errors.version_not_found", version=version_id)}

        return {"success": True, **details}
    except Exception as e:
        return {"success": False, "message": str(e)}


def install_server(config):
    """Download and install a Minecraft server.

    Config options:
        version (str): Minecraft version (e.g., "1.21.11")
        install_dir (str): Directory to install into
        server_executable (str): Name for server jar (default: "server.jar", 예전 이름 jar_name)
        accepted_agreements (list): 사용자가 동의한 약관 id. "eula"가 있으면 eula.txt에 동의를 쓴다
        settings (dict): 인스턴스 설정. server.properties 키는 처음 값으로 쓴다
        initial_settings (dict): Optional initial server.properties values (settings보다 우선)
    """
    try:
        version_id = config.get("version")
        install_dir = config.get("install_dir")

        if not version_id:
            return {"success": False, "message": i18n.t("errors.no_version")}
        if not install_dir:
            return {"success": False, "message": i18n.t("errors.no_install_dir")}

        jar_name = os.path.basename(config.get("server_executable") or config.get("jar_name") or "server.jar")
        do_eula = "eula" in (config.get("accepted_agreements") or [])
        settings = config.get("settings") or {}
        # 데몬이 넘긴 인스턴스 설정(프리셋·옮긴 포트) 중 server.properties 키만 처음 값으로 쓴다
        initial_settings = {k: v for k, v in settings.items() if k in _PROPERTY_KEY_MAP and v is not None}
        initial_settings.update(config.get("initial_settings") or {})
        # 서버 종류는 인스턴스 설정(`settings`)에서 — 데몬이 설치 때 넘긴다.
        # 없으면(GUI의 버전 설치) 폴더에 있는 서버의 종류를 따른다: Paper·Fabric을 바닐라로 덮지 않는다
        server_type = (settings.get("server_type") or config.get("server_type")
                       or server_types.detect(install_dir).get("type") or "vanilla")

        return ServerInstaller.install_server(
            version_id=version_id,
            install_dir=install_dir,
            jar_name=jar_name,
            accept_eula=do_eula,
            initial_settings=initial_settings,
            server_type=server_type,
        )
    except PathOutside:
        raise
    except Exception as e:
        import traceback
        print(traceback.format_exc(), file=sys.stderr)
        return {"success": False, "message": str(e)}


# ╔═══════════════════════════════════════════════════════════╗
# ║                      Main Entry                           ║
# ╚═══════════════════════════════════════════════════════════╝

# plugins/ 안에서 백업에서 뺄 것: 플러그인이 다시 그릴 수 있는 지도 타일 (경로 조각, 대소문자 무시)
# dynmap 기본 저장소 filetree의 tilespath = web/tiles (dynmap 위키 "Configuration.txt", fullrender로 다시 그림)
_PLUGIN_BACKUP_EXCLUDES = [("dynmap", "web", "tiles")]


def _plugin_entries(path, parts):
    """path 아래 항목 중 뺄 경로 조각에 걸리는 것은 그 안으로 들어가 나머지만 고른다"""
    try:
        names = sorted(os.listdir(path))
    except FileNotFoundError:
        return []
    picked = []
    for name in names:
        here = parts + (name.lower(),)
        full = os.path.join(path, name)
        if here in _PLUGIN_BACKUP_EXCLUDES:
            continue
        if os.path.isdir(full) and any(ex[:len(here)] == here for ex in _PLUGIN_BACKUP_EXCLUDES):
            picked.extend(_plugin_entries(full, here))
        else:
            picked.append(full)
    return picked


def backup_paths(config):
    """백업할 플러그인 데이터 (데몬 `[backup] hook`): plugins/ 전체에서 다시 그릴 수 있는 지도 타일만 뺀다.
    월드·설정 파일은 module.toml `paths`에 있다. 읽지 못하는 폴더는 실패로 답한다(빠진 백업을 성공으로 남기지 않는다)"""
    plugins = os.path.join(config.get("working_dir") or "", "plugins")
    try:
        return {"success": True, "paths": _plugin_entries(plugins, ())}
    except OSError as e:
        return {"success": False, "message": i18n.t("errors.backup_paths_failed", error=str(e))}


FUNCTIONS = {
    "stop": stop,
    "status": status,
    "command": command,
    "validate": validate,
    "get_launch_command": get_launch_command,
    "configure": configure,
    "read_properties": read_properties,
    "import_settings": import_settings,
    "config_files": config_files,
    "reset_properties": reset_properties,
    "reset_server": reset_server,
    "accept_agreements": accept_agreements,
    "diagnose_log": diagnose_log,
    "list_versions": list_versions,
    "get_version_details": get_version_details,
    "install_server": install_server,
    "get_installed_version": get_installed_version,
    "instance_commands": instance_commands,
    "player_count": player_count,
    "backup_paths": backup_paths,
}
# 데몬이 지원 기능을 묻는 예약 함수: 표의 함수 이름을 돌려준다
FUNCTIONS["functions"] = lambda _config: {"success": True, "functions": [f for f in FUNCTIONS if f != "functions"]}

if __name__ == "__main__":
    # 공용 진입점: 깨진 stdin·모르는 함수·함수 안의 예외도 JSON으로 답하고 종료 코드 0을 지킨다
    from daemon_rcon import run_lifecycle
    run_lifecycle(FUNCTIONS)
