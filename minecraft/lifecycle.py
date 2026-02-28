#!/usr/bin/env python3
"""
Minecraft server lifecycle management module for saba-chan.

Provides complete server management:
  - Java environment detection and validation
  - EULA management
  - server.properties read/write
  - Error detection and diagnosis with solutions
  - Server launch command generation (for Rust managed process)
  - Graceful shutdown via RCON /stop
  - Server List Ping for rich status info

Always outputs JSON to stdout, logs to stderr.
"""

import sys
import json
import subprocess
import os
import re
import socket
import struct
import time
import hashlib
from pathlib import Path
from i18n import I18n

# RCON communication delegates to the Rust daemon's HTTP API
from daemon_rcon import rcon_command as _rcon_command_bridge

def _rcon_command(host, port, password, command, timeout=5):
    """RCON via daemon API if instance_id available, else direct fallback."""
    return _rcon_command_bridge(host, port, password, command, timeout)

# ─── Init ─────────────────────────────────────────────────────

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
i18n = I18n(MODULE_DIR)
DAEMON_API_URL = os.environ.get('DAEMON_API_URL', 'http://127.0.0.1:57474')

# ─── Friendly name → server.properties key mapping ───────────
# Used by configure() and install_server() to translate settings.
_PROPERTY_KEY_MAP = {
    "accepts_transfers": "accepts-transfers",
    "allow_flight": "allow-flight",
    "allow_nether": "allow-nether",
    "broadcast_console_to_ops": "broadcast-console-to-ops",
    "broadcast_rcon_to_ops": "broadcast-rcon-to-ops",
    "bug_report_link": "bug-report-link",
    "difficulty": "difficulty",
    "enable_command_block": "enable-command-block",
    "enable_code_of_conduct": "enable-code-of-conduct",
    "enable_jmx_monitoring": "enable-jmx-monitoring",
    "enable_query": "enable-query",
    "enable_rcon": "enable-rcon",
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
    "rate_limit": "rate-limit",
    "rcon_password": "rcon.password",
    "rcon_port": "rcon.port",
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

    @staticmethod
    def find_java(preferred_path=None):
        """
        Find a working Java executable.
        Returns dict with path, version, major_version or None.
        """
        candidates = []

        # 1. User-specified path
        if preferred_path and preferred_path != "java":
            candidates.append(preferred_path)

        # 2. JAVA_HOME
        java_home = os.environ.get("JAVA_HOME")
        if java_home:
            exe = "java.exe" if os.name == "nt" else "java"
            candidates.append(os.path.join(java_home, "bin", exe))

        # 3. PATH (just "java")
        candidates.append("java")

        # 4. Platform-specific common paths
        if os.name == "nt":
            for base in JavaDetector.WINDOWS_PATHS:
                expanded = os.path.expandvars(base)
                if os.path.isdir(expanded):
                    for root, _dirs, files in os.walk(expanded):
                        if "java.exe" in files:
                            candidates.append(os.path.join(root, "java.exe"))
        else:
            for path in JavaDetector.UNIX_PATHS:
                if os.path.isfile(path):
                    candidates.append(path)
                elif os.path.isdir(path):
                    for root, _dirs, files in os.walk(path):
                        if "java" in files:
                            candidates.append(os.path.join(root, "java"))

        # Try each candidate
        seen = set()
        for candidate in candidates:
            if candidate in seen:
                continue
            seen.add(candidate)
            info = JavaDetector.get_java_info(candidate)
            if info:
                return info

        return None

    @staticmethod
    def get_java_info(java_path):
        """Get Java version information. Returns dict or None."""
        try:
            creationflags = 0x08000000 if os.name == "nt" else 0
            result = subprocess.run(
                [java_path, "-version"],
                capture_output=True, text=True, timeout=10,
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
                    "raw_output": output.strip()
                }
        except (subprocess.TimeoutExpired, FileNotFoundError, OSError, PermissionError):
            pass
        return None

    @staticmethod
    def parse_major_version(version_str):
        """Extract major version number. '17.0.2' → 17, '1.8.0_362' → 8"""
        parts = version_str.split(".")
        major = int(parts[0])
        if major == 1 and len(parts) > 1:
            major = int(parts[1])
        return major

    @staticmethod
    def validate_for_minecraft(java_info, mc_version=None):
        """Check if Java version meets Minecraft's minimum requirement."""
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
        min_java = 17  # Safe default for modern MC

        if mc_version:
            try:
                parts = mc_version.split(".")
                mc_major = int(parts[1]) if len(parts) > 1 else 0
                mc_minor = int(parts[2]) if len(parts) > 2 else 0
                if mc_major >= 20 and mc_minor >= 5:
                    min_java = 21
                elif mc_major >= 18:
                    min_java = 17
                elif mc_major >= 17:
                    min_java = 16
                else:
                    min_java = 8
            except (ValueError, IndexError):
                pass

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

                with open(tmp_file, "wb") as f:
                    while True:
                        chunk = resp.read(chunk_size)
                        if not chunk:
                            break
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total > 0:
                            pct = downloaded * 100 // total
                            print(f"\r  Java download: {pct}% ({downloaded // 1024 // 1024}MB)", end="", file=sys.stderr)

                print("", file=sys.stderr)

        except Exception as e:
            if os.path.exists(tmp_file):
                os.remove(tmp_file)
            return {"success": False, "message": f"Java download failed: {e}"}

        # Extract
        print(f"  Extracting to {java_dir}...", file=sys.stderr)
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
            return {"success": False, "message": f"Java extraction failed: {e}"}
        finally:
            if os.path.exists(tmp_file):
                os.remove(tmp_file)

        # Make java executable on Unix
        java_exe = JavaDownloader._find_bundled_java(java_dir)
        if java_exe and os.name != "nt":
            import stat
            st = os.stat(java_exe)
            os.chmod(java_exe, st.st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

        if not java_exe:
            return {"success": False, "message": "Java extracted but java executable not found"}

        # Verify it works
        info = JavaDetector.get_java_info(java_exe)
        if not info:
            return {"success": False, "message": "Java extracted but failed to run"}

        print(f"  Java {info['version']} ready at {java_exe}", file=sys.stderr)

        return {
            "success": True,
            "java_path": java_exe,
            "java_home": java_dir,
            "version": info["version"],
            "major_version": info["major_version"],
        }

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
            with open(eula_path, "w", encoding="utf-8") as f:
                f.write(content)
            return True
        except OSError:
            return False


# ╔═══════════════════════════════════════════════════════════╗
# ║             server.properties Manager                     ║
# ╚═══════════════════════════════════════════════════════════╝

def _load_default_properties():
    """Load vanilla defaults from server.properties reference file,
    then apply saba-chan-specific overrides (e.g. enable RCON)."""
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

    # saba-chan overrides: enable RCON for managed server control
    defaults["enable-rcon"] = "true"
    defaults.setdefault("pause-when-empty-seconds", "60")
    return defaults


DEFAULT_PROPERTIES = _load_default_properties()


class ServerPropertiesManager:
    """Read/write Minecraft server.properties files."""

    def __init__(self, working_dir):
        self.path = os.path.join(working_dir, "server.properties")

    def exists(self):
        return os.path.isfile(self.path)

    def read(self):
        """Parse server.properties into a dict."""
        props = {}
        if not self.exists():
            return props
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                for line in f:
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
        try:
            with open(self.path, "w", encoding="utf-8") as f:
                f.write(header)
                for key, value in sorted(properties.items()):
                    f.write(f"{key}={value}\n")
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
            with open(self.path, "r", encoding="utf-8") as f:
                for line in f:
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

            with open(self.path, "w", encoding="utf-8") as f:
                f.writelines(lines)
            return True
        except OSError:
            return False

    def get_defaults(self):
        return dict(DEFAULT_PROPERTIES)

    def ensure_rcon(self, port=25575, password=""):
        """Ensure RCON is enabled. Generates a password if none given."""
        import secrets
        import string
        if not password:
            alphabet = string.ascii_letters + string.digits
            password = "".join(secrets.choice(alphabet) for _ in range(16))
        self.update({
            "enable-rcon": "true",
            "rcon.port": str(port),
            "rcon.password": password,
        })
        return password


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
                            "matched_line": line.strip(),
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
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            sock.connect((host, port))

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
            sock.sendall(MinecraftPing._encode_varint(len(handshake_data)) + handshake_data)

            # Status request
            sock.sendall(MinecraftPing._encode_varint(1) + MinecraftPing._encode_varint(0x00))

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
                        sock.close()
                        return parsed
                except Exception:
                    continue

            sock.close()
            return None
        except (socket.timeout, socket.error, OSError):
            return None

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

    Checks keys in order: server_jar → server_executable → executable_path.
    If the path is relative, resolves it against working_dir (if set).
    Returns an absolute path string or None.
    """
    jar = (
        config.get("server_jar")
        or config.get("server_executable")
        or config.get("executable_path")
    )
    if not jar:
        return None

    # If relative, resolve against working_dir
    if not os.path.isabs(jar):
        working_dir = config.get("working_dir", "")
        if working_dir:
            jar = os.path.join(working_dir, jar)

    return os.path.abspath(jar)


def validate(config):
    """
    Validate all prerequisites before starting.
    Checks: Java, server jar, EULA, working dir, port availability.
    """
    issues = []
    java_path = config.get("java_path", "java")
    server_jar = _resolve_server_jar(config)
    working_dir = config.get("working_dir", "")

    # Java
    java_info = JavaDetector.find_java(java_path)
    issues.extend(JavaDetector.validate_for_minecraft(java_info))

    # Server JAR
    if not server_jar:
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

    # Port availability
    port = config.get("port", 25565)
    if port:
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


def _enforce_rcon_policy(working_dir, managed=True):
    """
    Enforce RCON policy in server.properties before server launch.

    - Managed mode (stdin control):  RCON is a security risk → force disable
    - Non-managed mode (RCON only):  RCON is the only control method → force enable + ensure password

    Returns dict with details of what was changed, or None if no changes needed.
    """
    mgr = ServerPropertiesManager(working_dir)
    if not mgr.exists():
        return None  # server.properties will be generated on first run

    props = mgr.read()
    changes = {}

    if managed:
        # Managed mode: RCON disabled (stdin is used for control)
        if props.get("enable-rcon", "false").lower() == "true":
            changes["enable-rcon"] = "false"
            print(i18n.t("messages.rcon_disabled_managed"), file=sys.stderr)
    else:
        # Non-managed mode: RCON must be enabled
        if props.get("enable-rcon", "false").lower() != "true":
            changes["enable-rcon"] = "true"
            print(i18n.t("messages.rcon_enabled_native"), file=sys.stderr)
        # Ensure RCON password exists
        current_pw = props.get("rcon.password", "")
        if not current_pw:
            import secrets
            import string
            alphabet = string.ascii_letters + string.digits
            password = "".join(secrets.choice(alphabet) for _ in range(16))
            changes["rcon.password"] = password
            print(i18n.t("messages.rcon_password_generated"), file=sys.stderr)

    if changes:
        mgr.update(changes)
        return {"changed": True, "changes": changes}
    return {"changed": False}


def get_launch_command(config):
    """
    Build the command for the Rust daemon to spawn as a ManagedProcess.
    Returns { success, program, args, working_dir, env_vars }.
    """
    java_path = config.get("java_path", "java")
    server_jar = _resolve_server_jar(config)
    ram_raw = config.get("ram", 2)
    # Accept plain number (GB). Fractional values converted to MB (e.g. 0.5 → 512M, 1.5 → 1536M)
    try:
        ram_gb = float(str(ram_raw).rstrip("GgMm"))
        if ram_gb == int(ram_gb):
            ram = f"{int(ram_gb)}G"
        else:
            ram = f"{int(ram_gb * 1024)}M"
    except (ValueError, TypeError):
        ram = "2G"
    working_dir = config.get("working_dir")
    extra_jvm_args = config.get("jvm_args", [])

    if not server_jar:
        return {
            "success": False,
            "action_required": "server_jar_not_found",
            "message": i18n.t("errors.server_jar_not_specified"),
            "options": ["update_path", "install_new"],
        }

    if not os.path.isfile(server_jar):
        return {
            "success": False,
            "action_required": "server_jar_not_found",
            "message": i18n.t("errors.server_jar_not_found", path=server_jar),
            "configured_path": server_jar,
            "options": ["update_path", "install_new"],
        }

    # Prefer bundled Java in <working_dir>/java/ over system Java
    _wd = working_dir or os.path.dirname(os.path.abspath(server_jar))
    bundled = JavaDownloader._find_bundled_java(os.path.join(_wd, "java"))
    if bundled:
        java_path = bundled
    else:
        java_info = JavaDetector.find_java(java_path)
        if java_info:
            java_path = java_info["path"]

    if not working_dir:
        working_dir = os.path.dirname(os.path.abspath(server_jar))

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

    args.extend(["-jar", os.path.abspath(server_jar), "nogui"])

    abs_working_dir = os.path.abspath(working_dir)

    # Managed mode → RCON must be OFF (stdin is used for control)
    is_managed = config.get("managed", True)  # get_launch_command default = managed
    _enforce_rcon_policy(abs_working_dir, managed=is_managed)

    return {
        "success": True,
        "program": java_path,
        "args": args,
        "working_dir": abs_working_dir,
        "env_vars": {},
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
        prop_key = _PROPERTY_KEY_MAP.get(key, key)
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


def import_settings(config):
    """Read server.properties and return settings in saba-chan key format.

    Used during migration to import existing server settings into saba-chan.
    """
    result = read_properties(config)
    if not result.get("success"):
        return result

    raw_props = result.get("properties", {})
    if not raw_props:
        return {"success": True, "settings": {}, "message": "No properties found."}

    # Build reverse map: server.properties key → saba-chan key
    reverse_map = {v: k for k, v in _PROPERTY_KEY_MAP.items()}

    settings = {}
    for prop_key, raw_value in raw_props.items():
        saba_key = reverse_map.get(prop_key, prop_key)
        val = str(raw_value).strip()
        if val.lower() == "true":
            settings[saba_key] = True
        elif val.lower() == "false":
            settings[saba_key] = False
        else:
            try:
                f = float(val)
                settings[saba_key] = int(f) if f == int(f) else f
            except (ValueError, OverflowError):
                settings[saba_key] = val

    return {"success": True, "settings": settings}


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


def reset_server(config):
    """Full server reset: delete everything except server.jar and eula.txt.

    server.properties will be recreated from module defaults on next configure().
    """
    import shutil

    working_dir = config.get("working_dir")
    if not working_dir:
        return {"success": False, "message": i18n.t("errors.no_working_dir")}

    working_path = Path(working_dir)
    if not working_path.is_dir():
        return {"success": False, "message": i18n.t("errors.no_working_dir")}

    # 보존할 파일 패턴 (대소문자 무시)
    KEEP_FILES = {"eula.txt"}
    KEEP_EXTENSIONS = {".jar"}

    deleted = []
    errors = []

    for item in list(working_path.iterdir()):
        name_lower = item.name.lower()

        # JAR 파일 보존 (server.jar 등)
        if item.is_file() and any(name_lower.endswith(ext) for ext in KEEP_EXTENSIONS):
            continue
        # eula.txt 보존
        if item.is_file() and name_lower in KEEP_FILES:
            continue

        try:
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()
            deleted.append(item.name)
        except OSError as e:
            errors.append(f"{item.name}: {e}")

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


def accept_eula(config):
    """Accept the Minecraft EULA."""
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


def start(config):
    """Start server (legacy — prefer get_launch_command + ManagedProcess)."""
    try:
        java_path = config.get("java_path", "java")
        server_jar = _resolve_server_jar(config)

        # Non-managed (legacy) mode → RCON must be ON (only control method)
        wd = config.get("working_dir")
        if wd:
            _enforce_rcon_policy(os.path.abspath(wd), managed=False)

        if not server_jar:
            return {
                "success": False,
                "action_required": "server_jar_not_found",
                "message": i18n.t("errors.server_jar_not_specified"),
                "options": ["update_path", "install_new"],
            }
        if not os.path.exists(server_jar):
            return {
                "success": False,
                "action_required": "server_jar_not_found",
                "message": i18n.t("errors.server_jar_not_found", path=server_jar),
                "configured_path": server_jar,
                "options": ["update_path", "install_new"],
            }

        java_info = JavaDetector.find_java(java_path)
        if java_info:
            java_path = java_info["path"]

        ram_raw = config.get("ram", 2)
        # Accept plain number (GB). Fractional → MB (e.g. 0.5 → 512M)
        try:
            ram_gb = float(str(ram_raw).rstrip("GgMm"))
            if ram_gb == int(ram_gb):
                ram = f"{int(ram_gb)}G"
            else:
                ram = f"{int(ram_gb * 1024)}M"
        except (ValueError, TypeError):
            ram = "2G"
        working_dir = config.get("working_dir") or os.path.dirname(server_jar)

        if config.get("auto_eula", False):
            EulaManager.accept_eula(working_dir)

        cmd = [java_path, f"-Xmx{ram}", f"-Xms{ram}", "-jar", server_jar, "nogui"]
        print(i18n.t("messages.starting_server", command=" ".join(cmd)), file=sys.stderr)

        if sys.platform == "win32":
            flags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS
            proc = subprocess.Popen(cmd, cwd=working_dir, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, creationflags=flags)
        else:
            proc = subprocess.Popen(cmd, cwd=working_dir, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, start_new_session=True)

        return {"success": True, "pid": proc.pid,
                "message": i18n.t("messages.server_starting", pid=proc.pid)}
    except Exception as e:
        import traceback
        print(traceback.format_exc(), file=sys.stderr)
        return {"success": False, "message": i18n.t("errors.failed_to_start", error=str(e))}


def stop(config):
    """Stop server — graceful RCON 'stop' before force kill."""
    try:
        pid = config.get("pid")
        if not pid:
            return {"success": False, "message": i18n.t("errors.no_pid_provided")}

        force = config.get("force", False)

        if not force:
            rcon_port = config.get("rcon_port", 25575)
            rcon_password = config.get("rcon_password", "")
            if rcon_password:
                try:
                    _rcon_command("127.0.0.1", rcon_port, rcon_password, "stop")
                    for _ in range(30):
                        time.sleep(1)
                        try:
                            os.kill(pid, 0)
                        except OSError:
                            return {"success": True,
                                    "message": i18n.t("messages.graceful_stop", pid=pid)}
                    print(i18n.t("messages.graceful_stop_timeout"), file=sys.stderr)
                except Exception as e:
                    print(f"RCON stop failed, falling back: {e}", file=sys.stderr)

        if sys.platform == "win32":
            try:
                subprocess.run(["taskkill", "/F", "/PID", str(pid)], check=True,
                               creationflags=0x08000000)
                return {"success": True, "message": i18n.t("messages.force_killed", pid=pid)}
            except subprocess.CalledProcessError as e:
                return {"success": False, "message": i18n.t("errors.failed_to_kill", error=str(e))}
        else:
            import signal as sig_mod
            os.kill(pid, sig_mod.SIGKILL if force else sig_mod.SIGTERM)
            return {"success": True, "message": i18n.t("messages.signal_sent",
                                                        signal="KILL" if force else "TERM", pid=pid)}
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

        if pid:
            try:
                os.kill(pid, 0)
                return {"success": True, "status": "starting", "pid": pid,
                        "online": False, "message": i18n.t("messages.server_starting_no_response")}
            except OSError:
                pass

        return {"success": True, "status": "stopped", "online": False,
                "message": i18n.t("messages.no_process_running")}
    except Exception as e:
        return {"success": False, "message": f"Status check failed: {e}"}


def command(config):
    """Execute command via daemon RCON API (legacy path)."""
    try:
        command_text = config.get("command")
        args = config.get("args", {})
        instance_id = config.get("instance_id")

        if not command_text:
            return {"success": False, "message": "No command specified"}
        if not instance_id:
            return {"success": False, "message": "No instance_id specified"}

        formatted = _format_command(command_text, args)

        import urllib.request
        api_url = f"{DAEMON_API_URL}/api/instance/{instance_id}/rcon"
        payload = json.dumps({"command": formatted}).encode("utf-8")
        req = urllib.request.Request(api_url, data=payload,
                                     headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=5) as response:
            result = json.loads(response.read().decode("utf-8"))
            return {"success": result.get("success", True), "message": f"RCON: {formatted}"}
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


def _send_rcon_command(host, port, password, command):
    """Legacy wrapper — delegates to daemon RCON bridge."""
    return _rcon_command(host, port, password, command)


# ╔═══════════════════════════════════════════════════════════╗
# ║              Server Installation / Download               ║
# ╚═══════════════════════════════════════════════════════════╝

VERSION_MANIFEST_URL = "https://piston-meta.mojang.com/mc/game/version_manifest_v2.json"


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
                "message": f"Version {version_id} does not have a dedicated server download",
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
                       accept_eula=False, initial_settings=None):
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
                "message": f"Version '{version_id}' not found in Mojang manifest",
            }

        if not details.get("has_server"):
            return {
                "success": False,
                "message": details.get("message", f"No server download for {version_id}"),
            }

        download_url = details["download_url"]
        expected_sha1 = details.get("sha1")
        expected_size = details.get("size")

        # Create install directory
        install_path = os.path.abspath(install_dir)
        try:
            os.makedirs(install_path, exist_ok=True)
        except OSError as e:
            return {
                "success": False,
                "message": i18n.t("errors.working_dir_error", path=str(e)),
            }

        jar_path = os.path.join(install_path, jar_name)

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
                downloaded = 0
                chunk_size = 65536

                with open(jar_path, "wb") as f:
                    while True:
                        chunk = resp.read(chunk_size)
                        if not chunk:
                            break
                        f.write(chunk)
                        sha1.update(chunk)
                        downloaded += len(chunk)
                        if total > 0:
                            pct = downloaded * 100 // total
                            print(f"\r  Progress: {pct}% ({downloaded}/{total} bytes)", end="", file=sys.stderr)

                print("", file=sys.stderr)  # newline after progress

        except Exception as e:
            # Clean up partial download
            if os.path.exists(jar_path):
                try:
                    os.remove(jar_path)
                except OSError:
                    pass
            return {
                "success": False,
                "message": f"Download failed: {e}",
            }

        # Verify SHA1
        actual_sha1 = sha1.hexdigest()
        if expected_sha1 and actual_sha1 != expected_sha1:
            os.remove(jar_path)
            return {
                "success": False,
                "message": f"SHA1 mismatch: expected {expected_sha1}, got {actual_sha1}",
            }

        print(f"  SHA1 verified: {actual_sha1}", file=sys.stderr)

        # Post-install setup
        result = {
            "success": True,
            "install_path": install_path,
            "jar_path": jar_path,
            "jar_name": jar_name,
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

            # Enable RCON by default with random password
            if "enable-rcon" not in props:
                mgr.ensure_rcon()

            if props:
                mgr.update(props)

            result["settings_applied"] = True

        # Download bundled Java if needed
        java_req = details.get("java_major_version")
        if java_req:
            # Check if system Java is already sufficient
            java_info = JavaDetector.find_java()
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
                    result["java_warning"] = (
                        f"Failed to download Java {java_req}: {jre_result.get('message')}. "
                        f"Please install Java {java_req}+ manually."
                    )

        result["message"] = f"Minecraft server {version_id} installed to {install_path}"
        return result


def list_versions(config):
    """List available Minecraft server versions.

    Config options:
        include_snapshots (bool): Include snapshots/pre-releases (default: false)
        page (int): Page number, 1-based (default: 1)
        per_page (int): Results per page (default: 25)
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
            return {"success": False, "message": "No version specified"}

        details = ServerInstaller.get_version_details(version_id)
        if not details:
            return {"success": False, "message": f"Version '{version_id}' not found"}

        return {"success": True, **details}
    except Exception as e:
        return {"success": False, "message": str(e)}


def install_server(config):
    """Download and install a Minecraft server.

    Config options:
        version (str): Minecraft version (e.g., "1.21.11")
        install_dir (str): Directory to install into
        jar_name (str): Name for server jar (default: "server.jar")
        accept_eula (bool): Auto-accept EULA (default: false)
        initial_settings (dict): Optional initial server.properties values
    """
    try:
        version_id = config.get("version")
        install_dir = config.get("install_dir")

        if not version_id:
            return {"success": False, "message": "No version specified"}
        if not install_dir:
            return {"success": False, "message": "No install_dir specified"}

        jar_name = config.get("jar_name", "server.jar")
        do_eula = config.get("accept_eula", False)
        initial_settings = config.get("initial_settings")

        return ServerInstaller.install_server(
            version_id=version_id,
            install_dir=install_dir,
            jar_name=jar_name,
            accept_eula=do_eula,
            initial_settings=initial_settings,
        )
    except Exception as e:
        import traceback
        print(traceback.format_exc(), file=sys.stderr)
        return {"success": False, "message": str(e)}


# ╔═══════════════════════════════════════════════════════════╗
# ║                      Main Entry                           ║
# ╚═══════════════════════════════════════════════════════════╝

FUNCTIONS = {
    "start": start,
    "stop": stop,
    "status": status,
    "command": command,
    "validate": validate,
    "get_launch_command": get_launch_command,
    "configure": configure,
    "read_properties": read_properties,
    "accept_eula": accept_eula,
    "diagnose_log": diagnose_log,
    "list_versions": list_versions,
    "get_version_details": get_version_details,
    "install_server": install_server,
}

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(json.dumps({"success": False,
                           "message": "Usage: lifecycle.py <function>"}))
        sys.exit(1)

    function_name = sys.argv[1]

    # Read config JSON from stdin (avoids command-line length limits
    # and prevents sensitive data from appearing in process listings)
    try:
        config_str = sys.stdin.read()
        config = json.loads(config_str) if config_str.strip() else {}
    except json.JSONDecodeError:
        print(json.dumps({"success": False, "message": "Invalid JSON config on stdin"}))
        sys.exit(1)

    fn = FUNCTIONS.get(function_name)
    if fn:
        result = fn(config)
    else:
        result = {"success": False, "message": f"Unknown function: {function_name}"}

    print(json.dumps(result))
