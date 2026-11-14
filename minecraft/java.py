#!/usr/bin/env python3
"""
Minecraft 모듈: Java 찾기(JavaDetector)와 포터블 JRE 받기(JavaDownloader).
lifecycle.py·installer.py가 쓴다. stdout에는 쓰지 않는다 (로그는 stderr)
"""

import json
import os
import re
import shutil
import subprocess
import sys
import time

from i18n import I18n

import server_types
from daemon_rcon import ChecksumMismatch, download, java_release_major, progress, safe_extract

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
i18n = I18n(MODULE_DIR)


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

    # Adoptium API v3 — 고정한 판의 플랫폼별 JRE 압축 주소와 SHA-256을 준다
    _API_BASE = "https://api.adoptium.net/v3"

    # 주 버전별로 고정한 Temurin 판 (PC마다 같은 판을 받는다). 요청한 주 버전 이상인 가장 낮은 판을 쓴다 (16 → 17).
    # 판을 올릴 때는 windows x64, linux x64·aarch64, mac x64·aarch64에 JRE가 있는 판을 고른다 (8은 mac aarch64 없음)
    _RELEASES = {
        8: "jdk8u504-b01",
        17: "jdk-17.0.20.1+1",
        21: "jdk-21.0.12.1+1",
        25: "jdk-25.0.5+8",
    }

    @staticmethod
    def pinned_major(major_version):
        """major_version 이상인 고정 판 중 가장 낮은 주 버전 (없으면 None)"""
        return next((m for m in sorted(JavaDownloader._RELEASES) if m >= major_version), None)

    # 받을 Java를 고를 때 가까이 가려는 주 버전 (Compact Object Headers 등 최신 LTS의 기능을 쓰려고)
    PREFERRED_MAJOR = 25

    @staticmethod
    def preferred_major(min_major, max_major=None):
        """min_major 이상 max_major 이하의 고정 판 중 PREFERRED_MAJOR에 가장 가까운(넘지 않는 가장 높은) 주 버전.
        서버가 받는 최대(max_major)를 모르면 min_major 이상인 가장 낮은 고정 판. 맞는 것이 없으면 None"""
        lowest = JavaDownloader.pinned_major(min_major)
        if lowest is None or not max_major:
            return lowest
        top = min(max_major, max(JavaDownloader.PREFERRED_MAJOR, lowest))
        fits = [m for m in JavaDownloader._RELEASES if min_major <= m <= top]
        return max(fits) if fits else None

    @staticmethod
    def _pinned_release(major_version):
        for major in sorted(JavaDownloader._RELEASES):
            if major >= major_version:
                return JavaDownloader._RELEASES[major]
        return None

    @staticmethod
    def _release_package(release, os_name, arch, image_type="jre"):
        """고정 판 release의 이 플랫폼 JRE(또는 JDK) 압축 정보({link, checksum, ...}). 이 플랫폼용이 없으면 None"""
        import urllib.parse
        url = (
            f"{JavaDownloader._API_BASE}/assets/release_name/eclipse/{urllib.parse.quote(release, safe='')}?"
            + urllib.parse.urlencode({"os": os_name, "architecture": arch, "image_type": image_type,
                                      "jvm_impl": "hotspot", "heap_size": "normal", "project": "jdk"})
        )
        print(f"  Release: {url}", file=sys.stderr)
        try:
            data = server_types._get_json(url, timeout=30)
        except ValueError as e:
            if str(e) == "version_unavailable":  # 404: 이 플랫폼용 JRE가 없는 판
                return None
            raise
        for binary in data.get("binaries") or []:
            package = binary.get("package") or {}
            if (binary.get("os"), binary.get("architecture"), binary.get("image_type")) == (os_name, arch, image_type) \
                    and str(package.get("link", "")).startswith("https://") and package.get("checksum"):
                return package
        return None

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
    def download_jre(major_version, install_dir, image_type="jre", max_major=None):
        """Download and extract a portable JRE into <install_dir>/java/.

        Args:
            major_version: Java major version (e.g., 21, 17)
            install_dir: Server install directory
            image_type: "jre", 또는 컴파일하는 설치기(BuildTools)에 줄 "jdk"
            max_major: 이보다 새 Java는 쓰지 않는다 (있는 것이 더 새것이면 고정 판으로 바꾼다)

        Returns:
            dict with success, java_path, java_home, version
        """
        os_name, arch = JavaDownloader._platform_info()
        java_dir = os.path.join(install_dir, "java")

        # Check if already downloaded
        existing = JavaDownloader.bundled_within(install_dir, major_version, max_major)
        if existing:
            return existing

        # Determine archive type
        ext = "zip" if os_name == "windows" else "tar.gz"

        # 고정 판의 이 플랫폼 압축과 SHA-256: 해시가 다르면 풀지 않는다
        unavailable = {"success": False, "message": i18n.t(
            "errors.java_release_unavailable", version=str(major_version), platform=f"{os_name} {arch}")}
        release = JavaDownloader._pinned_release(major_version)
        if not release or (max_major and JavaDownloader.pinned_major(major_version) > max_major):
            return unavailable
        print(f"Downloading Java {major_version} JRE ({release}) from Adoptium...", file=sys.stderr)
        try:
            package = JavaDownloader._release_package(release, os_name, arch, image_type)
        except Exception as e:
            return {"success": False, "message": i18n.t("errors.java_download_failed", error=str(e))}
        if not package:
            return unavailable
        url = package["link"]
        print(f"  URL: {url}", file=sys.stderr)

        tmp_file = os.path.join(install_dir, f"java-jre.{ext}")
        message = i18n.t("messages.downloading_java", version=str(major_version))
        shown = [-1]

        def report(done, total):
            pct = done * 100 // total if total else -1
            if pct != shown[0] and pct >= 0:
                shown[0] = pct
                progress(pct, message)

        try:
            download(url, tmp_file, on_progress=report, timeout=300,
                     checksums={"sha256": package["checksum"]}, user_agent="saba-chan/1.0")
        except ChecksumMismatch as e:
            return {"success": False, "message": i18n.t("errors.checksum_mismatch", algorithm=e.algorithm,
                                                         expected=e.expected, actual=e.actual)}
        except Exception as e:
            return {"success": False, "message": i18n.t("errors.java_download_failed", error=str(e))}

        # Extract: 임시 폴더에 다 푼 뒤에만 java/를 바꾼다 (중간 실패가 반쯤 풀린 Java를 남기지 않게).
        # Adoptium 압축은 맨 위에 "jdk-21.0.2+13-jre" 같은 폴더 하나가 있다. 폴더 밖으로 나가는 항목·링크는 풀지 않는다
        final_dir = java_dir
        java_dir = final_dir + ".part"
        print(f"  Extracting to {final_dir}...", file=sys.stderr)
        shutil.rmtree(java_dir, ignore_errors=True)
        os.makedirs(java_dir, exist_ok=True)

        try:
            safe_extract(tmp_file, java_dir, strip_top=True)
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

        try:
            JavaDownloader.swap_in(java_dir, final_dir)
        except OSError as e:
            shutil.rmtree(java_dir, ignore_errors=True)
            return {"success": False, "message": i18n.t("errors.java_extract_failed", error=str(e))}
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
    def bundled_within(install_dir, major_version, max_major=None):
        """<install_dir>/java/의 Java가 major_version 이상 max_major 이하이면 download_jre와 같은 응답, 아니면 None"""
        java_dir = os.path.join(install_dir, "java")
        existing = JavaDownloader._find_bundled_java(java_dir)
        info = JavaDetector.get_java_info(existing) if existing else None
        if info and major_version <= info["major_version"] <= (max_major or info["major_version"]):
            return {
                "success": True,
                "java_path": existing,
                "java_home": java_dir,
                "version": info["version"],
                "already_exists": True,
            }
        return None

    @staticmethod
    def swap_in(new_dir, final_dir):
        """new_dir을 final_dir 자리로 들인다. 옛 것은 먼저 옆으로 옮긴다: 쓰는 중이라 옮기지 못하면 손대지 않고
        OSError를 낸다. 새 것을 들이지 못하면 옛 것을 되돌린다"""
        old_dir = final_dir + ".old"
        shutil.rmtree(old_dir, ignore_errors=True)
        had_old = os.path.exists(final_dir)
        if had_old:
            os.replace(final_dir, old_dir)
        try:
            os.replace(new_dir, final_dir)
        except OSError:
            if had_old:
                os.replace(old_dir, final_dir)
            raise
        shutil.rmtree(old_dir, ignore_errors=True)

    @staticmethod
    def bundled_major(java_dir, java_exe):
        """번들 Java의 주 버전: JRE의 `release` 파일(실행하지 않는다, 공용 java_release_major), 없으면 `-version`. 모르면 0"""
        major = java_release_major(java_dir)
        if major:
            return major
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
