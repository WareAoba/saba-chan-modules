#!/usr/bin/env python3
"""
Minecraft 모듈: 서버 받기·설치(ServerInstaller). 바닐라 jar와 서버 종류별 설치기(server_types)를 다룬다.
lifecycle.py의 list_versions·get_version_details·install_server가 쓴다. stdout에는 쓰지 않는다 (로그는 stderr)
"""

import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile

from i18n import I18n

import server_types
from daemon_rcon import (ChecksumMismatch, download, force_kill, progress, remote_access_values, safe_join, setting_file_keys,
                         settings_to_file)
from java import JavaDetector, JavaDownloader, required_java
from properties import EulaManager, ServerPropertiesManager

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
i18n = I18n(MODULE_DIR)


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
        try:
            return server_types._get_json(VERSION_MANIFEST_URL)
        except Exception as e:
            raise ConnectionError(f"Failed to fetch version manifest: {e}")

    @staticmethod
    def list_versions(include_snapshots=False, page=1, per_page=25, only=None, manifest=None):
        """Return a paginated list of available Minecraft server versions.

        Args:
            include_snapshots: Include snapshot/pre-release versions
            page: Page number (1-based)
            per_page: Results per page
            only: 이 버전들만 (서버 종류가 받을 수 있는 것). 쪽을 나누기 전에 거른다
            manifest: 이미 받은 Mojang 목록 (없으면 받는다)

        Returns:
            dict with 'versions', 'latest', 'total', 'page', 'per_page', 'total_pages'
        """
        manifest = manifest or ServerInstaller.fetch_version_manifest()
        latest = manifest.get("latest", {})
        all_versions = manifest.get("versions", [])

        # Filter
        if not include_snapshots:
            versions = [v for v in all_versions if v.get("type") == "release"]
        else:
            versions = all_versions  # Already sorted newest-first by Mojang
        if only is not None:
            versions = [v for v in versions if v.get("id") in only]

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
        meta = server_types._get_json(version_entry["url"])

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
        installer = False
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
            installer = bool(source.get("installer"))
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
        # 해시가 있으면 해시로, 없으면(Fabric 런처) 크기와 jar 구조로 확인한다. 이름은 요청마다 다르다.
        # 공식 설치기는 서버 폴더 안의 설치기 폴더에 받아 거기서 돌리고, 끝나면 폴더째 지운다
        _remove_stale_downloads(install_path, jar_name)
        build_dir = None
        try:
            if installer:
                build_dir = ServerInstaller._make_build_dir(install_path)
                part_path = os.path.join(build_dir, "installer.jar")
            else:
                fd, part_path = tempfile.mkstemp(prefix=jar_name + ".", suffix=".download", dir=install_path)
                os.close(fd)
        except OSError as e:
            return {"success": False, "message": i18n.t("errors.download_failed", error=str(e))}
        if installer:
            try:
                return ServerInstaller._install_with_installer(
                    server_type, version_id, source, details, install_path, build_dir, part_path, jar_name,
                    accept_eula, initial_settings)
            finally:
                _remove_tree(build_dir)

        # Download with progress
        print(f"Downloading Minecraft server {version_id}...", file=sys.stderr)
        print(f"  URL: {download_url}", file=sys.stderr)
        print(f"  Destination: {jar_path}", file=sys.stderr)
        message = i18n.t("messages.downloading_server", version=version_id)
        shown = [-1]

        def report(done, total):
            pct = done * 100 // total if total else -1
            if pct != shown[0] and pct >= 0:
                shown[0] = pct
                progress(pct, message)

        expected = {"sha1": expected_sha1, "sha256": expected_sha256, "md5": expected_md5}
        try:
            digests = download(download_url, part_path, on_progress=report, timeout=120,
                               checksums=expected, user_agent="saba-chan/1.0")
            if not any(expected.values()):
                with zipfile.ZipFile(part_path) as z:
                    bad = z.testzip()
                if bad is not None:
                    raise IOError(f"corrupt jar entry: {bad}")
        except ChecksumMismatch as e:
            _remove_quietly(part_path)
            return {"success": False,
                    "message": i18n.t("errors.checksum_mismatch", algorithm=e.algorithm,
                                      expected=e.expected, actual=e.actual)}
        except Exception as e:
            # Clean up partial download
            _remove_quietly(part_path)
            return {
                "success": False,
                "message": i18n.t("errors.download_failed", error=str(e)),
            }
        actual_sha1 = digests["sha1"]
        downloaded = os.path.getsize(part_path)

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
        ServerInstaller._apply_first_settings(result, install_path, accept_eula, initial_settings)

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
                    result["warning"] = i18n.t("errors.java_download_warning", version=str(java_req),
                                               error=jre_result.get("message", ""))

        result["message"] = i18n.t("messages.server_installed", version=version_id, path=install_path)
        return result

    @staticmethod
    def _apply_first_settings(result, install_path, accept_eula, initial_settings):
        """설치한 서버 폴더에 EULA 동의와 처음 설정(server.properties)을 쓰고 result에 적는다"""
        # Accept EULA if requested
        if accept_eula:
            ok = EulaManager.accept_eula(install_path)
            result["eula_accepted"] = ok

        # Apply initial server settings
        if initial_settings and isinstance(initial_settings, dict):
            mgr = ServerPropertiesManager(install_path)
            # 설정 이름은 server.properties 키로, 그 밖의 키(이미 파일 키인 것)는 그대로
            keys = setting_file_keys(MODULE_DIR)
            try:
                props = {**{k: v for k, v in initial_settings.items() if k not in keys},
                         **settings_to_file(MODULE_DIR, initial_settings)}
            except ValueError:
                # 줄바꿈이 든 값: 서버 파일은 깔렸으니 설치는 성공으로 두고 설정만 적용하지 않았다고 알린다
                props = None
            if props is not None:
                props = {k: ("true" if v is True else "false" if v is False else str(v)) for k, v in props.items()}

                # 원격 접속 값은 사바쨩이 정한다 (module.toml [remote_access]: 실행 전에 관리형 여부로 다시 맞춘다)
                props.update(remote_access_values(MODULE_DIR))

                mgr.update(props)
            result["settings_applied"] = props is not None

    # 공식 설치기 한 번에 주는 시간 (데몬의 설치 한도 30분 안. BuildTools는 실측 4분, 처음 받는 PC는 더 걸린다)
    INSTALLER_TIMEOUT = 1500
    _BUILD_PREFIX = ".saba-install-"

    @staticmethod
    def _make_build_dir(install_path):
        """서버 폴더 안의 새 설치기 폴더. 지난번에 끊겨 한 시간 넘게 남은 설치기 폴더는 지운다
        (그보다 새것은 다른 설치가 쓰는 중일 수 있다). tempfile.mkdtemp 폴더는 격리 계정이 쓸 수 없어 직접 만든다"""
        import uuid
        cutoff = time.time() - _STALE_DOWNLOAD_SECONDS
        for name in os.listdir(install_path):
            path = os.path.join(install_path, name)
            try:
                stale = name.startswith(ServerInstaller._BUILD_PREFIX) and os.path.isdir(path) \
                    and not os.path.islink(path) and os.path.getmtime(path) < cutoff
            except OSError:
                stale = False
            if stale:
                _remove_tree(path)
        build_dir = os.path.join(install_path, ServerInstaller._BUILD_PREFIX + uuid.uuid4().hex[:12])
        for sub in ("tmp", "server"):
            os.makedirs(os.path.join(build_dir, sub))
        return build_dir

    @staticmethod
    def _run_installer_process(cmd, cwd, env):
        """설치기를 돌린다. env는 이 설치기에만 더할 환경 변수다 (나머지는 이 실행의 환경을 물려받는다).
        출력은 데몬 로그(stderr)로만 보낸다 (stdout은 응답 JSON 자리다).
        돌려주는 값: (종료 코드, 시간 초과면 None, 마지막 출력 줄들). 실행하지 못하면 OSError"""
        import collections
        import threading
        creationflags = 0x08000000 if os.name == "nt" else 0  # 콘솔 창을 띄우지 않는다
        saved = {key: os.environ.get(key) for key in env}
        os.environ.update(env)
        try:
            proc = subprocess.Popen(cmd, cwd=cwd, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, creationflags=creationflags)
        finally:
            for key, value in saved.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
        tail = collections.deque(maxlen=20)

        def pump():
            for raw in proc.stdout:
                line = raw.decode("utf-8", "replace").rstrip()
                if line.strip():
                    tail.append(line)
                print(f"  [installer] {line}", file=sys.stderr)

        reader = threading.Thread(target=pump, daemon=True)
        reader.start()
        try:
            code = proc.wait(timeout=ServerInstaller.INSTALLER_TIMEOUT)
        except subprocess.TimeoutExpired:
            force_kill(proc.pid, tree=True)
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                pass
            code = None
        reader.join(timeout=10)
        return code, list(tail)

    @staticmethod
    def _install_with_installer(server_type, version_id, source, details, install_path, build_dir, installer_jar,
                                jar_name, accept_eula, initial_settings):
        """공식 설치기(Quilt·Forge·NeoForge 설치기, BuildTools)를 받아 고정 판 포터블 Java로 돌린다.
        설치기는 서버 폴더 안의 build_dir에서 돌고 서버는 build_dir/server에 만든다. 메이븐 저장소와 임시 파일도
        build_dir에 둔다 (호출한 쪽이 지운다). 고른 종류·판의 서버가 생겼는지 확인한 뒤에만 서버 폴더로 옮긴다:
        설치가 실패하면 지금 쓰던 서버는 그대로다"""
        try:
            download(source["url"], installer_jar, timeout=120, user_agent="saba-chan/1.0",
                     checksums={"sha256": source.get("sha256"), "sha1": source.get("sha1"), "md5": source.get("md5")})
            if not (source.get("sha256") or source.get("sha1") or source.get("md5")):
                with zipfile.ZipFile(installer_jar) as z:  # BuildTools: 공식 체크섬이 없다
                    bad = z.testzip()
                if bad is not None:
                    raise IOError(f"corrupt jar entry: {bad}")
        except ChecksumMismatch as e:
            return {"success": False, "message": i18n.t("errors.checksum_mismatch", algorithm=e.algorithm,
                                                        expected=e.expected, actual=e.actual)}
        except Exception as e:
            return {"success": False, "message": i18n.t("errors.download_failed", error=str(e))}

        # 설치기와 서버는 PC에 깔린 Java가 아니라 버전을 고정한 포터블 Java로 돈다. 서버 폴더에 이미 있는 것이
        # 고정 판보다 새것이면 바꾼다 (옛 Forge·BuildTools는 너무 새 Java를 거절한다). 새 Java는 설치기 폴더에 받아
        # 두고 설치가 끝난 뒤에만 서버 폴더로 들인다: 설치가 실패하면 지금 서버의 Java도 그대로다
        # 받는 판은 설치기가 받는 범위(java_max, 아는 경우) 안에서 25에 가장 가까운 것이다. 범위를 모르면 최소를 넘는 가장 낮은 판
        java_req = max(details.get("java_major_version") or required_java(version_id), source.get("java_min") or 0)
        java_max = JavaDownloader.preferred_major(java_req, source.get("java_max")) or java_req
        jre = JavaDownloader.bundled_within(install_path, java_max, java_max)
        staged_java = None
        if not jre:
            staged = os.path.join(build_dir, "runtime")
            os.makedirs(staged, exist_ok=True)
            jre = JavaDownloader.download_jre(java_max, staged, max_major=java_max)
            staged_java = os.path.join(staged, "java")
        if not jre.get("success"):
            return {"success": False, "message": i18n.t("errors.java_download_failed", error=jre.get("message", ""))}
        runner = jre
        if source.get("jdk"):  # 컴파일하는 설치기(BuildTools)는 JDK가 든다: 설치기 폴더에 받아 쓰고 함께 지운다
            runner = JavaDownloader.download_jre(java_max, build_dir, image_type="jdk", max_major=java_max)
            if not runner.get("success"):
                return {"success": False, "message": i18n.t("errors.java_download_failed",
                                                            error=runner.get("message", ""))}

        target = os.path.join(build_dir, "server")
        args, cwd = server_types.installer_command(server_type, version_id, source, target, build_dir, jar_name)
        cmd = [runner["java_path"], f"-Djava.io.tmpdir={os.path.join(build_dir, 'tmp')}", "-jar", installer_jar, *args]
        env = {"MAVEN_OPTS": f"-Dmaven.repo.local={os.path.join(build_dir, 'm2')}"}
        progress(None, i18n.t("messages.running_installer", type=server_type, version=version_id))
        try:
            code, tail = ServerInstaller._run_installer_process(cmd, cwd, env)
        except OSError as e:
            return {"success": False, "message": i18n.t("errors.installer_failed", type=server_type, code="-",
                                                        detail=str(e))}
        if code != 0:
            key = "errors.installer_timeout" if code is None else "errors.installer_failed"
            return {"success": False, "message": i18n.t(key, type=server_type, code=str(code),
                                                        detail=_installer_reason(tail))}

        # Forge·NeoForge 설치기는 작업 폴더에 `<설치기 이름>.log`를 남긴다: 서버 폴더로 옮기지 않는다
        _remove_quietly(os.path.join(target, os.path.basename(installer_jar) + ".log"))
        name = server_types.installed_jar(server_type, source, target, jar_name)
        if name is None:
            return {"success": False, "message": i18n.t("errors.installer_no_server", type=server_type)}
        try:
            _move_into(target, install_path)
            if staged_java:
                JavaDownloader.swap_in(staged_java, os.path.join(install_path, "java"))
                jre = {**jre, "java_path": os.path.join(install_path, "java",
                                                        os.path.relpath(jre["java_path"], staged_java))}
            # 같은 폴더의 다른 Forge·NeoForge 빌드가 다음 실행을 가로채지 않게 한다
            server_types.retire_other_builds(server_type, source, install_path)
        except OSError as e:
            return {"success": False, "message": i18n.t("errors.download_failed", error=str(e))}
        jar_path = os.path.join(install_path, name) if name else None

        result = {
            "success": True,
            "install_path": install_path,
            "jar_path": jar_path,
            "jar_name": name or None,
            "server_type": server_type,
            "build": source.get("build"),
            "version": version_id,
            "java_major_version": java_req,
            "java_ok": True,
            "java_path": jre["java_path"],
            "java_version": jre.get("version", ""),
            "java_bundled": True,
        }
        # 1.17+ Forge·NeoForge는 실행 jar 없이 인자 파일로 뜬다 (server_types.launch_target)
        if jar_path:
            result["executable_path"] = jar_path
        ServerInstaller._apply_first_settings(result, install_path, accept_eula, initial_settings)
        result["message"] = i18n.t("messages.server_installed", version=version_id, path=install_path)
        return result


def _installer_reason(tail):
    """설치기 출력의 마지막 오류 줄 (사용자에게 실패 이유로 보인다). 없으면 마지막 줄"""
    errors = [line for line in tail if re.search(r"ERROR|Exception|error:|FAILURE", line)]
    line = (errors or tail or [""])[-1].strip()
    return line[:200]


def _remove_tree(path):
    """폴더를 지운다. 읽기 전용 파일(BuildTools가 받은 git 팩 파일, Windows)도 쓰기를 켜고 지운다"""
    import stat

    def retry(func, target, _exc):
        try:
            os.chmod(target, stat.S_IWRITE)
            func(target)
        except OSError:
            pass

    if sys.version_info >= (3, 12):
        shutil.rmtree(path, onexc=retry)
    else:
        shutil.rmtree(path, onerror=retry)


def _move_into(src, dst):
    """src 아래 모든 파일을 dst의 같은 자리로 옮긴다 (있으면 바꾼다). 설치기가 준비 폴더에 만든 서버를 서버 폴더로 넣는다"""
    for root, dirs, files in os.walk(src):
        rel = os.path.relpath(root, src)
        here = dst if rel == "." else os.path.join(dst, rel)
        os.makedirs(here, exist_ok=True)
        for name in files:
            os.replace(os.path.join(root, name), os.path.join(here, name))
