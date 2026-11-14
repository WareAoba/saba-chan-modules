#!/usr/bin/env python3
"""
Minecraft server lifecycle management module for saba-chan.

Provides complete server management:
  - Java environment detection and validation (java.py)
  - EULA management, server.properties read/write (properties.py)
  - Server download and install (installer.py)
  - Error detection and diagnosis with solutions
  - Server launch command generation (for Rust managed process)
  - Graceful shutdown (the daemon sends `stop` to the console; this module falls back to SIGTERM)
  - Server List Ping for rich status info

Always outputs JSON to stdout, logs to stderr.
"""

import sys
import json
import os
import re
import socket
import struct
import time
import zipfile
from pathlib import Path
from i18n import I18n

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))  # 같은 폴더의 server_types, plugins, java, properties, installer

import plugins
import server_types
from installer import ServerInstaller
from java import JavaDetector, JavaDownloader, required_java
from properties import EulaManager, ServerPropertiesManager, _default_properties
import daemon_rcon
from daemon_rcon import (PathOutside, auto_heap_mb, enforce_remote_access, graceful_stop, is_process_alive, rcon_access,
                         request_console, request_terminate, safe_join, setting_file_keys, settings_from_file,
                         settings_to_file, chat_reply, delete_paths, stop_command)

# ─── Init ─────────────────────────────────────────────────────

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
i18n = I18n(MODULE_DIR)


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

    Checks keys in order: server_executable → executable_path.
    If the path is relative, resolves it against working_dir (if set).
    Returns an absolute path string or None.
    """
    jar = config.get("server_executable") or config.get("executable_path")
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
    (소수는 M으로: 0.5 → 512M). 비었으면 PC에 맞춘 자동값(공용 auto_heap_mb), 읽을 수 없으면 2G"""
    if value is None or str(value).strip() == "":
        heap = auto_heap_mb()
        return f"{heap // 1024}G" if heap % 1024 == 0 else f"{heap}M"
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


def _heap_mb(size):
    """"4G"·"1024M"·"512K" → MB"""
    number, unit = int(size[:-1]), size[-1].upper()
    return {"K": number / 1024, "M": number, "G": number * 1024}[unit]


def _java_profile(size):
    """힙 크기로 정하는 실행 프로필: 6G 이하 small, 16G 미만 standard, 그 이상 large"""
    mb = _heap_mb(size)
    if mb <= 6 * 1024:
        return "small"
    return "standard" if mb < 16 * 1024 else "large"


# Aikar 옵션 (Paper 문서 권장 G1 설정, https://docs.papermc.io/paper/aikars-flags). 표준 프로필은 그대로 쓴다
_AIKAR_FLAGS = [
    "-XX:+UseG1GC", "-XX:+ParallelRefProcEnabled", "-XX:MaxGCPauseMillis=200", "-XX:+UnlockExperimentalVMOptions",
    "-XX:+DisableExplicitGC", "-XX:+AlwaysPreTouch", "-XX:G1NewSizePercent=30", "-XX:G1MaxNewSizePercent=40",
    "-XX:G1HeapRegionSize=8M", "-XX:G1ReservePercent=20", "-XX:G1HeapWastePercent=5", "-XX:G1MixedGCCountTarget=4",
    "-XX:InitiatingHeapOccupancyPercent=15", "-XX:G1MixedGCLiveThresholdPercent=90",
    "-XX:G1RSetUpdatingPauseTimePercent=5", "-XX:SurvivorRatio=32", "-XX:+PerfDisableSharedMem",
    "-XX:MaxTenuringThreshold=1",
]
# 소규모: 한가할 때 G1이 주기적으로 정리해 쓰지 않는 힙을 OS에 돌려준다 (JEP 346, JDK 12+). 그러려면 시작 힙(Xms)이 작아야 하고
# 힙 전체를 미리 잡는 AlwaysPreTouch는 뺀다. 간격은 1분: 그동안 GC가 한 번도 없었을 때만 돈다
_SMALL_START_HEAP = "1G"
_SMALL_FLAGS = [f for f in _AIKAR_FLAGS if f != "-XX:+AlwaysPreTouch"]
# 쉴 때 힙을 PC에 돌려준다 (JEP 346, JDK 12부터. 그 전 Java는 모르는 옵션이라 뜨지 않는다)
_PERIODIC_GC_FLAG = "-XX:G1PeriodicGCInterval=60000"
# 대규모: G1에 Aikar 문서의 12GB 초과용 값. PC 실측(Paper 봇 200개, 같은 16G 힙)에서 ZGC보다 틱이 10~15% 빨랐다
_LARGE_HEAP_VALUES = {
    "-XX:G1NewSizePercent": "40", "-XX:G1MaxNewSizePercent": "50", "-XX:G1HeapRegionSize": "16M",
    "-XX:G1ReservePercent": "15", "-XX:InitiatingHeapOccupancyPercent": "20",
}
_LARGE_FLAGS = [f"{name}={_LARGE_HEAP_VALUES[name]}" if name in _LARGE_HEAP_VALUES else flag
                for flag in _AIKAR_FLAGS for name in [flag.split("=")[0]]]
# 설정 java_gc = zgc: 세대별 ZGC (JEP 439, Java 21에서 -XX:+ZGenerational, 23부터 기본 JEP 474). 그 전 Java는 G1 프로필로
_ZGC_FLAGS = ["-XX:+UseZGC", "-XX:+AlwaysPreTouch", "-XX:+DisableExplicitGC", "-XX:+PerfDisableSharedMem"]
# Compact Object Headers: Java 25에서 정식 기능 (JEP 519), 27부터 기본
_COH_FLAG = "-XX:+UseCompactObjectHeaders"


def _java_tuning(config, ram, java_major, user_args):
    """힙 크기와 Java 주 버전으로 정한 메모리·GC 옵션. 설정 java_gc = zgc면(Java 21+) ZGC.
    사용자가 GC를 직접 고르면(-XX:+Use…GC) GC 옵션은 넣지 않는다. java_tuning을 끄면 힙 크기만 (Xms=Xmx)"""
    if config.get("java_tuning") is False:
        return [f"-Xmx{ram}", f"-Xms{ram}"]
    profile = _java_profile(ram)
    if config.get("java_gc") == "zgc" and java_major >= 21:
        profile = "zgc"
    user_gc = any(re.fullmatch(r"-XX:\+Use\w+GC", a) for a in user_args)
    if profile == "small":
        start = _SMALL_START_HEAP if _heap_mb(_SMALL_START_HEAP) < _heap_mb(ram) and not user_gc else ram
        periodic = [_PERIODIC_GC_FLAG] if java_major >= 12 else []
        flags = [f"-Xmx{ram}", f"-Xms{start}", *([] if user_gc else [*_SMALL_FLAGS, *periodic])]
    elif profile == "zgc":
        # 6G 이하는 소규모처럼 시작 힙을 작게 두고 힙을 미리 잡지 않는다 (ZGC는 쓰지 않는 메모리를 기본으로 돌려준다)
        small = _java_profile(ram) == "small" and _heap_mb(_SMALL_START_HEAP) < _heap_mb(ram)
        zgc = [f for f in _ZGC_FLAGS if not (small and f == "-XX:+AlwaysPreTouch")]
        gc = [] if user_gc else [*zgc, *(["-XX:+ZGenerational"] if java_major < 23 else [])]
        flags = [f"-Xmx{ram}", f"-Xms{_SMALL_START_HEAP if small else ram}", *gc]
    elif profile == "large":
        flags = [f"-Xmx{ram}", f"-Xms{ram}", *([] if user_gc else _LARGE_FLAGS)]
    else:
        flags = [f"-Xmx{ram}", f"-Xms{ram}", *([] if user_gc else _AIKAR_FLAGS)]
    if java_major >= 25:
        flags.append(_COH_FLAG)
    return flags


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
    이미 있으면 받지 않는다. 받지 못하면 {warning} (가져오기는 계속하고 시작할 때 Java 문제로 안내한다).
    데몬이 가져오기 표시(`migration_import`)를 실은 호출에서만 받는다: 그 호출만 설치와 같은 시간 한도와 인스턴스 폴더 쓰기를 받는다"""
    instance_dir = config.get("instance_dir")
    if not instance_dir or config.get("migration_import") is not True or (config.get("java_path") or "java") != "java":
        return {}
    server_jar = _resolve_server_jar(config)
    required = _required_java_for(config, server_jar)
    if not required or _portable_java(config, server_jar, required):
        return {}
    result = JavaDownloader.download_jre(required, instance_dir)
    if result.get("success"):
        return {}
    return {"warning": i18n.t("errors.java_download_warning", version=str(required), error=result.get("message", ""))}


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

    return {
        "success": len([i for i in issues if i["severity"] == "critical"]) == 0,
        "issues": issues,
        "java_info": java_info,
        "eula_accepted": eula_accepted,
    }


def _enforce_rcon_policy(working_dir, config=None):
    """
    RCON은 관리형 시작을 끈 서버에서만 켠다 (실행 직전마다). 쓸 값은 module.toml `[remote_access]` (공용 remote_access_values).

    관리형 시작은 saba-runner가 쥔 콘솔(stdin)로 명령을 보낸다: RCON을 끈다. 관리형 시작을 끄면 콘솔이 없으므로
    데몬이 만든 포트·비밀번호(config `rcon_port`·`rcon_password`)로 RCON을 켠다. 접속 정보가 없으면 끈다.
    """
    mgr = ServerPropertiesManager(working_dir)
    if not mgr.exists() and not rcon_access(config):
        return  # server.properties will be generated on first run (defaults keep RCON off)
    # 켤 때 파일이 없으면 기본값 위에 만든다 (ServerPropertiesManager.update)
    enforce_remote_access(MODULE_DIR, config, mgr.read(), mgr.update)


# 콘솔 입출력은 UTF-8로: 데몬은 stdin에 UTF-8을 쓰고 stdout을 UTF-8로 읽는다. JVM은 파이프의 문자 집합을
# OS 로캘(native.encoding)로 정해서, 로캘이 POSIX면 ASCII, 일본어 Windows면 MS932가 된다.
# 바닐라는 stdin을 UTF-8로 직접 읽지만 Paper 계열·Forge 콘솔은 stdout을, Spigot 계열은 stdin도 이 문자 집합으로 다뤄
# 한글 공지가 `???`로 바뀌었다 (클라우드 Linux 실측, 26.3).
# Paper 계열 콘솔(TerminalConsoleAppender)은 화면이 아닌 파이프에도 ANSI 색 코드를 넣어 `Seed: [\x1b[92m…`처럼
# 실패·답 모양과 로그 진단이 맞지 않는다. 데몬 콘솔은 색을 스스로 칠하므로(syntax_highlight) 색 코드를 끈다 (`terminal.ansi`)
# Java 17 이하(1.20.4 이하 서버)는 stdout.encoding 대신 sun.stdout.encoding을, stdin은 file.encoding을 본다
_CONSOLE_FLAGS = ["-Dstdin.encoding=UTF-8", "-Dstdout.encoding=UTF-8", "-Dstderr.encoding=UTF-8",
                  "-Dsun.stdout.encoding=UTF-8", "-Dsun.stderr.encoding=UTF-8", "-Dfile.encoding=UTF-8",
                  "-Dterminal.ansi=false"]

# 사바쨩이 스스로 정하는 JVM 옵션: 메모리, 실행 프로필의 GC 옵션, 콘솔 인코딩. 가져온 시작 스크립트와 Forge 인자 파일
# (user_jvm_args.txt)에서는 이것들을 빼고 나머지(자바 에이전트, -D 값 등)만 쓴다. 이름만 맞춰 본다(-XX:+X와 -XX:-X는 같은 옵션).
# 실험 옵션 잠금 풀기는 남긴다: 남은 다른 실험 옵션이 그것 없이는 JVM을 띄우지 못한다(겹쳐도 해가 없다)
_AIKAR_MARKERS = ["-Dusing.aikars.flags", "-Daikars.new.flags"]
_UNLOCK_OPTIONS = {"-XX:UnlockExperimentalVMOptions", "-XX:UnlockDiagnosticVMOptions"}


def _option_name(arg):
    return re.sub(r"^-XX:[+-]", "-XX:", arg.split("=", 1)[0])


_MANAGED_OPTIONS = {_option_name(f) for f in [*_AIKAR_FLAGS, *_LARGE_FLAGS, *_ZGC_FLAGS, _PERIODIC_GC_FLAG, _COH_FLAG,
                                              "-XX:+ZGenerational", *_CONSOLE_FLAGS, *_AIKAR_MARKERS]} - _UNLOCK_OPTIONS


def _managed_jvm_arg(arg):
    return bool(re.match(r"-X(mx|ms|mn)", arg) or re.fullmatch(r"-XX:[+-]Use\w+GC", arg)) or _option_name(arg) in _MANAGED_OPTIONS


def _split_args(line):
    """한 줄의 인자: 공백으로 나누고 따옴표로 묶은 것은 하나로, 인자 첫머리의 #부터는 주석 (Java 인자 파일 규칙)"""
    args, current, quote, started = [], [], None, False
    for ch in line:
        if quote:
            if ch == quote:
                quote = None
            else:
                current.append(ch)
        elif ch in "\"'":
            quote, started = ch, True
        elif ch == "#" and not started:
            break
        elif ch.isspace():
            if started:
                args.append("".join(current))
                current, started = [], False
        else:
            current.append(ch)
            started = True
    if started:
        args.append("".join(current))
    return args


def _read_text(path):
    try:
        with open(path, encoding="utf-8-sig", errors="replace") as f:
            return f.read()
    except OSError:
        return None


def _modpack_jvm_args(working_dir):
    """Forge·NeoForge 설치기가 만든 user_jvm_args.txt의 JVM 인자 (사바쨩이 정하는 것은 뺀다). 없으면 []"""
    text = _read_text(os.path.join(working_dir, "user_jvm_args.txt")) or ""
    return [a for line in text.splitlines() for a in _split_args(line) if not _managed_jvm_arg(a)]


def _script_java_args(working_dir):
    """가져온 서버가 쓰던 시작 스크립트의 실행 줄에서 java 다음, 실행 대상(-jar·-cp·@인자 파일) 앞의 인자. 없으면 None
    사용자가 만든 스크립트도 읽는다: 안내 문구(echo java …)처럼 실행 대상이 없는 줄은 건너뛰고, 메뉴처럼 실행 줄이 여럿이면 첫 줄"""
    for name in _START_SCRIPTS:
        if name == "user_jvm_args.txt":
            continue
        for line in (_read_text(os.path.join(working_dir, name)) or "").splitlines():
            if re.match(r"\s*(#|::|@?(rem|echo)\b)", line, re.IGNORECASE):  # 주석, 안내 문구
                continue
            args = [a.lstrip("(").rstrip(")") for a in _split_args(line)]
            # java 실행 파일, 또는 그것을 담은 변수(%JAVA%, $JAVA). 뒤에 실행 대상이 있어야 실행 줄로 본다
            start = next((i for i, a in enumerate(args)
                          if re.fullmatch(r"javaw?(\.exe)?", re.split(r"[\\/]", a)[-1], re.IGNORECASE)
                          or re.fullmatch(r"%\w+%|\$\{?\w+\}?", a)), None)
            if start is None:
                continue
            rest = args[start + 1:]
            end = next((i for i, a in enumerate(rest) if a in ("-jar", "-cp", "-classpath") or a.startswith("@")), None)
            if end is not None:
                return rest[:end]
    return None


def _script_jvm_args(working_dir):
    """쓰던 시작 스크립트의 JVM 인자 중 사바쨩이 정하지 않는 것. 없으면 []
    변수가 남은 인자(%RAM%, $@)는 값을 모르므로 옮기지 않는다. 인자 파일(@…)은 실행 때 따로 읽는다"""
    return [a for a in _script_java_args(working_dir) or []
            if a.startswith("-") and not re.search(r"[%$]", a) and not _managed_jvm_arg(a)]


def get_launch_command(config):
    """
    Build the command for the Rust daemon to spawn as a ManagedProcess.
    Returns { success, program, args, working_dir, env_vars }.
    """
    java_path = config.get("java_path", "java")
    server_jar = _resolve_server_jar(config)
    ram = _java_memory(config.get("ram"))
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

    java = _launch_java_info(config, server_jar)
    java_path = java["path"] if java else config.get("java_path", "java")

    # 사용자가 더한 JVM 인자(jvm_args·데몬의 extra_args)
    user_args = list(extra_jvm_args) if isinstance(extra_jvm_args, list) else str(extra_jvm_args or "").split()
    extra_args = config.get("extra_args") or []
    user_args += [str(a) for a in extra_args] if isinstance(extra_args, list) else []

    # Forge·NeoForge 인자 파일로 뜨는 서버는 설치기의 run.bat처럼 모드팩의 user_jvm_args.txt도 넣는다 (사용자 인자가 뒤에서 이긴다)
    modpack_args = _modpack_jvm_args(working_dir) if target[0].startswith("@") else []
    args = [*_java_tuning(config, ram, (java or {}).get("major_version") or 0, user_args), *_CONSOLE_FLAGS,
            *modpack_args, *user_args]
    # 사용자가 더한 실행 인자는 JVM 인자 자리, 실행 대상(-jar 등) 앞에 둔다
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

    # server.properties 키(module.toml file_key)가 없는 것(ram, java_path 같은 사바쨩 설정)은 쓰지 않는다
    try:
        props_changes = settings_to_file(MODULE_DIR, settings)
    except ValueError:
        return {"success": False, "message": i18n.t("errors.properties_write_failed")}

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
    """쓰던 실행 스크립트의 -Xmx ("10G", "3072M"). Forge user_jvm_args.txt가 먼저, 다음은 시작 스크립트의 실행 줄.
    없거나 실행할 때 입력받는 값(-Xmx%RAM%G)이면 None (모듈 기본값 그대로)"""
    text = _read_text(os.path.join(working_dir, "user_jvm_args.txt")) or ""
    args = [a for line in text.splitlines() for a in _split_args(line)] + (_script_java_args(working_dir) or [])
    for arg in args:
        m = re.fullmatch(r"-Xmx(\d+[KMGkmg])", arg)
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
    script_args = _script_jvm_args(working_dir)
    if script_args:
        extra["extra_args"] = script_args  # 데몬이 인스턴스의 추가 실행 인자로 저장한다 (JVM 인자 자리에 들어간다)
    extra.update(_fetch_portable_java(config))

    result = read_properties(config)
    if not result.get("success"):
        return result

    raw_props = result.get("properties", {})
    if not raw_props:
        return {"success": True, "settings": found, "message": i18n.t("messages.no_properties_found"), **extra}

    # 설정 화면에 없는 키는 이름 그대로 넘긴다. 있는 키는 스키마 타입대로만 바꾼다:
    # 숫자 같은 비밀번호("000123")나 이름은 문자열 그대로
    known = set(setting_file_keys(MODULE_DIR).values())
    settings = {key: value for key, value in raw_props.items() if key not in known}
    settings.update(settings_from_file(MODULE_DIR, raw_props))
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
            "properties": _default_properties(),
            "message": i18n.t("messages.properties_using_defaults"),
        }
    return {"success": True, "exists": True, "properties": mgr.read()}


def reset_properties(config):
    """Reset server.properties to default values."""
    working_dir = config.get("working_dir")
    if not working_dir:
        return {"success": False, "message": i18n.t("errors.no_working_dir")}

    mgr = ServerPropertiesManager(working_dir)
    defaults = _default_properties()
    # 포트는 이 서버의 값이다: 기본값(25565)으로 쓰면 데몬이 인스턴스 포트까지 되돌려 다른 서버와 겹친다
    if config.get("port"):
        defaults["server-port"] = str(config["port"])
    success = mgr.write(defaults)
    return {
        "success": success,
        "message": i18n.t("messages.properties_reset") if success
                   else i18n.t("errors.properties_write_failed"),
    }


# 서버 초기화가 지우는 것: 서버(바닐라·Bukkit 계열·모드 로더)가 서버 폴더에 만드는 월드 밖 항목
# 초기화가 지우는 서버 생성 항목, 미리 보기에 보이는 묶음별 (이름은 locales `reset.items.<묶음>`). 월드는 level-name으로 따로 구한다
_RESET_GROUPS = (
    ("settings", ("server.properties",)),
    ("lists", ("ops.json", "whitelist.json", "banned-players.json", "banned-ips.json")),
    ("addons", ("plugins", "mods", "config", "permissions.yml", "commands.yml", "help.yml",
                "bukkit.yml", "spigot.yml", "paper.yml", "purpur.yml", "wepif.yml")),
    ("logs", ("logs", "crash-reports", "cache", "debug", "usercache.json", "version_history.json", ".console_history")),
)
# 초기화 범위와 그 범위가 지우는 묶음 (None이면 전부). 범위 없이 부르면 "all"
_RESET_SCOPES = (("world", ("world",)), ("all", None))


# 초기화가 지우지 않는 것: 서버를 띄우는 파일·폴더(.jar도)와 인스턴스 폴더의 데몬 파일
_RESET_KEEP = {
    "eula.txt", "user_jvm_args.txt", "run.bat", "run.sh",
    "fabric-server-launcher.properties", "quilt-server-launcher.properties",
    "java", "libraries", "versions", ".fabric", ".quilt",
    "instance.json", "settings.json", "settings.json.tmp", "managed.pid", "run", "backups", "rcon.json", "rcon.json.tmp", "migration",
}


def _reset_world_paths(config, props, working_path, keep):
    """지울 월드 폴더(서버 폴더 기준 상대 경로)와 다른 차원 폴더. level-name이 폴더 안을 가리키지 않거나
    남길 항목과 겹치면 None"""
    name = props.get("level-name", "") or str(config.get("level_name") or "") or "world"
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


def _reset_plan(config):
    """초기화가 지울 것: [(묶음 이름, [서버 폴더 기준 경로])] (월드가 맨 앞). 지울 수 없는 폴더면 실패 응답(dict)

    서버 폴더(server.properties나 eula.txt가 있는 폴더)에서 서버가 만든 항목만 지운다.
    사용자가 둔 다른 파일과 데몬 파일(인스턴스 폴더에 서버를 둔 경우)은 남긴다.
    """
    working_dir = config.get("working_dir")
    if not working_dir:
        return {"success": False, "message": i18n.t("errors.no_working_dir")}

    working_path = Path(working_dir)
    if not working_path.is_dir():
        return {"success": False, "message": i18n.t("errors.no_working_dir")}
    if not any((working_path / name).is_file() for name in ("server.properties", "eula.txt")):
        return {"success": False, "message": i18n.t("errors.not_a_server_folder", path=str(working_path))}

    keep = set(_RESET_KEEP)
    groups = [(name, list(paths)) for name, paths in _RESET_GROUPS]
    # 인스턴스 폴더에 서버를 두면 logs/는 데몬 로그와 같은 폴더다
    if (working_path / "instance.json").is_file():
        keep.add("logs")
        dict(groups)["logs"].remove("logs")
    # 월드를 못 찾았는데 나머지만 지우고 성공이라 하면 다음 시작에 옛 월드가 그대로 뜬다
    props = ServerPropertiesManager(working_dir).read()
    worlds = _reset_world_paths(config, props, working_path, keep)
    if worlds is None:
        name = props.get("level-name") or config.get("level_name") or ""
        return {"success": False, "message": i18n.t("errors.unsafe_world_name", name=str(name))}
    return [("world", worlds)] + groups


def reset_preview(config):
    """초기화 확인 창에 보일 것 (데몬 `GET /server/reset`): 범위마다 지울 항목. 아무것도 지우지 않는다.
    크기와 지금 있는지는 데몬이 본다"""
    plan = _reset_plan(config)
    if isinstance(plan, dict):
        return plan
    scopes = []
    for scope, names in _RESET_SCOPES:
        items = [{"label": i18n.t(f"reset.items.{name}"), "paths": paths}
                 for name, paths in plan if names is None or name in names]
        scopes.append({"id": scope, "label": i18n.t(f"reset.scopes.{scope}"), "items": items})
    return {"success": True, "scopes": scopes}


# 플레이어 목록 (`[[lists]]` id → 서버가 서버 폴더에 두는 파일. PaperMC 문서 "Vanilla data files")
_PLAYER_LISTS = (("whitelist", "whitelist.json"), ("ops", "ops.json"), ("banned", "banned-players.json"))
# `ban`에 이유를 주지 않으면 서버가 적는 문구 (같은 문서 banned-players.json의 reason 기본값): 이유로 보이지 않는다
_DEFAULT_BAN_REASON = "Banned by an operator."


def _list_entry(entry):
    """목록 파일의 한 줄 → `{name, added?, note?}`. 이름이 없으면 None"""
    if not isinstance(entry, dict) or not isinstance(entry.get("name"), str) or not entry["name"]:
        return None
    out = {"name": entry["name"]}
    created = entry.get("created")
    if isinstance(created, str):
        # 서버가 적는 모양은 `yyyy-MM-dd HH:mm:ss Z` (예: 2026-11-09 21:03:11 +0900)
        try:
            out["added"] = time.strftime("%Y-%m-%dT%H:%M:%S", time.strptime(created[:19], "%Y-%m-%d %H:%M:%S"))
            zone = re.fullmatch(r" ([+-]\d{2})(\d{2})", created[19:])
            if zone:
                out["added"] += f"{zone.group(1)}:{zone.group(2)}"
        except ValueError:
            pass
    reason = entry.get("reason")
    if isinstance(reason, str) and reason and reason != _DEFAULT_BAN_REASON:
        out["note"] = reason
    return out


def read_lists(config):
    """플레이어 목록 화면에 보일 지금 명단 (데몬 `GET /lists`). 서버가 쓰는 파일을 읽기만 한다.
    넣기·빼기는 `[[lists]]`가 가리키는 명령으로 켜진 서버가 한다 (파일은 서버가 고친다)"""
    working_dir = config.get("working_dir")
    if not working_dir or not os.path.isdir(working_dir):
        return {"success": False, "message": i18n.t("errors.no_working_dir")}
    lists = {}
    for list_id, name in _PLAYER_LISTS:
        try:
            with open(os.path.join(working_dir, name), encoding="utf-8") as f:
                entries = json.load(f)
        except (OSError, ValueError):
            entries = []
        found = [_list_entry(e) for e in entries] if isinstance(entries, list) else []
        lists[list_id] = [e for e in found if e]
    return {"success": True, "lists": lists}


def reset_server(config):
    """Server reset: delete the worlds and settings the server made, keep what starts the server
    (jars, eula.txt, the bundled java/, loader files of Forge/NeoForge/Fabric/Quilt).

    `scope`(`reset_preview`가 밝힌 범위)가 "world"면 월드만, 없거나 "all"이면 전부 지운다.
    server.properties will be recreated from module defaults on next configure().
    """
    scope = config.get("scope") or "all"
    if scope not in dict(_RESET_SCOPES):
        return {"success": False, "message": i18n.t("errors.unknown_reset_scope", scope=str(scope))}
    plan = _reset_plan(config)
    if isinstance(plan, dict):
        return plan
    names = dict(_RESET_SCOPES)[scope]
    targets = [path for name, paths in plan if names is None or name in names for path in paths]

    deleted, errors = delete_paths(targets, root=Path(config["working_dir"]))

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
    """서버 로그 진단. 규칙은 module.toml [[diagnose]], 콘솔 줄이 없을 때 읽을 파일(logs/latest.log)은 [launch] diagnose_files"""
    return daemon_rcon.diagnose_log(config, MODULE_DIR, i18n)


def _request_stop(config):
    """저장 후 종료 요청: 콘솔 `stop_command`(stop, 관리형 시작을 끄면 데몬이 RCON으로 보낸다) → SIGTERM"""
    return request_console(config, stop_command(MODULE_DIR)) or request_terminate(config.get("pid"))


def stop(config):
    """서버 정지. 공용 절차(graceful_stop): 저장 후 종료를 요청하고, force이거나 기다려도 안 끝날 때만 강제 종료한다.
    요청할 길이 없으면 끄지 않고 `graceful_stop_unavailable`로 답한다.
    데몬은 콘솔이 있는 managed 서버를 직접 멈추므로, 이 함수는 콘솔이 없을 때 주로 불린다."""
    return graceful_stop(config, lambda: _request_stop(config), i18n, module_dir=MODULE_DIR)


def _ping_host(config):
    """Server List Ping을 보낼 주소. server.properties `enable-status=false`면 서버가 답하지 않으므로 None"""
    props = ServerPropertiesManager(config.get("working_dir", "")).read() if config.get("working_dir") else {}
    if props.get("enable-status", "").lower() == "false":
        return None
    # server-ip로 한 주소에만 묶었으면 127.0.0.1로는 닿지 않는다
    bound = props.get("server-ip", "")
    return bound if bound not in ("", "0.0.0.0", "::") else config.get("host", "127.0.0.1")


def status(config):
    """Enhanced status with Server List Ping."""
    try:
        pid = config.get("pid")
        port = config.get("port", 25565)
        host = _ping_host(config)

        ping_result = MinecraftPing.ping(host, int(port), timeout=3) if host else None
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
            if not host:
                # 핑에 답하지 않는 설정: 시작 중과 가를 수 없어 프로세스가 살아 있으면 실행 중으로 본다
                return {"success": True, "status": "running", "pid": pid}
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
    host = _ping_host(config)
    if not host:
        # 핑에 답하지 않는 설정: 수는 모르지만 실패가 아니다 (실패로 답하면 데몬이 응답 없음으로 알린다)
        return {"success": True}
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
_PLAYER_NAME_RE = re.compile(r"[A-Za-z0-9_.\-]{1,16}")


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
    """
    jar_path = _resolve_server_jar(config)
    install_dir = config.get("working_dir") or (os.path.dirname(jar_path) if jar_path else "")

    if not install_dir:
        return {"success": False, "message": i18n.t("errors.no_install_dir")}

    jar_path = jar_path or os.path.join(install_dir, "server.jar")
    install_dir, jar_name = os.path.split(jar_path)
    if not os.path.isfile(jar_path):
        found = server_types.find_server_jar(install_dir)
        if not found:
            # 1.17+ Forge·NeoForge는 실행 jar 없이 인자 파일로 뜬다: 로더 버전이 마인크래프트 버전을 알려 준다
            loader = server_types.detect(install_dir)
            if loader.get("version") and server_types.family(loader["type"]) == "mod":
                return {"success": True, "version": loader["version"], "server_type": loader["type"]}
            return {"success": False, "message": i18n.t("errors.server_jar_not_found", path=jar_path)}
        jar_path, jar_name = found, os.path.basename(found)

    info = _read_jar_version(install_dir, jar_name)
    if info:
        return {"success": True, **info}
    # 페이퍼 등 Paperclip jar는 version.json 대신 versions.list에 버전이 있다
    forked = server_types.inspect_jar(jar_path)
    if forked and forked.get("version"):
        return {"success": True, "version": forked["version"], "server_type": forked["type"]}
    # 로더 런처(Quilt, Forge shim)는 jar에 버전이 없다: 서버 폴더로 판정한다
    if forked and server_types.family(forked["type"]) == "mod":
        loader = server_types.detect(install_dir, jar_path)
        if loader.get("version"):
            return {"success": True, "version": loader["version"], "server_type": loader["type"]}
    return {"success": False, "message": i18n.t("errors.version_info_unreadable")}


def command(config):
    """정의에 없는 콘솔 한 줄. module.toml 명령은 전부 console이라 데몬이 직접 보내고, 켜진 서버에 콘솔도 RCON도
    없을 때만 그 한 줄을 이 함수로 넘긴다 (saba-chan src/ipc/handlers/command.rs Plan::Raw). 데몬에 그대로 맡기고,
    보낼 길이 없다는 코드(no_command_path)를 답에 실어 화면·디스코드가 자기 문구로 안내하게 한다"""
    from daemon_rcon import daemon_stdin_command
    line = config.get("command", "")
    try:
        daemon_stdin_command(config.get("instance_id", ""), line, timeout=5)
    except PathOutside:
        raise
    except Exception as e:
        return {"success": False, "message": str(e), "error_code": getattr(e, "error_code", None)}
    return {"success": True, "message": line}


# § 서식 코드(§ 뒤 한 글자, 끝에 홀로 남은 § 포함)
_SECTION_CODE = re.compile(r"§.?", re.S)


def send_chat(config):
    """디스코드 → 게임 채팅 (`[extensions.saba-core] chat_send`). 데몬이 한 줄로 다듬은 이름·본문을 받는다.
    본문은 tellraw JSON 글자로 넣어 명령으로 읽히지 않는다. 클라이언트는 JSON 글자 안의 § 서식도 읽으므로 § 코드는 뺀다"""
    from daemon_rcon import daemon_stdin_command
    name, text = (_SECTION_CODE.sub("", config.get(k, "")) for k in ("chat_name", "chat_text"))
    line = "tellraw @a " + json.dumps(["", {"text": f"<{name}> ", "color": "aqua"}, {"text": text}], ensure_ascii=False)
    return chat_reply(lambda: daemon_stdin_command(config.get("instance_id", ""), line, timeout=5), i18n)


# ╔═══════════════════════════════════════════════════════════╗
# ║              Server Installation / Download               ║
# ╚═══════════════════════════════════════════════════════════╝

def list_versions(config):
    """List available Minecraft server versions.

    Config options:
        include_snapshots (bool): Include snapshots/pre-releases (default: false)
        page (int): Page number, 1-based (default: 1)
        per_page (int): Results per page (default: 25)
        settings (dict): 인스턴스 설정. `server_type`이 있으면 목록은 그 종류가 받을 수 있는 버전만이고
            `latest.release`는 그중 받을 수 있는 가장 새 릴리스다(그보다 새 것은 받아 보니 아직 없어 목록에서도 뺀다).
            만들기 화면과 데몬이 새 인스턴스의 버전을 고를 때 넘긴다.
            그 종류의 받을 곳을 읽지 못하면 거르지 않는다 (설치가 그 버전을 다시 확인한다)
    """
    try:
        include_snapshots = config.get("include_snapshots", False)
        page = int(config.get("page", 1))
        per_page = int(config.get("per_page", 25))

        server_type = (config.get("settings") or {}).get("server_type")
        typed = bool(server_type) and server_type != "vanilla"
        only = None
        if typed:
            try:
                only = server_types.installable_versions(server_type)
            except Exception as e:
                print(f"Could not list the versions {server_type} offers: {e}", file=sys.stderr)
        manifest = ServerInstaller.fetch_version_manifest()
        latest = None
        if typed:
            releases = [v["id"] for v in ServerInstaller.list_versions(per_page=10, only=only, manifest=manifest)["versions"]]
            latest = server_types.latest_installable(server_type, releases)
            # 그보다 새 릴리스는 받아 보니 아직 받을 수 없었다 (Paper·Folia의 안정 빌드 전): 목록에서도 뺀다
            if latest and only is not None:
                only = only - set(releases[:releases.index(latest)])
        result = ServerInstaller.list_versions(
            include_snapshots=include_snapshots,
            page=page,
            per_page=per_page,
            only=only,
            manifest=manifest,
        )
        if latest:
            result["latest"] = {**result.get("latest", {}), "release": latest}
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
        server_executable (str): Name for server jar (default: "server.jar")
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

        jar_name = os.path.basename(config.get("server_executable") or "server.jar")
        do_eula = "eula" in (config.get("accepted_agreements") or [])
        settings = config.get("settings") or {}
        # 데몬이 넘긴 인스턴스 설정(프리셋·옮긴 포트) 중 server.properties 키만 처음 값으로 쓴다
        file_keys = setting_file_keys(MODULE_DIR)
        initial_settings = {k: v for k, v in settings.items() if k in file_keys and v is not None}
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
    "send_chat": send_chat,
    "validate": validate,
    "get_launch_command": get_launch_command,
    "configure": configure,
    "read_properties": read_properties,
    "import_settings": import_settings,
    "config_files": config_files,
    "reset_properties": reset_properties,
    "reset_server": reset_server,
    "reset_preview": reset_preview,
    "read_lists": read_lists,
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

if __name__ == "__main__":
    # 공용 진입점: 깨진 stdin·모르는 함수·함수 안의 예외도 JSON으로 답하고 종료 코드 0을 지킨다
    from daemon_rcon import run_lifecycle
    run_lifecycle(FUNCTIONS)
