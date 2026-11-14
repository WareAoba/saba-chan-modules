#!/usr/bin/env python3
"""
saba-chan — Project Zomboid Module Lifecycle

Manages the lifecycle of a Project Zomboid dedicated server.
Handles validation, launch command generation, status checks,
graceful shutdown, settings synchronization, and installation via SteamCMD.

Project Zomboid is a Java-based game server (Steam App 380870) that:
  - Uses a bundled JRE (jre64/bin/java.exe) with custom classpath
  - Stores configs in ~/Zomboid/Server/{servername}.ini (separate from install dir)
  - Takes commands on its console (stdin) with managed start; with managed start off, through RCON on the daemon-made port and password
"""

import sys
import json
import subprocess
import os
import re
import sqlite3
import hashlib
from pathlib import Path

# ─── Path setup ──────────────────────────────────────────

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))

from i18n import I18n

import daemon_rcon
from daemon_rcon import (PathOutside, safe_join, auto_heap_mb, daemon_stdin_command, enforce_remote_access, extension_required,
                         graceful_stop, java_release_major, list_dir, rcon_access, read_key_values, remote_access_values,
                         request_console, request_terminate, setting_file_keys,
                         delete_paths, setting_defaults_to_file, stop_command,
                         settings_to_file, update_key_values)

i18n = I18n(MODULE_DIR)


# ─── Constants ───────────────────────────────────────────



# ═══════════════════════════════════════════════════════════
#                    INI File Manager
# ═══════════════════════════════════════════════════════════

def _server_name(config):
    """서버 이름 (-servername). 칸을 비우면 PZ 기본값 servertest"""
    return str(config.get("server_name") or "servertest")


def _cache_dir(config):
    """인스턴스 데이터 폴더 설정 (자리표시자가 남아 있으면 없는 것으로 본다)"""
    cache_dir = str(config.get("cache_dir") or "")
    return cache_dir if cache_dir and "{" not in cache_dir else ""


def _legacy_config_dir(config):
    """예전 인스턴스의 config_dir. cache_dir이 있으면 쓰지 않는다. 끝의 구분자는 떼어 낸다"""
    if _cache_dir(config):
        return ""
    raw = str(config.get("config_dir") or "")
    return os.path.normpath(raw) if raw else ""


def _legacy_layout(config):
    """예전 config_dir을 (데이터 폴더, ini 폴더)로 푼다. 없으면 None.

    config_dir은 ini가 있는 폴더(<데이터 폴더>/Server)이거나 데이터 폴더 자체(…/Zomboid,
    ini는 그 아래 Server/)였다. 이름이 Server가 아니어도 아래에 Server/ 폴더가 있으면 데이터 폴더로 본다.
    """
    legacy = _legacy_config_dir(config)
    if not legacy:
        return None
    if os.path.basename(legacy).lower() == "server":
        return os.path.dirname(legacy), legacy
    # 게임은 언제나 <데이터 폴더>/Server/ 를 읽는다: Server/ 폴더가 있으면 그쪽이 맞다
    # (예전 버그로 데이터 폴더 바로 아래 남은 <이름>.ini는 게임이 읽지 않는 파일이다)
    server_dir = os.path.join(legacy, "Server")
    if os.path.isdir(server_dir):
        return legacy, server_dir
    return legacy, legacy


def _zomboid_home(config):
    """PZ 데이터 폴더 (-cachedir): ini, 세이브, 콘솔 로그가 여기 있다.

    `cache_dir` 설정이 있으면 그 폴더(새 인스턴스는 인스턴스마다 따로 받는다),
    없으면 PZ 기본값인 ~/Zomboid (이전에 만든 인스턴스와 가져온 서버).
    """
    cache_dir = _cache_dir(config)
    if cache_dir:
        return cache_dir
    layout = _legacy_layout(config)
    if layout:
        return layout[0]
    data_home = str(config.get("data_home") or "")
    if data_home and "{" not in data_home:
        return data_home
    if sys.platform == "win32":
        return os.path.join(os.environ.get("USERPROFILE", ""), "Zomboid")
    return os.path.join(os.path.expanduser("~"), "Zomboid")


class IniManager:
    """Read/write Project Zomboid server INI files.
    
    PZ uses a simple key=value format (one per line), no sections.
    Comments start with # and should be preserved.
    """

    @staticmethod
    def get_ini_path(config):
        """Determine the path to {servername}.ini."""
        server_name = _server_name(config)
        # 예전 인스턴스: config_dir 바로 아래에 ini가 있다 (cache_dir이 있으면 그쪽이 우선)
        layout = _legacy_layout(config)
        if layout:
            if layout[0] != layout[1]:
                return str(safe_join(layout[0], os.path.basename(layout[1]), f"{server_name}.ini"))
            return str(safe_join(layout[0], f"{server_name}.ini"))
        return str(safe_join(_zomboid_home(config), "Server", f"{server_name}.ini"))

    @staticmethod
    def read(ini_path):
        """Read an INI file and return {key: value} dict. 값은 첫 "=" 뒤를 그대로 둔다(비밀번호의 앞뒤 공백도 값이다).
        인코딩 추측(UTF-8 → cp949 → 로캘, PZ·메모장이 ANSI로 쓴 파일)은 공용 read_key_values가 한다"""
        if not os.path.exists(ini_path):
            return {}
        try:
            return read_key_values(ini_path)
        except OSError as e:
            print(f"Warning: Could not read {ini_path}: {e}", file=sys.stderr)
            return {}

    @staticmethod
    def update(ini_path, updates):
        """주어진 키만 고친다. 주석·순서·다른 키·원래 인코딩은 그대로 두고, 없는 키는 끝에 붙이며, 파일이 없으면 만든다"""
        os.makedirs(os.path.dirname(ini_path), exist_ok=True)
        update_key_values(ini_path, updates)

    write = update


# ═══════════════════════════════════════════════════════════
#              Server Executable Helpers
# ═══════════════════════════════════════════════════════════

def _find_server_dir(config):
    """Resolve the server installation directory.
    
    Checks multiple config keys:
      - executable_path: user-configured full path (from module settings)
      - server_executable: instance executable path (from Rust daemon)
      - working_dir: explicit working directory
    """
    # Check executable_path (module settings) first
    executable = config.get("executable_path", "")
    if executable and os.path.exists(executable):
        return os.path.dirname(os.path.abspath(executable))
    
    # Fallback: server_executable (Rust daemon sends instance.executable_path as this key)
    server_exe = config.get("server_executable", "")
    if server_exe and os.path.exists(server_exe):
        return os.path.dirname(os.path.abspath(server_exe))
    
    working_dir = config.get("working_dir", "")
    if working_dir and os.path.isdir(working_dir):
        return working_dir
    
    return ""


# ── 배치/쉘 스크립트 파싱으로 JVM 실행 정보 추출 ──

def _find_launch_script(server_dir):
    """서버 디렉토리에서 공식 실행 스크립트를 찾습니다."""
    if not server_dir:
        return None
    if sys.platform == "win32":
        candidates = [
            os.path.join(server_dir, "StartServer64.bat"),
            os.path.join(server_dir, "StartServer64_nosteam.bat"),
            os.path.join(server_dir, "StartServer.bat"),
        ]
    else:
        candidates = [
            os.path.join(server_dir, "start-server.sh"),
            os.path.join(server_dir, "StartServer.sh"),
        ]
    for path in candidates:
        if os.path.isfile(path):
            return path
    return None


def _parse_launch_script(script_path):
    """배치/쉘 스크립트를 파싱하여 JVM 실행 정보를 추출합니다.

    Returns dict:
        java_exe:    str   — JRE 상대 경로 (e.g. "jre64/bin/java.exe")
        classpath:   str   — classpath 문자열
        jvm_flags:   list  — -D, -XX, -Xms, -Xmx 등 JVM 플래그
        main_class:  str   — 메인 클래스 (e.g. "zombie.network.GameServer")
        extra_args:  list  — main_class 뒤 인자 (e.g. ["-statistic", "0"])
        memory:      tuple — (Xms, Xmx) 원본 문자열 (e.g. ("16g", "16g"))
    또는 파싱 실패 시 None.
    """
    if not script_path or not os.path.isfile(script_path):
        return None

    try:
        with open(script_path, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
    except PathOutside:
        raise
    except Exception:
        return None

    is_bat = script_path.lower().endswith(".bat")
    variables = {}  # SET 변수 저장

    if is_bat:
        return _parse_bat(content, variables)
    else:
        return _parse_sh(content, variables)


def _parse_bat(content, variables):
    """Windows .bat 파일에서 Java 실행 명령을 파싱합니다."""
    # 1단계: SET 변수 수집 (SET PZ_CLASSPATH=..., SET JAVA_HOME=... 등)
    for m in re.finditer(r'(?im)^\s*SET\s+(\w+)=(.+)$', content):
        variables[m.group(1).strip()] = m.group(2).strip()
    for m in re.finditer(r'(?im)^\s*SET\s+"(\w+)=([^"\r\n]*)"', content):  # set "VAR=값"
        variables[m.group(1)] = m.group(2).strip()

    # 2단계: java 실행 행 찾기 — "java" 또는 "java.exe"가 포함된 행
    #   SET, REM, PAUSE 등 명령은 제외
    java_line = None
    for line in content.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        # @ 접두사 제거
        bare = stripped.lstrip("@").strip()
        # SET, REM, PAUSE 등 비실행 명령 건너뛰기
        if re.match(r'(?i)^(SET|REM|PAUSE|ECHO|CD|SETLOCAL|ENDLOCAL|IF|GOTO|CALL)\b', bare):
            continue
        if re.search(r'java(?:\.exe)?\b', bare, re.IGNORECASE):
            java_line = bare
            break

    if not java_line:
        return None

    # 변수 치환 (%VAR%)
    def expand_var(m):
        name = m.group(1)
        return variables.get(name, m.group(0))
    java_line = re.sub(r'%(\w+)%', expand_var, java_line)
    # %~dp0 = 스크립트 폴더 (= 서버 폴더, 상대 경로와 같다)
    java_line = re.sub(r'(?i)%~dp0', '', java_line)
    # 후행 %1 %2 등 제거
    java_line = re.sub(r'%\d+', '', java_line).strip()

    return _parse_java_cmdline(java_line)


def _parse_sh(content, variables):
    """Linux .sh 파일에서 Java 실행 명령을 파싱합니다."""
    # 1단계: 변수 수집 (EXPORT VAR=..., VAR=... 등)
    for m in re.finditer(r'(?m)^(?:export\s+)?(\w+)=["\'"]?([^"\'"\n]+)', content):
        variables[m.group(1).strip()] = m.group(2).strip()

    # 2단계: java 실행 행 찾기
    java_line = None
    for line in content.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if re.search(r'java\b', stripped, re.IGNORECASE):
            java_line = stripped
            break

    if not java_line:
        return None

    # exec 접두사 제거
    java_line = re.sub(r'^exec\s+', '', java_line).strip()

    # 변수 치환 ($VAR, ${VAR})
    def expand_var(m):
        name = m.group(1) or m.group(2)
        return variables.get(name, m.group(0))
    java_line = re.sub(r'\$\{(\w+)\}|\$(\w+)', expand_var, java_line)
    # 후행 "$@" "$1" 등 제거
    java_line = re.sub(r'"\$[@\d]+"|\$[@\d]+', '', java_line).strip()

    return _parse_java_cmdline(java_line)


def _parse_java_cmdline(cmdline):
    """java 명령행 문자열을 파싱하여 구조화된 정보를 반환합니다."""
    # 따옴표 안의 java 경로 추출
    m = re.match(r'^"([^"]+)"\s*(.*)$', cmdline)
    if m:
        java_exe = m.group(1).strip().strip('"').strip(".")
        rest = m.group(2)
    else:
        parts = cmdline.split(None, 1)
        java_exe = parts[0].strip('"').lstrip("./\\")
        rest = parts[1] if len(parts) > 1 else ""

    # 경로 정규화 (.\jre64\bin\java.exe → jre64/bin/java.exe)
    java_exe = java_exe.lstrip(".\\/")
    java_exe = java_exe.replace("\\", "/")

    # 토큰 분리 (따옴표 인식)
    tokens = []
    for tok in re.finditer(r'"[^"]*"|[^\s]+', rest):
        tokens.append(tok.group().strip('"'))

    jvm_flags = []
    classpath = ""
    main_class = ""
    extra_args = []
    memory = (None, None)

    i = 0
    found_main = False
    while i < len(tokens):
        tok = tokens[i]
        if found_main:
            extra_args.append(tok)
        elif tok == "-cp" or tok == "-classpath":
            if i + 1 < len(tokens):
                classpath = tokens[i + 1]
                i += 1
        elif tok.startswith("-Xms"):
            jvm_flags.append(tok)
            xms = tok[4:]
            memory = (xms, memory[1])
        elif tok.startswith("-Xmx"):
            jvm_flags.append(tok)
            xmx = tok[4:]
            memory = (memory[0], xmx)
        elif tok.startswith("-"):
            jvm_flags.append(tok)
        else:
            # 첫 번째 비-플래그 토큰이 메인 클래스
            main_class = tok
            found_main = True
        i += 1

    if not main_class:
        return None

    return {
        "java_exe": java_exe,
        "classpath": classpath,
        "jvm_flags": jvm_flags,
        "main_class": main_class,
        "extra_args": extra_args,
        "memory": memory,
    }


# 스크립트가 실행할 때 입력받던 값처럼 사바쨩이 채울 수 없는 변수 (%RAM%, $RAM, ${RAM})
_UNRESOLVED_RE = re.compile(r"%[^%\s]+%|\$\{?\w+")


def _known_game_args(args):
    """메인 클래스 뒤 인자에서 값을 모르는 변수가 남은 것을 뺀다. 값 자리였으면 그 옵션(-adminusername 등)도 함께"""
    out = []
    for arg in args:
        if _UNRESOLVED_RE.search(arg):
            if out and out[-1].startswith("-") and not _UNRESOLVED_RE.search(out[-1]):
                out.pop()
            continue
        out.append(arg)
    return out


def _parse_launch_info(config, server_dir):
    """배치/쉘 스크립트를 파싱하여 실행 정보를 반환합니다.
    
    파싱 성공 시 dict, 실패 시 None (폴백 로직 사용).
    반환 dict에는 절대 경로가 설정됩니다.
    """
    script = _find_launch_script(server_dir)
    if not script:
        return None

    info = _parse_launch_script(script)
    if not info:
        return None

    # java_exe 절대 경로 해석
    java_abs = os.path.join(server_dir, info["java_exe"])
    if not os.path.isfile(java_abs):
        # 파싱된 경로에 파일이 없으면 기존 탐색으로 폴백
        return None
    info["java_exe_abs"] = java_abs

    return info


# ── 폴백 헬퍼 (배치파일 파싱 실패 시 사용) ──

def _find_java(server_dir):
    """Find the bundled Java executable within the PZ server directory."""
    if not server_dir:
        return None
    
    # PZ bundles its own JRE in jre64/bin/ (Windows) or jre64/bin/ (Linux)
    if sys.platform == "win32":
        candidates = [
            os.path.join(server_dir, "jre64", "bin", "java.exe"),
            os.path.join(server_dir, "jre", "bin", "java.exe"),
        ]
    else:
        candidates = [
            os.path.join(server_dir, "jre64", "bin", "java"),
            os.path.join(server_dir, "jre", "bin", "java"),
        ]
    
    for java_path in candidates:
        if os.path.isfile(java_path):
            return java_path
    
    return None


def _heap_size(ram):
    """설정한 메모리(GB 숫자) → JVM 크기 문자열("4g"). 비었으면 PC 메모리에 맞춘 자동값(공용 auto_heap_mb)"""
    if ram is not None and str(ram).strip() != "":
        try:
            return f"{int(ram)}g"
        except (ValueError, TypeError):
            return "4g"
    mb = auto_heap_mb()
    return f"{mb // 1024}g" if mb % 1024 == 0 else f"{mb}m"


_COH_FLAG = "-XX:+UseCompactObjectHeaders"


def _coh_flags(java_path, flags):
    """동봉 JRE가 Java 25 이상이면 Compact Object Headers(JEP 519, 힙 절약)를 켠다. 스크립트가 이미 정했으면 그대로 둔다"""
    java_home = os.path.dirname(os.path.dirname(java_path))
    if any("CompactObjectHeaders" in f for f in flags) or java_release_major(java_home) < 25:
        return []
    return [_COH_FLAG]


# 게임 공식 실행 스크립트의 JVM 인자 (메모리·GC·라이브러리 경로 말고)
_OFFICIAL_JVM_FLAGS = (
    "-Djava.awt.headless=true",
    "-Dzomboid.steam=1",
    "-Dzomboid.znetlog=1",
    "-XX:-CreateCoredumpOnCrash",
    "-XX:-OmitStackTraceInFastThrow",
)


def _is_jvm_arg(arg):
    """메인 클래스 앞에 와야 JVM이 읽는 인자 (-D·-X·-javaagent 등). PZ 게임 인자(-debug, -nosteam 등)는 이렇게 시작하지 않는다"""
    return arg.startswith(("-D", "-X", "-javaagent:", "-agentlib:", "-agentpath:", "--add-", "--enable-"))


def _managed_jvm_arg(arg):
    """사바쨩이 정하는 JVM 인자: 공식 스크립트 인자, 메모리, GC, 라이브러리 경로, 데이터 폴더, Compact Object Headers"""
    return (arg in _OFFICIAL_JVM_FLAGS or arg == "-Dzomboid.steam=0" or arg == _COH_FLAG
            or arg.startswith(("-Xms", "-Xmx", "-Djava.library.path=", "-Ddeployment.user.cachedir="))
            or bool(re.fullmatch(r"-XX:\+Use\w+GC|-XX:[+-]ZGenerational", arg)))


def _script_jvm_args(server_dir):
    """쓰던 실행 스크립트에만 있던 JVM 인자 (자바 모드 로더 등). Steam 업데이트가 스크립트를 공식 내용으로 되돌려도 남게
    가져오기가 추가 실행 인자로 옮긴다"""
    script = _find_launch_script(server_dir) if server_dir else None
    info = _parse_launch_script(script) if script else None
    if not info:
        return []
    return [a for a in info["jvm_flags"] if not _managed_jvm_arg(a) and not _UNRESOLVED_RE.search(a)]


def _gc_flags(config, flags):
    """기본은 게임 공식 스크립트의 GC 그대로(ZGC). 설정 java_gc = g1이면 스크립트의 GC 옵션을 G1 하나로 바꾼다
    (ZGC를 못 쓰는 오래된 Windows Server 등)"""
    if config.get("java_gc") != "g1":
        return flags
    out, placed = [], False
    for flag in flags:
        if re.fullmatch(r"-XX:\+Use\w+GC", flag):
            if not placed:
                out.append("-XX:+UseG1GC")
                placed = True
        elif not re.fullmatch(r"-XX:[+-]ZGenerational", flag):
            out.append(flag)
    return out if placed else ["-XX:+UseG1GC", *out]


def _resolve_classpath(server_dir):
    """Build the Java classpath from the PZ server's java/ directory."""
    java_dir = os.path.join(server_dir, "java")
    if not os.path.isdir(java_dir):
        return ""
    
    jars = []
    for f in sorted(os.listdir(java_dir)):
        if f.endswith(".jar"):
            jars.append(os.path.join("java", f))
    
    sep = ";" if sys.platform == "win32" else ":"
    return sep.join(jars)


def _get_native_libs(server_dir):
    """Build the java.library.path for PZ native libraries."""
    if sys.platform == "win32":
        paths = ["natives/", "natives/win64/", "."]
    else:
        paths = ["natives/", "natives/linux64/", "."]
    
    return ";".join(paths) if sys.platform == "win32" else ":".join(paths)


def _enforce_rcon_policy(ini_path, config=None):
    """RCON은 관리형 시작을 끈 서버에서만 켠다 (Project Zomboid는 RCONPassword가 비면 RCON을 끈다).

    관리형 시작은 콘솔(stdin)로 명령을 보낸다. 끄면 콘솔이 없어 데몬이 만든 포트·비밀번호의 RCON으로 보낸다.
    쓸 값은 module.toml `[remote_access]` (공용 remote_access_values).
    Returns the RCON password written ("" when RCON is off).
    """
    access = rcon_access(config)
    if os.path.exists(ini_path):
        enforce_remote_access(MODULE_DIR, config, IniManager.read(ini_path), lambda changes: IniManager.update(ini_path, changes))
    elif access:
        # 첫 시작: 서버가 만들 기본값 위에 RCON을 얹어 미리 만든다 (콘솔도 RCON도 없이 뜨지 않게)
        IniManager.update(ini_path, {**setting_defaults_to_file(MODULE_DIR), **remote_access_values(MODULE_DIR, config)})
    return access[1] if access else ""


# ╔═══════════════════════════════════════════════════════════╗
# ║                  Required Functions                       ║
# ╚═══════════════════════════════════════════════════════════╝

def validate(config):
    """Validate all prerequisites before starting the PZ server.
    
    Checks:
      1. Server installation directory exists
      2. Server executable (bat/sh) or Java binary exists
      3. Working directory is valid
      4. Port availability
      5. RCON configuration
    """
    issues = []
    server_dir = _find_server_dir(config)
    
    # Check server directory
    if not server_dir or not os.path.isdir(server_dir):
        issues.append({
            "severity": "critical",
            "code": "no_server_dir",
            "message": i18n.t("errors.no_server_dir"),
            "solution": i18n.t("errors.solutions.no_server_dir"),
        })
        return {"success": False, "issues": issues}
    
    # Check for server executable
    executable = config.get("executable_path", "")
    if not executable or not os.path.exists(executable):
        # 실행 경로(get_launch_command)와 같은 기준: 공식 스크립트, 없으면 동봉 JRE + java/*.jar
        found = bool(_find_launch_script(server_dir) or (_find_java(server_dir) and _resolve_classpath(server_dir)))
        
        if not found:
            issues.append({
                "severity": "critical",
                "code": "executable_not_found",
                "message": i18n.t("errors.executable_not_found"),
                "solution": i18n.t("errors.solutions.executable_not_found"),
            })
    
    # Check bundled Java
    java_path = _find_java(server_dir)
    if not java_path:
        issues.append({
            "severity": "warning",
            "code": "java_not_found",
            "message": i18n.t("errors.java_not_found"),
            "solution": i18n.t("errors.solutions.java_not_found"),
        })
    
    # 첫 실행에 관리자 비밀번호가 없으면 PZ가 콘솔에서 비밀번호를 기다리며 멈춘다: 켜기 전에 막는다
    if not config.get("admin_password") and not _has_server_db(config):
        issues.append({
            "severity": "critical",
            "code": "admin_password_required",
            "message": i18n.t("errors.admin_password_required"),
            "solution": i18n.t("errors.solutions.admin_password_required"),
        })

    has_errors = any(i["severity"] in ("error", "critical") for i in issues)
    return {"success": not has_errors, "issues": issues}


def _has_server_db(config):
    """이 서버 이름의 계정 DB(<데이터 폴더>/db/<이름>.db)에 계정이 있는가. 있으면 관리자 계정이 이미 만들어졌다.
    빈 파일·깨진 파일·계정 없는 DB(첫 실행이 비밀번호 전에 멈춘 경우)는 없는 것으로 본다"""
    server_name = _server_name(config)
    path = str(safe_join(_zomboid_home(config), "db", f"{server_name}.db"))
    if not os.path.isfile(path) or os.path.getsize(path) == 0:
        return False
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            return conn.execute("SELECT 1 FROM whitelist LIMIT 1").fetchone() is not None
        finally:
            conn.close()
    except sqlite3.Error:
        return False


def get_launch_command(config):
    """Build the server launch command for the Rust daemon.
    
    배치/쉘 스크립트를 파싱하여 JVM 실행 정보를 추출합니다.
    PZ 업데이트로 classpath·JVM 플래그가 변경되어도 자동 반영됩니다.
    파싱 실패 시 기존 하드코딩 폴백을 사용합니다.
    """
    server_dir = _find_server_dir(config)
    if not server_dir:
        return {"success": False, "message": i18n.t("errors.no_server_dir")}
    
    # 사용자 설정값. 비었으면 PC에 맞춘 자동값
    heap = _heap_size(config.get("ram"))
    server_name = _server_name(config)
    preview = bool(config.get("preview"))  # 미리보기는 파일·폴더를 만들거나 고치지 않는다
    if not preview:
        _enforce_rcon_policy(IniManager.get_ini_path(config), config)

    # ── 1차: 배치/쉘 스크립트 파싱 ──
    parsed = _parse_launch_info(config, server_dir)

    # 사용자가 더한 인자: JVM 인자는 메인 클래스 앞에, 게임 인자는 뒤에 (스크립트에 이미 있는 JVM 인자는 거듭 넣지 않는다)
    user_args = config.get("extra_args")
    user_args = [str(a) for a in user_args] if isinstance(user_args, list) else []
    in_script = set(parsed["jvm_flags"]) if parsed else set()
    user_jvm = [a for a in user_args if _is_jvm_arg(a) and a not in in_script]
    user_game = [a for a in user_args if not _is_jvm_arg(a)]

    if parsed:
        java_path = parsed["java_exe_abs"]
        classpath = parsed["classpath"]
        if not classpath or _UNRESOLVED_RE.search(classpath):
            classpath = _resolve_classpath(server_dir)
            if not classpath:
                return {"success": False, "message": i18n.t("errors.classpath_not_found")}
        
        # 파싱된 JVM 플래그에서 메모리 설정만 사용자 값으로 교체. 값을 모르는 변수가 남은 플래그는 뺀다 (JVM이 시작을 거절한다)
        jvm_flags = []
        for flag in parsed["jvm_flags"]:
            if flag.startswith("-Xms"):
                jvm_flags.append(f"-Xms{heap}")
            elif flag.startswith("-Xmx"):
                jvm_flags.append(f"-Xmx{heap}")
            elif not _UNRESOLVED_RE.search(flag):
                jvm_flags.append(flag)
        jvm_flags = _gc_flags(config, jvm_flags)
        jvm_flags += _coh_flags(java_path, jvm_flags)
        
        # main_class 뒤 인자: 스크립트 원본의 extra_args에서 -servername을 찾아 교체
        main_class = parsed["main_class"]
        extra_args = []
        skip_next = False
        servername_set = False
        for i, arg in enumerate(_known_game_args(parsed["extra_args"])):
            if skip_next:
                skip_next = False
                continue
            if arg == "-servername":
                extra_args.extend(["-servername", server_name])
                servername_set = True
                skip_next = True  # 다음 인자(원본 서버이름) 건너뛰기
            else:
                extra_args.append(arg)
        
        if not servername_set:
            extra_args.extend(["-servername", server_name])
        
        args = jvm_flags + user_jvm + ["-cp", classpath, main_class] + extra_args + user_game
    else:
        # ── 2차: 하드코딩 폴백 ──
        java_path = _find_java(server_dir)
        if not java_path:
            return {"success": False, "message": i18n.t("errors.java_not_found")}
        
        classpath = _resolve_classpath(server_dir)
        if not classpath:
            return {"success": False, "message": i18n.t("errors.classpath_not_found")}
        
        native_libs = _get_native_libs(server_dir)
        
        args = _gc_flags(config, [
            *_OFFICIAL_JVM_FLAGS,
            "-XX:+UseZGC",
            f"-Xms{heap}",
            f"-Xmx{heap}",
            *_coh_flags(java_path, []),
            f"-Djava.library.path={native_libs}",
            "-cp", classpath,
            "zombie.network.GameServer",
            "-servername", server_name,
            "-statistic", "0",
        ])
        main = args.index("-cp")
        args = args[:main] + user_jvm + args[main:] + user_game
    
    # ── 공통 후처리 ──
    # No-steam mode
    if config.get("no_steam", False):
        args.append("-nosteam")
        # -Dzomboid.steam=1 → 0으로 교체
        args = ["-Dzomboid.steam=0" if a == "-Dzomboid.steam=1" else a for a in args]
    
    # 데몬이 정한 포트(다른 인스턴스와 겹치지 않게 옮긴 값). ini의 DefaultPort보다 앞선다
    try:
        port = int(config.get("port") or 0)
    except (ValueError, TypeError):
        port = 0
    if 0 < port <= 65535:
        args.extend(["-port", str(port)])
        # 두 번째 포트(ini UDPPort): ini 값이 남으면 다른 인스턴스와 겹친다. udp_port 설정은 데몬이 다른 포트와
        # 함께 옮기고 충돌을 검사한다. 그 설정이 없던 인스턴스는 기본 간격대로 port+1
        try:
            udp_port = int(config.get("udp_port") or 0)
        except (ValueError, TypeError):
            udp_port = 0
        if not 0 < udp_port <= 65535:
            udp_port = port + 1 if port < 65535 else 0
        if udp_port:
            args.extend(["-udpport", str(udp_port)])

    # Admin password is needed only while the server creates its first account database.
    admin_password = config.get("admin_password", "")
    if admin_password and not _has_server_db(config):
        args.extend(["-adminpassword", admin_password])
    
    # 인스턴스마다 따로 둔 데이터 폴더 (없으면 PZ 기본 ~/Zomboid)
    if _cache_dir(config) or _legacy_config_dir(config) or config.get("data_home"):
        home = _zomboid_home(config)
        if not preview:
            try:
                os.makedirs(home, exist_ok=True)
            except OSError:
                return {"success": False, "message": i18n.t("errors.data_home_unavailable")}
        # 가져온 서버의 스크립트에 있던 -cachedir는 빼고 데몬이 정한 폴더 하나만 넘긴다
        args = [a for a in args if not a.startswith("-cachedir=")]
        args.append(f"-cachedir={home}")
    
    return {
        "success": True,
        "program": java_path,
        "args": args,
        "working_dir": server_dir,
        "extra_args_placed": True,
    }


# ╔═══════════════════════════════════════════════════════════╗
# ║                  Optional Functions                       ║
# ╚═══════════════════════════════════════════════════════════╝

def _request_stop(config):
    """저장 후 종료 요청: 콘솔 `[config] stop_command`(quit: 서버가 저장하고 끝난다, PZ 위키 "Server commands") → SIGTERM"""
    return request_console(config, stop_command(MODULE_DIR)) or request_terminate(config.get("pid"))


def stop(config):
    """Stop the server.

    Ask for a save-and-exit (console `quit`, else SIGTERM); waiting, force kill, managed and
    `graceful_stop_unavailable` are handled by the shared procedure (graceful_stop). With managed start off, the console `quit` goes over RCON (daemon).
    The daemon stops managed servers with a console itself, so this runs mostly without one.
    """
    return graceful_stop(config, lambda: _request_stop(config), i18n, module_dir=MODULE_DIR)


def _ini_updates(settings):
    """saba-chan 설정 키를 ini 키·값으로 (module.toml file_key가 없는 키는 뺀다)"""
    return settings_to_file(MODULE_DIR, settings)


def configure(config):
    """Apply settings from saba-chan to servertest.ini.
    
    Maps internal key names to PZ INI key names and writes them.
    """
    settings = config.get("settings", {})
    if not settings:
        return {"success": False, "message": i18n.t("errors.no_settings")}
    
    ini_path = IniManager.get_ini_path(config)
    try:
        updates = _ini_updates(settings)
    except ValueError as e:
        return {"success": False, "message": i18n.t("errors.properties_write_failed", error=str(e))}
    
    if not updates:
        return {"success": True, "updated_keys": [], "message": i18n.t("messages.no_mappable_settings")}
    
    try:
        IniManager.update(ini_path, updates)
        return {
            "success": True,
            "updated_keys": list(updates.keys()),
            "message": i18n.t("messages.properties_updated"),
        }
    except PathOutside:
        raise
    except Exception as e:
        return {
            "success": False,
            "message": i18n.t("errors.properties_write_failed", error=str(e)),
        }


def _heap_gb(value):
    """-Xmx 값("16g", "8192m") → GB 정수 (ram 설정 단위). 읽을 수 없으면 None"""
    m = re.fullmatch(r"(\d+)([kKmMgG])", str(value or "").strip())
    if not m:
        return None
    size = int(m.group(1))
    unit = m.group(2).lower()
    gb = size if unit == "g" else size // 1024 if unit == "m" else size // (1024 * 1024)
    return max(1, gb)


def _script_settings(server_dir):
    """쓰던 실행 스크립트가 정한 메모리(-Xmx)·서버 이름(-servername)·데이터 폴더(-cachedir).
    가져오기가 모듈 기본값(4GB, servertest, ~/Zomboid)으로 덮지 않게 한다. 읽지 못한 값은 빠진다"""
    # 실행할 때 쓰는 스크립트와 같은 것 (Windows는 .bat, 그 밖은 .sh)
    script = _find_launch_script(server_dir)
    info = _parse_launch_script(script) if script else None
    if not info:
        return {}
    found = {}
    ram = _heap_gb(info["memory"][1])
    if ram:
        found["ram"] = ram
    for flag in info["jvm_flags"]:
        if flag.startswith("-Ddeployment.user.cachedir=") and not _UNRESOLVED_RE.search(flag):
            found["data_home"] = flag.split("=", 1)[1]
    args = _known_game_args(info["extra_args"])  # 실행할 때 입력받던 값(%NAME%)은 가져오지 않는다
    for i, arg in enumerate(args):
        if arg == "-servername" and i + 1 < len(args):
            found["server_name"] = args[i + 1]
        elif arg.startswith("-cachedir="):
            found["data_home"] = arg.split("=", 1)[1]
    # 상대 경로는 서버가 도는 폴더(서버 폴더) 기준이다
    if found.get("data_home"):
        found["data_home"] = os.path.normpath(os.path.join(server_dir, os.path.expanduser(found["data_home"])))
    return found


def _only_profile(home):
    """데이터 폴더의 Server/에 프로필(ini)이 하나뿐이고 PZ 기본값(servertest)이 아니면 그 이름"""
    try:
        names = [f[:-4] for f in os.listdir(os.path.join(home, "Server")) if f.lower().endswith(".ini")]
    except OSError:
        return None
    return names[0] if len(names) == 1 and names[0] != "servertest" else None


def import_settings(config):
    """Read server INI and return settings in saba-chan key format.

    Used during migration to import existing server settings into saba-chan.
    실행 스크립트의 메모리·서버 이름·데이터 폴더도 읽고, 스크립트가 서버 이름을 정하지 않았으면
    데이터 폴더에 하나뿐인 프로필을 쓴다 (쓰던 월드 대신 새 servertest 월드가 뜨지 않게).
    """
    server_dir = _find_server_dir(config)
    found = _script_settings(server_dir) if server_dir else {}
    config = {**config, **found}
    if "server_name" not in found and not config.get("server_name"):
        profile = _only_profile(_zomboid_home(config))
        if profile:
            found["server_name"] = config["server_name"] = profile
    if "server_name" in found and not _plain_server_name(found["server_name"]):
        # 스크립트의 이름이 프로필 폴더 밖을 가리키면 그 이름은 쓰지 않는다
        found.pop("server_name")
        config.pop("server_name", None)

    # 읽지 못한 ini(없음·권한 없음) 대신 기본값을 가져오면 사용자 설정을 기본값으로 덮는다
    if not os.path.isfile(IniManager.get_ini_path(config)):
        raw_props = {}
    else:
        result = read_properties(config)
        if not result.get("success"):
            return result
        raw_props = result.get("properties", {})
    extra = {"extra_args": script_args} if (script_args := _script_jvm_args(server_dir)) else {}
    if not raw_props:
        return {"success": True, "settings": {**found, "data_home": _zomboid_home(config)}, **extra,
                "message": i18n.t("messages.no_properties_found")}

    from daemon_rcon import coerce_setting, load_setting_types
    types = load_setting_types(os.path.dirname(os.path.abspath(__file__)))
    settings = {key: coerce_setting(raw_value, types.get(key)) for key, raw_value in raw_props.items()}
    settings.update(found)
    settings["data_home"] = _zomboid_home(config)

    return {"success": True, "settings": settings, **extra}


def config_files(config):
    """사바쨩이 고칠 수 있는 게임 설정 파일 (가져온 서버를 해제할 때 데몬이 가져오기 전 내용으로 되돌린다)"""
    return {"success": True, "files": [IniManager.get_ini_path(config)]}


def read_properties(config):
    """Read the server INI file and return settings.
    
    Returns properties mapped back to saba-chan internal key names.
    """
    ini_path = IniManager.get_ini_path(config)
    # ini 키 → 설정 이름 (설정 화면에 없는 키는 이름 그대로)
    names = {ini_key: name for name, ini_key in setting_file_keys(MODULE_DIR).items()}

    if not os.path.exists(ini_path):
        # Return defaults mapped to internal keys
        mapped_defaults = {}
        for ini_key, value in setting_defaults_to_file(MODULE_DIR).items():
            internal_key = names.get(ini_key, ini_key)
            mapped_defaults[internal_key] = value
        return {
            "success": True,
            "properties": mapped_defaults,
            "message": i18n.t("messages.properties_using_defaults"),
        }
    
    raw_props = IniManager.read(ini_path)
    mapped = {}
    for ini_key, value in raw_props.items():
        internal_key = names.get(ini_key, ini_key)
        mapped[internal_key] = value
    
    return {"success": True, "properties": mapped}


# 이유(-r)가 선택인 명령. 서버는 빈 인자를 받지 않아(CommandBase 인자 규칙 `(.+)`) `kick "이름" -r ""`는 명령 전체를 거절한다.
# 콘솔 템플릿은 빈 자리의 따옴표를 남기므로, 이유가 있을 때만 -r을 붙여 여기서 한 줄을 만든다
_REASON_COMMANDS = ("kick", "banuser")


def _reason_command_line(name, args):
    """`kick "이름"` 또는 `kick "이름" -r "이유"`"""
    player = str(args.get("player") or "").strip()
    reason = str(args.get("reason") or "").strip()
    line = f'{name} "{player}"'
    return f'{line} -r "{reason}"' if reason else line


def _quote_refusal(args):
    """값에 따옴표가 있으면 거절 답 (서버가 따옴표로 인자를 가른다). 데몬이 콘솔 템플릿의 따옴표 자리에 주는
    거절(invalid_argument, rule = quote)과 같은 코드·칸·규칙이라 화면과 디스코드가 같은 문구로 알린다"""
    for field in ("player", "reason"):
        value = str(args.get(field) or "")
        if '"' in value:
            return {"success": False, "message": i18n.t("errors.quote_not_allowed"), "error_code": "invalid_argument",
                    "field": field, "rule": "quote", "params": {"value": value}}
    return None


def command(config):
    """Send a command to the server console (stdin) through the daemon."""
    cmd_name = config.get("command_name", "") or config.get("command", "")
    cmd_text = config.get("command_text", "")
    instance_id = config.get("instance_id", "")
    actual_cmd = cmd_text or cmd_name
    if cmd_name in _REASON_COMMANDS and not cmd_text:
        args = config.get("args") or {}
        refusal = _quote_refusal(args)
        if refusal:
            return refusal
        actual_cmd = _reason_command_line(cmd_name, args)
    if not actual_cmd:
        return {"success": False, "message": i18n.t("errors.no_command")}
    if not instance_id:
        return {"success": False, "message": i18n.t("errors.server_not_running")}
    try:
        result = daemon_stdin_command(instance_id, actual_cmd)
    except PathOutside:
        raise
    except Exception as e:
        return {"success": False, "message": i18n.t("errors.command_send_failed", error=str(e))}
    # 데몬은 module 명령의 답에서 message만 호출자에게 돌려준다: 서버가 콘솔에 찍은 답이 있으면 그것을 문구로 준다
    data = result.get("data") if isinstance(result, dict) else None
    answer = str(data.get("response") or "").strip() if isinstance(data, dict) else ""
    return {"success": True, "message": answer or i18n.t("messages.command_sent", command=actual_cmd), "data": result}


# 콘솔·RCON `players`의 답: "Players connected (N): " 다음 줄마다 "-이름" (서버 소스 PlayersCommand, PC 실측 필요)
_PLAYERS_HEAD_RE = re.compile(r"Players connected \((\d+)\)")


def player_count(config):
    """접속자 (module.toml [status] players). 1분 갱신(players_detail 없음)에는 콘솔·RCON에 묻지 않는다:
    수는 데몬이 접속·퇴장 줄로 센다. 사용자가 목록을 물을 때만 `players`를 한 번 보낸다"""
    try:
        max_players = int(config.get("max_players") or 0)
    except (TypeError, ValueError):
        max_players = 0
    if not config.get("players_detail"):
        return {"success": True, "max": max_players} if max_players else {"success": True}
    try:
        reply = daemon_stdin_command(config.get("instance_id", ""), "players", timeout=5)
    except PathOutside:
        raise
    except Exception as e:
        print(f"[Zomboid] players failed: {e}", file=sys.stderr)
        return {"success": False, "message": i18n.t("errors.players_unavailable")}
    response = ((reply or {}).get("data") or {}).get("response") or ""
    lines = [line.strip() for line in str(response).splitlines()]
    for i, line in enumerate(lines):
        head = _PLAYERS_HEAD_RE.search(line)
        if not head:
            continue
        online = int(head.group(1))
        names = []
        for rest in lines[i + 1:]:
            if not rest.startswith("-"):
                break
            names.append(rest[1:].strip())
        result = {"success": True, "online": online}
        if max_players:
            result["max"] = max_players
        if len(names) == online:
            result["players"] = [{"name": n} for n in names]
        return result
    return {"success": False, "message": i18n.t("errors.players_unavailable")}


def diagnose_log(config):
    """Analyze server log for known error patterns (module.toml [[diagnose]], 공용 diagnose_log).
    데몬이 준 이번 실행의 콘솔 줄을 먼저 보고, 없으면 서버 콘솔 로그(server-console.txt) 끝부분을 본다.
    클라이언트 로그(console.txt)는 보지 않는다: 같은 PC에서 게임을 한 기록을 서버 문제로 보게 된다"""
    files = [os.path.join(_zomboid_home(config), "server-console.txt")]
    if config.get("working_dir"):
        files.insert(0, os.path.join(config["working_dir"], "server-console.txt"))
    return daemon_rcon.diagnose_log(config, MODULE_DIR, i18n, files=files)


# 서버마다 고를 수 있는 Steam 브랜치 (첫 항목이 기본). 이름은 locales `versions.<id>`.
# public·unstable·legacy41: projectzomboid.com 42.20 공지 (Build 41로 남을 서버 주인은 legacy41 베타 채널을 고른다)
STEAM_BRANCHES = ("public", "unstable", "legacy41")


def list_versions(config):
    """고를 수 있는 Steam 브랜치. 앱의 "서버 버전" 칸이 보이고, 고른 id가 `install_server`의 `version`으로 온다."""
    return {
        "success": True,
        "versions": [{"id": branch, "name": i18n.t(f"versions.{branch}")} for branch in STEAM_BRANCHES],
        "latest": {"release": STEAM_BRANCHES[0]},
    }


def install_server(config):
    """Install/update the Project Zomboid dedicated server via SteamCMD.

    Uses the saba-chan SteamCMD extension for portable, automatic download
    (Steam app and anonymous login come from module.toml [install]).
    `version`: 받을 Steam 브랜치(`list_versions`의 id). 없으면 module.toml [install]의 브랜치(없으면 정식).
    """
    branch = config.get("version") or None
    if branch is not None and branch not in STEAM_BRANCHES:
        return {"success": False, "message": i18n.t("errors.unknown_branch", branch=branch)}
    install_dir = config.get("install_dir", "")
    if not install_dir:
        return {"success": False, "message": i18n.t("errors.no_install_dir")}

    os.makedirs(install_dir, exist_ok=True)

    # app_id는 module.toml [install]. 익스텐션이 없거나 꺼져 있으면(import 가드) 데몬이 켜기를 안내하게 답한다
    try:
        from extensions.steamcmd import install_for_module
    except ImportError:
        return extension_required(i18n, "steamcmd")
    try:
        # 정식으로 돌아갈 때도 브랜치 이름(public)을 적어 넘긴다: -beta 없이 받으면 받아 둔 브랜치에 머문다
        result = install_for_module(MODULE_DIR, install_dir, beta=branch)
    except PathOutside:
        raise
    except Exception as e:
        print(f"[Zomboid] SteamCMD extension install failed: {e}", file=sys.stderr)
        return {"success": False, "message": i18n.t("errors.install_failed", error=str(e))}

    if result.get("success"):
        if sys.platform == "win32":
            exe = os.path.join(install_dir, "StartServer64.bat")
        else:
            exe = os.path.join(install_dir, "start-server.sh")
        result["install_path"] = install_dir
        result["executable_path"] = exe if os.path.exists(exe) else ""
        settings_error = _write_initial_ini(config)
        if settings_error:
            return settings_error
    return result


def _write_initial_ini(config):
    """첫 설치: 만들 때 고른 설정(접속 암호·인원·포트 등)으로 서버 ini를 만든다. 서버는 첫 실행 때 나머지 키를
    기본값으로 채운다. 이미 있으면(업데이트) 건드리지 않는다"""
    settings = config.get("settings") or {}
    ini_path = IniManager.get_ini_path({**config, **settings})
    if os.path.exists(ini_path):
        return
    try:
        updates = _ini_updates(settings)
        if not updates:
            return
        IniManager.write(ini_path, updates)
    except PathOutside:
        raise
    except Exception as e:
        return {"success": False, "error_code": "settings_write_failed", "message": i18n.t("errors.settings_write_failed", error=str(e))}


def _plain_server_name(name):
    """폴더 하나를 가리키는 서버 이름인가: 구분자·'..'·드라이브(C:)가 없고, Windows가 잘라 내는 끝 공백·점이 없다"""
    return bool(name) and name not in (".", "..") and not any(sep in name for sep in ("/", "\\", ":")) and name[-1] not in " ."


def _reset_world_dir(config):
    """초기화가 지우는 이 서버의 월드 폴더 `<data folder>/Saves/Multiplayer/{servername}`. 이름이 안전하지 않으면 실패 응답(dict)"""
    server_name = _server_name(config)
    multiplayer = os.path.join(_zomboid_home(config), "Saves", "Multiplayer")
    saves_dir = os.path.join(multiplayer, server_name)
    # 이 서버의 월드 폴더 하나만 지운다 (이름에 구분자·'..'·드라이브(C:)가 있거나, Windows가 잘라 내는 끝 공백·점이 있으면 다른 서버의 월드까지 지울 수 있다)
    if not _plain_server_name(server_name) or os.path.dirname(os.path.abspath(saves_dir)) != os.path.abspath(multiplayer):
        return {"success": False, "message": i18n.t("errors.invalid_server_name", name=server_name)}
    return str(safe_join(_zomboid_home(config), "Saves", "Multiplayer", server_name))


def reset_preview(config):
    """초기화 확인 창에 보일 것 (데몬 `GET /server/reset`): 이 서버의 월드 폴더 하나. 설정은 남는다"""
    saves_dir = _reset_world_dir(config)
    if isinstance(saves_dir, dict):
        return saves_dir
    item = {"label": i18n.t("reset.items.world"), "paths": [saves_dir]}
    return {"success": True, "scopes": [{"id": "world", "items": [item], "note": i18n.t("reset.description")}]}


def reset_server(config):
    """Reset the server world (delete save data, keep settings).
    
    Deletes: <data folder>/Saves/Multiplayer/{servername}/
    Keeps: <data folder>/Server/{servername}.ini and .lua files
    """
    saves_dir = _reset_world_dir(config)
    if isinstance(saves_dir, dict):
        return saves_dir
    
    # 접근할 수 없는 폴더를 '없음'으로 보고 성공을 돌려주지 않는다 (delete_paths는 권한 오류를 오류로 센다)
    deleted, errors = delete_paths([saves_dir])
    
    if errors:
        return {
            "success": False,
            "message": i18n.t("errors.server_reset_partial", errors=", ".join(errors)),
        }
    
    return {
        "success": True,
        "message": i18n.t("messages.server_reset_complete"),
        "deleted": deleted,
    }


# 서버가 처음 띄울 때 Server/에 만드는 이 서버의 설정 파일(.ini 말고)
_SERVER_LUA_KINDS = ("SandboxVars", "spawnregions", "spawnpoints")


def _backup_zip_info(path):
    """PZ 백업 zip의 readme.txt: (서버 이름, 월드가 들었는가). 읽지 못하면 None.
    42.21 실측: `ServerName: <이름>`, 월드가 없던 때는 `World version in this backup is:World isn't exist`"""
    import zipfile
    try:
        with zipfile.ZipFile(path) as z:
            text = z.read("readme.txt").decode("utf-8", "replace")
    except (OSError, KeyError, zipfile.BadZipFile):
        return None
    fields = dict(line.split(":", 1) for line in text.splitlines() if ":" in line)
    name = fields.get("ServerName", "").strip()
    world = fields.get("World version in this backup is", "").strip()
    return name, bool(world) and world != "World isn't exist"


def backup_snapshots(config):
    """서버가 스스로 만든 백업 (데몬 `[backup] snapshots`): BackupsOnStart·BackupsOnVersionChange 등이 만든
    `<데이터 폴더>/backups/<종류>/backup_N.zip`(N=1이 가장 새것, 42.21 실측). zip 하나라 `archive`로 알리고,
    zip 안의 이 서버 세이브·DB·설정(`Saves/Multiplayer/<이름>`, `db/<이름>.db`, `Server/<이름>…`)만 데이터 폴더의 같은 자리로 돌아간다.
    다른 서버 이름의 백업과 월드가 없던 때의 백업은 뺀다"""
    server_name = _server_name(config)
    home = _zomboid_home(config)
    root = safe_join(home, "backups")
    try:
        snapshots = []
        for kind in list_dir(root):
            folder = safe_join(root, kind)
            if not os.path.isdir(folder):
                continue
            for name in list_dir(folder):
                path = safe_join(folder, name)
                if not (name.endswith(".zip") and os.path.isfile(path)):
                    continue
                info = _backup_zip_info(path)
                if not info or info != (server_name, True):
                    continue
                inner = [f"Saves/Multiplayer/{server_name}", f"db/{server_name}.db", f"Server/{server_name}.ini"]
                inner += [f"Server/{server_name}_{kind_}.lua" for kind_ in _SERVER_LUA_KINDS]
                snapshots.append({
                    "id": f"{kind}/{name[:-4]}",
                    "created": int(os.path.getmtime(path)),
                    "archive": str(path),
                    "paths": [{"from": p, "to": str(safe_join(home, *p.split("/")))} for p in inner],
                })
        snapshots.sort(key=lambda s: s["created"], reverse=True)
        return {"success": True, "snapshots": snapshots}
    except OSError as e:
        return {"success": False, "message": i18n.t("errors.backup_paths_failed", error=str(e))}





def _server_config_files(server_dir, server_name):
    """Server/ 폴더에서 이 서버 이름의 파일: `<이름>.ini`와 `<이름>_<종류>.lua`(SandboxVars·spawnregions·spawnpoints).
    `<이름>_2.ini`나 `<이름>_2_SandboxVars.lua`는 다른 서버(`<이름>_2`) 것이다.
    정해진 파일은 지금 없어도 낸다: 데몬은 훅 경로 안만 복원하므로, 지워진 파일도 되살릴 수 있어야 한다"""
    ini = str(safe_join(server_dir, f"{server_name}.ini"))
    prefix = f"{server_name}_"
    lua = {f"{prefix}{kind}.lua" for kind in _SERVER_LUA_KINDS}
    try:
        names = os.listdir(server_dir)
    except FileNotFoundError:
        names = []
    # 읽지 못하는 폴더(권한·I/O)는 OSError로 올린다: 설정 일부가 조용히 빠진 백업을 성공으로 남기지 않는다
    lua.update(n for n in names
               if n.startswith(prefix) and n.endswith(".lua") and "_" not in n[len(prefix):])
    return [ini] + [str(safe_join(server_dir, n)) for n in sorted(lua)]


def backup_paths(config):
    """백업할 세이브 위치 (데몬 `[backup] hook`): 이 서버 이름의 월드, 플레이어 DB, 서버 설정 파일.
    Server/ 폴더째 넣으면 다른 서버 이름의 설정까지 들어가므로 이 서버 것만 고른다"""
    server_name = _server_name(config)
    home = _zomboid_home(config)
    server_dir = os.path.dirname(IniManager.get_ini_path(config))
    try:
        config_files = _server_config_files(server_dir, server_name)
    except OSError as e:
        return {"success": False, "message": i18n.t("errors.backup_paths_failed", error=str(e))}
    paths = [
        str(safe_join(home, "Saves", "Multiplayer", server_name)),
        str(safe_join(home, "db", f"{server_name}.db")),
        *config_files,
    ]
    # 옛 백업은 Server/ 폴더를 통째로 담았다: 그 폴더를 복원 허용 범위로 알린다(백업 대상은 아니다, MA3)
    return {"success": True, "paths": paths, "restore_roots": [server_dir]}


# ╔═══════════════════════════════════════════════════════════╗
# ║                  Function Registry                        ║
# ╚═══════════════════════════════════════════════════════════╝

FUNCTIONS = {
    "validate":           validate,
    "get_launch_command": get_launch_command,
    "stop":               stop,
    "command":            command,
    "configure":          configure,
    "import_settings":    import_settings,
    "config_files":       config_files,
    "read_properties":    read_properties,
    "diagnose_log":       diagnose_log,
    "list_versions":      list_versions,
    "install_server":     install_server,
    "reset_server":       reset_server,
    "reset_preview":      reset_preview,
    "backup_paths":       backup_paths,
    "backup_snapshots":   backup_snapshots,
    "player_count":       player_count,
}


# ╔═══════════════════════════════════════════════════════════╗
# ║                    Entry Point                            ║
# ╚═══════════════════════════════════════════════════════════╝

def main():
    """Entry point when invoked by the saba-chan daemon: `python lifecycle.py <function_name>`, config JSON on stdin.

    공용 진입점: 깨진 stdin은 빈 설정으로 계속 실행하지 않고 거절한다(기본 세이브 이름으로 지우지 않게).
    모르는 함수·함수 안의 예외도 JSON으로 답하고 종료 코드 0을 지킨다.
    """
    from daemon_rcon import run_lifecycle
    run_lifecycle(FUNCTIONS)


if __name__ == "__main__":
    main()
