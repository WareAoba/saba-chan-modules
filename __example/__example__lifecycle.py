#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════
 saba-chan Module Template — lifecycle.py

 이 파일은 게임 서버의 생명주기를 관리하는 진입점입니다.
 데몬(Rust)이 각 함수를 호출하고, 결과는 반드시 JSON(dict)으로 반환합니다.
 stdout → JSON 결과, stderr → 로그 (print(..., file=sys.stderr))

 ★ 처음 만드는 분은 아래 순서로 작업하세요:

   1. get_launch_command()의 args를 게임의 실제 커맨드라인 인자로 수정
      → 서버를 수동으로 한번 실행해보고 인자를 확인하세요
      → 이 함수만 제대로 만들면 서버가 켜집니다

   2. validate()에 게임별 검증 추가 (Java 확인, 설정 파일 존재 등)

   3. stop()에 graceful 종료 구현 (RCON/REST로 종료 명령 전송)
      → 처음엔 force kill만으로도 동작합니다

   4. configure()에 설정 파일 쓰기 로직 구현
      → _KEY_MAP에 saba-chan 키 → 게임 키 매핑 추가

   5. 나머지 함수는 필요할 때 하나씩 구현

 ★ 데몬 없이 테스트하기:
   echo '{"server_executable": "C:\\path\\server.exe"}' | python lifecycle.py validate
   echo '{"server_executable": "C:\\path\\server.exe", "port": 7777}' | python lifecycle.py get_launch_command

 참고 모듈: palworld, minecraft, zomboid
═══════════════════════════════════════════════════════════════════

■ 함수 요약 ──────────────────────────────────────────────────────

  필수 (전부 구현):              데몬 호출 시점:
  ─────────────────              ─────────────
  validate(config)               서버 시작 직전 (실패하면 시작 안 됨)
  get_launch_command(config)     validate 통과 후 (★ 가장 중요)
  stop(config)                   사용자가 서버 종료 요청 시
  status(config)                 주기적 상태 조회
  configure(config)              시작 전 + 사용자가 설정 변경 시
  import_settings(config)        인스턴스 최초 등록 시 (기존 설정 가져오기)
  command(config)                rcon_template 없는 커스텀 명령어 실행 시

  선택 (게임에 따라):
  ─────────────────
  install_server(config)         서버 설치/업데이트
  accept_eula(config)            EULA 동의 (Minecraft 등)
  diagnose_log(config)           로그 분석/에러 진단
  list_versions(config)          사용 가능한 버전 목록
  get_version_details(config)    버전 상세정보
  get_credentials(config)        설정 파일에서 RCON/REST 자격증명 읽기
  read_properties(config)        설정 파일 원본 읽기
  reset_properties(config)       설정 초기화
  reset_server(config)           서버 전체 리셋

■ config 객체 (데몬이 stdin으로 전달하는 JSON) ──────────────────

  항상 포함:
    config["instance_id"]       인스턴스 UUID
    config["server_executable"] 실행 파일 경로
    config["working_dir"]       작업 디렉토리
    config["port"]              게임 포트
    config["settings"]          { "server_name": "My Server", ... }

  RCON 모듈:
    config["rcon_port"]         RCON 포트
    config["rcon_password"]     RCON 비밀번호 (자동 생성)

  REST 모듈:
    config["rest_host/port/username/password"]

  stop() 호출 시:
    config["pid"]               종료할 프로세스 PID
    config["force"]             True면 즉시 강제 종료

  command() 호출 시:
    config["command"]           명령어 이름 (예: "backup")
    config["args"]              명령어 인자 dict
"""

import sys
import json
import subprocess
import os
import time
from i18n import I18n

# ─── Path setup ──────────────────────────────────────────────────
MODULE_DIR = os.path.dirname(os.path.abspath(__file__))

# daemon_rcon.py, i18n.py는 saba-chan 본체(modules/)에 있습니다.
# 데몬이 실행 시 PYTHONPATH로 해당 경로를 자동 주입하므로,
# 모듈 폴더에 복사할 필요 없이 바로 import 가능합니다.
from daemon_rcon import rcon_command as _rcon_command_bridge

# i18n 초기화 — 이 모듈의 locales/ 폴더에서 번역 파일을 로드합니다
i18n = I18n(MODULE_DIR)

# 데몬 API 엔드포인트 (환경 변수로 주입됨)
DAEMON_API_URL = os.environ.get('DAEMON_API_URL', 'http://127.0.0.1:57474')


# ═══════════════════════════════════════════════════════════════════
#  설정 키 매핑: saba-chan 내부 키 → 게임 설정 파일 키
# ═══════════════════════════════════════════════════════════════════
#
#  module.toml [[settings.fields]] 의 name  →  게임 설정 파일의 실제 키
#  configure()와 import_settings()에서 사용합니다.
#
_KEY_MAP = {
    # saba-chan 키          # 게임 설정 파일 키
    "server_name":          "ServerName",
    "max_players":          "MaxPlayers",
    "port":                 "ServerPort",
    "server_password":      "Password",
    "pvp":                  "PVP",
    "difficulty":           "Difficulty",
    "tick_rate":            "TickRate",
    # ... 게임 설정 파일의 키에 맞춰 확장
}


# ═══════════════════════════════════════════════════════════════════
#  validate(config)
# ═══════════════════════════════════════════════════════════════════
#  서버를 시작하기 전 환경을 검증합니다.
#  issues 배열에 발견된 문제를 수집합니다.
#
#  severity:
#    "critical" → success=False (서버 시작 불가)
#    "warning"  → success=True  (경고만 표시)
#
def validate(config):
    issues = []

    # 1) 실행 파일 존재 여부
    executable = config.get("server_executable", "")
    if not executable or not os.path.exists(executable):
        issues.append({
            "code": "EXECUTABLE_NOT_FOUND",
            "severity": "critical",
            "message": f"Server executable not found: {executable}",
            "solution": "Set the correct path in server settings, or install the server first."
        })

    # 2) 작업 디렉토리
    working_dir = config.get("working_dir", "")
    if working_dir and not os.path.isdir(working_dir):
        issues.append({
            "code": "WORKING_DIR_MISSING",
            "severity": "warning",
            "message": f"Working directory does not exist: {working_dir}",
            "solution": "The directory will be created on server start."
        })

    # 3) 포트 충돌 검사
    port = config.get("port", 7777)
    # ... 필요 시 소켓 바인드 테스트

    has_critical = any(i["severity"] == "critical" for i in issues)
    return {
        "success": not has_critical,
        "issues": issues
    }


# ═══════════════════════════════════════════════════════════════════
#  get_launch_command(config)  ★ 가장 중요한 함수
# ═══════════════════════════════════════════════════════════════════
#  데몬의 ManagedProcess가 서버를 실행하기 위한 정보를 반환합니다.
#  데몬은 이 결과로 프로세스를 spawn하고, 상태를 추적합니다.
#
#  ── 필수 반환 키 ──
#    success: bool
#    program: str          실행 파일 절대 경로
#    args: list[str]       커맨드라인 인자
#    working_dir: str      작업 디렉토리 절대 경로
#    env_vars: dict        환경 변수 (빈 dict 가능)
#
#  ── 선택 반환 키 (credential_map 연동) ──
#    "GameSpecificKey": str   module.toml [credential_map]에서 선언한
#                             게임 키의 값. 데몬이 자동으로 데몬 키에 동기화.
#
#    예: [credential_map] rest_password = "AdminPassword" 이면
#        launch_result에 "AdminPassword": "xxxx" 포함 시
#        → instance.rest_password = "xxxx" 자동 동기화
#
def get_launch_command(config):
    executable = config.get("server_executable")
    if not executable:
        return {"success": False, "message": "server_executable not specified"}
    if not os.path.exists(executable):
        return {"success": False, "message": f"Executable not found: {executable}"}

    port = config.get("port", 7777)
    working_dir = config.get("working_dir") or os.path.dirname(os.path.abspath(executable))

    # ── 실행 전 준비 작업 (게임별로 다름) ──
    # 예: 설정 파일 동기화, RCON 정책 적용, EULA 확인 등
    # _enforce_rcon_policy(working_dir, config)

    args = [
        f"--port={port}",
        # ... 게임별 인자
    ]

    result = {
        "success": True,
        "program": os.path.abspath(executable),
        "args": args,
        "working_dir": os.path.abspath(working_dir),
        "env_vars": {},
    }

    # ── credential_map 연동 ──
    # module.toml에 [credential_map] rcon_password = "GameRconKey" 이면:
    # result["GameRconKey"] = actual_rcon_password_value
    # → 데몬이 instance.rcon_password에 자동 동기화

    return result


# ═══════════════════════════════════════════════════════════════════
#  stop(config)
# ═══════════════════════════════════════════════════════════════════
#  서버를 종료합니다.
#
#  ── 종료 전략 (우선순위) ──
#   1. Graceful: RCON/stdin으로 "stop"/"quit" 전송 → 프로세스 종료 대기
#   2. Force: 즉시 taskkill/kill (config["force"]=True이거나 graceful 실패 시)
#
#  config["pid"]: 데몬이 알려주는 프로세스 PID (managed 모드)
#  config["force"]: 강제 종료 플래그
#
def stop(config):
    pid = config.get("pid")
    force = config.get("force", False)
    graceful = config.get("graceful_stop", True) and not force

    if graceful and pid:
        # 1) Graceful: RCON으로 종료 명령 전송
        rcon_password = config.get("rcon_password", "")
        rcon_port = config.get("rcon_port", 25575)
        if rcon_password:
            try:
                _rcon_command_bridge("127.0.0.1", rcon_port, rcon_password, "stop")
                # 프로세스 종료 대기 (최대 30초)
                for _ in range(30):
                    if not _is_process_running(pid):
                        return {"success": True, "message": "Server stopped gracefully"}
                    time.sleep(1)
            except Exception as e:
                print(f"[STOP] Graceful stop failed: {e}", file=sys.stderr)

    # 2) Force kill
    if pid:
        _force_kill(pid)
        return {"success": True, "message": "Server force-killed"}

    return {"success": False, "message": "No PID available to stop the server"}


# ═══════════════════════════════════════════════════════════════════
#  status(config)
# ═══════════════════════════════════════════════════════════════════
#  서버 상태를 조회합니다.
#
#  ── 반환 형식 ──
#  { "success": True, "status": "running"|"stopped"|"starting",
#    "players_online": int, "players_max": int, ... }
#
def status(config):
    pid = config.get("pid")

    if pid and _is_process_running(pid):
        result = {
            "success": True,
            "status": "running",
        }
        # 선택: RCON/Query로 접속자 수 등 상세 정보 조회
        # rcon_password = config.get("rcon_password", "")
        # if rcon_password:
        #     result["players_online"] = ...
        #     result["players_max"] = ...
        return result

    return {"success": True, "status": "stopped"}


# ═══════════════════════════════════════════════════════════════════
#  configure(config)
# ═══════════════════════════════════════════════════════════════════
#  사용자 설정을 게임 설정 파일에 적용합니다.
#
#  config["settings"]: { "server_name": "My Server", ... }
#  → _KEY_MAP으로 게임 키 변환 후 설정 파일에 기록
#
def configure(config):
    settings = config.get("settings", {})
    if not settings:
        return {"success": True, "message": "No settings to apply"}

    working_dir = config.get("working_dir", "")
    config_file = os.path.join(working_dir, "server.cfg")  # ← 게임별 설정 파일 경로

    try:
        # 1) 기존 설정 파일 읽기 (없으면 빈 dict)
        current = _read_config_file(config_file)

        # 2) 키 매핑 적용
        for saba_key, value in settings.items():
            game_key = _KEY_MAP.get(saba_key)
            if game_key:
                current[game_key] = value

        # 3) 설정 파일 쓰기
        _write_config_file(config_file, current)

        return {"success": True, "message": f"Applied {len(settings)} settings"}
    except Exception as e:
        return {"success": False, "message": str(e)}


# ═══════════════════════════════════════════════════════════════════
#  import_settings(config)
# ═══════════════════════════════════════════════════════════════════
#  기존 게임 설정 파일을 읽어 saba-chan 키로 변환합니다 (역매핑).
#  configure()의 반대 방향입니다. 첫 연결 시 기존 설정을 자동 임포트합니다.
#
def import_settings(config):
    working_dir = config.get("working_dir", "")
    config_file = os.path.join(working_dir, "server.cfg")

    if not os.path.exists(config_file):
        return {"success": False, "message": "Config file not found"}

    try:
        current = _read_config_file(config_file)

        # 역매핑: 게임 키 → saba-chan 키
        reverse_map = {v: k for k, v in _KEY_MAP.items()}
        imported = {}
        for game_key, value in current.items():
            saba_key = reverse_map.get(game_key)
            if saba_key:
                imported[saba_key] = value

        return {"success": True, "settings": imported}
    except Exception as e:
        return {"success": False, "message": str(e)}


# ═══════════════════════════════════════════════════════════════════
#  command(config)
# ═══════════════════════════════════════════════════════════════════
#  서버에 명령어를 실행합니다.
#
#  config["command"]: 명령어 이름 (module.toml [[commands.fields]]의 name)
#  config["args"]: 명령어 인자 dict
#
#  module.toml에서 method="rcon" + rcon_template 을 정의한 명령어는
#  데몬이 직접 RCON으로 전송하므로, lifecycle.py에 도달하지 않습니다.
#  여기에는 커스텀 로직이 필요한 명령어만 구현하면 됩니다.
#
def command(config):
    cmd = config.get("command", "")
    args = config.get("args", {})
    instance_id = config.get("instance_id", "")

    # 커스텀 명령어 처리 예시
    if cmd == "backup":
        return _do_backup(config)

    # 기본: RCON으로 전달
    rcon_password = config.get("rcon_password", "")
    rcon_port = config.get("rcon_port", 25575)

    if not rcon_password:
        return {"success": False, "message": "RCON password not configured"}

    try:
        response = _rcon_command_bridge(
            "127.0.0.1", rcon_port, rcon_password, cmd
        )
        return {"success": True, "response": response}
    except Exception as e:
        return {"success": False, "message": str(e)}


# ═══════════════════════════════════════════════════════════════════
#  install_server(config)  [선택]
# ═══════════════════════════════════════════════════════════════════
#  서버를 설치하거나 업데이트합니다.
#
#  module.toml [install] method에 따라:
#    - "steamcmd": SteamCMD 확장을 호출
#    - "download": URL에서 다운로드
#    - "manual": 안내 메시지만 반환
#
def install_server(config):
    install_dir = config.get("install_dir", "")
    if not install_dir:
        return {"success": False, "message": "install_dir not specified"}

    # SteamCMD 예시
    try:
        from extensions.steamcmd import install_game
        result = install_game(
            app_id=123456,
            install_dir=install_dir,
            anonymous=True,
        )
        if result.get("success"):
            # 설치 후 초기 설정 적용
            configure(config)
        return result
    except ImportError:
        return {"success": False, "message": "steamcmd extension not installed"}


# ═══════════════════════════════════════════════════════════════════
#  diagnose_log(config)  [선택]
# ═══════════════════════════════════════════════════════════════════
#  서버 로그를 분석하여 에러와 해결책을 제시합니다.
#
def diagnose_log(config):
    # 구현 예시: 로그 파일 읽기 → 패턴 매칭 → 진단 결과 반환
    return {
        "success": True,
        "diagnostics": [],  # [{ "level": "error", "message": "...", "solution": "..." }]
    }


# ═══════════════════════════════════════════════════════════════════
#  내부 헬퍼 함수들
# ═══════════════════════════════════════════════════════════════════

def _is_process_running(pid):
    """PID가 실행 중인지 확인"""
    try:
        if os.name == 'nt':
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
                capture_output=True, text=True, timeout=5
            )
            return str(pid) in result.stdout
        else:
            os.kill(pid, 0)
            return True
    except (ProcessLookupError, PermissionError):
        return False
    except Exception:
        return False


def _force_kill(pid):
    """프로세스 강제 종료"""
    try:
        if os.name == 'nt':
            subprocess.run(["taskkill", "/F", "/PID", str(pid)],
                           capture_output=True, timeout=10)
        else:
            import signal
            os.kill(pid, signal.SIGKILL)
    except Exception as e:
        print(f"[STOP] Force kill failed: {e}", file=sys.stderr)


def _read_config_file(path):
    """게임 설정 파일 읽기 (게임별로 구현)"""
    # 예: INI 파서, properties 파서, JSON 등
    config = {}
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if '=' in line and not line.startswith('#'):
                    key, _, value = line.partition('=')
                    config[key.strip()] = value.strip()
    return config


def _write_config_file(path, config):
    """게임 설정 파일 쓰기 (게임별로 구현)"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        for key, value in config.items():
            f.write(f"{key}={value}\n")


def _do_backup(config):
    """커스텀 백업 명령어 예시"""
    return {"success": True, "message": "Backup not implemented yet"}


# ═══════════════════════════════════════════════════════════════════
#  CLI 진입점
# ═══════════════════════════════════════════════════════════════════
#
#  데몬이 호출하는 방식:
#    echo '<config_json>' | python lifecycle.py <function>
#
#  config는 반드시 stdin으로 전달해야 합니다.
#  (커맨드라인 인자로 전달 시 민감정보 노출 및 OS 길이 제한 문제 발생)
#
FUNCTIONS = {
    "validate": validate,
    "get_launch_command": get_launch_command,
    "start": get_launch_command,  # start → get_launch_command 위임
    "stop": stop,
    "status": status,
    "configure": configure,
    "import_settings": import_settings,
    "command": command,
    "install_server": install_server,
    "diagnose_log": diagnose_log,
}

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(json.dumps({"success": False, "message": "Usage: lifecycle.py <function>"}))
        sys.exit(1)

    function_name = sys.argv[1]

    # config JSON을 stdin에서 읽기
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

    print(json.dumps(result, ensure_ascii=False))
