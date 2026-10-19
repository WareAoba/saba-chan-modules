#!/usr/bin/env python3
"""
Palworld server lifecycle management module.
Always outputs JSON to stdout, logs to stderr.
"""

import sys
import json
import subprocess
import os
import socket
import struct
import time
import random
import base64
import io
import urllib.request
import urllib.error
import urllib.parse
from i18n import I18n

# PYTHONPATH is injected by the Rust plugin runner; fallback for direct execution:
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

# 명령은 데몬의 REST 경로로 보낸다 (RCON은 보안 정책으로 꺼 둔다)
try:
    from daemon_rcon import daemon_post
except ImportError:
    # 예전 코어의 daemon_rcon에는 daemon_post가 없다: 모듈은 불러지고, 그 경로를 쓰는 명령만 안내와 함께 실패한다
    def daemon_post(path, payload, timeout=10):
        raise ConnectionError("this saba-chan core is too old for this Palworld module; update saba-chan")
# 격리 실행(미확인 모듈)은 127.0.0.1에 닿지 못한다: 데몬이 이 인스턴스 포트로 대신 보낸다 (SABA_LOOPBACK_DIR)
try:
    from daemon_rcon import loopback_http
except ImportError:
    loopback_http = None


class _RelayedResponse(io.BytesIO):
    """`urllib.request.urlopen` 응답처럼 쓰는 중계 응답"""

    def __init__(self, status, body):
        super().__init__(body)
        self.status = status

    def getcode(self):
        return self.status


def _urlopen(req, timeout):
    """`urllib.request.urlopen`과 같다. 격리 실행이 루프백에 보내는 요청만 데몬 중계(`loopback_http`)로 보낸다.
    실패는 urlopen과 같은 예외(HTTPError·URLError)로 낸다"""
    url = urllib.parse.urlsplit(req.full_url)
    if not (os.environ.get("SABA_LOOPBACK_DIR") and loopback_http and url.hostname in ("127.0.0.1", "localhost")):
        return urllib.request.urlopen(req, timeout=timeout)
    path = url.path + (f"?{url.query}" if url.query else "")
    try:
        status, body = loopback_http(url.port or 80, req.get_method(), path, headers=dict(req.header_items()),
                                     body=req.data, timeout=timeout)
    except OSError as e:
        raise urllib.error.URLError(e) from e
    if status >= 400:
        raise urllib.error.HTTPError(req.full_url, status, "", None, io.BytesIO(body))
    return _RelayedResponse(status, body)


# UE4 INI parser (ue4-ini extension). Without it the module still answers;
# functions that touch PalWorldSettings.ini return extension_required.
try:
    from extensions.ue4_ini import parse_option_settings, write_option_settings
except ImportError:
    parse_option_settings = write_option_settings = None

# Initialize i18n
MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
i18n = I18n(MODULE_DIR)


class PalworldRestClient:
    """Minimal REST client for Palworld built-in REST API."""

    def __init__(self, host='127.0.0.1', port=8212, username='', password='', timeout=5):
        self.base_url = f"http://{host}:{port}/v1/api"
        self.auth_header = self._build_auth(username, password) if username or password else None
        self.timeout = timeout

    def _build_auth(self, username, password):
        token = base64.b64encode(f"{username}:{password}".encode('utf-8')).decode('utf-8')
        return f"Basic {token}"

    def _request(self, method, path, payload=None):
        url = f"{self.base_url}{path}"
        headers = {"Accept": "application/json"}
        if self.auth_header:
            headers["Authorization"] = self.auth_header

        data = None
        if payload is not None:
            data = json.dumps(payload).encode('utf-8')
            headers["Content-Type"] = "application/json"

        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        print(f"[REST] {method} {url} payload={payload}", file=sys.stderr)

        try:
            with _urlopen(req, timeout=self.timeout) as resp:
                body = resp.read().decode('utf-8', errors='ignore')
                if not body:
                    return None
                try:
                    return json.loads(body)
                except json.JSONDecodeError:
                    return body
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8', errors='ignore') if hasattr(e, 'read') else ''
            raise RuntimeError(f"HTTP {e.code} {e.reason}: {error_body}")
        except Exception as e:
            raise RuntimeError(str(e))

    def announce(self, message):
        return self._request("POST", "/announce", {"message": message})

    def info(self):
        return self._request("GET", "/info")

    def players(self):
        return self._request("GET", "/players")

    def metrics(self):
        return self._request("GET", "/metrics")

    def kick(self, userid, message=None):
        payload = {"userid": userid}
        if message:
            payload["message"] = message
        return self._request("POST", "/kick", payload)

    def ban(self, userid, message=None):
        payload = {"userid": userid}
        if message:
            payload["message"] = message
        return self._request("POST", "/ban", payload)

    def unban(self, userid):
        return self._request("POST", "/unban", {"userid": userid})


def resolve_player_id(instance_id, player_input, config):
    """
    플레이어 입력값을 SteamID로 변환합니다.
    - 이미 SteamID 형식(steam_xxxxxxxxx 또는 숫자만)이면 그대로 반환
    - 닉네임이면 플레이어 목록에서 검색하여 SteamID로 변환
    
    Returns:
        tuple: (success, steamid_or_error_message)
    """
    # 이미 SteamID 형식인지 확인 (steam_로 시작하거나 숫자만 있는 경우)
    if player_input.startswith("steam_") or player_input.isdigit():
        print(f"[Palworld] Input '{player_input}' is already a SteamID format", file=sys.stderr)
        return (True, player_input)
    
    # 닉네임으로 간주하고 플레이어 목록에서 검색
    print(f"[Palworld] Resolving nickname '{player_input}' to SteamID...", file=sys.stderr)
    
    try:
        # config에서 REST 설정 가져오기
        rest_host = config.get("rest_host", "127.0.0.1")
        rest_port = config.get("rest_port", 8212)
        rest_username = config.get("rest_username") or "admin"
        rest_password = _read_admin_password_from_ini(config) or ""
        
        # 직접 Palworld 서버에 REST 요청 (Daemon을 거치지 않음 - 데드락 방지)
        palworld_url = f"http://{rest_host}:{rest_port}/v1/api/players"
        print(f"[Palworld] Fetching players from: {palworld_url}", file=sys.stderr)
        
        req = urllib.request.Request(palworld_url, method='GET')
        
        # Basic Auth 설정
        if rest_username and rest_password:
            credentials = f"{rest_username}:{rest_password}"
            encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
            req.add_header('Authorization', f'Basic {encoded_credentials}')
        
        with _urlopen(req, timeout=5) as response:
            result = json.loads(response.read().decode('utf-8'))
            
            # Palworld API 직접 응답 구조
            players = result.get("players", [])
            
            print(f"[Palworld] Found {len(players)} online players", file=sys.stderr)
            
            # 닉네임으로 플레이어 검색 (대소문자 무시)
            # name (캐릭터 이름)과 accountName (Steam 계정 이름) 둘 다 검색
            player_input_lower = player_input.lower()
            for player in players:
                player_name = player.get("name", "")
                account_name = player.get("accountName", "")
                player_userid = player.get("userId") or player.get("playerId") or player.get("steamId")
                
                # 정확 일치 (캐릭터 이름 또는 Steam 계정 이름)
                if player_name.lower() == player_input_lower or account_name.lower() == player_input_lower:
                    matched_name = account_name if account_name.lower() == player_input_lower else player_name
                    print(f"[Palworld] Found player: {matched_name} (char: {player_name}) -> {player_userid}", file=sys.stderr)
                    return (True, player_userid)
            
            # 부분 일치 검색
            partial_matches = []
            for player in players:
                player_name = player.get("name", "")
                account_name = player.get("accountName", "")
                player_userid = player.get("userId") or player.get("playerId") or player.get("steamId")
                
                # 부분 일치 (캐릭터 이름 또는 Steam 계정 이름)
                if player_input_lower in player_name.lower() or player_input_lower in account_name.lower():
                    display_name = f"{account_name} ({player_name})" if account_name else player_name
                    partial_matches.append((display_name, player_userid))
            
            if len(partial_matches) == 1:
                name, userid = partial_matches[0]
                print(f"[Palworld] Found partial match: {name} -> {userid}", file=sys.stderr)
                return (True, userid)
            elif len(partial_matches) > 1:
                names = [m[0] for m in partial_matches]
                return (False, i18n.t("errors.player_ambiguous", player=player_input, names=", ".join(names)))
            
            return (False, i18n.t("errors.player_not_found"))
    
    except urllib.error.URLError as e:
        print(f"[Palworld] Failed to fetch player list: {e}", file=sys.stderr)
        return (False, i18n.t("errors.connection_failed", error=str(e)))
    except Exception as e:
        print(f"[Palworld] Error resolving player: {e}", file=sys.stderr)
        return (False, i18n.t("errors.player_list_failed", error=str(e)))


def _read_admin_password_from_ini(config):
    """
    PalWorldSettings.ini에서 AdminPassword를 직접 읽어 반환합니다.
    매번 파일에서 읽으므로 메모리 동기화 문제가 없습니다.
    """
    try:
        ini_path = _get_settings_ini_path(config)
        if ini_path and os.path.isfile(ini_path):
            props = _parse_option_settings(ini_path)
            pw = props.get("AdminPassword", "").strip('"').strip("'")
            if pw:
                return pw
    except Exception as e:
        print(f"[Palworld] Warning: failed to read AdminPassword from INI: {e}", file=sys.stderr)
    return None

def _enforce_rest_policy(working_dir, config=None):
    """
    Enforce REST-only policy in PalWorldSettings.ini before server launch.

    Palworld RCON is deprecated by the developer — saba-chan uses REST API exclusively.
    - RESTAPIEnabled = True  (always on)
    - RCONEnabled = False    (always off)
    - AdminPassword synced with daemon's rest_password (required for REST API auth)
    - ServerPassword is NOT touched (user-facing join password)

    Returns dict with details of what was changed + the actual admin_password, or None if INI doesn't exist yet.
    """
    # Build a fake config to reuse _get_settings_ini_path
    fake_config = {"working_dir": working_dir}
    ini_path = _get_settings_ini_path(fake_config)
    if not ini_path or not os.path.isfile(ini_path):
        return None  # INI will be generated on first run

    props = _parse_option_settings(ini_path)
    changes = {}

    # INI 파일이 존재하지만 비어있는 경우(빈 파일/파싱 실패) → 기본 설정으로 채움
    if not props:
        print("[Palworld] INI file is empty/corrupt, populating with defaults", file=sys.stderr)
        props = dict(DEFAULT_PALWORLD_SETTINGS)
        changes["__populated_defaults"] = True

    # Force REST API enabled
    if props.get("RESTAPIEnabled", "False") != "True":
        props["RESTAPIEnabled"] = "True"
        changes["RESTAPIEnabled"] = "True"
        print(i18n.t("messages.rest_api_forced_on"), file=sys.stderr)

    # 데몬이 접속할 REST 포트(인스턴스 rest_port, 자동 배정될 수 있다)를 INI에 맞춘다
    rest_port = (config or {}).get("rest_port")
    if rest_port and props.get("RESTAPIPort") != str(rest_port):
        props["RESTAPIPort"] = str(rest_port)
        changes["RESTAPIPort"] = str(rest_port)

    # Force RCON disabled
    if props.get("RCONEnabled", "False") != "False":
        props["RCONEnabled"] = "False"
        changes["RCONEnabled"] = "False"
        print(i18n.t("messages.rcon_forced_off"), file=sys.stderr)

    # Sync AdminPassword with daemon's rest_password to ensure credentials match.
    # daemon의 ensure_passwords()가 항상 rest_password를 생성하므로 config에 존재함.
    current_admin_pw = props.get("AdminPassword", "").strip('"').strip("'")
    daemon_pw = (config or {}).get("rest_password") or (config or {}).get("admin_password", "")

    if current_admin_pw:
        # INI already has a password — use it
        pass
    elif daemon_pw:
        # No INI password but daemon has one — write it to INI
        props["AdminPassword"] = daemon_pw
        current_admin_pw = daemon_pw
        changes["AdminPassword"] = "***synced-from-config***"
        print(f"[Palworld] Synced AdminPassword from config", file=sys.stderr)
    else:
        # 비밀번호가 어디에도 없는 경우 (첫 실행, 빈 INI 등)
        # RESTAPIEnabled=True / RCONEnabled=False는 반드시 기록해야 하므로
        # 임시 비밀번호를 생성하여 INI에 기록하고, credential_map을 통해
        # 데몬에 동기화되도록 한다.
        import uuid
        fallback_pw = uuid.uuid4().hex[:16]
        props["AdminPassword"] = fallback_pw
        current_admin_pw = fallback_pw
        changes["AdminPassword"] = "***auto-generated***"
        print(f"[Palworld] No password from daemon or INI — generated fallback AdminPassword", file=sys.stderr)

    if changes:
        _write_option_settings(ini_path, props)
        return {"changed": True, "changes": changes, "admin_password": current_admin_pw}
    return {"changed": False, "admin_password": current_admin_pw}


def get_launch_command(config):
    """
    Build the command for the Rust daemon to spawn as a ManagedProcess.
    Returns { success, program, args, working_dir, env_vars }.
    """
    executable = config.get("server_executable")
    if not executable:
        return {
            "success": False,
            "message": i18n.t("errors.no_executable")
        }

    if not os.path.exists(executable):
        return {
            "success": False,
            "message": i18n.t("errors.executable_not_found", path=executable)
        }

    port = config.get("port", 8211)
    working_dir = config.get("working_dir")
    if not working_dir:
        working_dir = os.path.dirname(os.path.abspath(executable))

    abs_working_dir = os.path.abspath(working_dir)

    # Enforce REST-only policy before launch (미리보기는 INI를 고치지 않는다)
    rest_result = None if config.get("preview") else _enforce_rest_policy(abs_working_dir, config)

    args = [f"--port={port}"]

    # PalServer.exe는 실제 서버(Shipping-Cmd)를 띄우고 빠지는 런처다. 데몬(saba-runner)이
    # 실제 서버의 PID·종료 코드를 쥐도록 런처가 하는 그대로 실제 서버를 직접 띄운다.
    # 실제 서버 exe가 없으면(다른 배포 형태) 예전처럼 런처를 띄운다.
    program = os.path.abspath(executable)
    real_server = os.path.join(os.path.dirname(program), "Pal", "Binaries", "Win64",
                               "PalServer-Win64-Shipping-Cmd.exe")
    if os.path.exists(real_server):
        program = real_server
        args = ["Pal"] + args  # 런처가 붙이는 프로젝트 이름

    result = {
        "success": True,
        "program": program,
        "args": args,
        "working_dir": abs_working_dir,
        "env_vars": {},
    }

    # credential_map 기반 자동 동기화: module.toml [credential_map]에서
    # rest_password = "AdminPassword" 매핑이 선언되어 있으므로,
    # AdminPassword 값을 최상위에 반환하면 데몬이 자동으로 rest_password에 동기화한다.
    if rest_result and rest_result.get("admin_password"):
        result["AdminPassword"] = rest_result["admin_password"]

    return result


def _pid_running(pid):
    if sys.platform == 'win32':
        out = subprocess.run(['tasklist', '/FI', f'PID eq {pid}', '/NH'],
                             capture_output=True, text=True, check=False,
                             creationflags=0x08000000).stdout
        return str(pid) in out
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _rest_client(config):
    """설정의 rest_host와 INI의 AdminPassword·RESTAPIPort로 이 서버의 REST 클라이언트를 만든다. 비밀번호가 없으면 None"""
    password = _read_admin_password_from_ini(config)
    if not password:
        return None
    port = config.get("rest_port")
    if not port:
        try:
            port = int(_parse_option_settings(_get_settings_ini_path(config)).get("RESTAPIPort", 8212))
        except Exception:
            port = 8212
    return PalworldRestClient(host=config.get("rest_host") or "127.0.0.1", port=port, username="admin", password=password, timeout=5)


def player_count(config):
    """접속자 수 (module.toml [status] players). 데몬이 켜진 서버에 1분마다 부른다"""
    client = _rest_client(config)
    if client is None:
        return {"success": False, "message": i18n.t("errors.player_list_failed", error="AdminPassword")}
    try:
        metrics = client.metrics()
        return {"success": True, "online": int(metrics["currentplayernum"]), "max": int(metrics.get("maxplayernum") or 0)}
    except (RuntimeError, KeyError, TypeError, ValueError) as e:
        return {"success": False, "message": i18n.t("errors.player_list_failed", error=str(e))}


def _rest_shutdown(config):
    """REST로 저장 후 종료를 요청한다. 요청이 들어가면 True."""
    client = _rest_client(config)
    if client is None:
        return False
    try:
        client._request("POST", "/save", {})
    except RuntimeError as e:
        print(f"[Palworld] REST save before shutdown failed: {e}", file=sys.stderr)
    try:
        client._request("POST", "/shutdown", {"waittime": 1, "message": "Server is shutting down"})
        return True
    except RuntimeError as e:
        print(f"[Palworld] REST shutdown failed: {e}", file=sys.stderr)
        return False


def _kill(pid):
    """그 PID(와 자식)만 강제 종료한다. 실행 파일 이름으로는 끝내지 않는다: 같은 게임의 다른 인스턴스까지 죽는다"""
    if sys.platform == 'win32':
        subprocess.run(['taskkill', '/F', '/T', '/PID', str(pid)], capture_output=True, text=True,
                       check=False, creationflags=0x08000000)
    else:
        try:
            os.kill(pid, 9)
        except OSError:
            pass


def stop(config):
    """Stop Palworld server.

    Palworld는 콘솔(stdin) 명령을 받지 않으므로 REST로 저장·종료를 요청하고,
    시간 안에 끝나지 않으면 강제 종료한다. 데몬이 PID를 주면(managed) 그 프로세스만 다룬다.
    """
    try:
        pid = config.get("pid")
        force = config.get("force", False)
        exe_name = os.path.basename(config.get("server_executable") or "PalServer.exe")
        if not pid:
            # 데몬이 PID를 모르면 이 인스턴스의 서버가 아니다(또는 안 돌고 있다)
            return {"success": True, "message": i18n.t("messages.process_not_running", name=exe_name)}

        if not force and _rest_shutdown(config):
            for _ in range(60):
                time.sleep(1)
                if not _pid_running(pid):
                    return {"success": True, "message": i18n.t("messages.terminated", name=exe_name)}
            print("[Palworld] Server did not exit after REST shutdown, force killing", file=sys.stderr)

        _kill(pid)
        return {"success": True, "message": i18n.t("messages.terminated", name=exe_name)}
    except Exception as e:
        error_msg = i18n.t("errors.failed_to_stop_process", error=str(e))
        print(error_msg, file=sys.stderr)
        return {"success": False, "message": error_msg}


def get_credentials(config):
    """게임 설정 파일에서 직접 credential을 읽어 반환합니다.
    Rust daemon이 명령어 실행 전에 호출하여 실제 비밀번호를 얻습니다.
    매번 파일에서 읽으므로 메모리 동기화 문제가 없습니다."""
    try:
        pw = _read_admin_password_from_ini(config)
        return {
            "success": True,
            "rest_password": pw or "",
            "rest_username": "admin",
        }
    except Exception as e:
        return {"success": False, "message": str(e)}


def status(config):
    """서버 상태. 데몬이 추적하는 PID로만 판단한다: 실행 파일 이름으로 찾으면 같은 게임의 다른 인스턴스와 섞인다"""
    try:
        exe_name = os.path.basename(config.get("server_executable") or "PalServer.exe")
        pid = config.get("pid")
        running = bool(pid) and _pid_running(pid)
        key = "messages.process_running" if running else "messages.process_not_running"
        return {"success": True, "status": "running" if running else "stopped", "message": i18n.t(key, name=exe_name)}
    except Exception as e:
        return {
            "success": False,
            "message": i18n.t("errors.failed_to_get_status", error=str(e))
        }

# ╔═══════════════════════════════════════════════════════════╗
# ║            PalWorldSettings.ini Manager                   ║
# ╚═══════════════════════════════════════════════════════════╝

# Default values from DefaultPalWorldSettings.ini
DEFAULT_PALWORLD_SETTINGS = {
    "Difficulty": "None",
    "RandomizerType": "None",
    "RandomizerSeed": "",
    "bIsRandomizerPalLevelRandom": "False",
    "DayTimeSpeedRate": "1.000000",
    "NightTimeSpeedRate": "1.000000",
    "ExpRate": "1.000000",
    "PalCaptureRate": "1.000000",
    "PalSpawnNumRate": "1.000000",
    "PalDamageRateAttack": "1.000000",
    "PalDamageRateDefense": "1.000000",
    "PlayerDamageRateAttack": "1.000000",
    "PlayerDamageRateDefense": "1.000000",
    "PlayerStomachDecreaceRate": "1.000000",
    "PlayerStaminaDecreaceRate": "1.000000",
    "PlayerAutoHPRegeneRate": "1.000000",
    "PlayerAutoHpRegeneRateInSleep": "1.000000",
    "PalStomachDecreaceRate": "1.000000",
    "PalStaminaDecreaceRate": "1.000000",
    "PalAutoHPRegeneRate": "1.000000",
    "PalAutoHpRegeneRateInSleep": "1.000000",
    "BuildObjectHpRate": "1.000000",
    "BuildObjectDamageRate": "1.000000",
    "BuildObjectDeteriorationDamageRate": "1.000000",
    "CollectionDropRate": "1.000000",
    "CollectionObjectHpRate": "1.000000",
    "CollectionObjectRespawnSpeedRate": "1.000000",
    "EnemyDropItemRate": "1.000000",
    "DeathPenalty": "All",
    "bEnablePlayerToPlayerDamage": "False",
    "bEnableFriendlyFire": "False",
    "bEnableInvaderEnemy": "True",
    "bActiveUNKO": "False",
    "bEnableAimAssistPad": "True",
    "bEnableAimAssistKeyboard": "False",
    "DropItemMaxNum": "3000",
    "DropItemMaxNum_UNKO": "100",
    "BaseCampMaxNum": "128",
    "BaseCampWorkerMaxNum": "15",
    "DropItemAliveMaxHours": "1.000000",
    "bAutoResetGuildNoOnlinePlayers": "False",
    "AutoResetGuildTimeNoOnlinePlayers": "72.000000",
    "GuildPlayerMaxNum": "20",
    "BaseCampMaxNumInGuild": "4",
    "PalEggDefaultHatchingTime": "72.000000",
    "WorkSpeedRate": "1.000000",
    "AutoSaveSpan": "30.000000",
    "bIsMultiplay": "False",
    "bIsPvP": "False",
    "bHardcore": "False",
    "bPalLost": "False",
    "bCharacterRecreateInHardcore": "False",
    "bCanPickupOtherGuildDeathPenaltyDrop": "False",
    "bEnableNonLoginPenalty": "True",
    "bEnableFastTravel": "True",
    "bEnableFastTravelOnlyBaseCamp": "False",
    "bIsStartLocationSelectByMap": "True",
    "bExistPlayerAfterLogout": "False",
    "bEnableDefenseOtherGuildPlayer": "False",
    "bInvisibleOtherGuildBaseCampAreaFX": "False",
    "bBuildAreaLimit": "False",
    "ItemWeightRate": "1.000000",
    "CoopPlayerMaxNum": "4",
    "ServerPlayerMaxNum": "32",
    "ServerName": "Default Palworld Server",
    "ServerDescription": "",
    "AdminPassword": "",
    "ServerPassword": "",
    "bAllowClientMod": "True",
    "PublicPort": "8211",
    "PublicIP": "",
    "RCONEnabled": "False",
    "RCONPort": "25575",
    "Region": "",
    "bUseAuth": "True",
    "BanListURL": "https://api.palworldgame.com/api/banlist.txt",
    "RESTAPIEnabled": "False",
    "RESTAPIPort": "8212",
    "bShowPlayerList": "False",
    "ChatPostLimitPerMinute": "30",
    "CrossplayPlatforms": "(Steam,Xbox,PS5,Mac)",
    "bIsUseBackupSaveData": "True",
    "LogFormatType": "Text",
    "bIsShowJoinLeftMessage": "True",
    "SupplyDropSpan": "180",
    "EnablePredatorBossPal": "True",
    "MaxBuildingLimitNum": "0",
    "ServerReplicatePawnCullDistance": "15000.000000",
    "bAllowGlobalPalboxExport": "True",
    "bAllowGlobalPalboxImport": "False",
    "EquipmentDurabilityDamageRate": "1.000000",
    "ItemContainerForceMarkDirtyInterval": "1.000000",
    "ItemCorruptionMultiplier": "1.000000",
    "DenyTechnologyList": "",
    "GuildRejoinCooldownMinutes": "0",
    "BlockRespawnTime": "5.000000",
    "RespawnPenaltyDurationThreshold": "0.000000",
    "RespawnPenaltyTimeScale": "2.000000",
    "bDisplayPvPItemNumOnWorldMap_BaseCamp": "False",
    "bDisplayPvPItemNumOnWorldMap_Player": "False",
    "AdditionalDropItemWhenPlayerKillingInPvPMode": "PlayerDropItem",
    "AdditionalDropItemNumWhenPlayerKillingInPvPMode": "1",
    "bAdditionalDropItemWhenPlayerKillingInPvPMode": "False",
    "bAllowEnhanceStat_Health": "True",
    "bAllowEnhanceStat_Attack": "True",
    "bAllowEnhanceStat_Stamina": "True",
    "bAllowEnhanceStat_Weight": "True",
    "bAllowEnhanceStat_WorkSpeed": "True",
    "PhysicsActiveDropItemMaxNum": "-1",
    "PlayerDataPalStorageUpdateCheckTickInterval": "1.000000",
    "MonsterFarmActionSpeedRate": "1.000000",
    "AutoTransferMasterCheckIntervalSeconds": "3600.000000",
    "AutoTransferMasterThresholdDays": "14",
    "MaxGuildsPerFrame": "10",
    "bEnableVoiceChat": "False",
    "VoiceChatMaxVolumeDistance": "3000.000000",
    "VoiceChatZeroVolumeDistance": "15000.000000",
    "bEnableBuildingPlayerUIdDisplay": "False",
    "BuildingNameDisplayCacheTTLSeconds": "60",
}

# saba-chan key → PalWorldSettings.ini key (full mapping)
_PALWORLD_KEY_MAP = {
    # Server identity
    "port": "PublicPort",
    "public_ip": "PublicIP",
    "max_players": "ServerPlayerMaxNum",
    "coop_max_players": "CoopPlayerMaxNum",
    "server_name": "ServerName",
    "server_description": "ServerDescription",
    "server_password": "ServerPassword",
    "admin_password": "AdminPassword",
    "region": "Region",
    # Connectivity / Auth (RCONEnabled는 보안 정책상 항상 False라 설정 키가 없다)
    "rest_api_enabled": "RESTAPIEnabled",
    "rest_port": "RESTAPIPort",
    "use_auth": "bUseAuth",
    "allow_client_mod": "bAllowClientMod",
    "ban_list_url": "BanListURL",
    "show_player_list": "bShowPlayerList",
    "crossplay_platforms": "CrossplayPlatforms",
    # Difficulty / Game mode
    "difficulty": "Difficulty",
    "randomizer_type": "RandomizerType",
    "randomizer_seed": "RandomizerSeed",
    "randomizer_pal_level_random": "bIsRandomizerPalLevelRandom",
    "hardcore": "bHardcore",
    "pal_lost": "bPalLost",
    "character_recreate_in_hardcore": "bCharacterRecreateInHardcore",
    "is_pvp": "bIsPvP",
    "is_multiplay": "bIsMultiplay",
    "death_penalty": "DeathPenalty",
    "enable_player_to_player_damage": "bEnablePlayerToPlayerDamage",
    "enable_friendly_fire": "bEnableFriendlyFire",
    "enable_invader_enemy": "bEnableInvaderEnemy",
    "enable_predator_boss_pal": "EnablePredatorBossPal",
    # Rates
    "day_time_speed_rate": "DayTimeSpeedRate",
    "night_time_speed_rate": "NightTimeSpeedRate",
    "exp_rate": "ExpRate",
    "pal_capture_rate": "PalCaptureRate",
    "pal_spawn_num_rate": "PalSpawnNumRate",
    "pal_damage_rate_attack": "PalDamageRateAttack",
    "pal_damage_rate_defense": "PalDamageRateDefense",
    "player_damage_rate_attack": "PlayerDamageRateAttack",
    "player_damage_rate_defense": "PlayerDamageRateDefense",
    "player_stomach_decrease_rate": "PlayerStomachDecreaceRate",
    "player_stamina_decrease_rate": "PlayerStaminaDecreaceRate",
    "player_auto_hp_regen_rate": "PlayerAutoHPRegeneRate",
    "player_auto_hp_regen_rate_in_sleep": "PlayerAutoHpRegeneRateInSleep",
    "pal_stomach_decrease_rate": "PalStomachDecreaceRate",
    "pal_stamina_decrease_rate": "PalStaminaDecreaceRate",
    "pal_auto_hp_regen_rate": "PalAutoHPRegeneRate",
    "pal_auto_hp_regen_rate_in_sleep": "PalAutoHpRegeneRateInSleep",
    "work_speed_rate": "WorkSpeedRate",
    "collection_drop_rate": "CollectionDropRate",
    "collection_object_hp_rate": "CollectionObjectHpRate",
    "collection_object_respawn_speed_rate": "CollectionObjectRespawnSpeedRate",
    "enemy_drop_item_rate": "EnemyDropItemRate",
    "item_weight_rate": "ItemWeightRate",
    "equipment_durability_damage_rate": "EquipmentDurabilityDamageRate",
    "item_corruption_multiplier": "ItemCorruptionMultiplier",
    # Building
    "build_object_hp_rate": "BuildObjectHpRate",
    "build_object_damage_rate": "BuildObjectDamageRate",
    "build_object_deterioration_damage_rate": "BuildObjectDeteriorationDamageRate",
    "build_area_limit": "bBuildAreaLimit",
    "max_building_limit_num": "MaxBuildingLimitNum",
    # Base camp
    "base_camp_max_num": "BaseCampMaxNum",
    "base_camp_worker_max_num": "BaseCampWorkerMaxNum",
    "base_camp_max_num_in_guild": "BaseCampMaxNumInGuild",
    # Items / Drops
    "drop_item_max_num": "DropItemMaxNum",
    "drop_item_max_num_unko": "DropItemMaxNum_UNKO",
    "drop_item_alive_max_hours": "DropItemAliveMaxHours",
    "active_unko": "bActiveUNKO",
    # Guild
    "guild_player_max_num": "GuildPlayerMaxNum",
    "auto_reset_guild_no_online_players": "bAutoResetGuildNoOnlinePlayers",
    "auto_reset_guild_time_no_online_players": "AutoResetGuildTimeNoOnlinePlayers",
    "guild_rejoin_cooldown_minutes": "GuildRejoinCooldownMinutes",
    "can_pickup_other_guild_death_penalty_drop": "bCanPickupOtherGuildDeathPenaltyDrop",
    "enable_defense_other_guild_player": "bEnableDefenseOtherGuildPlayer",
    "invisible_other_guild_base_camp_area_fx": "bInvisibleOtherGuildBaseCampAreaFX",
    # Travel / Spawn
    "enable_fast_travel": "bEnableFastTravel",
    "enable_fast_travel_only_base_camp": "bEnableFastTravelOnlyBaseCamp",
    "is_start_location_select_by_map": "bIsStartLocationSelectByMap",
    "exist_player_after_logout": "bExistPlayerAfterLogout",
    "enable_non_login_penalty": "bEnableNonLoginPenalty",
    # Pal / Egg
    "pal_egg_default_hatching_time": "PalEggDefaultHatchingTime",
    # Aim assist
    "enable_aim_assist_pad": "bEnableAimAssistPad",
    "enable_aim_assist_keyboard": "bEnableAimAssistKeyboard",
    # Auto-save / Backup
    "auto_save_span": "AutoSaveSpan",
    "is_use_backup_save_data": "bIsUseBackupSaveData",
    # Supply drop
    "supply_drop_span": "SupplyDropSpan",
    # Logging / Chat
    "log_format_type": "LogFormatType",
    "is_show_join_left_message": "bIsShowJoinLeftMessage",
    "chat_post_limit_per_minute": "ChatPostLimitPerMinute",
    # Network / Performance
    "server_replicate_pawn_cull_distance": "ServerReplicatePawnCullDistance",
    # Palbox
    "allow_global_palbox_export": "bAllowGlobalPalboxExport",
    "allow_global_palbox_import": "bAllowGlobalPalboxImport",
    # Technology
    "deny_technology_list": "DenyTechnologyList",
    # Respawn
    "block_respawn_time": "BlockRespawnTime",
    "respawn_penalty_duration_threshold": "RespawnPenaltyDurationThreshold",
    "respawn_penalty_time_scale": "RespawnPenaltyTimeScale",
    # PvP specifics
    "display_pvp_item_num_on_world_map_base_camp": "bDisplayPvPItemNumOnWorldMap_BaseCamp",
    "display_pvp_item_num_on_world_map_player": "bDisplayPvPItemNumOnWorldMap_Player",
    "additional_drop_item_when_player_killing_in_pvp_mode": "AdditionalDropItemWhenPlayerKillingInPvPMode",
    "additional_drop_item_num_when_player_killing_in_pvp_mode": "AdditionalDropItemNumWhenPlayerKillingInPvPMode",
    "additional_drop_item_when_player_killing_in_pvp_mode_enabled": "bAdditionalDropItemWhenPlayerKillingInPvPMode",
    # Enhance stat
    "allow_enhance_stat_health": "bAllowEnhanceStat_Health",
    "allow_enhance_stat_attack": "bAllowEnhanceStat_Attack",
    "allow_enhance_stat_stamina": "bAllowEnhanceStat_Stamina",
    "allow_enhance_stat_weight": "bAllowEnhanceStat_Weight",
    "allow_enhance_stat_work_speed": "bAllowEnhanceStat_WorkSpeed",
    "item_container_force_mark_dirty_interval": "ItemContainerForceMarkDirtyInterval",
    # 2026-07 업데이트로 추가된 옵션
    "physics_active_drop_item_max_num": "PhysicsActiveDropItemMaxNum",
    "player_data_pal_storage_update_check_tick_interval": "PlayerDataPalStorageUpdateCheckTickInterval",
    "monster_farm_action_speed_rate": "MonsterFarmActionSpeedRate",
    "auto_transfer_master_check_interval_seconds": "AutoTransferMasterCheckIntervalSeconds",
    "auto_transfer_master_threshold_days": "AutoTransferMasterThresholdDays",
    "max_guilds_per_frame": "MaxGuildsPerFrame",
    "enable_voice_chat": "bEnableVoiceChat",
    "voice_chat_max_volume_distance": "VoiceChatMaxVolumeDistance",
    "voice_chat_zero_volume_distance": "VoiceChatZeroVolumeDistance",
    "enable_building_player_uid_display": "bEnableBuildingPlayerUIdDisplay",
    "building_name_display_cache_ttl_seconds": "BuildingNameDisplayCacheTTLSeconds",
}

def _get_settings_ini_path(config):
    """Resolve path to PalWorldSettings.ini from config."""
    working_dir = config.get("working_dir")
    if not working_dir:
        exe = config.get("server_executable")
        if exe:
            working_dir = os.path.dirname(os.path.abspath(exe))
    if not working_dir:
        return None

    platform_dir = "WindowsServer" if sys.platform == "win32" else "LinuxServer"

    ini_path = os.path.join(working_dir, "Pal", "Saved", "Config", platform_dir, "PalWorldSettings.ini")
    return ini_path


# UE4 INI parse/write delegated to extensions.ue4_ini
class ExtensionMissing(Exception):
    def __init__(self, extension):
        super().__init__(extension)
        self.extension = extension


def _parse_option_settings(ini_path):
    if parse_option_settings is None:
        raise ExtensionMissing("ue4-ini")
    return parse_option_settings(ini_path)


def _write_option_settings(ini_path, props):
    if write_option_settings is None:
        raise ExtensionMissing("ue4-ini")
    return write_option_settings(ini_path, props)


# ╔═══════════════════════════════════════════════════════════╗
# ║            Error Detection (Palworld)                     ║
# ╚═══════════════════════════════════════════════════════════╝

PALWORLD_ERROR_PATTERNS = [
    {
        "code": "PORT_IN_USE",
        "patterns": [
            r"Address already in use",
            r"Failed.*bind.*port",
            r"Only one usage of each socket address",
        ],
        "severity": "critical",
    },
    {
        "code": "OUT_OF_MEMORY",
        "patterns": [
            r"OutOfMemory",
            r"Ran out of memory",
            r"Could not allocate memory",
        ],
        "severity": "critical",
    },
    {
        "code": "EXECUTABLE_CRASH",
        "patterns": [
            r"Unhandled Exception",
            r"Fatal error",
            r"Assertion failed",
            r"appError called",
        ],
        "severity": "critical",
    },
    {
        "code": "SAVE_CORRUPT",
        "patterns": [
            r"Failed to load.*SaveData",
            r"Corrupted save",
            r"Failed to read save file",
        ],
        "severity": "error",
    },
    {
        "code": "STEAM_AUTH_FAILED",
        "patterns": [
            r"LogOnline.*Warning.*Steam",
            r"SteamAPI.*failed",
            r"Steam must be running",
        ],
        "severity": "error",
    },
]


# ╔═══════════════════════════════════════════════════════════╗
# ║          Additional Lifecycle Functions                    ║
# ╚═══════════════════════════════════════════════════════════╝

def validate(config):
    """Validate prerequisites before starting Palworld server."""
    issues = []
    executable = config.get("server_executable") or config.get("server_jar")

    # 실행 파일 검사
    if not executable:
        issues.append({
            "code": "NO_EXECUTABLE",
            "severity": "critical",
            "message": i18n.t("validate.no_executable"),
            "solution": i18n.t("validate.no_executable_hint"),
        })
    elif not os.path.isfile(executable):
        issues.append({
            "code": "EXECUTABLE_NOT_FOUND",
            "severity": "critical",
            "message": i18n.t("validate.executable_not_found", path=executable),
            "solution": i18n.t("validate.executable_not_found_hint"),
        })

    # Working directory
    working_dir = config.get("working_dir")
    if not working_dir and executable:
        working_dir = os.path.dirname(os.path.abspath(executable))
    if working_dir and not os.path.isdir(working_dir):
        try:
            os.makedirs(working_dir, exist_ok=True)
        except OSError:
            issues.append({
                "code": "WORKING_DIR_ERROR",
                "severity": "critical",
                "message": i18n.t("validate.cannot_create_dir", path=working_dir),
                "solution": i18n.t("validate.cannot_create_dir_hint"),
            })

    # Port availability — 이 서버가 이미 켜져 있으면 그 포트는 자기 것이다
    port = config.get("port", 8211)
    if port and not config.get("running"):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(1)
                if s.connect_ex(("127.0.0.1", int(port))) == 0:
                    issues.append({
                        "code": "PORT_IN_USE",
                        "severity": "warning",
                        "message": i18n.t("validate.port_in_use", port=port),
                        "solution": i18n.t("validate.port_in_use_hint", port=port),
                    })
        except (OSError, ValueError):
            pass

    # Settings file check
    ini_path = _get_settings_ini_path(config)
    settings_exist = ini_path and os.path.isfile(ini_path)

    return {
        "success": len([i for i in issues if i["severity"] == "critical"]) == 0,
        "issues": issues,
        "settings_file_exists": settings_exist,
    }


# 목록 값 INI 키 → 항목 따옴표. 화면에서는 쉼표로 적고("PALBOX,RepairBench"),
# INI에는 괄호 목록으로 쓴다: DenyTechnologyList=("PALBOX","RepairBench"). 새 목록 키는 여기 한 줄만 추가
_LIST_KEYS = {
    "DenyTechnologyList": '"',
}


def _to_ini_list(value, quote):
    """"a, b" → ("a","b"). 이미 괄호 목록이면 그대로, 빈 값은 빈 값"""
    text = str(value).strip()
    if not text or text.startswith("("):
        return text
    items = [item.strip().strip('"') for item in text.split(",")]
    return "(" + ",".join(f"{quote}{item}{quote}" for item in items if item) + ")"


def _from_ini_list(value):
    """("a","b") → a,b"""
    text = _unquote(value).strip()
    if text.startswith("(") and text.endswith(")"):
        text = text[1:-1]
    return ",".join(item.strip().strip('"') for item in text.split(",") if item.strip().strip('"'))


def configure(config):
    """Apply settings to PalWorldSettings.ini."""
    settings = config.get("settings", {})
    if not settings:
        return {"success": False, "message": i18n.t("errors.no_settings")}

    ini_path = _get_settings_ini_path(config)
    if not ini_path:
        return {"success": False, "message": i18n.t("errors.no_server_dir")}

    # Read existing properties (or start fresh)
    props = _parse_option_settings(ini_path)

    updated_keys = []
    for key, value in settings.items():
        ini_key = _PALWORLD_KEY_MAP.get(key)
        if ini_key is None:
            # Not a recognized INI setting — skip (saba-chan internal fields)
            continue
        if isinstance(value, bool):
            value = "True" if value else "False"
        elif ini_key in _LIST_KEYS:
            value = _to_ini_list(value, _LIST_KEYS[ini_key])
        props[ini_key] = str(value)
        updated_keys.append(ini_key)

    # RCON은 보안 정책으로 막는다: 손으로 켠 INI라도 저장할 때 끈다
    props["RCONEnabled"] = "False"
    ok = _write_option_settings(ini_path, props)
    return {
        "success": ok,
        "message": i18n.t("messages.settings_updated") if ok else i18n.t("errors.settings_write_failed"),
        "updated_keys": updated_keys,
    }


def _unquote(value):
    """PalWorldSettings.ini는 문자열을 따옴표로 감싼다: 짝이 맞는 바깥 따옴표 한 쌍만 벗긴다"""
    val = str(value)
    if len(val) >= 2 and val[0] == val[-1] == '"':
        return val[1:-1]
    return val


def import_settings(config):
    """Read PalWorldSettings.ini and return settings in saba-chan key format.

    Used during migration to import existing server settings into saba-chan.
    Returns {"success": True, "settings": { saba_key: value, ... }}
    """
    result = read_properties(config)
    if not result.get("success"):
        return result

    ini_props = result.get("properties", {})
    if not ini_props:
        return {"success": True, "settings": {}, "message": i18n.t("messages.no_properties_found")}

    # Build reverse map: INI key → saba-chan key
    reverse_map = {v: k for k, v in _PALWORLD_KEY_MAP.items()}

    from daemon_rcon import coerce_setting, load_setting_types
    types = load_setting_types(os.path.dirname(os.path.abspath(__file__)))
    settings = {}
    for ini_key, raw_value in ini_props.items():
        saba_key = reverse_map.get(ini_key)
        if saba_key is None:
            continue

        if ini_key in _LIST_KEYS:
            settings[saba_key] = _from_ini_list(raw_value)
            continue
        settings[saba_key] = coerce_setting(_unquote(raw_value), types.get(saba_key))

    return {"success": True, "settings": settings}


def read_properties(config):
    """Read current PalWorldSettings.ini."""
    ini_path = _get_settings_ini_path(config)
    if not ini_path:
        return {"success": False, "message": i18n.t("errors.no_server_dir")}

    if not os.path.isfile(ini_path):
        # Try DefaultPalWorldSettings.ini as reference
        default_path = None
        working_dir = config.get("working_dir")
        if not working_dir:
            exe = config.get("server_executable")
            if exe:
                working_dir = os.path.dirname(os.path.abspath(exe))
        if working_dir:
            default_path = os.path.join(working_dir, "DefaultPalWorldSettings.ini")

        if default_path and os.path.isfile(default_path):
            props = _parse_option_settings(default_path)
            return {
                "success": True,
                "exists": False,
                "properties": props,
                "message": i18n.t("messages.using_defaults"),
            }
        return {
            "success": True,
            "exists": False,
            "properties": {},
            "message": i18n.t("errors.settings_not_found"),
        }

    props = _parse_option_settings(ini_path)
    return {"success": True, "exists": True, "properties": props}


def reset_properties(config):
    """Reset PalWorldSettings.ini to factory defaults (preserves world data)."""
    ini_path = _get_settings_ini_path(config)
    if not ini_path:
        return {"success": False, "message": i18n.t("reset.no_path")}

    if not os.path.isfile(ini_path):
        return {"success": False, "message": i18n.t("reset.not_found")}

    ok = _write_option_settings(ini_path, dict(DEFAULT_PALWORLD_SETTINGS))
    return {
        "success": ok,
        "message": i18n.t("reset.settings_success") if ok else i18n.t("reset.settings_failed"),
    }


def reset_server(config):
    """Reset Palworld server settings to defaults.

    Unlike Minecraft, Palworld reset only restores settings to defaults.
    World / save data is NOT deleted.
    """
    return reset_properties(config)



def diagnose_log(config):
    """Diagnose errors from provided log lines or server log files."""
    import re as _re

    log_lines = config.get("log_lines", [])
    working_dir = config.get("working_dir", "")

    if isinstance(log_lines, str):
        log_lines = log_lines.splitlines()

    # If no lines provided, try to read from Palworld log file
    if not log_lines and working_dir:
        log_path = os.path.join(working_dir, "Pal", "Saved", "Logs", "Pal.log")
        if os.path.isfile(log_path):
            try:
                with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                    log_lines = f.readlines()[-500:]
            except OSError:
                pass

    issues = []
    seen_codes = set()
    for line in log_lines:
        for pdef in PALWORLD_ERROR_PATTERNS:
            if pdef["code"] in seen_codes:
                continue
            for regex in pdef["patterns"]:
                if _re.search(regex, line, _re.IGNORECASE):
                    code = pdef["code"]
                    seen_codes.add(code)
                    issues.append({
                        "code": code,
                        "severity": pdef["severity"],
                        "matched_line": line.strip()[:200],
                        "message": i18n.t(f"validate.log_{code.lower()}"),
                        "solution": i18n.t(f"validate.log_{code.lower()}_hint"),
                    })
                    break

    return {
        "success": True,
        "issues": issues,
        "lines_analyzed": len(log_lines),
    }


def list_versions(config):
    """Palworld server versions are managed via Steam/SteamCMD, not a download API."""
    return {
        "success": True,
        "versions": [],
        "message": i18n.t("messages.versions_via_steam"),
        "install_method": "steamcmd",
        "steam_app_id": "2394010",
    }


def get_version_details(config):
    """Palworld does not expose a public version API."""
    return {
        "success": True,
        "message": i18n.t("messages.versions_managed_via_steam"),
        "install_method": "steamcmd",
        "steam_app_id": "2394010",
    }


def install_server(config):
    """Install Palworld dedicated server via SteamCMD extension.

    Uses the saba-chan SteamCMD extension for portable, automatic
    download.  Falls back to manual instructions when the extension
    is unavailable.
    """
    install_dir = config.get("install_dir", "")
    if not install_dir:
        working_dir = config.get("working_dir", "")
        if working_dir:
            install_dir = os.path.join(working_dir, "server")
        else:
            return {"success": False, "message": i18n.t("errors.no_install_dir")}

    os.makedirs(install_dir, exist_ok=True)

    # ── SteamCMD extension 사용 시도 ──
    # 먼저 익스텐션 활성화 상태 확인
    enabled_exts = os.environ.get("SABA_ENABLED_EXTENSIONS", "").split(",")
    if "steamcmd" not in enabled_exts:
        return {
            "success": False,
            "message": i18n.t("messages.extension_not_enabled"),
            "error_code": "extension_required",
            "missing_extensions": ["steamcmd"],
        }

    try:
        from extensions.steamcmd import SteamCMD

        steam = SteamCMD()
        steam.ensure_available()

        result = steam.install(
            app_id=2394010,
            install_dir=install_dir,
            anonymous=True,
        )

        if result.get("success"):
            exe_name = "PalServer.exe" if sys.platform == "win32" else "PalServer.sh"
            exe_path = os.path.join(install_dir, exe_name)
            result["install_path"] = install_dir
            result["executable_path"] = exe_path if os.path.exists(exe_path) else ""

            # ── 프로비저닝 마지막 단계: 기본 설정으로 PalWorldSettings.ini 생성 ──
            # 서버를 한 번도 실행하지 않으면 INI가 비어있어 REST API가 활성화되지 않음.
            # 기본 값 + RESTAPIEnabled=True, RCONEnabled=False 로 미리 기록한다.
            try:
                if sys.platform == "win32":
                    platform_dir = "WindowsServer"
                else:
                    platform_dir = "LinuxServer"
                ini_dir = os.path.join(
                    install_dir, "Pal", "Saved", "Config", platform_dir
                )
                os.makedirs(ini_dir, exist_ok=True)
                ini_path = os.path.join(ini_dir, "PalWorldSettings.ini")

                # 업데이트도 install_server를 다시 부른다: 사용자가 채운 INI는 그대로 둔다
                if os.path.isfile(ini_path) and _parse_option_settings(ini_path):
                    print("[Palworld] Provisioning: keeping existing PalWorldSettings.ini", file=sys.stderr)
                else:
                    defaults = dict(DEFAULT_PALWORLD_SETTINGS)
                    defaults["RESTAPIEnabled"] = "True"
                    defaults["RCONEnabled"] = "False"
                    _write_option_settings(ini_path, defaults)
                    print(
                        f"[Palworld] Provisioning: wrote default PalWorldSettings.ini with REST enabled",
                        file=sys.stderr,
                    )
            except Exception as e:
                print(
                    f"[Palworld] Provisioning: failed to write default INI: {e}",
                    file=sys.stderr,
                )

        return result

    except ImportError:
        print("[Palworld] SteamCMD extension not available, returning instructions", file=sys.stderr)
    except Exception as e:
        print(f"[Palworld] SteamCMD install failed: {e}", file=sys.stderr)
        return {"success": False, "message": i18n.t("errors.install_failed", error=str(e))}

    # ── Fallback: 수동 설치 안내 ──
    return {
        "success": False,
        "message": i18n.t("messages.install_via_steamcmd"),
        "install_method": "steamcmd",
        "steam_app_id": "2394010",
        "instructions": [
            i18n.t("messages.install_step_download"),
            i18n.t("messages.install_step_run",
                   command='steamcmd +force_install_dir "{}" +login anonymous +app_update 2394010 validate +quit'.format(
                       install_dir or "<install_directory>")),
            i18n.t("messages.install_step_set_path"),
        ],
    }


def command(config):
    """서버 명령을 데몬의 REST 경로로 실행한다"""
    try:
        # 디버그: 전달받은 config 출력
        print(f"[Palworld] Received config keys: {list(config.keys())}", file=sys.stderr)
        
        command_text = config.get("command")
        args = config.get("args", {})
        instance_id = config.get("instance_id")
        print(f"[Palworld] command={command_text}, instance_id={instance_id}", file=sys.stderr)
        
        if not command_text:
            return {"success": False, "message": i18n.t("errors.no_command")}
        
        if not instance_id:
            return {"success": False, "message": i18n.t("errors.no_instance_id")}
        
        print(f"[Palworld] Executing command '{command_text}' with args: {args}", file=sys.stderr)
        
        # Normalize command for branching
        command_lower = command_text.lower()
        
        # 플레이어 ID 변환 (kick, ban, unban 명령어의 경우)
        if command_lower in ["kick", "ban", "unban"]:
            player_input = args.get("userid") or args.get("player_id") or args.get("steam_id") or args.get("name") or args.get("player")
            if player_input:
                success, result = resolve_player_id(instance_id, player_input, config)
                if success:
                    args = dict(args)  # 원본 수정 방지
                    args["userid"] = result
                elif command_lower != "unban":
                    # unban은 오프라인일 수 있으므로 실패해도 진행
                    return {"success": False, "message": result}
                else:
                    print(f"[Palworld] Player lookup failed for unban, using original: {player_input}", file=sys.stderr)
        
        return execute_command_via_rest(instance_id, command_lower, args, config)
    
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"Error details: {error_details}", file=sys.stderr)
        return {
            "success": False,
            "message": i18n.t("errors.command_failed", error=str(e))
        }


def execute_command_via_rest(instance_id, command_lower, args, config):
    """Execute command via REST API - 직접 Palworld 서버에 요청 (Daemon 데드락 방지)"""
    endpoint = None
    body = None
    method = "POST"
    
    if command_lower == "announce":
        message = args.get("message", "")
        if not message:
            return {"success": False, "message": i18n.t("errors.message_required")}
        endpoint = "/v1/api/announce"
        body = {"message": message}
    
    elif command_lower == "kick":
        userid = args.get("userid")
        if not userid:
            return {"success": False, "message": i18n.t("errors.userid_required")}
        message = args.get("message", "Kicked from server")
        endpoint = "/v1/api/kick"
        body = {"userid": userid, "message": message}
    
    elif command_lower == "ban":
        userid = args.get("userid")
        if not userid:
            return {"success": False, "message": i18n.t("errors.userid_required")}
        message = args.get("message", "Banned from server")
        endpoint = "/v1/api/ban"
        body = {"userid": userid, "message": message}
    
    elif command_lower == "unban":
        userid = args.get("userid")
        if not userid:
            return {"success": False, "message": i18n.t("errors.userid_required")}
        endpoint = "/v1/api/unban"
        body = {"userid": userid}
    
    elif command_lower == "info":
        endpoint = "/v1/api/info"
        method = "GET"
    
    elif command_lower == "players":
        endpoint = "/v1/api/players"
        method = "GET"
    
    elif command_lower == "metrics":
        endpoint = "/v1/api/metrics"
        method = "GET"
    
    elif command_lower == "settings":
        endpoint = "/v1/api/settings"
        method = "GET"
    
    elif command_lower == "save":
        endpoint = "/v1/api/save"
        body = {}
    
    elif command_lower == "shutdown":
        waittime = int(args.get("waittime", args.get("seconds", 30)))
        message = args.get("message", "Server shutting down")
        endpoint = "/v1/api/shutdown"
        body = {"waittime": waittime, "message": message}
    
    else:
        return {"success": False, "message": i18n.t("errors.unknown_command", command=command_lower)}
    
    # 직접 Palworld 서버에 요청 (Daemon을 거치지 않음 - 데드락 방지)
    return execute_rest_direct(endpoint, body, method, command_lower, config)


def execute_rest_direct(endpoint, body, method, command_text, config):
    """직접 Palworld 서버에 REST API 요청 (Daemon을 거치지 않음)"""
    rest_host = config.get("rest_host", "127.0.0.1")
    rest_port = config.get("rest_port", 8212)
    rest_username = config.get("rest_username") or "admin"
    rest_password = _read_admin_password_from_ini(config) or ""
    
    url = f"http://{rest_host}:{rest_port}{endpoint}"
    print(f"[Palworld] Direct REST request: {method} {url}", file=sys.stderr)
    if body:
        print(f"[Palworld] Request body: {body}", file=sys.stderr)
    
    try:
        if method == "GET":
            req = urllib.request.Request(url, method='GET')
        else:
            data = json.dumps(body).encode('utf-8') if body else None
            req = urllib.request.Request(url, data=data, method=method)
            req.add_header('Content-Type', 'application/json')
        
        # Basic Auth 설정
        if rest_username and rest_password:
            credentials = f"{rest_username}:{rest_password}"
            encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
            req.add_header('Authorization', f'Basic {encoded_credentials}')
        
        with _urlopen(req, timeout=10) as response:
            response_text = response.read().decode('utf-8')
            status_code = response.getcode()
            
            print(f"[Palworld] Response status: {status_code}", file=sys.stderr)
            print(f"[Palworld] Response: {response_text[:200]}", file=sys.stderr)
            
            # 응답 파싱 시도
            try:
                result = json.loads(response_text) if response_text else {}
            except json.JSONDecodeError:
                result = {"raw": response_text}
            
            return {
                "success": True,
                "message": i18n.t("messages.command_executed", command=command_text),
                "data": result,
                "protocol": "rest"
            }
    
    except urllib.error.HTTPError as e:
        error_body = e.read().decode('utf-8') if e.fp else str(e)
        print(f"[Palworld] HTTP Error {e.code}: {error_body}", file=sys.stderr)
        return {
            "success": False,
            "message": i18n.t("errors.http_error", code=e.code, body=error_body)
        }
    except urllib.error.URLError as e:
        print(f"[Palworld] Connection error: {e}", file=sys.stderr)
        return {
            "success": False,
            "message": i18n.t("errors.connection_failed", error=str(e))
        }
    except Exception as e:
        print(f"[Palworld] Error: {e}", file=sys.stderr)
        return {
            "success": False,
            "message": i18n.t("errors.command_failed", error=str(e))
        }


def execute_rest_via_daemon(instance_id, endpoint, body, command_text):
    """Execute REST API command via daemon"""
    payload = {
        "endpoint": endpoint,
        "method": "POST" if body else "GET",
        "body": body
    }
    
    try:
        result = daemon_post(f"/api/instance/{instance_id}/rest", payload, timeout=5)
        print(f"[Palworld] Daemon REST response: {result}", file=sys.stderr)
        
        return {
            "success": result.get("success", True),
            "message": i18n.t("messages.command_executed", command=command_text),
            "data": result.get("data"),
            "protocol": "rest"
        }
    
    except ConnectionError as e:
        print(f"[Palworld] Daemon connection error: {e}", file=sys.stderr)
        return {
            "success": False,
            "message": i18n.t("errors.daemon_connection_error", error=str(e))
        }
    except json.JSONDecodeError as e:
        print(f"[Palworld] Invalid JSON response from daemon: {e}", file=sys.stderr)
        return {
            "success": False,
            "message": i18n.t("errors.invalid_json_response", error=str(e))
        }
    except Exception as e:
        print(f"[Palworld] Daemon error: {e}", file=sys.stderr)
        return {
            "success": False,
            "message": i18n.t("errors.daemon_error", error=str(e))
        }


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(json.dumps({"success": False, "message": "Usage: lifecycle.py <function>"}))
        sys.exit(1)
    
    function_name = sys.argv[1]
    print(f"[Palworld] function: {function_name}", file=sys.stderr)
    
    # Read config JSON from stdin (avoids command-line length limits
    # and prevents sensitive data from appearing in process listings)
    try:
        config_str = sys.stdin.read()
        config = json.loads(config_str) if config_str.strip() else {}
        print(f"[Palworld] Parsed config keys: {list(config.keys())}", file=sys.stderr)
    except json.JSONDecodeError as e:
        print(json.dumps({"success": False, "message": f"Invalid JSON config on stdin: {str(e)}"}))
        print(f"[Palworld] JSON parse error: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Function dispatch table (matches daemon's expected interface)
    FUNCTIONS = {
        "stop": stop,
        "status": status,
        "command": command,
        "get_launch_command": get_launch_command,
        "get_credentials": get_credentials,
        "validate": validate,
        "configure": configure,
        "read_properties": read_properties,
        "import_settings": import_settings,
        "reset_properties": reset_properties,
        "reset_server": reset_server,
        "diagnose_log": diagnose_log,
        "list_versions": list_versions,
        "get_version_details": get_version_details,
        "install_server": install_server,
        "player_count": player_count,
    }
    
    fn = FUNCTIONS.get(function_name)
    if fn:
        try:
            result = fn(config)
        except ExtensionMissing as e:
            result = {
                "success": False,
                "message": i18n.t("messages.extension_missing", extension=e.extension),
                "error_code": "extension_required",
                "missing_extensions": [e.extension],
            }
    else:
        result = {"success": False, "message": f"Unknown function: {function_name}"}
    
    # Output JSON only
    print(json.dumps(result))
