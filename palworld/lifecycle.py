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
import urllib.request
import urllib.error
import urllib.parse
from i18n import I18n

# PYTHONPATH is injected by the Rust plugin runner; fallback for direct execution:
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

# RCON communication delegates to the Rust daemon's HTTP API
from daemon_rcon import DaemonRconClient as RconClient
# UE4 INI parser (shared extension)
from extensions.ue4_ini import parse_option_settings, write_option_settings

# Initialize i18n
MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
i18n = I18n(MODULE_DIR)

# Daemon API endpoint (localhost by default)
DAEMON_API_URL = os.environ.get('DAEMON_API_URL', 'http://127.0.0.1:57474')

class PalworldRconClient:
    """Palworld RCON client — delegates to Rust daemon's RCON API."""
    
    def __init__(self, host='127.0.0.1', port=25575, password='', instance_id=None):
        self._instance_id = instance_id
        self._client = RconClient(instance_id or '', host, int(port), password)
    
    def connect(self):
        """Connect to RCON server"""
        print(f"[RCON] {i18n.t('rcon.connecting', host=self._client.host, port=self._client.port)}", file=sys.stderr)
        result = self._client.connect()
        if result:
            print(f"[RCON] {i18n.t('rcon.authenticated')}", file=sys.stderr)
        else:
            print(f"[RCON] {i18n.t('rcon.connection_failed', error='see above')}", file=sys.stderr)
        return result
    
    def disconnect(self):
        """Disconnect from RCON server"""
        self._client.disconnect()
    
    def send_command(self, command):
        """Send a command to the server"""
        try:
            if not self._client.socket:
                if not self.connect():
                    return None
            print(f"[RCON] {i18n.t('rcon.sending_command', command=command)}", file=sys.stderr)
            response = self._client.command(command)
            print(f"[RCON] {i18n.t('rcon.response_received', response=response[:100] if response else 'None')}", file=sys.stderr)
            return response
        except Exception as e:
            print(f"[RCON] {i18n.t('rcon.command_failed', error=str(e))}", file=sys.stderr)
            return None


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
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
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
        rest_username = config.get("rest_username", "")
        rest_password = config.get("rest_password", "")
        
        # 직접 Palworld 서버에 REST 요청 (Daemon을 거치지 않음 - 데드락 방지)
        palworld_url = f"http://{rest_host}:{rest_port}/v1/api/players"
        print(f"[Palworld] Fetching players from: {palworld_url}", file=sys.stderr)
        
        req = urllib.request.Request(palworld_url, method='GET')
        
        # Basic Auth 설정
        if rest_username and rest_password:
            credentials = f"{rest_username}:{rest_password}"
            encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
            req.add_header('Authorization', f'Basic {encoded_credentials}')
        
        with urllib.request.urlopen(req, timeout=5) as response:
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
                return (False, f"Multiple players match '{player_input}': {', '.join(names)}. Please be more specific.")
            
            return (False, f"Player '{player_input}' not found online. Please check the name or use SteamID directly.")
    
    except urllib.error.URLError as e:
        print(f"[Palworld] Failed to fetch player list: {e}", file=sys.stderr)
        return (False, f"Failed to connect to server: {str(e)}")
    except Exception as e:
        print(f"[Palworld] Error resolving player: {e}", file=sys.stderr)
        return (False, f"Error resolving player: {str(e)}")


def start(config):
    """Start Palworld server"""
    try:
        executable = config.get("server_executable")
        if not executable:
            return {
                "success": False,
                "message": i18n.t("errors.no_executable", defaultValue="server_executable not specified in instance configuration. Please add the path to PalServer.exe")
            }
        
        # Check if executable exists
        if not os.path.exists(executable):
            return {
                "success": False,
                "message": i18n.t("errors.executable_not_found", path=executable, defaultValue=f"Executable not found: {executable}. Please check the path in instance settings.")
            }
        
        port = config.get("port", 8211)
        working_dir = config.get("working_dir")
        
        # Use working directory if specified, otherwise use executable's directory
        if not working_dir:
            working_dir = os.path.dirname(executable)
        
        # Construct command
        cmd = [
            executable,
            f"--port={port}"
        ]
        
        # Log for debugging (to stderr)
        print(i18n.t('messages.starting_server', command=' '.join(cmd)), file=sys.stderr)
        print(i18n.t('messages.working_directory', path=working_dir), file=sys.stderr)
        
        # Enforce REST-only policy before launch
        _enforce_rest_policy(working_dir)
        
        # Start process (detached, cross-platform)
        if sys.platform == 'win32':
            creationflags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS
            proc = subprocess.Popen(
                cmd,
                cwd=working_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                creationflags=creationflags
            )
        else:
            # Unix/Linux/macOS: Use start_new_session for detached process
            proc = subprocess.Popen(
                cmd,
                cwd=working_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True
            )
        
        return {
            "success": True,
            "pid": proc.pid,
            "message": i18n.t('messages.server_starting', pid=proc.pid)
        }
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"Error details: {error_details}", file=sys.stderr)
        return {
            "success": False,
            "message": i18n.t('errors.failed_to_start', error=str(e))
        }


def _enforce_rest_policy(working_dir):
    """
    Enforce REST-only policy in PalWorldSettings.ini before server launch.

    Palworld RCON is deprecated by the developer — saba-chan uses REST API exclusively.
    - RESTAPIEnabled = True  (always on)
    - RCONEnabled = False    (always off)
    - AdminPassword auto-generated if empty (required for REST API auth)
    - ServerPassword is NOT touched (user-facing join password)

    Returns dict with details of what was changed, or None if INI doesn't exist yet.
    """
    import secrets
    import string

    # Build a fake config to reuse _get_settings_ini_path
    fake_config = {"working_dir": working_dir}
    ini_path = _get_settings_ini_path(fake_config)
    if not ini_path or not os.path.isfile(ini_path):
        return None  # INI will be generated on first run

    props = _parse_option_settings(ini_path)
    changes = {}

    # Force REST API enabled
    if props.get("RESTAPIEnabled", "False") != "True":
        props["RESTAPIEnabled"] = "True"
        changes["RESTAPIEnabled"] = "True"
        print(i18n.t("messages.rest_api_forced_on", defaultValue="[Policy] REST API force-enabled"), file=sys.stderr)

    # Force RCON disabled
    if props.get("RCONEnabled", "False") != "False":
        props["RCONEnabled"] = "False"
        changes["RCONEnabled"] = "False"
        print(i18n.t("messages.rcon_forced_off", defaultValue="[Policy] RCON force-disabled (deprecated)"), file=sys.stderr)

    # Auto-generate AdminPassword if empty (needed for REST API authentication)
    current_admin_pw = props.get("AdminPassword", "").strip('"').strip("'")
    if not current_admin_pw:
        alphabet = string.ascii_letters + string.digits
        password = "".join(secrets.choice(alphabet) for _ in range(16))
        props["AdminPassword"] = password
        # Store password in INI but don't expose in return value
        changes["AdminPassword"] = "***auto-generated***"
        print(i18n.t("messages.admin_password_generated", defaultValue="[Policy] AdminPassword auto-generated for REST API auth"), file=sys.stderr)

    if changes:
        _write_option_settings(ini_path, props)
        return {"changed": True, "changes": changes}
    return {"changed": False}


def get_launch_command(config):
    """
    Build the command for the Rust daemon to spawn as a ManagedProcess.
    Returns { success, program, args, working_dir, env_vars }.
    """
    executable = config.get("server_executable")
    if not executable:
        return {
            "success": False,
            "message": i18n.t("errors.no_executable", defaultValue="server_executable not specified in instance configuration. Please add the path to PalServer.exe")
        }

    if not os.path.exists(executable):
        return {
            "success": False,
            "message": i18n.t("errors.executable_not_found", path=executable, defaultValue=f"Executable not found: {executable}. Please check the path in instance settings.")
        }

    port = config.get("port", 8211)
    working_dir = config.get("working_dir")
    if not working_dir:
        working_dir = os.path.dirname(os.path.abspath(executable))

    abs_working_dir = os.path.abspath(working_dir)

    # Enforce REST-only policy before launch
    _enforce_rest_policy(abs_working_dir)

    args = [f"--port={port}"]

    return {
        "success": True,
        "program": os.path.abspath(executable),
        "args": args,
        "working_dir": abs_working_dir,
        "env_vars": {},
    }


def stop(config):
    """Stop Palworld server"""
    try:
        executable = config.get("server_executable")
        if not executable:
            return {"success": False, "message": "server_executable not specified"}
        
        # Extract executable name from path (e.g., "D:\\path\\PalServer.exe" -> "PalServer.exe")
        exe_name = os.path.basename(executable)
        force = config.get("force", False)
        
        if sys.platform == 'win32':
            # Windows: Always use /F /T for forceful termination with tree kill
            # /F = force kill, /T = terminate child processes
            try:
                result = subprocess.run(
                    ['taskkill', '/F', '/T', '/IM', exe_name],
                    capture_output=True,
                    text=True,
                    check=False
                )
                
                # Log the output for debugging
                if result.stdout:
                    print(f"taskkill stdout: {result.stdout}", file=sys.stderr)
                if result.stderr:
                    print(f"taskkill stderr: {result.stderr}", file=sys.stderr)
                print(f"taskkill return code: {result.returncode}", file=sys.stderr)
                
                # taskkill returns 0 on success, but can also return 128 if process not found
                # Return success in either case since the goal is to stop the server
                return {
                    "success": True,
                    "message": i18n.t("messages.terminated", name=exe_name, defaultValue=f"Terminated {exe_name}")
                }
            except Exception as e:
                error_msg = i18n.t("errors.failed_to_stop_process", error=str(e), defaultValue=f"Failed to stop process: {str(e)}")
                print(error_msg, file=sys.stderr)
                return {
                    "success": False,
                    "message": error_msg
                }
        else:
            # Unix-like: Use pkill with force
            try:
                # Use SIGKILL (9) for immediate termination
                result = subprocess.run(
                    ['pkill', '-9', os.path.splitext(exe_name)[0]],
                    capture_output=True,
                    text=True,
                    check=False
                )
                
                if result.stdout:
                    print(f"pkill stdout: {result.stdout}", file=sys.stderr)
                if result.stderr:
                    print(f"pkill stderr: {result.stderr}", file=sys.stderr)
                print(f"pkill return code: {result.returncode}", file=sys.stderr)
                
                return {
                    "success": True,
                    "message": i18n.t("messages.terminated", name=exe_name, defaultValue=f"Terminated {exe_name}")
                }
            except Exception as e:
                error_msg = i18n.t("errors.failed_to_stop_process", error=str(e), defaultValue=f"Failed to stop process: {str(e)}")
                print(error_msg, file=sys.stderr)
                return {
                    "success": False,
                    "message": error_msg
                }
    except Exception as e:
        return {
            "success": False,
            "message": i18n.t("errors.failed_to_stop", error=str(e), defaultValue=f"Failed to stop: {str(e)}")
        }

def status(config):
    """Get server status"""
    try:
        executable = config.get("server_executable")
        if not executable:
            return {"success": True, "status": "stopped", "message": i18n.t("errors.no_executable_specified", defaultValue="No executable specified")}
        
        # Extract executable name from path (e.g., "D:\\path\\PalServer.exe" -> "PalServer.exe")
        exe_name = os.path.basename(executable)
        
        # Check if process is running by name
        if sys.platform == 'win32':
            try:
                # Use tasklist to check if process is running
                result = subprocess.run(
                    ['tasklist', '/FI', f'IMAGENAME eq {exe_name}'],
                    capture_output=True,
                    text=True,
                    check=False
                )
                if exe_name in result.stdout:
                    return {
                        "success": True,
                        "status": "running",
                        "message": i18n.t("messages.process_running", name=exe_name, defaultValue=f"{exe_name} is running")
                    }
                else:
                    return {
                        "success": True,
                        "status": "stopped",
                        "message": i18n.t("messages.process_not_running", name=exe_name, defaultValue=f"{exe_name} is not running")
                    }
            except Exception as e:
                return {
                    "success": True,
                    "status": "stopped",
                    "message": i18n.t("errors.status_unknown", error=str(e), defaultValue=f"Could not determine status: {str(e)}")
                }
        else:
            # Unix-like: Use pgrep
            try:
                result = subprocess.run(
                    ['pgrep', '-f', os.path.splitext(exe_name)[0]],
                    capture_output=True,
                    check=False
                )
                if result.returncode == 0:
                    pid = result.stdout.decode().strip().split('\n')[0]
                    return {
                        "success": True,
                        "status": "running",
                        "pid": int(pid) if pid else None,
                        "message": i18n.t("messages.process_running", name=exe_name, defaultValue=f"{exe_name} is running")
                    }
                else:
                    return {
                        "success": True,
                        "status": "stopped",
                        "message": i18n.t("messages.process_not_running", name=exe_name, defaultValue=f"{exe_name} is not running")
                    }
            except Exception as e:
                return {
                    "success": True,
                    "status": "stopped",
                    "message": i18n.t("errors.status_unknown", error=str(e), defaultValue=f"Could not determine status: {str(e)}")
                }
    except Exception as e:
        return {
            "success": False,
            "message": i18n.t("errors.failed_to_get_status", error=str(e), defaultValue=f"Failed to get status: {str(e)}")
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
    # Connectivity / Auth
    "rcon_enabled": "RCONEnabled",
    "rcon_port": "RCONPort",
    "rest_api_enabled": "RESTAPIEnabled",
    "rest_api_port": "RESTAPIPort",
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

    # 컨테이너 모드: 서버가 Linux 컨테이너에서 실행되므로 항상 LinuxServer 경로 사용
    ext_data = config.get("extension_data", {})
    use_container = ext_data.get("docker_enabled", False) if isinstance(ext_data, dict) else config.get("use_docker", False)
    if use_container:
        platform_dir = "LinuxServer"
    elif sys.platform == "win32":
        platform_dir = "WindowsServer"
    else:
        platform_dir = "LinuxServer"

    ini_path = os.path.join(working_dir, "Pal", "Saved", "Config", platform_dir, "PalWorldSettings.ini")
    return ini_path


# UE4 INI parse/write delegated to extensions.ue4_ini
_parse_option_settings = parse_option_settings
_write_option_settings = write_option_settings


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
    executable = config.get("server_executable")
    ext_data = config.get("extension_data", {})
    use_container = ext_data.get("docker_enabled", False) if isinstance(ext_data, dict) else config.get("use_docker", False)

    # 실행 파일 검사 (컨테이너 모드에서는 컨테이너 내부에서 실행하므로 검사 불필요)
    if not use_container:
        if not executable:
            issues.append({
                "code": "NO_EXECUTABLE",
                "severity": "critical",
                "message": i18n.t("validate.no_executable", defaultValue="Server executable path not specified."),
                "solution": i18n.t("validate.no_executable_hint", defaultValue="Set the path to PalServer.exe in instance settings."),
            })
        elif not os.path.isfile(executable):
            issues.append({
                "code": "EXECUTABLE_NOT_FOUND",
                "severity": "critical",
                "message": i18n.t("validate.executable_not_found", path=executable, defaultValue=f"Executable not found: {executable}"),
                "solution": i18n.t("validate.executable_not_found_hint", defaultValue="Check the executable path or reinstall the server."),
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
                "message": i18n.t("validate.cannot_create_dir", path=working_dir, defaultValue=f"Cannot create working directory: {working_dir}"),
                "solution": i18n.t("validate.cannot_create_dir_hint", defaultValue="Check folder permissions or choose a different path."),
            })

    # Port availability
    port = config.get("port", 8211)
    if port:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(1)
                if s.connect_ex(("127.0.0.1", int(port))) == 0:
                    issues.append({
                        "code": "PORT_IN_USE",
                        "severity": "warning",
                        "message": i18n.t("validate.port_in_use", port=port, defaultValue=f"Port {port} is already in use."),
                        "solution": i18n.t("validate.port_in_use_hint", port=port, defaultValue=f"Stop the other process using port {port} or change the port."),
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


def configure(config):
    """Apply settings to PalWorldSettings.ini."""
    settings = config.get("settings", {})
    if not settings:
        return {"success": False, "message": i18n.t("errors.no_settings", defaultValue="No settings provided.")}

    ini_path = _get_settings_ini_path(config)
    if not ini_path:
        return {"success": False, "message": i18n.t("errors.no_server_dir", defaultValue="Cannot determine server directory. Set working_dir or server_executable.")}

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
        props[ini_key] = str(value)
        updated_keys.append(ini_key)

    ok = _write_option_settings(ini_path, props)
    return {
        "success": ok,
        "message": i18n.t("messages.settings_updated", defaultValue="Settings updated successfully.") if ok else i18n.t("errors.settings_write_failed", defaultValue="Failed to write settings file."),
        "updated_keys": updated_keys,
    }


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
        return {"success": True, "settings": {}, "message": "No properties found."}

    # Build reverse map: INI key → saba-chan key
    reverse_map = {v: k for k, v in _PALWORLD_KEY_MAP.items()}

    settings = {}
    for ini_key, raw_value in ini_props.items():
        saba_key = reverse_map.get(ini_key)
        if saba_key is None:
            continue

        # Type coercion: booleans, numbers, strings
        val = str(raw_value).strip().strip('"')
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
    """Read current PalWorldSettings.ini."""
    ini_path = _get_settings_ini_path(config)
    if not ini_path:
        return {"success": False, "message": i18n.t("errors.no_server_dir", defaultValue="Cannot determine server directory. Set working_dir or server_executable.")}

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
                "message": i18n.t("messages.using_defaults", defaultValue="Using defaults from DefaultPalWorldSettings.ini (server not yet configured)."),
            }
        return {
            "success": True,
            "exists": False,
            "properties": {},
            "message": i18n.t("errors.settings_not_found", defaultValue="Settings file not found. Start the server once to generate it."),
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


def accept_eula(config):
    """Palworld does not require EULA acceptance."""
    return {
        "success": True,
        "message": i18n.t("messages.no_eula_needed", defaultValue="Palworld does not require separate EULA acceptance."),
    }


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
                    solutions = {
                        "PORT_IN_USE": "Stop any other process using the port or change the server port.",
                        "OUT_OF_MEMORY": "Increase system RAM or reduce server settings (max players, world size).",
                        "EXECUTABLE_CRASH": "Verify game files via Steam, check for mod conflicts, or update the server.",
                        "SAVE_CORRUPT": "Restore from a backup save. Check Pal/Saved/SaveGames/ for backups.",
                        "STEAM_AUTH_FAILED": "Ensure Steam is running and steamclient libraries are accessible.",
                    }
                    issues.append({
                        "code": code,
                        "severity": pdef["severity"],
                        "matched_line": line.strip()[:200],
                        "message": f"Detected issue: {code.replace('_', ' ').title()}",
                        "solution": solutions.get(code, "Check server logs for details."),
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
        "message": i18n.t("messages.versions_via_steam", defaultValue="Palworld server is distributed via Steam/SteamCMD (App ID 2394010). Use SteamCMD to install or update."),
        "install_method": "steamcmd",
        "steam_app_id": "2394010",
    }


def get_version_details(config):
    """Palworld does not expose a public version API."""
    return {
        "success": True,
        "message": i18n.t("messages.versions_managed_via_steam", defaultValue="Palworld server versions are managed through Steam. Use SteamCMD with app_update 2394010 to update."),
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
            return {"success": False, "message": "No install_dir specified"}

    os.makedirs(install_dir, exist_ok=True)

    # ── SteamCMD extension 사용 시도 ──
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

        return result

    except ImportError:
        print("[Palworld] SteamCMD extension not available, returning instructions", file=sys.stderr)
    except Exception as e:
        print(f"[Palworld] SteamCMD install failed: {e}", file=sys.stderr)
        return {"success": False, "message": f"SteamCMD install failed: {e}"}

    # ── Fallback: 수동 설치 안내 ──
    return {
        "success": False,
        "message": i18n.t("messages.install_via_steamcmd", defaultValue="Palworld dedicated server must be installed via SteamCMD."),
        "install_method": "steamcmd",
        "steam_app_id": "2394010",
        "instructions": [
            "1. Download SteamCMD from https://developer.valvesoftware.com/wiki/SteamCMD",
            "2. Run: steamcmd +force_install_dir \"{}\" +login anonymous +app_update 2394010 validate +quit".format(
                install_dir or "<install_directory>"
            ),
            "3. Set the executable path in saba-chan to PalServer.exe in the installed directory.",
        ],
    }


def command(config):
    """Execute server command via daemon REST/RCON API based on protocol_mode"""
    try:
        # 디버그: 전달받은 config 출력
        print(f"[Palworld] Received config keys: {list(config.keys())}", file=sys.stderr)
        
        command_text = config.get("command")
        args = config.get("args", {})
        instance_id = config.get("instance_id")
        protocol_mode = config.get("protocol_mode", "rest")  # "rest", "rcon", "auto"
        
        print(f"[Palworld] command={command_text}, instance_id={instance_id}, protocol_mode={protocol_mode}", file=sys.stderr)
        
        if not command_text:
            return {
                "success": False,
                "message": f"No command specified. Config keys: {list(config.keys())}"
            }
        
        if not instance_id:
            return {
                "success": False,
                "message": f"No instance_id specified. Config keys: {list(config.keys())}, config: {str(config)[:300]}"
            }
        
        print(f"[Palworld] Executing command '{command_text}' via protocol_mode='{protocol_mode}' with args: {args}", file=sys.stderr)
        
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
        
        # === RCON 모드 ===
        if protocol_mode == "rcon":
            return execute_command_via_rcon(instance_id, command_lower, args, config)
        
        # === REST 모드 (기본값) ===
        return execute_command_via_rest(instance_id, command_lower, args, config)
    
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"Error details: {error_details}", file=sys.stderr)
        return {
            "success": False,
            "message": f"Failed to execute command: {str(e)}"
        }


def execute_command_via_rest(instance_id, command_lower, args, config):
    """Execute command via REST API - 직접 Palworld 서버에 요청 (Daemon 데드락 방지)"""
    endpoint = None
    body = None
    method = "POST"
    
    if command_lower == "announce":
        message = args.get("message", "")
        if not message:
            return {"success": False, "message": "Message parameter required"}
        endpoint = "/v1/api/announce"
        body = {"message": message}
    
    elif command_lower == "kick":
        userid = args.get("userid")
        if not userid:
            return {"success": False, "message": "userid is required"}
        message = args.get("message", "Kicked from server")
        endpoint = "/v1/api/kick"
        body = {"userid": userid, "message": message}
    
    elif command_lower == "ban":
        userid = args.get("userid")
        if not userid:
            return {"success": False, "message": "userid is required"}
        message = args.get("message", "Banned from server")
        endpoint = "/v1/api/ban"
        body = {"userid": userid, "message": message}
    
    elif command_lower == "unban":
        userid = args.get("userid")
        if not userid:
            return {"success": False, "message": "userid is required"}
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
        return {"success": False, "message": f"Unknown REST command: {command_lower}"}
    
    # 직접 Palworld 서버에 요청 (Daemon을 거치지 않음 - 데드락 방지)
    return execute_rest_direct(endpoint, body, method, command_lower, config)


def execute_rest_direct(endpoint, body, method, command_text, config):
    """직접 Palworld 서버에 REST API 요청 (Daemon을 거치지 않음)"""
    rest_host = config.get("rest_host", "127.0.0.1")
    rest_port = config.get("rest_port", 8212)
    rest_username = config.get("rest_username", "")
    rest_password = config.get("rest_password", "")
    
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
        
        with urllib.request.urlopen(req, timeout=10) as response:
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
                "message": f"REST command '{command_text}' executed successfully",
                "data": result,
                "protocol": "rest"
            }
    
    except urllib.error.HTTPError as e:
        error_body = e.read().decode('utf-8') if e.fp else str(e)
        print(f"[Palworld] HTTP Error {e.code}: {error_body}", file=sys.stderr)
        return {
            "success": False,
            "message": f"HTTP Error {e.code}: {error_body}"
        }
    except urllib.error.URLError as e:
        print(f"[Palworld] Connection error: {e}", file=sys.stderr)
        return {
            "success": False,
            "message": f"Failed to connect to Palworld server: {str(e)}"
        }
    except Exception as e:
        print(f"[Palworld] Error: {e}", file=sys.stderr)
        return {
            "success": False,
            "message": f"Failed to execute REST command: {str(e)}"
        }


def execute_command_via_rcon(instance_id, command_lower, args, config):
    """Execute command via RCON"""
    
    # RCON 명령어 템플릿 매핑
    rcon_templates = {
        "announce": "Broadcast {message}",
        "info": "Info",
        "players": "ShowPlayers",
        "save": "Save",
        "shutdown": "Shutdown {waittime} {message}",
        "kick": "KickPlayer {userid}",
        "ban": "BanPlayer {userid}",
        "unban": "UnBanPlayer {userid}",
    }
    
    if command_lower not in rcon_templates:
        return {
            "success": False,
            "message": f"Command '{command_lower}' is not available via RCON. Try using REST mode."
        }
    
    # 템플릿에서 RCON 명령어 생성
    template = rcon_templates[command_lower]
    
    # 인자 치환
    rcon_cmd = template
    for key, value in args.items():
        rcon_cmd = rcon_cmd.replace("{" + key + "}", str(value) if value else "")
    
    # 남은 플레이스홀더 제거 및 공백 정리
    import re
    rcon_cmd = re.sub(r'\{[^}]+\}', '', rcon_cmd)
    rcon_cmd = ' '.join(rcon_cmd.split())  # 연속 공백 제거
    
    print(f"[Palworld] RCON command: {rcon_cmd}", file=sys.stderr)
    
    return execute_rcon_via_daemon(instance_id, rcon_cmd)


def execute_rest_via_daemon(instance_id, endpoint, body, command_text):
    """Execute REST API command via daemon"""
    api_url = f"{DAEMON_API_URL}/api/instance/{instance_id}/rest"
    
    payload = json.dumps({
        "endpoint": endpoint,
        "method": "POST" if body else "GET",
        "body": body
    }).encode('utf-8')
    
    try:
        req = urllib.request.Request(
            api_url,
            data=payload,
            headers={'Content-Type': 'application/json'},
            method='POST'
        )
        
        with urllib.request.urlopen(req, timeout=5) as response:
            result = json.loads(response.read().decode('utf-8'))
            print(f"[Palworld] Daemon REST response: {result}", file=sys.stderr)
            
            return {
                "success": result.get("success", True),
                "message": f"REST API command executed: {command_text}",
                "data": result.get("data"),
                "protocol": "rest"
            }
    
    except urllib.error.URLError as e:
        print(f"[Palworld] Daemon connection error: {e}", file=sys.stderr)
        return {
            "success": False,
            "message": f"Failed to connect to daemon: {str(e)}"
        }
    except json.JSONDecodeError as e:
        print(f"[Palworld] Invalid JSON response from daemon: {e}", file=sys.stderr)
        return {
            "success": False,
            "message": f"Invalid daemon response: {str(e)}"
        }
    except Exception as e:
        print(f"[Palworld] Daemon error: {e}", file=sys.stderr)
        return {
            "success": False,
            "message": f"Failed to execute via daemon: {str(e)}"
        }


def execute_rcon_via_daemon(instance_id, rcon_cmd):
    """Execute RCON command via daemon"""
    api_url = f"{DAEMON_API_URL}/api/instance/{instance_id}/rcon"
    
    payload = json.dumps({
        "command": rcon_cmd
    }).encode('utf-8')
    
    try:
        req = urllib.request.Request(
            api_url,
            data=payload,
            headers={'Content-Type': 'application/json'},
            method='POST'
        )
        
        with urllib.request.urlopen(req, timeout=10) as response:
            result = json.loads(response.read().decode('utf-8'))
            print(f"[Palworld] Daemon RCON response: {result}", file=sys.stderr)
            
            return {
                "success": result.get("success", True),
                "message": f"RCON command executed: {rcon_cmd}",
                "data": result.get("data"),
                "protocol": "rcon"
            }
    
    except urllib.error.URLError as e:
        print(f"[Palworld] Daemon RCON connection error: {e}", file=sys.stderr)
        return {
            "success": False,
            "message": f"Failed to connect to daemon for RCON: {str(e)}"
        }
    except json.JSONDecodeError as e:
        print(f"[Palworld] Invalid JSON response from daemon RCON: {e}", file=sys.stderr)
        return {
            "success": False,
            "message": f"Invalid daemon RCON response: {str(e)}"
        }
    except Exception as e:
        print(f"[Palworld] Daemon RCON error: {e}", file=sys.stderr)
        return {
            "success": False,
            "message": f"Failed to execute RCON via daemon: {str(e)}"
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
        "start": start,
        "stop": stop,
        "status": status,
        "command": command,
        "get_launch_command": get_launch_command,
        "validate": validate,
        "configure": configure,
        "read_properties": read_properties,
        "reset_properties": reset_properties,
        "reset_server": reset_server,
        "accept_eula": accept_eula,
        "diagnose_log": diagnose_log,
        "list_versions": list_versions,
        "get_version_details": get_version_details,
        "install_server": install_server,
    }
    
    fn = FUNCTIONS.get(function_name)
    if fn:
        result = fn(config)
    else:
        result = {"success": False, "message": f"Unknown function: {function_name}"}
    
    # Output JSON only
    print(json.dumps(result))
