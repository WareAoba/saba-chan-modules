#!/usr/bin/env python3
"""
Palworld server lifecycle management module.
Always outputs JSON to stdout, logs to stderr.
"""

import sys
import json
import os
import struct
import time
import random
from i18n import I18n

# PYTHONPATH is injected by the Rust plugin runner; fallback for direct execution:
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

from daemon_rcon import (PathOutside, daemon_rest, diagnose_lines, graceful_stop, is_process_alive, load_diagnose_rules,
                         safe_join, setting_file_keys, tail_lines)


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
    """인스턴스의 REST 요청을 데몬에 맡긴다."""

    def __init__(self, instance_id, timeout=5):
        self.instance_id = instance_id
        self.timeout = timeout

    def _request(self, method, path, payload=None):
        try:
            result = daemon_rest(self.instance_id, method, f"/v1/api{path}", body=payload, timeout=self.timeout)
        except ConnectionError as e:
            raise RuntimeError(str(e)) from e
        if not result.get("success", True):
            raise RuntimeError(result.get("message") or result.get("error") or i18n.t("errors.daemon_error", error="REST"))
        return (result.get("data") or {}).get("response")

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
        result = PalworldRestClient(instance_id).players()
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
    
    except PathOutside:
        raise
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
    except PathOutside:
        raise
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
    # 데몬은 시작할 때 "비밀번호 자동 생성"이 켜져 있으면 빈 rest_password만 채우고, 꺼져 있으면 비워 둔다
    # (ensure_passwords). 그래서 config에 없을 수 있다: 그때는 INI 값을 쓰거나 아래에서 만든다.
    current_admin_pw = props.get("AdminPassword", "").strip('"').strip("'")
    daemon_pw = (config or {}).get("rest_password") or (config or {}).get("admin_password", "")

    ini_admin_pw = current_admin_pw
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
        if not _write_option_settings(ini_path, props):
            # 저장하지 못했다: INI에 실제로 있는 비밀번호만 데몬에 넘긴다
            print("[Palworld] Failed to write REST policy to PalWorldSettings.ini", file=sys.stderr)
            return {"changed": False, "write_failed": True, "admin_password": ini_admin_pw}
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
    if rest_result and rest_result.get("write_failed"):
        # REST가 꺼진 채 뜨면 데몬이 상태 확인·종료를 못 한다: 띄우지 않고 이유를 알린다
        return {"success": False, "message": i18n.t("errors.rest_policy_write_failed")}

    args = [f"--port={port}"]

    # PalServer.exe는 실제 서버(Shipping-Cmd)를 띄우고 빠지는 런처다. 데몬(saba-runner)이
    # 실제 서버의 PID·종료 코드를 쥐도록 런처가 하는 그대로 실제 서버를 직접 띄운다.
    # 실제 서버 exe가 없으면(다른 배포 형태) 예전처럼 런처를 띄운다.
    program = os.path.abspath(executable)
    real_server = str(safe_join(os.path.dirname(program), "Pal", "Binaries", "Win64",
                                "PalServer-Win64-Shipping-Cmd.exe"))
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


def _rest_client(config):
    """데몬의 인스턴스 문맥이 있으면 REST 클라이언트를 만든다."""
    instance_id = config.get("instance_id")
    if not instance_id and not os.environ.get("SABA_HOST_DIR"):
        return None
    return PalworldRestClient(instance_id)


def player_count(config):
    """접속자 수 (module.toml [status] players). 데몬이 켜진 서버에 1분마다 부른다"""
    client = _rest_client(config)
    if client is None:
        return {"success": False, "message": i18n.t("errors.player_list_failed", error="AdminPassword")}
    try:
        metrics = client.metrics()
        result = {"success": True, "online": int(metrics["currentplayernum"]), "max": int(metrics.get("maxplayernum") or 0)}
    except (RuntimeError, KeyError, TypeError, ValueError) as e:
        return {"success": False, "message": i18n.t("errors.player_list_failed", error=str(e))}
    # 이름은 데몬이 목록을 물을 때(players_detail)만 읽는다. 1분 갱신은 수만 쓴다
    if not config.get("players_detail"):
        return result
    if result["online"] == 0:
        result["players"] = []
        return result
    # 이름은 공식 REST GET /players: name(게임 안 이름), 비면 accountName. IP·ID는 내지 않는다
    try:
        names = (p.get("name") or p.get("accountName") for p in client.players()["players"])
        result["players"] = [{"name": n} for n in names if n]
    except (RuntimeError, KeyError, TypeError, AttributeError) as e:
        # 이름을 못 읽어도 수는 맞다: 수만 답한다
        print(f"[Palworld] REST players failed: {e}", file=sys.stderr)
    return result


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


def stop(config):
    """Stop Palworld server.

    Palworld는 콘솔(stdin) 명령을 받지 않으므로 REST로 저장·종료를 요청한다. 기다림·강제 종료·managed·
    요청 불가(`graceful_stop_unavailable`)는 공용 절차(graceful_stop)가 맡는다. 데몬이 준 PID(와 자식)만 다룬다.
    """
    return graceful_stop(config, lambda: _rest_shutdown(config), i18n, module_dir=MODULE_DIR)


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
    except PathOutside:
        raise
    except Exception as e:
        return {"success": False, "message": str(e)}


def status(config):
    """서버 상태. 데몬이 추적하는 PID로만 판단한다: 실행 파일 이름으로 찾으면 같은 게임의 다른 인스턴스와 섞인다"""
    try:
        exe_name = os.path.basename(config.get("server_executable") or "PalServer.exe")
        pid = config.get("pid")
        running = bool(pid) and is_process_alive(pid)
        key = "messages.process_running" if running else "messages.process_not_running"
        return {"success": True, "status": "running" if running else "stopped", "message": i18n.t(key, name=exe_name)}
    except PathOutside:
        raise
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

    ini_path = str(safe_join(working_dir, "Pal", "Saved", "Config", platform_dir, "PalWorldSettings.ini"))
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


def _apply_settings(props, settings):
    """saba-chan 설정 키를 INI 값으로 바꿔 `props`에 넣고, 바꾼 INI 키 목록을 돌려준다.
    module.toml file_key가 없는 키(데몬 내부 필드 등)는 건너뛴다"""
    keys = setting_file_keys(MODULE_DIR)
    updated_keys = []
    for key, value in (settings or {}).items():
        ini_key = keys.get(key)
        if ini_key is None or value is None:
            continue
        if isinstance(value, bool):
            value = "True" if value else "False"
        elif ini_key in _LIST_KEYS:
            value = _to_ini_list(value, _LIST_KEYS[ini_key])
        props[ini_key] = str(value)
        updated_keys.append(ini_key)
    return updated_keys


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
    updated_keys = _apply_settings(props, settings)

    # RCON은 보안 정책으로 막는다: 손으로 켠 INI라도 저장할 때 끈다
    props["RCONEnabled"] = "False"
    try:
        ok = _write_option_settings(ini_path, props)
    except OSError:
        ok = False
    if not ok:
        return {"success": False, "error_code": "settings_write_failed",
                "message": i18n.t("errors.settings_write_failed")}
    return {
        "success": True,
        "message": i18n.t("messages.settings_updated"),
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

    # INI key → saba-chan key (module.toml file_key)
    reverse_map = {v: k for k, v in setting_file_keys(MODULE_DIR).items()}

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


def config_files(config):
    """사바쨩이 고칠 수 있는 게임 설정 파일 (가져온 서버를 해제할 때 데몬이 가져오기 전 내용으로 되돌린다)"""
    ini_path = _get_settings_ini_path(config)
    if not ini_path:
        return {"success": False, "message": i18n.t("errors.no_server_dir")}
    return {"success": True, "files": [ini_path]}


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
            default_path = str(safe_join(working_dir, "DefaultPalWorldSettings.ini"))

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


def _entries(path):
    """폴더 항목. 없는 폴더는 빈 목록, 읽지 못하는 폴더는 OSError (빈 세이브로 보고 성공하면 백업이 비어도 모른다)"""
    try:
        return sorted(os.listdir(path))
    except FileNotFoundError:
        return []


def backup_paths(config):
    """백업할 세이브 (데몬 `[backup] hook`): `Pal/Saved/SaveGames/<번호>/<월드>/` 안의 항목 전부.
    다만 게임이 월드 폴더 안에 쌓는 자동 백업 `backup/`(bIsUseBackupSaveData)은 뺀다.
    설정 폴더(`Pal/Saved/Config`)는 module.toml `paths`에 있다."""
    saves = safe_join(config.get("working_dir") or "", "Pal", "Saved", "SaveGames")
    try:
        return {"success": True, "paths": _save_entries(saves)}
    except OSError as e:
        return {"success": False, "message": i18n.t("errors.backup_paths_failed", error=str(e))}


def _snapshot_time(stamp, path):
    """스냅샷 시각: 게임이 폴더 이름에 적은 시각(YYYY.MM.DD-HH.MM.SS, 이 PC 시간대). 읽지 못하면 폴더 수정 시각.
    수정 시각은 복사·압축 해제로 바뀌어 순서가 뒤집힐 수 있다"""
    import time
    try:
        return int(time.mktime(time.strptime(stamp, "%Y.%m.%d-%H.%M.%S")))
    except (ValueError, OverflowError):
        return int(os.path.getmtime(path))


def backup_snapshots(config):
    """게임이 스스로 만든 월드 자동 백업 (데몬 `[backup] snapshots`): `SaveGames/<번호>/<월드>/backup/world/<시각>/`.
    새것부터. 스냅샷 안의 항목(Level.sav, Players/ …)은 월드 폴더의 같은 이름 자리로 돌아간다"""
    saves = safe_join(config.get("working_dir") or "", "Pal", "Saved", "SaveGames")
    try:
        snapshots = []
        for slot in _entries(saves):
            slot_path = safe_join(saves, slot)
            if not os.path.isdir(slot_path):
                continue
            for world in _entries(slot_path):
                world_path = safe_join(slot_path, world)
                kept = safe_join(world_path, "backup", "world")
                if not os.path.isdir(kept):
                    continue
                for stamp in _entries(kept):
                    snap = safe_join(kept, stamp)
                    if not os.path.isdir(snap):
                        continue
                    snapshots.append({
                        "id": f"{slot}/{world}/{stamp}",
                        "created": _snapshot_time(stamp, snap),
                        "paths": [{"from": str(safe_join(snap, name)), "to": str(safe_join(world_path, name))}
                                  for name in _entries(snap)],
                    })
        # 게임이 적은 시각 순 (CLI·GUI의 "가장 최근"과 데몬의 기본 선택이 같은 것을 고른다)
        snapshots.sort(key=lambda s: s["created"], reverse=True)
        return {"success": True, "snapshots": snapshots}
    except OSError as e:
        return {"success": False, "message": i18n.t("errors.backup_paths_failed", error=str(e))}


def _save_entries(saves):
    paths = []
    for slot in _entries(saves):
        slot_path = safe_join(saves, slot)
        if not os.path.isdir(slot_path):
            paths.append(str(slot_path))
            continue
        for world in _entries(slot_path):
            world_path = safe_join(slot_path, world)
            if not os.path.isdir(world_path):
                paths.append(str(world_path))
                continue
            paths.extend(str(safe_join(world_path, name)) for name in _entries(world_path)
                         if name.lower() != "backup")
    return paths



def diagnose_log(config):
    """Diagnose errors from provided log lines or server log files. 규칙은 module.toml [[diagnose]]"""
    log_lines = config.get("log_lines", [])
    working_dir = config.get("working_dir", "")

    if isinstance(log_lines, str):
        log_lines = log_lines.splitlines()

    # If no lines provided, try to read from Palworld log file
    if not log_lines and working_dir:
        log_lines = tail_lines(safe_join(working_dir, "Pal", "Saved", "Logs", "Pal.log"))

    issues = diagnose_lines(log_lines, load_diagnose_rules(MODULE_DIR), i18n,
                            message="validate.log_{code}", solution="validate.log_{code}_hint")

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
            install_dir = str(safe_join(working_dir, "server"))
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
            exe_path = str(safe_join(install_dir, exe_name))
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
                ini_dir = safe_join(
                    install_dir, "Pal", "Saved", "Config", platform_dir
                )
                os.makedirs(ini_dir, exist_ok=True)
                ini_path = safe_join(ini_dir, "PalWorldSettings.ini")

                # 업데이트도 install_server를 다시 부른다: 사용자가 채운 INI는 그대로 둔다
                if os.path.isfile(ini_path) and _parse_option_settings(ini_path):
                    print("[Palworld] Provisioning: keeping existing PalWorldSettings.ini", file=sys.stderr)
                else:
                    defaults = dict(DEFAULT_PALWORLD_SETTINGS)
                    defaults["RESTAPIEnabled"] = "True"
                    # 만들 때 고른 설정(이름·비밀번호·인원·포트 등)을 처음 값으로
                    _apply_settings(defaults, config.get("settings"))
                    defaults["RCONEnabled"] = "False"
                    if _write_option_settings(ini_path, defaults):
                        print(
                            f"[Palworld] Provisioning: wrote default PalWorldSettings.ini with REST enabled",
                            file=sys.stderr,
                        )
                    else:
                        print("[Palworld] Provisioning: failed to write default INI", file=sys.stderr)
                        return {"success": False, "error_code": "settings_write_failed",
                                "message": i18n.t("errors.settings_write_failed")}
            except PathOutside:
                raise
            except Exception as e:
                print(
                    f"[Palworld] Provisioning: failed to write default INI: {e}",
                    file=sys.stderr,
                )
                return {"success": False, "error_code": "settings_write_failed",
                        "message": i18n.t("errors.settings_write_failed")}

        return result

    except ImportError:
        print("[Palworld] SteamCMD extension not available, returning instructions", file=sys.stderr)
    except PathOutside:
        raise
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
        
        if not instance_id and not os.environ.get("SABA_HOST_DIR"):
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
    
    except PathOutside:
        raise
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"Error details: {error_details}", file=sys.stderr)
        return {
            "success": False,
            "message": i18n.t("errors.command_failed", error=str(e))
        }


def execute_command_via_rest(instance_id, command_lower, args, config):
    """명령의 HTTP 메서드와 endpoint를 보존해 데몬에 보낸다."""
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
    
    return execute_rest_via_daemon(instance_id, endpoint, body, command_lower, method=method)


def execute_rest_via_daemon(instance_id, endpoint, body, command_text, method="POST"):
    """SDK가 HOST 통로와 데몬 HTTP 호환 경로를 고른다."""
    try:
        result = daemon_rest(instance_id, method, endpoint, body=body, timeout=5)
        result.setdefault("success", True)
        result["protocol"] = "rest"
        if result["success"]:
            result["message"] = i18n.t("messages.command_executed", command=command_text)
        else:
            result.setdefault("message", result.get("error") or i18n.t("errors.daemon_error", error="REST"))
        return result
    
    except ConnectionError as e:
        print(f"[Palworld] Daemon connection error: {e}", file=sys.stderr)
        result = {
            "success": False,
            "message": i18n.t("errors.daemon_connection_error", error=str(e))
        }
        if getattr(e, "error_code", None):
            result["error_code"] = e.error_code
        return result
    except json.JSONDecodeError as e:
        print(f"[Palworld] Invalid JSON response from daemon: {e}", file=sys.stderr)
        return {
            "success": False,
            "message": i18n.t("errors.invalid_json_response", error=str(e))
        }
    except PathOutside:
        raise
    except Exception as e:
        print(f"[Palworld] Daemon error: {e}", file=sys.stderr)
        return {
            "success": False,
            "message": i18n.t("errors.daemon_error", error=str(e))
        }


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
    "config_files": config_files,
    "reset_properties": reset_properties,
    "reset_server": reset_server,
    "diagnose_log": diagnose_log,
    "list_versions": list_versions,
    "get_version_details": get_version_details,
    "install_server": install_server,
    "player_count": player_count,
    "backup_paths": backup_paths,
    "backup_snapshots": backup_snapshots,
}


def _answering_missing_extension(fn):
    """익스텐션이 꺼져 있으면 예외 대신 `extension_required`로 답한다 (데몬이 설치 안내를 띄운다)"""
    def call(config):
        try:
            return fn(config)
        except ExtensionMissing as e:
            return {
                "success": False,
                "message": i18n.t("messages.extension_missing", extension=e.extension),
                "error_code": "extension_required",
                "missing_extensions": [e.extension],
            }
    return call


if __name__ == "__main__":
    # 공용 진입점: 깨진 stdin·모르는 함수·함수 안의 예외도 JSON으로 답하고 종료 코드 0을 지킨다
    from daemon_rcon import run_lifecycle
    run_lifecycle({name: _answering_missing_extension(fn) for name, fn in FUNCTIONS.items()})
