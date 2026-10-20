#!/usr/bin/env python3
"""
saba-chan — Project Zomboid Module Lifecycle

Manages the lifecycle of a Project Zomboid dedicated server.
Handles validation, launch command generation, status checks,
graceful shutdown, settings synchronization, and installation via SteamCMD.

Project Zomboid is a Java-based game server (Steam App 380870) that:
  - Uses a bundled JRE (jre64/bin/java.exe) with custom classpath
  - Stores configs in ~/Zomboid/Server/{servername}.ini (separate from install dir)
  - Takes commands on its console (stdin); RCON stays off (saba-chan security policy)
"""

import sys
import json
import subprocess
import locale
import os
import re
import socket
import sqlite3
import hashlib
import time
from pathlib import Path

# ─── Path setup ──────────────────────────────────────────

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))

# Add project root for shared extensions
_root_dir = os.path.normpath(os.path.join(MODULE_DIR, '..', '..'))
if _root_dir not in sys.path:
    sys.path.insert(0, _root_dir)

from i18n import I18n

from daemon_rcon import daemon_stdin_command, force_kill, is_process_alive, wait_for_exit

i18n = I18n(MODULE_DIR)


# ─── Constants ───────────────────────────────────────────

# Mapping: saba-chan internal key → servertest.ini key
_INI_KEY_MAP = {
    "public_name":           "PublicName",
    "public_description":    "PublicDescription",
    "password":              "Password",
    "max_players":           "MaxPlayers",
    "port":                  "DefaultPort",
    "udp_port":              "UDPPort",
    "pvp":                   "PVP",
    "open":                  "Open",
    "public":                "Public",
    "pause_empty":           "PauseEmpty",
    "global_chat":           "GlobalChat",
    "map":                   "Map",
    "mods":                  "Mods",
    "workshop_items":        "WorkshopItems",
    "announce_death":        "AnnounceDeath",
    "safety_system":         "SafetySystem",
    "sleep_allowed":         "SleepAllowed",
    "sleep_needed":          "SleepNeeded",
    "save_world_every_minutes": "SaveWorldEveryMinutes",
    "ping_limit":            "PingLimit",
    "hours_for_loot_respawn": "HoursForLootRespawn",
    "max_items_for_loot_respawn": "MaxItemsForLootRespawn",
    "no_fire":               "NoFire",
    "player_safehouse":      "PlayerSafehouse",
    "safehouse_allow_trespass": "SafehouseAllowTrepass",
    "safehouse_allow_loot":  "SafehouseAllowLoot",
    "safehouse_allow_respawn": "SafehouseAllowRespawn",
    "faction":               "Faction",
    "allow_destruction_by_sledgehammer": "AllowDestructionBySledgehammer",
    "speed_limit":           "SpeedLimit",
    "do_lua_checksum":       "DoLuaChecksum",
    "steam_vac":             "SteamVAC",
    "voice_enable":          "VoiceEnable",
    "minutes_per_page":      "MinutesPerPage",
    "spawn_point":           "SpawnPoint",
    "max_accounts_per_user": "MaxAccountsPerUser",
    "spawn_items":           "SpawnItems",
    # ── Player / Display ──
    "server_welcome_message":  "ServerWelcomeMessage",
    "auto_create_user_in_whitelist": "AutoCreateUserInWhiteList",
    "display_user_name":       "DisplayUserName",
    "show_first_and_last_name": "ShowFirstAndLastName",
    "chat_streams":            "ChatStreams",
    "allow_non_ascii_username": "AllowNonAsciiUsername",
    "mouse_over_to_see_display_name": "MouseOverToSeeDisplayName",
    "hide_players_behind_you": "HidePlayersBehindYou",
    "player_bump_player":      "PlayerBumpPlayer",
    "player_respawn_with_self": "PlayerRespawnWithSelf",
    "player_respawn_with_other": "PlayerRespawnWithOther",
    "drop_off_whitelist_after_death": "DropOffWhiteListAfterDeath",
    # ── Safety / PVP ──
    "show_safety":             "ShowSafety",
    "safety_toggle_timer":     "SafetyToggleTimer",
    "safety_cooldown_timer":   "SafetyCooldownTimer",
    "pvp_melee_while_hit_reaction": "PVPMeleeWhileHitReaction",
    "pvp_melee_damage_modifier": "PVPMeleeDamageModifier",
    "pvp_firearm_damage_modifier": "PVPFirearmDamageModifier",
    # ── Safehouse (extended) ──
    "admin_safehouse":         "AdminSafehouse",
    "safehouse_allow_fire":    "SafehouseAllowFire",
    "safehouse_day_survived_to_claim": "SafehouseDaySurvivedToClaim",
    "safehouse_removal_time":  "SafeHouseRemovalTime",
    "disable_safehouse_when_player_connected": "DisableSafehouseWhenPlayerConnected",
    # ── Faction (extended) ──
    "faction_day_survived_to_create": "FactionDaySurvivedToCreate",
    "faction_players_required_for_tag": "FactionPlayersRequiredForTag",
    "allow_trade_ui":          "AllowTradeUI",
    # ── Loot / World ──
    "construction_prevents_loot_respawn": "ConstructionPreventsLootRespawn",
    "hours_for_world_item_removal": "HoursForWorldItemRemoval",
    "world_item_removal_list": "WorldItemRemovalList",
    "blood_splat_lifespan_days": "BloodSplatLifespanDays",
    "item_numbers_limit_per_container": "ItemNumbersLimitPerContainer",
    "trash_delete_all":        "TrashDeleteAll",
    # ── Network / Steam ──
    "ping_frequency":          "PingFrequency",
    "deny_login_on_overloaded_server": "DenyLoginOnOverloadedServer",
    "kick_fast_players":       "KickFastPlayers",
    "steam_port1":             "SteamPort1",
    "steam_port2":             "SteamPort2",
    "steam_scoreboard":        "SteamScoreboard",
    "upnp":                    "UPnP",
    "upnp_lease_time":         "UPnPLeaseTime",
    "upnp_zero_lease_time_fallback": "UPnPZeroLeaseTimeFallback",
    "upnp_force":              "UPnPForce",
    "server_browser_announced_ip": "server_browser_announced_ip",
    "use_tcp_for_map_downloads": "UseTCPForMapDownloads",
    "physics_delay":           "PhysicsDelay",
    # ── Voice ──
    "voice_complexity":        "VoiceComplexity",
    "voice_period":            "VoicePeriod",
    "voice_sample_rate":       "VoiceSampleRate",
    "voice_buffering":         "VoiceBuffering",
    "voice_min_distance":      "VoiceMinDistance",
    "voice_max_distance":      "VoiceMaxDistance",
    "voice_3d":                "Voice3D",
    # ── Co-op ──
    "coop_server_launch_timeout": "CoopServerLaunchTimeout",
    "coop_master_ping_timeout": "CoopMasterPingTimeout",
    # ── Gameplay misc ──
    "fast_forward_multiplier": "FastForwardMultiplier",
    "player_save_on_damage":   "PlayerSaveOnDamage",
    "save_transaction_id":     "SaveTransactionID",
    "car_engine_attraction_modifier": "CarEngineAttractionModifier",
    "ban_kick_global_sound":   "BanKickGlobalSound",
    "remove_player_corpses_on_corpse_removal": "RemovePlayerCorpsesOnCorpseRemoval",
    "client_command_filter":   "ClientCommandFilter",
    # ── Discord ──
    "discord_enable":          "DiscordEnable",
    "discord_token":           "DiscordToken",
    "discord_channel":         "DiscordChannel",
    "discord_channel_id":      "DiscordChannelID",
    # ── Radio ──
    "disable_radio_staff":     "DisableRadioStaff",
    "disable_radio_admin":     "DisableRadioAdmin",
    "disable_radio_gm":        "DisableRadioGM",
    "disable_radio_overseer":  "DisableRadioOverseer",
    "disable_radio_moderator": "DisableRadioModerator",
    "disable_radio_invisible": "DisableRadioInvisible",
    # ── Zombie performance ──
    "zombie_update_max_high_priority": "ZombieUpdateMaxHighPriority",
    "zombie_update_delta":     "ZombieUpdateDelta",
    "zombie_update_radius_low_priority": "ZombieUpdateRadiusLowPriority",
    "zombie_update_radius_high_priority": "ZombieUpdateRadiusHighPriority",
}

# Reverse mapping for read_properties
_INI_KEY_MAP_REV = {v: k for k, v in _INI_KEY_MAP.items()}

# Default servertest.ini values
DEFAULT_INI_PROPERTIES = {
    "PVP": "true",
    "PauseEmpty": "true",
    "GlobalChat": "true",
    "Open": "true",
    "ServerWelcomeMessage": "Welcome to Project Zomboid Multiplayer!",
    "AutoCreateUserInWhiteList": "false",
    "DisplayUserName": "true",
    "ShowFirstAndLastName": "false",
    "SpawnPoint": "0,0,0",
    "SafetySystem": "true",
    "ShowSafety": "true",
    "SafetyToggleTimer": "2",
    "SafetyCooldownTimer": "3",
    "SpawnItems": "",
    "DefaultPort": "16261",
    "Mods": "",
    "Map": "Muldraugh, KY",
    "DoLuaChecksum": "true",
    "DenyLoginOnOverloadedServer": "true",
    "Public": "false",
    "PublicName": "My PZ Server",
    "PublicDescription": "",
    "MaxPlayers": "16",
    "PingFrequency": "10",
    "PingLimit": "400",
    "HoursForLootRespawn": "0",
    "MaxItemsForLootRespawn": "4",
    "ConstructionPreventsLootRespawn": "true",
    "DropOffWhiteListAfterDeath": "false",
    "NoFire": "false",
    "AnnounceDeath": "false",
    "MinutesPerPage": "1.0",
    "SaveWorldEveryMinutes": "0",
    "PlayerSafehouse": "true",
    "AdminSafehouse": "false",
    "SafehouseAllowTrepass": "true",
    "SafehouseAllowFire": "true",
    "SafehouseAllowLoot": "true",
    "SafehouseAllowRespawn": "false",
    "SafehouseDaySurvivedToClaim": "0",
    "SafeHouseRemovalTime": "144",
    "AllowDestructionBySledgehammer": "true",
    "KickFastPlayers": "false",
    "RCONPort": "27015",
    "RCONPassword": "",
    "Password": "",
    "MaxAccountsPerUser": "0",
    "SleepAllowed": "false",
    "SleepNeeded": "false",
    "SteamVAC": "true",
    "WorkshopItems": "",
    "SteamScoreboard": "true",
    "UPnP": "true",
    "VoiceEnable": "true",
    "SpeedLimit": "70.0",
    "Faction": "true",
    "FactionDaySurvivedToCreate": "0",
    "FactionPlayersRequiredForTag": "1",
    "AllowTradeUI": "true",
    # ── Player / Display ──
    "ChatStreams": "s,r,a,w,y,sh,f,all",
    "ShowSafety": "true",
    "SafetyToggleTimer": "2",
    "SafetyCooldownTimer": "3",
    "DenyLoginOnOverloadedServer": "true",
    "PingFrequency": "10",
    "ConstructionPreventsLootRespawn": "true",
    "DropOffWhiteListAfterDeath": "false",
    "AdminSafehouse": "false",
    "SafehouseAllowFire": "true",
    "SafehouseDaySurvivedToClaim": "0",
    "SafeHouseRemovalTime": "144",
    "KickFastPlayers": "false",
    # ── Network / Steam ──
    "SteamPort1": "8766",
    "SteamPort2": "8767",
    "SteamScoreboard": "true",
    "UPnP": "true",
    "UPnPLeaseTime": "86400",
    "UPnPZeroLeaseTimeFallback": "true",
    "UPnPForce": "true",
    "CoopServerLaunchTimeout": "20",
    "CoopMasterPingTimeout": "60",
    # ── Voice ──
    "VoiceComplexity": "5",
    "VoicePeriod": "20",
    "VoiceSampleRate": "24000",
    "VoiceBuffering": "8000",
    "VoiceMinDistance": "10.0",
    "VoiceMaxDistance": "300.0",
    "Voice3D": "true",
    # ── Misc ──
    "PhysicsDelay": "500",
    "server_browser_announced_ip": "",
    "UseTCPForMapDownloads": "false",
    "PlayerRespawnWithSelf": "false",
    "PlayerRespawnWithOther": "false",
    "FastForwardMultiplier": "40.0",
    "PlayerSaveOnDamage": "true",
    "SaveTransactionID": "false",
    "DisableSafehouseWhenPlayerConnected": "false",
    # ── Discord ──
    "DiscordEnable": "false",
    "DiscordToken": "",
    "DiscordChannel": "",
    "DiscordChannelID": "",
    # ── Radio ──
    "DisableRadioStaff": "false",
    "DisableRadioAdmin": "true",
    "DisableRadioGM": "true",
    "DisableRadioOverseer": "false",
    "DisableRadioModerator": "false",
    "DisableRadioInvisible": "true",
    # ── PVP / Combat ──
    "PVPMeleeWhileHitReaction": "false",
    "PVPMeleeDamageModifier": "30.0",
    "PVPFirearmDamageModifier": "50.0",
    "MouseOverToSeeDisplayName": "true",
    "HidePlayersBehindYou": "true",
    "CarEngineAttractionModifier": "0.5",
    "PlayerBumpPlayer": "false",
    # ── Item / World cleanup ──
    "ClientCommandFilter": "-vehicle.*;+vehicle.damageWindow;+vehicle.fixPart;+vehicle.installPart;+vehicle.uninstallPart",
    "ItemNumbersLimitPerContainer": "0",
    "BloodSplatLifespanDays": "0",
    "AllowNonAsciiUsername": "false",
    "BanKickGlobalSound": "true",
    "RemovePlayerCorpsesOnCorpseRemoval": "false",
    "TrashDeleteAll": "false",
    "HoursForWorldItemRemoval": "0.0",
    "WorldItemRemovalList": "Base.Vest,Base.Shirt,Base.Blouse,Base.Skirt,Base.Shoes",
    # ── Zombie performance ──
    "ZombieUpdateMaxHighPriority": "50",
    "ZombieUpdateDelta": "0.5",
    "ZombieUpdateRadiusLowPriority": "45.0",
    "ZombieUpdateRadiusHighPriority": "10.0",
}


# ═══════════════════════════════════════════════════════════
#                    INI File Manager
# ═══════════════════════════════════════════════════════════

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
        server_name = config.get("server_name", "servertest")
        # 예전 인스턴스: config_dir 바로 아래에 ini가 있다 (cache_dir이 있으면 그쪽이 우선)
        layout = _legacy_layout(config)
        if layout:
            return os.path.join(layout[1], f"{server_name}.ini")
        zomboid_dir = os.path.join(_zomboid_home(config), "Server")
        return os.path.join(zomboid_dir, f"{server_name}.ini")

    @staticmethod
    def encoding_of(ini_path):
        """ini의 인코딩: UTF-8이 아니면(PZ·메모장이 ANSI로 쓴 cp949 등) cp949·시스템 로캘·latin-1 순서
        (영문 Windows의 cp1252는 거의 모든 바이트를 받아 한글을 깨진 글자로 읽으므로 cp949가 먼저다).
        쓸 때도 같은 인코딩을 써서 건드리지 않은 키의 한글 값이 깨지지 않게 한다"""
        try:
            with open(ini_path, "rb") as f:
                raw = f.read()
        except OSError:
            return "utf-8"
        for encoding in ("utf-8", "cp949", locale.getpreferredencoding(False), "latin-1"):
            try:
                raw.decode(encoding)
                return encoding
            except (UnicodeDecodeError, LookupError):
                continue
        return "utf-8"

    @staticmethod
    def read(ini_path):
        """Read an INI file and return {key: value} dict."""
        props = {}
        if not os.path.exists(ini_path):
            return props
        
        try:
            with open(ini_path, 'r', encoding=IniManager.encoding_of(ini_path), errors='replace') as f:
                for raw in f:
                    # 값은 첫 "=" 뒤를 그대로 둔다 (비밀번호의 앞뒤 공백도 값이다). 줄바꿈만 뗀다
                    line = raw.rstrip('\r\n')
                    if not line.strip() or line.lstrip().startswith('#'):
                        continue
                    if '=' in line:
                        key, _, value = line.partition('=')
                        props[key.strip()] = value
        except (IOError, OSError) as e:
            print(f"Warning: Could not read {ini_path}: {e}", file=sys.stderr)
        
        return props

    @staticmethod
    def write(ini_path, properties):
        """Write properties to INI file, preserving comments and order.
        
        If the file exists, updates existing keys and appends new ones.
        If it doesn't exist, creates it from scratch.
        """
        os.makedirs(os.path.dirname(ini_path), exist_ok=True)
        
        if os.path.exists(ini_path):
            # Read existing file, update values in place
            lines = []
            updated_keys = set()
            encoding = IniManager.encoding_of(ini_path)
            
            with open(ini_path, 'r', encoding=encoding, errors='replace') as f:
                for line in f:
                    stripped = line.strip()
                    if stripped and not stripped.startswith('#') and '=' in stripped:
                        key, _, old_val = stripped.partition('=')
                        key = key.strip()
                        if key in properties:
                            lines.append(f"{key}={properties[key]}\n")
                            updated_keys.add(key)
                        else:
                            lines.append(line)
                    else:
                        lines.append(line)
            
            # Append any new keys not already in the file
            for key, value in properties.items():
                if key not in updated_keys:
                    lines.append(f"{key}={value}\n")
            
            text = "".join(lines)
            try:
                data = text.encode(encoding)
            except UnicodeEncodeError:
                # 원래 인코딩에 없는 글자를 새로 넣었다: UTF-8로 바꿔 쓴다
                data = text.encode("utf-8")
            with open(ini_path, 'wb') as f:
                f.write(data)
        else:
            # Create new file
            with open(ini_path, 'w', encoding='utf-8') as f:
                for key, value in properties.items():
                    f.write(f"{key}={value}\n")

    @staticmethod
    def update(ini_path, updates):
        """Update specific keys in an INI file."""
        existing = IniManager.read(ini_path) if os.path.exists(ini_path) else {}
        existing.update(updates)
        IniManager.write(ini_path, existing)


# ═══════════════════════════════════════════════════════════
#                   Error Detector
# ═══════════════════════════════════════════════════════════

class ErrorDetector:
    """Detect known error patterns in PZ server logs."""

    PATTERNS = [
        {
            "pattern": re.compile(r"java\.lang\.OutOfMemoryError|unable to create new native thread", re.IGNORECASE),
            "code": "out_of_memory",
            "severity": "error",
        },
        {
            "pattern": re.compile(r"Address already in use|BindException", re.IGNORECASE),
            "code": "port_in_use",
            "severity": "error",
        },
        {
            "pattern": re.compile(r"Assertion Failed.*Illegal termination", re.IGNORECASE),
            "code": "illegal_termination",
            "severity": "error",
        },
        {
            "pattern": re.compile(r"java\.io\.FileNotFoundException|AccessDeniedException|Permission denied", re.IGNORECASE),
            "code": "permission_denied",
            "severity": "error",
        },
        {
            "pattern": re.compile(r"crash|EXCEPTION_ACCESS_VIOLATION|StackOverflowError", re.IGNORECASE),
            "code": "server_crash",
            "severity": "error",
        },
        {
            "pattern": re.compile(r"mod.*error|lua.*error|LuaException", re.IGNORECASE),
            "code": "mod_error",
            "severity": "warning",
        },
        {
            "pattern": re.compile(r"Save (complete|failed|error)", re.IGNORECASE),
            "code": "save_status",
            "severity": "info",
        },
    ]

    @classmethod
    def scan(cls, log_text):
        """Scan log text and return list of detected issues."""
        issues = []
        seen_codes = set()

        for entry in cls.PATTERNS:
            if entry["code"] in seen_codes:
                continue
            match = entry["pattern"].search(log_text)
            if match:
                seen_codes.add(entry["code"])
                issues.append({
                    "severity": entry["severity"],
                    "code": entry["code"],
                    "message": i18n.t(f"errors.detect.{entry['code']}"),
                    "solution": i18n.t(f"errors.solutions.{entry['code']}"),
                    "matched_text": match.group(0)[:200],
                })

        return issues


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


def _enforce_rcon_policy(ini_path):
    """Keep RCON off in every mode (an empty RCONPassword disables it in Project Zomboid).

    saba-chan blocks RCON for security: commands go through the server console (stdin).
    Returns the RCON password, always "".
    """
    props = IniManager.read(ini_path) if os.path.exists(ini_path) else {}
    if props.get("RCONPassword", ""):
        IniManager.update(ini_path, {"RCONPassword": ""})
        print(i18n.t("messages.rcon_disabled_managed"), file=sys.stderr)
    return ""


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
    
    # Check port availability
    port = int(config.get("port", 16261))
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(1)
        result = sock.connect_ex(("127.0.0.1", port))
        sock.close()
    except Exception:
        pass  # UDP port check is unreliable; skip
    
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
    server_name = config.get("server_name", "servertest")
    path = os.path.join(_zomboid_home(config), "db", f"{server_name}.db")
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
    
    # 사용자 설정값
    ram_gb = config.get("ram", 4)
    try:
        ram_gb = int(ram_gb)
    except (ValueError, TypeError):
        ram_gb = 4
    server_name = config.get("server_name", "servertest")
    preview = bool(config.get("preview"))  # 미리보기는 파일·폴더를 만들거나 고치지 않는다
    if not preview:
        _enforce_rcon_policy(IniManager.get_ini_path(config))

    # ── 1차: 배치/쉘 스크립트 파싱 ──
    parsed = _parse_launch_info(config, server_dir)

    if parsed:
        java_path = parsed["java_exe_abs"]
        classpath = parsed["classpath"]
        
        # 파싱된 JVM 플래그에서 메모리 설정만 사용자 값으로 교체
        jvm_flags = []
        for flag in parsed["jvm_flags"]:
            if flag.startswith("-Xms"):
                jvm_flags.append(f"-Xms{ram_gb}g")
            elif flag.startswith("-Xmx"):
                jvm_flags.append(f"-Xmx{ram_gb}g")
            else:
                jvm_flags.append(flag)
        
        # main_class 뒤 인자: 스크립트 원본의 extra_args에서 -servername을 찾아 교체
        main_class = parsed["main_class"]
        extra_args = []
        skip_next = False
        servername_set = False
        for i, arg in enumerate(parsed["extra_args"]):
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
        
        args = jvm_flags + ["-cp", classpath, main_class] + extra_args
    else:
        # ── 2차: 하드코딩 폴백 ──
        java_path = _find_java(server_dir)
        if not java_path:
            return {"success": False, "message": i18n.t("errors.java_not_found")}
        
        classpath = _resolve_classpath(server_dir)
        if not classpath:
            return {"success": False, "message": i18n.t("errors.classpath_not_found")}
        
        native_libs = _get_native_libs(server_dir)
        
        args = [
            "-Djava.awt.headless=true",
            "-Dzomboid.steam=1",
            "-Dzomboid.znetlog=1",
            "-XX:+UseZGC",
            "-XX:-CreateCoredumpOnCrash",
            "-XX:-OmitStackTraceInFastThrow",
            f"-Xms{ram_gb}g",
            f"-Xmx{ram_gb}g",
            f"-Djava.library.path={native_libs}",
            "-cp", classpath,
            "zombie.network.GameServer",
            "-servername", server_name,
            "-statistic", "0",
        ]
    
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

    # Admin password (only on first launch to bypass prompt)
    admin_password = config.get("admin_password", "")
    if admin_password:
        args.extend(["-adminpassword", admin_password])
    
    # 인스턴스마다 따로 둔 데이터 폴더 (없으면 PZ 기본 ~/Zomboid)
    if _cache_dir(config) or _legacy_config_dir(config):
        home = _zomboid_home(config)
        if not preview:
            os.makedirs(home, exist_ok=True)
        args.append(f"-cachedir={home}")
    
    return {
        "success": True,
        "program": java_path,
        "args": args,
        "working_dir": server_dir,
    }


def status(config):
    """Check the current server status by the daemon-tracked PID."""
    pid = config.get("pid")
    if pid and is_process_alive(pid):
        return {
            "success": True,
            "status": "running",
            "message": i18n.t("messages.server_starting_no_response"),
        }
    return {"success": True, "status": "stopped"}


# ╔═══════════════════════════════════════════════════════════╗
# ║                  Optional Functions                       ║
# ╚═══════════════════════════════════════════════════════════╝

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


def _request_terminate(pid):
    """POSIX: SIGTERM (서버가 저장하고 끝난다). Windows 콘솔 앱에는 보낼 정상 종료 신호가 없어 False"""
    if os.name == "nt":
        return False
    import signal
    try:
        os.kill(int(pid), signal.SIGTERM)
        return True
    except (OSError, ValueError) as e:
        print(f"[Zomboid] SIGTERM failed: {e}", file=sys.stderr)
        return False


def _console_quit(instance_id):
    """콘솔(stdin)로 save 후 quit. 콘솔이 없으면(409, 데몬 없음) False"""
    if not instance_id:
        return False
    try:
        daemon_stdin_command(instance_id, "save")
        print(i18n.t("messages.save_before_quit"), file=sys.stderr)
        time.sleep(5)
        daemon_stdin_command(instance_id, "quit")
        return True
    except Exception as e:
        print(f"[Zomboid] console stop unavailable: {e}", file=sys.stderr)
        return False


def _kill(pid):
    ok, error = force_kill(pid)
    if ok:
        return {"success": True, "message": i18n.t("messages.force_killed", pid=pid)}
    return {"success": False, "message": i18n.t("errors.failed_to_kill", error=error)}


def stop(config):
    """Stop the server.

    Normal stop: console `save` + `quit`, else SIGTERM, then wait for the exit.
    Force kill only when force=true or the server did not exit in time (RCON is off by policy).
    With neither a console nor SIGTERM, do not kill: answer `graceful_stop_unavailable`.
    The daemon stops managed servers with a console itself, so this runs mostly without one.
    """
    pid = config.get("pid")
    if not pid:
        return {"success": True, "message": i18n.t("messages.no_process_running")}
    if config.get("force", False):
        return _kill(pid)

    if not (_console_quit(config.get("instance_id", "")) or _request_terminate(pid)):
        # 저장 후 종료를 요청할 길이 없다(콘솔 없음, Windows): 저장 없이 죽이지 않고 데몬이 사용자에게 묻게 한다
        return {"success": False, "error_code": "graceful_stop_unavailable",
                "message": i18n.t("errors.graceful_stop_unavailable")}
    print(i18n.t("messages.shutdown_signal_sent", pid=pid), file=sys.stderr)
    if wait_for_exit(pid, timeout=_stop_timeout(config)):
        return {"success": True, "message": i18n.t("messages.graceful_stop", pid=pid)}
    print(i18n.t("messages.graceful_stop_timeout"), file=sys.stderr)
    return _kill(pid)


def _ini_updates(settings):
    """saba-chan 설정 키를 ini 키·값으로 (ini에 없는 키는 뺀다)"""
    updates = {}
    for key, value in (settings or {}).items():
        ini_key = _INI_KEY_MAP.get(key)
        if ini_key and value is not None:
            # Convert booleans to lowercase strings
            if isinstance(value, bool):
                value = "true" if value else "false"
            updates[ini_key] = str(value)
    return updates


def configure(config):
    """Apply settings from saba-chan to servertest.ini.
    
    Maps internal key names to PZ INI key names and writes them.
    """
    settings = config.get("settings", {})
    if not settings:
        return {"success": False, "message": i18n.t("errors.no_settings")}
    
    ini_path = IniManager.get_ini_path(config)
    updates = _ini_updates(settings)
    
    if not updates:
        return {"success": True, "updated_keys": [], "message": i18n.t("messages.no_mappable_settings")}
    
    try:
        IniManager.update(ini_path, updates)
        return {
            "success": True,
            "updated_keys": list(updates.keys()),
            "message": i18n.t("messages.properties_updated"),
        }
    except Exception as e:
        return {
            "success": False,
            "message": i18n.t("errors.properties_write_failed", error=str(e)),
        }


def import_settings(config):
    """Read server INI and return settings in saba-chan key format.

    Used during migration to import existing server settings into saba-chan.
    """
    result = read_properties(config)
    if not result.get("success"):
        return result

    raw_props = result.get("properties", {})
    if not raw_props:
        return {"success": True, "settings": {}, "message": i18n.t("messages.no_properties_found")}

    from daemon_rcon import coerce_setting, load_setting_types
    types = load_setting_types(os.path.dirname(os.path.abspath(__file__)))
    settings = {key: coerce_setting(raw_value, types.get(key)) for key, raw_value in raw_props.items()}

    return {"success": True, "settings": settings}


def read_properties(config):
    """Read the server INI file and return settings.
    
    Returns properties mapped back to saba-chan internal key names.
    """
    ini_path = IniManager.get_ini_path(config)
    
    if not os.path.exists(ini_path):
        # Return defaults mapped to internal keys
        mapped_defaults = {}
        for ini_key, value in DEFAULT_INI_PROPERTIES.items():
            internal_key = _INI_KEY_MAP_REV.get(ini_key, ini_key)
            mapped_defaults[internal_key] = value
        return {
            "success": True,
            "properties": mapped_defaults,
            "message": i18n.t("messages.properties_using_defaults"),
        }
    
    raw_props = IniManager.read(ini_path)
    mapped = {}
    for ini_key, value in raw_props.items():
        internal_key = _INI_KEY_MAP_REV.get(ini_key, ini_key)
        mapped[internal_key] = value
    
    return {"success": True, "properties": mapped}


def command(config):
    """Send a command to the server console (stdin) through the daemon."""
    cmd_name = config.get("command_name", "")
    cmd_text = config.get("command_text", "")
    instance_id = config.get("instance_id", "")
    actual_cmd = cmd_text or cmd_name
    if not actual_cmd:
        return {"success": False, "message": i18n.t("errors.no_command")}
    if not instance_id:
        return {"success": False, "message": i18n.t("errors.server_not_running")}
    try:
        result = daemon_stdin_command(instance_id, actual_cmd)
    except Exception as e:
        return {"success": False, "message": i18n.t("errors.command_send_failed", error=str(e))}
    return {"success": True, "message": i18n.t("messages.command_sent", command=actual_cmd), "data": result}


def diagnose_log(config):
    """Analyze server log for known error patterns."""
    working_dir = config.get("working_dir", "")
    server_name = config.get("server_name", "servertest")
    
    # PZ console log locations
    log_paths = []
    if working_dir:
        log_paths.append(os.path.join(working_dir, "server-console.txt"))
    
    zomboid_dir = _zomboid_home(config)
    log_paths.append(os.path.join(zomboid_dir, "console.txt"))
    log_paths.append(os.path.join(zomboid_dir, "server-console.txt"))
    
    log_text = ""
    for log_path in log_paths:
        if os.path.exists(log_path):
            try:
                with open(log_path, 'r', encoding='utf-8', errors='replace') as f:
                    # Read last 100KB
                    f.seek(0, 2)
                    size = f.tell()
                    f.seek(max(0, size - 102400))
                    log_text += f.read()
            except (IOError, OSError):
                continue
    
    if not log_text:
        return {"success": True, "issues": [], "message": i18n.t("messages.no_log_files")}
    
    issues = ErrorDetector.scan(log_text)
    return {"success": True, "issues": issues}


def install_server(config):
    """Install/update the Project Zomboid dedicated server via SteamCMD.

    Uses the saba-chan SteamCMD extension for portable, automatic download.
    Falls back to raw SteamCMD if the extension is unavailable.

    Steam App ID: 380870
    Anonymous login: Yes
    Optionally use -beta unstable for Build 42 unstable.
    """
    install_dir = config.get("install_dir", "")
    if not install_dir:
        working_dir = config.get("working_dir", "")
        if working_dir:
            install_dir = os.path.join(working_dir, "server")
        else:
            return {"success": False, "message": i18n.t("errors.no_install_dir")}

    os.makedirs(install_dir, exist_ok=True)
    use_beta = config.get("use_beta", False)

    # ── SteamCMD extension 사용 시도 ──
    # 먼저 익스텐션 활성화 상태 확인
    enabled_exts = os.environ.get("SABA_ENABLED_EXTENSIONS", "").split(",")
    if "steamcmd" not in enabled_exts:
        return {
            "success": False,
            "message": i18n.t("errors.extension_not_enabled"),
            "error_code": "extension_required",
            "missing_extensions": ["steamcmd"],
        }

    try:
        from extensions.steamcmd import SteamCMD

        steam = SteamCMD()
        steam.ensure_available()

        result = steam.install(
            app_id=380870,
            install_dir=install_dir,
            anonymous=True,
            beta="unstable" if use_beta else None,
        )

        if result.get("success"):
            if sys.platform == "win32":
                exe = os.path.join(install_dir, "StartServer64.bat")
            else:
                exe = os.path.join(install_dir, "start-server.sh")
            result["install_path"] = install_dir
            result["executable_path"] = exe if os.path.exists(exe) else ""
            _write_initial_ini(config)

        return result

    except ImportError:
        print("[Zomboid] SteamCMD extension not available, trying raw SteamCMD", file=sys.stderr)
    except Exception as e:
        print(f"[Zomboid] SteamCMD extension install failed: {e}", file=sys.stderr)
        return {"success": False, "message": i18n.t("errors.install_failed", error=str(e))}

    # ── Fallback: raw SteamCMD ──
    steamcmd = _find_steamcmd()
    if not steamcmd:
        return {
            "success": False,
            "message": i18n.t("errors.steamcmd_not_found"),
        }

    cmd = [
        steamcmd,
        "+force_install_dir", install_dir,
        "+login", "anonymous",
        "+app_update", "380870",
    ]

    if use_beta:
        cmd.extend(["-beta", "unstable"])

    cmd.extend(["validate", "+quit"])

    print(f"Running SteamCMD: {' '.join(cmd)}", file=sys.stderr)

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600,
        )

        if result.returncode == 0 or "Success" in result.stdout:
            if sys.platform == "win32":
                exe = os.path.join(install_dir, "StartServer64.bat")
            else:
                exe = os.path.join(install_dir, "start-server.sh")
            _write_initial_ini(config)

            return {
                "success": True,
                "message": i18n.t("messages.install_complete"),
                "install_path": install_dir,
                "executable_path": exe if os.path.exists(exe) else "",
            }
        else:
            return {
                "success": False,
                "message": i18n.t("errors.install_failed", error=result.stderr[-500:] if result.stderr else i18n.t("errors.unknown_error")),
            }
    except subprocess.TimeoutExpired:
        return {"success": False, "message": i18n.t("errors.install_timeout")}
    except Exception as e:
        return {"success": False, "message": i18n.t("errors.install_failed", error=str(e))}


def _write_initial_ini(config):
    """첫 설치: 만들 때 고른 설정(접속 암호·인원·포트 등)으로 서버 ini를 만든다. 서버는 첫 실행 때 나머지 키를
    기본값으로 채운다. 이미 있으면(업데이트) 건드리지 않는다"""
    settings = config.get("settings") or {}
    ini_path = IniManager.get_ini_path({**config, **settings})
    if os.path.exists(ini_path):
        return
    updates = _ini_updates(settings)
    if not updates:
        return
    try:
        IniManager.write(ini_path, updates)
    except Exception as e:
        print(f"[Zomboid] Provisioning: could not write {ini_path}: {e}", file=sys.stderr)


def _find_steamcmd():
    """Find SteamCMD executable."""
    if sys.platform == "win32":
        candidates = [
            "steamcmd.exe",
            os.path.join("C:\\", "steamcmd", "steamcmd.exe"),
            os.path.join(os.environ.get("PROGRAMFILES(X86)", ""), "Steam", "steamcmd.exe"),
        ]
    else:
        candidates = [
            "steamcmd",
            "/usr/games/steamcmd",
            "/usr/bin/steamcmd",
            os.path.expanduser("~/steamcmd/steamcmd.sh"),
        ]
    
    for path in candidates:
        # Check PATH first
        try:
            result = subprocess.run(
                ["which" if sys.platform != "win32" else "where", path],
                capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                return path
        except Exception:
            pass
        
        # Check absolute path
        if os.path.isfile(path):
            return path
    
    return None


def reset_server(config):
    """Reset the server world (delete save data, keep settings).
    
    Deletes: <data folder>/Saves/Multiplayer/{servername}/
    Keeps: <data folder>/Server/{servername}.ini and .lua files
    """
    server_name = config.get("server_name", "servertest")
    
    saves_dir = os.path.join(_zomboid_home(config), "Saves", "Multiplayer", server_name)
    
    deleted = []
    errors = []
    
    # 접근할 수 없는 폴더를 '없음'으로 보고 성공을 돌려주지 않는다 (os.path.isdir은 권한 오류를 삼킨다)
    try:
        os.stat(saves_dir)
        exists = os.path.isdir(saves_dir)
    except FileNotFoundError:
        exists = False
    except OSError as e:
        return {"success": False, "message": i18n.t("errors.server_reset_partial", errors=f"{saves_dir}: {e}")}
    if exists:
        import shutil
        try:
            shutil.rmtree(saves_dir)
            deleted.append(saves_dir)
        except Exception as e:
            errors.append(f"{saves_dir}: {e}")
    
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


def _server_config_files(server_dir, server_name):
    """Server/ 폴더에서 이 서버 이름의 파일: `<이름>.ini`와 `<이름>_<종류>.lua`(SandboxVars·spawnregions·spawnpoints).
    `<이름>_2.ini`나 `<이름>_2_SandboxVars.lua`는 다른 서버(`<이름>_2`) 것이다.
    정해진 파일은 지금 없어도 낸다: 데몬은 훅 경로 안만 복원하므로, 지워진 파일도 되살릴 수 있어야 한다"""
    ini = os.path.join(server_dir, f"{server_name}.ini")
    prefix = f"{server_name}_"
    lua = {f"{prefix}{kind}.lua" for kind in _SERVER_LUA_KINDS}
    try:
        names = os.listdir(server_dir)
    except FileNotFoundError:
        names = []
    # 읽지 못하는 폴더(권한·I/O)는 OSError로 올린다: 설정 일부가 조용히 빠진 백업을 성공으로 남기지 않는다
    lua.update(n for n in names
               if n.startswith(prefix) and n.endswith(".lua") and "_" not in n[len(prefix):])
    return [ini] + [os.path.join(server_dir, n) for n in sorted(lua)]


def backup_paths(config):
    """백업할 세이브 위치 (데몬 `[backup] hook`): 이 서버 이름의 월드, 플레이어 DB, 서버 설정 파일.
    Server/ 폴더째 넣으면 다른 서버 이름의 설정까지 들어가므로 이 서버 것만 고른다"""
    server_name = config.get("server_name") or "servertest"
    home = _zomboid_home(config)
    layout = _legacy_layout(config)
    server_dir = layout[1] if layout else os.path.join(home, "Server")
    try:
        config_files = _server_config_files(server_dir, server_name)
    except OSError as e:
        return {"success": False, "message": i18n.t("errors.backup_paths_failed", error=str(e))}
    paths = [
        os.path.join(home, "Saves", "Multiplayer", server_name),
        os.path.join(home, "db", f"{server_name}.db"),
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
    "status":             status,
    "stop":               stop,
    "command":            command,
    "configure":          configure,
    "import_settings":    import_settings,
    "read_properties":    read_properties,
    "diagnose_log":       diagnose_log,
    "install_server":     install_server,
    "reset_server":       reset_server,
    "backup_paths":       backup_paths,
}
# 데몬이 지원 기능을 묻는 예약 함수: 표의 함수 이름을 돌려준다
FUNCTIONS["functions"] = lambda _config: {"success": True, "functions": [f for f in FUNCTIONS if f != "functions"]}


# ╔═══════════════════════════════════════════════════════════╗
# ║                    Entry Point                            ║
# ╚═══════════════════════════════════════════════════════════╝

def main():
    """Entry point when invoked by the saba-chan daemon.
    
    Usage: python lifecycle.py <function_name>
    Config is passed via stdin as JSON.
    Result is printed to stdout as JSON.
    Logs go to stderr.
    """
    if len(sys.argv) < 2:
        print(json.dumps({"success": False, "message": "Usage: lifecycle.py <function_name>"}))
        sys.exit(1)
    
    func_name = sys.argv[1]
    
    # Answered errors exit 0: the daemon discards stdout when the exit code is not 0.
    if func_name not in FUNCTIONS:
        print(json.dumps({
            "success": False,
            "message": f"Unknown function: {func_name}",
            "available": list(FUNCTIONS.keys())
        }))
        return
    
    try:
        config_str = sys.stdin.read()
        config = json.loads(config_str) if config_str.strip() else {}
    except json.JSONDecodeError:
        config = {}
    
    try:
        result = FUNCTIONS[func_name](config)
        print(json.dumps(result, ensure_ascii=False))
    except Exception as e:
        print(json.dumps({
            "success": False,
            "message": f"Error in {func_name}: {str(e)}"
        }))


if __name__ == "__main__":
    main()
