#!/usr/bin/env python3
"""
saba-chan — Project Zomboid Module Lifecycle

Manages the lifecycle of a Project Zomboid dedicated server.
Handles validation, launch command generation, status checks,
graceful shutdown, settings synchronization, and installation via SteamCMD.

Project Zomboid is a Java-based game server (Steam App 380870) that:
  - Uses a bundled JRE (jre64/bin/java.exe) with custom classpath
  - Stores configs in ~/Zomboid/Server/{servername}.ini (separate from install dir)
  - Supports RCON on port 27015 for remote administration
  - Supports stdin console for direct command input
"""

import sys
import json
import subprocess
import os
import re
import socket
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

# RCON communication delegates to the Rust daemon's HTTP API
try:
    from daemon_rcon import rcon_command as _rcon_command_bridge
    def _rcon_command(host, port, password, command, timeout=5):
        return _rcon_command_bridge(host, port, password, command, timeout)
except ImportError:
    _rcon_command = None

i18n = I18n(MODULE_DIR)

DAEMON_API_URL = os.environ.get('DAEMON_API_URL', 'http://127.0.0.1:57474')


# ─── Constants ───────────────────────────────────────────

# Mapping: saba-chan internal key → servertest.ini key
_INI_KEY_MAP = {
    "public_name":           "PublicName",
    "public_description":    "PublicDescription",
    "password":              "Password",
    "max_players":           "MaxPlayers",
    "port":                  "DefaultPort",
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
    "rcon_port":             "RCONPort",
    "rcon_password":         "RCONPassword",
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

class IniManager:
    """Read/write Project Zomboid server INI files.
    
    PZ uses a simple key=value format (one per line), no sections.
    Comments start with # and should be preserved.
    """

    @staticmethod
    def get_ini_path(config):
        """Determine the path to {servername}.ini."""
        server_name = config.get("server_name", "servertest")
        
        # Try explicit config_dir first
        config_dir = config.get("config_dir", "")
        if config_dir and os.path.isdir(config_dir):
            return os.path.join(config_dir, f"{server_name}.ini")
        
        # Default locations
        if sys.platform == "win32":
            zomboid_dir = os.path.join(os.environ.get("USERPROFILE", ""), "Zomboid", "Server")
        else:
            zomboid_dir = os.path.join(os.path.expanduser("~"), "Zomboid", "Server")
        
        return os.path.join(zomboid_dir, f"{server_name}.ini")

    @staticmethod
    def read(ini_path):
        """Read an INI file and return {key: value} dict."""
        props = {}
        if not os.path.exists(ini_path):
            return props
        
        try:
            with open(ini_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    if '=' in line:
                        key, _, value = line.partition('=')
                        props[key.strip()] = value.strip()
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
            
            with open(ini_path, 'r', encoding='utf-8') as f:
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
            
            with open(ini_path, 'w', encoding='utf-8') as f:
                f.writelines(lines)
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
    """Resolve the server installation directory."""
    executable = config.get("executable_path", "")
    if executable and os.path.exists(executable):
        return os.path.dirname(os.path.abspath(executable))
    
    working_dir = config.get("working_dir", "")
    if working_dir and os.path.isdir(working_dir):
        return working_dir
    
    return ""


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


def _enforce_rcon_policy(config, ini_path, managed=True):
    """Enforce RCON settings based on managed/non-managed mode.
    
    - Managed mode (stdin control):  RCON is optional; disable by default
      to reduce attack surface.  stdin is used for all commands.
    - Non-managed mode (RCON only):  RCON is the sole control method;
      force enable + ensure password exists.
    
    Returns the RCON password (may be empty in managed mode).
    """
    props = IniManager.read(ini_path) if os.path.exists(ini_path) else {}
    updates = {}
    
    rcon_port = config.get("rcon_port", 27015)
    updates["RCONPort"] = str(rcon_port)
    
    if managed:
        # Managed mode: RCON not required (stdin is used for control)
        # Respect explicit user-set password; otherwise leave empty (disabled)
        rcon_password = config.get("rcon_password", "") or props.get("RCONPassword", "")
        if not rcon_password:
            updates["RCONPassword"] = ""
            print(i18n.t("messages.rcon_disabled_managed"), file=sys.stderr)
        else:
            updates["RCONPassword"] = rcon_password
    else:
        # Non-managed mode: RCON must be enabled (it's the only control method)
        rcon_password = config.get("rcon_password", "") or props.get("RCONPassword", "")
        if not rcon_password:
            rcon_password = hashlib.sha256(os.urandom(32)).hexdigest()[:24]
            print(i18n.t("messages.rcon_password_generated"), file=sys.stderr)
        updates["RCONPassword"] = rcon_password
    
    if updates:
        IniManager.update(ini_path, updates)
    
    return rcon_password


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
            "severity": "error",
            "code": "no_server_dir",
            "message": i18n.t("errors.no_server_dir"),
            "fix_hint": i18n.t("errors.solutions.no_server_dir"),
        })
        return {"success": False, "issues": issues}
    
    # Check for server executable
    executable = config.get("executable_path", "")
    if not executable or not os.path.exists(executable):
        # Try to find it automatically
        if sys.platform == "win32":
            auto_paths = [
                os.path.join(server_dir, "StartServer64.bat"),
                os.path.join(server_dir, "StartServer64_nosteam.bat"),
            ]
        else:
            auto_paths = [
                os.path.join(server_dir, "start-server.sh"),
            ]
        
        found = False
        for p in auto_paths:
            if os.path.exists(p):
                found = True
                break
        
        if not found:
            issues.append({
                "severity": "error",
                "code": "executable_not_found",
                "message": i18n.t("errors.executable_not_found"),
                "fix_hint": i18n.t("errors.solutions.executable_not_found"),
            })
    
    # Check bundled Java
    java_path = _find_java(server_dir)
    if not java_path:
        issues.append({
            "severity": "warning",
            "code": "java_not_found",
            "message": i18n.t("errors.java_not_found"),
            "fix_hint": i18n.t("errors.solutions.java_not_found"),
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
    
    # Check RCON port (TCP)
    rcon_port = int(config.get("rcon_port", 27015))
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        result = sock.connect_ex(("127.0.0.1", rcon_port))
        sock.close()
        if result == 0:
            issues.append({
                "severity": "warning",
                "code": "rcon_port_in_use",
                "message": i18n.t("errors.detect.port_in_use", port=rcon_port),
                "fix_hint": i18n.t("errors.solutions.port_in_use", port=rcon_port),
            })
    except Exception:
        pass
    
    has_errors = any(i["severity"] == "error" for i in issues)
    return {"success": not has_errors, "issues": issues}


def get_launch_command(config):
    """Build the server launch command for the Rust daemon.
    
    Project Zomboid uses a bundled JRE with specific JVM flags:
      jre64/bin/java.exe -Djava.awt.headless=true -Dzomboid.steam=1
        -Dzomboid.znetlog=1 -XX:+UseZGC ... -Xms{ram}g -Xmx{ram}g
        -Djava.library.path=natives/;natives/win64/;.
        -cp {classpath} zombie.network.GameServer
        -servername {name} -adminpassword {pass}
    """
    server_dir = _find_server_dir(config)
    if not server_dir:
        return {"success": False, "message": i18n.t("errors.no_server_dir")}
    
    java_path = _find_java(server_dir)
    if not java_path:
        return {"success": False, "message": i18n.t("errors.java_not_found")}
    
    # Memory
    ram_gb = config.get("ram", 4)
    try:
        ram_gb = int(ram_gb)
    except (ValueError, TypeError):
        ram_gb = 4
    
    # Classpath
    classpath = _resolve_classpath(server_dir)
    if not classpath:
        return {"success": False, "message": i18n.t("errors.classpath_not_found")}
    
    # Native library path
    native_libs = _get_native_libs(server_dir)
    
    # Server name
    server_name = config.get("server_name", "servertest")
    
    # Managed mode (stdin control vs RCON-only)
    is_managed = config.get("managed", True)
    
    # RCON policy enforcement
    ini_path = IniManager.get_ini_path(config)
    _enforce_rcon_policy(config, ini_path, managed=is_managed)
    
    # Build JVM args
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
    
    # No-steam mode
    if config.get("no_steam", False):
        args.append("-nosteam")
        # Also change steam flag
        args[1] = "-Dzomboid.steam=0"
    
    # Admin password (only on first launch to bypass prompt)
    admin_password = config.get("admin_password", "")
    if admin_password:
        args.extend(["-adminpassword", admin_password])
    
    # Custom cache dir for data files
    config_dir = config.get("config_dir", "")
    if config_dir:
        if sys.platform == "win32":
            args.insert(0, f"-Duser.home={config_dir}")
        else:
            args.insert(0, f"-Ddeployment.user.cachedir={config_dir}")
    
    return {
        "success": True,
        "program": java_path,
        "args": args,
        "working_dir": server_dir,
    }


def status(config):
    """Check the current server status.
    
    Uses the process PID if available, and optionally probes RCON.
    """
    pid = config.get("pid")
    
    if pid:
        try:
            if sys.platform == "win32":
                result = subprocess.run(
                    ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
                    capture_output=True, text=True, timeout=5
                )
                is_running = str(pid) in result.stdout
            else:
                os.kill(pid, 0)
                is_running = True
        except (OSError, subprocess.SubprocessError):
            is_running = False
        
        if is_running:
            # Try RCON for detailed status
            rcon_info = _try_rcon_status(config)
            if rcon_info:
                return {
                    "success": True,
                    "status": "running",
                    **rcon_info,
                }
            return {
                "success": True,
                "status": "running",
                "message": i18n.t("messages.server_starting_no_response"),
            }
    
    return {"success": True, "status": "stopped"}


def _try_rcon_status(config):
    """Try to get server info via RCON 'players' command."""
    if not _rcon_command:
        return None
    
    try:
        ini_path = IniManager.get_ini_path(config)
        props = IniManager.read(ini_path)
        rcon_port = int(props.get("RCONPort", config.get("rcon_port", 27015)))
        rcon_password = props.get("RCONPassword", config.get("rcon_password", ""))
        
        if not rcon_password:
            return None
        
        result = _rcon_command("127.0.0.1", rcon_port, rcon_password, "players")
        if result and result.get("success"):
            response = result.get("response", "")
            # Parse player list: "Players connected (X):\n-name\n-name"
            match = re.search(r"Players connected \((\d+)\)", response)
            player_count = int(match.group(1)) if match else 0
            return {
                "players_online": player_count,
                "message": i18n.t("messages.server_online"),
            }
    except Exception as e:
        print(f"RCON status check failed: {e}", file=sys.stderr)
    
    return None


# ╔═══════════════════════════════════════════════════════════╗
# ║                  Optional Functions                       ║
# ╚═══════════════════════════════════════════════════════════╝

def stop(config):
    """Stop the server gracefully.
    
    In managed mode:
      1. Send 'save' via daemon stdin IPC
      2. Wait 5 seconds
      3. Send 'quit' via daemon stdin IPC
      4. Wait for process to exit (up to 30s)
      5. Force kill if still running
    
    In non-managed mode:
      Same sequence but via RCON protocol.
    """
    pid = config.get("pid")
    force = config.get("force", False)
    instance_id = config.get("instance_id", "")
    is_managed = config.get("managed", True)
    
    if not pid:
        return {"success": True, "message": i18n.t("messages.no_process_running")}
    
    if force:
        return _force_kill(pid)
    
    # ── Managed mode: use daemon stdin IPC ────────────────────
    if is_managed and instance_id:
        try:
            _send_stdin_command(instance_id, "save")
            print(i18n.t("messages.save_before_quit"), file=sys.stderr)
            time.sleep(5)
            
            _send_stdin_command(instance_id, "quit")
            print(i18n.t("messages.shutdown_signal_sent", pid=pid), file=sys.stderr)
            
            for _ in range(30):
                time.sleep(1)
                if not _is_process_running(pid):
                    return {
                        "success": True,
                        "message": i18n.t("messages.graceful_stop", pid=pid),
                    }
            
            print(i18n.t("messages.graceful_stop_timeout"), file=sys.stderr)
            return _force_kill(pid)
        except Exception as e:
            print(f"Managed stdin stop failed: {e}", file=sys.stderr)
    
    # ── Non-managed / Fallback: use RCON ──────────────────────
    try:
        ini_path = IniManager.get_ini_path(config)
        props = IniManager.read(ini_path)
        rcon_port = int(props.get("RCONPort", config.get("rcon_port", 27015)))
        rcon_password = props.get("RCONPassword", config.get("rcon_password", ""))
        
        if rcon_password and _rcon_command:
            # Save first
            _rcon_command("127.0.0.1", rcon_port, rcon_password, "save")
            print(i18n.t("messages.save_before_quit"), file=sys.stderr)
            time.sleep(5)
            
            # Then quit
            _rcon_command("127.0.0.1", rcon_port, rcon_password, "quit")
            print(i18n.t("messages.shutdown_signal_sent", pid=pid), file=sys.stderr)
            
            # Wait for process to exit
            for _ in range(30):
                time.sleep(1)
                if not _is_process_running(pid):
                    return {
                        "success": True,
                        "message": i18n.t("messages.graceful_stop", pid=pid),
                    }
            
            # Process didn't exit, force kill
            print(i18n.t("messages.graceful_stop_timeout"), file=sys.stderr)
            return _force_kill(pid)
    except Exception as e:
        print(f"Graceful stop failed: {e}", file=sys.stderr)
    
    # Fallback: force kill
    return _force_kill(pid)


def _send_stdin_command(instance_id, cmd_text):
    """Send a command to the managed server's stdin via daemon API."""
    import urllib.request
    url = f"{DAEMON_API_URL}/api/instance/{instance_id}/stdin"
    data = json.dumps({"input": cmd_text + "\n"}).encode('utf-8')
    req = urllib.request.Request(url, data=data, method='POST',
                                headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode('utf-8'))


def _is_process_running(pid):
    """Check if a process is still running."""
    try:
        if sys.platform == "win32":
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
                capture_output=True, text=True, timeout=5
            )
            return str(pid) in result.stdout
        else:
            os.kill(pid, 0)
            return True
    except (OSError, subprocess.SubprocessError):
        return False


def _force_kill(pid):
    """Force kill a process."""
    try:
        if sys.platform == "win32":
            subprocess.run(["taskkill", "/F", "/PID", str(pid)],
                           capture_output=True, timeout=10)
        else:
            import signal
            os.kill(pid, signal.SIGKILL)
        
        return {
            "success": True,
            "message": i18n.t("messages.force_killed", pid=pid),
        }
    except Exception as e:
        return {
            "success": False,
            "message": i18n.t("errors.failed_to_kill", error=str(e)),
        }


def configure(config):
    """Apply settings from saba-chan to servertest.ini.
    
    Maps internal key names to PZ INI key names and writes them.
    """
    settings = config.get("settings", {})
    if not settings:
        return {"success": False, "message": i18n.t("errors.no_settings")}
    
    ini_path = IniManager.get_ini_path(config)
    updates = {}
    
    for key, value in settings.items():
        ini_key = _INI_KEY_MAP.get(key)
        if ini_key:
            # Convert booleans to lowercase strings
            if isinstance(value, bool):
                value = "true" if value else "false"
            updates[ini_key] = str(value)
    
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
        return {"success": True, "settings": {}, "message": "No properties found."}

    settings = {}
    for key, raw_value in raw_props.items():
        val = str(raw_value).strip()
        if val.lower() == "true":
            settings[key] = True
        elif val.lower() == "false":
            settings[key] = False
        else:
            try:
                f = float(val)
                settings[key] = int(f) if f == int(f) else f
            except (ValueError, OverflowError):
                settings[key] = val

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
    """Execute a command via stdin (managed) or RCON (non-managed).
    
    In managed mode: forwards command to server's stdin via daemon API.
    In non-managed mode: sends command via RCON protocol.
    Falls back to the other method if primary fails.
    """
    cmd_name = config.get("command_name", "")
    cmd_text = config.get("command_text", "")
    cmd_args = config.get("args", {})
    instance_id = config.get("instance_id", "")
    is_managed = config.get("managed", True)
    
    if not cmd_text and not cmd_name:
        return {"success": False, "message": "No command specified"}
    
    # Determine the actual command text
    actual_cmd = cmd_text if cmd_text else cmd_name
    
    # ── Managed mode: prefer stdin ──
    if is_managed and instance_id:
        try:
            result = _send_stdin_command(instance_id, actual_cmd)
            return {
                "success": True,
                "message": f"Command sent via stdin: {actual_cmd}",
                "data": result,
            }
        except Exception as e:
            print(f"Stdin command failed, trying RCON: {e}", file=sys.stderr)
    
    # ── Non-managed / Fallback: try RCON ──
    try:
        ini_path = IniManager.get_ini_path(config)
        props = IniManager.read(ini_path)
        rcon_port = int(props.get("RCONPort", config.get("rcon_port", 27015)))
        rcon_password = props.get("RCONPassword", config.get("rcon_password", ""))
        
        if rcon_password and _rcon_command:
            result = _rcon_command("127.0.0.1", rcon_port, rcon_password, actual_cmd)
            if result and result.get("success"):
                return {
                    "success": True,
                    "message": result.get("response", "Command executed"),
                    "data": result,
                }
    except Exception as e:
        print(f"RCON command failed: {e}", file=sys.stderr)
    
    # ── Last resort: daemon stdin IPC (if not yet tried) ──
    if not is_managed and instance_id:
        try:
            result = _send_stdin_command(instance_id, actual_cmd)
            return {"success": True, "message": f"Command sent via stdin fallback: {actual_cmd}", "data": result}
        except Exception as e:
            return {"success": False, "message": f"Failed to send command: {e}"}
    
    return {"success": False, "message": i18n.t("errors.server_not_running")}


def diagnose_log(config):
    """Analyze server log for known error patterns."""
    working_dir = config.get("working_dir", "")
    server_name = config.get("server_name", "servertest")
    
    # PZ console log locations
    log_paths = []
    if working_dir:
        log_paths.append(os.path.join(working_dir, "server-console.txt"))
    
    if sys.platform == "win32":
        zomboid_dir = os.path.join(os.environ.get("USERPROFILE", ""), "Zomboid")
    else:
        zomboid_dir = os.path.join(os.path.expanduser("~"), "Zomboid")
    
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
        return {"success": True, "issues": [], "message": "No log files found"}
    
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
            "message": i18n.t("errors.extension_not_enabled",
                              defaultValue="SteamCMD extension is not enabled. Enable it in Settings → Extensions first."),
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

        return result

    except ImportError:
        print("[Zomboid] SteamCMD extension not available, trying raw SteamCMD", file=sys.stderr)
    except Exception as e:
        print(f"[Zomboid] SteamCMD extension install failed: {e}", file=sys.stderr)
        return {"success": False, "message": f"SteamCMD install failed: {e}"}

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

            return {
                "success": True,
                "message": i18n.t("messages.install_complete"),
                "install_path": install_dir,
                "executable_path": exe if os.path.exists(exe) else "",
            }
        else:
            return {
                "success": False,
                "message": i18n.t("errors.install_failed", error=result.stderr[-500:] if result.stderr else "Unknown error"),
            }
    except subprocess.TimeoutExpired:
        return {"success": False, "message": i18n.t("errors.install_timeout")}
    except Exception as e:
        return {"success": False, "message": i18n.t("errors.install_failed", error=str(e))}


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
    
    Deletes: ~/Zomboid/Saves/Multiplayer/{servername}/
    Keeps: ~/Zomboid/Server/{servername}.ini and .lua files
    """
    server_name = config.get("server_name", "servertest")
    
    if sys.platform == "win32":
        saves_dir = os.path.join(os.environ.get("USERPROFILE", ""), "Zomboid", "Saves", "Multiplayer", server_name)
    else:
        saves_dir = os.path.join(os.path.expanduser("~"), "Zomboid", "Saves", "Multiplayer", server_name)
    
    deleted = []
    errors = []
    
    if os.path.isdir(saves_dir):
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
    "read_properties":    read_properties,
    "diagnose_log":       diagnose_log,
    "install_server":     install_server,
    "reset_server":       reset_server,
}


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
    
    if func_name not in FUNCTIONS:
        print(json.dumps({
            "success": False,
            "message": f"Unknown function: {func_name}",
            "available": list(FUNCTIONS.keys())
        }))
        sys.exit(1)
    
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
        sys.exit(1)


if __name__ == "__main__":
    main()
