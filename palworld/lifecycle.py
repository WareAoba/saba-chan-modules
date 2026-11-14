#!/usr/bin/env python3
"""
Palworld server lifecycle management module.
Always outputs JSON to stdout, logs to stderr.
"""

import sys
import os
import re
from i18n import I18n

import daemon_rcon
from daemon_rcon import (PathOutside, chat_reply, coerce_setting, config_value, daemon_rest, enforce_remote_access, extension_required,
                         format_setting, graceful_stop, list_dir, load_setting_types, remote_access_values, safe_join, setting_file_keys)


# UE4 INI parser (ue4-ini extension). Without it the module still answers;
# functions that touch PalWorldSettings.ini return extension_required.
try:
    from extensions.ue4_ini import parse_option_settings, write_option_settings
except ImportError:
    parse_option_settings = write_option_settings = None

# Initialize i18n
MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
i18n = I18n(MODULE_DIR)


class RestError(RuntimeError):
    """REST 요청 실패. 데몬이 거절한 이유(error_code)가 있으면 지닌다"""

    def __init__(self, message, error_code=None):
        super().__init__(message)
        self.error_code = error_code


class PalworldRestClient:
    """인스턴스의 REST 요청을 데몬에 맡긴다 (주소·비밀번호는 데몬이 붙인다). 이 모듈의 REST 호출은 모두 여기를 지난다"""

    def __init__(self, instance_id, timeout=5):
        self.instance_id = instance_id
        self.timeout = timeout

    def call(self, method, path, payload=None):
        """데몬의 답 전체. 요청을 보내지 못했거나 서버가 거절하면 RestError"""
        try:
            result = daemon_rest(self.instance_id, method, f"/v1/api{path}", body=payload, timeout=self.timeout)
        except ConnectionError as e:
            raise RestError(str(e), getattr(e, "error_code", None)) from e
        if not result.get("success", True):
            raise RestError(result.get("message") or result.get("error") or i18n.t("errors.daemon_error", error="REST"),
                            result.get("error_code"))
        return result

    def _get(self, path):
        return (self.call("GET", path).get("data") or {}).get("response")

    def players(self):
        return self._get("/players")

    def metrics(self):
        return self._get("/metrics")

    def save(self):
        return self.call("POST", "/save", {})

    def shutdown(self, message):
        return self.call("POST", "/shutdown", {"waittime": 1, "message": message})

    def announce(self, message):
        return self.call("POST", "/announce", {"message": message})

    def kick(self, userid, message):
        return self.call("POST", "/kick", {"userid": userid, "message": message})

    def ban(self, userid, message):
        return self.call("POST", "/ban", {"userid": userid, "message": message})

    def unban(self, userid):
        return self.call("POST", "/unban", {"userid": userid})


def _exact_player_id(players, player_input):
    """name(캐릭터 이름)이나 accountName(Steam 계정 이름)이 정확히 같은 사람의 ID (대소문자 무시)"""
    player_input_lower = player_input.lower()
    for player in players:
        player_name = player.get("name", "")
        account_name = player.get("accountName", "")
        if player_name.lower() == player_input_lower or account_name.lower() == player_input_lower:
            player_userid = player.get("userId") or player.get("playerId") or player.get("steamId")
            matched_name = account_name if account_name.lower() == player_input_lower else player_name
            print(f"[Palworld] Found player: {matched_name} (char: {player_name}) -> {player_userid}", file=sys.stderr)
            return player_userid
    return None


def resolve_player_id(instance_id, player_input, config, allow_partial=True):
    """
    플레이어 입력값을 SteamID로 변환합니다.
    - steam_ 로 시작하면 그대로 반환
    - 숫자만 있으면 이름이 정확히 같은 사람을 먼저 찾고, 없으면(목록을 못 읽어도) ID로 보고 그대로 반환
    - 닉네임이면 플레이어 목록에서 검색하여 SteamID로 변환
    - allow_partial=False 면 이름이 정확히 같은 사람만 찾는다 (내보내기·차단이 엉뚱한 사람에게 가지 않게)
    
    Returns:
        tuple: (success, steamid_or_error_message)
    """
    if player_input.startswith("steam_"):
        print(f"[Palworld] Input '{player_input}' is already a SteamID format", file=sys.stderr)
        return (True, player_input)

    # 숫자만 있는 입력은 닉네임(예: 12345)일 수도 있으니 정확히 같은 이름을 먼저 찾는다
    if player_input.isdigit():
        try:
            matched = _exact_player_id(PalworldRestClient(instance_id).players().get("players", []), player_input)
        except PathOutside:
            raise
        except Exception as e:
            print(f"[Palworld] Player list unavailable for '{player_input}': {e}", file=sys.stderr)
            matched = None
        if matched:
            return (True, matched)
        print(f"[Palworld] Input '{player_input}' treated as an ID", file=sys.stderr)
        return (True, player_input)
    
    # 닉네임으로 간주하고 플레이어 목록에서 검색
    print(f"[Palworld] Resolving nickname '{player_input}' to SteamID...", file=sys.stderr)
    
    try:
        result = PalworldRestClient(instance_id).players()
        players = result.get("players", [])
            
        print(f"[Palworld] Found {len(players)} online players", file=sys.stderr)
            
        matched = _exact_player_id(players, player_input)
        if matched:
            return (True, matched)

        player_input_lower = player_input.lower()
        if not allow_partial:
            return (False, i18n.t("errors.player_not_found"))

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
            pw = props.get("AdminPassword", "").strip()
            if pw:
                return pw
    except PathOutside:
        raise
    except Exception as e:
        print(f"[Palworld] Warning: failed to read AdminPassword from INI: {e}", file=sys.stderr)
    return None

def _enforce_rest_policy(working_dir, config=None):
    """실행 전 PalWorldSettings.ini를 사바쨩이 조작할 수 있게 맞춘다.

    - 원격 접속 값(REST 켜기, RCON 끄기)은 module.toml `[remote_access]` (공용 enforce_remote_access)
    - RESTAPIPort는 데몬이 접속할 포트(설정 rest_port)
    - AdminPassword(REST 인증 비밀번호)가 비어 있으면 설정 admin_password, 그것도 없으면 새로 만든다
    - ServerPassword(접속 비밀번호)는 건드리지 않는다

    바뀐 것과 실제 AdminPassword를 돌려준다. INI가 아직 없으면 None
    """
    config = config or {}
    ini_path = _get_settings_ini_path({"working_dir": working_dir})
    if not ini_path or not os.path.isfile(ini_path):
        return None  # INI will be generated on first run

    if _option_line_unreadable(ini_path):
        # 손으로 고친 줄이 깨졌다: 덮어쓰면 사용자 값이 사라지므로 손대지 않고 시작을 막는다
        print("[Palworld] OptionSettings line in PalWorldSettings.ini is unreadable, leaving it untouched", file=sys.stderr)
        return {"changed": False, "unreadable": True}

    props = _parse_option_settings(ini_path)
    changed = False

    # INI 파일이 존재하지만 비어있는 경우(빈 파일/파싱 실패) → 기본 설정으로 채움
    if not props:
        print("[Palworld] INI file is empty/corrupt, populating with defaults", file=sys.stderr)
        props = _default_settings()
        changed = True

    if enforce_remote_access(MODULE_DIR, config, props, props.update):
        changed = True

    # 데몬이 접속할 REST 포트(설정 rest_port, 자동 배정될 수 있다)를 INI에 맞춘다
    rest_port = config_value(config, "rest_port")
    if rest_port and props.get("RESTAPIPort") != str(rest_port):
        props["RESTAPIPort"] = str(rest_port)
        changed = True

    # 데몬은 시작할 때 "비밀번호 자동 생성"이 켜져 있으면 빈 admin_password만 채우고, 꺼져 있으면 비워 둔다.
    # 그래서 config에 없을 수 있다: 그때는 INI 값을 쓰거나 여기서 만든다 (get_launch_command가 데몬에 돌려준다)
    ini_admin_pw = current_admin_pw = props.get("AdminPassword", "").strip()
    if not current_admin_pw:
        current_admin_pw = config_value(config, "admin_password")
        if current_admin_pw:
            print("[Palworld] Synced AdminPassword from config", file=sys.stderr)
        else:
            import uuid
            current_admin_pw = uuid.uuid4().hex[:16]
            print("[Palworld] No password from daemon or INI — generated fallback AdminPassword", file=sys.stderr)
        props["AdminPassword"] = current_admin_pw
        changed = True

    if changed:
        if not _write_option_settings(ini_path, props):
            # 저장하지 못했다: INI에 실제로 있는 비밀번호만 데몬에 넘긴다
            print("[Palworld] Failed to write REST policy to PalWorldSettings.ini", file=sys.stderr)
            return {"changed": False, "write_failed": True, "admin_password": ini_admin_pw}
        return {"changed": True, "admin_password": current_admin_pw}
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
    if rest_result and rest_result.get("unreadable"):
        return {"success": False, "error_code": "settings_unreadable",
                "message": i18n.t("errors.settings_unreadable")}

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

    # 실제로 쓰는 REST 비밀번호를 데몬에 돌려준다: 데몬이 module.toml `[protocols.rest] password_setting`에 적는다
    if rest_result and rest_result.get("admin_password"):
        result["rest_credentials"] = {"rest_password": rest_result["admin_password"]}

    return result


def _rest_client(config):
    """데몬의 인스턴스 문맥이 있으면 REST 클라이언트를 만든다."""
    instance_id = config.get("instance_id")
    if not instance_id and not os.environ.get("SABA_HOST_DIR"):
        return None
    return PalworldRestClient(instance_id)


def _int_or_none(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def player_count(config):
    """접속자 수 (module.toml [status] players). 데몬이 켜진 서버에 1분마다 부른다.
    1분 갱신(players_detail 없음)에는 REST를 부르지 않는다: 부를 때마다 서버 로그에 `REST accessed endpoint` 줄이 남는다.
    그때 수는 데몬이 접속·퇴장 줄로 센다(player_join/leave_patterns). 최대 인원만 알려 준다"""
    if not config.get("players_detail"):
        max_players = _int_or_none(config_value(config, "max_players", None))
        return {"success": True, "max": max_players} if max_players else {"success": True}
    client = _rest_client(config)
    if client is None:
        return {"success": False, "message": i18n.t("errors.player_list_failed", error="AdminPassword")}
    try:
        metrics = client.metrics()
        result = {"success": True, "online": int(metrics["currentplayernum"]), "max": int(metrics.get("maxplayernum") or 0)}
    except (RuntimeError, KeyError, TypeError, ValueError) as e:
        return {"success": False, "message": i18n.t("errors.player_list_failed", error=str(e))}
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
        client.save()
    except RuntimeError as e:
        print(f"[Palworld] REST save before shutdown failed: {e}", file=sys.stderr)
    try:
        client.shutdown(i18n.t("messages.shutdown_notice"))
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
        # 사용자 이름은 module.toml `[protocols.rest] username`에 있다
        return {"success": True, "rest_password": _read_admin_password_from_ini(config) or ""}
    except PathOutside:
        raise
    except Exception as e:
        return {"success": False, "message": str(e)}


# ╔═══════════════════════════════════════════════════════════╗
# ║            PalWorldSettings.ini Manager                   ║
# ╚═══════════════════════════════════════════════════════════╝

def _default_settings():
    """게임이 주는 공식 기본값 (모듈에 실린 DefaultPalWorldSettings.ini). 기본값은 이 파일 한 곳에만 둔다"""
    return _parse_option_settings(os.path.join(MODULE_DIR, "DefaultPalWorldSettings.ini"))

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


def _option_line_unreadable(ini_path):
    """파일에 OptionSettings 값이 있는데 읽지 못했는지(닫는 괄호 빠짐·잘림 등).
    파서는 이때도 빈 dict를 돌려주므로, 빈 파일·헤더만 있는 파일과 가려 사용자 값을 기본값으로 덮지 않는다"""
    if not os.path.isfile(ini_path) or _parse_option_settings(ini_path):
        return False
    with open(ini_path, "rb") as f:
        raw = f.read()
    text = raw.decode("utf-16" if raw.startswith((b"\xff\xfe", b"\xfe\xff")) else "latin-1", errors="replace")
    return any(re.fullmatch(r"\s*OptionSettings\s*=\s*(?!\(\s*\)\s*$)\S.*", line) for line in text.splitlines())


def _write_option_settings(ini_path, props):
    if write_option_settings is None:
        raise ExtensionMissing("ue4-ini")
    # 값 모양이 아니라 module.toml 필드 종류로 따옴표를 정한다: 숫자뿐인 비밀번호도 문자열, 열거형은 따옴표 없이
    keys, types = setting_file_keys(MODULE_DIR), load_setting_types(MODULE_DIR)
    quoted = {k for name, k in keys.items() if types.get(name) in ("text", "password")} - set(_LIST_KEYS)
    bare = {k for name, k in keys.items() if types.get(name) == "select"} | set(_LIST_KEYS)
    return write_option_settings(ini_path, props, quoted_keys=quoted, bare_keys=bare)


# ╔═══════════════════════════════════════════════════════════╗
# ║          Additional Lifecycle Functions                    ║
# ╚═══════════════════════════════════════════════════════════╝

def validate(config):
    """Validate prerequisites before starting Palworld server."""
    issues = []
    executable = config.get("server_executable")

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
    "CrossplayPlatforms": "",  # 플랫폼 이름은 열거형이라 따옴표 없이: (Steam,Xbox,PS5,Mac)
}


# 목록 항목(기술·플랫폼 이름)에 쓸 수 있는 글자. 목록은 따옴표 없이 OptionSettings 줄에 들어가므로
# `,` `=` `(` `)` `"`가 들어가면 AdminPassword 같은 다른 키를 만들어 낸다
_LIST_ITEM = re.compile(r"[A-Za-z0-9_.\-]+")


def _to_ini_list(value, quote):
    """"a, b" → ("a","b"). 괄호 목록으로 적어도 항목을 다시 검사한다. 빈 값은 빈 값.
    항목에 허용하지 않는 글자가 있으면 ValueError"""
    text = str(value).strip()
    if text.startswith("(") and text.endswith(")"):
        text = text[1:-1].strip()
    if not text:
        return text
    items = [item.strip().strip('"') for item in text.split(",")]
    items = [item for item in items if item]
    if not all(_LIST_ITEM.fullmatch(item) for item in items):
        raise ValueError("invalid list item")
    return "(" + ",".join(f"{quote}{item}{quote}" for item in items) + ")"


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
        # 줄바꿈이 든 값은 거절한다(ValueError): 다른 키를 만들어 내거나 파일을 깨뜨린다
        value = format_setting(value, true="True", false="False", name=key)
        if ini_key in _LIST_KEYS:
            value = _to_ini_list(value, _LIST_KEYS[ini_key])
        props[ini_key] = value
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
    if _option_line_unreadable(ini_path):
        return {"success": False, "error_code": "settings_unreadable",
                "message": i18n.t("errors.settings_unreadable")}
    props = _parse_option_settings(ini_path)
    try:
        updated_keys = _apply_settings(props, settings)
    except ValueError:
        return {"success": False, "error_code": "settings_invalid_value",
                "message": i18n.t("errors.settings_invalid_value")}

    # 원격 접속 값은 사바쨩이 정한다(module.toml [remote_access]): 손으로 바꾼 INI라도 저장할 때 되돌린다
    props.update(remote_access_values(MODULE_DIR, config))
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

    types = load_setting_types(MODULE_DIR)
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

    ok = _write_option_settings(ini_path, _default_settings())
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


def reset_preview(config):
    """초기화 확인 창에 보일 것 (데몬 `GET /server/reset`): 설정 파일 하나를 기본값으로 되돌리고 월드는 남는다"""
    ini_path = _get_settings_ini_path(config)
    if not ini_path:
        return {"success": False, "message": i18n.t("reset.no_path")}
    item = {"label": i18n.t("reset.items.settings"), "paths": [ini_path]}
    return {"success": True, "scopes": [{"id": "settings", "items": [item], "note": i18n.t("reset.description")}]}


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
    새것부터. 스냅샷 안의 항목(Level.sav, Players/ …)은 월드 폴더의 같은 이름 자리로 돌아간다.
    `folder`는 사바쨩에서 지울 때 없앨 그 스냅샷 폴더다"""
    saves = safe_join(config.get("working_dir") or "", "Pal", "Saved", "SaveGames")
    try:
        snapshots = []
        for slot in list_dir(saves):
            slot_path = safe_join(saves, slot)
            if not os.path.isdir(slot_path):
                continue
            for world in list_dir(slot_path):
                world_path = safe_join(slot_path, world)
                kept = safe_join(world_path, "backup", "world")
                if not os.path.isdir(kept):
                    continue
                for stamp in list_dir(kept):
                    snap = safe_join(kept, stamp)
                    if not os.path.isdir(snap):
                        continue
                    snapshots.append({
                        "id": f"{slot}/{world}/{stamp}",
                        "created": _snapshot_time(stamp, snap),
                        "folder": str(snap),
                        "paths": [{"from": str(safe_join(snap, name)), "to": str(safe_join(world_path, name))}
                                  for name in list_dir(snap)],
                    })
        # 게임이 적은 시각 순 (CLI·GUI의 "가장 최근"과 데몬의 기본 선택이 같은 것을 고른다)
        snapshots.sort(key=lambda s: s["created"], reverse=True)
        return {"success": True, "snapshots": snapshots}
    except OSError as e:
        return {"success": False, "message": i18n.t("errors.backup_paths_failed", error=str(e))}


def _save_entries(saves):
    paths = []
    for slot in list_dir(saves):
        slot_path = safe_join(saves, slot)
        if not os.path.isdir(slot_path):
            paths.append(str(slot_path))
            continue
        for world in list_dir(slot_path):
            world_path = safe_join(slot_path, world)
            if not os.path.isdir(world_path):
                paths.append(str(world_path))
                continue
            paths.extend(str(safe_join(world_path, name)) for name in list_dir(world_path)
                         if name.lower() != "backup")
    return paths



def diagnose_log(config):
    """서버 로그 진단. 규칙은 module.toml [[diagnose]], 콘솔 줄이 없을 때 읽을 파일은 [launch] diagnose_files"""
    return daemon_rcon.diagnose_log(config, MODULE_DIR, i18n)


def install_server(config):
    """Install Palworld dedicated server via the SteamCMD extension (app_id는 module.toml [install]).

    익스텐션이 없거나 꺼져 있으면(import 가드) `extension_required`로 답해 데몬이 켜기를 안내한다.
    """
    install_dir = config.get("install_dir", "")
    if not install_dir:
        return {"success": False, "message": i18n.t("errors.no_install_dir")}

    os.makedirs(install_dir, exist_ok=True)

    try:
        from extensions.steamcmd import install_for_module
    except ImportError:
        return extension_required(i18n, "steamcmd")
    try:
        result = install_for_module(MODULE_DIR, install_dir)
    except PathOutside:
        raise
    except Exception as e:
        print(f"[Palworld] SteamCMD install failed: {e}", file=sys.stderr)
        return {"success": False, "message": i18n.t("errors.install_failed", error=str(e))}

    if result.get("success"):
        exe_name = "PalServer.exe" if sys.platform == "win32" else "PalServer.sh"
        exe_path = str(safe_join(install_dir, exe_name))
        result["install_path"] = install_dir
        result["executable_path"] = exe_path if os.path.exists(exe_path) else ""

        # ── 프로비저닝 마지막 단계: 기본 설정으로 PalWorldSettings.ini 생성 ──
        # 서버를 한 번도 실행하지 않으면 INI가 비어있어 REST API가 활성화되지 않음.
        # 기본 값 + 사바쨩이 정하는 원격 접속 값(module.toml [remote_access])을 미리 기록한다.
        try:
            ini_path = _get_settings_ini_path({"working_dir": install_dir})
            os.makedirs(os.path.dirname(ini_path), exist_ok=True)

            # 업데이트도 install_server를 다시 부른다: 사용자가 채운 INI는 그대로 둔다
            if os.path.isfile(ini_path) and (_parse_option_settings(ini_path) or _option_line_unreadable(ini_path)):
                print("[Palworld] Provisioning: keeping existing PalWorldSettings.ini", file=sys.stderr)
            else:
                defaults = _default_settings()
                # 만들 때 고른 설정(이름·비밀번호·인원·포트 등)을 처음 값으로
                _apply_settings(defaults, config.get("settings"))
                defaults.update(remote_access_values(MODULE_DIR, config))
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


def command(config):
    """method = "module" 명령(kick·ban·unban)을 REST로 실행한다.
    method = "rest" 명령은 module.toml 정의대로 데몬이 직접 보내므로 여기 오지 않는다"""
    try:
        command_text = config.get("command")
        args = config.get("args", {})
        if not command_text:
            return {"success": False, "message": i18n.t("errors.no_command")}
        client = _rest_client(config)
        if client is None:
            return {"success": False, "message": i18n.t("errors.no_instance_id")}

        command_lower = command_text.lower()

        # 플레이어 ID 변환 (kick, ban, unban 명령어의 경우)
        if command_lower in ["kick", "ban", "unban"]:
            player_input = args.get("userid") or args.get("player_id") or args.get("steam_id") or args.get("name") or args.get("player")
            if player_input:
                success, result = resolve_player_id(client.instance_id, player_input, config, allow_partial=command_lower == "unban")
                if success:
                    args = dict(args)  # 원본 수정 방지
                    args["userid"] = result
                elif command_lower != "unban":
                    # unban은 오프라인일 수 있으므로 실패해도 진행
                    return {"success": False, "message": result}
                else:
                    print(f"[Palworld] Player lookup failed for unban, using original: {player_input}", file=sys.stderr)

        return execute_command_via_rest(client, command_lower, args)

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


def execute_command_via_rest(client, command_lower, args):
    """kick·ban·unban을 보내고 데몬의 답에 결과 문구를 붙인다. 사유를 주지 않으면 기본 사유(locales)"""
    userid = args.get("userid")
    if command_lower not in ("kick", "ban", "unban"):
        return {"success": False, "message": i18n.t("errors.unknown_command", command=command_lower)}
    if not userid:
        return {"success": False, "message": i18n.t("errors.userid_required")}
    try:
        if command_lower == "kick":
            result = client.kick(userid, args.get("message") or i18n.t("messages.kick_reason"))
        elif command_lower == "ban":
            result = client.ban(userid, args.get("message") or i18n.t("messages.ban_reason"))
        else:
            result = client.unban(userid)
    except RestError as e:
        print(f"[Palworld] REST {command_lower} failed: {e}", file=sys.stderr)
        result = {"success": False, "message": i18n.t("errors.command_failed", error=str(e))}
        if e.error_code:
            result["error_code"] = e.error_code
        return result
    return {**result, "success": True, "protocol": "rest", "message": i18n.t("messages.command_executed", command=command_lower)}


def send_chat(config):
    """디스코드 → 게임 채팅 (`[extensions.saba-core] chat_send`). 콘솔 입력이 없어 REST 공지로 이름·본문을 알린다"""
    message = f"{config.get('chat_name', '')}: {config.get('chat_text', '')}"
    return chat_reply(lambda: PalworldRestClient(config.get("instance_id", "")).announce(message), i18n)


# Function dispatch table (matches daemon's expected interface)
FUNCTIONS = {
    "stop": stop,
    "command": command,
    "send_chat": send_chat,
    "get_launch_command": get_launch_command,
    "get_credentials": get_credentials,
    "validate": validate,
    "configure": configure,
    "read_properties": read_properties,
    "import_settings": import_settings,
    "config_files": config_files,
    "reset_properties": reset_properties,
    "reset_server": reset_server,
    "reset_preview": reset_preview,
    "diagnose_log": diagnose_log,
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
            return extension_required(i18n, e.extension)
    return call


if __name__ == "__main__":
    # 공용 진입점: 깨진 stdin·모르는 함수·함수 안의 예외도 JSON으로 답하고 종료 코드 0을 지킨다
    from daemon_rcon import run_lifecycle
    run_lifecycle({name: _answering_missing_extension(fn) for name, fn in FUNCTIONS.items()})
