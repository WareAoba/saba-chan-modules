#!/usr/bin/env python3
"""
saba-chan — Terraria Module Lifecycle

Terraria 공식 데디케이티드 서버(TerrariaServer)를 다룬다.
  - 설치: terraria.org 공식 zip(`/api/get/dedicated-servers-names`, `/api/download/pc-dedicated-server/<파일>`)
  - 설정: 작업 폴더의 serverconfig.txt (`key=value`, `#` 주석), 실행은 `-config serverconfig.txt`
  - 명령: 서버 콘솔(stdin). 게임에 REST·RCON이 없다
  - 월드: 인스턴스마다 `world_dir/<world_name>.wld`로 고정한다(공용 Documents 폴더로 되돌아가지 않게)
조사 근거는 RESEARCH.md.
"""

import json
import os
import re
import shutil
import sys
import tempfile
import zipfile

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))

from i18n import I18n

from daemon_rcon import (PathOutside, atomic_write, coerce_setting, config_value, daemon_stdin_command,
                         force_kill, is_process_alive, load_setting_types, safe_join, wait_for_exit)

i18n = I18n(MODULE_DIR)


# ─── Constants ───────────────────────────────────────────

CONFIG_FILE = "serverconfig.txt"
DEFAULT_WORLD_NAME = "world"
DEFAULT_PORT = 7777
DEFAULT_BANLIST = "banlist.txt"
DEFAULT_AUTOCREATE = "2"
STOP_COMMAND = "exit"

SERVER_NAMES_URL = "https://terraria.org/api/get/dedicated-servers-names"
DOWNLOAD_URL = "https://terraria.org/api/download/pc-dedicated-server/{name}"

# 설정 이름 → serverconfig.txt 키 (configure·import_settings·read_properties·install_server가 함께 쓴다)
_JOURNEY_KEYS = [
    "journeypermission_time_setfrozen",
    "journeypermission_time_setdawn",
    "journeypermission_time_setnoon",
    "journeypermission_time_setdusk",
    "journeypermission_time_setmidnight",
    "journeypermission_godmode",
    "journeypermission_wind_setstrength",
    "journeypermission_rain_setstrength",
    "journeypermission_time_setspeed",
    "journeypermission_rain_setfrozen",
    "journeypermission_wind_setfrozen",
    "journeypermission_increaseplacementrange",
    "journeypermission_setdifficulty",
    "journeypermission_biomespread_setfrozen",
    "journeypermission_setspawnrate",
]
_KEY_MAP = {
    "world_name": "worldname",
    "world_dir": "worldpath",
    "autocreate": "autocreate",
    "seed": "seed",
    "difficulty": "difficulty",
    "max_players": "maxplayers",
    "port": "port",
    "password": "password",
    "motd": "motd",
    "banlist": "banlist",
    "secure": "secure",
    "language": "language",
    "upnp": "upnp",
    "npcstream": "npcstream",
    "worldrollbackstokeep": "worldrollbackstokeep",
    "slowliquids": "slowliquids",
    "seed_celebration": "seed_celebration",
    "seed_theconstant": "seed_theconstant",
    "seed_notthebees": "seed_notthebees",
    "seed_notraps": "seed_notraps",
    "seed_fortheworthy": "seed_fortheworthy",
    "seed_remix": "seed_remix",
    "seed_drunk": "seed_drunk",
    "seed_zenith": "seed_zenith",
    "priority": "priority",
    **{key: key for key in _JOURNEY_KEYS},
}
_KEY_MAP_REV = {v: k for k, v in _KEY_MAP.items()}
# 위키: secure·upnp는 "1 to enable"
_BOOL_KEYS = {"secure", "upnp", "slowliquids", "seed_celebration", "seed_theconstant", "seed_notthebees",
              "seed_notraps", "seed_fortheworthy", "seed_remix", "seed_drunk", "seed_zenith"}

# 실행 인자로만 들어가는 설정(group = "saba-chan"): (설정 이름, 인자 모양)
_LAUNCH_ARGS = [
    ("ip", ["-ip", "{value}"]),
    ("steam", ["-steam"]),
    ("lobby", ["-lobby", "{value}"]),
    ("disable_announcement_box", ["-disableannouncementbox"]),
    ("announcement_box_range", ["-announcementboxrange", "{value}"]),
]

# 이름 조각 검사: 빈 값·구분자·드라이브·Windows가 잘라 내는 끝 공백/점("."·".." 포함)
_BAD_NAME_CHARS = set('/\\:*?"<>|')


# ─── Helpers ─────────────────────────────────────────────

def _progress(percent, message):
    """데몬이 읽는 진행률 줄 (stderr `PROGRESS:{json}`)"""
    print("PROGRESS:" + json.dumps({"percent": percent, "message": message}, ensure_ascii=True),
          file=sys.stderr, flush=True)


def _server_exe_name():
    """이 OS의 공식 서버 실행 파일 이름 (zip의 Windows/Linux 폴더)"""
    return "TerrariaServer.exe" if os.name == "nt" else "TerrariaServer.bin.x86_64"


def _is_windows():
    return os.name == "nt"


def _working_dir(config):
    working_dir = config.get("working_dir") or ""
    if not working_dir and config.get("server_executable"):
        working_dir = os.path.dirname(config["server_executable"])
    return working_dir


def _executable(config):
    """데몬이 준 server_executable, 없으면 작업 폴더의 이 OS 실행 파일"""
    exe = config.get("server_executable") or ""
    if exe and os.path.isfile(exe):
        return exe
    working_dir = _working_dir(config)
    if working_dir:
        candidate = os.path.join(working_dir, _server_exe_name())
        if os.path.isfile(candidate):
            return candidate
    return ""


def _check_name(name):
    """경로 조각이 되는 이름이 안전한가 (한 단계, 구분자·드라이브·끝 공백/점 없음)"""
    return bool(name) and not (set(name) & _BAD_NAME_CHARS) and name[-1] not in " ." and name.strip() == name


def _world(config):
    """(월드 폴더, 월드 이름, 월드 파일). 모든 경로 계산이 이 함수 하나를 쓴다.
    이름이 안전하지 않으면 ValueError, 폴더 밖이면 PathOutside"""
    working_dir = _working_dir(config)
    world_dir = config_value(config, "world_dir", "") or os.path.join(working_dir, "Worlds")
    name = str(config_value(config, "world_name", "") or DEFAULT_WORLD_NAME)
    if not _check_name(name):
        raise ValueError(i18n.t("errors.invalid_world_name", name=name))
    world_file = str(safe_join(world_dir, f"{name}.wld"))
    return world_dir, name, world_file


def _banlist_path(config):
    name = str(config_value(config, "banlist", "") or DEFAULT_BANLIST)
    if not _check_name(name):
        raise ValueError(i18n.t("errors.invalid_banlist", name=name))
    return str(safe_join(_working_dir(config), name))


def _config_path(config):
    return os.path.join(_working_dir(config), CONFIG_FILE)


def _format_value(key, value):
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    text = str(value)
    if "\n" in text or "\r" in text:
        raise ValueError(i18n.t("errors.line_break", setting=key))
    return text


# ─── serverconfig.txt ────────────────────────────────────

def _read_lines(path):
    """파일의 줄, 줄바꿈, 인코딩. 없으면 빈 UTF-8 파일로 취급한다.
    (못 읽은 파일을 빈 설정으로 보고 덮어쓰지 않는다)"""
    try:
        with open(path, "rb") as f:
            raw = f.read()
    except FileNotFoundError:
        return [], "\n", "utf-8"
    encoding = "utf-8-sig" if raw.startswith(b"\xef\xbb\xbf") else "utf-8"
    try:
        text = raw.decode(encoding)
    except UnicodeDecodeError:
        encoding = "cp1252"
        text = raw.decode(encoding)
    newline = "\r\n" if "\r\n" in text else "\n"
    return text.splitlines(), newline, encoding


def _parse_line(line):
    """`key=value` → (소문자 키, 값). 주석(#)·빈 줄·`=` 없는 줄은 None. 값은 첫 `=` 뒤 전부"""
    stripped = line.strip()
    if not stripped or stripped.startswith("#") or "=" not in stripped:
        return None
    key, value = line.split("=", 1)
    return key.strip().lower(), value


def _read_config_file(path):
    """serverconfig.txt → {키: 값}. 같은 키가 여러 번이면 첫 줄 (쓰기도 첫 줄만 남긴다)"""
    lines, _, _ = _read_lines(path)
    props = {}
    for line in lines:
        parsed = _parse_line(line)
        if parsed and parsed[0] not in props:
            props[parsed[0]] = parsed[1]
    return props


def _update_config_file(path, updates):
    """바뀐 키만 고친다. 주석·다른 키·순서·줄바꿈 표기는 그대로. 값이 None·""이면 그 키 줄을 지운다(게임 기본값).
    같은 키의 둘째 줄부터는 지운다(어느 줄을 읽을지 애매하지 않게)"""
    lines, newline, encoding = _read_lines(path)
    pending = {key: (None if value in (None, "") else _format_value(key, value)) for key, value in updates.items()}
    out, seen = [], set()
    for line in lines:
        parsed = _parse_line(line)
        if parsed and parsed[0] in pending:
            key = parsed[0]
            if key in seen or pending[key] is None:
                seen.add(key)
                continue
            seen.add(key)
            out.append(f"{key}={pending[key]}")
        else:
            out.append(line)
    for key, value in pending.items():
        if key not in seen and value is not None:
            out.append(f"{key}={value}")
    data = (newline.join(out) + newline) if out else ""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    atomic_write(path, data.encode(encoding))


def _file_updates(settings):
    """설정 이름 → serverconfig 키 값 (표에 없는 키는 뺀다)"""
    return {_KEY_MAP[k]: v for k, v in (settings or {}).items() if k in _KEY_MAP}


def _settings_from_file(props):
    """serverconfig 값 → 설정 이름과 화면 형식. world=가 있으면 월드 폴더·이름은 그 경로에서 얻는다"""
    types = load_setting_types(MODULE_DIR)
    settings = {}
    for key, raw in props.items():
        name = _KEY_MAP_REV.get(key)
        if not name:
            continue
        if key in _BOOL_KEYS:
            if raw.strip() in ("0", "1"):
                settings[name] = raw.strip() == "1"
            continue
        settings[name] = coerce_setting(raw, types.get(name))
    world = (props.get("world") or "").strip()
    if world:
        settings["world_dir"] = os.path.dirname(world)
        base = os.path.basename(world)
        settings["world_name"] = base[:-4] if base.lower().endswith(".wld") else base
    return settings


# ╔═══════════════════════════════════════════════════════════╗
# ║                  Lifecycle Functions                      ║
# ╚═══════════════════════════════════════════════════════════╝

def validate(config):
    """시작 전 점검: 실행 파일, 월드 이름, 밴 목록 이름"""
    issues = []
    if not _executable(config):
        issues.append({"severity": "critical", "code": "executable_not_found",
                       "message": i18n.t("errors.executable_not_found")})
    if _is_windows() and not os.path.isdir(os.path.join(
            os.environ.get("WINDIR", r"C:\Windows"), "Microsoft.NET", "assembly", "GAC_32",
            "Microsoft.Xna.Framework")):
        issues.append({"severity": "critical", "code": "xna_framework_missing",
                       "message": i18n.t("errors.xna_framework_missing")})
    try:
        _world(config)
    except PathOutside:
        raise
    except ValueError as e:
        issues.append({"severity": "critical", "code": "invalid_world_name", "message": str(e)})
    try:
        _banlist_path(config)
    except PathOutside:
        raise
    except ValueError as e:
        issues.append({"severity": "critical", "code": "invalid_banlist", "message": str(e)})
    return {"success": not any(i["severity"] == "critical" for i in issues), "issues": issues}


def _launch_args(config):
    args = ["-config", CONFIG_FILE]
    steam = bool(config_value(config, "steam", False))
    for name, shape in _LAUNCH_ARGS:
        value = config_value(config, name, None)
        if value in (None, "", False):
            continue
        if name == "lobby" and not steam:
            # 위키: -lobby는 -steam이 있어야 한다
            continue
        if "{value}" in " ".join(shape):
            text = _format_value(name, value)
            args += [part.replace("{value}", text) for part in shape]
        else:
            args += shape
    return args


def get_launch_command(config):
    """`TerrariaServer -config serverconfig.txt`.
    월드를 고르는 대화형 질문이 뜨지 않게, 그리고 월드가 공용 Documents 폴더로 가지 않게
    시작 전에 serverconfig.txt의 world·worldpath·worldname·autocreate·port를 이 인스턴스 값으로 맞춘다(preview면 쓰지 않는다)."""
    exe = _executable(config)
    if not exe:
        return {"success": False, "action_required": "executable_not_found",
                "message": i18n.t("errors.executable_not_found")}
    working_dir = _working_dir(config) or os.path.dirname(exe)
    try:
        world_dir, name, world_file = _world(config)
        args = _launch_args(config)
    except PathOutside:
        raise
    except ValueError as e:
        return {"success": False, "message": str(e)}

    if not config.get("preview"):
        updates = {
            "world": world_file,
            "worldpath": world_dir,
            "worldname": name,
            "autocreate": config_value(config, "autocreate", "") or DEFAULT_AUTOCREATE,
            "port": config_value(config, "port", DEFAULT_PORT),
        }
        try:
            os.makedirs(world_dir, exist_ok=True)
            _update_config_file(_config_path(config), updates)
        except PathOutside:
            raise
        except (OSError, UnicodeDecodeError, ValueError) as e:
            return {"success": False, "message": i18n.t("errors.properties_write_failed", error=str(e))}

    env = {}
    if os.name != "nt":
        # 위키: Linux 서버는 TERM이 xterm이 아니면 시작하지 못한다
        env["TERM"] = "xterm"
    return {"success": True, "program": exe, "args": args, "working_dir": working_dir, "env_vars": env}


def status(config):
    """non-managed 상태: 데몬이 준 PID가 살아 있는가"""
    pid = config.get("pid")
    if pid and is_process_alive(pid):
        return {"success": True, "status": "running"}
    return {"success": True, "status": "stopped"}


# 데몬이 호출 제한을 알려 주지 않으면(0.1.8 데몬, 모듈 호출 120초) 강제 종료까지 그 안에 끝나도록 90초에서 멈춘다
STOP_TIMEOUT_MAX = 90
STOP_CALL_MARGIN = 30


def _stop_timeout(config):
    try:
        value = int(config.get("stop_timeout") or 60)
    except (TypeError, ValueError):
        value = 60
    cap = STOP_TIMEOUT_MAX
    try:
        cap = int(config["stop_call_limit"]) - STOP_CALL_MARGIN
    except (KeyError, TypeError, ValueError):
        pass
    return max(1, min(value, cap))


def _kill(pid):
    ok, error = force_kill(pid)
    if ok:
        return {"success": True, "message": i18n.t("messages.force_killed", pid=pid)}
    return {"success": False, "message": i18n.t("errors.failed_to_kill", error=error)}


def stop(config):
    """정지. 공식 `exit`는 월드를 저장하고 끝낸다(위키), 그래서 앞에 save를 붙이지 않는다.
    managed: 요청만 보내고 반환(대기·강제 종료는 데몬). non-managed: stop_timeout만큼 기다린 뒤 강제 종료.
    콘솔이 없으면 저장 없이 죽이지 않고 graceful_stop_unavailable (SIGTERM이 저장하는지는 공식 근거가 없어 쓰지 않는다)."""
    pid = config.get("pid")
    if config.get("force"):
        if not pid:
            return {"success": True, "message": i18n.t("messages.no_process_running")}
        return _kill(pid)
    try:
        daemon_stdin_command(config.get("instance_id", ""), STOP_COMMAND)
    except PathOutside:
        raise
    except Exception as e:
        print(f"[Terraria] console stop unavailable: {e}", file=sys.stderr)
        return {"success": False, "error_code": "graceful_stop_unavailable",
                "message": i18n.t("errors.graceful_stop_unavailable")}
    if config.get("managed") or not pid:
        return {"success": True, "message": i18n.t("messages.stop_requested")}
    if wait_for_exit(pid, timeout=_stop_timeout(config)):
        return {"success": True, "message": i18n.t("messages.graceful_stop")}
    print(i18n.t("messages.graceful_stop_timeout"), file=sys.stderr)
    return _kill(pid)


def configure(config):
    """설정 화면 → serverconfig.txt (바뀐 키만). 월드 이름·폴더가 바뀌면 world= 경로도 같이 맞춘다"""
    settings = config.get("settings") or {}
    updates = _file_updates(settings)
    try:
        if "world_name" in settings or "world_dir" in settings:
            _, _, world_file = _world({**config, **settings})
            updates["world"] = world_file
        if "banlist" in settings and settings["banlist"]:
            _banlist_path({**config, **settings})
        if not updates:
            return {"success": True, "updated_keys": [], "message": i18n.t("messages.no_mappable_settings")}
        _update_config_file(_config_path(config), updates)
    except PathOutside:
        raise
    except (OSError, UnicodeDecodeError, ValueError) as e:
        return {"success": False, "message": i18n.t("errors.properties_write_failed", error=str(e))}
    return {"success": True, "updated_keys": sorted(updates), "message": i18n.t("messages.properties_updated")}


def read_properties(config):
    """serverconfig.txt → 설정 이름. 파일이 없으면 빈 값(게임이 기본 파일을 만들지 않는다)"""
    try:
        props = _read_config_file(_config_path(config))
    except (OSError, UnicodeDecodeError) as e:
        return {"success": False, "message": i18n.t("errors.properties_read_failed", error=str(e))}
    return {"success": True, "properties": _settings_from_file(props)}


def import_settings(config):
    """기존 서버 가져오기·live 설정: serverconfig.txt 값과 찾은 실행 파일"""
    result = read_properties(config)
    if not result.get("success"):
        return result
    out = {"success": True, "settings": result["properties"]}
    exe = _executable({"working_dir": _working_dir(config)})
    if exe:
        out["executable_path"] = os.path.basename(exe)
    return out


def backup_paths(config):
    """이 월드 파일, serverconfig.txt, 밴 목록만. 게임이 만든 굴림 백업, 다른 월드, 실행 파일은 뺀다"""
    try:
        _, _, world_file = _world(config)
        banlist = _banlist_path(config)
    except PathOutside:
        raise
    except ValueError as e:
        return {"success": False, "message": i18n.t("errors.backup_paths_failed", error=str(e))}
    return {"success": True, "paths": [world_file, _config_path(config), banlist]}


def reset_server(config):
    """월드 초기화: 이 월드의 `<이름>.wld`와 게임이 만든 `<이름>.wld.bak[숫자]`를 지운다.
    설정·밴 목록·다른 월드·사용자 파일은 남는다. 다음 시작 때 autocreate로 새 월드가 생긴다."""
    try:
        world_dir, name, world_file = _world(config)
    except PathOutside:
        raise
    except ValueError as e:
        return {"success": False, "message": str(e)}
    # 서버 폴더가 아니면(실행 파일이 없으면) 지우지 않는다
    if not _executable(config):
        return {"success": False, "message": i18n.t("errors.not_server_folder")}
    backup_re = re.compile(re.escape(name) + r"\.wld\.bak\d*$")
    try:
        backups = [filename for filename in os.listdir(world_dir) if backup_re.fullmatch(filename)
                   and os.path.isfile(os.path.join(world_dir, filename))]
    except FileNotFoundError:
        backups = []
    except OSError as e:
        return {"success": False, "message": i18n.t(
            "errors.server_reset_partial", errors=f"{name}.wld.bak*: {e}")}
    targets = [world_file] + [str(safe_join(world_dir, filename)) for filename in backups]
    deleted, errors = [], []
    for target in targets:
        if os.path.dirname(os.path.abspath(target)) != os.path.abspath(world_dir):
            return {"success": False, "message": i18n.t("errors.invalid_world_name", name=name)}
        try:
            os.remove(target)
            deleted.append(target)
        except FileNotFoundError:
            pass
        except OSError as e:
            errors.append(f"{os.path.basename(target)}: {e}")
    if errors:
        return {"success": False, "message": i18n.t("errors.server_reset_partial", errors=", ".join(errors))}
    return {"success": True, "message": i18n.t("messages.server_reset_complete"), "deleted": deleted}


# ─── 버전·설치 ───────────────────────────────────────────

_NAME_RE = re.compile(r"^terraria-server-(\d+)\.zip$")
_VERSION_RE = re.compile(r"^\d+(\.\d+)*$")


def _fetch_server_names():
    import urllib.request
    req = urllib.request.Request(SERVER_NAMES_URL, headers={"User-Agent": "saba-chan"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            names = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        raise ConnectionError(str(e))
    if not isinstance(names, list):
        raise ConnectionError("unexpected answer")
    return [n for n in names if isinstance(n, str)]


def _version_of(file_name):
    """`terraria-server-1458.zip` → `1.4.5.8` (공식 파일 이름은 버전의 점을 뺀 숫자다)"""
    match = _NAME_RE.match(file_name)
    return ".".join(match.group(1)) if match else None


def list_versions(config):
    """terraria.org 공식 목록의 서버 버전 (새것부터)"""
    try:
        names = _fetch_server_names()
    except ConnectionError as e:
        return {"success": False, "message": i18n.t("errors.version_list_failed", error=str(e))}
    versions = []
    for name in names:
        version = _version_of(name)
        if version and version not in versions:
            versions.append(version)
    versions.sort(key=lambda v: [int(p) for p in v.split(".")], reverse=True)
    items = [{"id": v, "type": "release"} for v in versions]
    return {"success": True, "versions": items, "latest": {"release": versions[0] if versions else None},
            "total": len(items), "page": 1, "per_page": len(items), "total_pages": 1}


def _download(url, dest):
    import urllib.request
    req = urllib.request.Request(url, headers={"User-Agent": "saba-chan"})
    with urllib.request.urlopen(req, timeout=60) as resp, open(dest, "wb") as out:
        total = int(resp.headers.get("Content-Length") or 0)
        done = 0
        while True:
            chunk = resp.read(1 << 20)
            if not chunk:
                break
            out.write(chunk)
            done += len(chunk)
            if total:
                _progress(5 + int(done * 75 / total), i18n.t("messages.downloading"))


def _extract_server(archive, install_dir):
    """zip에서 이 OS 서버 실행 파일이 든 폴더만 install_dir에 푼다. 기존 serverconfig.txt는 덮지 않는다.
    zip에 체크섬이 없으므로(공식 제공 없음) CRC 검사와 실행 파일 존재로 확인한다"""
    exe_name = _server_exe_name()
    platform_dir = "Windows" if exe_name.endswith(".exe") else "Linux"
    with zipfile.ZipFile(archive) as z:
        bad = z.testzip()
        if bad is not None:
            raise ValueError(i18n.t("errors.archive_corrupt", file=bad))
        entries = [i for i in z.infolist() if not i.is_dir()]
        exes = [i.filename for i in entries if i.filename.rsplit("/", 1)[-1] == exe_name
                and i.filename.rsplit("/", 1)[0].rsplit("/", 1)[-1] == platform_dir]
        if len(exes) != 1:
            raise ValueError(i18n.t("errors.server_not_in_archive", file=exe_name))
        prefix = exes[0].rsplit("/", 1)[0] + "/"
        members = [i for i in entries if i.filename.startswith(prefix)]
        # 먼저 모두 검사하고 나서 쓴다: 바깥으로 나가는 항목이 하나라도 있으면 아무것도 풀지 않는다
        planned = [(i, safe_join(install_dir, *i.filename[len(prefix):].split("/"))) for i in members]
        for info, dest in planned:
            rel = info.filename[len(prefix):]
            if rel == CONFIG_FILE and dest.exists():
                continue
            dest.parent.mkdir(parents=True, exist_ok=True)
            with z.open(info) as src, open(dest, "wb") as out:
                shutil.copyfileobj(src, out)
    exe = os.path.join(install_dir, exe_name)
    if os.name != "nt":
        os.chmod(exe, 0o755)
    return exe


def install_server(config):
    """공식 zip을 받아 이 OS 서버 폴더를 install_dir에 푼다. 월드·밴 목록·기존 설정은 건드리지 않는다.
    인스턴스 설정(settings)은 serverconfig.txt에 처음 값으로 쓴다"""
    version = str(config.get("version") or "").strip()
    install_dir = config.get("install_dir") or ""
    if not _VERSION_RE.match(version):
        return {"success": False, "message": i18n.t("errors.no_version")}
    if not install_dir:
        return {"success": False, "message": i18n.t("errors.no_install_dir")}
    url = DOWNLOAD_URL.format(name=f"terraria-server-{version.replace('.', '')}.zip")
    os.makedirs(install_dir, exist_ok=True)
    fd, archive = tempfile.mkstemp(prefix=".terraria-server-", suffix=".download", dir=install_dir)
    os.close(fd)
    try:
        _progress(5, i18n.t("messages.downloading"))
        try:
            _download(url, archive)
        except Exception as e:
            return {"success": False, "message": i18n.t("errors.download_failed", error=str(e))}
        _progress(85, i18n.t("messages.extracting"))
        try:
            exe = _extract_server(archive, install_dir)
        except PathOutside:
            raise
        except (zipfile.BadZipFile, ValueError, OSError) as e:
            return {"success": False, "message": i18n.t("errors.install_failed", error=str(e))}
    finally:
        try:
            os.remove(archive)
        except OSError:
            pass

    settings = config.get("settings") or {}
    updates = _file_updates(settings)
    if updates:
        try:
            _update_config_file(os.path.join(install_dir, CONFIG_FILE), updates)
        except PathOutside:
            raise
        except (OSError, UnicodeDecodeError, ValueError) as e:
            return {"success": False, "error_code": "settings_write_failed",
                    "message": i18n.t("errors.settings_write_failed", error=str(e))}
    _progress(100, i18n.t("messages.install_complete"))
    return {"success": True, "version": version, "executable_path": exe, "install_path": install_dir,
            "message": i18n.t("messages.install_complete")}


# ╔═══════════════════════════════════════════════════════════╗
# ║                  Function Registry                        ║
# ╚═══════════════════════════════════════════════════════════╝

FUNCTIONS = {
    "validate":           validate,
    "get_launch_command": get_launch_command,
    "status":             status,
    "stop":               stop,
    "configure":          configure,
    "read_properties":    read_properties,
    "import_settings":    import_settings,
    "backup_paths":       backup_paths,
    "reset_server":       reset_server,
    "list_versions":      list_versions,
    "install_server":     install_server,
}
# 데몬이 지원 기능을 묻는 예약 함수: 표의 함수 이름을 돌려준다
FUNCTIONS["functions"] = lambda _config: {"success": True, "functions": [f for f in FUNCTIONS if f != "functions"]}


if __name__ == "__main__":
    from daemon_rcon import run_lifecycle
    run_lifecycle(FUNCTIONS)
