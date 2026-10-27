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

from daemon_rcon import (PathOutside, config_value, daemon_stdin_command, download, graceful_stop,
                         is_process_alive, progress, read_key_values, request_console, safe_extract, safe_join,
                         setting_file_keys, settings_from_file, update_key_values)

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
# 서버 파일의 `playing` 답 마지막 줄(CLI.NoPlayers·OnePlayerConnected·PlayersConnected, 1.4.5.8 TerrariaServer.exe 12개 언어).
# 0명·1명 문구는 숫자를 그대로 적는다
_PLAYING_COUNT = [
    ("No players connected.", "1 player connected.", "{0} players connected."),
    ("Keine Spieler verbunden.", "1 Spieler verbunden.", "{0} Spieler verbunden."),
    ("Nessun giocatore connesso.", "1 giocatore connesso.", "{0} giocatori connessi."),
    ("Aucun joueur connecté.", "1 joueur connecté.", "{0} joueurs connectés."),
    ("No hay jugadores conectados.", "1 jugador conectado.", "{0} jugadores conectados."),
    ("Нет подключенных игроков.", "Подключён 1 игрок.", "Подключено игроков: {0}."),
    ("无玩家连接。", "1个玩家已连接。", "{0}个玩家已连接。"),
    ("Nenhum jogador conectado.", "1 jogador conectado.", "{0} jogadores conectados."),
    ("Nie przyłączyli się żadni gracze.", "Przyłączył się 1 gracz.", "Przyłączyła się następująca liczba graczy: {0}."),
    ("接続中のプレイヤーはいません。", "1人のプレイヤーが接続中です。", "{0}人のプレイヤーが接続中です。"),
    ("접속 중인 플레이어가 없음.", "플레이어 1명 접속 중.", "플레이어 {0}명 접속 중."),
    ("沒有玩家連線。", "已與 1 名玩家連線。", "已與 {0} 名玩家連線。"),
]
# 플레이어마다 한 줄 "<이름> (<주소>)"
_PLAYING_LINE_RE = re.compile(r"^(.+) \(([^()]+)\)$")
# 줄 앞 서버 프롬프트(줄바꿈 없이 찍힌 ": ", 의사 콘솔은 ":")
_PROMPT_RE = re.compile(r"^:(?: |$)")

# 이름 조각 검사: 빈 값·구분자·드라이브·Windows가 잘라 내는 끝 공백/점("."·".." 포함)
_BAD_NAME_CHARS = set('/\\:*?"<>|')


# ─── Helpers ─────────────────────────────────────────────

def _server_exe_name():
    """이 OS의 공식 서버 실행 파일 이름 (zip의 Windows/Linux 폴더)"""
    return "TerrariaServer.exe" if os.name == "nt" else "TerrariaServer.bin.x86_64"


def _is_windows():
    return os.name == "nt"


def _xna():
    """XNA 익스텐션(Windows 서버의 실행 전제). 없거나 꺼져 있으면 None (데몬이 import를 막는다)"""
    try:
        import extensions.xna as xna
    except ImportError:
        return None
    return xna


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
    if not os.path.isabs(world_dir):
        # 가져온 serverconfig.txt의 상대 경로는 서버 폴더 기준이다 (모듈 실행 폴더가 아니라)
        world_dir = os.path.join(working_dir, world_dir)
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

# 공식 예시 파일은 cp1252지만 서버는 UTF-8로 읽는다(PC 실측). cp1252로 못 쓰는 값(한국어 MOTD·경로)이면 UTF-8로 쓴다.
# 키는 대소문자를 가리지 않는다
_FILE_FORMAT = {"encodings": ("utf-8", "cp1252"), "case_insensitive": True}


def _read_config_file(path):
    """serverconfig.txt → {소문자 키: 값}. 같은 키가 여러 번이면 첫 줄 (쓰기도 첫 줄만 남긴다)"""
    return read_key_values(path, first_wins=True, **_FILE_FORMAT)


def _update_config_file(path, updates):
    """바뀐 키만 고친다. 주석·다른 키·순서·줄바꿈 표기는 그대로. 값이 None·""이면 그 키 줄을 지운다(게임 기본값)"""
    # 있는 폴더에는 makedirs를 부르지 않는다: 위 폴더를 보지 못하는 격리 실행(드라이브 바로 아래 서버 폴더)에서 실패한다
    folder = os.path.dirname(path) or "."
    if not os.path.isdir(folder):
        os.makedirs(folder, exist_ok=True)
    values = {key: value if value is None or value == "" else _format_value(key, value) for key, value in updates.items()}
    update_key_values(path, values, delete_empty=True, **_FILE_FORMAT)


def _file_updates(settings):
    """설정 이름 → serverconfig 키 값 (module.toml file_key가 없는 설정은 뺀다)"""
    keys = setting_file_keys(MODULE_DIR)
    return {keys[k]: v for k, v in (settings or {}).items() if k in keys}


def _settings_from_file(props):
    """serverconfig 값 → 설정 이름과 화면 형식(위키: 켜기·끄기는 1·0). world=가 있으면 월드 폴더·이름은 그 경로에서 얻는다"""
    settings = settings_from_file(MODULE_DIR, props, true="1", false="0", case_insensitive=True)
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
    if _is_windows():
        xna = _xna()
        if xna is None:
            issues.append({"severity": "critical", "code": "extension_required",
                           "message": i18n.t("errors.xna_extension_missing")})
        elif not xna.present(_working_dir(config)):
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


def _launch_arg_shapes():
    """실행 인자로만 들어가는 설정(group = "saba-chan")의 인자 모양: [(설정 이름, 토큰)]. module.toml `arg`가 원본이다"""
    import tomllib
    with open(os.path.join(MODULE_DIR, "module.toml"), "rb") as f:
        fields = tomllib.loads(f.read().decode("utf-8-sig")).get("settings", {}).get("fields", [])
    shapes = []
    for field in fields:
        arg = field.get("arg")
        if field.get("group") == "saba-chan" and arg:
            shapes.append((field["name"], arg.split() if isinstance(arg, str) else list(arg)))
    return shapes


def _launch_args(config):
    args = ["-config", CONFIG_FILE]
    steam = bool(config_value(config, "steam", False))
    for name, shape in _launch_arg_shapes():
        value = config_value(config, name, None)
        # 0은 값이다(announcement_box_range = 0). 꺼진 플래그만 뺀다
        if value is None or value is False or value == "":
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


def stop(config):
    """정지. 공식 `exit`는 월드를 저장하고 끝낸다(위키), 그래서 앞에 save를 붙이지 않는다.
    기다림·강제 종료·managed·요청 불가는 공용 절차(graceful_stop)가 맡는다.
    콘솔이 없으면 저장 없이 죽이지 않고 graceful_stop_unavailable (SIGTERM이 저장하는지는 공식 근거가 없어 쓰지 않는다)."""
    return graceful_stop(config, lambda: request_console(config, STOP_COMMAND), i18n, module_dir=MODULE_DIR)


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


def config_files(config):
    """사바쨩이 고치는 게임 설정 파일 (가져온 서버를 해제할 때 데몬이 가져오기 전 내용으로 되돌린다)"""
    if not _working_dir(config):
        return {"success": False, "message": i18n.t("errors.no_working_dir")}
    return {"success": True, "files": [os.path.abspath(_config_path(config))]}


def _int_or_none(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _playing_count(line):
    for none, one, many in _PLAYING_COUNT:
        if line == none:
            return 0
        if line == one:
            return 1
        head, tail = many.split("{0}")
        if line.startswith(head) and line.endswith(tail) and line[len(head):len(line) - len(tail)].isdigit():
            return int(line[len(head):len(line) - len(tail)])
    return None


def player_count(config):
    """접속자 (module.toml [status] players). 게임에 조회 API가 없어 목록은 콘솔 `playing`으로 읽는다.
    1분 갱신(players_detail 없음)에는 콘솔에 찍히지 않게 묻지 않는다. 그때 수는 데몬이 접속·퇴장 줄로 센다(0.1.11)"""
    max_players = _int_or_none(config_value(config, "max_players", None))
    if not config.get("players_detail"):
        # 수는 데몬이 접속·퇴장 줄로 센다(player_join/leave_patterns). 최대 인원만 알려 준다
        return {"success": True, "max": max_players} if max_players else {"success": True}
    try:
        reply = daemon_stdin_command(config.get("instance_id", ""), "playing", timeout=5)
    except PathOutside:
        raise
    except Exception as e:
        print(f"[Terraria] playing failed: {e}", file=sys.stderr)
        return {"success": False, "message": i18n.t("errors.players_unavailable")}
    response = ((reply or {}).get("data") or {}).get("response") or ""
    names = []
    for raw in response.splitlines():
        line = _PROMPT_RE.sub("", raw.strip()).strip()
        if line == "playing":
            # 명령 메아리 앞의 줄은 이 답이 아니다
            names = []
            continue
        count = _playing_count(line)
        if count is not None:
            result = {"success": True, "online": count}
            if max_players:
                result["max"] = max_players
            if len(names) == count:
                result["players"] = [{"name": n} for n in names]
            return result
        match = _PLAYING_LINE_RE.match(line)
        if match:
            names.append(match.group(1))
    return {"success": False, "message": i18n.t("errors.players_unavailable")}


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
    message = i18n.t("messages.downloading")
    download(url, dest, on_progress=lambda done, total: total and progress(5 + int(done * 75 / total), message))


def _extract_server(archive, install_dir):
    """zip에서 이 OS 서버 실행 파일이 든 폴더만 install_dir에 푼다(공용 safe_extract). 기존 serverconfig.txt는 덮지 않는다.
    zip에 체크섬이 없으므로(공식 제공 없음) CRC 검사와 실행 파일 존재로 확인한다"""
    exe_name = _server_exe_name()
    platform_dir = "Windows" if exe_name.endswith(".exe") else "Linux"
    with zipfile.ZipFile(archive) as z:
        names = [i.filename for i in z.infolist() if not i.is_dir()]
    exes = [n for n in names if n.rsplit("/", 1)[-1] == exe_name and n.rsplit("/", 1)[0].rsplit("/", 1)[-1] == platform_dir]
    if len(exes) != 1:
        raise ValueError(i18n.t("errors.server_not_in_archive", file=exe_name))
    safe_extract(archive, install_dir, prefix=exes[0].rsplit("/", 1)[0], keep_existing={CONFIG_FILE})
    exe = os.path.join(install_dir, exe_name)
    if os.name != "nt":
        os.chmod(exe, 0o755)
    return exe


_CHANGELOG_RE = re.compile(r"^Version (\d+(?:\.\d+)+) Changes")


def get_installed_version(config):
    """설치된 서버 버전: 공식 zip의 changelog.txt 첫 줄 "Version 1.4.5.8 Changes" (실측, Windows zip)"""
    try:
        with open(os.path.join(_working_dir(config), "changelog.txt"), "rb") as f:
            first = f.readline().decode("utf-8", "replace")
    except OSError:
        first = ""
    match = _CHANGELOG_RE.match(first)
    if not match:
        return {"success": False, "message": i18n.t("errors.version_unknown")}
    return {"success": True, "version": match.group(1)}


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
        progress(5, i18n.t("messages.downloading"))
        try:
            _download(url, archive)
        except Exception as e:
            return {"success": False, "message": i18n.t("errors.download_failed", error=str(e))}
        progress(85, i18n.t("messages.extracting"))
        try:
            exe = _extract_server(archive, install_dir)
        except PathOutside:
            raise
        except zipfile.BadZipFile:
            return {"success": False, "message": i18n.t("errors.archive_corrupt", file=url.rsplit("/", 1)[-1])}
        except (ValueError, OSError) as e:
            return {"success": False, "message": i18n.t("errors.install_failed", error=str(e))}
    finally:
        try:
            os.remove(archive)
        except OSError:
            pass

    if _is_windows():
        xna = _xna()
        if xna is None:
            return {"success": False, "error_code": "extension_required", "missing_extensions": ["xna"],
                    "message": i18n.t("errors.xna_extension_missing")}
        progress(90, i18n.t("messages.installing_xna"))
        try:
            xna.install(install_dir)
        except xna.XnaError as e:
            return {"success": False, "error_code": "xna_install_failed",
                    "message": i18n.t("errors.xna_install_failed", error=str(e))}

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
    progress(100, i18n.t("messages.install_complete"))
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
    "config_files":       config_files,
    "player_count":       player_count,
    "reset_server":       reset_server,
    "list_versions":      list_versions,
    "install_server":     install_server,
    "get_installed_version": get_installed_version,
}


if __name__ == "__main__":
    from daemon_rcon import run_lifecycle
    run_lifecycle(FUNCTIONS)
