"""다른 관리 도구의 단서 (모듈 메이커)
=====================================

    python maker/prior_art.py "<게임 이름>" [--egg <검색어>] [--lgsm <shortname>] [--out <파일>] [--json]
    python maker/prior_art.py "<게임 이름>" --egg-file <egg.json> --lgsm-file <_default.cfg>   (내려받지 않고)

다른 게임 서버 관리 도구가 이미 정리해 둔 값을 모아 조사의 출발점으로 쓴다:

- Pelican eggs (https://github.com/pelican-eggs, MIT): 게임 하나가 egg 파일 하나. SteamCMD app id와 익명 설치 여부,
  실행 인자, 준비 줄(`done`), 정지 방법(`^C`는 Ctrl+C), 포트, 사용자가 바꾸는 변수, 편집하는 설정 파일.
- LinuxGSM (https://github.com/GameServerManagers/LinuxGSM, MIT): 게임마다 `_default.cfg`. app id, Steam 계정 필요 여부
  (`steamuser`), 정지 방법(`stopmode`), 실행 파일, 실행 인자, 포트, 설정 파일, 세이브·로그 위치.

여기서 나온 값은 **단서**다. 조사 표(RESEARCH.md)의 근거(문서·실측)가 아니다. 두 도구 모두 리눅스·도커 기준이라
Windows 실행 파일·폴더·설치 스크립트는 다르다. 공식 문서나 실측으로 확인한 값만 조사 표에 옮긴다.
(`check.py` research 관문은 출처가 이 두 도구뿐인 줄을 막는다.) 표준 라이브러리만 쓴다.
"""

import argparse
import csv
import io
import json
import pathlib
import re
import sys
import urllib.error
import urllib.request

EGG_ORG = "pelican-eggs"
# 게임 서버 egg가 있는 저장소. 이 밖(generic, software 등)은 게임이 아니다
EGG_REPOS = ("games-steamcmd", "games-standalone", "minecraft")
EGG_BRANCH = "main"
LGSM_RAW = "https://raw.githubusercontent.com/GameServerManagers/LinuxGSM/master"
LGSM_LIST = f"{LGSM_RAW}/lgsm/data/serverlist.csv"
USER_AGENT = "saba-module-maker (https://github.com/WareAoba/saba-chan-modules)"
TIMEOUT = 20

# 조사 표 칸에 닿는 LinuxGSM 키. 알림·백업 주기 같은 LinuxGSM 자체 설정은 단서가 아니다
LGSM_KEYS = re.compile(
    r"^(appid|steamuser|steamcmdforcewindows|branch|stopmode|querymode|querytype|startparameters|executable|executabledir|"
    r"servercfg|servercfgdefault|servercfgdir|servercfgfullpath|savedir|gamelogdir|engine|glibc|consoleinteract|ip|"
    r"[a-z]*port)$"
)
# LinuxGSM `command_stop.sh` `fn_stop_graceful_select`의 stopmode 번호
LGSM_STOPMODES = {
    "1": "tmux 세션 종료 (정상 종료 없음)",
    "2": "Ctrl+C",
    "3": "콘솔 `quit` (30초)",
    "4": "콘솔 `quit` (120초)",
    "5": "콘솔 `stop`",
    "6": "콘솔 `q`",
    "7": "콘솔 `exit`",
    "8": "텔넷 `shutdown` (7 Days to Die)",
    "9": "GoldSrc 콘솔 `quit`",
    "10": "Avorion 전용 절차",
    "11": "콘솔 `end`",
    "12": "콘솔 `shutdown`",
    "13": "텔넷 `saveworld 1` → `quit 1`",
    "14": "Rising World 전용 절차",
    "15": "Rising World 전용 절차",
    "16": "전용 절차",
}
PLACEHOLDER = re.compile(r"\{\{\s*([A-Za-z0-9_]+)\s*\}\}")


def words(text):
    """비교용 낱말: 소문자, 글자·숫자 덩어리"""
    return re.findall(r"[a-z0-9]+", text.lower())


def squash(text):
    return "".join(words(text))


# ─── Pelican eggs ──────────────────────────────────────────────

def match_eggs(query, paths):
    """egg 파일 경로 중 게임 이름에 맞는 것. 같은 폴더에 Pelican판과 Pterodactyl판이 함께 있으면 Pelican판만"""
    want = squash(query)
    found = []
    for path in paths:
        name = path.rsplit("/", 1)[-1]
        if not name.startswith(("egg-", "pterodactyl-egg-")) or not name.endswith((".json", ".yaml", ".yml")):
            continue
        if want and want in squash(path.rsplit("/", 1)[0]) + squash(name):
            found.append(path)
    pelican = {p.rsplit("/", 1)[0] for p in found if "pterodactyl" not in p.rsplit("/", 1)[-1]}
    return sorted(p for p in found if "pterodactyl" not in p.rsplit("/", 1)[-1] or p.rsplit("/", 1)[0] not in pelican)


def _json_text(value):
    """egg의 config 값은 JSON을 담은 문자열이거나 이미 객체다"""
    if isinstance(value, str):
        try:
            return json.loads(value) if value.strip() else {}
        except json.JSONDecodeError:
            return {}
    return value if isinstance(value, (dict, list)) else {}


def read_egg(egg, url):
    """egg(JSON 객체) 하나에서 조사 표로 갈 단서"""
    config = egg.get("config") or {}
    startup = egg.get("startup_commands") or egg.get("startup") or []
    startup = list(startup.values()) if isinstance(startup, dict) else [startup] if startup else []
    started = _json_text(config.get("startup"))
    done = started.get("done") if isinstance(started, dict) else None
    ready = [done] if isinstance(done, str) else [d for d in done or [] if isinstance(d, str)]
    script = ((egg.get("scripts") or {}).get("installation") or {}).get("script") or ""
    variables = [v for v in egg.get("variables") or [] if isinstance(v, dict)]
    env = {v.get("env_variable", ""): str(v.get("default_value", "")) for v in variables}
    rules = {v.get("env_variable", ""): str(v.get("rules", "")) for v in variables}
    app_id = env.get("SRCDS_APPID") or env.get("STEAMCMD_APPID") or next(iter(re.findall(r"app_update\s+(\d+)", script)), None)
    # 계정이 필요한 게임은 STEAM_USER를 필수 변수로 둔다. 공용 설치 스크립트는 비어 있으면 anonymous로 로그인한다
    if "required" in rules.get("STEAM_USER", ""):
        anonymous = False
    elif re.search(r"\+?login\s+anonymous|STEAM_USER=anonymous", script):
        anonymous = True
    else:
        anonymous = None
    stop = config.get("stop")
    images = " ".join(str(i) for i in (egg.get("docker_images") or {}).values())
    files = _json_text(config.get("files"))
    return {
        "source": "egg",
        "name": egg.get("name") or "",
        "url": url,
        "app_id": app_id,
        "anonymous": anonymous,
        "startup": startup,
        "placeholders": sorted({m for line in startup for m in PLACEHOLDER.findall(line)}),
        "ready": ready,
        "stop": stop,
        "stop_signal": "interrupt" if stop == "^C" else None,
        "ports": {k: val for k, val in env.items() if "PORT" in k},
        "windows_via_wine": bool(re.search(r"\b(wine|proton)", images + " " + " ".join(startup), re.IGNORECASE)),
        "config_files": sorted(files) if isinstance(files, dict) else [],
        "variables": [(v.get("env_variable", ""), str(v.get("default_value", "")), (v.get("description") or "").strip())
                      for v in variables],
    }


# ─── LinuxGSM ─────────────────────────────────────────────────

def match_lgsm(query, csv_text):
    """serverlist.csv에서 shortname·gameservername이 같거나 게임 이름에 맞는 줄"""
    want, rows = squash(query), []
    for row in csv.DictReader(io.StringIO(csv_text)):
        names = (row.get("shortname", ""), row.get("gameservername", ""))
        if query.lower() in names or (want and want == squash(row.get("gamename", ""))):
            rows.append({k: row.get(k, "") for k in ("shortname", "gameservername", "gamename")})
    return rows


def read_lgsm_cfg(text):
    """`_default.cfg`의 `키="값"` 중 조사 표에 닿는 것"""
    keys = {}
    for line in text.splitlines():
        m = re.match(r'^([a-z][a-z0-9]*)="(.*)"(?:\s*#.*)?$', line.strip())
        if m and LGSM_KEYS.match(m.group(1)):
            keys[m.group(1)] = m.group(2)
    mode = keys.get("stopmode")
    return {
        "source": "lgsm",
        "keys": keys,
        "stop": LGSM_STOPMODES.get(mode, f"stopmode {mode}") if mode else None,
        "stop_signal": "interrupt" if mode == "2" else None,
        "needs_login": "steamuser" in keys if "appid" in keys else None,
        "windows_only": keys.get("steamcmdforcewindows") == "yes",
    }


# ─── 보고서 ───────────────────────────────────────────────────

def _cell(text):
    return str(text).replace("|", "\\|").replace("\n", " ")


def markdown(game, eggs, lgsms, missing):
    out = [
        f"# {game}: 다른 관리 도구의 단서",
        "",
        "아래 값은 **단서**다. 조사 표(`RESEARCH.md`)의 근거(문서·실측)가 아니다. 공식 문서나 실측으로 확인한 값만 조사 표에 옮기고,",
        "출처 칸에는 그 공식 URL이나 실측 근거를 쓴다(`check.py` research 관문은 출처가 이 도구들뿐인 줄을 막는다).",
        "두 도구는 리눅스·도커 기준이다. Windows 실행 파일·폴더·설치 스크립트는 공식 문서와 `inspect_dist.py`로 따로 찾는다.",
        "",
        "출처: Pelican eggs (https://github.com/pelican-eggs, MIT License), LinuxGSM (https://github.com/GameServerManagers/LinuxGSM, MIT License, Copyright (c) Daniel Gibbs).",
        "",
    ]
    for egg in eggs:
        if egg.get("unreadable"):
            out += [f"## egg: {egg['url']}", "", f"- {egg['unreadable']}", ""]
            continue
        out += [f"## egg: {egg['name']}", "", egg["url"], "", "| 단서 | 값 | 조사 표 칸 / 확인할 것 |", "|---|---|---|"]
        if egg["app_id"]:
            how = {True: "익명 설치", False: "Steam 계정 로그인 필요", None: "로그인 방식 모름"}[egg["anonymous"]]
            out.append(f"| SteamCMD app id | {egg['app_id']} ({how}) | 설치 방법 / SteamDB·공식 문서로 확인 |")
        for line in egg["startup"]:
            out.append(f"| 실행 명령 | `{_cell(line)}` | 실행 인자 / 공식 표기와 Windows 실행 파일 확인 |")
        for line in egg["ready"]:
            out.append(f"| 준비 완료 줄 | `{_cell(line)}` | 준비 완료 줄 / `probe_server.py --ready`로 원문 확인 |")
        if egg["stop"]:
            hint = "`[config] stop_signal = \"interrupt\"` 후보" if egg["stop_signal"] else "`[config] stop_command` 후보"
            out.append(f"| 정지 | `{_cell(egg['stop'])}` | 정상 종료 명령 / {hint}, 저장되는지 실측 |")
        for key, value in egg["ports"].items():
            out.append(f"| 포트 변수 | {key} = {value} | 기본 포트 / 공식 문서로 확인 |")
        if egg["windows_via_wine"]:
            out.append("| Wine·Proton | 리눅스에서 Windows 서버를 돌린다 | 공식 리눅스 서버가 없을 수 있다 / Windows 실행 파일 이름은 실행 명령 참고 |")
        for name in egg["config_files"]:
            out.append(f"| 편집하는 설정 파일 | `{_cell(name)}` | 설정 / 서버가 만든 기본 파일로 확인 |")
        for key, value, desc in egg["variables"]:
            out.append(f"| 변수 | {_cell(key)} = {_cell(value)} | 설정 후보: {_cell(desc[:120])} |")
        out.append("")
    for lgsm in lgsms:
        keys = lgsm["keys"]
        out += [f"## LinuxGSM: {lgsm.get('name', '')}", "", lgsm.get("url", ""), "", "| 단서 | 값 | 조사 표 칸 / 확인할 것 |", "|---|---|---|"]
        if "appid" in keys:
            how = "Steam 계정 로그인 필요 (`steamuser`)" if lgsm["needs_login"] else "익명 설치"
            out.append(f"| SteamCMD app id | {keys['appid']} ({how}) | 설치 방법 |")
        if lgsm["windows_only"]:
            out.append("| steamcmdforcewindows | yes | 리눅스판 서버가 없어 Windows판을 받는다 |")
        if lgsm["stop"]:
            hint = "`[config] stop_signal = \"interrupt\"` 후보" if lgsm["stop_signal"] else "정지 방법"
            out.append(f"| 정지 (stopmode {keys.get('stopmode')}) | {lgsm['stop']} | 정상 종료 명령 / {hint}, 실측 |")
        for key, value in keys.items():
            if key not in ("appid", "stopmode", "steamuser", "steamcmdforcewindows"):
                out.append(f"| {key} | `{_cell(value)}` | 실행·포트·경로 / 공식 문서로 확인 |")
        out.append("")
    if missing:
        out += ["## 찾지 못한 것", ""] + [f"- {m}" for m in missing] + [""]
    return "\n".join(out)


# ─── 내려받기 ─────────────────────────────────────────────────

def fetch(url):
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
        return response.read().decode("utf-8")


def egg_tree(repo, get=fetch):
    """저장소의 파일 경로 전체 (GitHub git trees API, 로그인 없이 한 시간 60회)"""
    data = json.loads(get(f"https://api.github.com/repos/{EGG_ORG}/{repo}/git/trees/{EGG_BRANCH}?recursive=1"))
    return [item["path"] for item in data.get("tree", []) if item.get("type") == "blob"]


def collect(game, egg_query, lgsm_query, get=fetch):
    eggs, lgsms, missing = [], [], []
    for repo in EGG_REPOS:
        try:
            paths = match_eggs(egg_query, egg_tree(repo, get))
        except (urllib.error.URLError, OSError, ValueError, KeyError) as e:
            missing.append(f"Pelican `{repo}` 목록을 읽지 못함: {e}")
            continue
        for path in paths:
            url = f"https://raw.githubusercontent.com/{EGG_ORG}/{repo}/{EGG_BRANCH}/{path}"
            if not path.endswith(".json"):
                eggs.append({"url": url, "unreadable": "YAML egg: 표준 라이브러리로 읽지 않는다. 파일을 직접 연다"})
                continue
            try:
                eggs.append(read_egg(json.loads(get(url)), url))
            except (urllib.error.URLError, OSError, ValueError) as e:
                eggs.append({"url": url, "unreadable": f"읽지 못함: {e}"})
    if not eggs:
        missing.append(f"Pelican eggs에 `{egg_query}`에 맞는 egg가 없다")
    try:
        rows = match_lgsm(lgsm_query, get(LGSM_LIST))
    except (urllib.error.URLError, OSError) as e:
        rows = []
        missing.append(f"LinuxGSM 목록을 읽지 못함: {e}")
    for row in rows:
        url = f"{LGSM_RAW}/lgsm/config-default/config-lgsm/{row['gameservername']}/_default.cfg"
        try:
            lgsms.append(dict(read_lgsm_cfg(get(url)), url=url, name=f"{row['gamename']} ({row['gameservername']})"))
        except (urllib.error.URLError, OSError) as e:
            missing.append(f"LinuxGSM `{row['gameservername']}` 설정을 읽지 못함: {e}")
    if not rows:
        missing.append(f"LinuxGSM 목록에 `{lgsm_query}`가 없다 (리눅스 공식 서버가 없는 게임일 수 있다)")
    return eggs, lgsms, missing


def main(argv=None):
    parser = argparse.ArgumentParser(description="Pelican eggs·LinuxGSM에서 조사 단서를 모은다 (출처가 아니라 단서)")
    parser.add_argument("game", help="게임 이름 (보고서 제목, 기본 검색어)")
    parser.add_argument("--egg", help="egg 경로에서 찾을 말 (기본: 게임 이름)")
    parser.add_argument("--lgsm", help="LinuxGSM shortname·gameservername이나 게임 이름 (기본: 게임 이름)")
    parser.add_argument("--egg-file", action="append", default=[], help="내려받아 둔 egg JSON (여러 번). 주면 내려받지 않는다")
    parser.add_argument("--lgsm-file", action="append", default=[], help="내려받아 둔 LinuxGSM _default.cfg (여러 번)")
    parser.add_argument("--json", action="store_true", help="보고서 대신 JSON")
    parser.add_argument("--out", help="보고서를 쓸 파일 (기본: 화면)")
    args = parser.parse_args(argv)

    if args.egg_file or args.lgsm_file:
        eggs = [read_egg(json.loads(pathlib.Path(f).read_text(encoding="utf-8")), str(f)) for f in args.egg_file]
        lgsms = [dict(read_lgsm_cfg(pathlib.Path(f).read_text(encoding="utf-8")), url=str(f), name=pathlib.Path(f).parent.name)
                 for f in args.lgsm_file]
        missing = []
    else:
        eggs, lgsms, missing = collect(args.game, args.egg or args.game, args.lgsm or args.game)

    if args.json:
        text = json.dumps({"eggs": eggs, "linuxgsm": lgsms, "missing": missing}, ensure_ascii=False, indent=2)
    else:
        text = markdown(args.game, eggs, lgsms, missing)
    if args.out:
        pathlib.Path(args.out).write_text(text + "\n", encoding="utf-8")
        print(f"[prior_art] {args.out}: egg {len(eggs)}개, LinuxGSM {len(lgsms)}개, 못 찾은 것 {len(missing)}개")
    else:
        sys.stdout.write(text + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
