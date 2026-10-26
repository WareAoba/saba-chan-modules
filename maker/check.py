"""PR 전 관문 검사 (모듈 메이커)
=============================

    python maker/check.py <모듈 폴더> [--saba-core <saba-core>] [--shared-libs <saba-chan/modules>] [--base origin/main]
        [--static-only] [--trial [--accept-agreement <id>]... [--extensions <저장소>]] [--out <보고서.md>]

`maker/SKILL.md`의 PR 전 관문 중 기계로 잴 수 있는 것을 한 번에 돌리고 표로 보고한다.

- 정적 관문: 틀 자리표시가 남았는지, 조사 표에 출처 URL이 있는지, 10개 언어 파일과 한국어 번역, 모듈 테스트 등록,
  버전을 올리지 않았는지(새 모듈은 0.1.0, 있던 모듈은 `--base`와 같은 버전)
- 도구 관문: 모듈 테스트(`pytest <모듈>`), 적합성 키트(`pytest conformance --module`), saba-core가 이 메이커의 요구
  버전(`SKILL.md` metadata)을 맞추는지(compat), `saba-core module validate`, `saba-core module audit`,
  `--trial`이면 실측(`conformance/trial.py`)

사바쨩이 설치돼 있으면 `--saba-core`와 공용 런타임 경로를 `saba-chan-cli module maker --json`에서 얻는다.
보고서 머리에 메이커 버전을 적고, 깃허브에 새 `maker-v*` 태그가 있으면 알린다.

조사·실물 확인·PC 격리 실측처럼 사람이나 에이전트가 판단할 관문은 "직접 확인"으로 따로 적는다. 실패가 하나라도
있으면 종료 코드 1이다.
"""

import argparse
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tomllib
import urllib.request

MAKER = pathlib.Path(__file__).resolve().parent
REPO = MAKER.parent
RELEASES = "https://api.github.com/repos/WareAoba/saba-chan-modules/tags"
LANGUAGES = ("de", "en", "es", "fr", "ja", "ko", "pt-BR", "ru", "zh-CN", "zh-TW")
PLACEHOLDERS = re.compile(r"MyGame|마이게임")
URL = re.compile(r"https?://\S+")
MANUAL = (
    "조사 표의 모든 명령·설정·포트·경로·콘솔 문구에 출처가 있고 문서·실측이 구별돼 있다",
    "배포물 항목 전체와 동봉 기본 파일의 원래 바이트를 봤다 (inspect_dist.py)",
    "서버를 파이프로 띄워 준비·정지 줄, help, 저장 뒤 파일을 기록했다 (probe_server.py)",
    "선언한 명령이 help와 한 줄씩 맞고, 모든 명령의 risk 판단 근거가 조사 표에 있다",
    "실행 전제 런타임을 고정 판으로 서버 폴더에 두고 체크섬으로 검증한다",
    "설치가 이 OS 폴더만 푼다, 설정 파일을 원래 인코딩·줄바꿈·문법으로 왕복한다",
    "초기화가 계산된 대상만 지우고 서버 폴더가 아니면 거절한다",
    "Windows PC 격리(AppContainer) 실측을 했다 (trial.py --saba-core <설치된 saba-core>)",
    "위험 패턴 표(maker/references/lessons.md)를 한 줄씩 지웠다",
)


def gate(name, status, detail=""):
    return {"gate": name, "status": status, "detail": detail}


def leaves(data, prefix=""):
    if isinstance(data, dict):
        for key, value in data.items():
            yield from leaves(value, f"{prefix}{key}.")
    else:
        yield prefix[:-1]


def same_json(path, data):
    try:
        return json.loads(path.read_text(encoding="utf-8")) == data
    except (OSError, ValueError):
        return False


def placeholders_gate(folder):
    hits = []
    for name in ("module.toml", "lifecycle.py"):
        path = folder / name
        if path.is_file():
            for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                if PLACEHOLDERS.search(line.split("#", 1)[0] if name == "module.toml" else line):
                    hits.append(f"{name}:{number}")
    return gate("placeholders", "fail" if hits else "ok", ("틀 자리표시가 남았다: " + ", ".join(hits)) if hits else "")


def research_gate(folder):
    path = folder / "RESEARCH.md"
    if not path.is_file():
        return gate("research", "fail", "RESEARCH.md가 없다 (조사 표를 PR 본문에 둔다면 이 관문은 직접 확인)")
    urls = URL.findall(path.read_text(encoding="utf-8"))
    return gate("research", "ok" if urls else "fail", f"출처 URL {len(urls)}개" if urls else "조사 표에 출처 URL이 없다")


def locales_gate(folder):
    problems = []
    locales = folder / "locales"
    missing = [f"{lang}.json" for lang in LANGUAGES if not (locales / f"{lang}.json").is_file()]
    if missing:
        problems.append("없는 언어 파일: " + ", ".join(missing))
    try:
        en = json.loads((locales / "en.json").read_text(encoding="utf-8"))
        ko = json.loads((locales / "ko.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        problems.append(f"en·ko를 읽지 못했다: {e}")
    else:
        untranslated = sorted(set(leaves(en)) - set(leaves(ko)))
        if untranslated:
            problems.append("ko에 없는 키: " + ", ".join(untranslated))
        # new_module.py는 틀에 없는 언어를 en으로 채운다. 그대로 두면 번역하지 않은 것이다
        copies = [f"{lang}.json" for lang in LANGUAGES if lang not in ("en", "ko") and same_json(locales / f"{lang}.json", en)]
        if copies:
            problems.append("en.json과 같은 (번역 안 된) 언어 파일: " + ", ".join(copies))
    return gate("locales", "fail" if problems else "ok", "; ".join(problems))


def tests_gate(folder, repo):
    problems = []
    if not any(folder.glob("test_*.py")):
        problems.append("모듈 테스트(test_*.py)가 없다")
    ini = repo / "pytest.ini"
    if ini.is_file() and not any(line.strip() == folder.name for line in ini.read_text(encoding="utf-8").splitlines()):
        problems.append(f"pytest.ini testpaths에 {folder.name}이 없다")
    return gate("tests-registered", "fail" if problems else "ok", "; ".join(problems))


def version_gate(folder, repo, base):
    try:
        version = tomllib.loads((folder / "module.toml").read_text(encoding="utf-8-sig"))["module"]["version"]
    except (OSError, ValueError, KeyError) as e:
        return gate("version", "fail", f"module.toml에서 버전을 읽지 못했다: {e}")
    before = None
    if base:
        found = subprocess.run(["git", "rev-parse", "--verify", "--quiet", f"{base}^{{commit}}"], cwd=repo, capture_output=True, stdin=subprocess.DEVNULL)
        if found.returncode != 0:
            return gate("version", "fail", f"비교할 기준 {base}를 찾지 못했다")
        shown = subprocess.run(["git", "show", f"{base}:{folder.relative_to(repo).as_posix()}/module.toml"],
                               cwd=repo, capture_output=True, stdin=subprocess.DEVNULL, text=True, encoding="utf-8-sig")
        if shown.returncode == 0:  # 기준에 없으면 새 모듈이다
            try:
                before = tomllib.loads(shown.stdout)["module"]["version"]
            except (ValueError, KeyError) as e:
                return gate("version", "fail", f"{base}의 module.toml에서 버전을 읽지 못했다: {e}")
    expected = before or "0.1.0"
    if version != expected:
        where = f"{base}의 {before}" if before else "새 모듈은 0.1.0"
        return gate("version", "fail", f"버전은 발행 때만 올린다: {where}인데 {version}")
    return gate("version", "ok", version)


def maker_version():
    """메이커 버전의 원본은 플러그인 매니페스트 하나다"""
    return json.loads((MAKER / ".claude-plugin" / "plugin.json").read_text(encoding="utf-8"))["version"]


def requirement():
    """SKILL.md 머리 metadata의 saba-core 요구 (예: ">=0.1.10")"""
    head = (MAKER / "SKILL.md").read_text(encoding="utf-8").split("---", 2)[1]
    found = re.search(r'^\s+saba-core:\s*"([^"]+)"', head, re.MULTILINE)
    return found.group(1) if found else None


def version_tuple(text):
    return tuple(int(n) for n in re.findall(r"\d+", text)[:3])


def compat_gate(installed, needed):
    """saba-core 버전이 이 메이커가 기대하는 계약을 맞추는지 (">=X.Y.Z"만 쓴다)"""
    if not installed:
        return gate("compat", "skip", "saba-core 버전을 알 수 없다")
    floor = needed.removeprefix(">=")
    if version_tuple(installed) < version_tuple(floor):
        return gate("compat", "fail", f"saba-core {installed}: 이 메이커는 {needed}가 필요하다. 사바쨩을 업데이트한다")
    return gate("compat", "ok", f"saba-core {installed} ({needed})")


def saba_core_version(saba_core):
    try:
        done = subprocess.run([saba_core, "--version"], capture_output=True, stdin=subprocess.DEVNULL, text=True, timeout=20)
    except (OSError, subprocess.TimeoutExpired):
        return None
    found = re.search(r"\d+\.\d+\.\d+", done.stdout)
    return found.group(0) if found else None


def cli_command():
    """이 PC의 saba-chan-cli. SABA_CHAN_CLI로 바꿀 수 있다"""
    if os.environ.get("SABA_CHAN_CLI"):
        return [os.environ["SABA_CHAN_CLI"]]
    found = shutil.which("saba-chan-cli")
    if not found and sys.platform == "win32":
        installed = pathlib.Path(os.environ.get("ProgramFiles", r"C:\Program Files")) / "Saba-chan" / "saba-chan-cli.exe"
        found = str(installed) if installed.is_file() else None
    return [found] if found else None


def app_paths(command=None):
    """`saba-chan-cli module maker --json`이 알려 주는 이 PC의 경로. 앱이 없거나 옛 판이면 None"""
    command = command or cli_command()
    if not command:
        return None
    try:
        done = subprocess.run([*command, "module", "maker", "--json"], capture_output=True, stdin=subprocess.DEVNULL,
                              text=True, encoding="utf-8", timeout=30)
        return json.loads(done.stdout) if done.returncode == 0 else None
    except (OSError, ValueError, subprocess.TimeoutExpired):
        return None


def newer_release():
    """깃허브에 이 메이커보다 새 `maker-v*` 태그가 있으면 그 버전. 확인하지 못하면 None"""
    try:
        with urllib.request.urlopen(RELEASES, timeout=3) as response:
            tags = [t["name"].removeprefix("maker-v") for t in json.load(response) if t["name"].startswith("maker-v")]
    except (OSError, ValueError, KeyError, TypeError):
        return None
    latest = max(tags, key=version_tuple, default=None)
    return latest if latest and version_tuple(latest) > version_tuple(maker_version()) else None


def static_gates(folder, repo=REPO, base=None):
    folder, repo = pathlib.Path(folder).resolve(), pathlib.Path(repo).resolve()
    return [placeholders_gate(folder), research_gate(folder), locales_gate(folder), tests_gate(folder, repo),
            version_gate(folder, repo, base)]


def run(name, command, cwd, env=None):
    done = subprocess.run(command, cwd=cwd, capture_output=True, stdin=subprocess.DEVNULL, text=True, encoding="utf-8", errors="replace", env=env)
    tail = (done.stdout + done.stderr).strip().splitlines()[-1:] or [""]
    return gate(name, "ok" if done.returncode == 0 else "fail", tail[0][:300])


def tool_gates(folder, repo, args):
    python = sys.executable
    # 저장소 밖의 모듈에는 conftest가 공용 런타임(i18n.py, daemon_rcon.py)을 넣지 않으므로 PYTHONPATH로 준다
    # 공용 런타임: 지정한 것 → SABA_RUNTIME_DIR → 옆에 받은 saba-chan 저장소 → 설치된 사바쨩(앱이 알려 준 경로)
    sibling = REPO.parent / "saba-chan" / "modules"
    shared = (args.shared_libs or os.environ.get("SABA_RUNTIME_DIR")
              or (str(sibling) if (sibling / "i18n.py").is_file() else None) or args.app_shared_libs or str(sibling))
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(filter(None, [shared, os.environ.get("PYTHONPATH")]))}
    results = [run("module-tests", [python, "-m", "pytest", "-q", str(folder)], repo, env)]
    conformance = [python, "-m", "pytest", "-q", "conformance", "--module", str(folder)]
    if os.path.isfile(os.path.join(shared, "i18n.py")):
        conformance += ["--shared-libs", shared]
    results.append(run("conformance", conformance, repo, env))
    if args.saba_core:
        # audit이 `python`을 찾으므로 이 Python을 PATH 앞에 둔다
        env = {**env, "PATH": os.path.dirname(python) + os.pathsep + os.environ.get("PATH", "")}
        results.append(compat_gate(saba_core_version(args.saba_core), requirement()))
        results.append(run("module-validate", [args.saba_core, "module", "validate", str(folder)], repo, env))
        results.append(run("module-audit", [args.saba_core, "module", "audit", str(folder)], repo, env))
        if args.trial:
            trial = [python, str(repo / "conformance" / "trial.py"), str(folder), "--saba-core", args.saba_core,
                     "--out", str(pathlib.Path(args.trial_out).resolve())]
            for agreement in args.accept_agreement:
                trial += ["--accept-agreement", agreement]
            if args.extensions:
                trial += ["--extensions", args.extensions]
            results.append(run("trial", trial, repo))
    else:
        missing = "saba-core를 찾지 못했다 (--saba-core, 또는 사바쨩 설치)"
        results.append(gate("compat", "skip", missing))
        results.append(gate("module-validate", "skip", missing))
        results.append(gate("module-audit", "skip", missing))
    return results


def markdown(folder, results, newer=None):
    marks = {"ok": "✅", "fail": "❌", "skip": "⏭️"}
    out = [f"## 모듈 관문 검사: {pathlib.Path(folder).name}", "", f"- 모듈 메이커 {maker_version()}"
           + (f" (새 판 {newer}이 있다: 스킬을 업데이트한다)" if newer else ""), "", "| 관문 | 결과 | 내용 |", "|---|---|---|"]
    out += [f"| {r['gate']} | {marks[r['status']]} | {r['detail'].replace('|', '/')} |" for r in results]
    out += ["", "### 직접 확인 (기계로 재지 않는다)", *[f"- [ ] {m}" for m in MANUAL]]
    return "\n".join(out) + "\n"


def default_base(repo):
    found = subprocess.run(["git", "rev-parse", "--verify", "--quiet", "origin/main"], cwd=repo, capture_output=True, stdin=subprocess.DEVNULL)
    top = subprocess.run(["git", "rev-parse", "--show-toplevel"], cwd=repo, capture_output=True, stdin=subprocess.DEVNULL, text=True)
    if top.returncode != 0 or pathlib.Path(top.stdout.strip()).resolve() != pathlib.Path(repo).resolve():
        return None
    return "origin/main" if found.returncode == 0 else None


def main():
    parser = argparse.ArgumentParser(description="모듈의 PR 전 관문 중 기계로 잴 수 있는 것을 돌린다")
    parser.add_argument("module", help="모듈 폴더")
    parser.add_argument("--saba-core", help="saba-core 실행 파일 (module validate·audit·trial)")
    parser.add_argument("--shared-libs", help="i18n.py·daemon_rcon.py가 있는 saba-chan/modules")
    parser.add_argument("--base", help="버전을 비교할 기준 (기본: origin/main)")
    parser.add_argument("--static-only", action="store_true", help="정적 관문만")
    parser.add_argument("--trial", action="store_true", help="conformance/trial.py 실측도 돌린다 (--saba-core 필요)")
    parser.add_argument("--trial-out", default="trial-report", help="실측 보고서 폴더")
    parser.add_argument("--accept-agreement", action="append", default=[], help="실측에서 동의할 약관 id")
    parser.add_argument("--extensions", help="실측에 쓸 익스텐션 저장소")
    parser.add_argument("--out", help="보고서를 이 파일에 쓴다")
    args = parser.parse_args()
    # 사바쨩이 설치돼 있으면 saba-core와 공용 런타임 경로를 앱에서 얻는다
    app = None if args.static_only or (args.saba_core and args.shared_libs) else app_paths()
    args.app_shared_libs = (app or {}).get("shared_libs")
    args.saba_core = args.saba_core or ((app or {}).get("saba_core") or {}).get("path")
    if args.trial and not args.saba_core:
        parser.error("--trial에는 --saba-core가 필요하다 (사바쨩이 설치돼 있지 않으면 경로를 준다)")
    if args.trial and args.static_only:
        parser.error("--trial과 --static-only는 함께 쓰지 않는다")
    folder = pathlib.Path(args.module).resolve()
    if not (folder / "module.toml").is_file():
        print(f"error: {folder}에 module.toml이 없다", file=sys.stderr)
        return 1
    # 다른 곳에서 만든 모듈은 그 상위 폴더를 모듈 저장소로 본다
    repo = REPO if folder.is_relative_to(REPO) else folder.parent
    results = static_gates(folder, repo, args.base or default_base(repo))
    if not args.static_only:
        results += tool_gates(folder, REPO, args)
    report = markdown(folder, results, newer_release())
    if args.out:
        pathlib.Path(args.out).write_text(report, encoding="utf-8")
    sys.stdout.reconfigure(encoding="utf-8")
    print(report)
    # 건너뛴 관문도 통과가 아니다 (--saba-core 없이 돌리면 validate·audit를 재지 않는다)
    return 1 if any(r["status"] != "ok" for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
