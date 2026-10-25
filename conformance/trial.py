"""모듈 실측 (적합성 키트 실측 모드)
==================================

    python conformance/trial.py <모듈 폴더> --saba-core <saba-core 실행 파일>
        [--extensions <익스텐션 저장소>] [--accept-agreement <id>]... [--help-command help]
        [--out <보고서 폴더>] [--keep]

빈 작업 폴더에서 함수를 한 번씩 부르는 `test_conformance.py`와 달리, 진짜 데몬을 임시 데이터 폴더로 하나 더 띄우고
데몬 API만 불러 사용자가 밟는 흐름을 그대로 밟는다: 실제 배포판 설치, 설정 쓰기·읽기, 데몬과 같은 러너(pipe·pty 콘솔)로
시작, 준비 문구, help와 선언한 콘솔 명령 대조, 저장 후 정지, 백업 대상, 초기화 대상.

모듈은 임시 모듈 폴더에 복사되어 확인되지 않은 모듈이 된다. 그래서 Windows에서는 모든 lifecycle 호출이 데몬의 실제
격리(AppContainer) 안에서 돈다. 격리가 없는 OS에서는 데몬 API로 이 복사본에만 비격리 동의를 하고 돌린다.
약관(`[[agreements]]`)은 `--accept-agreement`로 준 것만 동의한다. 동의하지 않은 약관이 있으면 시작부터는 건너뛴다.

결과는 `<out>/trial-report.md`(PR에 붙인다)와 `trial-report.json`이다. 실패가 하나라도 있으면 종료 코드 1이다.
"""

import argparse
import json
import os
import pathlib
import platform
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request
import zipfile

ISOLATED = sys.platform == "win32"

# 격리 실행에서 자주 나는 실패의 흔적 (권한 거부, msiexec 1601: Windows Installer에 닿지 못함)
SANDBOX_CODES = ("sandbox_unavailable", "isolation_unsupported")
SANDBOX_MARKS = ("PermissionError", "Access is denied", "WinError 5", "액세스가 거부", "Errno 13", "1601")

# 인스턴스 폴더의 데몬 항목: 서버 파일로 보지 않는다
DAEMON_ENTRIES = {"instance.json", "settings.json", "rcon.json", "managed.pid", "exit-events.json"}
DAEMON_DIRS = {"backups", "run"}
# 서버가 쓰지만 플레이 데이터가 아닌 것 (백업 누락으로 보지 않는다)
# 실행 파일·라이브러리는 서버가 첫 실행 때 풀어 놓아도 프로그램 파일이다 (예: libraries/*.jar)
NOT_PLAY_DATA = re.compile(r"(^|/)logs?/|\.log$|\.lock$|(^|/)Saved/Logs/|\.(jar|dll|exe|so|dylib|pdb)$", re.IGNORECASE)


# ── 판정 규칙 (test_trial.py) ──

def console_verbs(manifest):
    """콘솔로 보내는 선언 명령의 첫 낱말과 정지 명령 (정렬)"""
    verbs = set()
    for field in manifest.get("commands", {}).get("fields", []):
        if field.get("method") not in ("console", "rcon"):
            continue
        words = field.get("console_template", "").split()
        if words and not words[0].startswith("{"):
            verbs.add(words[0].lstrip("/").lower())
    stop = manifest.get("config", {}).get("stop_command", "").split()
    if stop:
        verbs.add(stop[0].lstrip("/").lower())
    return sorted(verbs)


def help_gaps(lines, verbs):
    """help 출력에 한 번도 나오지 않는 선언 명령"""
    text = "\n".join(lines).lower()
    return [v for v in verbs if not re.search(rf"(?<![\w-]){re.escape(v)}(?![\w-])", text)]


def sandbox_suspect(result):
    """실패한 응답이 격리 실행 문제로 보이면 그 흔적"""
    if not isinstance(result, dict) or result.get("success") is not False:
        return None
    code = result.get("error_code")
    if code in SANDBOX_CODES:
        return code
    text = json.dumps({k: result.get(k) for k in ("message", "error", "detail", "stderr_tail")}, ensure_ascii=False)
    return next((mark for mark in SANDBOX_MARKS if mark in text), None)


def snapshot(root):
    """폴더 안 서버 파일의 {상대 경로: (크기, 수정 시각)} (데몬 항목은 뺀다)"""
    root = pathlib.Path(root)
    files = {}
    if not root.is_dir():
        return files
    for dirpath, dirnames, filenames in os.walk(root):
        rel_dir = pathlib.Path(dirpath).relative_to(root)
        if rel_dir == pathlib.Path("."):
            dirnames[:] = [d for d in dirnames if d not in DAEMON_DIRS]
            filenames = [f for f in filenames if f not in DAEMON_ENTRIES]
        for name in filenames:
            path = pathlib.Path(dirpath) / name
            try:
                st = path.stat()
            except OSError:
                continue
            files[(rel_dir / name).as_posix()] = (st.st_size, st.st_mtime_ns)
    return files


def changed(before, after):
    return sorted(p for p, meta in after.items() if before.get(p) != meta)


def removed(before, after):
    return sorted(p for p in before if p not in after)


def play_data(files):
    return [f for f in files if not NOT_PLAY_DATA.search(f)]


def live_lines(body, since):
    """콘솔 응답에서 `since` 이후 줄과 다음 `since`. 서버가 끝나 디스크 기록으로 넘어간 응답(`epoch` 없음)은
    since를 무시하고 앞 줄을 다시 주며 줄 모양도 달라 쓰지 않는다 (live=False)"""
    if not isinstance(body, dict) or "epoch" not in body:
        return [], since, False
    lines = [l for l in body.get("lines", []) if l.get("id", -1) >= since]
    return [l.get("content", "") for l in lines], (lines[-1]["id"] + 1 if lines else since), True


def backup_targets(zip_path):
    """데몬 백업 zip이 담은 원래 경로들. 읽지 못하면 None"""
    try:
        with zipfile.ZipFile(zip_path) as z:
            names = set(z.namelist())
            name = next((n for n in ("saba-backup-v2.json", "saba-backup.json") if n in names), None)
            if name is None:
                return None
            manifest = json.loads(z.read(name))
    except (OSError, zipfile.BadZipFile, ValueError):
        return None
    return [pathlib.Path(entry["path"]) for entry in manifest.get("paths", [])]


def uncovered(root, files, targets):
    """`root` 기준 상대 경로 중 어느 백업 대상 안에도 없는 것"""
    resolved = [os.path.normcase(os.path.abspath(t)) for t in targets]
    out = []
    for f in files:
        path = os.path.normcase(os.path.abspath(pathlib.Path(root) / f))
        if not any(path == t or path.startswith(t.rstrip(os.sep) + os.sep) for t in resolved):
            out.append(f)
    return out


def port_settings(manifest, pick):
    """포트 설정(`port`, `port = true`인 칸)마다 빈 포트. 기본값으로 두면 이 PC에서 도는 실제 서버와 겹친다"""
    fields = manifest.get("settings", {}).get("fields", [])
    return {f["name"]: pick() for f in fields if f.get("name") == "port" or f.get("port")}


# ── 보고서 ──

STATUS_MARK = {"ok": "✅", "fail": "❌", "warn": "⚠️", "skip": "⏭️"}
STATUS_ORDER = ("ok", "fail", "warn", "skip")


class Report:
    def __init__(self, module, platform, isolated):
        self.module, self.platform, self.isolated = module, platform, isolated
        self.version = None
        self.daemon = None
        self.console = None
        self.steps = []

    def add(self, name, status, detail, functions=(), suspect=None):
        assert status in STATUS_ORDER, status
        self.steps.append({"name": name, "status": status, "detail": detail, "functions": list(functions), "suspect": suspect})
        print(f"{STATUS_MARK[status]} {name}: {detail}", flush=True)

    def counts(self):
        return {s: sum(1 for step in self.steps if step["status"] == s) for s in STATUS_ORDER}

    def exit_code(self):
        return int(self.counts()["fail"] > 0)

    def json(self):
        return json.dumps({
            "module": self.module, "version": self.version, "platform": self.platform, "isolated": self.isolated,
            "daemon": self.daemon, "console": self.console, "counts": self.counts(), "steps": self.steps,
        }, ensure_ascii=False, indent=2)

    def markdown(self):
        isolation = "AppContainer (데몬의 실제 격리)" if self.isolated else "격리 없음 (이 OS는 격리를 지원하지 않아 비격리 동의로 실행)"
        c = self.counts()
        lines = [
            f"## 모듈 실측 보고서: {self.module} {self.version or ''}".rstrip(),
            "",
            f"- 플랫폼: {self.platform}, 데몬: {self.daemon or '?'}, 콘솔: {self.console or 'pipe'}",
            f"- lifecycle 격리: {isolation}",
            f"- 결과: 통과 {c['ok']}, 실패 {c['fail']}, 경고 {c['warn']}, 건너뜀 {c['skip']}",
            "",
            "| 단계 | 결과 | 함수 | 내용 |",
            "|---|---|---|---|",
        ]
        for s in self.steps:
            detail = s["detail"].replace("|", "\\|").replace("\n", " ")
            if s["suspect"]:
                detail += f" (격리 오류 의심: `{s['suspect']}`)"
            functions = ", ".join(f"`{f}`" for f in s["functions"]) or "-"
            lines.append(f"| {s['name']} | {STATUS_MARK[s['status']]} | {functions} | {detail} |")
        return "\n".join(lines) + "\n"


# ── 데몬 ──

def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Daemon:
    """임시 데이터 폴더로 띄운 saba-core 하나와 그 API"""

    def __init__(self, exe, root):
        self.root = root
        self.port = free_port()
        self.base = f"http://127.0.0.1:{self.port}"
        self.data = root / "data"
        self.modules = root / "modules"
        self.extensions = root / "extensions"
        self.instances = root / "instances"
        for d in (self.data, self.modules, self.extensions, self.instances):
            d.mkdir(parents=True, exist_ok=True)
        self.exe = exe
        self.proc = None
        self.token = None
        self.events = []
        # 데몬은 루프백에만 있다: 프록시 환경 변수를 따르지 않는다
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def start(self, timeout=60):
        env = {k: v for k, v in os.environ.items() if not k.startswith("SABA_")}
        env.update({
            "SABA_DATA_DIR": str(self.data), "SABA_MODULES_PATH": str(self.modules),
            "SABA_EXTENSIONS_DIR": str(self.extensions), "SABA_INSTANCES_PATH": str(self.instances),
            "SABA_IPC_PORT": str(self.port), "RUST_LOG": env.get("RUST_LOG", "info"),
        })
        # 업데이트 확인을 끈다: 켜 두면 --saba-core 옆 설치본의 파일을 바꿀 수 있다
        config = self.data / "config.json"
        if not config.exists():
            config.write_text(json.dumps({"updates": {"enabled": False}}), encoding="utf-8")
        log = open(self.root / "daemon.log", "wb")
        self.proc = subprocess.Popen([str(self.exe), "--spawned"], env=env, stdin=subprocess.DEVNULL,
                                     stdout=log, stderr=subprocess.STDOUT, cwd=self.root)
        deadline = time.monotonic() + timeout
        token_file = self.data / ".ipc_token"
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"saba-core exited with code {self.proc.returncode} (see daemon.log)")
            if token_file.is_file() and self._health():
                self.token = token_file.read_text(encoding="utf-8").strip()
                threading.Thread(target=self._follow_events, daemon=True).start()
                return
            time.sleep(0.3)
        raise RuntimeError("saba-core did not answer /health in time")

    def _health(self):
        try:
            with self.opener.open(self.base + "/health", timeout=2) as r:
                return r.status == 200
        except OSError:
            return False

    def call(self, method, path, body=None, timeout=120):
        """(HTTP 상태, JSON 또는 텍스트)"""
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(self.base + path, data=data, method=method,
                                     headers={"X-Saba-Token": self.token or "", "Content-Type": "application/json",
                                              "Accept-Language": "ko"})
        try:
            with self.opener.open(req, timeout=timeout) as r:
                status, raw = r.status, r.read()
        except urllib.error.HTTPError as e:
            status, raw = e.code, e.read()
        try:
            return status, json.loads(raw) if raw else None
        except ValueError:
            return status, raw.decode("utf-8", "replace")

    def _follow_events(self):
        req = urllib.request.Request(self.base + "/api/events", headers={"X-Saba-Token": self.token})
        try:
            with self.opener.open(req, timeout=None) as stream:
                kind = None
                for raw in stream:
                    line = raw.decode("utf-8", "replace").rstrip("\r\n")
                    if line.startswith("event:"):
                        kind = line[6:].strip()
                    elif line.startswith("data:") and kind:
                        try:
                            self.events.append((kind, json.loads(line[5:])))
                        except ValueError:
                            pass
        except OSError:
            pass

    def wait_event(self, kind, match, timeout):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for k, data in list(self.events):
                if k == kind and match(data):
                    return data
            time.sleep(0.5)
        return None

    def stop(self):
        if self.proc is None or self.proc.poll() is not None:
            return
        try:
            self.call("POST", "/api/daemon/shutdown", {}, timeout=10)
            self.proc.wait(timeout=30)
        except (OSError, subprocess.TimeoutExpired):
            self.proc.kill()
            self.proc.wait()


# ── 실측 흐름 ──

def load_manifest(module_dir):
    path = module_dir / "module.toml"
    if not path.is_file():
        # __example 처럼 접두어가 붙은 견본
        path = next(module_dir.glob("*module.toml"))
    return tomllib.loads(path.read_text(encoding="utf-8-sig"))


def required_extensions(manifest):
    ids = list(manifest.get("install", {}).get("requires_extensions", []))
    ids += [k.removeprefix("ext-") for k in manifest.get("module", {}).get("dependencies", {}) if k.startswith("ext-")]
    return list(dict.fromkeys(ids))


class Trial:
    def __init__(self, args):
        self.args = args
        self.module_dir = pathlib.Path(args.module).resolve()
        self.manifest = load_manifest(self.module_dir)
        self.name = self.manifest["module"]["name"]
        self.report = Report(self.name, f"{platform.system()} {platform.release()}", ISOLATED)
        self.report.version = self.manifest["module"].get("version")
        self.report.console = self.manifest.get("launch", {}).get("console", "pipe")
        self.root = pathlib.Path(tempfile.mkdtemp(prefix="saba-trial-"))
        self.daemon = Daemon(pathlib.Path(args.saba_core).resolve(), self.root)
        self.instance = None
        self.functions = None
        self.instance_dir = None
        self.server_dir = None

    # 단계 하나: 결과를 보고서에 적고, 이어 갈 수 있으면 True
    def step(self, name, functions, status_code, body, ok_when=lambda b: True, describe=None):
        good = 200 <= status_code < 300 and isinstance(body, dict) and body.get("success") is not False and ok_when(body)
        detail = (describe(body) if describe else "") if good else self.reason(status_code, body)
        self.report.add(name, "ok" if good else "fail", detail, functions, None if good else sandbox_suspect(body if isinstance(body, dict) else {}))
        return good

    @staticmethod
    def reason(status_code, body):
        if isinstance(body, dict):
            for key in ("error", "message", "error_code"):
                if body.get(key):
                    return f"HTTP {status_code}: {str(body[key])[:300]}"
        return f"HTTP {status_code}: {str(body)[:300]}"

    def run(self):
        try:
            self.boot() and self.install() and self.settings() and self.serve()
        except KeyboardInterrupt:
            self.report.add("trial", "fail", "중단됨")
        except Exception as e:  # 보고서는 끝까지 남긴다
            self.report.add("trial", "fail", f"{type(e).__name__}: {e}")
        finally:
            try:
                self.cleanup()
            finally:
                self.write_report()
        return self.report.exit_code()

    def implements(self, name, function):
        """모듈이 밝힌 함수 목록에 없으면 그 단계를 건너뛴다 (목록을 모르면 부른다)"""
        if self.functions is None or function in self.functions:
            return True
        self.report.add(name, "skip", f"모듈에 {function} 함수가 없다")
        return False

    def snap(self):
        """인스턴스 폴더(모듈 데이터 폴더 포함)와, 그 밖에 있으면 서버 폴더. 키는 인스턴스 폴더 기준 상대 경로이거나 절대 경로"""
        files = snapshot(self.instance_dir)
        server = self.server_dir.resolve()
        if not server.is_relative_to(self.instance_dir.resolve()):
            files.update({(server / k).as_posix(): v for k, v in snapshot(server).items()})
        return files

    def boot(self):
        d = self.daemon
        shutil.copytree(self.module_dir, d.modules / self.name, ignore=shutil.ignore_patterns("__pycache__", "tests", "test_*.py"))
        for f in (d.modules / self.name).glob("__example__*"):
            f.rename(f.with_name(f.name.removeprefix("__example__")))
        exts = required_extensions(self.manifest)
        for ext in exts:
            src = pathlib.Path(self.args.extensions or "").resolve() / ext.replace("-", "_")
            if not self.args.extensions or not src.is_dir():
                self.report.add("boot", "fail", f"필요한 익스텐션 {ext}을 찾지 못했다 (--extensions <익스텐션 저장소>)")
                return False
            shutil.copytree(src, d.extensions / src.name, ignore=shutil.ignore_patterns("__pycache__", "tests", "node_modules"))
        runner = d.exe.with_name("saba-runner" + (".exe" if ISOLATED else ""))
        if not runner.is_file():
            self.report.add("boot", "fail", f"{runner.name}가 saba-core 옆에 없다: 데몬이 러너 없이 띄우면 실제와 다른 콘솔로 잰다")
            return False
        d.start()
        status, version = d.call("GET", "/api/version")
        self.report.daemon = (version or {}).get("version") if isinstance(version, dict) else None
        # Python 준비 (처음이면 venv를 만든다)
        deadline = time.monotonic() + 900
        while time.monotonic() < deadline:
            _, pre = d.call("GET", "/api/runtime/preflight")
            if isinstance(pre, dict) and pre.get("complete"):
                break
            time.sleep(1)
        if isinstance(pre, dict) and pre.get("error"):
            self.report.add("boot", "fail", f"데몬 Python 준비 실패: {pre['error']}")
            return False
        components = [f"module-{self.name}"] + [f"ext-{e}" for e in exts]
        if not ISOLATED:
            for c in components:
                d.call("POST", f"/api/sandbox/unisolated/{c}")
        status, listing = d.call("POST", "/api/modules/refresh")
        failed = [f for f in (listing or {}).get("failed", []) if f.get("module") == self.name or self.name in str(f.get("path"))]
        module = next((m for m in (listing or {}).get("modules", []) if m.get("name") == self.name), None)
        if failed or module is None:
            self.report.add("boot", "fail", f"데몬이 모듈을 읽지 못했다: {failed[0] if failed else listing}")
            return False
        functions = module.get("functions")
        self.functions = set(functions) if isinstance(functions, list) else None
        for ext in exts:
            d.call("POST", "/api/extensions/rescan")
            status, body = d.call("POST", f"/api/extensions/{ext}/enable")
            if not self.step("boot", [], status, body, describe=lambda b, ext=ext: f"익스텐션 {ext} 켬"):
                return False
        warnings = module.get("warnings") or []
        self.report.add("boot", "warn" if warnings else "ok",
                        f"데몬 {self.report.daemon} 기동, 모듈 로드" + (f", 경고: {'; '.join(warnings)}" if warnings else ""), ["functions"])
        if not self.implements("versions", "list_versions"):
            return True
        status, versions = d.call("GET", f"/api/module/{self.name}/versions")
        self.step("versions", ["list_versions"], status, versions,
                  describe=lambda b: f"{len(b.get('versions', []))}개" if isinstance(b, dict) and "versions" in b else "")
        return True

    def install(self):
        d = self.daemon
        ports = port_settings(self.manifest, free_port)
        port = ports.pop("port", None) or free_port()
        body = {"name": f"trial-{self.name}", "module_name": self.name, "port": port, "module_settings": ports,
                "accept_agreements": self.args.accept_agreement}
        status, created = d.call("POST", "/api/instances", body)
        if not self.step("create", ["accept_agreements"] if self.args.accept_agreement else [], status, created,
                         ok_when=lambda b: bool(b.get("id")), describe=lambda b: f"포트 {', '.join(map(str, [port, *ports.values()]))}"):
            return False
        self.instance = created["id"]
        self.instance_dir = d.instances / self.instance
        progress = None
        started = time.monotonic()
        deadline = started + self.args.install_timeout
        while time.monotonic() < deadline:
            _, progress = d.call("GET", f"/api/provision-progress/{body['name']}")
            if not isinstance(progress, dict) or not progress.get("active") or progress.get("done"):
                break
            time.sleep(1)
        else:
            self.report.add("install", "fail", f"{self.args.install_timeout}초 안에 설치가 끝나지 않았다", ["install_server"])
            return False
        if isinstance(progress, dict) and progress.get("error"):
            # 실패 진행 표시에는 모듈 호출 오류 필드(error_code, stderr_tail 등)가 함께 실린다
            code = progress.get("error_code")
            self.report.add("install", "fail", f"{progress['error']}" + (f" ({code})" if code else ""),
                            ["install_server"], sandbox_suspect({**progress, "success": False}))
            return False
        _, inst = d.call("GET", f"/api/instance/{self.instance}")
        self.server_dir = pathlib.Path(inst.get("working_dir") or self.instance_dir)
        files = snapshot(self.server_dir)
        if not files:
            self.report.add("install", "fail", "설치가 끝났다는데 서버 폴더가 비었다", ["install_server"])
            return False
        self.report.add("install", "ok", f"{len(files)}개 파일, {time.monotonic() - started:.0f}초", ["list_versions", "install_server"])
        if not self.implements("installed-version", "get_installed_version"):
            return True
        status, ver = d.call("GET", f"/api/instance/{self.instance}/installed-version")
        self.step("installed-version", ["get_installed_version"], status, ver, describe=lambda b: str(b.get("version", "")))
        return True

    def settings(self):
        d = self.daemon
        if self.implements("settings", "import_settings") and self.implements("settings-write", "configure"):
            self.check_settings()
        if self.implements("properties", "read_properties"):
            status, props = d.call("GET", f"/api/instance/{self.instance}/properties")
            self.step("properties", ["read_properties"], status, props, describe=lambda b: "")
        return True

    def check_settings(self):
        d = self.daemon
        status, live = d.call("GET", f"/api/instance/{self.instance}/settings/live")
        if not self.step("settings", ["import_settings"], status, live, describe=lambda b: "게임 파일에서 설정을 읽었다"):
            return
        # 게임 파일에 쓰는 숫자 설정 하나를 바꿔 쓰고 다시 읽는다 (데몬이 쓴 값을 게임이 받는 모양인지)
        field = next((f for f in self.manifest.get("settings", {}).get("fields", [])
                      if f.get("type") == "number" and f.get("group") != "saba-chan" and f.get("name") != "port"
                      and not f.get("port") and not f.get("install") and isinstance(f.get("default"), (int, float))), None)
        if field is None:
            self.report.add("settings-write", "skip", "바꿔 볼 숫자 설정이 없다")
        else:
            self.write_setting(field)

    def write_setting(self, field):
        d = self.daemon
        value = field["default"] + 1 if field.get("max") is None or field["default"] + 1 <= field["max"] else field["default"] - 1
        status, saved = d.call("PATCH", f"/api/instance/{self.instance}", {field["name"]: value})
        _, live = d.call("GET", f"/api/instance/{self.instance}/settings/live")
        read = (live or {}).get("settings", live or {}).get(field["name"]) if isinstance(live, dict) else None
        if 200 <= status < 300 and str(read) == str(value):
            self.report.add("settings-write", "ok", f"{field['name']}={value} 쓰고 다시 읽음", ["configure", "import_settings"])
        else:
            self.report.add("settings-write", "fail", f"{field['name']}={value}를 썼는데 다시 읽은 값은 {read!r} ({self.reason(status, saved)})",
                            ["configure", "import_settings"], sandbox_suspect(saved if isinstance(saved, dict) else {}))

    def console_since(self, since):
        _, body = self.daemon.call("GET", f"/api/instance/{self.instance}/console?since={since}&count=2000")
        lines = (body or {}).get("lines", []) if isinstance(body, dict) else []
        return lines

    def serve(self):
        d = self.daemon
        agreements = self.manifest.get("agreements") or []
        missing = [a.get("id") for a in agreements if a.get("id") not in self.args.accept_agreement]
        if missing:
            self.report.add("start", "skip", f"동의하지 않은 약관 {', '.join(missing)}: --accept-agreement로 동의해야 시작한다")
            self.reset()
            return False
        if self.implements("validate", "validate"):
            status, body = d.call("POST", f"/api/instance/{self.instance}/validate")
            self.step("validate", ["validate"], status, body, describe=lambda b: "")
        before_start = self.snap()
        status, body = d.call("POST", f"/api/instance/{self.instance}/start", timeout=300)
        if not self.step("start", ["validate", "get_launch_command"], status, body,
                         describe=lambda b: f"{'러너' if b.get('managed') else '직접'} 실행"):
            self.reset()
            return False
        patterns = self.manifest.get("extensions", {}).get("saba-core", {}).get("ready_patterns", [])
        timeout = int(self.manifest.get("extensions", {}).get("saba-core", {}).get("ready_timeout", 600)) + 30
        ready = d.wait_event("instance.ready", lambda e: e.get("instanceId") == self.instance, timeout)
        if ready is None:
            self.report.add("ready", "fail", f"{timeout}초 안에 준비 알림이 없다")
        elif not ready.get("ok"):
            tail = [l.get("content", "") for l in self.console_since(0)[-5:]]
            self.report.add("ready", "fail", f"{ready.get('code')}: 콘솔 끝 {tail}")
            # 준비 시간 초과여도 서버는 돈다: 끄지 않으면 초기화가 server_running으로 거절된다
            d.call("POST", f"/api/instance/{self.instance}/stop", {"force": True}, timeout=60)
            self.reset()
            return False
        elif not patterns:
            self.report.add("ready", "warn", f"ready_patterns가 없어 준비 시점을 모른다: {self.args.warmup}초 기다린 뒤 잰다 ([extensions.saba-core] ready_patterns)")
            time.sleep(self.args.warmup)
        else:
            self.report.add("ready", "ok", "준비 문구가 콘솔에 나왔다")
        self.help()
        status, body = d.call("GET", f"/api/instance/{self.instance}/commands")
        hook = bool(self.manifest.get("commands", {}).get("hook"))
        self.step("commands", ["instance_commands"] if hook else [], status, body,
                  ok_when=lambda b: "commands" in b, describe=lambda b: f"{len(b.get('commands', []))}개")
        if self.manifest.get("status", {}).get("players"):
            status, body = d.call("GET", f"/api/instance/{self.instance}/players")
            self.step("players", [self.manifest["status"]["players"]], status, body, describe=lambda b: f"{b.get('online')}명")
        if self.implements("diagnose", "diagnose_log"):
            status, body = d.call("POST", f"/api/instance/{self.instance}/diagnose")
            self.step("diagnose", ["diagnose_log"], status, body, describe=lambda b: "")
        self.stop(before_start)
        return True

    def help(self):
        if "stdin" not in self.manifest.get("protocols", {}).get("supported", []):
            self.report.add("help", "skip", "콘솔(stdin)이 없는 모듈")
            return
        lines = self.console_since(0)
        last = lines[-1]["id"] + 1 if lines else 0
        status, body = self.daemon.call("POST", f"/api/instance/{self.instance}/stdin", {"command": self.args.help_command})
        time.sleep(3)
        out = [l.get("content", "") for l in self.console_since(last)]
        response = (body or {}).get("data") or {} if isinstance(body, dict) else {}
        if isinstance(response, dict) and isinstance(response.get("response"), str):
            out.append(response["response"])
        if not (200 <= status < 300) or not any(o.strip() for o in out):
            self.report.add("help", "warn", f"`{self.args.help_command}` 뒤 콘솔 출력이 없다 ({self.reason(status, body)})")
            return
        gaps = help_gaps(out, console_verbs(self.manifest))
        if gaps:
            self.report.add("help", "warn", f"help에 없는 선언 명령: {', '.join(gaps)} (명령 이름이나 console_template 확인)")
        else:
            self.report.add("help", "ok", f"선언한 콘솔 명령 {len(console_verbs(self.manifest))}개가 help에 있다")

    def stop(self, before_start):
        d = self.daemon
        time.sleep(self.args.play_seconds)
        before_stop = self.snap()
        lines = self.console_since(0)
        last = lines[-1]["id"] + 1 if lines else 0
        # 정지 요청은 서버가 끝날 때까지 돌아오지 않고, 끝나면 데몬은 콘솔 버퍼를 놓는다:
        # 정지 출력(graceful_exit_patterns)은 요청이 도는 동안 읽어 둔다
        answer = {}
        request = threading.Thread(target=lambda: answer.update(r=d.call(
            "POST", f"/api/instance/{self.instance}/stop", {"force": False}, timeout=300)), daemon=True)
        request.start()
        tail = []
        while True:
            _, console = d.call("GET", f"/api/instance/{self.instance}/console?since={last}&count=2000")
            got, last, live = live_lines(console, last)
            tail += got
            if not live or not request.is_alive():
                break
            time.sleep(0.3)
        request.join()
        status, body = answer.get("r", (0, None))
        functions = ["stop"] if "stdin" not in self.manifest.get("protocols", {}).get("supported", []) else []
        if not self.step("stop", functions, status, body, describe=lambda b: ""):
            d.call("POST", f"/api/instance/{self.instance}/stop", {"force": True}, timeout=60)
            self.reset()
            return
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            _, servers = d.call("GET", "/api/servers")
            mine = next((s for s in (servers or {}).get("servers", []) if s.get("id") == self.instance), {})
            if mine.get("status") != "running":
                break
            time.sleep(1)
        _, events = d.call("GET", f"/api/instance/{self.instance}/events")
        kind = ((events or {}).get("events") or [{}])[-1].get("kind") if isinstance(events, dict) else None
        after_stop = self.snap()
        saved = play_data(changed(before_stop, after_stop))
        exit_patterns = self.manifest.get("detection", {}).get("graceful_exit_patterns", [])
        notes = []
        if exit_patterns and not any(re.search(p, t) for p in exit_patterns for t in tail):
            notes.append("graceful_exit_patterns가 정지 출력에 없다")
        if not saved:
            notes.append("정지하는 동안 저장된 플레이 데이터가 없다")
        if kind != "user_stop":
            self.report.add("stop-result", "fail", f"종료 판정이 user_stop이 아니라 {kind}", functions)
        else:
            self.report.add("stop-result", "warn" if notes else "ok",
                            "; ".join(notes) or f"정지 중 {len(saved)}개 파일 저장 ({', '.join(saved[:5])})", functions)
        self.backup(before_start, after_stop)
        self.reset()

    def backup(self, before_start, after_stop):
        status, body = self.daemon.call("POST", f"/api/instance/{self.instance}/backups", {})
        functions = ["backup_paths"] if self.manifest.get("backup", {}).get("hook") else []
        if not self.step("backup", functions, status, body, ok_when=lambda b: bool(b.get("backup")), describe=lambda b: ""):
            self.report.steps[-1]["status"] = "fail"
            return
        name = body["backup"].get("name")
        targets = backup_targets(self.instance_dir / "backups" / name)
        step = self.report.steps[-1]
        if targets is None:
            step.update(status="warn", detail=f"백업 {name}의 목록을 읽지 못했다")
            return
        written = play_data(changed(before_start, after_stop))
        missing = uncovered(self.instance_dir, written, targets)
        absent = [str(t) for t in targets if not t.exists()]
        executable = self.manifest.get("config", {}).get("server_executable")
        binaries = [str(t) for t in targets if executable and (t.name == executable or (t / executable).exists())]
        notes = []
        if missing:
            notes.append(f"서버가 쓴 플레이 데이터 중 백업에 없는 것: {', '.join(missing[:10])}")
        if absent:
            notes.append(f"없는 백업 대상: {', '.join(absent[:5])}")
        if binaries:
            notes.append(f"백업에 서버 실행 파일이 들어 있다: {', '.join(binaries[:3])}")
        step.update(status="warn" if notes else "ok",
                    detail="; ".join(notes) or f"대상 {len(targets)}개가 서버가 쓴 플레이 데이터를 모두 담는다")

    def reset(self):
        if self.instance is None or not self.implements("reset", "reset_server"):
            return
        before = self.snap()
        status, body = self.daemon.call("POST", f"/api/instance/{self.instance}/server/reset", timeout=300)
        if not self.step("reset", ["reset_server"], status, body, describe=lambda b: ""):
            return
        after = self.snap()
        gone = removed(before, after)
        executable = self.manifest.get("config", {}).get("server_executable") or ""
        lost_binary = [g for g in gone if executable and pathlib.PurePosixPath(g).name == executable]
        step = self.report.steps[-1]
        if lost_binary:
            step.update(status="warn", detail=f"초기화가 서버 실행 파일까지 지웠다: {lost_binary[0]}")
        elif not play_data(gone):
            step.update(status="warn", detail="초기화가 지운 플레이 데이터가 없다")
        else:
            step.update(detail=f"{len(gone)}개 파일 삭제 ({', '.join(play_data(gone)[:5])})")

    def cleanup(self):
        try:
            if self.instance:
                self.daemon.call("POST", f"/api/instance/{self.instance}/stop", {"force": True}, timeout=60)
                if not self.args.keep:
                    self.daemon.call("DELETE", f"/api/instance/{self.instance}")
        except OSError:
            pass
        self.daemon.stop()

    def write_report(self):
        out = pathlib.Path(self.args.out).resolve()
        out.mkdir(parents=True, exist_ok=True)
        (out / "trial-report.md").write_text(self.report.markdown(), encoding="utf-8")
        (out / "trial-report.json").write_text(self.report.json(), encoding="utf-8")
        if (self.root / "daemon.log").is_file():
            shutil.copy(self.root / "daemon.log", out / "daemon.log")
        if self.args.keep:
            print(f"kept {self.root}")
        else:
            shutil.rmtree(self.root, ignore_errors=True)
        print(f"report: {out / 'trial-report.md'}")


def main(argv=None):
    p = argparse.ArgumentParser(description="모듈 실측: 진짜 데몬으로 설치·시작·정지·백업·초기화를 밟는다")
    p.add_argument("module", help="모듈 폴더")
    p.add_argument("--saba-core", required=True, help="saba-core 실행 파일 (옆에 saba-runner가 있어야 러너로 띄운다)")
    p.add_argument("--extensions", help="모듈이 쓰는 익스텐션 폴더들이 있는 곳 (saba-chan-extensions 저장소)")
    p.add_argument("--accept-agreement", action="append", default=[], help="동의할 [[agreements]] id")
    p.add_argument("--help-command", default="help", help="콘솔 명령 목록을 찍는 명령")
    p.add_argument("--play-seconds", type=int, default=10, help="준비된 뒤 정지하기 전에 기다릴 시간")
    p.add_argument("--warmup", type=int, default=90, help="ready_patterns가 없을 때 시작 뒤 기다릴 시간")
    p.add_argument("--install-timeout", type=int, default=1900)
    p.add_argument("--out", default="trial-report", help="보고서 폴더")
    p.add_argument("--keep", action="store_true", help="임시 폴더를 지우지 않는다")
    return Trial(p.parse_args(argv)).run()


if __name__ == "__main__":
    sys.exit(main())
