"""서버 시험 실행 (모듈 메이커)
=============================

    python maker/probe_server.py --cwd <서버 폴더> [--ready <정규식>] [--send help]... [--stop stop]
        [--timeout 120] [--settle 3] [--encoding utf-8] [--out <보고서 폴더>] -- <실행 파일> [인자...]

모듈을 쓰기 전에, 데몬과 같은 방식(stdin·stdout 파이프)으로 서버를 띄워 조사 표의 "실측" 원문을 얻는다:
준비 완료 줄, 보낸 명령마다의 응답(`help` 출력 포함), 정지 명령 뒤의 줄과 종료 코드, 실행 중에 생기거나 바뀐 파일.
보낸 명령에 아무 답이 없으면 그 서버는 파이프 입력을 읽지 않는 것일 수 있다(테라리아 Windows 서버). 그때는
`[launch] console = "pty"`로 다시 잰다. 정지 명령 뒤 `--timeout`초 안에 끝나지 않으면 이 도구가 띄운 프로세스만 끝낸다.

`--out`을 주면 `probe-report.md`, `probe-report.json`, 원래 바이트 그대로의 `probe-output.log`를 남긴다.
"""

import argparse
import json
import os
import pathlib
import re
import subprocess
import sys
import threading
import time


def snapshot(root):
    files = {}
    for dirpath, _, filenames in os.walk(root):
        for name in filenames:
            path = pathlib.Path(dirpath) / name
            try:
                st = path.stat()
            except OSError:
                continue
            files[path.relative_to(root).as_posix()] = (st.st_size, st.st_mtime_ns)
    return files


class Output:
    """자식 stdout을 줄 단위로 모은다 (원래 바이트와 받은 시각)"""

    def __init__(self, stream, started):
        self.lines = []
        self.cond = threading.Condition()
        self.started = started
        self.closed = False
        self.thread = threading.Thread(target=self._read, args=(stream,), daemon=True)
        self.thread.start()

    def _read(self, stream):
        for raw in iter(stream.readline, b""):
            with self.cond:
                self.lines.append((time.monotonic() - self.started, raw))
                self.cond.notify_all()
        with self.cond:
            self.closed = True
            self.cond.notify_all()

    def wait(self, until, timeout):
        """until(lines)가 참이거나 출력이 닫히거나 timeout이 지날 때까지"""
        deadline = time.monotonic() + timeout
        with self.cond:
            while not until(self.lines) and not self.closed:
                left = deadline - time.monotonic()
                if left <= 0:
                    break
                self.cond.wait(left)

    def wait_quiet(self, settle, timeout):
        """settle초 동안 새 줄이 없을 때까지 (최대 timeout초). 조용해졌으면 True"""
        deadline = time.monotonic() + timeout
        with self.cond:
            count, quiet_since = len(self.lines), time.monotonic()
            while not self.closed:
                now = time.monotonic()
                if now - quiet_since >= settle:
                    return True
                if now >= deadline:
                    return False
                self.cond.wait(min(settle - (now - quiet_since), deadline - now))
                if len(self.lines) != count:
                    count, quiet_since = len(self.lines), time.monotonic()
            return True

    def since(self, mark):
        with self.cond:
            return self.lines[mark:]


def text(raw, encoding):
    return raw.rstrip(b"\r\n").decode(encoding, errors="replace")


def probe(command, cwd, ready=None, sends=(), stop=None, timeout=120, settle=3.0, encoding="utf-8"):
    cwd = pathlib.Path(cwd)
    try:
        pattern = re.compile(ready) if ready else None
    except re.error as e:
        raise ValueError(f"준비 줄 정규식이 틀렸다: {e}") from None
    before = snapshot(cwd)
    started = time.monotonic()
    # stdin은 버퍼 없이 연다: 막힌 쓰기가 남긴 버퍼를 나중에 비우려다 닫힌 파이프에 쓰지 않게
    proc = subprocess.Popen(command, cwd=cwd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=0)
    writers = []
    try:
        return _drive(proc, writers, cwd, before, started, pattern, ready, sends, stop, timeout, settle, encoding)
    finally:
        if proc.poll() is None:
            proc.kill()  # 이 도구가 띄운 자식만 (예외로 빠져나와도 남기지 않는다)
            proc.wait()
        for writer in writers:  # 자식이 끝나면 막힌 쓰기도 실패로 풀린다
            writer.join(5)
        try:
            proc.stdin.close()
        except OSError:
            pass


def _drive(proc, writers, cwd, before, started, pattern, ready, sends, stop, timeout, settle, encoding):
    out = Output(proc.stdout, started)
    result = {"command": proc.args, "cwd": str(cwd), "ready_pattern": ready, "ready_line": None, "ready_after": None, "sends": []}

    def send(line):
        """stdin에 한 줄 쓴다. 서버가 stdin을 읽지 않아 파이프가 차면 쓰기가 막히므로 기한을 둔다"""
        error = []

        def write():
            try:
                data = memoryview(line.encode(encoding) + b"\n")
                while data:
                    data = data[proc.stdin.write(data):]
            except (OSError, ValueError) as e:
                error.append(f"{type(e).__name__}: {e}")

        writer = threading.Thread(target=write, daemon=True)
        writers.append(writer)
        writer.start()
        writer.join(min(timeout, max(settle, 1.0)))
        if writer.is_alive():
            return "stdin 쓰기가 끝나지 않았다: 서버가 stdin을 읽지 않는다"
        return error[0] if error else None

    if pattern:
        out.wait(lambda lines: any(pattern.search(text(raw, encoding)) for _, raw in lines), timeout)
        hit = next(((t, raw) for t, raw in out.since(0) if pattern.search(text(raw, encoding))), None)
        if hit:
            result["ready_after"], result["ready_line"] = round(hit[0], 2), text(hit[1], encoding)
    else:
        out.wait_quiet(settle, min(timeout, settle * 3))

    for command_line in sends:
        # 보내기 전에 조용해지지 않으면 서버가 혼자 출력하는 중이라 뒤의 줄이 답인지 알 수 없다
        quiet = out.wait_quiet(settle, min(timeout, settle * 3))
        mark = len(out.since(0))
        error = send(command_line)
        out.wait_quiet(settle, timeout)
        lines = [text(raw, encoding) for _, raw in out.since(mark)]
        answered = (bool(lines) if quiet else None) if not error else False
        result["sends"].append({"command": command_line, "output": lines, "answered": answered, "error": error})

    mark = len(out.since(0))
    stop_result = {"command": stop, "output": [], "exit_code": None, "exited": False}
    error = send(stop) if stop else None
    try:
        stop_result["exit_code"] = proc.wait(timeout=timeout)
        stop_result["exited"] = True
    except subprocess.TimeoutExpired:
        proc.kill()  # 이 도구가 띄운 자식만
        proc.wait()
    out.thread.join(5)
    stop_result["output"] = [text(raw, encoding) for _, raw in out.since(mark)]
    if error:
        stop_result["error"] = error
    result["stop"] = stop_result
    result["elapsed"] = round(time.monotonic() - started, 2)

    after = snapshot(cwd)
    result["created"] = sorted(p for p in after if p not in before)
    result["changed"] = sorted(p for p in after if p in before and before[p] != after[p])
    result["raw"] = b"".join(raw for _, raw in out.since(0))
    return result


def notes(result):
    found = []
    if result["ready_pattern"] and result["ready_line"] is None:
        found.append("준비 줄을 찾지 못했다: 정규식이나 서버 출력을 다시 본다")
    if any(s["answered"] is False for s in result["sends"]):
        found.append('보낸 명령에 답이 없었다: 파이프 stdin을 읽지 않는 서버일 수 있다. `[launch] console = "pty"`로 다시 잰다')
    if any(s["answered"] is None for s in result["sends"]):
        found.append("서버가 계속 출력하는 중이라 뒤의 줄이 명령의 답인지 알 수 없다: 출력에서 답 문구를 직접 찾는다")
    if not result["stop"]["exited"]:
        found.append("정지 명령 뒤 끝나지 않아 이 도구가 띄운 프로세스를 끝냈다: 정지 명령·stop_timeout을 다시 본다")
    return found


def file_list(paths, limit=30):
    """파일이 많으면 맨 위 폴더별 개수로 줄인다 (전체는 probe-report.json)"""
    if len(paths) <= limit:
        return ", ".join(f"`{p}`" for p in paths) or "없음"
    groups = {}
    for p in paths:
        top = p.split("/", 1)[0] + ("/" if "/" in p else "")
        groups[top] = groups.get(top, 0) + 1
    return f"{len(paths)}개: " + ", ".join(f"`{top}`" + (f" ({n}개)" if top.endswith("/") else "") for top, n in groups.items())


def markdown(result):
    stop = result["stop"]
    out = [
        "## 서버 시험 실행", "",
        f"- 명령: `{' '.join(result['command'])}`",
        f"- 준비 줄 (`{result['ready_pattern']}`): " + (f"{result['ready_after']}초 뒤 `{result['ready_line']}`" if result["ready_line"] else "없음"),
    ]
    for s in result["sends"]:
        state = {True: "답함", False: "답 없음", None: "답인지 알 수 없음"}[s["answered"]]
        out += ["", f"### `{s['command'][:80]}` → {state}" + (f" ({s['error']})" if s.get("error") else ""),
                "", "```", *s["output"], "```"]
    out += ["", f"### 정지 `{stop['command']}` → " + (f"종료 코드 {stop['exit_code']}" if stop["exited"] else "끝나지 않음 (강제 종료)"),
            "", "```", *stop["output"], "```", "",
            "- 생긴 파일: " + file_list(result["created"]),
            "- 바뀐 파일: " + file_list(result["changed"])]
    found = notes(result)
    if found:
        out += ["", "### 확인할 것", *[f"- {n}" for n in found]]
    return "\n".join(out) + "\n"


def main():
    parser = argparse.ArgumentParser(description="서버를 파이프로 띄워 준비·명령·정지 원문을 기록한다")
    parser.add_argument("--cwd", required=True, help="서버 폴더 (작업 폴더)")
    parser.add_argument("--ready", help="준비 완료 줄 정규식")
    parser.add_argument("--send", action="append", default=[], help="준비 뒤 보낼 명령 (여러 번)")
    parser.add_argument("--stop", help="정지 명령")
    parser.add_argument("--timeout", type=float, default=120, help="준비·정지를 기다릴 최대 초")
    parser.add_argument("--settle", type=float, default=3, help="명령 뒤 이만큼 조용하면 답이 끝난 것으로 본다")
    parser.add_argument("--encoding", default="utf-8", help="콘솔 출력 인코딩")
    parser.add_argument("--out", help="보고서 폴더")
    parser.add_argument("program", nargs=argparse.REMAINDER, help="-- 뒤에 실행 파일과 인자")
    args = parser.parse_args()
    program = args.program[1:] if args.program[:1] == ["--"] else args.program
    if not program:
        parser.error("-- 뒤에 실행 파일을 준다")
    try:
        result = probe(program, args.cwd, args.ready, args.send, args.stop, args.timeout, args.settle, args.encoding)
    except (ValueError, OSError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    report = markdown(result)
    if args.out:
        out = pathlib.Path(args.out)
        out.mkdir(parents=True, exist_ok=True)
        (out / "probe-output.log").write_bytes(result["raw"])
        (out / "probe-report.md").write_text(report, encoding="utf-8")
        data = {k: v for k, v in result.items() if k != "raw"}
        (out / "probe-report.json").write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    sys.stdout.reconfigure(encoding="utf-8")
    print(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
