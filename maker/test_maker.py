"""모듈 메이커 도구 (new_module, inspect_dist, probe_server, check)"""
import importlib.util
import io
import json
import os
import pathlib
import subprocess
import sys
import tarfile
import textwrap
import time
import zipfile

import pytest

MAKER = pathlib.Path(__file__).resolve().parent
REPO = MAKER.parent


def load(name):
    spec = importlib.util.spec_from_file_location(f"maker_{name}", MAKER / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


new_module = load("new_module")
inspect_dist = load("inspect_dist")
probe_server = load("probe_server")
check = load("check")
prior_art = load("prior_art")


@pytest.fixture
def repo(tmp_path):
    """틀(__example)과 pytest.ini만 있는 가짜 모듈 저장소"""
    root = tmp_path / "repo"
    root.mkdir()
    import shutil
    shutil.copytree(REPO / "__example", root / "__example", ignore=shutil.ignore_patterns("__pycache__"))
    (root / "pytest.ini").write_text("[pytest]\naddopts = --import-mode=importlib\ntestpaths =\n    minecraft\n", encoding="utf-8")
    return root


# ── new_module ──

def test_new_module_copies_the_template_under_the_module_name(repo):
    folder = new_module.create(repo, "mygamex", "My Game X")

    assert folder == repo / "mygamex"
    assert sorted(p.name for p in folder.iterdir()) == ["RESEARCH.md", "icon.png", "lifecycle.py", "locales", "module.toml", "test_lifecycle.py"]
    import tomllib
    module = tomllib.loads((folder / "module.toml").read_text(encoding="utf-8"))["module"]
    assert (module["name"], module["game_name"], module["display_name"], module["version"]) == ("mygamex", "My Game X", "My Game X", "0.1.0")
    assert module["description"] == "My Game X server module"
    text = (folder / "module.toml").read_text(encoding="utf-8")
    assert 'module_aliases = ["My Game X", "mygamex"]' in text
    assert "MyGameServer" in text  # 실행 파일 이름은 조사로 채운다


def test_new_module_test_file_loads_the_new_lifecycle(repo):
    folder = new_module.create(repo, "mygamex", "My Game X")

    test = (folder / "test_lifecycle.py").read_text(encoding="utf-8")
    assert "__example__lifecycle.py" not in test
    assert '"lifecycle.py"' in test


def test_new_module_fills_every_language_from_english(repo):
    folder = new_module.create(repo, "mygamex", "My Game X")

    names = sorted(p.name for p in (folder / "locales").iterdir())
    assert names == sorted(f"{lang}.json" for lang in new_module.LANGUAGES)
    en = json.loads((folder / "locales" / "en.json").read_text(encoding="utf-8"))
    assert json.loads((folder / "locales" / "de.json").read_text(encoding="utf-8")) == en
    ko = json.loads((folder / "locales" / "ko.json").read_text(encoding="utf-8"))
    assert ko != en  # 틀의 한국어를 그대로 쓴다


def test_new_module_registers_the_tests(repo):
    new_module.create(repo, "mygamex", "My Game X")

    assert "    mygamex\n" in (repo / "pytest.ini").read_text(encoding="utf-8")


def test_new_module_writes_a_research_table_for_the_game(repo):
    folder = new_module.create(repo, "mygamex", "My Game X")

    research = (folder / "RESEARCH.md").read_text(encoding="utf-8")
    assert research.startswith("# My Game X 서버 조사")
    assert "| 출처 |" in research


@pytest.mark.parametrize("name", ["", "MyGame", "my game", "../evil", "1game", "__example"])
def test_new_module_refuses_bad_names(repo, name):
    with pytest.raises(ValueError):
        new_module.create(repo, name, "Game")


def test_new_module_never_overwrites(repo):
    (repo / "mygamex").mkdir()
    (repo / "mygamex" / "keep.txt").write_text("x")

    with pytest.raises(FileExistsError):
        new_module.create(repo, "mygamex", "Game")
    assert (repo / "mygamex" / "keep.txt").read_text() == "x"


# ── inspect_dist ──

def make_zip(path, files):
    with zipfile.ZipFile(path, "w") as z:
        for name, data in files.items():
            z.writestr(name, data)
    return path


def test_inspect_lists_every_entry_and_os_folders(tmp_path):
    archive = make_zip(tmp_path / "server.zip", {
        "1450/Windows/TerrariaServer.exe": b"MZ",
        "1450/Linux/TerrariaServer.exe": b"MZ",
        "1450/Linux/TerrariaServer.bin.x86_64": b"\x7fELF",
        "1450/Mac/TerrariaServer.app/x": b"",
    })

    info = inspect_dist.inspect(archive)

    assert info["count"] == 4
    assert info["os_folders"] == {"Linux": ["1450/Linux"], "Mac": ["1450/Mac"], "Windows": ["1450/Windows"]}
    assert info["duplicate_executables"] == {"TerrariaServer.exe": ["1450/Linux/TerrariaServer.exe", "1450/Windows/TerrariaServer.exe"]}


def test_inspect_reads_the_original_bytes_of_config_files(tmp_path):
    archive = make_zip(tmp_path / "server.zip", {
        "Windows/serverconfig.txt": b"#Don\x92t change\r\n#maxplayers=8\r\npriority=1\r\n",
        "Windows/bom.json": b"\xef\xbb\xbf{\"a\": 1}\n",
        "Windows/plain.ini": b"[s]\nkey=v\n",
    })

    files = {f["path"]: f for f in inspect_dist.inspect(archive)["config_files"]}

    legacy = files["Windows/serverconfig.txt"]
    assert legacy["encoding"] == "not utf-8"
    assert legacy["first_bad_byte"] == {"offset": 4, "byte": "0x92"}
    assert legacy["newline"] == "CRLF"
    assert legacy["commented_keys"] == ["maxplayers"]
    assert legacy["keys"] == {"priority": "1"}
    assert files["Windows/bom.json"]["encoding"] == "utf-8-sig"
    assert files["Windows/plain.ini"] == {**files["Windows/plain.ini"], "encoding": "ascii", "newline": "LF", "keys": {"key": "v"}}


@pytest.mark.parametrize("encoding, bom, name", [
    ("utf-16-le", b"\xff\xfe", "utf-16-le"), ("utf-16-be", b"\xfe\xff", "utf-16-be"),
    ("utf-32-le", b"\xff\xfe\x00\x00", "utf-32-le"), ("utf-32-be", b"\x00\x00\xfe\xff", "utf-32-be"),
])
def test_inspect_reads_wide_encodings_by_their_bom(tmp_path, encoding, bom, name):
    archive = make_zip(tmp_path / "server.zip", {"cfg.ini": bom + "a=1\r\nb=2\r\n".encode(encoding)})

    info = inspect_dist.inspect(archive)["config_files"][0]

    assert (info["encoding"], info["newline"], info["keys"]) == (name, "CRLF", {"a": "1", "b": "2"})


def test_inspect_handles_tar_and_folders(tmp_path):
    tar = tmp_path / "s.tar.gz"
    with tarfile.open(tar, "w:gz") as t:
        data = b"a=1\n"
        member = tarfile.TarInfo("server/linux64/server.cfg")
        member.size = len(data)
        t.addfile(member, io.BytesIO(data))
    folder = tmp_path / "dir"
    (folder / "win64").mkdir(parents=True)
    (folder / "win64" / "s.ini").write_bytes(b"a=1\n")

    assert inspect_dist.inspect(tar)["os_folders"] == {"Linux": ["server/linux64"]}
    assert inspect_dist.inspect(folder)["os_folders"] == {"Windows": ["win64"]}


def test_inspect_markdown_names_the_findings(tmp_path):
    archive = make_zip(tmp_path / "server.zip", {"Windows/a.exe": b"", "Linux/a.exe": b"", "Linux/c.txt": b"\x92"})

    text = inspect_dist.markdown(inspect_dist.inspect(archive))

    assert "Windows/a.exe" in text and "Linux/a.exe" in text
    assert "not utf-8" in text


# ── probe_server ──

FAKE_SERVER = textwrap.dedent("""
    import sys, time, pathlib
    quiet = "--ignore-stdin" in sys.argv
    print("Loading world...", flush=True)
    print("[12:00:00] [Server thread/INFO]: Done (1.0s)! For help, type \\"help\\"", flush=True)
    if quiet:
        time.sleep(30)
        sys.exit(3)
    for line in sys.stdin:
        cmd = line.strip()
        if cmd == "help":
            print("/list\\n/say <message>\\n/stop", flush=True)
        elif cmd == "stop":
            print("Stopping the server", flush=True)
            pathlib.Path("world.dat").write_text("saved")
            sys.exit(0)
        else:
            print("Unknown command: " + cmd, flush=True)
""")


@pytest.fixture
def fake_server(tmp_path):
    script = tmp_path / "bin" / "server.py"
    script.parent.mkdir()
    script.write_text(FAKE_SERVER, encoding="utf-8")
    work = tmp_path / "work"
    work.mkdir()
    return script, work


def test_probe_records_ready_help_stop_and_saved_files(fake_server):
    script, work = fake_server

    result = probe_server.probe([sys.executable, str(script)], work, ready=r"Done \(", sends=["help", "death"],
                                stop="stop", timeout=20, settle=1.0)

    assert result["ready_line"].endswith('For help, type "help"')
    assert [s["command"] for s in result["sends"]] == ["help", "death"]
    assert result["sends"][0]["answered"] and "/say <message>" in result["sends"][0]["output"]
    assert result["sends"][1]["output"] == ["Unknown command: death"]
    assert result["stop"] == {"command": "stop", "output": ["Stopping the server"], "exit_code": 0, "exited": True}
    assert result["created"] == ["world.dat"]


def test_probe_reports_when_the_server_ignores_piped_input(fake_server):
    script, work = fake_server

    result = probe_server.probe([sys.executable, str(script), "--ignore-stdin"], work, ready=r"Done \(", sends=["help"],
                                stop="stop", timeout=3, settle=1.0)

    assert result["sends"][0]["answered"] is False
    assert result["stop"]["exited"] is False  # 도구가 띄운 자식만 끝낸다
    assert "pty" in probe_server.markdown(result)


def test_probe_cannot_tell_an_answer_from_a_noisy_log(tmp_path):
    work = tmp_path / "work"
    work.mkdir()
    script = tmp_path / "tick.py"
    script.write_text("import time\nwhile True:\n    print('tick', flush=True)\n    time.sleep(0.2)\n", encoding="utf-8")

    result = probe_server.probe([sys.executable, str(script)], work, sends=["help"], stop="stop", timeout=3, settle=1.0)

    assert result["sends"][0]["answered"] is None  # 출력은 있었지만 명령의 답인지 알 수 없다
    assert "알 수 없" in probe_server.markdown(result)


def test_probe_bounds_a_blocked_stdin_write(tmp_path):
    work = tmp_path / "work"
    work.mkdir()
    script = tmp_path / "deaf.py"
    script.write_text("import time\nprint('ready', flush=True)\ntime.sleep(30)\n", encoding="utf-8")

    started = time.monotonic()
    result = probe_server.probe([sys.executable, str(script)], work, ready="ready", sends=["x" * (1 << 20)],
                                stop="stop", timeout=2, settle=0.5)

    assert time.monotonic() - started < 15
    assert "stdin" in result["sends"][0]["error"]
    assert result["stop"]["exited"] is False


def test_probe_rejects_a_bad_pattern_before_starting(tmp_path):
    marker = tmp_path / "started"
    script = tmp_path / "s.py"
    script.write_text(f"open({str(marker)!r}, 'w').close()\n", encoding="utf-8")

    with pytest.raises(ValueError):
        probe_server.probe([sys.executable, str(script)], tmp_path, ready="[", timeout=2)
    assert not marker.exists()


def test_probe_reports_a_missing_ready_line(fake_server):
    script, work = fake_server

    result = probe_server.probe([sys.executable, str(script)], work, ready=r"never matches", sends=[],
                                stop="stop", timeout=2, settle=0.5)

    assert result["ready_line"] is None
    assert result["stop"]["exited"] is True


# ── check ──

def finished_module(repo):
    """자리표시·조사 표를 채운 새 모듈"""
    folder = new_module.create(repo, "mygamex", "My Game X")
    path = folder / "module.toml"
    path.write_text(path.read_text(encoding="utf-8").replace("MyGameServer", "XServer"), encoding="utf-8")
    for lang in new_module.LANGUAGES:
        if lang not in ("en", "ko"):
            locale = folder / "locales" / f"{lang}.json"
            data = json.loads(locale.read_text(encoding="utf-8"))
            data["settings"]["server_name"]["label"] = f"[{lang}] name"
            locale.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    (folder / "RESEARCH.md").write_text("# 조사\n\n| 항목 | 값 | 출처 |\n|---|---|---|\n| 포트 | 1 | https://example.com 문서 |\n", encoding="utf-8")
    # 실측 로그에서 만든 하이라이트 규칙 하나 (틀의 예시 규칙만 두면 관문이 막는다)
    with path.open("a", encoding="utf-8") as f:
        f.write(MEASURED_RULE)
    return folder


MEASURED_RULE = "\n[[syntax_highlight.rules]]\nname = \"server_ready\"\npattern = '^(?P<hl>X Server ready)$'\ncolor = \"success\"\n"


def statuses(results):
    return {r["gate"]: r["status"] for r in results}


def test_check_static_passes_a_finished_module(repo, tmp_path):
    folder = finished_module(repo)
    report = probe_report(tmp_path / "probe-report.json", [("help", ["say <message>", "save", "kick <player>", "quit"])])

    result = statuses(check.static_gates(folder, repo, help_output=report))

    assert set(result.values()) == {"ok"}, result


def test_check_static_flags_leftover_placeholders_and_empty_research(repo):
    folder = new_module.create(repo, "mygamex", "My Game X")

    result = statuses(check.static_gates(folder, repo))

    assert result["placeholders"] == "fail"
    assert result["research"] == "fail"


def test_check_static_flags_highlight_rules_left_from_the_template(repo):
    """새 모듈 틀의 예시 규칙(WARN:, ERROR:, INFO:, IP)만 있으면 그 게임의 콘솔을 본 적이 없는 것이다"""
    folder = finished_module(repo)
    path = folder / "module.toml"
    path.write_text(path.read_text(encoding="utf-8").replace(MEASURED_RULE, ""), encoding="utf-8")

    assert statuses(check.static_gates(folder, repo))["highlight"] == "fail"


def test_check_highlight_compares_with_the_makers_own_template(repo, tmp_path):
    """저장소 밖 모듈을 검사해도(넘긴 repo에 __example이 없어도) 틀 예시 규칙만 있으면 실패한다"""
    folder = finished_module(repo)
    path = folder / "module.toml"
    path.write_text(path.read_text(encoding="utf-8").replace(MEASURED_RULE, ""), encoding="utf-8")
    elsewhere = tmp_path / "outside"
    elsewhere.mkdir()

    assert statuses(check.static_gates(folder, elsewhere))["highlight"] == "fail"


def test_check_static_flags_a_download_module_without_installed_version(repo):
    """모듈이 직접 내려받아 설치하면(download) 설치된 버전도 알려야 한다 (server version, 업데이트 비교)"""
    folder = finished_module(repo)
    path = folder / "module.toml"
    path.write_text(path.read_text(encoding="utf-8").replace('method = "steamcmd"', 'method = "download"', 1), encoding="utf-8")

    assert statuses(check.static_gates(folder, repo))["installed-version"] == "fail"

    lifecycle = folder / "lifecycle.py"
    lifecycle.write_text(lifecycle.read_text(encoding="utf-8").replace(
        "FUNCTIONS = {", 'def get_installed_version(config):\n    return {"success": True, "version": "1"}\n\n\n'
        'FUNCTIONS = {\n    "get_installed_version": get_installed_version,', 1), encoding="utf-8")
    assert statuses(check.static_gates(folder, repo))["installed-version"] == "ok"


def test_check_static_flags_missing_languages_and_untranslated_korean(repo):
    folder = finished_module(repo)
    (folder / "locales" / "ja.json").unlink()
    ko = json.loads((folder / "locales" / "ko.json").read_text(encoding="utf-8"))
    ko["settings"]["server_name"].pop("label")
    (folder / "locales" / "ko.json").write_text(json.dumps(ko, ensure_ascii=False), encoding="utf-8")

    details = {r["gate"]: r for r in check.static_gates(folder, repo)}

    assert details["locales"]["status"] == "fail"
    assert "ja.json" in details["locales"]["detail"]
    assert "settings.server_name.label" in details["locales"]["detail"]


def test_check_static_flags_languages_left_as_english_copies(repo):
    folder = finished_module(repo)
    en = (folder / "locales" / "en.json").read_text(encoding="utf-8")
    (folder / "locales" / "fr.json").write_text(en, encoding="utf-8")

    details = {r["gate"]: r for r in check.static_gates(folder, repo)}

    assert details["locales"]["status"] == "fail"
    assert "fr.json" in details["locales"]["detail"]


def test_check_static_flags_unregistered_tests(repo):
    folder = finished_module(repo)
    (repo / "pytest.ini").write_text("[pytest]\ntestpaths =\n    minecraft\n", encoding="utf-8")

    assert statuses(check.static_gates(folder, repo))["tests-registered"] == "fail"


def test_check_version_bump_against_the_base(repo):
    folder = finished_module(repo)
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=repo, check=True, capture_output=True, stdin=subprocess.DEVNULL)
    subprocess.run(["git", "add", "-A"], cwd=repo, check=True, capture_output=True, stdin=subprocess.DEVNULL)
    subprocess.run(["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "base"], cwd=repo, check=True, capture_output=True, stdin=subprocess.DEVNULL)
    toml = folder / "module.toml"
    toml.write_text(toml.read_text(encoding="utf-8").replace('version = "0.1.0"', 'version = "0.1.1"', 1), encoding="utf-8")

    result = {r["gate"]: r for r in check.static_gates(folder, repo, base="main")}

    assert result["version"]["status"] == "fail"
    assert "0.1.0" in result["version"]["detail"] and "0.1.1" in result["version"]["detail"]

    unknown = {r["gate"]: r for r in check.static_gates(folder, repo, base="no-such-ref")}
    assert unknown["version"]["status"] == "fail"
    assert "no-such-ref" in unknown["version"]["detail"]


def test_check_version_failure_tells_how_to_refresh_a_stale_base(repo):
    """로컬 origin/main이 오래되면 이미 발행된 버전도 '올렸다'로 보인다: 기준을 새로 받으라고 알린다"""
    folder = finished_module(repo)
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=repo, check=True, capture_output=True, stdin=subprocess.DEVNULL)
    subprocess.run(["git", "add", "-A"], cwd=repo, check=True, capture_output=True, stdin=subprocess.DEVNULL)
    subprocess.run(["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "base"], cwd=repo, check=True, capture_output=True, stdin=subprocess.DEVNULL)
    toml = folder / "module.toml"
    toml.write_text(toml.read_text(encoding="utf-8").replace('version = "0.1.0"', 'version = "0.1.1"', 1), encoding="utf-8")

    detail = {r["gate"]: r for r in check.static_gates(folder, repo, base="main")}["version"]["detail"]

    assert "git fetch" in detail


SABA_CORE_TABLE = "\n[extensions.saba-core]\n"


def add_to_manifest(folder, text):
    path = folder / "module.toml"
    path.write_text(path.read_text(encoding="utf-8") + text, encoding="utf-8")


def test_check_optional_lists_what_the_module_supports(repo):
    """선택 기능은 넣지 않아도 통과하되, 무엇을 넣고 무엇을 뺐는지 보고에 보인다"""
    folder = finished_module(repo)

    bare = {r["gate"]: r for r in check.static_gates(folder, repo)}["optional"]
    assert bare["status"] == "ok"
    assert "게임 채팅" in bare["detail"]

    add_to_manifest(folder, SABA_CORE_TABLE + "chat_patterns = ['^<(?P<name>\\w+)> (?P<message>.+)$']\n")
    declared = {r["gate"]: r for r in check.static_gates(folder, repo)}["optional"]
    assert declared["status"] == "ok"
    assert declared["detail"].index("게임 채팅") < declared["detail"].index("안 넣음")


def test_check_optional_flags_chat_patterns_without_both_groups(repo):
    """데몬은 name·message 그룹이 둘 다 없는 채팅 패턴을 경고만 남기고 버린다: 조용히 채팅이 안 온다"""
    folder = finished_module(repo)
    add_to_manifest(folder, SABA_CORE_TABLE + "chat_patterns = ['^<(?P<name>\\w+)> .+$']\n")

    result = {r["gate"]: r for r in check.static_gates(folder, repo)}["optional"]

    assert result["status"] == "fail"
    assert "message" in result["detail"]


def test_check_optional_flags_chat_send_without_the_function(repo):
    """chat_send가 FUNCTIONS에 없는 함수를 가리키면 디스코드 → 게임 채팅이 실패한다"""
    folder = finished_module(repo)
    add_to_manifest(folder, SABA_CORE_TABLE + 'chat_send = "send_chat"\n')

    result = {r["gate"]: r for r in check.static_gates(folder, repo)}["optional"]
    assert result["status"] == "fail"
    assert "send_chat" in result["detail"]

    lifecycle = folder / "lifecycle.py"
    lifecycle.write_text(lifecycle.read_text(encoding="utf-8").replace(
        "FUNCTIONS = {", 'def send_chat(config):\n    return {"success": True}\n\n\n'
        'FUNCTIONS = {\n    "send_chat": send_chat,', 1), encoding="utf-8")
    assert {r["gate"]: r for r in check.static_gates(folder, repo)}["optional"]["status"] == "ok"


def probe_report(path, sends):
    """probe_server.py --out이 남기는 probe-report.json 모양"""
    path.write_text(json.dumps({"sends": [{"command": c, "output": out, "answered": True, "error": None} for c, out in sends]}),
                    encoding="utf-8")
    return path


def test_check_commands_flags_declared_commands_the_server_help_lacks(repo, tmp_path):
    """틀은 say·save·kick·quit을 선언한다. 서버 help에 kick이 없으면 그 명령은 서버에서 Unknown command로 끝난다"""
    folder = finished_module(repo)
    # 시험으로 보낸 kick의 답(Unknown command: kick)이 help 대신 읽히면 안 된다
    report = probe_report(tmp_path / "probe-report.json", [("help", ["/say <message>", "/save", "/quit"]),
                                                            ("kick nobody", ["Unknown command: kick"])])

    result = {r["gate"]: r for r in check.static_gates(folder, repo, help_output=report)}["commands"]

    assert result["status"] == "fail"
    assert "kick" in result["detail"] and "say" not in result["detail"]


def test_check_commands_passes_with_a_saved_help_list(repo, tmp_path):
    folder = finished_module(repo)
    saved = tmp_path / "help.txt"
    saved.write_text("say <message>\nsave\nkick <player> [reason]\nquit\n", encoding="utf-8")

    assert statuses(check.static_gates(folder, repo, help_output=saved))["commands"] == "ok"


def test_check_commands_is_not_a_pass_without_the_servers_help(repo):
    """help 출력 없이는 잴 수 없다: 건너뛴 관문은 통과가 아니다"""
    folder = finished_module(repo)

    result = {r["gate"]: r for r in check.static_gates(folder, repo)}["commands"]

    assert result["status"] == "skip"
    assert "--help-output" in result["detail"]


def test_check_commands_passes_a_module_without_a_console(repo):
    """stdin이 없는 모듈(팰월드: REST만)에는 help를 보낼 콘솔이 없다. 실측(trial.py)과 같이 대조하지 않는다.
    데몬이 보내지 않는 stop_command가 있어도 같다"""
    folder = finished_module(repo)
    path = folder / "module.toml"
    path.write_text(path.read_text(encoding="utf-8").replace('supported = ["stdin"]', 'supported = ["rest"]'), encoding="utf-8")

    assert statuses(check.static_gates(folder, repo))["commands"] == "ok"


def test_check_commands_takes_the_trial_help_result(tmp_path):
    """--trial은 진짜 서버에 help를 보내 같은 규칙으로 대조한다. help 출력을 따로 주지 않았으면 그 결과가 commands 관문이다"""
    skipped = [check.gate("placeholders", "ok"), check.gate("commands", "skip", "서버 help 출력이 없다")]
    out = tmp_path / "trial-report"
    out.mkdir()

    def with_help(status, detail):
        (out / "trial-report.json").write_text(json.dumps({"steps": [{"name": "help", "status": status, "detail": detail}]}),
                                               encoding="utf-8")
        return statuses(check.with_trial_help(skipped, out)), check.with_trial_help(skipped, out)[1]["detail"]

    assert with_help("ok", "선언한 콘솔 명령 4개가 help에 있다")[0] == {"placeholders": "ok", "commands": "ok"}
    gaps, detail = with_help("warn", "help에 없는 선언 명령: kick")
    assert gaps["commands"] == "fail" and "kick" in detail
    (out / "trial-report.json").unlink()
    assert check.with_trial_help(skipped, out) == skipped


def test_check_trial_sends_the_same_help_command(repo, tmp_path, monkeypatch):
    """--help-command는 정적 대조와 실측에 같이 쓴다: 실측만 `help`를 보내면 두 대조가 다른 명령의 답을 본다"""
    folder = finished_module(repo)
    sent = {}
    monkeypatch.setattr(check, "run", lambda name, command, cwd, env=None: sent.setdefault(name, command) and check.gate(name, "ok"))
    args = check.argparse.Namespace(shared_libs=None, app_shared_libs=None, saba_core=sys.executable, trial=True,
                                    trial_out=str(tmp_path / "trial-report"), accept_agreement=[], extensions=None,
                                    help_command="?")

    check.tool_gates(folder, REPO, args)

    assert sent["trial"][sent["trial"].index("--help-command") + 1] == "?"


def test_check_reads_its_own_version_and_requirement():
    plugin = json.loads((MAKER / ".claude-plugin" / "plugin.json").read_text(encoding="utf-8"))

    assert check.maker_version() == plugin["version"]
    assert check.requirement() == ">=0.1.14-beta.2"


@pytest.mark.parametrize("installed, status", [("0.1.10", "ok"), ("0.1.11", "ok"), ("0.2.0", "ok"), ("0.1.9", "fail")])
def test_check_compat_compares_saba_core_versions(installed, status):
    assert check.compat_gate(installed, ">=0.1.10")["status"] == status


def test_check_takes_paths_from_the_app(tmp_path):
    cli = tmp_path / "cli.py"
    cli.write_text("import json, sys\nassert sys.argv[1:] == ['module', 'maker', '--json']\n"
                   "print(json.dumps({'saba_core': {'path': 'C:/S/saba-core.exe', 'version': '0.1.10'}, 'shared_libs': 'C:/D/python-shared'}))\n",
                   encoding="utf-8")

    app = check.app_paths([sys.executable, str(cli)])

    assert app["saba_core"]["path"] == "C:/S/saba-core.exe"
    assert check.app_paths([sys.executable, "-c", "import sys; sys.exit(2)"]) is None


def test_check_trial_needs_saba_core(repo):
    folder = finished_module(repo)

    env = {**os.environ, "SABA_CHAN_CLI": "no-such-cli"}  # 앱에서 saba-core 경로를 얻지 못하게
    done = subprocess.run([sys.executable, str(MAKER / "check.py"), str(folder), "--trial"], capture_output=True, text=True,
                          stdin=subprocess.DEVNULL, env=env)

    assert done.returncode == 2
    assert "--saba-core" in done.stderr


# ─── prior_art: 다른 관리 도구(Pelican eggs, LinuxGSM)의 단서 ────────────────

EGG = {
    "meta": {"version": "PLCN_v3"},
    "name": "Valheim",
    "docker_images": {"Wine": "ghcr.io/parkervcp/yolks:wine_latest"},
    "startup_commands": {"Default": "wine ./valheim_server.exe -port {{SERVER_PORT}} -world \"{{WORLD}}\""},
    "config": {"files": "{\"BepInEx/config.cfg\": {}}", "startup": "{\r\n    \"done\": \"DungeonDB Start\"\r\n}", "logs": "{}", "stop": "^C"},
    "scripts": {"installation": {"script": "./steamcmd.sh +login anonymous +app_update ${SRCDS_APPID} validate +quit"}},
    "variables": [
        {"name": "World", "env_variable": "WORLD", "default_value": "Dedicated", "description": "World name"},
        {"name": "App ID", "env_variable": "SRCDS_APPID", "default_value": "896660", "description": ""},
        {"name": "Query Port", "env_variable": "QUERY_PORT", "default_value": "2457", "description": "Steam query"},
    ],
}

LGSM_CSV = "shortname,gameservername,gamename,os\nvh,vhserver,Valheim,ubuntu-26.04\npz,pzserver,Project Zomboid,ubuntu-26.04\n"

LGSM_CFG = """# comment
port="2456"
startparameters="-name '${servername}' -port ${port}"
appid="896660"
steamcmdforcewindows="no"
stopmode="2"
executable="./valheim_server.x86_64"
discordalert="off"
"""


def test_prior_art_reads_the_leads_an_egg_gives():
    """egg에서 조사 표 칸으로 옮길 단서: app id, 익명 설치, 실행 인자, 준비 줄, 정지 방법(^C = Ctrl+C), 포트, 변수"""
    lead = prior_art.read_egg(EGG, "https://example.invalid/egg-valheim.json")

    assert lead["app_id"] == "896660"
    assert lead["anonymous"] is True
    assert lead["startup"] == ["wine ./valheim_server.exe -port {{SERVER_PORT}} -world \"{{WORLD}}\""]
    assert lead["ready"] == ["DungeonDB Start"]
    assert lead["stop"] == "^C" and lead["stop_signal"] == "interrupt"
    assert lead["ports"] == {"QUERY_PORT": "2457"}
    assert lead["windows_via_wine"] is True
    assert lead["config_files"] == ["BepInEx/config.cfg"]
    assert ("WORLD", "Dedicated", "World name") in lead["variables"]


def test_prior_art_names_the_stop_command_when_it_is_not_ctrl_c():
    egg = dict(EGG, config=dict(EGG["config"], stop="quit"), docker_images={"Java": "ghcr.io/parkervcp/yolks:java_21"})
    egg["scripts"] = {"installation": {"script": "steamcmd +login ${STEAM_USER} ${STEAM_PASS} +app_update ${STEAMCMD_APPID}"}}
    egg["variables"] = [{"env_variable": "STEAM_USER", "default_value": "", "rules": ["required", "string"]},
                        {"env_variable": "STEAMCMD_APPID", "default_value": "233780", "rules": "required|integer"}]
    egg["startup_commands"] = {"Default": "./arma3server_x64 -port={{SERVER_PORT}}"}

    lead = prior_art.read_egg(egg, "u")

    assert lead["stop"] == "quit" and lead["stop_signal"] is None
    assert lead["app_id"] == "233780"
    assert lead["anonymous"] is False, "a required Steam user means the server needs an account"
    assert lead["windows_via_wine"] is False


def test_prior_art_treats_the_shared_installer_fallback_as_anonymous():
    """Pelican 공용 설치 스크립트는 STEAM_USER가 비면 anonymous로 로그인한다 (계정이 필요한 게임은 STEAM_USER가 필수 변수)"""
    egg = dict(EGG, scripts={"installation": {"script": 'if [[ "${STEAM_USER}" == "" ]]; then\r\n    STEAM_USER=anonymous\r\nfi\r\n./steamcmd.sh +login ${STEAM_USER} ${STEAM_PASS} +app_update ${SRCDS_APPID}'}})

    assert prior_art.read_egg(egg, "u")["anonymous"] is True


def test_prior_art_finds_eggs_by_name_and_prefers_the_pelican_copy():
    tree = [
        "valheim/valheim_vanilla/egg-valheim.json",
        "valheim/valheim_vanilla/egg-pterodactyl-valheim.json",
        "valheim/valheim_bepinex/egg-pterodactyl-valheim-bep-i-nex.json",
        "v_rising/v_rising_vanilla/egg-v-rising.json",
        "team_fortress_2/egg-team-fortress2.yaml",
        "valheim/README.md",
    ]

    assert prior_art.match_eggs("Valheim", tree) == [
        "valheim/valheim_bepinex/egg-pterodactyl-valheim-bep-i-nex.json",
        "valheim/valheim_vanilla/egg-valheim.json",
    ]
    assert prior_art.match_eggs("V Rising", tree) == ["v_rising/v_rising_vanilla/egg-v-rising.json"]
    # Pelican판이 YAML이면 이 도구가 읽지 못한다: 같은 폴더의 Pterodactyl JSON을 버리지 않는다
    yaml_beside_json = [
        "valheim/valheim_bepinex/egg-valheim-bep-in-ex.yaml",
        "valheim/valheim_bepinex/egg-pterodactyl-valheim-bep-i-nex.json",
    ]
    assert prior_art.match_eggs("Valheim", yaml_beside_json) == yaml_beside_json[::-1]
    assert prior_art.match_eggs("Team Fortress 2", tree) == ["team_fortress_2/egg-team-fortress2.yaml"]


def test_prior_art_reads_linuxgsm_rows_and_the_keys_a_module_needs():
    assert prior_art.match_lgsm("valheim", LGSM_CSV) == [{"shortname": "vh", "gameservername": "vhserver", "gamename": "Valheim"}]
    assert prior_art.match_lgsm("pz", LGSM_CSV)[0]["gameservername"] == "pzserver"
    assert prior_art.match_lgsm("Rust", LGSM_CSV) == []

    lead = prior_art.read_lgsm_cfg(LGSM_CFG)

    assert lead["keys"]["appid"] == "896660"
    assert lead["keys"]["port"] == "2456"
    assert "discordalert" not in lead["keys"], "LinuxGSM's own alert settings are not leads"
    assert lead["stop"] == "Ctrl+C" and lead["stop_signal"] == "interrupt"
    assert lead["needs_login"] is False
    # stopmode 16도 Ctrl+C로 끝낸다(command_stop.sh fn_stop_graceful_ce). 콘솔 명령 모드는 후보가 아니다
    assert prior_art.read_lgsm_cfg('stopmode="16"')["stop_signal"] == "interrupt"
    assert prior_art.read_lgsm_cfg('stopmode="3"')["stop_signal"] is None


def test_prior_art_report_marks_everything_as_a_lead_with_its_source(tmp_path):
    """보고서는 단서다: 출처 URL을 달되, 조사 표의 근거(문서·실측)로 쓰지 말라고 적는다"""
    egg = prior_art.read_egg(EGG, "https://raw.githubusercontent.com/pelican-eggs/games-steamcmd/main/valheim/valheim_vanilla/egg-valheim.json")
    lgsm = dict(prior_art.read_lgsm_cfg(LGSM_CFG), url="https://raw.githubusercontent.com/GameServerManagers/LinuxGSM/master/lgsm/config-default/config-lgsm/vhserver/_default.cfg", name="vhserver")

    text = prior_art.markdown("Valheim", [egg], [lgsm], missing=[])

    assert "단서" in text and "RESEARCH.md" in text
    assert "stop_signal = \"interrupt\"" in text
    assert "896660" in text and "DungeonDB Start" in text
    assert egg["url"] in text and lgsm["url"] in text
    assert "MIT" in text


def test_prior_art_runs_offline_from_saved_files(tmp_path):
    egg_file = tmp_path / "egg.json"
    egg_file.write_text(json.dumps(EGG), encoding="utf-8")
    cfg_file = tmp_path / "_default.cfg"
    cfg_file.write_text(LGSM_CFG, encoding="utf-8")
    out = tmp_path / "prior-art.md"

    code = prior_art.main(["Valheim", "--egg-file", str(egg_file), "--lgsm-file", str(cfg_file), "--out", str(out)])

    assert code == 0
    assert "DungeonDB Start" in out.read_text(encoding="utf-8")


def test_check_research_refuses_rows_that_cite_only_other_tools(repo):
    """다른 관리 도구는 단서일 뿐 출처가 아니다: 출처가 egg·LinuxGSM뿐인 조사 표 줄은 관문을 막는다"""
    folder = finished_module(repo)
    (folder / "RESEARCH.md").write_text(
        "| 항목 | 값 | 근거 | 출처 |\n|---|---|---|---|\n"
        "| 포트 | 2456 | 문서 | https://valheimgame.com/support/a-guide-to-dedicated-servers/ |\n"
        "| 정지 | Ctrl+C | 문서 | https://github.com/pelican-eggs/games-steamcmd/blob/main/valheim/valheim_vanilla/egg-valheim.json |\n",
        encoding="utf-8",
    )

    result = check.research_gate(folder)

    assert result["status"] == "fail"
    assert "정지" in result["detail"]


def test_check_research_accepts_a_measured_row_that_also_links_another_tool(repo):
    """근거가 실측이면 출처 칸에 egg 링크를 곁들여도 된다: 근거는 실측 로그다"""
    folder = finished_module(repo)
    (folder / "RESEARCH.md").write_text(
        "| 항목 | 값 | 근거 | 출처 |\n|---|---|---|---|\n"
        "| 정지 | Ctrl+C | 실측 | probe/probe-output.log, https://github.com/pelican-eggs/games-steamcmd/blob/main/valheim/valheim_vanilla/egg-valheim.json |\n",
        encoding="utf-8",
    )

    assert check.research_gate(folder)["status"] == "ok"
