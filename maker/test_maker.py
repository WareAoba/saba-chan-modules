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
            data["messages"]["stop_requested"] = f"[{lang}] stop"
            locale.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    (folder / "RESEARCH.md").write_text("# 조사\n\n| 항목 | 값 | 출처 |\n|---|---|---|\n| 포트 | 1 | https://example.com 문서 |\n", encoding="utf-8")
    # 실측 로그에서 만든 하이라이트 규칙 하나 (틀의 예시 규칙만 두면 관문이 막는다)
    with path.open("a", encoding="utf-8") as f:
        f.write(MEASURED_RULE)
    return folder


MEASURED_RULE = "\n[[syntax_highlight.rules]]\nname = \"server_ready\"\npattern = '^(?P<hl>X Server ready)$'\ncolor = \"success\"\n"


def statuses(results):
    return {r["gate"]: r["status"] for r in results}


def test_check_static_passes_a_finished_module(repo):
    folder = finished_module(repo)

    result = statuses(check.static_gates(folder, repo))

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
    ko["messages"].pop("stop_requested")
    (folder / "locales" / "ko.json").write_text(json.dumps(ko, ensure_ascii=False), encoding="utf-8")

    details = {r["gate"]: r for r in check.static_gates(folder, repo)}

    assert details["locales"]["status"] == "fail"
    assert "ja.json" in details["locales"]["detail"]
    assert "messages.stop_requested" in details["locales"]["detail"]


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


def test_check_reads_its_own_version_and_requirement():
    plugin = json.loads((MAKER / ".claude-plugin" / "plugin.json").read_text(encoding="utf-8"))

    assert check.maker_version() == plugin["version"]
    assert check.requirement() == ">=0.1.10"


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
