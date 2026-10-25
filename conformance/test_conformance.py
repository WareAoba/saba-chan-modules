"""모듈 계약 검사 (M10). 계약 원문은 docs/module-spec.md.

1. module.toml이 스키마 v1을 통과한다
2. 진입 파일의 `FUNCTIONS` 표: `get_launch_command`가 있고, 데몬이 부르는 이름으로
   만든 함수는 빠짐없이 표에 있다 (빠지면 데몬은 "Unknown function"을 받는다).
   훅을 켰거나 module.toml이 이름으로 가리킨 함수도 표에 있다
3. 모르는 함수에는 `{"success": false, "message": "Unknown function: X"}`
4. 부르면 안전한 함수들을 빈 작업 폴더에서 불러, 제한 시간 안에 종료 코드 0으로 끝나고
   stdout 마지막 줄이 JSON 객체이며, stderr의 `PROGRESS:` 줄이 JSON인지
5. 예약 함수 `functions`가 FUNCTIONS 표의 이름을 돌려준다
6. 코드에서 쓰는 번역 키가 `locales/en.json`에 있고, 다른 언어 파일의 키가 en과 같다
7. 명령마다 `risk`와 `description`이 있다
8. 바꾼 설정 값을 `configure`로 쓰고 `import_settings`로 읽으면 같은 값이다
9. 콘솔 없는 서버의 일반 정지는 저장 후 종료를 요청하거나, 길이 없으면 죽이지 않고 `graceful_stop_unavailable`로 알린다
10. `[protocols]`: rcon은 stdin·rest가 없을 때만, 그때 rcon_port
"""
import ast
import json
import os
import pathlib
import subprocess
import sys
import threading
import time
import warnings

import jsonschema
import pytest

REPO = pathlib.Path(__file__).resolve().parents[1]
SCHEMA = json.loads((REPO / "schema" / "module.schema.json").read_text(encoding="utf-8"))
# 0.1.8 데몬이 내보낸 스키마(saba-chan 77333cab, 모듈 5980f20). 모듈 업데이트는 코어보다 먼저 들어갈 수 있고
# 0.1.8 업데이터는 requires를 보지 않으므로, 새 모듈도 0.1.8 로더가 거부하지 않는 모양이어야 한다
SCHEMA_0_1_8 = json.loads((REPO / "schema" / "module.schema.0.1.8.json").read_text(encoding="utf-8"))

# 데몬이 이름으로 부르는 함수 (saba-chan src/에서 부르는 이름 전부)
DAEMON_FUNCTIONS = {
    "get_launch_command", "stop", "status", "command", "get_credentials",
    "validate", "configure", "read_properties", "reset_properties", "reset_server",
    "accept_agreements", "diagnose_log",
    "list_versions", "get_version_details", "install_server", "get_installed_version",
    "import_settings", "backup_paths", "backup_snapshots", "instance_commands",
}
# 켜면 데몬이 그 함수를 부르는 module.toml 훅
# (섹션, 키, 함수): 키가 true면 데몬이 그 함수를 부른다
HOOK_FUNCTIONS = [("backup", "hook", "backup_paths"), ("commands", "hook", "instance_commands"),
                  ("backup", "snapshots", "backup_snapshots")]
REQUIRED_FUNCTIONS = {"get_launch_command"}
# 빈 작업 폴더에서 불러도 아무것도 바꾸지 않는 함수
SAFE_FUNCTIONS = ["get_launch_command", "validate", "status", "read_properties", "diagnose_log", "get_installed_version"]
CALL_TIMEOUT = 30


def _host_call(argv, config, env, tmp_path, accepted_requests):
    """SDK의 실제 HOST 요청을 받아 성공 응답을 쓰고 요청 접수를 관찰한다."""
    host_dir = tmp_path / "host"
    host_dir.mkdir()
    env["SABA_HOST_DIR"] = str(host_dir)
    stdout, stderr = [], []
    with subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          text=True, encoding="utf-8", env=env, cwd=tmp_path) as proc:
        def respond():
            for line in proc.stderr:
                stderr.append(line)
                if not line.startswith("HOST:"):
                    continue
                request = json.loads(line.removeprefix("HOST:"))
                if request["op"] in ("instance.stdin", "instance.rest"):
                    accepted_requests.append(request)
                    data = {"success": True, "data": {"response": {}}}
                    reply = {"ok": True, "data": data}
                else:
                    reply = {"ok": False, "error_code": "op_not_allowed", "error": "unsupported operation"}
                pending = host_dir / f"{request['id']}.tmp"
                pending.write_text(json.dumps(reply), encoding="utf-8")
                pending.replace(host_dir / f"{request['id']}.json")

        reader = threading.Thread(target=respond, daemon=True)
        output = threading.Thread(target=lambda: stdout.append(proc.stdout.read()), daemon=True)
        reader.start()
        output.start()
        proc.stdin.write(config)
        proc.stdin.close()
        try:
            proc.wait(timeout=CALL_TIMEOUT)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            raise
        reader.join(timeout=5)
        output.join(timeout=5)
        return subprocess.CompletedProcess(argv, proc.returncode, "".join(stdout), "".join(stderr))


def call(module, shared_libs, function, config, tmp_path, accepted_requests=None):
    """데몬과 같은 방식으로 모듈 함수를 부른다."""
    env = {
        **os.environ,
        "PYTHONPATH": str(shared_libs),
        "PYTHONIOENCODING": "utf-8",
        "PYTHONUNBUFFERED": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        # 닫힌 포트: 데몬 역호출은 바로 실패한다
        "SABA_IPC_PORT": "9",
        "SABA_IPC_URL": "http://127.0.0.1:9",
        "SABA_TOKEN_PATH": str(tmp_path / "no-token"),
        "SABA_LANG": "en",
        "SABA_ENABLED_EXTENSIONS": "",
        # 사용자 폴더를 기본 위치로 쓰는 게임(예: ~/Zomboid)도 임시 폴더 안에만 쓰게 한다
        "HOME": str(tmp_path),
        "USERPROFILE": str(tmp_path),
    }
    for key in ("SABA_HOST_DIR", "SABA_LOOPBACK_DIR", "DAEMON_API_URL"):
        env.pop(key, None)
    argv = [sys.executable, str(module.entry), function]
    stdin = config if isinstance(config, str) else json.dumps(config)
    try:
        if accepted_requests is None:
            proc = subprocess.run(argv, input=stdin, capture_output=True, text=True, encoding="utf-8",
                                  env=env, cwd=tmp_path, timeout=CALL_TIMEOUT)
        else:
            proc = _host_call(argv, stdin, env, tmp_path, accepted_requests)
    except subprocess.TimeoutExpired:
        pytest.fail(f"{function}: {CALL_TIMEOUT}초 안에 끝나지 않았다")
    where = f"{module.name}.{function}"
    assert proc.returncode == 0, (
        f"{where}: 종료 코드 {proc.returncode}. 데몬은 종료 코드가 0이 아니면 stdout을 버린다.\n"
        f"stderr 끝:\n{proc.stderr[-1500:]}"
    )
    lines = [l for l in proc.stdout.splitlines() if l.strip()]
    assert lines, f"{where}: stdout이 비었다"
    try:
        result = json.loads(proc.stdout)
    except json.JSONDecodeError:
        try:
            result = json.loads(lines[-1])
        except json.JSONDecodeError:
            pytest.fail(f"{where}: stdout 전체도 마지막 줄도 JSON이 아니다: {lines[-1][:200]}")
    assert isinstance(result, dict), f"{where}: 결과가 JSON 객체가 아니다"
    for line in proc.stderr.splitlines():
        if line.startswith("PROGRESS:"):
            try:
                json.loads(line.removeprefix("PROGRESS:"))
            except json.JSONDecodeError:
                pytest.fail(f"{where}: PROGRESS 줄이 JSON이 아니다: {line[:200]}")
    return result


def functions_table(module):
    """진입 파일의 FUNCTIONS 표 (키 → 값 이름)와 최상위 함수 이름."""
    tree = ast.parse(module.entry.read_text(encoding="utf-8-sig"), filename=str(module.entry))
    defined = {n.name for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    table = None
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "FUNCTIONS" for t in node.targets):
            if isinstance(node.value, ast.Dict):
                table = {
                    k.value: (v.id if isinstance(v, ast.Name) else None)
                    for k, v in zip(node.value.keys, node.value.values)
                    if isinstance(k, ast.Constant) and isinstance(k.value, str)
                }
    return table, defined


def hook_functions_missing(manifest, table):
    """`hook = true`(·`[backup] snapshots = true`)인데 데몬이 부를 함수가 FUNCTIONS 표에 없는 것"""
    return [f"[{section}] {key} → {function}" for section, key, function in HOOK_FUNCTIONS
            if (manifest.get(section) or {}).get(key) is True and function not in table]


def flat_keys(obj, prefix=""):
    keys = set()
    for k, v in obj.items():
        keys |= flat_keys(v, f"{prefix}{k}.") if isinstance(v, dict) else {prefix + k}
    return keys


def sandbox_config(tmp_path):
    work = tmp_path / "server"
    work.mkdir(exist_ok=True)
    return {"instance_id": "conformance", "working_dir": str(work), "managed": True}


# ── 1. module.toml ──────────────────────────────────────

def test_manifest_matches_schema(module):
    errors = [
        f"{'.'.join(map(str, e.path)) or '(root)'}: {e.message}"
        for e in jsonschema.Draft7Validator(SCHEMA).iter_errors(module.manifest)
    ]
    assert not errors, "module.toml이 스키마 v1과 맞지 않는다:\n" + "\n".join(errors)


def _supports_core_0_1_8(manifest):
    """[module.dependencies] saba-core의 최소 버전이 0.1.8 이하면(또는 없으면) 0.1.8 데몬에서도 로드되어야 한다"""
    import re
    req = str(manifest.get("module", {}).get("dependencies", {}).get("saba-core", ""))
    m = re.search(r">=\s*(\d+)\.(\d+)\.(\d+)", req)
    return not m or tuple(map(int, m.groups())) <= (0, 1, 8)


def test_the_0_1_8_check_applies_only_to_modules_that_allow_that_core():
    """saba-core >=0.1.9를 요구하는 외부 모듈은 0.1.8 데몬이 아예 로드하지 않으므로 0.1.8 스키마로 검사하지 않는다 (PR #46 리뷰)"""
    deps = lambda v: {"module": {"dependencies": {"saba-core": v}}}
    assert _supports_core_0_1_8({}) and _supports_core_0_1_8(deps(">=0.1.5")) and _supports_core_0_1_8(deps(">=0.1.8"))
    assert not _supports_core_0_1_8(deps(">=0.1.9")) and not _supports_core_0_1_8(deps(">= 0.2.0"))


def test_manifest_still_loads_on_a_0_1_8_daemon(module):
    """0.1.8 로더는 schema v1의 모르는 키를 거부한다: 새 선언은 그 로더가 검사하지 않는 [extensions.saba-core]에 둔다"""
    if not _supports_core_0_1_8(module.manifest):
        pytest.skip("saba-core 0.1.9 이상을 요구하는 모듈")
    errors = [
        f"{'.'.join(map(str, e.path)) or '(root)'}: {e.message[:200]}"
        for e in jsonschema.Draft7Validator(SCHEMA_0_1_8).iter_errors(module.manifest)
    ]
    assert not errors, "0.1.8 데몬이 이 module.toml을 거부한다:\n" + "\n".join(errors)


def test_folder_name_is_module_name(module):
    assert module.dir.name == module.name, f"폴더 이름({module.dir.name})과 [module] name({module.name})이 다르다"


def test_entry_file_exists(module):
    assert module.entry.is_file(), f"[module] entry 파일이 없다: {module.entry}"


# 데몬이 콘솔 줄을 토큰으로 나눌 때 받는 이름 (saba-chan src/supervisor/highlight.rs TOKENS + 별칭)
HIGHLIGHT_TOKENS = {
    "error", "warn", "info", "debug", "success", "string", "number", "keyword",
    "timestamp", "player", "command", "selector", "ip", "uuid", "path", "dim",
    "warning", "trace", "comment",
}


def test_highlight_rules_fit_the_daemon(module):
    """하이라이팅 규칙은 데몬(Rust regex)이 돌린다: 색 대신 토큰, lookaround 없음."""
    problems = []
    for rule in (module.manifest.get("syntax_highlight") or {}).get("rules", []):
        if rule["color"].lower() not in HIGHLIGHT_TOKENS:
            problems.append(f"{rule['name']}: color '{rule['color']}'는 토큰이 아니다")
        if any(t in rule["pattern"] for t in ("(?=", "(?!", "(?<=", "(?<!")):
            problems.append(f"{rule['name']}: lookaround는 데몬 정규식이 지원하지 않는다")
    assert not problems, "\n".join(problems)


def commands_missing_risk(manifest):
    """`[[commands.fields]]`에 `risk`나 `description`이 없는 명령. risk가 없으면 데몬은 조용히 caution으로 본다"""
    fields = (manifest.get("commands") or {}).get("fields", [])
    return ([f"{c.get('name')}: risk" for c in fields if "risk" not in c]
            + [f"{c.get('name')}: description" for c in fields if not c.get("description")])


def test_commands_declare_risk_and_description(module):
    """명령의 위험도와 설명은 모듈이 스스로 판단해 적는다 (권한·챗봇 확인·AI가 읽는다)"""
    missing = commands_missing_risk(module.manifest)
    assert not missing, "명령에 빠진 선언:\n" + "\n".join(missing)


def reply_problems(manifest, locales):
    """`reply_pattern`이 있는 명령: 모든 언어 파일에 `commands.<이름>.done`이 있고, 그 `{{자리}}`가 패턴의 이름 있는 그룹이다"""
    import re
    problems = []
    for c in (manifest.get("commands") or {}).get("fields", []):
        pattern = c.get("reply_pattern")
        if not pattern:
            continue
        groups = set(re.compile(pattern).groupindex)
        for lang, data in locales.items():
            done = ((data.get("commands") or {}).get(c["name"]) or {}).get("done")
            if not done:
                problems.append(f"{lang}: commands.{c['name']}.done 없음")
                continue
            unknown = set(re.findall(r"\{\{(\w+)\}\}", done)) - groups
            if unknown:
                problems.append(f"{lang}: commands.{c['name']}.done의 {sorted(unknown)}는 reply_pattern 그룹이 아니다")
    return problems


def test_command_replies_have_text_in_every_language(module):
    """모듈이 정한 명령 답(reply_pattern)은 데몬이 응답 언어의 `commands.<이름>.done`으로 쓴다"""
    locales = {p.stem: json.loads(p.read_text(encoding="utf-8")) for p in (module.dir / "locales").glob("*.json")}
    problems = reply_problems(module.manifest, locales)
    assert not problems, "\n".join(problems)


def test_kit_catches_a_reply_without_text():
    manifest = {"commands": {"fields": [{"name": "seed", "reply_pattern": r"Seed: \[(?P<seed>-?\d+)\]"}]}}
    assert reply_problems(manifest, {"en": {"commands": {"seed": {"done": "Seed {{seed}}"}}}}) == []
    assert reply_problems(manifest, {"ko": {}}) == ["ko: commands.seed.done 없음"]
    assert reply_problems(manifest, {"ko": {"commands": {"seed": {"done": "{{x}}"}}}}) == ["ko: commands.seed.done의 ['x']는 reply_pattern 그룹이 아니다"]


def test_kit_catches_command_without_risk():
    manifest = {"commands": {"fields": [{"name": "kick", "description": "추방"}, {"name": "say", "risk": "safe"}]}}
    assert commands_missing_risk(manifest) == ["kick: risk", "say: description"]
    assert commands_missing_risk({"commands": {"fields": [{"name": "a", "risk": "safe", "description": "d"}]}}) == []

def protocol_problems(manifest):
    """명령 경로는 stdin → rest → rcon. rcon은 둘 다 없을 때만 선언하고, 그때 rcon_port가 있어야 한다
    (데몬은 다른 경로가 있으면 rcon을 쓰지 않고, rcon뿐인데 포트가 없으면 로드를 거부한다)"""
    protocols = manifest.get("protocols") or {}
    supported = protocols.get("supported") or []
    if "rcon" not in supported:
        return []
    others = sorted(set(supported) & {"stdin", "rest"})
    if others:
        return [f"{'·'.join(others)}이 있는데 rcon도 선언했다 (다른 경로가 있으면 RCON은 빼고 게임 설정에서 끈다)"]
    port = protocols.get("rcon_port")
    if port is None:
        return ["rcon만 선언했는데 [protocols] rcon_port가 없다"]
    # 데몬은 u16으로 읽는다. 스키마 format uint16은 검사기가 상한을 보지 않으므로 여기서 본다
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        return [f"rcon_port {port!r}는 1~65535 정수가 아니다"]
    return []


def test_protocols_follow_the_priority(module):
    problems = protocol_problems(module.manifest)
    assert not problems, "\n".join(problems)


def test_kit_catches_rcon_misuse():
    assert protocol_problems({"protocols": {"supported": ["stdin", "rcon"]}}) == [
        "stdin이 있는데 rcon도 선언했다 (다른 경로가 있으면 RCON은 빼고 게임 설정에서 끈다)"]
    assert protocol_problems({"protocols": {"supported": ["rcon"]}}) == ["rcon만 선언했는데 [protocols] rcon_port가 없다"]
    assert protocol_problems({"protocols": {"supported": ["rcon"], "rcon_port": 27015}}) == []
    for bad in (0, 65536, -1, "27015", True):
        assert protocol_problems({"protocols": {"supported": ["rcon"], "rcon_port": bad}}) == [
            f"rcon_port {bad!r}는 1~65535 정수가 아니다"]
    assert protocol_problems({"protocols": {"supported": ["rest"]}}) == []

# ── 2. FUNCTIONS 표 ─────────────────────────────────────

def test_functions_table(module):
    table, defined = functions_table(module)
    assert table is not None, "FUNCTIONS = {...} 표를 찾지 못했다 (사전 리터럴이어야 한다)"
    missing_required = REQUIRED_FUNCTIONS - table.keys()
    assert not missing_required, f"필수 함수가 FUNCTIONS에 없다: {sorted(missing_required)}"
    forgotten = sorted((defined & DAEMON_FUNCTIONS) - table.keys())
    assert not forgotten, f"만들어 놓고 FUNCTIONS에 안 넣은 함수: {forgotten}"
    dangling = sorted(k for k, v in table.items() if v is not None and v not in defined and k in DAEMON_FUNCTIONS)
    assert not dangling, f"FUNCTIONS가 가리키는 최상위 함수가 없다: {dangling}"
    # module.toml이 이름으로 선언한 함수 ([[queries]]·[status] players)도 표에 있어야 데몬이 부를 수 있다
    named = {q.get("function") for q in module.manifest.get("queries", [])}
    named.add((module.manifest.get("status") or {}).get("players"))
    undeclared = sorted(f for f in named if f and f not in table)
    assert not undeclared, f"module.toml이 부르는 함수가 FUNCTIONS에 없다: {undeclared}"
    hooks = hook_functions_missing(module.manifest, table)
    assert not hooks, f"훅을 켰는데 그 함수가 FUNCTIONS에 없다: {hooks}"
    unknown = sorted(table.keys() - DAEMON_FUNCTIONS - named)
    if unknown:
        warnings.warn(f"{module.name}: 데몬이 부르지 않는 이름이 FUNCTIONS에 있다: {unknown}")


def test_kit_catches_hook_without_function():
    """훅을 켜고 그 함수를 빠뜨린 모듈은 데몬이 부를 때 실패한다 (백업 훅이면 업데이트·복원까지 멈춘다)"""
    manifest = {"backup": {"paths": ["world"], "hook": True}, "commands": {"hook": True}}
    assert hook_functions_missing(manifest, {"get_launch_command": "get_launch_command"}) == [
        "[backup] hook → backup_paths", "[commands] hook → instance_commands"]
    table = {"backup_paths": "backup_paths", "instance_commands": "instance_commands"}
    assert hook_functions_missing(manifest, table) == []
    assert hook_functions_missing({"backup": {"hook": False}}, {}) == []
    assert hook_functions_missing({"backup": {"snapshots": True}}, {}) == ["[backup] snapshots → backup_snapshots"]


# ── 3·4. 실행 계약 ──────────────────────────────────────

def test_unknown_function_is_answered(module, shared_libs, tmp_path):
    result = call(module, shared_libs, "__conformance_no_such_function__", sandbox_config(tmp_path), tmp_path)
    assert result.get("success") is False
    assert "Unknown function" in str(result.get("message", "")), f"모르는 함수 응답: {result}"


def test_bad_stdin_is_answered_not_run(module, shared_libs, tmp_path):
    """깨진 JSON을 빈 설정으로 읽어 계속 실행하면 기본값으로 엉뚱한 파일을 건드린다: JSON으로 거절하고 종료 코드 0"""
    result = call(module, shared_libs, "validate", "{not json", tmp_path)
    assert result.get("success") is False and result.get("message"), f"깨진 stdin 응답: {result}"


@pytest.mark.parametrize("function", ["validate", "status", "read_properties"])
def test_a_crashing_function_still_answers_json(module, shared_libs, tmp_path, function):
    """함수 안의 예외도 JSON으로 답한다 (종료 코드 0). 형식이 틀린 설정으로 부른다"""
    table, _ = functions_table(module)
    if function not in (table or {}):
        pytest.skip(f"{function} 미지원")
    config = {**sandbox_config(tmp_path), "working_dir": {"not": "a path"}, "server_executable": ["x"], "port": "nope"}
    result = call(module, shared_libs, function, config, tmp_path)
    assert isinstance(result.get("success"), bool), f"success가 bool이 아니다: {result}"


@pytest.mark.parametrize("function", SAFE_FUNCTIONS)
def test_safe_call_keeps_the_contract(module, shared_libs, tmp_path, function):
    table, _ = functions_table(module)
    if function not in (table or {}):
        pytest.skip(f"{function} 미지원")
    result = call(module, shared_libs, function, sandbox_config(tmp_path), tmp_path)
    assert isinstance(result.get("success"), bool), f"success가 bool이 아니다: {result}"
    if result["success"] is False:
        explained = any(result.get(k) for k in ("message", "issues", "error_code", "action_required"))
        assert explained, f"실패했는데 이유가 없다 (message·issues·error_code·action_required 중 하나): {result}"
        if result.get("error_code") == "extension_required":
            assert isinstance(result.get("missing_extensions"), list), "extension_required에는 missing_extensions[]가 있어야 한다"
    if function == "get_launch_command" and result["success"]:
        assert isinstance(result.get("program"), str) and isinstance(result.get("args", []), list), result


def test_functions_call_lists_the_table(module, shared_libs, tmp_path):
    """데몬은 예약 함수 `functions`로 모듈이 무엇을 지원하는지 묻고, 없는 기능은 화면에서 숨긴다"""
    table, _ = functions_table(module)
    result = call(module, shared_libs, "functions", {}, tmp_path)
    assert result.get("success") is True, f"functions 응답: {result}"
    listed = result.get("functions")
    assert isinstance(listed, list) and all(isinstance(f, str) for f in listed), f"functions[]가 문자열 목록이 아니다: {result}"
    assert set(listed) - {"functions"} == set(table or {}), (
        f"functions 응답이 FUNCTIONS 표와 다르다: 응답만 {sorted(set(listed) - set(table) - {'functions'})}, "
        f"표만 {sorted(set(table) - set(listed))}")


def changed_setting_values(fields, work_dir):
    """게임 파일 설정(`group = "saba-chan"`이 아닌 것)의 기본값을 채우고, 바꿀 수 있는 형식
    (boolean·number·select)은 기본과 다른 값으로 바꾼다. 사바쨩 설정은 데몬도 configure에 넘기지 않는다(MK6).
    돌려주는 것: (configure에 넣을 settings, 바꾼 값)"""
    settings, changed = {}, {}
    for f in fields:
        if "default" not in f or f.get("group") == "saba-chan":
            continue
        value = f["default"]
        if isinstance(value, str):
            value = value.replace("{instance_dir}", str(work_dir))
        settings[f["name"]] = value
        kind, new = f.get("type"), None
        if kind == "boolean" and isinstance(value, bool):
            new = not value
        elif kind == "number" and isinstance(value, (int, float)) and not isinstance(value, bool):
            step = 1 if isinstance(value, int) else 0.5
            new = value + step if f.get("max") is None or value + step <= f["max"] else value - step
            if f.get("min") is not None and new < f["min"]:
                new = None
        elif kind == "select":
            options = [o.get("value") if isinstance(o, dict) else o for o in f.get("options", [])]
            new = next((o for o in options if o != value), None)
        if new is not None:
            settings[f["name"]] = changed[f["name"]] = new
    return settings, changed


def test_settings_round_trip(module, shared_libs, tmp_path):
    """앱에서 고친 값은 게임 파일에 쓰이고, 다시 읽으면 같은 값이어야 한다 (configure → import_settings).
    게임 파일에 두지 않는 설정은 `group = "saba-chan"`으로 선언하므로 비교하지 않는다. 그 밖의 바꾼 설정은
    import_settings가 모두 돌려줘야 한다(빠지면 앱이 게임 파일 값을 모른다)"""
    table, _ = functions_table(module)
    if not {"configure", "import_settings"} <= set(table or {}):
        pytest.skip("configure·import_settings 미지원")
    config = sandbox_config(tmp_path)
    settings, changed = changed_setting_values((module.manifest.get("settings") or {}).get("fields", []),
                                               config["working_dir"])
    instance = {**config, **settings}
    written = call(module, shared_libs, "configure", {**instance, "settings": settings}, tmp_path)
    if written.get("error_code") == "extension_required":
        pytest.skip(f"익스텐션 필요: {written.get('missing_extensions')}")
    assert written.get("success") is True, f"새 작업 폴더에서 configure 실패: {written}"
    read = call(module, shared_libs, "import_settings", instance, tmp_path)
    assert read.get("success") is True, f"import_settings 실패: {read}"
    back = read.get("settings") or {}
    missing = sorted(k for k in changed if k not in back)
    assert not missing, ("configure로 쓴 설정을 import_settings가 돌려주지 않는다 "
                         f"(게임 파일에 두지 않는 설정이면 group = \"saba-chan\"): {missing}")
    differ = [f"{k}: 썼음 {v!r} → 읽음 {back[k]!r}" for k, v in changed.items() if k in back and back[k] != v]
    assert not differ, "앱 값과 게임 파일 값이 다르다:\n" + "\n".join(differ)


def test_kit_changes_every_kind_it_can():
    fields = [
        {"name": "b", "type": "boolean", "default": False},
        {"name": "n", "type": "number", "default": 10, "max": 10},
        {"name": "s", "type": "select", "default": "a", "options": ["a", "b"]},
        {"name": "t", "type": "text", "default": "{instance_dir}/x"},
        {"name": "ram", "type": "number", "default": 4, "group": "saba-chan"},
    ]
    settings, changed = changed_setting_values(fields, "W")
    assert changed == {"b": True, "n": 9, "s": "b"}
    assert settings["t"] == "W/x" and "ram" not in settings

def stop_request_accepted(manifest, requests):
    """설정의 정지 명령 또는 그 명령이 선언한 REST endpoint 접수를 확인한다."""
    stop = str(manifest.get("config", {}).get("stop_command") or "stop").casefold()
    commands = manifest.get("commands", {}).get("fields", [])
    endpoints = {(str(field.get("http_method") or "GET").upper(), field.get("endpoint_template"))
                 for field in commands if field.get("method") == "rest" and str(field.get("name") or "").casefold() == stop}
    for request in requests:
        args = request.get("args", {})
        command = str(args.get("command") or "").strip().casefold()
        if request.get("op") == "instance.stdin" and (command == stop or command.startswith(stop + " ")):
            return True
        if request.get("op") == "instance.rest" and (args.get("method"), args.get("path")) in endpoints:
            return True
    return False


def stop_answer_problems(result, still_alive, managed=False, request_accepted=False):
    """managed는 종료 요청 수락이 성공이다. 일반 정지에서 요청할 길이 없으면 서버를 그대로 둔다."""
    if result.get("error_code") == "graceful_stop_unavailable":
        problems = []
        if result.get("success") is not False:
            problems.append("graceful_stop_unavailable인데 success가 false가 아니다")
        if not result.get("message"):
            problems.append("graceful_stop_unavailable에 message(모듈 locales)가 없다")
        if not still_alive:
            problems.append("graceful_stop_unavailable라면서 서버를 끝냈다 (저장 없이 죽였다)")
        return problems
    if result.get("success") is True:
        if managed and not request_accepted:
            return ["종료 요청 접수 없이 성공으로 답했다"]
        if still_alive and not managed:
            return ["성공이라면서 서버가 아직 살아 있다"]
    return []


def _spawn_server_stand_in():
    """정지 대상 프로세스. POSIX에서는 고아로 만들어 끝나면 바로 사라지게 한다(좀비로 남아 살아 보이지 않게)"""
    if os.name == "nt":
        return subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"]).pid
    out = subprocess.run(["sh", "-c", "sleep 60 >/dev/null 2>&1 & echo $!"], capture_output=True, text=True, check=True)
    return int(out.stdout.strip())


def _alive(pid):
    if os.name == "nt":
        out = subprocess.run(["tasklist", "/FI", f"PID eq {pid}", "/NH"], capture_output=True, text=True)
        return str(pid) in out.stdout
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def test_plain_stop_never_kills_without_saving(module, shared_libs, tmp_path):
    """콘솔이 없는(데몬이 닫힌) 서버에 일반 정지: 저장 후 종료를 요청해 끝내거나, 길이 없으면 죽이지 않고 알린다"""
    table, _ = functions_table(module)
    if "stop" not in (table or {}):
        pytest.skip("stop 미지원")
    pid = _spawn_server_stand_in()
    try:
        result = call(module, shared_libs, "stop", {**sandbox_config(tmp_path), "managed": False, "pid": pid, "force": False}, tmp_path)
        for _ in range(20):  # 끝낸 프로세스가 목록에서 사라지기까지 잠깐 걸린다
            if not _alive(pid):
                break
            time.sleep(0.1)
        problems = stop_answer_problems(result, _alive(pid))
        assert not problems, f"{result}\n" + "\n".join(problems)
    finally:
        if _alive(pid):
            if os.name == "nt":
                subprocess.run(["taskkill", "/F", "/PID", str(pid)], capture_output=True)
            else:
                os.kill(pid, 9)


def test_kit_catches_a_stop_that_kills_without_saving():
    unavailable = {"success": False, "error_code": "graceful_stop_unavailable", "message": "콘솔이 없어요"}
    assert stop_answer_problems(unavailable, still_alive=True) == []
    assert stop_answer_problems(unavailable, still_alive=False) == ["graceful_stop_unavailable라면서 서버를 끝냈다 (저장 없이 죽였다)"]
    assert stop_answer_problems({"success": True, "error_code": "graceful_stop_unavailable", "message": "x"}, True) == [
        "graceful_stop_unavailable인데 success가 false가 아니다"]
    assert stop_answer_problems({"success": True}, still_alive=False) == []
    assert stop_answer_problems({"success": True}, still_alive=True) == ["성공이라면서 서버가 아직 살아 있다"]


def test_kit_accepts_a_managed_stop_request_while_the_server_exits():
    assert stop_answer_problems({"success": True}, still_alive=True, managed=True, request_accepted=True) == []
    assert stop_answer_problems({"success": True}, still_alive=True, managed=True) == ["종료 요청 접수 없이 성공으로 답했다"]
    unavailable = {"success": False, "error_code": "graceful_stop_unavailable", "message": "콘솔이 없어요"}
    assert stop_answer_problems(unavailable, still_alive=False, managed=True)


def test_managed_stop_returns_after_an_accepted_request(module, shared_libs, tmp_path):
    table, _ = functions_table(module)
    if "stop" not in (table or {}):
        pytest.skip("stop 미지원")
    requests = []
    with subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"]) as server:
        try:
            result = call(module, shared_libs, "stop", {**sandbox_config(tmp_path), "pid": server.pid, "force": False},
                          tmp_path, accepted_requests=requests)
            problems = stop_answer_problems(result, server.poll() is None, managed=True,
                                           request_accepted=stop_request_accepted(module.manifest, requests))
            assert result.get("success") is True and not problems, f"{result}\n{problems}"
            assert server.poll() is None, "managed 모듈이 요청 뒤 직접 프로세스를 끝냈다"
        finally:
            if server.poll() is None:
                server.terminate()


def test_host_call_drains_stdout_while_the_module_runs(tmp_path, monkeypatch):
    monkeypatch.setitem(globals(), "CALL_TIMEOUT", 1)
    result = _host_call([sys.executable, "-c", "import sys; sys.stdout.write('x' * 262144)"], "{}",
                        {**os.environ}, tmp_path, [])
    assert result.returncode == 0 and len(result.stdout) == 262144


def test_kit_requires_the_stop_request_instead_of_a_status_or_save_request():
    manifest = {"config": {"stop_command": "shutdown"}, "commands": {"fields": [
        {"name": "shutdown", "method": "rest", "http_method": "POST", "endpoint_template": "/shutdown"}]}}
    assert not stop_request_accepted(manifest, [{"op": "instance.rest", "args": {"method": "GET", "path": "/metrics"}}])
    assert not stop_request_accepted(manifest, [{"op": "instance.rest", "args": {"method": "POST", "path": "/save"}}])
    assert stop_request_accepted(manifest, [{"op": "instance.rest", "args": {"method": "POST", "path": "/shutdown"}}])
    assert not stop_request_accepted({}, [{"op": "instance.stdin", "args": {"command": "save"}}])
    assert stop_request_accepted({}, [{"op": "instance.stdin", "args": {"command": "stop"}}])


@pytest.mark.parametrize("catch", ["Exception", "ValueError", "(OSError, ValueError)"])
def test_kit_catches_a_swallowed_path_error(catch):
    source = f'''from daemon_rcon import safe_join, PathOutside
def get_path(config):
    return safe_join(config["root"], config["name"])
def install(config):
    try:
        get_path(config)
    except {catch}:
        return {{"success": True}}
'''
    assert swallowed_path_errors(source) == [7]
    guarded = source.replace(f"    except {catch}:", "    except PathOutside:\n        raise\n" + f"    except {catch}:")
    assert swallowed_path_errors(guarded) == []


def test_kit_allows_unrelated_error_handlers():
    source = '''from daemon_rcon import safe_join
def get_path(root, name):
    try:
        int(name)
    except ValueError:
        pass
    return safe_join(root, name)
'''
    assert swallowed_path_errors(source) == []


@pytest.mark.parametrize("call", ["Settings.get('port')", "settings.get('port')", "other.get('port')"])
def test_kit_distinguishes_path_method_owners(call):
    source = f'''from daemon_rcon import safe_join
import other
class Paths:
    @staticmethod
    def get(root, name):
        return safe_join(root, name)
class Settings:
    @staticmethod
    def get(key):
        return 7777
settings = Settings()
def install(config):
    try:
        return {call}
    except Exception:
        return {{"success": False}}
'''
    assert swallowed_path_errors(source) == []
    guarded_path = source.replace(call, "Paths.get(config['root'], config['name'])")
    assert swallowed_path_errors(guarded_path) == [15]


@pytest.mark.parametrize(("import_line", "call"), [
    ("from paths import get_path as resolve_path", "resolve_path(config)"),
    ("import paths as helper", "helper.get_path(config)"),
])
def test_kit_tracks_path_errors_through_local_imports(import_line, call):
    source = f'''{import_line}
def install(config):
    try:
        return {call}
    except Exception:
        return {{"success": True}}
'''
    helpers = {"paths": '''from daemon_rcon import safe_join as join
def get_path(config):
    return join(config["root"], config["name"])
'''}
    assert swallowed_path_errors(source, local_sources=helpers) == [5]
    guarded = "from daemon_rcon import PathOutside\n" + source.replace(
        "    except Exception:", "    except PathOutside:\n        raise\n    except Exception:")
    assert swallowed_path_errors(guarded, local_sources=helpers) == []


@pytest.mark.parametrize(("call", "expected"), [("get(config)", []), ("self.get(config)", [10])])
def test_kit_keeps_method_lookup_separate_from_global_functions(call, expected):
    source = f'''from daemon_rcon import safe_join
def get(config):
    return 7777
class Paths:
    def get(self, config):
        return safe_join(config["root"], config["name"])
    def install(self, config):
        try:
            return {call}
        except Exception:
            return {{"success": True}}
'''
    assert swallowed_path_errors(source) == expected


def swallowed_path_errors(source, *, local_sources=None, module_name="lifecycle"):
    """safe_join을 부르는 경로의 넓은 예외 처리는 PathOutside를 그대로 올려야 한다."""
    sources = {**(local_sources or {}), module_name: source}
    trees = {key: ast.parse(text) for key, text in sources.items()}
    bindings, parents, contexts, functions, classes, assignments = {}, {}, {}, {}, set(), []
    definitions = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)

    def collect(node, scope):
        contexts[node] = scope
        if isinstance(node, ast.Import):
            for item in node.names:
                bindings[scope][item.asname or item.name.split(".")[0]] = item.name if item.asname else item.name.split(".")[0]
        elif isinstance(node, ast.ImportFrom) and node.level <= 1:
            for item in node.names:
                bindings[scope][item.asname or item.name] = f"{node.module}.{item.name}"
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            assignments.extend((scope, target.id, node.value) for target in targets if isinstance(target, ast.Name))
        for child in ast.iter_child_nodes(node):
            if isinstance(child, definitions):
                owner = f"{scope}.{child.name}"
                bindings[scope][child.name] = owner
                bindings[owner], parents[owner] = {}, scope
                if isinstance(child, ast.ClassDef):
                    classes.add(owner)
                else:
                    functions[owner] = child
                    if scope in classes:
                        parents[owner] = parents[scope]
                    args = child.args.posonlyargs + child.args.args + child.args.kwonlyargs
                    for arg in args:
                        bindings[owner][arg.arg] = None
                    if scope in classes and args and args[0].arg in ("self", "cls"):
                        bindings[owner][args[0].arg] = scope
                collect(child, owner)
            else:
                collect(child, scope)

    for key, tree in trees.items():
        bindings[key], parents[key] = {}, None
        collect(tree, key)

    def canonical(value):
        seen = set()
        while value and value not in seen:
            seen.add(value)
            owner, _, member = value.rpartition(".")
            target = bindings.get(owner, {}).get(member)
            if not target or target == value:
                return value
            value = target
        return None

    def resolve(node, scope):
        if isinstance(node, ast.Name):
            while scope is not None:
                if node.id in bindings[scope]:
                    return canonical(bindings[scope][node.id])
                scope = parents[scope]
            if node.id in ("Exception", "BaseException", "ValueError"):
                return f"builtins.{node.id}"
        elif isinstance(node, ast.Attribute):
            owner = resolve(node.value, scope)
            return canonical(f"{owner}.{node.attr}") if owner else None
        elif isinstance(node, ast.Call):
            owner = resolve(node.func, scope)
            return owner if owner in classes else None
        return None

    for scope, target, value in assignments:
        bindings[scope][target] = resolve(value, scope)

    paths = {"daemon_rcon.safe_join"}

    def calls_path(nodes, scope):
        pending = list(nodes)
        while pending:
            node = pending.pop()
            if isinstance(node, ast.Call) and resolve(node.func, scope) in paths:
                return True
            if not isinstance(node, definitions + (ast.Lambda,)):
                pending.extend(ast.iter_child_nodes(node))
        return False

    while True:
        found = {key for key, node in functions.items() if calls_path(node.body, key)}
        if found <= paths:
            break
        paths |= found

    problems = []
    catches_path = {"daemon_rcon.PathOutside", "builtins.Exception", "builtins.BaseException", "builtins.ValueError"}
    for node in ast.walk(trees[module_name]):
        scope = contexts[node]
        if not isinstance(node, ast.Try) or not calls_path(node.body, scope):
            continue
        for handler in node.handlers:
            caught = {resolve(part, scope) for part in ast.walk(handler.type)} if handler.type else {"builtins.BaseException"}
            if not caught & catches_path:
                continue
            reraises = any(isinstance(statement, ast.Raise) and
                           (statement.exc is None or isinstance(statement.exc, ast.Name) and statement.exc.id == handler.name)
                           for statement in handler.body)
            if not reraises:
                problems.append(handler.lineno)
            break
    return problems


def test_path_errors_are_not_swallowed(module):
    problems = []
    sources = {path.stem: path.read_text(encoding="utf-8-sig") for path in sorted(module.dir.glob("*.py"))
               if not path.name.startswith("test_") and path.name != "conftest.py"}
    for name, source in sources.items():
        problems.extend(f"{name}.py:{line}" for line in swallowed_path_errors(
            source, local_sources=sources, module_name=name))
    assert not problems, "PathOutside를 다시 던지지 않는 예외 처리: " + ", ".join(problems)

# ── 5. 번역 ─────────────────────────────────────────────

def test_translation_keys(module):
    locales = module.dir / "locales"
    if not locales.is_dir():
        pytest.skip("locales 없음")
    en_file = locales / "en.json"
    assert en_file.is_file(), "locales/en.json이 없다 (다른 언어의 기준)"
    en = flat_keys(json.loads(en_file.read_text(encoding="utf-8-sig")))

    used = set()
    tree = ast.parse(module.entry.read_text(encoding="utf-8-sig"))
    for n in ast.walk(tree):
        if (isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == "t"
                and n.args and isinstance(n.args[0], ast.Constant) and isinstance(n.args[0].value, str)):
            used.add(n.args[0].value)
    assert not (used - en), f"코드가 쓰는데 en.json에 없는 키: {sorted(used - en)}"

    problems = []
    for f in sorted(locales.glob("*.json")):
        try:
            keys = flat_keys(json.loads(f.read_text(encoding="utf-8-sig")))
        except json.JSONDecodeError as e:
            problems.append(f"{f.name}: JSON 오류 {e}")
            continue
        if keys - en:
            problems.append(f"{f.name}: en.json에 없는 키 {sorted(keys - en)[:5]}")
        if en - keys:
            # 빠진 키는 그 언어에서 영어로 보인다 (사용자 문구는 10개 언어)
            problems.append(f"{f.name}: 번역 안 된 키 {sorted(en - keys)[:5]}")
    assert not problems, "\n".join(problems)


# ── 코드 검사 (M12) ─────────────────────────────────────

@pytest.fixture
def audit_findings(module, shared_libs):
    audit = shared_libs / "saba_audit.py"
    if not audit.is_file():
        pytest.skip("이 saba-chan에는 코드 검사기(saba_audit.py)가 없다")
    out = subprocess.run([sys.executable, "-I", str(audit), str(module.dir)], capture_output=True, text=True,
                         encoding="utf-8", timeout=CALL_TIMEOUT)
    result = json.loads(out.stdout)
    assert result["success"], result
    return result["findings"]


def test_sdk_minimum_covers_imports(audit_findings):
    missing = [f"{f['file']}:{f['line']} {f['detail']}" for f in audit_findings if f["permission"] == "sdk_version"]
    assert not missing, "SDK 최소 버전 선언이 사용한 이름보다 낮다:\n" + "\n".join(missing)


def test_code_uses_only_declared_permissions(module, audit_findings):
    """lifecycle 코드가 쓰는 능력(네트워크, 프로세스 실행, 동적 코드 등)은 [permissions]에 선언한다"""
    declared = module.manifest.get("permissions", {})
    missing = [f"{f['permission']} {f['file']}:{f['line']} {f['detail']}"
               for f in audit_findings if f["permission"] != "sdk_version" and declared.get(f["permission"]) is not True]
    assert not missing, "[permissions]에 선언하지 않은 능력을 쓴다:\n" + "\n".join(missing)
