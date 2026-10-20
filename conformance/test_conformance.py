"""모듈 계약 검사 (M10). 계약 원문은 docs/module-spec.md.

1. module.toml이 스키마 v1을 통과한다
2. 진입 파일의 `FUNCTIONS` 표: `get_launch_command`가 있고, 데몬이 부르는 이름으로
   만든 함수는 빠짐없이 표에 있다 (빠지면 데몬은 "Unknown function"을 받는다).
   훅을 켰거나 module.toml이 이름으로 가리킨 함수도 표에 있다
3. 모르는 함수에는 `{"success": false, "message": "Unknown function: X"}`
4. 부르면 안전한 함수들을 빈 작업 폴더에서 불러, 제한 시간 안에 종료 코드 0으로 끝나고
   stdout 마지막 줄이 JSON 객체이며, stderr의 `PROGRESS:` 줄이 JSON인지
5. 예약 함수 `functions`가 FUNCTIONS 표의 이름을 돌려준다
6. 코드에서 쓰는 번역 키가 `locales/en.json`에 있고, 다른 언어 파일에 en에 없는 키가 없다
7. 명령마다 `risk`와 `description`이 있다
8. 바꾼 설정 값을 `configure`로 쓰고 `import_settings`로 읽으면 같은 값이다
9. 콘솔 없는 서버의 일반 정지는 저장 후 종료를 요청하거나, 길이 없으면 죽이지 않고 `graceful_stop_unavailable`로 알린다
"""
import ast
import json
import os
import pathlib
import subprocess
import sys
import time
import warnings

import jsonschema
import pytest

REPO = pathlib.Path(__file__).resolve().parents[1]
SCHEMA = json.loads((REPO / "schema" / "module.schema.json").read_text(encoding="utf-8"))

# 데몬이 이름으로 부르는 함수 (saba-chan src/에서 부르는 이름 전부)
DAEMON_FUNCTIONS = {
    "get_launch_command", "stop", "status", "command", "get_credentials",
    "validate", "configure", "read_properties", "reset_properties", "reset_server",
    "accept_agreements", "diagnose_log",
    "list_versions", "get_version_details", "install_server", "get_installed_version",
    "import_settings", "backup_paths", "instance_commands",
}
# 켜면 데몬이 그 함수를 부르는 module.toml 훅
HOOK_FUNCTIONS = {"backup": "backup_paths", "commands": "instance_commands"}
REQUIRED_FUNCTIONS = {"get_launch_command"}
# 빈 작업 폴더에서 불러도 아무것도 바꾸지 않는 함수
SAFE_FUNCTIONS = ["get_launch_command", "validate", "status", "read_properties", "diagnose_log", "get_installed_version"]
CALL_TIMEOUT = 30


def call(module, shared_libs, function, config, tmp_path):
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
    try:
        proc = subprocess.run(
            [sys.executable, str(module.entry), function],
            input=json.dumps(config),
            capture_output=True,
            text=True,
            encoding="utf-8",
            env=env,
            cwd=tmp_path,
            timeout=CALL_TIMEOUT,
        )
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
    """`hook = true`인 섹션인데 데몬이 부를 함수가 FUNCTIONS 표에 없는 것"""
    return [f"[{section}] hook → {function}" for section, function in HOOK_FUNCTIONS.items()
            if (manifest.get(section) or {}).get("hook") is True and function not in table]


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


def test_kit_catches_command_without_risk():
    manifest = {"commands": {"fields": [{"name": "kick", "description": "추방"}, {"name": "say", "risk": "safe"}]}}
    assert commands_missing_risk(manifest) == ["kick: risk", "say: description"]
    assert commands_missing_risk({"commands": {"fields": [{"name": "a", "risk": "safe", "description": "d"}]}}) == []

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


# ── 3·4. 실행 계약 ──────────────────────────────────────

def test_unknown_function_is_answered(module, shared_libs, tmp_path):
    result = call(module, shared_libs, "__conformance_no_such_function__", sandbox_config(tmp_path), tmp_path)
    assert result.get("success") is False
    assert "Unknown function" in str(result.get("message", "")), f"모르는 함수 응답: {result}"


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

def stop_answer_problems(result, still_alive):
    """일반 정지(force=false)의 답: 끝냈으면 성공, 저장 후 종료를 요청할 길이 없으면 서버를 그대로 두고
    `graceful_stop_unavailable`(데몬이 사용자에게 강제 종료를 고르게 한다, 02 §3.4)"""
    if result.get("error_code") == "graceful_stop_unavailable":
        problems = []
        if result.get("success") is not False:
            problems.append("graceful_stop_unavailable인데 success가 false가 아니다")
        if not result.get("message"):
            problems.append("graceful_stop_unavailable에 message(모듈 locales)가 없다")
        if not still_alive:
            problems.append("graceful_stop_unavailable라면서 서버를 끝냈다 (저장 없이 죽였다)")
        return problems
    if result.get("success") is True and still_alive:
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
        result = call(module, shared_libs, "stop", {**sandbox_config(tmp_path), "pid": pid, "force": False}, tmp_path)
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

    problems, untranslated = [], {}
    for f in sorted(locales.glob("*.json")):
        try:
            keys = flat_keys(json.loads(f.read_text(encoding="utf-8-sig")))
        except json.JSONDecodeError as e:
            problems.append(f"{f.name}: JSON 오류 {e}")
            continue
        if keys - en:
            problems.append(f"{f.name}: en.json에 없는 키 {sorted(keys - en)[:5]}")
        if en - keys:
            untranslated[f.stem] = len(en - keys)
    assert not problems, "\n".join(problems)
    if untranslated:
        warnings.warn(f"{module.name}: 번역 안 된 키 수 (영어로 보인다): {untranslated}")


# ── 코드 검사 (M12) ─────────────────────────────────────

def test_code_uses_only_declared_permissions(module, shared_libs):
    """lifecycle 코드가 쓰는 능력(네트워크, 프로세스 실행, 동적 코드 등)은 [permissions]에 선언한다"""
    audit = shared_libs / "saba_audit.py"
    if not audit.is_file():
        pytest.skip("이 saba-chan에는 코드 검사기(saba_audit.py)가 없다")
    out = subprocess.run([sys.executable, "-I", str(audit), str(module.dir)], capture_output=True, text=True,
                         encoding="utf-8", timeout=CALL_TIMEOUT)
    result = json.loads(out.stdout)
    assert result["success"], result
    declared = module.manifest.get("permissions", {})
    missing = [f"{f['permission']} {f['file']}:{f['line']} {f['detail']}"
               for f in result["findings"] if declared.get(f["permission"]) is not True]
    assert not missing, "[permissions]에 선언하지 않은 능력을 쓴다:\n" + "\n".join(missing)
