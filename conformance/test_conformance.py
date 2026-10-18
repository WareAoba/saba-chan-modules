"""모듈 계약 검사 (M10). 계약 원문은 docs/module-spec.md.

1. module.toml이 스키마 v1을 통과한다
2. 진입 파일의 `FUNCTIONS` 표: `get_launch_command`가 있고, 데몬이 부르는 이름으로
   만든 함수는 빠짐없이 표에 있다 (빠지면 데몬은 "Unknown function"을 받는다)
3. 모르는 함수에는 `{"success": false, "message": "Unknown function: X"}`
4. 부르면 안전한 함수들을 빈 작업 폴더에서 불러, 제한 시간 안에 종료 코드 0으로 끝나고
   stdout 마지막 줄이 JSON 객체이며, stderr의 `PROGRESS:` 줄이 JSON인지
5. 코드에서 쓰는 번역 키가 `locales/en.json`에 있고, 다른 언어 파일에 en에 없는 키가 없다
"""
import ast
import json
import os
import pathlib
import subprocess
import sys
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
    "import_settings", "backup_paths",
}
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
    unknown = sorted(table.keys() - DAEMON_FUNCTIONS)
    if unknown:
        warnings.warn(f"{module.name}: 데몬이 부르지 않는 이름이 FUNCTIONS에 있다: {unknown}")


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
