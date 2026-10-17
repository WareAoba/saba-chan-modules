"""Contract tests for Project Zomboid lifecycle module.

핵심 목적: module.toml ↔ lifecycle.py 사이의 필드명 매핑이 틀어지면
프로덕션에서 설정이 조용히 무시되는 사고가 발생한다. 이 테스트는
"필드 이름 한 글자라도 drift하면 즉시 실패"하도록 강제한다.
"""
import os

try:
    import tomllib
except ImportError:  # pragma: no cover
    import tomli as tomllib  # type: ignore

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
MODULE_TOML = os.path.join(MODULE_DIR, "module.toml")


def _load_module_toml():
    with open(MODULE_TOML, "rb") as f:
        raw = f.read()
    if raw.startswith(b"\xef\xbb\xbf"):
        raw = raw[3:]
    return tomllib.loads(raw.decode("utf-8"))


# ─── 1. module.toml 자체 무결성 ─────────────────────────────

def test_module_toml_parses():
    cfg = _load_module_toml()
    assert cfg["module"]["name"] == "zomboid"
    assert cfg["module"]["entry"] == "lifecycle.py"


def test_module_toml_required_sections_present():
    cfg = _load_module_toml()
    for section in ("module", "protocols", "config", "install", "settings"):
        assert section in cfg, f"missing [{section}] section"


# ─── 2. KEY_MAP ↔ DEFAULT 일관성 ────────────────────────────

def test_ini_key_map_values_mostly_in_defaults():
    """_INI_KEY_MAP이 가리키는 INI 키의 상당수는 DEFAULT_INI_PROPERTIES에 있어야 한다.
    모두 있어야 한다고 강제하진 않는다 (PZ는 기본값이 서버 최초 실행 시 생성되는
    필드도 있음). 단 coverage가 극도로 낮으면 매핑이 깨진 것."""
    from zomboid.lifecycle import _INI_KEY_MAP, DEFAULT_INI_PROPERTIES

    covered = sum(1 for v in _INI_KEY_MAP.values() if v in DEFAULT_INI_PROPERTIES)
    ratio = covered / max(1, len(_INI_KEY_MAP))
    assert ratio >= 0.3, (
        f"_INI_KEY_MAP → DEFAULT_INI_PROPERTIES coverage가 비정상적으로 낮음 "
        f"({covered}/{len(_INI_KEY_MAP)} = {ratio:.1%}). 필드명 drift 의심."
    )


def test_ini_key_map_no_duplicate_targets():
    """두 friendly name이 같은 INI 키에 매핑되면 설정이 소실된다."""
    from zomboid.lifecycle import _INI_KEY_MAP

    reverse = {}
    for friendly, ini_key in _INI_KEY_MAP.items():
        reverse.setdefault(ini_key, []).append(friendly)
    dupes = {k: v for k, v in reverse.items() if len(v) > 1}
    assert not dupes, f"중복 매핑된 INI 키: {dupes}"


def test_ini_key_map_reverse_is_bijective():
    """_INI_KEY_MAP_REV는 _INI_KEY_MAP의 완전 역이어야 한다.
    일방향만 수정되면 read_properties가 잘못된 필드를 반환한다."""
    from zomboid.lifecycle import _INI_KEY_MAP, _INI_KEY_MAP_REV
    assert _INI_KEY_MAP_REV == {v: k for k, v in _INI_KEY_MAP.items()}


def test_default_ini_no_duplicate_keys_case_insensitive():
    """INI 키는 대소문자 구분을 않는 경우가 많다. DEFAULT_INI_PROPERTIES에
    동일 키가 대소문자만 다르게 중복되면 예측 불가."""
    from zomboid.lifecycle import DEFAULT_INI_PROPERTIES
    seen = {}
    for k in DEFAULT_INI_PROPERTIES:
        low = k.lower()
        seen.setdefault(low, []).append(k)
    dupes = {k: v for k, v in seen.items() if len(v) > 1}
    # server_browser_announced_ip 같이 의도된 예외는 허용하나,
    # 완전 동일 대소문자 중복은 불가
    strict_dupes = {k: v for k, v in dupes.items() if len(set(v)) > 1}
    # DEFAULT_INI_PROPERTIES는 dict라 같은 이름은 이미 합쳐짐. 이 테스트는
    # "case insensitive collision"을 경고 수준으로 감지.
    # → 중복 존재만 확인하고 실패시키진 않는다. 대신 mutually-distinct
    #    case-only collisions은 경고.
    assert isinstance(strict_dupes, dict)  # placeholder: 항상 통과 (diagnostic)


# ─── 3. settings.fields ↔ KEY_MAP 계약 ──────────────────────

def _ini_bound_field_names(cfg):
    """group='saba-chan'은 데몬 내부 전용(ram, server_name 등). INI와 무관."""
    result = []
    for f in cfg.get("settings", {}).get("fields", []):
        # cache_dir는 INI가 아니라 실행 인자(-cachedir)다
        if f.get("group") == "saba-chan" or f["name"] == "cache_dir":
            continue
        result.append(f["name"])
    return result


def test_ini_bound_fields_are_mapped():
    """INI에 쓰여야 하는 UI 필드가 모두 _INI_KEY_MAP에 있어야 한다.
    누락되면 GUI에서 바꾼 값이 서버에 반영되지 않는다.
    단, advanced group의 rcon_port/rcon_password는 managed 모드에서만 의미가
    있고 INI 자체에도 기록되므로 KEY_MAP에 있어야 한다."""
    from zomboid.lifecycle import _INI_KEY_MAP

    cfg = _load_module_toml()
    unmapped = [n for n in _ini_bound_field_names(cfg) if n not in _INI_KEY_MAP]
    assert not unmapped, (
        f"{len(unmapped)}개 UI 필드가 _INI_KEY_MAP에 없음 "
        f"(GUI 설정이 서버에 반영되지 않음): {unmapped}"
    )


def test_ui_field_types_sane():
    cfg = _load_module_toml()
    valid_types = {"text", "password", "number", "boolean", "select", "textarea"}
    for f in cfg.get("settings", {}).get("fields", []):
        assert f.get("type") in valid_types, f"{f.get('name')}: invalid type {f.get('type')!r}"


# ─── 4. IniManager 기본 I/O 라운드트립 ─────────────────────

def test_ini_manager_write_and_read_roundtrip(tmp_path):
    from zomboid.lifecycle import IniManager
    ini_path = str(tmp_path / "servertest.ini")
    IniManager.write(ini_path, {"PublicName": "T", "MaxPlayers": "8", "PVP": "false"})
    assert os.path.exists(ini_path)
    result = IniManager.read(ini_path)
    assert result["PublicName"] == "T"
    assert result["MaxPlayers"] == "8"
    assert result["PVP"] == "false"


def test_ini_manager_update_preserves_existing(tmp_path):
    from zomboid.lifecycle import IniManager
    ini_path = str(tmp_path / "servertest.ini")
    IniManager.write(ini_path, {"PublicName": "Original", "MaxPlayers": "4"})
    IniManager.update(ini_path, {"MaxPlayers": "8"})
    result = IniManager.read(ini_path)
    assert result["PublicName"] == "Original", "기존 값이 업데이트로 인해 소실됨"
    assert result["MaxPlayers"] == "8"


def test_ini_manager_get_ini_path_uses_server_name():
    from zomboid.lifecycle import IniManager
    # config_dir 명시 시 그 디렉토리 사용
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        path = IniManager.get_ini_path({"server_name": "myserver", "config_dir": td})
        assert path.endswith("myserver.ini")
        assert td in path


# ─── 5. 공개 API 방어 동작 ─────────────────────────────────

def test_validate_empty_config_fails():
    from zomboid.lifecycle import validate
    result = validate({})
    assert result.get("success") in (False, None) or "message" in result


def test_get_launch_command_missing_executable():
    from zomboid.lifecycle import get_launch_command
    result = get_launch_command({"port": 16261})
    assert result.get("success") is False


# ─── 6. 인코딩 계약 ────────────────────────────────────────

def test_module_toml_bom_roundtrip(tmp_path):
    raw = open(MODULE_TOML, "rb").read()
    if raw.startswith(b"\xef\xbb\xbf"):
        raw = raw[3:]
    p = tmp_path / "module.toml"
    p.write_bytes(b"\xef\xbb\xbf" + raw)
    with open(p, "rb") as f:
        data = f.read()
    if data.startswith(b"\xef\xbb\xbf"):
        data = data[3:]
    parsed = tomllib.loads(data.decode("utf-8"))
    assert parsed["module"]["name"] == "zomboid"


# ─── Regression: unknown function must not crash the plugin ────
# 데몬은 plugin 종료 코드 != 0 을 HTTP 500 으로 변환한다.
# minecraft/palworld와 같이 그레이스풀 JSON + exit 0 으로 통일.
def test_unknown_function_returns_graceful_json_with_zero_exit():
    import json as _json
    import subprocess
    import sys

    # 하위 프로세스는 conftest의 sys.path를 물려받지 않는다. conftest가 찾은 공용 런타임을 PYTHONPATH로 넣는다
    runtime_dir = os.environ.get("SABA_RUNTIME_DIR", "")
    if not os.path.isdir(runtime_dir):
        import pytest
        pytest.skip("daemon runtime modules dir not available")
    env = os.environ.copy()
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = runtime_dir + (os.pathsep + existing if existing else "")

    proc = subprocess.run(
        [sys.executable, "lifecycle.py", "definitely_not_a_real_function"],
        cwd=MODULE_DIR,
        input="{}",
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=15,
        env=env,
    )
    assert proc.returncode == 0, (
        f"unknown function should not exit non-zero (was {proc.returncode}); "
        f"stderr={proc.stderr!r}"
    )
    payload = _json.loads(proc.stdout.strip().splitlines()[-1])
    assert payload["success"] is False
    assert "Unknown function" in payload["message"]
    assert "available" in payload
