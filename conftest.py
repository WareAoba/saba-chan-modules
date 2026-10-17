"""pytest conftest for saba-chan-modules.

런타임에는 데몬이 공용 파일(`i18n.py`, `daemon_rcon.py`)과 익스텐션 상위
디렉터리를 PYTHONPATH로 주입한다. 테스트도 같은 실제 파일을 쓴다 — 스텁은
실제 계약과 어긋나므로 두지 않는다.

공용 런타임 탐색 순서:
  1. 이미 import 가능한 경우 (run-tests.py가 PYTHONPATH 지정)
  2. SABA_RUNTIME_DIR 환경 변수 (CI가 saba-chan/modules를 체크아웃한 경로)
  3. 형제 레포 ../saba-chan/modules
  4. %APPDATA%/saba-chan/modules (설치본)
익스텐션은 SABA_EXTENSIONS_DIR → ../saba-chan-extensions → %APPDATA% 순서로
찾아 `extensions` 패키지로 alias 한다.
"""
import importlib.util
import os
import sys
import types

_MODULES_ROOT = os.path.dirname(os.path.abspath(__file__))
_REPO_PARENT = os.path.dirname(_MODULES_ROOT)
_APPDATA = os.environ.get("APPDATA", "")


def _first_dir(candidates, marker):
    for path in candidates:
        if path and os.path.isfile(os.path.join(path, marker)):
            return path
    return None


def _install_runtime():
    if importlib.util.find_spec("daemon_rcon") and importlib.util.find_spec("i18n"):
        return
    runtime = _first_dir([
        os.environ.get("SABA_RUNTIME_DIR"),
        os.path.join(_REPO_PARENT, "saba-chan", "modules"),
        os.path.join(_APPDATA, "saba-chan", "modules") if _APPDATA else None,
    ], "daemon_rcon.py")
    if runtime is None:
        raise RuntimeError(
            "saba-chan 공용 런타임(daemon_rcon.py, i18n.py)을 찾지 못했습니다. "
            "saba-chan 레포를 형제 디렉터리에 두거나 SABA_RUNTIME_DIR을 지정하세요."
        )
    sys.path.insert(0, runtime)


def _install_extensions_alias():
    if "extensions" in sys.modules:
        return
    ext_dir = _first_dir([
        os.environ.get("SABA_EXTENSIONS_DIR"),
        os.path.join(_REPO_PARENT, "saba-chan-extensions"),
        os.path.join(_APPDATA, "saba-chan", "extensions") if _APPDATA else None,
    ], os.path.join("ue4_ini", "ue4_ini.py"))
    pkg = types.ModuleType("extensions")
    # 익스텐션이 없으면 빈 패키지 — ue4_ini 지연 import 경로가 ImportError를 처리한다.
    pkg.__path__ = [ext_dir] if ext_dir else []
    pkg.__package__ = "extensions"
    sys.modules["extensions"] = pkg


_install_runtime()
if _MODULES_ROOT not in sys.path:
    sys.path.insert(0, _MODULES_ROOT)
_install_extensions_alias()
