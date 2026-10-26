"""사바쨩 모듈 적합성 키트 설정 (M10).

    pytest conformance                              # 이 저장소의 모든 모듈
    pytest conformance --module path/to/mymodule    # 다른 곳의 모듈 하나
    pytest conformance --shared-libs path/to/saba-chan/modules

모듈은 데몬처럼 `python <entry> <함수>`로 실행하고, stdin에 JSON을 쓴다.
`i18n.py`, `daemon_rcon.py` 같은 공용 파일은 데몬 저장소(saba-chan)의 `modules/`에 있다.
`--shared-libs`나 `SABA_SHARED_LIBS`로 그 폴더를 알려 준다. 없으면 옆에 받아 둔
`../saba-chan/modules`를 찾는다. 익스텐션은 넣지 않는다: 익스텐션이 없어도 모듈은
죽지 않고 `error_code: "extension_required"`로 답해야 한다.
"""
import os
import pathlib
import shutil
import tempfile
import tomllib
from dataclasses import dataclass

import pytest

REPO = pathlib.Path(__file__).resolve().parents[1]


@dataclass
class Module:
    name: str
    dir: pathlib.Path
    manifest_path: pathlib.Path
    manifest: dict

    @property
    def entry(self) -> pathlib.Path:
        return self.dir / self.manifest.get("module", {}).get("entry", "lifecycle.py")


def pytest_addoption(parser):
    parser.addoption("--module", action="append", default=[], help="검사할 모듈 폴더 (여러 번 줄 수 있다)")
    parser.addoption("--shared-libs", default=os.environ.get("SABA_SHARED_LIBS") or os.environ.get("SABA_RUNTIME_DIR"),
                     help="i18n.py, daemon_rcon.py가 있는 폴더")
    parser.addoption("--saba-core", default=os.environ.get("SABA_CORE"),
                     help="실측 모드(trial.py) 전체 흐름을 돌릴 saba-core 실행 파일")


def _repo_modules(tmp_root: pathlib.Path) -> list[pathlib.Path]:
    dirs = sorted(p.parent for p in REPO.glob("*/module.toml"))
    # __example은 파일 이름이 `__example__module.toml`이라, 이름을 바꾼 복사본을 검사한다
    example = REPO / "__example"
    if (example / "__example__module.toml").is_file():
        name = tomllib.loads((example / "__example__module.toml").read_text(encoding="utf-8-sig"))["module"]["name"]
        copy = tmp_root / name
        shutil.copytree(example, copy)
        for f in copy.glob("__example__*"):
            f.rename(copy / f.name.removeprefix("__example__"))
        dirs.append(copy)
    return dirs


def _load(path: pathlib.Path) -> Module:
    manifest_path = path / "module.toml"
    manifest = tomllib.loads(manifest_path.read_text(encoding="utf-8-sig"))
    name = manifest.get("module", {}).get("name") or path.name
    return Module(name=name, dir=path, manifest_path=manifest_path, manifest=manifest)


def pytest_generate_tests(metafunc):
    if "module" not in metafunc.fixturenames:
        return
    given = [pathlib.Path(p).resolve() for p in metafunc.config.getoption("--module")]
    dirs = given or _repo_modules(pathlib.Path(tempfile.mkdtemp(prefix="saba-conformance-")))
    modules = [_load(d) for d in dirs]
    metafunc.parametrize("module", modules, ids=[m.name for m in modules])


@pytest.fixture(scope="session")
def shared_libs(pytestconfig) -> pathlib.Path:
    given = pytestconfig.getoption("--shared-libs")
    candidates = [pathlib.Path(given)] if given else [REPO.parent / "saba-chan" / "modules"]
    for c in candidates:
        if (c / "i18n.py").is_file() and (c / "daemon_rcon.py").is_file():
            return c.resolve()
    pytest.fail(
        "공용 파일(i18n.py, daemon_rcon.py)을 찾지 못했다. "
        "--shared-libs <saba-chan 저장소>/modules 또는 SABA_SHARED_LIBS로 알려 줘."
    )
