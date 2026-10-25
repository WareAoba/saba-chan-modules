"""새 모듈 뼈대 (모듈 메이커)
============================

    python maker/new_module.py <name> --game-name "<게임 이름>" [--display-name ...] [--description ...]

`__example/` 틀을 `<name>/`으로 복사한다: module.toml, lifecycle.py, test_lifecycle.py, locales(10개 언어), icon.png,
그리고 조사 표 `RESEARCH.md`. `[module]`의 이름·설명·별명을 채우고 `pytest.ini`의 testpaths에 등록한다.
실행 파일 이름(`MyGameServer`) 같은 나머지 자리표시는 조사로 채운다. `maker/check.py`가 남은 자리표시를 잡는다.
이미 있는 폴더는 덮지 않는다.
"""

import argparse
import json
import pathlib
import re
import shutil
import sys

REPO = pathlib.Path(__file__).resolve().parent.parent
TEMPLATE = "__example"
RESEARCH_TEMPLATE = pathlib.Path(__file__).resolve().parent / "templates" / "RESEARCH.md"
LANGUAGES = ("de", "en", "es", "fr", "ja", "ko", "pt-BR", "ru", "zh-CN", "zh-TW")
NAME = re.compile(r"[a-z][a-z0-9_-]*")


def toml_string(value):
    return json.dumps(value, ensure_ascii=False)


def fill_manifest(text, name, game_name, display_name, description):
    values = {"name": name, "description": description, "game_name": game_name, "display_name": display_name}
    for key, value in values.items():
        # [module]의 첫 `key = "..."` 줄만 바꾼다 (주석은 남긴다)
        text, count = re.subn(rf'(?m)^{key} = "[^"]*"', lambda _: f"{key} = {toml_string(value)}", text, count=1)
        if not count:
            raise ValueError(f"틀의 module.toml에 {key} 줄이 없다")
    aliases = toml_string(list(dict.fromkeys([display_name, name])))
    return re.sub(r"(?m)^module_aliases = \[.*\]", lambda _: f"module_aliases = {aliases}", text, count=1)


def register_tests(ini, name):
    """pytest.ini testpaths 목록 끝에 모듈을 넣는다"""
    lines = ini.read_text(encoding="utf-8").splitlines(keepends=True)
    if any(line.strip() == name for line in lines):
        return
    start = next((i for i, line in enumerate(lines) if line.split("=")[0].strip() == "testpaths"), None)
    if start is None:
        raise ValueError(f"{ini.name}에 testpaths가 없다")
    end = start + 1
    while end < len(lines) and lines[end][:1] in (" ", "\t") and lines[end].strip():
        end += 1
    lines.insert(end, f"    {name}\n")
    ini.write_text("".join(lines), encoding="utf-8")


def create(repo, name, game_name, display_name=None, description=None):
    repo = pathlib.Path(repo)
    if not NAME.fullmatch(name or "") or name == TEMPLATE:
        raise ValueError(f"모듈 이름은 영문 소문자로 시작하는 소문자·숫자·-·_ 여야 한다: {name!r}")
    target = repo / name
    if target.exists():
        raise FileExistsError(f"{target}가 이미 있다")
    display_name = display_name or game_name
    description = description or f"{game_name} server module"
    source = repo / TEMPLATE

    target.mkdir()
    try:
        manifest = (source / "__example__module.toml").read_text(encoding="utf-8")
        (target / "module.toml").write_text(fill_manifest(manifest, name, game_name, display_name, description), encoding="utf-8")
        shutil.copyfile(source / "__example__lifecycle.py", target / "lifecycle.py")
        test = (source / "test_lifecycle.py").read_text(encoding="utf-8")
        test = test.replace('"__example__lifecycle.py"', '"lifecycle.py"').replace('"example_lifecycle"', f'"{name}_lifecycle"')
        (target / "test_lifecycle.py").write_text(test, encoding="utf-8")
        shutil.copyfile(source / "icon.png", target / "icon.png")

        locales = target / "locales"
        locales.mkdir()
        english = (source / "locales" / "en.json").read_text(encoding="utf-8")
        for lang in LANGUAGES:
            own = source / "locales" / f"{lang}.json"
            (locales / f"{lang}.json").write_text(own.read_text(encoding="utf-8") if own.is_file() else english, encoding="utf-8")

        research = RESEARCH_TEMPLATE.read_text(encoding="utf-8").replace("{game_name}", game_name)
        (target / "RESEARCH.md").write_text(research, encoding="utf-8")
    except BaseException:
        shutil.rmtree(target)
        raise
    ini = repo / "pytest.ini"
    if ini.is_file():
        register_tests(ini, name)
    return target


def main():
    parser = argparse.ArgumentParser(description="__example 틀로 새 모듈 뼈대를 만든다")
    parser.add_argument("name", help="모듈 이름 = 폴더 이름 (영문 소문자)")
    parser.add_argument("--game-name", required=True, help="게임 전체 이름")
    parser.add_argument("--display-name", help="GUI에 보이는 이름 (기본: 게임 이름)")
    parser.add_argument("--description", help="한 줄 설명 (기본: '<게임 이름> server module')")
    parser.add_argument("--repo", default=str(REPO), help="모듈 저장소 루트")
    args = parser.parse_args()
    try:
        folder = create(args.repo, args.name, args.game_name, args.display_name, args.description)
    except (ValueError, FileExistsError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    print(folder)
    print("다음: RESEARCH.md 조사 표 채우기 → python maker/inspect_dist.py → python maker/probe_server.py (maker/SKILL.md)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
