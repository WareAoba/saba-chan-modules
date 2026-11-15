"""보안 민감 명령은 기본 권한에서 제한한다."""
import json
import re
from pathlib import Path

import tomllib


ROOT = Path(__file__).parent


def _catalog_risks(filename):
    catalog = json.loads((ROOT / "plugin_catalog" / filename).read_text(encoding="utf-8-sig"))
    return {command["name"]: command["risk"] for command in catalog["commands"]}


def _catalog_commands(filename):
    catalog = json.loads((ROOT / "plugin_catalog" / filename).read_text(encoding="utf-8-sig"))
    return {command["console_template"]: command for command in catalog["commands"]}


def test_plugin_risks_follow_user_rule():
    """위험도 기준(사용자 지정): 특정 유저에게 큰 영향을 주는 게임 안 명령(밴·킥·sudo)과
    설정·데이터를 다시 읽거나 되돌리는 명령(reload·rollback)은 위험이다"""
    danger = re.compile(r"(^|-)(ban|kick|sudo|reload|rollback)($|-)")
    for path in sorted((ROOT / "plugin_catalog").glob("*.json")):
        for name, risk in _catalog_risks(path.name).items():
            if danger.search(name):
                assert risk == "danger", f"{path.name}: {name}"


def test_player_network_and_location_commands_are_not_public():
    commands = _catalog_commands("essentialsx.json")
    for template in ("whois {player}", "seen {player}", "getpos {player}", "near {player} {radius}"):
        assert commands[template]["risk"] in ("caution", "danger"), template
    assert _catalog_risks("viaversion.json")["viaversion-player"] in ("caution", "danger")
    placeholder_risks = _catalog_risks("placeholderapi.json")
    for name in ("placeholderapi-parse", "placeholderapi-parserel"):
        assert placeholder_risks[name] in ("caution", "danger"), name

    manifest = tomllib.loads((ROOT.parent / "palworld" / "module.toml").read_text(encoding="utf-8-sig"))
    players = next(field for field in manifest["commands"]["fields"] if field["name"] == "players")
    # 조회라 안전이지만 접속자 IP·계정 ID가 실리므로 디스코드·챗봇에는 내지 않는다
    assert players["remote"] is False


def test_placeholder_expansion_management_requires_admin():
    risks = _catalog_risks("placeholderapi.json")
    for name in ("placeholderapi-ecloud-download", "placeholderapi-ecloud-update", "placeholderapi-register"):
        assert risks[name] == "danger", name


def test_external_uploads_and_operator_links_are_not_public():
    templates_by_file = {
        "essentialsx.json": ["essentials dump {options}"],
        "geyser.json": ["geyser dump {options}", "geyser nethernet action"],
        "viaversion.json": ["viaversion dump"],
        "multiverse-core.json": ["mv dumps {flags}"],
        "spark.json": ["spark health upload"],
        "placeholderapi.json": ["placeholderapi dump"],
        "luckperms.json": ["lp editor {type} {filter}", "lp user {user} editor",
                           "lp group {group} editor", "lp track {track} editor"],
        "worldguard.json": ["worldguard report {flags}"],
    }
    for filename, templates in templates_by_file.items():
        commands = _catalog_commands(filename)
        for template in templates:
            assert commands[template]["risk"] in ("caution", "danger"), f"{filename}: {template}"

