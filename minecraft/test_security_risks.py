"""보안 민감 명령은 기본 권한에서 제한한다."""
import json
from pathlib import Path

import tomllib


ROOT = Path(__file__).parent


def _catalog_risks(filename):
    catalog = json.loads((ROOT / "plugin_catalog" / filename).read_text(encoding="utf-8-sig"))
    return {command["name"]: command["risk"] for command in catalog["commands"]}


def _catalog_commands(filename):
    catalog = json.loads((ROOT / "plugin_catalog" / filename).read_text(encoding="utf-8-sig"))
    return {command["console_template"]: command for command in catalog["commands"]}


def test_world_and_player_impacting_commands_require_admin():
    expected = {
        "worldedit.json": ["we-world", "we-pos", "we-pos1", "we-pos2", "we-chunk", "we-set",
                           "we-replace", "we-cut", "we-drain", "removenear", "replacenear",
                           "we-contract", "we-shift", "we-outset", "we-inset", "we-trim", "we-sel",
                           "we-expand", "we-expand-vert", "toggleplace", "placement",
                           "we-line", "we-curve", "we-overlay", "we-center", "we-naturalize",
                           "we-walls", "we-faces", "we-smooth", "we-snowsmooth", "we-move",
                           "we-stack", "we-deform", "we-hollow", "we-forest", "we-flora",
                           "we-update", "we-hcyl", "we-cyl", "we-cone", "we-hsphere", "we-sphere",
                           "forestgen", "pumpkins", "we-feature", "we-structure", "we-hpyramid",
                           "we-pyramid", "we-generate", "we-generatebiome", "we-copy", "we-paste",
                           "we-rotate", "we-flip", "we-revolve", "we-setbiome", "we-replacebiome",
                           "we-fill", "we-fillr", "fixlava", "fixwater", "removeabove", "removebelow",
                           "snow", "thaw", "green", "undo", "redo"],
        "essentialsx.json": ["back", "burn", "lightning", "spawnmob", "tppos", "tppos-rotation", "ess-tp",
                             "tpall", "tpr", "togglejail", "warp-player", "ess-time-set",
                             "ess-time-add", "ess-weather"],
        "essentialsx-spawn.json": ["spawn"],
        "multiverse-core.json": ["mv-create", "mv-import", "mv-unload", "mv-remove", "mv-clone"],
        "chunky.json": ["chunky-start", "chunky-continue"],
    }
    for filename, names in expected.items():
        risks = _catalog_risks(filename)
        for name in names:
            assert risks[name] == "danger", f"{filename}: {name}"

    assert _catalog_risks("citizens.json")["npc-tpto"] == "danger"
    assert _catalog_risks("multiverse-core.json")["mv-teleport"] == "danger"
    assert _catalog_risks("multiverse-core.json")["mv-worldborder-damage-amount"] == "danger"
    assert _catalog_risks("multiverse-inventories.json")["mvinv-playerdata-import"] == "danger"
    assert _catalog_risks("multiverse-netherportals.json")["mvnp-link"] == "danger"
    assert _catalog_risks("worldguard.json")["allowfire"] == "danger"


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
    assert players["risk"] in ("caution", "danger")

    minecraft = tomllib.loads((ROOT / "module.toml").read_text(encoding="utf-8-sig"))
    teleport = next(field for field in minecraft["commands"]["fields"] if field["name"] == "tp")
    assert teleport["risk"] == "danger"


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

