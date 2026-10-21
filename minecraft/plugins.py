"""설치된 플러그인·모드 판정과 인스턴스별 명령 카탈로그

판정은 jar 파일 이름이 아니라 jar 안의 메타데이터로 한다:
  - Bukkit/Spigot/Paper 플러그인: `plugin.yml`, `paper-plugin.yml` (plugins/*.jar)
  - Fabric/Quilt 모드: `fabric.mod.json`, `quilt.mod.json` (mods/*.jar)
  - Forge/NeoForge 모드: `META-INF/mods.toml`, `META-INF/neoforge.mods.toml` (mods/*.jar)

명령은 두 곳에서 온다:
  1. plugin.yml이 선언한 명령 — 모든 플러그인에 범용으로 (자유 입력 한 칸)
  2. `plugin_catalog/<slug>.json` — 널리 쓰는 플러그인의 공식 문서 기준 명령 정의 (입력 칸까지)
     플러그인 id(`match`)를 키로 하는 선언형 데이터라, 플러그인을 더할 때 파일만 늘리면 된다.
"""

import glob
import json
import os
import re
import sys
import zipfile

from i18n import I18n

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
CATALOG_DIR = os.path.join(MODULE_DIR, "plugin_catalog")

_module_i18n = I18n(MODULE_DIR)
_catalog_i18n = I18n(CATALOG_DIR)

# 메타데이터 파일 → 종류. 같은 jar에 여럿 있으면 앞의 것이 이긴다
# (Paper 플러그인은 paper-plugin.yml이 우선이지만 명령 선언은 plugin.yml에만 있다)
_DESCRIPTORS = [
    ("paper-plugin.yml", "paper"),
    ("plugin.yml", "bukkit"),
    ("fabric.mod.json", "fabric"),
    ("quilt.mod.json", "quilt"),
    ("META-INF/neoforge.mods.toml", "neoforge"),
    ("META-INF/mods.toml", "forge"),
]

_SCAN_DIRS = ("plugins", "mods")


# ─── plugin.yml (YAML의 일부) ───────────────────────────────────
# 데몬의 Python에는 PyYAML이 없다. plugin.yml에서 쓰는 모양(맵, 블록·흐름 목록, 따옴표 문자열,
# | > 블록 문자열)만 읽는다. 앵커·태그 같은 나머지 YAML은 plugin.yml에 쓰이지 않는다.

def _strip_comment(line):
    out, quote = [], None
    for i, ch in enumerate(line):
        if quote:
            if ch == quote:
                quote = None
        elif ch in "'\"":
            quote = ch
        elif ch == "#" and (i == 0 or line[i - 1] in " \t"):
            break
        out.append(ch)
    return "".join(out).rstrip()


def _scalar(text):
    text = text.strip()
    if not text:
        return None
    if text[0] == '"' and text.endswith('"') and len(text) >= 2:
        return re.sub(r'\\(["\\/nt])', lambda m: {"n": "\n", "t": "\t"}.get(m.group(1), m.group(1)), text[1:-1])
    if text[0] == "'" and text.endswith("'") and len(text) >= 2:
        return text[1:-1].replace("''", "'")
    if text[0] == "[" and text.endswith("]"):
        return [_scalar(p) for p in _split_flow(text[1:-1]) if p.strip()]
    if text[0] == "{" and text.endswith("}"):
        out = {}
        for part in _split_flow(text[1:-1]):
            key, sep, value = part.partition(":")
            if sep:
                out[_scalar(key)] = _scalar(value)
        return out
    lowered = text.lower()
    if lowered in ("true", "yes", "on"):
        return True
    if lowered in ("false", "no", "off"):
        return False
    if lowered in ("null", "~"):
        return None
    if re.fullmatch(r"-?\d+", text):
        return int(text)
    return text


def _split_flow(text):
    parts, depth, quote, buf = [], 0, None, []
    for ch in text:
        if quote:
            buf.append(ch)
            if ch == quote:
                quote = None
            continue
        if ch in "'\"":
            quote = ch
        elif ch in "[{":
            depth += 1
        elif ch in "]}":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append("".join(buf))
            buf = []
            continue
        buf.append(ch)
    parts.append("".join(buf))
    return parts


def parse_yaml(text):
    """plugin.yml 정도의 YAML을 dict로. 읽을 수 없는 줄은 건너뛴다."""
    raw = text.replace("\t", "    ").splitlines()
    lines = []  # (indent, content, raw line) — 블록 문자열은 원문이 필요하다
    for line in raw:
        content = _strip_comment(line)
        if content.strip() and content.strip() != "---":
            lines.append((len(content) - len(content.lstrip()), content.strip(), line))
    pos = 0

    def block_scalar(style, parent_indent):
        nonlocal pos
        collected = []
        indent = None
        while pos < len(lines) and lines[pos][0] > parent_indent:
            raw_line = lines[pos][2]
            if indent is None:
                indent = len(raw_line) - len(raw_line.lstrip())
            collected.append(raw_line[indent:].rstrip())
            pos += 1
        text = " ".join(c.strip() for c in collected) if style.startswith(">") else "\n".join(collected)
        return text if style.endswith("-") else text + "\n"  # YAML 기본(clip)은 끝 줄바꿈 하나를 남긴다

    def value_after(rest, indent):
        nonlocal pos
        if rest in ("|", "|-", "|+", ">", ">-", ">+"):
            return block_scalar(rest, indent)
        if rest:
            return _scalar(rest)
        if pos < len(lines) and lines[pos][0] > indent:
            return parse_node(lines[pos][0])
        if pos < len(lines) and lines[pos][0] == indent and lines[pos][1].startswith("- "):
            return parse_node(indent)  # 키와 같은 깊이에 쓴 목록
        return None

    def parse_node(indent):
        nonlocal pos
        if lines[pos][1].startswith("-"):
            items = []
            while pos < len(lines) and lines[pos][0] == indent and lines[pos][1].startswith("-"):
                rest = lines[pos][1][1:].strip()
                pos += 1
                if ":" in rest and not rest.startswith(("'", '"', "[", "{")):
                    key, _, value = rest.partition(":")
                    item = {key.strip(): value_after(value.strip(), indent + 1)}
                    if pos < len(lines) and lines[pos][0] > indent:
                        more = parse_node(lines[pos][0])
                        if isinstance(more, dict):
                            item.update(more)
                    items.append(item)
                else:
                    items.append(value_after(rest, indent))
            return items
        out = {}
        while pos < len(lines) and lines[pos][0] == indent and not lines[pos][1].startswith("- "):
            content = lines[pos][1]
            pos += 1
            key, sep, rest = content.partition(":")
            if not sep:
                continue
            out[_scalar(key) if key.strip()[:1] in "'\"" else key.strip()] = value_after(rest.strip(), indent)
        # 더 깊은 곳에 남은 줄(잘못된 들여쓰기)은 버린다
        while pos < len(lines) and lines[pos][0] > indent:
            pos += 1
        return out

    if not lines:
        return {}
    result = parse_node(lines[0][0])
    return result if isinstance(result, dict) else {}


# ─── mods.toml (TOML의 일부) ───────────────────────────────────
# Python 3.10에는 tomllib가 없다. [[mods]] 블록의 modId/version/displayName만 읽는다.

def _parse_mods_toml(text):
    mods, current = [], None
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("[[") and line.endswith("]]"):
            current = {} if line[2:-2].strip() == "mods" else None
            if current is not None:
                mods.append(current)
            continue
        if line.startswith("["):
            current = None
            continue
        if current is None:
            continue
        m = re.match(r'^(modId|version|displayName)\s*=\s*(["\'])(.*?)\2', line)
        if m:
            current[m.group(1)] = m.group(3)
    return mods


def _read_descriptor(zf, names):
    for path, kind in _DESCRIPTORS:
        if path not in names:
            continue
        text = zf.read(path).decode("utf-8", "replace")
        if kind in ("paper", "bukkit"):
            meta = parse_yaml(text)
            if not meta.get("name"):
                continue
            commands = meta.get("commands")
            # paper-plugin.yml은 명령을 선언하지 않는다 — 같은 jar의 plugin.yml에 있으면 그것을 쓴다
            if kind == "paper" and "plugin.yml" in names and not commands:
                commands = parse_yaml(zf.read("plugin.yml").decode("utf-8", "replace")).get("commands")
            return [{"id": str(meta["name"]), "name": str(meta["name"]), "version": _str(meta.get("version")),
                     "kind": kind, "commands": commands if isinstance(commands, dict) else {}}]
        if kind in ("fabric", "quilt"):
            try:
                meta = json.loads(text, strict=False)
            except ValueError:
                continue
            if kind == "quilt":
                meta = meta.get("quilt_loader", {})
                name = (meta.get("metadata") or {}).get("name")
            else:
                name = meta.get("name")
            if not meta.get("id"):
                continue
            return [{"id": str(meta["id"]), "name": str(name or meta["id"]), "version": _str(meta.get("version")),
                     "kind": kind, "commands": {}}]
        mods = _parse_mods_toml(text)
        if mods:
            return [{"id": m["modId"], "name": m.get("displayName") or m["modId"],
                     "version": m.get("version"), "kind": kind, "commands": {}} for m in mods if m.get("modId")]
    return []


def _str(value):
    return None if value is None else str(value)


def scan(working_dir):
    """작업 폴더의 plugins/·mods/에서 플러그인·모드를 찾는다.

    jar 하나에 메타데이터가 없으면(라이브러리 등) 건너뛴다. `.jar.disabled` 같은 꺼진 파일은
    enabled=false로 알려 준다 (서버가 읽지 않는다).
    """
    found = []
    if not working_dir:
        return found
    for sub in _SCAN_DIRS:
        folder = os.path.join(working_dir, sub)
        if not os.path.isdir(folder):
            continue
        pattern = os.path.join(glob.escape(folder), "*.jar")
        for path in sorted(glob.glob(pattern) + glob.glob(pattern + ".disabled")):
            try:
                with zipfile.ZipFile(path) as zf:
                    entries = _read_descriptor(zf, set(zf.namelist()))
            except Exception as e:  # 깨진 jar 하나 때문에 나머지 목록을 잃지 않는다
                print(f"skipped {sub}/{os.path.basename(path)}: {type(e).__name__}", file=sys.stderr)
                continue
            for entry in entries:
                entry["file"] = f"{sub}/{os.path.basename(path)}"
                entry["enabled"] = path.endswith(".jar")
                found.append(entry)
    return found


# ─── 명령 카탈로그 ─────────────────────────────────────────────

def load_catalog():
    """plugin_catalog/*.json — 플러그인 id(소문자) → 정의"""
    by_id = {}
    for path in sorted(glob.glob(os.path.join(CATALOG_DIR, "*.json"))):
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, ValueError):
            continue
        data["slug"] = os.path.splitext(os.path.basename(path))[0]
        for plugin_id in data.get("match", []):
            by_id.setdefault(str(plugin_id).lower(), data)
    return by_id


def _translated(key, fallback):
    text = _catalog_i18n.t(key)
    return fallback if text == key else text


def _catalog_command(slug, cmd):
    """데이터 정의 하나를 데몬 명령 필드로 (지금 언어로 번역해서)"""
    base = f"{slug}.{cmd['name']}"
    out = {
        "name": cmd["name"],
        "label": _translated(f"{base}.label", cmd.get("label") or cmd["name"]),
        "description": _translated(f"{base}.description", cmd.get("description", "")),
        "method": "console",
        "console_template": cmd["console_template"],
        "risk": cmd.get("risk", "danger"),
        "inputs": [],
    }
    for item in cmd.get("inputs", []):
        field = dict(item)
        field.setdefault("type", "text")
        field["label"] = _translated(f"{base}.inputs.{item['name']}.label", item.get("label") or item["name"])
        if item.get("placeholder"):
            field["placeholder"] = _translated(f"{base}.inputs.{item['name']}.placeholder", item["placeholder"])
        out["inputs"].append(field)
    return out


def _declared_command(name, spec):
    """plugin.yml이 선언한 명령 — 인자는 한 칸으로 받아 그대로 붙인다"""
    spec = spec if isinstance(spec, dict) else {}
    usage = spec.get("usage")
    usage = usage.replace("<command>", name).strip() if isinstance(usage, str) else ""
    out = {
        "name": name,
        "label": f"/{name}",
        "description": str(spec.get("description") or "").strip(),
        "method": "console",
        "console_template": f"{name} {{args}}",
        # 무엇을 하는지 모르는 명령이고 인자도 자유롭다
        "risk": "danger",
        "inputs": [{
            "name": "args",
            "type": "text",
            "label": _module_i18n.t("plugins.args_label"),
            "required": False,
            "placeholder": usage.splitlines()[0] if usage else "",
        }],
    }
    return out


def instance_commands(working_dir):
    """설치된 플러그인·모드가 콘솔에서 받는 명령 전체와 판정한 구성요소 목록"""
    catalog = load_catalog()
    components, commands, seen = [], [], set()

    def add(cmd, plugin):
        key = cmd["name"].lower()
        if key in seen:
            return
        seen.add(key)
        cmd["group"] = plugin
        commands.append(cmd)

    for entry in scan(working_dir):
        data = catalog.get(entry["id"].lower())
        components.append({k: entry[k] for k in ("id", "name", "version", "kind", "file", "enabled")}
                          | {"catalog": data["slug"] if data else None})
        if not entry["enabled"]:
            continue
        # 공식 문서 기준 정의가 먼저, plugin.yml에만 있는 명령은 자유 입력으로
        if data:
            for cmd in data.get("commands", []):
                add(_catalog_command(data["slug"], cmd), entry["name"])
        # 카탈로그가 플레이어 전용으로 적은 명령은 콘솔에서 실패하므로 plugin.yml에 있어도 내지 않는다.
        # 하위 명령("co inspect")이 플레이어 전용이면 그 루트의 자유 입력도 내지 않는다 (콘솔용 하위 명령은 카탈로그 정의로 남는다)
        player_only = {str(n).strip().lower() for n in (data or {}).get("player_only", [])}
        blocked = player_only | {n.split()[0] for n in player_only if " " in n}
        for name, spec in entry["commands"].items():
            if str(name).lower() not in blocked:
                add(_declared_command(str(name), spec), entry["name"])
    return {"commands": commands, "components": components}
