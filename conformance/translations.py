"""번역 파일(locales/*.json) 검사 규칙. 적합성 키트(test_conformance.py)와 메이커 관문(maker/check.py)이 같이 쓴다.
표준 라이브러리만 쓴다 (메이커 도구는 jsonschema·pytest 없이 돈다)"""
import json
import re


def without_aliases(locale):
    return {k: v for k, v in locale.items() if k != "aliases"}


def flat_keys(obj, prefix=""):
    keys = set()
    for k, v in obj.items():
        keys |= flat_keys(v, f"{prefix}{k}.") if isinstance(v, dict) else {prefix + k}
    return keys


# en과 같아도 되는 문구: 고유명사·상표·약어·게임 안 이름, 언어 자기 이름
SAME_AS_EN_OK = {
    "MOTD", "PVP", "PvP", "IP", "JSON", "Deflate", "LZ4", "X", "Y", "x,y,z", "Steam VAC", "UPnP",
    "Vanilla", "CraftBukkit", "Fabric", "Folia", "Forge", "NeoForge", "Paper", "Pufferfish", "Purpur", "Quilt", "Spigot",
    "For the Worthy", "The Constant", "Remix", "Zenith",
    "Deutsch", "English", "Español", "Français", "Italiano", "Polski", "Português (Brasil)", "Русский",
    "日本語", "한국어", "简体中文", "繁體中文",
}
# 그 언어에서도 철자가 같은 낱말
SAME_AS_EN_OK_BY_LANG = {
    "de": {"Whitelist", "Hardcore", "Debug", "Normal", "Port", "Version", "Name", "Text", "Region", "Mods", "Ping"},
    "es": {"Whitelist", "Hardcore", "Normal", "Casual", "Invisible", "Mods", "Ping"},
    "fr": {"Message", "Destination", "Action", "Arguments", "Description", "Hardcore", "Normal", "Port", "Version",
           "Expert", "Invisible", "Factions", "Mods", "Ping"},
    "pt-BR": {"Whitelist", "Hardcore", "Normal", "Casual", "Expert", "Mods", "Ping"},
    "ja": {"Ping"},
}
SAME_AS_EN_MAX_RATIO = 0.01


def flat_strings(obj, prefix=""):
    out = {}
    for k, v in obj.items():
        if isinstance(v, dict):
            out.update(flat_strings(v, f"{prefix}{k}."))
        elif isinstance(v, str):
            out[prefix + k] = v
    return out


def untranslated(en, other, lang):
    """en과 문자가 같은데 허용 목록에 없는 키. 글자 없는 값({{path}}, 숫자)과 module.display_name은 뺀다"""
    ok = SAME_AS_EN_OK | SAME_AS_EN_OK_BY_LANG.get(lang, set())
    out = []
    for key, value in other.items():
        if key == "module.display_name" or en.get(key) != value or value in ok:
            continue
        if not any(c.isalpha() for c in re.sub(r"\{\{.*?\}\}", "", value)):
            continue
        out.append(key)
    return sorted(out)


def _load(path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def key_problems(locales):
    """언어 파일마다 en.json과 다른 키. 별명(`aliases`)은 그 언어로 답할 때만 맞추는 낱말이라 비교하지 않는다"""
    en = flat_keys(without_aliases(_load(locales / "en.json")))
    problems = []
    for f in sorted(locales.glob("*.json")):
        try:
            keys = flat_keys(without_aliases(_load(f)))
        except json.JSONDecodeError as e:
            problems.append(f"{f.name}: JSON 오류 {e}")
            continue
        if keys - en:
            problems.append(f"{f.name}: en.json에 없는 키 {sorted(keys - en)[:5]}")
        if en - keys:
            # 빠진 키는 그 언어에서 영어로 보인다 (사용자 문구는 10개 언어)
            problems.append(f"{f.name}: 번역 안 된 키 {sorted(en - keys)[:5]}")
    return problems


def english_left(locales):
    """en이 아닌 언어에서 en과 같은 문구가 많은 파일 (번역하지 않고 남긴 영어)"""
    en = flat_strings(_load(locales / "en.json"))
    problems = []
    for f in sorted(locales.glob("*.json")):
        if f.stem == "en":
            continue
        other = flat_strings(_load(f))
        left = untranslated(en, other, f.stem)
        if other and len(left) / len(other) > SAME_AS_EN_MAX_RATIO:
            problems.append(f"{f.name}: en과 같은 문구 {len(left)}/{len(other)}개 {left[:5]}")
    return problems
