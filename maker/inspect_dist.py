"""배포물 점검 (모듈 메이커)
==========================

    python maker/inspect_dist.py <zip | tar | 폴더> [--json] [--out <파일>]

서버 배포물을 풀지 않고 읽어 조사 표의 "실측" 근거를 만든다:
항목 전체 목록, OS 폴더(Windows·Linux·Mac), 여러 폴더에 같은 이름으로 든 실행 파일, 동봉 설정 파일의 원래 바이트
(인코딩·BOM·줄바꿈, 주석이 아닌 키=값, 주석 처리된 예시 키). 테라리아 zip처럼 `Linux/`에도 `TerrariaServer.exe`가 있거나
설정 파일이 cp1252인 배포물을 짐작 없이 알아보려는 것이다.
"""

import argparse
import json
import os
import pathlib
import re
import sys
import tarfile
import zipfile

CONFIG_SUFFIXES = {".txt", ".ini", ".cfg", ".conf", ".properties", ".json", ".yml", ".yaml", ".toml", ".lua", ".xml"}
EXECUTABLE_SUFFIXES = {".exe", ".jar", ".bat", ".cmd", ".sh", ".x86_64"}
MAX_CONFIG_BYTES = 1 << 20
OS_FOLDERS = {
    "Windows": re.compile(r"(windows|win)(32|64|[-_]?x64|[-_]?x86)?", re.IGNORECASE),
    "Linux": re.compile(r"linux(32|64|[-_]?x64|[-_]?x86|[-_]?arm64)?", re.IGNORECASE),
    "Mac": re.compile(r"(mac|macos|osx)(64|[-_]?arm64|[-_]?x64)?", re.IGNORECASE),
}
KEY = re.compile(r"[A-Za-z0-9_.\-]+")
COMMENT = ("#", ";", "//")


def entries(path):
    """(상대 경로, 읽기 함수) 목록. 폴더는 빼고 파일만"""
    path = pathlib.Path(path)
    if path.is_dir():
        found = []
        for dirpath, _, filenames in os.walk(path):
            for name in filenames:
                full = pathlib.Path(dirpath) / name
                found.append((full.relative_to(path).as_posix(), full.stat().st_size, full.read_bytes))
        return sorted(found, key=lambda e: e[0])
    if zipfile.is_zipfile(path):
        z = zipfile.ZipFile(path)
        return [(i.filename, i.file_size, lambda i=i: z.read(i)) for i in z.infolist() if not i.is_dir()]
    if tarfile.is_tarfile(path):
        t = tarfile.open(path)
        return [(m.name, m.size, lambda m=m: t.extractfile(m).read()) for m in t.getmembers() if m.isfile()]
    raise ValueError(f"zip·tar·폴더가 아니다: {path}")


def os_folders(paths):
    found = {}
    for p in paths:
        parts = p.split("/")[:-1]
        for i, part in enumerate(parts):
            for os_name, pattern in OS_FOLDERS.items():
                if pattern.fullmatch(part):
                    found.setdefault(os_name, set()).add("/".join(parts[:i + 1]))
    return {k: sorted(v) for k, v in sorted(found.items())}


def duplicate_executables(paths):
    by_name = {}
    for p in paths:
        name = p.rsplit("/", 1)[-1]
        if pathlib.PurePosixPath(name).suffix.lower() in EXECUTABLE_SUFFIXES:
            by_name.setdefault(name, []).append(p)
    return {k: sorted(v) for k, v in sorted(by_name.items()) if len(v) > 1}


# UTF-32 LE BOM이 UTF-16 LE BOM으로 시작하므로 긴 것부터 본다
BOMS = ((b"\x00\x00\xfe\xff", "utf-32-be"), (b"\xff\xfe\x00\x00", "utf-32-le"), (b"\xef\xbb\xbf", "utf-8-sig"),
        (b"\xff\xfe", "utf-16-le"), (b"\xfe\xff", "utf-16-be"))


def encoding_of(data):
    for bom, name in BOMS:
        if data.startswith(bom):
            return name, None
    if all(b < 0x80 for b in data):
        return "ascii", None
    try:
        data.decode("utf-8")
        return "utf-8", None
    except UnicodeDecodeError as e:
        return "not utf-8", {"offset": e.start, "byte": f"0x{data[e.start]:02x}"}


def decode(data, encoding):
    """판정한 인코딩으로 푼 글 (BOM은 뺀다). UTF-8이 아니면 cp1252로 본다"""
    bom = next((len(b) for b, name in BOMS if name == encoding), 0)
    codec = {"not utf-8": "cp1252", "utf-8-sig": "utf-8"}.get(encoding, encoding)
    return data[bom:].decode(codec, errors="replace")


def newline_of(text):
    crlf = text.count("\r\n")
    lf = text.count("\n") - crlf
    cr = text.count("\r") - crlf
    kinds = [name for name, count in (("CRLF", crlf), ("LF", lf), ("CR", cr)) if count]
    return kinds[0] if len(kinds) == 1 else ("mixed" if kinds else "none")


def config_file(path, data):
    encoding, bad = encoding_of(data)
    text = decode(data, encoding)
    keys, commented = {}, []
    for raw in text.splitlines():
        line = raw.strip()
        comment = next((c for c in COMMENT if line.startswith(c)), None)
        body = line[len(comment):].strip() if comment else line
        if "=" not in body:
            continue
        key, value = (s.strip() for s in body.split("=", 1))
        if not KEY.fullmatch(key):
            continue
        if comment:
            commented.append(key)
        else:
            keys.setdefault(key, value)
    info = {"path": path, "size": len(data), "encoding": encoding, "newline": newline_of(text), "keys": keys, "commented_keys": commented}
    if bad:
        info["first_bad_byte"] = bad
    return info


def inspect(path):
    listing = entries(path)
    paths = [p for p, _, _ in listing]
    configs = [config_file(p, read()) for p, size, read in listing
               if pathlib.PurePosixPath(p).suffix.lower() in CONFIG_SUFFIXES and size <= MAX_CONFIG_BYTES]
    return {
        "source": str(path), "count": len(paths), "entries": [{"path": p, "size": s} for p, s, _ in listing],
        "os_folders": os_folders(paths), "duplicate_executables": duplicate_executables(paths), "config_files": configs,
    }


def markdown(info):
    out = [f"## 배포물 점검: {pathlib.Path(info['source']).name}", "", f"- 파일 {info['count']}개"]
    if info["os_folders"]:
        out.append("- OS 폴더: " + "; ".join(f"{k} = {', '.join(f'`{p}`' for p in v)}" for k, v in info["os_folders"].items())
                   + " → 설치는 OS 폴더 이름으로 고른다")
    else:
        out.append("- OS 폴더: 없음")
    for name, paths in info["duplicate_executables"].items():
        out.append(f"- 같은 이름의 실행 파일 `{name}`: " + ", ".join(f"`{p}`" for p in paths))
    if info["config_files"]:
        out += ["", "| 설정 파일 | 인코딩 | 줄바꿈 | 키 (주석 아님) | 주석 처리된 키 |", "|---|---|---|---|---|"]
        for f in info["config_files"]:
            enc = f["encoding"] + (f" (첫 바이트 {f['first_bad_byte']['byte']} @ {f['first_bad_byte']['offset']})" if "first_bad_byte" in f else "")
            keys = ", ".join(f"`{k}={v}`" for k, v in f["keys"].items()) or "-"
            out.append(f"| `{f['path']}` | {enc} | {f['newline']} | {keys} | {', '.join(f['commented_keys']) or '-'} |")
    out += ["", "<details><summary>항목 전체</summary>", "", "```"] + [f"{e['size']:>12}  {e['path']}" for e in info["entries"]] + ["```", "", "</details>"]
    return "\n".join(out) + "\n"


def main():
    parser = argparse.ArgumentParser(description="서버 배포물의 목록·OS 폴더·설정 파일 원래 바이트를 본다")
    parser.add_argument("source", help="zip·tar 파일이나 풀어 둔 폴더")
    parser.add_argument("--json", action="store_true", help="JSON으로 출력")
    parser.add_argument("--out", help="출력을 이 파일에 쓴다")
    args = parser.parse_args()
    try:
        info = inspect(args.source)
    except (ValueError, OSError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    text = json.dumps(info, ensure_ascii=False, indent=2) if args.json else markdown(info)
    if args.out:
        pathlib.Path(args.out).write_text(text, encoding="utf-8")
    else:
        sys.stdout.reconfigure(encoding="utf-8")
        print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
