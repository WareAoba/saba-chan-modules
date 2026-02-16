#!/usr/bin/env python3
"""
saba-chan-modules: Build & Release Script
=========================================

모듈 디렉토리를 스캔하여 module.toml을 파싱하고,
manifest.json을 생성한 뒤 각 모듈을 zip으로 압축합니다.

출력:
  dist/
    manifest.json          — 업데이터가 참조하는 모듈 버전 매니페스트
    module-{name}.zip      — 각 모듈의 배포용 압축 파일
    RELEASE_BODY.md        — GitHub Release 본문
    summary_table.md       — Step Summary용 테이블 조각

GitHub Actions Outputs:
  should_release   — 릴리즈를 생성해야 하는지 (true/false)
  tag              — 릴리즈 태그 (modules-YYYYMMDD-HHMMSS)
  release_name     — 릴리즈 이름
  module_count     — 모듈 수
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import uuid
import zipfile

try:
    import tomllib  # Python 3.11+
except ModuleNotFoundError:
    try:
        import tomli as tomllib  # pip install tomli
    except ModuleNotFoundError:
        tomllib = None  # type: ignore[assignment]  — fallback parser 사용
from datetime import datetime, timezone
from pathlib import Path

# ── 설정 ──────────────────────────────────────────────────

REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", Path(__file__).resolve().parents[2]))
DIST_DIR = REPO_ROOT / "dist"

# zip 제외 대상
EXCLUDE_DIRS = {"__pycache__", ".pytest_cache", ".git", "__pypackages__"}
EXCLUDE_EXTENSIONS = {".pyc", ".pyo"}
EXCLUDE_PREFIXES = ("test_",)


# ── 유틸리티 ──────────────────────────────────────────────

def set_output(name: str, value: str) -> None:
    """GitHub Actions output 설정"""
    output_file = os.environ.get("GITHUB_OUTPUT")
    if output_file:
        with open(output_file, "a", encoding="utf-8") as f:
            if "\n" in value:
                delimiter = uuid.uuid4().hex
                f.write(f"{name}<<{delimiter}\n{value}\n{delimiter}\n")
            else:
                f.write(f"{name}={value}\n")
    else:
        # 로컬 실행 시 콘솔 출력
        preview = value[:80] + "..." if len(value) > 80 else value
        print(f"  [OUTPUT] {name} = {preview}")


def sha256_file(path: Path) -> str:
    """파일의 SHA256 해시 계산"""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


# ── 모듈 탐색 ────────────────────────────────────────────

def find_modules() -> list[Path]:
    """module.toml이 있는 디렉토리를 찾아 반환 (_, . 접두사 제외)"""
    modules = []
    for entry in sorted(REPO_ROOT.iterdir()):
        if entry.is_dir() and not entry.name.startswith((".", "_")):
            if (entry / "module.toml").exists():
                modules.append(entry)
    return modules


def parse_module_toml(toml_path: Path) -> dict:
    """module.toml 파싱 → 메타데이터 딕셔너리 반환"""
    if tomllib is not None:
        with open(toml_path, "rb") as f:
            data = tomllib.load(f)
        module = data.get("module", {})
    else:
        # tomllib / tomli 모두 없을 때 간이 파서
        module = _parse_module_section_fallback(toml_path)

    return {
        "name": module.get("name", toml_path.parent.name),
        "version": module.get("version", "0.0.0"),
        "description": module.get("description", ""),
        "display_name": module.get("display_name", module.get("name", "")),
        "game_name": module.get("game_name", ""),
        "entry": module.get("entry", "lifecycle.py"),
    }


def _parse_module_section_fallback(toml_path: Path) -> dict:
    """tomllib 없이 [module] 섹션의 key = "value" 를 파싱하는 간이 파서"""
    result: dict[str, str] = {}
    in_module = False
    kv_re = re.compile(r'^(\w+)\s*=\s*"([^"]*)"')

    with open(toml_path, encoding="utf-8-sig") as f:
        for line in f:
            stripped = line.strip()
            if stripped == "[module]":
                in_module = True
                continue
            if in_module and stripped.startswith("["):
                break  # 다음 섹션
            if in_module:
                m = kv_re.match(stripped)
                if m:
                    result[m.group(1)] = m.group(2)
    return result


# ── 압축 ─────────────────────────────────────────────────

def should_exclude(file_path: Path, base_dir: Path) -> bool:
    """zip에서 제외할 파일인지 판단"""
    rel = file_path.relative_to(base_dir)

    # 제외 디렉토리 하위
    for part in rel.parts:
        if part in EXCLUDE_DIRS:
            return True

    # 제외 확장자
    if file_path.suffix in EXCLUDE_EXTENSIONS:
        return True

    # 테스트 파일
    if file_path.name.startswith(EXCLUDE_PREFIXES):
        return True

    return False


def create_module_zip(module_dir: Path, output_path: Path) -> tuple[str, int, int]:
    """
    모듈 디렉토리를 zip으로 압축.
    Returns: (sha256, file_count, zip_size_bytes)
    """
    file_count = 0
    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        for file_path in sorted(module_dir.rglob("*")):
            if file_path.is_file() and not should_exclude(file_path, module_dir):
                arcname = str(file_path.relative_to(module_dir))
                zf.write(file_path, arcname)
                file_count += 1

    sha256 = sha256_file(output_path)
    zip_size = output_path.stat().st_size
    return sha256, file_count, zip_size


# ── 매니페스트 ────────────────────────────────────────────

def load_previous_manifest() -> dict | None:
    """이전 릴리즈의 manifest.json 로드 (환경변수 PREV_MANIFEST 경로)"""
    prev_path = os.environ.get("PREV_MANIFEST", "")
    if prev_path:
        p = Path(prev_path)
        if p.exists():
            with open(p, encoding="utf-8") as f:
                return json.load(f)
    return None


def detect_changes(
    current: dict[str, dict],
    previous: dict | None,
) -> list[str]:
    """현재 모듈 vs 이전 매니페스트 비교 → 변경 사항 목록"""
    changes: list[str] = []

    if previous is None:
        # 첫 릴리즈
        for info in current.values():
            changes.append(f"✨ 새 모듈: {info['display_name']} v{info['version']}")
        return changes

    prev_modules = previous.get("modules", {})

    for name, info in current.items():
        prev = prev_modules.get(name)
        if prev is None:
            changes.append(f"✨ 새 모듈: {info['display_name']} v{info['version']}")
        elif prev.get("version") != info["version"]:
            changes.append(
                f"⬆️ {info['display_name']}: {prev['version']} → {info['version']}"
            )
        elif prev.get("sha256") != info.get("sha256"):
            changes.append(
                f"🔄 {info['display_name']}: 내용 변경 (버전 동일: v{info['version']})"
            )

    for name in prev_modules:
        if name not in current:
            display = prev_modules[name].get("display_name", name)
            changes.append(f"🗑️ 모듈 제거: {display}")

    return changes


# ── 릴리즈 본문 생성 ─────────────────────────────────────

def build_release_body(
    modules: dict[str, dict],
    changes: list[str],
    generated_at: str,
) -> str:
    """GitHub Release 본문 Markdown 생성"""
    lines: list[str] = []

    # 모듈 버전 테이블
    lines.append("## 📦 Module Versions\n")
    lines.append("| Module | Display Name | Version | Asset |")
    lines.append("|--------|-------------|---------|-------|")
    for name, m in modules.items():
        lines.append(
            f"| `{name}` | **{m['display_name']}** "
            f"| `v{m['version']}` | `{m['asset']}` |"
        )

    # 변경 사항
    if changes:
        lines.append("\n## 📝 Changes\n")
        for c in changes:
            lines.append(f"- {c}")

    # 사용법 안내
    lines.append("\n## 🔧 Usage\n")
    lines.append("```")
    lines.append("# manifest.json을 다운로드하여 모듈 버전 확인")
    lines.append(
        "gh release download --repo WareAoba/saba-chan-modules "
        "--pattern 'manifest.json'"
    )
    lines.append("")
    lines.append("# 특정 모듈만 다운로드")
    lines.append(
        "gh release download --repo WareAoba/saba-chan-modules "
        "--pattern 'module-minecraft.zip'"
    )
    lines.append("```")

    lines.append(f"\n---\n*Generated at {generated_at}*")
    return "\n".join(lines)


# ── 메인 ─────────────────────────────────────────────────

def main() -> None:
    print("=" * 60)
    print("  saba-chan Module Builder")
    print("=" * 60)

    DIST_DIR.mkdir(parents=True, exist_ok=True)

    # ── 1. 모듈 탐색 ──────────────────────────────────
    modules = find_modules()
    if not modules:
        print("\n⚠️  모듈을 찾을 수 없습니다!")
        set_output("should_release", "false")
        sys.exit(0)

    print(f"\n📦 {len(modules)}개 모듈 발견:")
    for m in modules:
        print(f"   └─ {m.name}/")

    # ── 2. 파싱 + 압축 + 매니페스트 데이터 수집 ──────
    manifest_modules: dict[str, dict] = {}
    summary_rows: list[str] = []

    for module_dir in modules:
        toml_path = module_dir / "module.toml"
        meta = parse_module_toml(toml_path)
        name = meta["name"]

        print(f"\n🔍 {meta['display_name']} (v{meta['version']})")

        # zip 생성
        asset_name = f"module-{name}.zip"
        zip_path = DIST_DIR / asset_name
        sha256, file_count, zip_size = create_module_zip(module_dir, zip_path)

        print(f"   📦 {asset_name}  ({file_count} files, {zip_size:,} bytes)")
        print(f"   🔒 SHA256: {sha256[:16]}...")

        manifest_modules[name] = {
            "version": meta["version"],
            "asset": asset_name,
            "sha256": sha256,
            "install_dir": f"modules/{name}",
            "display_name": meta["display_name"],
            "description": meta["description"],
            "game_name": meta["game_name"],
        }

        summary_rows.append(f"| **{meta['display_name']}** | `v{meta['version']}` |")

    # ── 3. manifest.json 생성 ─────────────────────────
    now = datetime.now(timezone.utc)
    generated_at = now.isoformat()

    manifest = {
        "schema_version": 1,
        "generated_at": generated_at,
        "modules": manifest_modules,
    }

    manifest_path = DIST_DIR / "manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    print(f"\n📋 manifest.json 생성 완료")

    # ── 4. 변경 사항 감지 ─────────────────────────────
    prev_manifest = load_previous_manifest()
    changes = detect_changes(manifest_modules, prev_manifest)
    force = os.environ.get("FORCE_RELEASE", "false").lower() == "true"

    should_release = len(changes) > 0 or force

    if not should_release:
        print("\n⏭️  변경 사항 없음 — 릴리즈 건너뜀")
        set_output("should_release", "false")
        return

    print(f"\n📝 변경 사항 {len(changes)}건:")
    for c in changes:
        print(f"   {c}")

    # ── 5. 릴리즈 정보 생성 ───────────────────────────
    tag = now.strftime("modules-%Y%m%d-%H%M%S")

    # 릴리즈 이름: 각 모듈 버전 요약
    version_parts = [
        f"{m['display_name']} v{m['version']}" for m in manifest_modules.values()
    ]
    release_name = f"Modules — {', '.join(version_parts)}"

    # 릴리즈 본문
    release_body = build_release_body(manifest_modules, changes, generated_at)
    body_path = DIST_DIR / "RELEASE_BODY.md"
    with open(body_path, "w", encoding="utf-8") as f:
        f.write(release_body)

    # Step summary용 테이블
    table_path = DIST_DIR / "summary_table.md"
    with open(table_path, "w", encoding="utf-8") as f:
        f.write("\n".join(summary_rows))

    # ── 6. GitHub Actions Outputs ─────────────────────
    set_output("should_release", "true")
    set_output("tag", tag)
    set_output("release_name", release_name)
    set_output("module_count", str(len(manifest_modules)))

    print(f"\n{'=' * 60}")
    print(f"  ✅ 빌드 완료!")
    print(f"  🏷️  태그: {tag}")
    print(f"  📦 모듈: {len(manifest_modules)}개")
    print(f"  📝 변경: {len(changes)}건")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
