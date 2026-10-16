"""module.toml 파일들을 schema/module.schema.json으로 검사한다 (스키마 v1).

스키마는 데몬 코드에서 뽑는다: `saba-core module schema > schema/module.schema.json`
"""
import json
import pathlib
import sys
import tomllib

import jsonschema

root = pathlib.Path(__file__).resolve().parents[2]
schema = json.loads((root / "schema" / "module.schema.json").read_text(encoding="utf-8"))
files = sorted(root.glob("*/module.toml")) + [root / "__example" / "__example__module.toml"]
failed = 0
for f in files:
    data = tomllib.loads(f.read_text(encoding="utf-8-sig"))
    errors = list(jsonschema.Draft7Validator(schema).iter_errors(data))
    for e in errors:
        print(f"FAIL {f.relative_to(root)}: {'.'.join(map(str, e.path)) or '(root)'}: {e.message}")
    if not errors:
        print(f"ok   {f.relative_to(root)}")
    failed += bool(errors)
sys.exit(1 if failed else 0)
