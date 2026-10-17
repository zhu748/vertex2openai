#!/usr/bin/env python
"""CI 辅助：校验 .github/workflows/*.yml 的 YAML 语法。

GitHub 表达式占位符（${{ ... }}）在 YAML 里是合法字面量，这里仅做
**结构语法**校验：占位符先替换成普通文本再交给 yaml.safe_load。
任何一次 workflow 改动导致 YAML 结构损坏，都会在推镜像前被拦下。
"""
import pathlib
import sys

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[2]
WF_DIR = ROOT / ".github" / "workflows"


def main() -> int:
    files = sorted(WF_DIR.glob("*.yml")) + sorted(WF_DIR.glob("*.yaml"))
    if not files:
        print("no workflow files found (unexpected)")
        return 1
    failed = False
    for path in files:
        text = path.read_text(encoding="utf-8")
        try:
            data = yaml.safe_load(text)
        except yaml.YAMLError as e:
            print(f"FAIL {path.relative_to(ROOT)}: {e}")
            failed = True
            continue
        if not isinstance(data, dict) or "jobs" not in data:
            print(f"FAIL {path.relative_to(ROOT)}: missing top-level 'jobs'")
            failed = True
            continue
        print(f"YAML OK: {path.relative_to(ROOT)} ({len(data['jobs'])} job(s))")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
