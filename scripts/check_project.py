"""统一离线检查入口：python scripts/check_project.py。

先安装 requirements-dev.txt；每个测试脚本独立运行，避免模块级状态相互污染。
"""
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    checks = [
        ("语法", ["-m", "compileall", "-q", "app", "tests", "scripts", ".github/scripts"]),
        ("静态检查", ["-m", "ruff", "check", "app", "tests", "scripts", "--select", "E9,F63,F7,F82,B012,F401,F811,F841"]),
        ("工作流", [".github/scripts/check_workflows.py"]),
        ("模块边界", ["scripts/check_architecture.py"]),
    ]
    checks.extend((path.name, [str(path.relative_to(ROOT))])
                  for path in sorted((ROOT / "tests").glob("test_*.py")))
    checks.append(("SDK 档案兼容", ["scripts/probe_sdk_channel_profiles.py"]))
    failures = []
    for name, args in checks:
        print(f"\n检查：{name}", flush=True)
        try:
            result = subprocess.run([sys.executable, *args], cwd=ROOT,
                                    capture_output=True, text=True, timeout=120)
        except subprocess.TimeoutExpired:
            failures.append(name)
            print("检查超时", file=sys.stderr)
            continue
        if result.returncode:
            failures.append(name)
            print(result.stdout)
            print(result.stderr, file=sys.stderr)
        else:
            print("通过", flush=True)
    print(f"\n{len(checks) - len(failures)}/{len(checks)} 组检查通过。")
    return int(bool(failures))


if __name__ == "__main__":
    raise SystemExit(main())
