"""统一离线检查入口：python scripts/check_project.py。

先安装 requirements-dev.txt。测试自 2026-11 P2-8 起改走 pytest 单进程：
conftest.py 的三层隔离（规范环境抢先导入 / 收集后复位 / 模块切换前恢复
单例出厂快照）取代了旧的「逐文件独立子进程」模式，68 个测试文件头部的
STATE_DIR + sys.path 样板随之删除。静态与架构检查仍各自独立运行。
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
        ("单元测试", ["-m", "pytest", "tests/", "-q"]),
    ]
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
