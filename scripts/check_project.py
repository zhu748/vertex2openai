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

# ENG-P3-10（2026-11 审查）：入口韧性——compileall 对缺失目录静默 rc=0
# 通过（形同跳过）；脚本类检查的目标文件缺失时 subprocess 抛
# FileNotFoundError 直接炸掉整个检查入口。先做存在性预检，缺失即显式失败。
_REQUIRED_PATHS = {
    "app": "应用源码目录",
    "tests": "测试目录",
    "scripts": "脚本目录",
    ".github/scripts/check_workflows.py": "工作流检查脚本",
    "scripts/check_architecture.py": "模块边界检查脚本",
    "scripts/probe_sdk_channel_profiles.py": "SDK 档案探针脚本",
}


def _missing_paths() -> list:
    return [label for path, label in _REQUIRED_PATHS.items() if not (ROOT / path).exists()]


def main() -> int:
    missing = _missing_paths()
    if missing:
        print("以下检查目标缺失，无法执行检查：", file=sys.stderr)
        for label in missing:
            print(f"  - {label}", file=sys.stderr)
        return 1
    checks = [
        ("语法", ["-m", "compileall", "-q", "app", "tests", "scripts", ".github/scripts"]),
        # ENG-P3-13（2026-11 收尾）：在原有骨架上扩入 B（bugbear）与安全的 UP（pyupgrade）
        # 规则族——B023/B904/B007/B017/B905 已全部清零；UP006/UP035/UP045（合计约
        # 390 处注解风格改写）按审查「逐步扩」的意见继续分批，不在本次范围。
        # tests/ 的 B023 豁免：测试夹具中「闭包捕获循环变量」是参数化 mock 的
        # 合法模式（92 处，均在同迭代内消费），无延迟绑定风险。
        ("静态检查", ["-m", "ruff", "check", "app", "tests", "scripts",
            "--select", "E9,F63,F7,F82,B012,B023,B007,B017,B904,B905,F401,F811,F841,"
                       "UP009,UP012,UP015,UP017,UP034,UP037,UP041",
            "--per-file-ignores", "tests/**:B023"]),
        # QUA-P2-5 第一阶段闸（R13）：upstreams 家族全量注解；第二阶段（R14）
        # 纳入核心管线工具（first_reply / streaming / tool_stream /
        # message_processing / usage）；其余目录按「纯逻辑模块先行」逐步纳入。
        ("注解闸（upstreams+管线工具）", ["-m", "ruff", "check", "app/upstreams",
            "app/first_reply.py", "app/streaming.py", "app/tool_stream.py",
            "app/message_processing.py", "app/usage.py",
            "--select", "ANN001,ANN201"]),
        # QUA-P3-2 复杂度闸：max-complexity=40（R14 起零基线——四个超标
        # 函数全部拆分完毕），只挡新增；后续目标 35（extract_events 40 /
        # _claude_stream_response 37 待拆）。
        ("复杂度闸（C901≤40）", ["-m", "ruff", "check", "app", "--select", "C901",
                                "--config", "lint.mccabe.max-complexity=40"]),
        ("工作流", [".github/scripts/check_workflows.py"]),
        ("模块边界", ["scripts/check_architecture.py"]),
        # QUA-P2-10：裸 print 只降不升（基线化存量 262 处，新代码用 logger.log）。
        ("print 基线", ["scripts/check_print_baseline.py"]),
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
        except FileNotFoundError as exc:
            # 防御：预检已覆盖已知目标，这里兜底未来新增的检查项。
            failures.append(name)
            print(f"检查程序缺失：{exc}", file=sys.stderr)
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
