"""QUA-P2-10（2026-11 审查收敛）：裸 print 基线闸——只降不升。

旧代码 262 处裸 print（emoji 当级别用）逐步迁移到 logger.log 门面；
在迁移完成前，本闸保证存量只减不增：

- 任一文件 print 数超过基线 → 失败（防止新增）
- 新文件出现 print → 失败
- 某文件 print 数低于基线 → 提示更新基线（运行 --update 重写）

用法：
    python scripts/check_print_baseline.py           # 检查
    python scripts/check_print_baseline.py --update  # 基线随迁移下调

计数口径：app/**.py 中 `print(` 词法出现次数（排除 logger.py——
custom_print 管线本体；排除注释/字符串中的 "print(" ——逐行粗判）。
与 ruff T201 的区别：T201 需要逐处 noqa 基线化（262 行噪音 diff），
基线闸零侵入且天然允许「同文件减一处、别处不加」的渐进迁移。
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASELINE_FILE = Path(__file__).with_suffix(".json")
# 管线本体（original_print/custom_print 的 print 调用是机制而非日志）
EXEMPT = {"app/logger.py"}

_CALL = re.compile(r"(?<![\w.])print\(")
# 抹掉字符串字面量内容后再匹配：字符串里的 "print(" 不是调用。
_STRINGS = re.compile(r"""(['"])(?:\\.|(?!\1).)*\1""")


def count_prints(path: Path) -> int:
    """统计真实 print 调用：剔除注释行与字符串字面量内的 "print("。
    本闸只需防增量，逐行粗判（跨行三引号按行处理）已足够。"""
    count = 0
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        code_only = _STRINGS.sub("''", stripped)
        if _CALL.search(code_only):
            count += 1
    return count


def collect() -> dict[str, int]:
    result: dict[str, int] = {}
    for path in sorted((ROOT / "app").rglob("*.py")):
        rel = path.relative_to(ROOT).as_posix()
        if rel in EXEMPT:
            continue
        n = count_prints(path)
        if n:
            result[rel] = n
    return result


def main() -> int:
    if "--update" in sys.argv:
        current = collect()
        BASELINE_FILE.write_text(json.dumps(current, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                                 encoding="utf-8")
        print(f"基线已更新：{len(current)} 个文件 / {sum(current.values())} 处 print。")
        return 0
    if not BASELINE_FILE.exists():
        print(f"基线文件缺失：{BASELINE_FILE}", file=sys.stderr)
        return 1
    baseline: dict[str, int] = json.loads(BASELINE_FILE.read_text(encoding="utf-8"))
    current = collect()

    problems: list[str] = []
    for path, n in sorted(current.items()):
        allowed = baseline.get(path, 0)
        if n > allowed:
            problems.append(f"  + {path}: {allowed} -> {n}（新增裸 print 禁止，请改用 logger.log）")
    removed = [(p, baseline[p]) for p in sorted(baseline) if p not in current]
    for path, n in removed:
        problems.append(f"  - {path}: 基线 {n}，现已清零（请运行 --update 下调基线）")
    for path in sorted(set(baseline) & set(current)):
        if current[path] < baseline[path]:
            problems.append(f"  ~ {path}: {baseline[path]} -> {current[path]}（已迁移，请运行 --update 下调基线）")

    if problems:
        print("print 基线闸未通过：", file=sys.stderr)
        for line in problems:
            print(line, file=sys.stderr)
        return 1
    print(f"print 基线闸通过：{len(current)} 个文件 / {sum(current.values())} 处（只降不升）。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
