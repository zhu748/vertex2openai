"""复现用户报告的场景：出口 1 首回复超时被切换、出口 2 快速成功。

验证两点：
1. 修复前「第 N 次尝试等待 18.4 秒」把出口 1 的超时窗口累加进出口 2；
   修复后 elapsed 归因到当前出口（约等于出口 2 的真实等待）。
2. 切换日志不再武断输出英文类名 FirstReplyTimeout，而是提示
   「首回复超时 N 秒窗口（模型思考较慢或出口不畅均可能…）」。
"""
import asyncio
import os
from pathlib import Path
import sys
import tempfile
import time
from unittest import mock

_TEMP = tempfile.TemporaryDirectory(prefix="v2o-repro-")
os.environ["STATE_DIR"] = _TEMP.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import httpx

from first_reply import timed_call
from proxy_failover import ProxyChain
import request_tracking
from request_tracking import attempt_started_at, upstream_attempt

WINDOW = 1.5      # 模拟 first_reply_timeout_seconds（真实场景 15s，压缩为 1.5s 加速复现）
SECOND_EXIT_WAIT = 0.5  # 出口 2 的真实等待（对应真实场景的 3.4s）


async def main():
    async def operation(raw, resolved):
        if raw.endswith(":6370"):  # 出口 1：一直无首回复
            await asyncio.Event().wait()
        await asyncio.sleep(SECOND_EXIT_WAIT)  # 出口 2：等 0.5s 后出分片
        return httpx.Response(200)

    upstream_attempt()  # 对应 api_helpers 外层 attempt 循环的首次记录
    request_started = time.monotonic()
    # 挂上 trace，展示多出口超时路径同样会发“超过 N 秒”的请求跟踪说明
    # （单出口路径一直有；多出口 proxy_attempt 路径曾缺失，本次补齐）
    trace = request_tracking.RequestTrace("/v1/chat/completions")
    trace_token = request_tracking.current_trace.set(trace)
    try:
        with mock.patch("first_reply.timeout_seconds", return_value=WINDOW):
            result = await timed_call(
                "gemini-4-argon",
                ProxyChain(candidates=["socks5://u:p@1.1.1.1:6370", "socks5://u:p@2.2.2.2:6361"]).run(operation))
    finally:
        request_tracking.current_trace.reset(trace_token)

    # api_helpers:762 的修复后口径：归因到当前出口
    elapsed_new = time.monotonic() - (attempt_started_at() or request_started)
    # 修复前的口径：只在外层 attempt 循环重置（本复现里从未重置）
    elapsed_old = time.monotonic() - request_started
    print()
    print(f"HTTP 状态: {result.status_code}")
    timeout_events = [e for e in trace.events if e["kind"] == "first_reply_timeout"]
    assert len(timeout_events) == 1, "出口 1 超时应发出一条 first_reply_timeout 请求跟踪事件"
    print(f"✅ 超时说明已恢复：trace 事件 → {timeout_events[0]['message'][:60]}...")
    print(f"修复前日志口径: 📥 第 1 次尝试等待 {elapsed_old:.1f} 秒收到分片  (出口1超时 {WINDOW}s + 出口2 {SECOND_EXIT_WAIT}s 被累加)")
    print(f"修复后日志口径: 📥 第 1 次尝试等待 {elapsed_new:.1f} 秒收到分片  (归因到当前出口的真实等待)")
    assert elapsed_new < WINDOW, "修复后 elapsed 必须小于首回复窗口（不再包含出口 1 的超时时间）"
    assert abs(elapsed_new - SECOND_EXIT_WAIT) < 0.3, "修复后 elapsed 应约为出口 2 的真实等待"
    print("✅ 计时归因修复验证通过")


asyncio.run(main())
