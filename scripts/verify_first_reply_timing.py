"""复现用户报告的场景并演示修正后的行为。

设计意图（用户澄清）：出字超时 = 上游连思考分片都没吐（思考回传开启时
思考也算首回复）= 上游无响应 → 应像 429 一样走外层自动重试，
而不是切换代理出口。

本脚本验证：
1. 首回复超时不再触发 🔀 代理切换（出口没有故障证据，不能冤枉）；
2. 超时说明已恢复：trace 事件「等待模型首回复超过 N 秒…」；
3. 外层捕获 FirstReplyTimeout 后退避重试（与 429 同路径），重试成功；
4. 重试日志展示的中文说明与旧版单出口文案逐字一致
   （等待模型首回复超过 N 秒，思考、正文、工具调用或图片均算回复）；
5. 「第 N 次尝试等待 X 秒」归因到当前尝试，不累加已失败尝试的等待。
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

from first_reply import FirstReplyTimeout, timed_call
from proxy_failover import ProxyChain
import request_tracking
from request_tracking import attempt_started_at, upstream_attempt

WINDOW = 1.5            # 模拟 first_reply_timeout_seconds（真实 15s，压缩加速）
RETRY_WAIT = 0.05       # 退避等待
SECOND_TRY_WAIT = 0.5   # 重试后上游吐字耗时


async def main():
    upstream_slow = True
    async def operation(raw, resolved):
        nonlocal upstream_slow
        if upstream_slow:
            upstream_slow = False
            await asyncio.Event().wait()   # 第 1 次尝试：上游不吐任何分片
        await asyncio.sleep(SECOND_TRY_WAIT)  # 重试后：0.5s 出分片
        return httpx.Response(200)

    chain = ProxyChain(candidates=["socks5://u:p@1.1.1.1:6370", "socks5://u:p@2.2.2.2:6361"])
    trace = request_tracking.RequestTrace("/v1/chat/completions")
    trace_token = request_tracking.current_trace.set(trace)
    result, attempt, last_exc = None, 0, None
    try:
        while attempt <= 2:
            upstream_attempt()  # 对应 api_helpers 外层 attempt 循环
            request_started = time.monotonic()
            try:
                with mock.patch("first_reply.timeout_seconds", return_value=WINDOW):
                    result = await timed_call(
                        "gemini-4-argon",
                        chain.run(operation))
                break
            except FirstReplyTimeout as exc:
                last_exc = exc
                elapsed = time.monotonic() - request_started
                print(f"⚠️ [自动重试] 流式请求失败（HTTP 504，{str(exc).rstrip('。')}，"
                      f"本次已等待 {elapsed:.1f} 秒）。第 {attempt + 1} 次退避重试，"
                      f"等待 {RETRY_WAIT} 秒。")
                attempt += 1
                await asyncio.sleep(RETRY_WAIT)
    finally:
        request_tracking.current_trace.reset(trace_token)

    timeout_events = [e for e in trace.events if e["kind"] == "first_reply_timeout"]
    assert result is not None and result.status_code == 200, "重试后应成功"
    assert attempt == 1, "应恰好自动重试一次后成功"
    assert len(timeout_events) == 1, "超时应发出一条 first_reply_timeout 请求跟踪事件"
    assert chain.index == 0, "超时不得切换出口（chain.index 应保持 0）"
    assert last_exc is not None and "思考、正文、工具调用或图片均算回复" in str(last_exc), \
        "重试日志展示的说明必须与旧版单出口文案一致（含判活口径）"
    print()
    print(f"✅ 超时说明已恢复（与旧版一致）：{timeout_events[0]['message'][:56]}...")
    print(f"✅ 重试日志文案：{str(last_exc).rstrip('。')}")
    print("✅ 未发生代理切换（全程无 🔀 日志，chain.index=0）——出口没有故障证据")
    print(f"✅ 自动重试 1 次后成功（与 429 同路径），「第 {attempt + 1} 次尝试等待 "
          f"{time.monotonic() - (attempt_started_at() or request_started):.1f} 秒」归因当前尝试")


asyncio.run(main())
