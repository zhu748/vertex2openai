"""复现：假流式 / 非流式把「整段生成时间」计入首回复窗口，真流式只计到首个思考分片。

场景：上游模型是"慢但有效"的思考模型——
  - 真流式：首个思考分片立刻到达，正文 0.4s 后到达（总生成 > 窗口）。
  - 非流式/假流式：完整响应 0.4s 后一次性到达（思考 + 正文都在最后）。
窗口 0.15s。三种模式用同一个"上游延迟画像"，对比结果。

修复（2026-11）：整取路径不参与首回复计时，预期三种模式全部成功。
"""
import asyncio
import os
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-repro-fr-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from google.genai import types

import api_helpers
import config
import first_reply
from logger import ProxyStats

WINDOW = 0.15      # 首回复窗口（模拟默认 15s）
SLOW_TOTAL = 0.40  # 上游真实总生成时间（模拟长思考/长输出 > 窗口）


def text_chunk(text="answer", thought=False, finish="STOP"):
    return types.GenerateContentResponse(candidates=[types.Candidate(
        content=types.Content(parts=[types.Part(text=text, thought=thought)]),
        finish_reason=finish)])


SETTINGS = dict(
    config.DEFAULT_SETTINGS,
    retry_max=1, retry_backoff_seconds=0, retry_jitter_seconds=0,
    fake_streaming=False, fake_streaming_interval=0,
)


async def run_case(stream, fake_streaming, label):
    SETTINGS["fake_streaming"] = fake_streaming
    stats = ProxyStats()
    whole_calls, stream_calls = [], []

    async def generate(**kwargs):          # 非流式整取（假流式 & 非流式走这里）
        whole_calls.append(1)
        await asyncio.sleep(SLOW_TOTAL)    # 完整响应在最后才到
        return text_chunk()

    async def generate_stream(**kwargs):   # 真流式
        stream_calls.append(1)
        async def chunks():
            yield text_chunk("thinking", thought=True, finish=None)  # 思考立刻到
            await asyncio.sleep(SLOW_TOTAL)                          # 正文慢慢来
            yield text_chunk()
        return chunks()

    client = SimpleNamespace(aio=SimpleNamespace(models=SimpleNamespace(
        generate_content=generate, generate_content_stream=generate_stream)))
    obj = api_helpers.OpenAIRequest(model="gemini-test", messages=[], stream=stream)

    patches = (
        mock.patch.object(first_reply, "timeout_seconds", return_value=WINDOW),
        mock.patch.object(api_helpers, "timeout_seconds",
                          side_effect=lambda m: first_reply.timeout_seconds(m)),
        mock.patch.object(api_helpers, "stats", stats),
        mock.patch.object(api_helpers.app_state, "get_setting",
                          side_effect=lambda k, d=None: SETTINGS.get(k, d)),
    )
    for p in patches:
        p.start()
    try:
        response = await api_helpers.execute_gemini_call(
            client, obj.model, lambda messages: [], {}, obj)
        try:
            if stream:
                body = "".join([c async for c in response.body_iterator])
            else:
                body = response.body.decode()
        except Exception as e:              # 生成器中途抛出的异常（若未兜底成帧）
            body = f"<EXCEPTION {type(e).__name__}: {e}>"
    finally:
        for p in patches:
            p.stop()

    ok = "answer" in body
    err_line = next((ln for ln in body.splitlines() if '"error"' in ln or "EXCEPTION" in ln), "")
    print(f"[{label}] stream={stream} fake_streaming={fake_streaming}")
    print(f"    结果: {'✅ 拿到正文' if ok else '❌ 没有正文'} | 上游整取调用={len(whole_calls)} 次 "
          f"流式调用={len(stream_calls)} 次 | 重试={stats.retry_counts}")
    if not ok and err_line:
        print(f"    错误帧: {err_line[:180]}")
    return ok


async def main():
    print(f"参数: 首回复窗口={WINDOW}s, 上游总生成={SLOW_TOTAL}s (思考模型, 慢但有效)\n")
    r1 = await run_case(stream=True, fake_streaming=False, label="A 真流式")
    r2 = await run_case(stream=True, fake_streaming=True, label="B 假流式")
    r3 = await run_case(stream=False, fake_streaming=False, label="C 非流式(JSON)")
    print()
    print("结论: 同一延迟画像下（修复后预期全部成功）——")
    print(f"  A 真流式   : {'成功(首思考分片即结束首回复计时, 后续生成不受窗口约束)' if r1 else '失败'}")
    print(f"  B 假流式   : {'成功(整取不参与首回复计时)' if r2 else '失败(整段生成时间被计入首回复窗口 → 必超时 → 重试空烧 → 最终失败)'}")
    print(f"  C 非流式   : {'成功(整取不参与首回复计时, 挂起由读超时/整轮期限兑底)' if r3 else '失败(同 B, 且客户端无法通过改请求规避)'}")


if __name__ == "__main__":
    asyncio.run(main())
