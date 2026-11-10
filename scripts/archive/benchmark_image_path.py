"""生图链路优化基准（2026-11-03）。

量化本轮两项可测优化（其余为正确性类，见 docs/optimization-2026-11-03.md）：
1. 输入图透明抹平的峰值内存：旧“无条件 convert(RGBA)+白底+paste” vs
   新“按模式与实际透明度分流”。PIL 像素缓冲走 C 层 malloc，tracemalloc
   不可见，故用子进程 ru_maxrss（峰值 RSS 高水位）差值法：子进程先解码
   输入并记录水位，再单独执行抹平段，差值即抹平段新增峰值。
2. Express 假流式巨帧发射耗时：逐帧 sleep(0.01) 下限 vs 零步进实测。

组件级测量（非 HTTP 端到端、非多请求并发）。复现：
    python scripts/benchmark_image_path.py
"""

import asyncio
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

_STATE = tempfile.TemporaryDirectory(prefix="v2o-imgbench-")
os.environ.setdefault("STATE_DIR", _STATE.name)
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from upstreams.express_response import (  # noqa: E402
    _chunk_openai_response_dict_for_sse,
)
from message_processing import GIANT_CONTENT_THRESHOLD  # noqa: E402

RUNS = 5

# Cookie 生图整取收集峰值的子进程脚本（与回归测试同构的离线桩）：
# 子进程先完成导入、构造响应文本并记录水位，再跑 _collect_full_response，
# 高水位差即「解析→事件消费→正文拼接」整段的收集期新增峰值。
_CHILD_COLLECT = r"""
import asyncio, json, resource, sys
from pathlib import Path
sys.path.insert(0, str(Path(sys.argv[2]) / "app"))
import upstreams.cookie_proxy as cp

mb = int(sys.argv[1])
b64 = "QkFTRTY0" * (mb * 1024 * 1024 // 8)
body = {"results": [{"data": {"candidates": [{
    "content": {"parts": [{"text": "这是生成的图："}, {"inlineData": {"mimeType": "image/png", "data": b64}}, {"text": "（完）"}]},
    "finishReason": "STOP"}]}}]}

class FakeResponse:
    status_code = 200
    headers = {}
    text = json.dumps(body)

class FakeClient:
    async def post(self, *a, **k):
        return FakeResponse()

class FakeCM:
    async def __aenter__(self): return FakeClient()
    async def __aexit__(self, *exc): return False

class FakeReq:
    async def is_disconnected(self): return False

async def fake_timed_call(model, op, *, whole_call=False, seconds=None):
    return await op

from unittest import mock
patches = [
    mock.patch.object(cp, "open_upstream_client", return_value=FakeCM()),
    mock.patch.object(cp, "timed_call", side_effect=fake_timed_call),
    mock.patch.object(cp, "build_headers", return_value={"auth": "1"}),
    mock.patch.object(cp, "build_request_body_async", mock.AsyncMock(return_value={"contents": []})),
    mock.patch.object(cp, "stats"),
]
def maxrss():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
with patches[0], patches[1], patches[2], patches[3], patches[4]:
    before = maxrss()
    result = asyncio.run(cp._collect_full_response(
        "proj-1", "gemini-3.1-flash-image", mock.Mock(), "cookie", {},
        retry_max=0, backoff_sec=0, jitter_sec=0, fastapi_request=FakeReq()))
    after = maxrss()
assert result["kind"] == "ok", result
print(after - before)
"""


def bench_cookie_collect(root: Path):
    print(f"== Cookie 生图整取收集期新增峰值 RSS（{RUNS} 次中位数；含返回的 full_text 驻留）==")
    for mb in (12, 48):
        results = []
        for _ in range(RUNS):
            proc = subprocess.run(
                [sys.executable, "-c", _CHILD_COLLECT, str(mb), str(root)],
                capture_output=True, text=True, check=True,
            )
            results.append(int(proc.stdout.strip()) / 1024)
        results.sort()
        print(f"  单对象 base64 {mb:2d}MB → 收集期新增峰值 {results[RUNS // 2]:7.1f}MB")


_CHILD = r"""
import io, resource, sys
from PIL import Image, ImageOps

mode = sys.argv[1]
variant = sys.argv[2]
width, height = int(sys.argv[3]), int(sys.argv[4])

def maxrss():
    # Linux 上 ru_maxrss 单位是 KiB。
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

if mode == "RGBA-opaque":
    img = Image.new("RGBA", (width, height), (128, 64, 32, 255))
elif mode == "RGBA-transparent":
    img = Image.new("RGBA", (width, height), (128, 64, 32, 0))
else:  # RGBA-semi
    img = Image.new("RGBA", (width, height), (0, 0, 0, 128))

img.load()
before = maxrss()

def flatten_old(im):
    if im.mode in ("RGBA", "LA", "P"):
        background = Image.new("RGB", im.size, (255, 255, 255))
        rgba = im.convert("RGBA")
        background.paste(rgba, mask=rgba.getchannel("A"))
        return background
    if im.mode != "RGB":
        return im.convert("RGB")
    return im

def flatten_new(im):
    if im.mode == "P":
        im = im.convert("RGBA") if "transparency" in im.info else im.convert("RGB")
    if im.mode in ("RGBA", "LA"):
        lo, hi = im.getextrema()[-1]
        if lo == 255:
            return im.convert("RGB")
        if hi == 0:
            return Image.new("RGB", im.size, (255, 255, 255))
        background = Image.new("RGB", im.size, (255, 255, 255))
        background.paste(im, mask=im.getchannel("A"))
        return background
    if im.mode != "RGB":
        return im.convert("RGB")
    return im

out = flatten_old(img) if variant == "old" else flatten_new(img)
out.load()
after = maxrss()
print(after - before)
"""


def _flatten_peak_mb(mode: str, variant: str, size: tuple[int, int]) -> float:
    """单次子进程测量抹平段新增峰值 RSS（KiB→MB），取 RUNS 次中位数。"""
    results = []
    for _ in range(RUNS):
        proc = subprocess.run(
            [sys.executable, "-c", _CHILD, mode, variant, str(size[0]), str(size[1])],
            capture_output=True, text=True, check=True,
        )
        results.append(int(proc.stdout.strip()) / 1024)
    results.sort()
    return results[RUNS // 2]


def bench_flatten():
    size = (6000, 4000)   # 24MP：约等于 4K（3840×2160≈8.3MP）的两倍多，
                          # 也覆盖多图并发叠加的场景
    print(f"== 输入图透明抹平段新增峰值 RSS（{size[0]}x{size[1]}，子进程高水位差，{RUNS} 次中位数）==")
    for mode in ("RGBA-opaque", "RGBA-semi", "RGBA-transparent"):
        old = _flatten_peak_mb(mode, "old", size)
        new = _flatten_peak_mb(mode, "new", size)
        note = "（行为不变，仍做白底合成）" if mode == "RGBA-semi" else ""
        print(f"  {mode:18s} 旧={old:7.1f}MB  新={new:7.1f}MB  差={new-old:+7.1f}MB {note}")


def bench_pacing():
    print("== Express 假流式巨帧发射（2.25MiB content）==")
    content = "![Image](data:image/png;base64," + "A" * ((1 << 20) + (1 << 18) * 5) + ")"
    assert len(content) > GIANT_CONTENT_THRESHOLD
    response_dict = {
        "id": "chatcmpl-bench",
        "model": "gemini-3.1-flash-image",
        "created": 1,
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": content},
            "finish_reason": "stop",
        }],
    }

    async def emit():
        count = 0
        async for _frame in _chunk_openai_response_dict_for_sse(response_dict):
            count += 1
        return count

    count = asyncio.run(emit())
    timings = []
    for _ in range(RUNS):
        started = time.perf_counter()
        asyncio.run(emit())
        timings.append(time.perf_counter() - started)
    timings.sort()
    new_median = timings[RUNS // 2]
    print(f"  帧数={count}（切片拼接逐字节一致由回归测试保证）")
    print(f"  新实现发射耗时中位数    {new_median*1000:7.1f}ms（sleep(0) 仅让出循环）")
    print(f"  旧节奏耗时下限         {count*10:7.0f}ms（逐帧 sleep(0.01)）")


def main():
    root = Path(__file__).resolve().parents[1]
    print(f"Python {sys.version.split()[0]}\n")
    bench_flatten()
    print()
    bench_cookie_collect(root)
    print()
    bench_pacing()


if __name__ == "__main__":
    main()
