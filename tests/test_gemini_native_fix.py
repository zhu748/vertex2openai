# -*- coding: utf-8 -*-
"""vertex2openai Gemini 原生透传 404 修复的回归测试。

背景（根因）：
  旧版 gemini_native._passthrough_once 无条件给 model_path 拼上
  publishers/google/models/ 前缀。当 express_location 生效（默认 global）
  且 Key 带 Project ID 时，resolve_express_model_path 返回完整路径
  projects/{p}/locations/{l}/publishers/google/models/{m}，叠加后变成
  …/v1beta1/publishers/google/models/projects/… 双重前缀畸形 URL，
  Google 前端直接返回 404 空 body（客户端显示 Unknown error: 404）。
  OpenAI 兼容路径走 SDK（t_model() 对 projects/ 原样透传），URL 正确。

修复内容：
  1. _build_upstream_url：完整资源路径原样拼接，对齐 SDK t_model()；
  2. _passthrough_request：钉定路径失败（404 Publisher model not found /
     403 requires billing）自动退回裸模型名重试一次（对齐 SDK 通道）。

运行：python tests/test_gemini_native_fix.py
"""
import asyncio
import json
import os
import sys
import tempfile
import types
from pathlib import Path

# 必须在 import 项目模块之前设置（runtime_state 模块级读取）
_TEST_STATE_DIR = tempfile.TemporaryDirectory(prefix="v2o-native-test-")
os.environ["STATE_DIR"] = _TEST_STATE_DIR.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import httpx
import unittest.mock as mock

from routes import gemini_native

PROJECT = "141662177969"
MODEL = "gemini-4-argon"
EXPRESS_HOST = "https://aiplatform.googleapis.com"

# 正确的钉定 URL（修复后透传应生成的形态，与 SDK 一致；
# api_version 钉 v1beta1 = google-genai SDK 在 vertex 模式的真实默认）
PINNED_PATH = f"projects/{PROJECT}/locations/global/publishers/google/models/{MODEL}"
PINNED_URL = f"{EXPRESS_HOST}/v1beta1/{PINNED_PATH}"
# 裸模型名 URL（Express 端点格式）
BARE_URL = f"{EXPRESS_HOST}/v1beta1/publishers/google/models/{MODEL}"


# ============================================================
# 测试基建：stub request / manager / client-slot / MockTransport
# ============================================================
class StubRequest:
    def __init__(self, manager):
        self.app = types.SimpleNamespace(
            state=types.SimpleNamespace(express_key_manager=manager))

    async def is_disconnected(self):
        return False


class StubManager:
    """轮询语义的 ExpressKeyManager 替身。"""
    def __init__(self, records):
        self.records = list(records)
        self.i = 0

    def get_total_keys(self):
        return len(self.records)

    def get_express_record(self, exclude_key=None):
        if not self.records:
            return None
        rec = self.records[self.i % len(self.records)]
        self.i += 1
        return dict(rec)


class StubSlot:
    """client_pool.passthrough_client_pool 替身：永远返回给定的 mock 客户端。"""
    def __init__(self, client):
        self.client = client

    def get(self, factory, proxy_uri=None):
        return self.client


def make_transport(recorder, responder):
    """recorder: 收集请求 URL/key/头线序/body；responder(request) -> httpx.Response。"""

    def handler(request: httpx.Request) -> httpx.Response:
        recorder.append({
            "url": str(request.url),
            "api_key": request.headers.get("x-goog-api-key", ""),
            "accept_encoding": request.headers.get("accept-encoding", ""),
            "raw_headers": [(k.decode("latin-1"), v.decode("latin-1"))
                             for k, v in request.headers.raw],
            "body": request.content,
        })
        return responder(request)

    return httpx.MockTransport(handler)


def sse_body(text: str) -> bytes:
    return (f'data: {{"candidates":[{{"content":{{"parts":[{{"text":"{text}"}}]}}}}]}}\n\n').encode()


def streaming_sse_response(*chunks: bytes):
    """构造流式形态的上游响应。

    注意：httpx.MockTransport 的 handler 里用 content=bytes 构造 Response 会
    置 is_stream_consumed=True，之后 aiter_raw()（_relay_stream 中继用）会抛
    StreamConsumed。真实网络栈返回的是字节流形态，因此这里必须用
    async generator 构造，让响应保持「未消费的流」。
    """
    async def gen():
        for c in chunks:
            yield c
    return httpx.Response(200, headers={"content-type": "text/event-stream"},
                          content=gen())


def run_passthrough(model, action, query, payload, records, responder, settings):
    """端到端跑一次 _passthrough_request，返回 (response, body, recorder)。

    payload 支持 dict（按客户端 SDK 形态序列化为原始字节：默认分隔符、
    保留键序）或直接传 bytes（模拟任意客户端原始字节）。

    注意：StreamingResponse 的 body_iterator 是异步生成器，必须与请求
    在同一个事件循环里消费（跨 asyncio.run 会 StreamConsumed），
    因此请求与消费合并在一次 asyncio.run 里完成。
    """
    if not isinstance(payload, (bytes, bytearray)):
        payload = json.dumps(payload).encode("utf-8")
    recorder = []
    client = httpx.AsyncClient(transport=make_transport(recorder, responder),
                               timeout=httpx.Timeout(10.0))
    slot = StubSlot(client)
    req = StubRequest(StubManager(records))

    async def _run():
        with mock.patch.object(gemini_native, "passthrough_client_pool", slot), \
             mock.patch.object(gemini_native.app_state, "get_settings",
                               return_value=dict(settings)):
            resp = await gemini_native._passthrough_request(
                req, model, action, query, payload)
            if hasattr(resp, "body_iterator"):
                chunks = []
                async for chunk in resp.body_iterator:
                    chunks.append(chunk if isinstance(chunk, bytes) else bytes(chunk))
                body = b"".join(chunks)
            else:
                body = resp.body if resp.body is not None else b""
            return resp, body

    resp, body = asyncio.run(_run())
    return resp, body, recorder


PASS, FAIL = 0, 0
def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✅ {name}")
    else:
        FAIL += 1
        print(f"  ❌ {name} {detail}")


# ============================================================
# 1. 纯函数：_build_upstream_url
# ============================================================
def test_build_upstream_url():
    print("\n[1] _build_upstream_url 纯函数")
    f = gemini_native._build_upstream_url

    check("裸模型名 → Express 端点格式",
          f(MODEL, "streamGenerateContent") == f"{BARE_URL}:streamGenerateContent",
          f(MODEL, "streamGenerateContent"))

    check("钉定完整路径 → 原样拼接（不再叠前缀）",
          f(PINNED_PATH, "generateContent") == f"{PINNED_URL}:generateContent",
          f(PINNED_PATH, "generateContent"))

    check("客户端自带 projects/ 完整路径 → 原样",
          f(f"projects/{PROJECT}/locations/us-central1/publishers/google/models/x", "countTokens")
          == f"{EXPRESS_HOST}/v1beta1/projects/{PROJECT}/locations/us-central1/publishers/google/models/x:countTokens")

    check("models/ 前缀 → 原样",
          f("models/gemini-2.5-pro", "generateContent")
          == f"{EXPRESS_HOST}/v1beta1/models/gemini-2.5-pro:generateContent")

    check("publisher/model 形态 → 原样（publishers/ 开头）",
          f("publishers/google/models/gemini-x", "generateContent")
          == f"{EXPRESS_HOST}/v1beta1/publishers/google/models/gemini-x:generateContent")

    # 关键回归断言：任何情况下都不允许出现双重前缀
    for path in (MODEL, PINNED_PATH, "models/x", "tunedModels/y", "endpoints/z"):
        url = f(path, "generateContent")
        check(f"无双重前缀（{path[:30]}…）",
              "/publishers/google/models/projects/" not in url
              and "/publishers/google/models/publishers/" not in url
              and "/publishers/google/models/models/" not in url, url)


def test_url_matches_official_sdk():
    """[1b] 与官方 SDK 实测 URL 逐段一致（防 api_version 漂移回归）。

    背景：透传曾写死 v1，而 google-genai SDK（vertex 模式）默认
    v1beta1——v1 端点的 streamGenerateContent 对 alt=sse 行为不一致，
    导致客户端无限转圈且无任何报错。本测试直接构造官方 SDK
    （MockTransport 注入，不发网络）抓其真实 URL，与
    _build_upstream_url 的输出逐段对照，SDK 改默认版本时测试会立刻叫。
    """
    print("\n[1b] 上游 URL 与官方 SDK 实测一致（api_version 防漂移）")

    from httpx import MockTransport

    box = {}

    async def handler(request):
        box["url"] = str(request.url)

        async def sse():
            yield b'data: {"candidates": []}\n\n'

        return httpx.Response(200, content=sse(),
                              headers={"content-type": "text/event-stream"})

    async def capture():
        from google import genai
        from google.genai import types
        client = genai.Client(
            vertexai=True, api_key="K",
            http_options=types.HttpOptions(
                async_client_args={"transport": MockTransport(handler)}))
        try:
            stream = await client.aio.models.generate_content_stream(
                model=MODEL, contents="hi")
            async for _ in stream:
                pass
        except Exception:
            pass
        finally:
            try:
                await client.aio.aclose()
            except AttributeError:
                client.close()

    asyncio.run(capture())
    sdk_url = box.get("url") or ""
    check("SDK 流式 URL 抓取成功", sdk_url != "", sdk_url or "（空）")
    if sdk_url:
        # 逐段对照：scheme://host/version/path:action?query
        from urllib.parse import urlsplit
        s, o = urlsplit(sdk_url), urlsplit(gemini_native._build_upstream_url(MODEL, "streamGenerateContent"))
        check("scheme+host 一致", (s.scheme, s.netloc) == (o.scheme, o.netloc),
              f"sdk={s.scheme}://{s.netloc} ours={o.scheme}://{o.netloc}")
        check("api_version 一致（v1beta1 防漂移核心断言）",
              s.path.split("/")[1] == o.path.split("/")[1],
              f"sdk=/{s.path.split('/')[1]}/ ours=/{o.path.split('/')[1]}/")
        check("模型路径一致（裸名 → publishers/google/models/…）",
              "/".join(s.path.split("/")[2:]) == "/".join(o.path.split("/")[2:]),
              f"sdk={s.path} ours={o.path}")
        check("action 一致（:streamGenerateContent）",
              s.path.endswith(":streamGenerateContent") and o.path.endswith(":streamGenerateContent"),
              f"sdk={s.path} ours={o.path}")


# ============================================================
# 2. 端到端：钉定路径 200 流式（核心修复场景 = 用户报障场景）
# ============================================================
def test_pinned_stream_200():
    print("\n[2] 钉定 global + 流式 streamGenerateContent → 200（用户报障场景）")
    payload = {"contents": [{"role": "user", "parts": [{"text": "你好"}]}]}

    def responder(request):
        # 两个 chunk，顺带验证逐块中继
        return streaming_sse_response(sse_body("你好，"),
                                       sse_body("我是 Gemini。"))

    resp, body, rec = run_passthrough(
        MODEL, "streamGenerateContent", "alt=sse", payload,
        records=[{"key": "AIza-test-key-1", "project_id": PROJECT}],
        responder=responder,
        settings={"express_location": "global"})

    got_url = rec[0]["url"] if rec else ""
    check("上游 URL 无双重前缀（核心断言）",
          got_url == f"{PINNED_URL}:streamGenerateContent?alt=sse", got_url)
    check("状态码 200", resp.status_code == 200, str(resp.status_code))
    check("SSE 字节透传", b"data:" in body, repr(body[:60]))
    # Optional Brotli/Zstandard decoders expand httpx's advertised encodings.
    # Compare the running client's defaults, without making a network request.
    with httpx.Client(trust_env=False) as default_client:
        expected_encoding = default_client.headers["accept-encoding"]
    check("accept-encoding 保持当前 httpx 默认值（与 SDK 一致，不再钉 identity）",
          rec and rec[0]["accept_encoding"] == expected_encoding,
          rec[0]["accept_encoding"] if rec else "无请求")
    check("x-goog-api-key 已转发", rec and rec[0]["api_key"] == "AIza-test-key-1")


# ============================================================
# 3. 端到端：未钉定（裸模型名）
# ============================================================
def test_unpinned_200():
    print("\n[3] 未钉定（express_location 空）→ 裸模型名 URL")
    payload = {"contents": [{"role": "user", "parts": [{"text": "hi"}]}]}

    def responder(request):
        return httpx.Response(200, json={"candidates": [{"content": {"parts": [{"text": "ok"}]}}]})

    resp, body, rec = run_passthrough(
        MODEL, "generateContent", "", payload,
        records=[{"key": "AIza-test-key-1", "project_id": PROJECT}],
        responder=responder,
        settings={"express_location": ""})

    check("裸模型名 URL", rec and rec[0]["url"] == f"{BARE_URL}:generateContent",
          rec[0]["url"] if rec else "无请求")
    check("状态码 200", resp.status_code == 200)
    check("JSON body 透传", b'"ok"' in body)


# ============================================================
# 4. 端到端：钉定失败 404 → 自动兜底裸模型名（新增能力）
# ============================================================
def test_pin_404_fallback():
    print("\n[4] 钉定路径 404 Publisher model not found → 兜底裸模型名 → 200")
    payload = {"contents": [{"role": "user", "parts": [{"text": "hi"}]}]}

    def responder(request):
        url = str(request.url)
        if url.startswith(PINNED_URL):
            return httpx.Response(404, json={"error": {
                "code": 404,
                "message": f"Publisher model {PINNED_PATH} was not found "
                           "for the project.",
                "status": "NOT_FOUND"}})
        return httpx.Response(200, json={"candidates": [{"content": {"parts": [{"text": "fallback-ok"}]}}]})

    resp, body, rec = run_passthrough(
        MODEL, "generateContent", "", payload,
        records=[{"key": "AIza-test-key-1", "project_id": PROJECT}],
        responder=responder,
        settings={"express_location": "global"})

    check("共发起 2 次请求（钉定 + 兜底）", len(rec) == 2,
          f"实际 {len(rec)} 次: {[r['url'][:90] for r in rec]}")
    check("第 1 次为钉定路径", len(rec) >= 1 and rec[0]["url"].startswith(PINNED_URL))
    check("第 2 次退回裸模型名", len(rec) >= 2 and rec[1]["url"] == f"{BARE_URL}:generateContent",
          rec[1]["url"] if len(rec) >= 2 else "")
    check("兜底后 Key 不变（换路径不换 Key）",
          len(rec) == 2 and rec[0]["api_key"] == rec[1]["api_key"])
    check("最终 200", resp.status_code == 200, str(resp.status_code))
    check("返回兜底结果", b"fallback-ok" in body)


# ============================================================
# 5. 端到端：钉定失败 403 requires billing → 兜底
# ============================================================
def test_pin_403_billing_fallback():
    print("\n[5] 钉定路径 403 requires billing → 兜底裸模型名 → 200")
    payload = {"contents": [{"role": "user", "parts": [{"text": "hi"}]}]}

    def responder(request):
        if str(request.url).startswith(PINNED_URL):
            return httpx.Response(403, json={"error": {
                "code": 403,
                "message": "This API method requires billing to be enabled "
                           f"for project #{PROJECT}",
                "status": "PERMISSION_DENIED"}})
        return httpx.Response(200, json={"candidates": [{"content": {"parts": [{"text": "billing-fallback-ok"}]}}]})

    resp, body, rec = run_passthrough(
        MODEL, "generateContent", "", payload,
        records=[{"key": "AIza-test-key-1", "project_id": PROJECT}],
        responder=responder,
        settings={"express_location": "global"})

    check("兜底生效（2 次请求）", len(rec) == 2)
    check("最终 200", resp.status_code == 200)
    check("返回兜底结果", b"billing-fallback-ok" in body)


# ============================================================
# 6. 端到端：404 空 body（非钉定特征）→ 原样透传，不兜底
# ============================================================
def test_404_empty_passthrough():
    print("\n[6] 404 空 body（非钉定特征）→ 原样透传，不兜底")
    payload = {"contents": [{"role": "user", "parts": [{"text": "hi"}]}]}

    def responder(request):
        return httpx.Response(404, content=b"", headers={"content-type": "text/plain"})

    resp, body, rec = run_passthrough(
        MODEL, "generateContent", "", payload,
        records=[{"key": "AIza-test-key-1", "project_id": PROJECT}],
        responder=responder,
        settings={"express_location": "global"})

    check("只发起 1 次请求（不兜底）", len(rec) == 1, f"实际 {len(rec)} 次")
    check("404 原样透传", resp.status_code == 404, str(resp.status_code))


# ============================================================
# 7. 端到端：429 → 换 Key 重试（保留原有行为）
# ============================================================
def test_429_key_rotation():
    print("\n[7] 429 → 换下一个 Express Key → 200（原有行为不回归）")
    payload = {"contents": [{"role": "user", "parts": [{"text": "hi"}]}]}

    def responder(request):
        if request.headers.get("x-goog-api-key") == "AIza-test-key-1":
            return httpx.Response(429, headers={"retry-after": "0.05"},
                                  json={"error": {"code": 429, "message": "Resource exhausted"}})
        return httpx.Response(200, json={"candidates": [{"content": {"parts": [{"text": "key2-ok"}]}}]})

    resp, body, rec = run_passthrough(
        MODEL, "generateContent", "", payload,
        records=[{"key": "AIza-test-key-1", "project_id": PROJECT},
                 {"key": "AIza-test-key-2", "project_id": PROJECT + "x"}],
        responder=responder,
        settings={"express_location": ""})

    check("共发起 2 次请求", len(rec) == 2, f"实际 {len(rec)} 次")
    check("第 1 次用 Key1", len(rec) >= 1 and rec[0]["api_key"] == "AIza-test-key-1")
    check("第 2 次换 Key2", len(rec) >= 2 and rec[1]["api_key"] == "AIza-test-key-2")
    check("最终 200", resp.status_code == 200)
    check("返回 Key2 结果", b"key2-ok" in body)


# ============================================================
# 8. 端到端：客户端自带完整 projects/ 路径（修复前同样双重前缀）
# ============================================================
def test_client_full_path():
    print("\n[8] 客户端自带完整 projects/ 资源路径 → 原样透传")
    payload = {"contents": [{"role": "user", "parts": [{"text": "hi"}]}]}
    full = f"projects/{PROJECT}/locations/us-central1/publishers/google/models/{MODEL}"

    def responder(request):
        return httpx.Response(200, json={"candidates": [{"content": {"parts": [{"text": "fullpath-ok"}]}}]})

    resp, body, rec = run_passthrough(
        full, "generateContent", "", payload,
        records=[{"key": "AIza-test-key-1", "project_id": ""}],
        responder=responder,
        settings={"express_location": "global"})  # 即使设置了钉定，完整路径也应原样

    expected = f"{EXPRESS_HOST}/v1beta1/{full}:generateContent"
    check("URL 原样（无双重前缀）", rec and rec[0]["url"] == expected,
          rec[0]["url"] if rec else "无请求")
    check("状态码 200", resp.status_code == 200)


# ============================================================
# 9. 端到端：无 Project ID → 退回裸模型名（resolve 兜底不变）
# ============================================================
def test_no_project_bare():
    print("\n[9] Key 无 Project ID → 裸模型名（不拼半截路径）")
    payload = {"contents": [{"role": "user", "parts": [{"text": "hi"}]}]}

    def responder(request):
        return httpx.Response(200, json={"candidates": [{"content": {"parts": [{"text": "bare-ok"}]}}]})

    resp, body, rec = run_passthrough(
        MODEL, "generateContent", "", payload,
        records=[{"key": "AIza-test-key-1", "project_id": ""}],
        responder=responder,
        settings={"express_location": "global"})

    check("退回裸模型名 URL", rec and rec[0]["url"] == f"{BARE_URL}:generateContent",
          rec[0]["url"] if rec else "无请求")
    check("状态码 200", resp.status_code == 200)


# ============================================================
# 10. 指纹保真：出站请求与官方 google-genai SDK 逐字节同形态
# ============================================================
# 基准：MockTransport 抓真实 SDK（vertexai=True + api_key）的线序
# （scripts/probe_fingerprint_diff.py 可随时重新验证）
_SDK_WIRE_ORDER = ["Host", "Accept", "Accept-Encoding", "Connection",
                   "Content-Type", "x-goog-api-key", "user-agent",
                   "x-goog-api-client", "Content-Length"]


def test_fingerprint_parity():
    print("\n[10] 指纹保真：线序/头值/body 字节 = 官方 SDK 同形态")
    # 客户端原始字节（含 SDK 风格空白与键序，验证不重组）
    raw_client_body = b'{"contents": [{"parts": [{"text": "hi"}], "role": "user"}]}'

    def responder(request):
        return httpx.Response(200, json={"candidates": []})

    resp, body, rec = run_passthrough(
        MODEL, "generateContent", "", raw_client_body,
        records=[{"key": "AIza-test-key-1", "project_id": ""}],
        responder=responder,
        settings={"express_location": ""})

    check("有出站请求", len(rec) == 1)
    if not rec:
        return
    names = [k for k, _ in rec[0]["raw_headers"]]
    check("线序与官方 SDK 完全一致", names == _SDK_WIRE_ORDER, str(names))
    hdr_map = dict(rec[0]["raw_headers"])
    lower_map = {k.lower(): v for k, v in rec[0]["raw_headers"]}
    check("Content-Type 大小写与 SDK 一致",
          hdr_map.get("Content-Type") == "application/json", str(hdr_map.get("Content-Type")))
    check("user-agent 为官方 SDK 形态",
          lower_map.get("user-agent", "").startswith("google-genai-sdk/"),
          lower_map.get("user-agent", ""))
    check("x-goog-api-client 为官方 SDK 形态",
          lower_map.get("x-goog-api-client", "").startswith("google-genai-sdk/"))
    check("UA 与 x-goog-api-client 同值（SDK 同款）",
          lower_map.get("user-agent") == lower_map.get("x-goog-api-client"))
    dumped = json.dumps(lower_map, ensure_ascii=False)
    check("无 python-httpx / 本项目名泄漏",
          "python-httpx" not in dumped and "vertex2openai" not in dumped.lower())
    check("body 原始字节透传（不解析重组）", rec[0]["body"] == raw_client_body,
          repr(rec[0]["body"][:60]))


# ============================================================
# 11. 指纹保真：gzip 响应透明解压（流式 aiter_bytes）
# ============================================================
def test_gzip_stream_relay():
    print("\n[11] 上游 gzip SSE → 客户端拿到解压后字节")
    import gzip
    payload = {"contents": [{"role": "user", "parts": [{"text": "hi"}]}]}
    plain = sse_body("hello-gzip") + sse_body("done")

    def responder(request):
        # 上游按 accept-encoding: gzip 返回压缩 SSE（aiplatform 支持 gzip）
        return httpx.Response(200, headers={"content-type": "text/event-stream",
                                            "content-encoding": "gzip"},
                              content=gzip.compress(plain))

    resp, body, rec = run_passthrough(
        MODEL, "streamGenerateContent", "alt=sse", payload,
        records=[{"key": "AIza-test-key-1", "project_id": ""}],
        responder=responder,
        settings={"express_location": ""})

    check("gzip 响应已解压（拿到明文 SSE）", b"hello-gzip" in body, repr(body[:80]))
    check("无 gzip 魔数泄漏（无压缩字节直出）",
          b"\x1f\x8b" not in body, repr(body[:20]))


if __name__ == "__main__":
    print("=" * 64)
    print("vertex2openai Gemini 原生透传 回归测试（含指纹保真）")
    print("=" * 64)
    test_build_upstream_url()
    test_url_matches_official_sdk()
    test_pinned_stream_200()
    test_unpinned_200()
    test_pin_404_fallback()
    test_pin_403_billing_fallback()
    test_404_empty_passthrough()
    test_429_key_rotation()
    test_client_full_path()
    test_no_project_bare()
    test_fingerprint_parity()
    test_gzip_stream_relay()
    print("\n" + "=" * 64)
    print(f"结果：{PASS} 通过 / {FAIL} 失败")
    raise SystemExit(1 if FAIL else 0)
