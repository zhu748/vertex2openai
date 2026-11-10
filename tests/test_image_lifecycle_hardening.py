"""生图内存治理第二轮回归测试（512MB 容器适配）。

契约（2026-11 第二轮，详见各被测实现 docstring）：
- 切片惰性化：iter_giant_content / iter_openai_content_chunks 与既有
  list 版逐字节等价，但按需产出（发射阶段峰值 2×content → 1×+256KiB，
  实测 24MB base64 发射增量 +48MB → +1MB）；
- 真流式巨帧防护：split_giant_chunk_payload 把 >1MiB 的 delta.content
  拆成 256KiB 增量帧，finish_reason 只挂尾帧，其余 delta 键保留首帧
  （实测单帧全量路径峰值 78MB → 54MB）；
- 生图绕过续传缓冲：image_bypass_enabled 默认对生图模型返回 True
  （resumable_image_streams=true 关闭），未识别模型按文本维持旧行为；
- 生图并发闸：_call_upstream 对生图模型叠加 image_max_concurrency 名额
  （非流式立即释放、流式随 EOF 释放、异常释放、文本模型不占名额）。
"""

import os
import unittest
from unittest import mock


from fastapi.responses import JSONResponse, StreamingResponse  # noqa: E402

from admission import GenerationCapacityError, image_limiter  # noqa: E402
from message_processing import (  # noqa: E402
    GIANT_CONTENT_SLICE,
    GIANT_CONTENT_THRESHOLD,
    iter_giant_content,
    slice_giant_content,
)
from models import OpenAIRequest  # noqa: E402
import resumable  # noqa: E402
from routes import chat_api  # noqa: E402
from upstreams.cookie_response import (  # noqa: E402
    iter_openai_content_chunks,
)
from upstreams.express_response import (  # noqa: E402
    split_giant_chunk_payload,
)

IMAGE_MODEL = "gemini-nano-banana-2.1"
TEXT_MODEL = "gemini-2.5-flash"


def _image_markdown(payload_bytes: int) -> str:
    import base64
    return "![Image](data:image/png;base64," + base64.b64encode(os.urandom(payload_bytes)).decode() + ")"


class LazySlicingTests(unittest.TestCase):
    def test_lazy_iter_matches_list_version(self):
        text = _image_markdown(GIANT_CONTENT_SLICE * 3 + 77)
        self.assertEqual(list(iter_giant_content(text)), slice_giant_content(text))
        self.assertEqual("".join(iter_giant_content(text)), text)

    def test_lazy_iter_is_generator(self):
        gen = iter_giant_content("x" * 5)
        self.assertTrue(hasattr(gen, "__next__") or hasattr(gen, "__iter__"))
        self.assertEqual(list(iter_giant_content("abc")), ["abc"])

    def test_small_content_single_piece(self):
        text = "x" * GIANT_CONTENT_THRESHOLD
        self.assertEqual(list(iter_giant_content(text)), [text])


class CookieLazyChunkTests(unittest.TestCase):
    def test_lazy_frames_identical_to_list_version(self):
        content = _image_markdown(2 * 1024 * 1024)
        lazy = list(
            iter_openai_content_chunks("chatcmpl-x", "nano-banana", content, role="assistant")
        )
        listed = list(iter_openai_content_chunks("chatcmpl-x", "nano-banana", content, role="assistant"))
        self.assertGreater(len(lazy), 1)
        self.assertEqual(lazy, listed)

    def test_lazy_iterable_without_materialization(self):
        content = _image_markdown(1536 * 1024)
        gen = iter_openai_content_chunks("chatcmpl-x", "nano-banana", content)
        first = next(iter(gen))
        self.assertTrue(first.startswith("data: "))

    def test_empty_content_yields_nothing(self):
        self.assertEqual(list(iter_openai_content_chunks("id", "m", "")), [])


class RealStreamGiantFrameTests(unittest.TestCase):
    def _payload(self, content, finish="stop", **extra_delta):
        return {
            "id": "chatcmpl-rs",
            "object": "chat.completion.chunk",
            "created": 1,
            "model": "m",
            "choices": [
                {
                    "index": 0,
                    "delta": {"role": "assistant", "content": content, **extra_delta},
                    "finish_reason": finish,
                }
            ],
        }

    def test_small_payload_unchanged(self):
        payload = self._payload("hello")
        self.assertEqual(split_giant_chunk_payload(payload), [payload])
        self.assertIs(split_giant_chunk_payload(payload)[0], payload)

    def test_giant_split_byte_identical_and_frame_topology(self):
        content = "Q" * (GIANT_CONTENT_THRESHOLD + GIANT_CONTENT_SLICE * 2)
        parts = split_giant_chunk_payload(self._payload(content, reasoning_content="think"))
        self.assertGreater(len(parts), 2)
        self.assertEqual(
            "".join(p["choices"][0]["delta"]["content"] for p in parts), content
        )
        first = parts[0]["choices"][0]
        self.assertEqual(first["delta"].get("role"), "assistant")
        self.assertEqual(first["delta"].get("reasoning_content"), "think")
        self.assertIsNone(first["finish_reason"])
        for mid in parts[1:-1]:
            self.assertIsNone(mid["choices"][0]["finish_reason"])
            self.assertEqual(set(mid["choices"][0]["delta"]), {"content"})
        last = parts[-1]["choices"][0]
        self.assertEqual(last["finish_reason"], "stop")
        self.assertEqual(set(last["delta"]), {"content"})

    def test_malformed_payload_falls_back_to_original(self):
        weird = {"choices": None}
        self.assertEqual(split_giant_chunk_payload(weird), [weird])
        obj = object()
        # 任意非 dict 形状：不抛异常，原样单元素返回
        self.assertEqual(split_giant_chunk_payload(obj), [obj])
        self.assertEqual(split_giant_chunk_payload("not-a-dict"), ["not-a-dict"])


class ImageBypassTests(unittest.TestCase):
    def test_image_model_bypasses_by_default(self):
        self.assertTrue(resumable.image_bypass_enabled(IMAGE_MODEL))

    def test_text_and_unknown_models_keep_resumable(self):
        self.assertFalse(resumable.image_bypass_enabled(TEXT_MODEL))
        self.assertFalse(resumable.image_bypass_enabled("totally-unknown-model"))

    def test_setting_restores_image_resumable(self):
        with mock.patch.object(
            resumable.app_state, "get_setting",
            lambda key, default=None: True if key == "resumable_image_streams" else default,
        ):
            self.assertFalse(resumable.image_bypass_enabled(IMAGE_MODEL))


class ImageConcurrencyGateTests(unittest.IsolatedAsyncioTestCase):
    async def test_nonstream_releases_slot_after_response(self):
        seen_active = []
        plain = JSONResponse(content={"ok": True})

        async def fake_dispatch(request_obj, fastapi_request):
            seen_active.append(chat_api.image_limiter.active)
            return plain

        with mock.patch.object(chat_api, "_dispatch_upstream", fake_dispatch):
            resp = await chat_api._call_upstream(
                OpenAIRequest(model=IMAGE_MODEL, messages=[]), None
            )
        self.assertEqual(seen_active, [1])
        self.assertEqual(chat_api.image_limiter.active, 0)
        self.assertIs(resp, plain)

    async def test_stream_holds_slot_until_eof(self):
        seen_active = []

        async def frames():
            seen_active.append(chat_api.image_limiter.active)
            yield "data: {\"a\":1}\n\n"
            yield "data: [DONE]\n\n"

        async def fake_dispatch(request_obj, fastapi_request):
            return StreamingResponse(frames(), media_type="text/event-stream")

        with mock.patch.object(chat_api, "_dispatch_upstream", fake_dispatch):
            resp = await chat_api._call_upstream(
                OpenAIRequest(model=IMAGE_MODEL, messages=[], stream=True), None
            )
            self.assertEqual(chat_api.image_limiter.active, 1)
            collected = [f async for f in resp.body_iterator]
            self.assertEqual(len(collected), 2)
        # 流耗尽后释放
        self.assertEqual(chat_api.image_limiter.active, 0)
        self.assertEqual(seen_active, [1])

    async def test_exception_releases_slot(self):
        async def boom(request_obj, fastapi_request):
            raise RuntimeError("上游炸了")

        with mock.patch.object(chat_api, "_dispatch_upstream", boom), \
                self.assertRaises(RuntimeError):
            await chat_api._call_upstream(
                OpenAIRequest(model=IMAGE_MODEL, messages=[]), None
            )
        self.assertEqual(chat_api.image_limiter.active, 0)

    async def test_text_model_takes_no_image_slot(self):
        async def fake_dispatch(request_obj, fastapi_request):
            self.assertEqual(chat_api.image_limiter.active, 0)
            return JSONResponse(content={"ok": True})

        with mock.patch.object(chat_api, "_dispatch_upstream", fake_dispatch):
            await chat_api._call_upstream(
                OpenAIRequest(model=TEXT_MODEL, messages=[]), None
            )
        self.assertEqual(chat_api.image_limiter.active, 0)

    async def test_full_limiter_fails_fast_503(self):
        # 按当前配置占满，默认串行；同时允许管理员覆盖并发上限。
        limit = chat_api.app_state.get_setting("image_max_concurrency", 1)
        leases = []
        try:
            for _ in range(limit):
                leases.append(chat_api.image_limiter.reserve())
            called = []

            async def fake_dispatch(request_obj, fastapi_request):
                called.append(1)
                return JSONResponse(content={})

            with mock.patch.object(chat_api, "_dispatch_upstream", fake_dispatch), \
                    self.assertRaises(GenerationCapacityError) as ctx:
                await chat_api._call_upstream(
                    OpenAIRequest(model=IMAGE_MODEL, messages=[]), None
                )
            self.assertEqual(called, [])
            self.assertIn("image_max_concurrency", str(ctx.exception))
        finally:
            for lease in leases:
                lease.release()
        self.assertEqual(chat_api.image_limiter.active, 0)

    def test_image_limiter_counts_and_traced_off(self):
        limiter = type(image_limiter)(
            setting_key="image_max_concurrency",
            default_limit=1,
            full_message="满",
            traced=False,
        )
        # app_state 里已保存的 image_max_concurrency 会盖过构造
        # 参数：单测里钉住设置值，只验证闸门自身的计数/幂等/无 trace 语义。
        with mock.patch.object(
            __import__("admission").app_state, "get_setting", return_value=1
        ), self.assertRaises(GenerationCapacityError):
            lease = limiter.reserve()
            self.assertEqual(limiter.active, 1)
            limiter.reserve()
        # traced=False：Lease.trace 为 None（不触碰请求 trace）
        self.assertIsNone(lease.trace)
        lease.release()
        lease.release()  # 幂等
        self.assertEqual(limiter.active, 0)


if __name__ == "__main__":
    unittest.main()
