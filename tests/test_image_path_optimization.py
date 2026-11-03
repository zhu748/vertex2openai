"""生图链路全面优化回归（2026-11-03，不限于 4K）。

覆盖六项契约：
1. 模型名分辨率别名（-1k/-2k/-4k/-512）端到端生效：档位转成
   imageConfig.imageSize（Express 与 Cookie 两通道），且从发往上游的
   模型名里剥离（目录无此 ID，原样发送 404）；
2. OpenAI images 风格 size 的宽高比映射补全（1536x1024→3:2 等）与
   无歧义档位推断（≥2048→2K/4K、≤640→512、1792 保持默认）；
3. 输入图透明抹平按模式与实际透明度分流：全不透明/全透明/无透明
   调色板免合成，真半透明仍白底合成且颜色一致；
4. Express 假流式巨帧切片零步进（sleep(0)），文本节奏 0.01 保留；
   帧拼接逐字节一致；
5. Cookie 生图整取在 JSON 解析开始前释放 httpx 响应对象（bytes
   缓冲不再与 .text 双驻留）；
6. 别名剥离只对生图档案生效：文本模型名原样保留。
"""

import asyncio
import io
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

_STATE = tempfile.TemporaryDirectory(prefix="v2o-imgopt-test-")
os.environ.setdefault("STATE_DIR", _STATE.name)
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from PIL import Image  # noqa: E402

import model_capabilities as mc  # noqa: E402
import image_processing  # noqa: E402
from models import OpenAIRequest  # noqa: E402
from generation_config import create_generation_config  # noqa: E402
from upstreams.express_sdk import _normalize_model_name  # noqa: E402
from upstreams.express_response import (  # noqa: E402
    _chunk_openai_response_dict_for_sse,
)


def _request(model, **extra):
    req = OpenAIRequest(model=model, messages=[{"role": "user", "content": "画一张图"}])
    for key, value in extra.items():
        setattr(req, key, value)
    return req


def _settings(**kwargs):
    base = {
        "img_compress_enabled": True,
        "img_compress_max_mb": 0.0,      # 强制进入压缩路径
        "img_compress_max_dim": 1024,
        "img_compress_quality": 90,
    }
    base.update(kwargs)
    return base


class ResolutionSuffixTests(unittest.TestCase):
    """契约 1/6：-1k/-2k/-4k/-512 别名解析与上游名剥离。"""

    def test_suffix_parsed_and_stripped_for_image_models(self):
        for name, size in (
            ("gemini-3.1-flash-image-4k", "4K"),
            ("gemini-3.1-flash-image-2K", "2K"),
            ("gemini-nano-banana-2.1-1k", "1K"),
            ("gemini-3.1-flash-image-512", "512"),
            ("gemini-3-pro-image-4k", "4K"),
        ):
            with self.subTest(name=name):
                base, hint = mc.image_resolution_suffix(name)
                self.assertEqual(base, name[: -(len(size) + 1)])
                self.assertEqual(hint, size)

    def test_text_models_and_bare_suffixes_are_untouched(self):
        # 文本模型名（即使以 -4k 结尾的未来命名）与纯后缀不剥离。
        for name in ("gemini-3-pro", "gemini-3-flash-4k-ish", "-4k", "", "gemini-3-pro-preview"):
            with self.subTest(name=name):
                base, hint = mc.image_resolution_suffix(name)
                self.assertEqual((base, hint), (name, None))

    def test_express_normalize_strips_suffix_only_for_image_models(self):
        base, search, err = _normalize_model_name("gemini-3.1-flash-image-4k")
        self.assertEqual((base, search, err), ("gemini-3.1-flash-image", False, None))
        base, _, _ = _normalize_model_name("gemini-3-pro")
        self.assertEqual(base, "gemini-3-pro")
        # 与 -search 组合时两个后缀都剥。
        base, search, _ = _normalize_model_name("gemini-3.1-flash-image-4k-search")
        self.assertEqual((base, search), ("gemini-3.1-flash-image", True))

    def test_resolve_image_size_priority_chain(self):
        # 请求字段 > 模型名后缀 > size 参数 > 控制台 > 1K。
        self.assertEqual(
            mc.resolve_image_size("gemini-3.1-flash-image-2k", _request("m", image_size="4K"), {}),
            "4K",
        )
        self.assertEqual(
            mc.resolve_image_size("gemini-3.1-flash-image-2k", _request("m"), {}), "2K"
        )
        self.assertEqual(
            mc.resolve_image_size("gemini-3.1-flash-image", _request("m", size="2048x2048"), {}),
            "2K",
        )
        self.assertEqual(
            mc.resolve_image_size("gemini-3.1-flash-image", _request("m"), {"image_size": "2K"}),
            "2K",
        )
        self.assertEqual(mc.resolve_image_size("gemini-3.1-flash-image", _request("m"), {}), "1K")
        # 请求字段优先于后缀；后缀优先于 size。
        self.assertEqual(
            mc.resolve_image_size(
                "gemini-3.1-flash-image-1k", _request("m", image_size="2K", size="4K-ish"), {}
            ),
            "2K",
        )

    def test_suffix_falls_back_along_the_ladder(self):
        # pro-image 不支持 512：-512 就近向下回落 1K；flash-image 支持 512。
        self.assertEqual(
            mc.resolve_image_size("gemini-3-pro-image-512", _request("m"), {}), "1K"
        )
        self.assertEqual(
            mc.resolve_image_size("gemini-3.1-flash-image-512", _request("m"), {}), "512"
        )

    def test_express_generation_config_uses_suffix_size(self):
        config = create_generation_config(_request("gemini-3.1-flash-image-4k"))
        self.assertEqual(config["response_modalities"], ["TEXT", "IMAGE"])
        self.assertEqual(config["image_config"].image_size, "4K")

    def test_cookie_request_body_uses_suffix_size_and_clean_model(self):
        from upstreams.cookie_request import build_request_body

        body = build_request_body(
            "proj-1", "gemini-3.1-flash-image-2k", _request("gemini-3.1-flash-image-2k")
        )
        variables = body["variables"]
        self.assertTrue(
            variables["model"].endswith("models/gemini-3.1-flash-image"),
            f"上游模型路径未剥离后缀：{variables['model']}",
        )
        self.assertNotIn("-2k", variables["model"])
        self.assertEqual(variables["generationConfig"]["imageConfig"]["imageSize"], "2K")


class SizeParamMappingTests(unittest.TestCase):
    """契约 2/6：OpenAI images 风格 size 的 AR/档位映射。"""

    def test_dalle3_and_gpt_image_sizes_map_to_aspect_ratios(self):
        cases = {
            "1024x1024": "1:1",
            "1024x768": "4:3",
            "768x1024": "3:4",
            "1536x1024": "3:2",
            "1024x1536": "2:3",
            "1792x1024": "16:9",
            "1024x1792": "9:16",
        }
        for size, ar in cases.items():
            with self.subTest(size=size):
                self.assertEqual(
                    mc.resolve_aspect_ratio("gemini-3.1-flash-image", _request("m", size=size), {}),
                    ar,
                )

    def test_size_to_size_hint_only_for_unambiguous_tiers(self):
        cases = {
            "2048x2048": "2K",
            "2048x1536": "2K",
            "4096x4096": "4K",
            "512x512": "512",
            "256x256": "512",
            "1024x1024": None,     # 保持默认 1K，不放大成本
            "1536x1024": None,
            "1792x1024": None,
            "auto": None,
            "bogus": None,
        }
        for size, tier in cases.items():
            with self.subTest(size=size):
                self.assertEqual(mc._size_param_hint(_request("m", size=size)), tier)

    def test_explicit_aspect_ratio_field_beats_size(self):
        req = _request("m", aspect_ratio="1:1", size="1792x1024")
        self.assertEqual(
            mc.resolve_aspect_ratio("gemini-3.1-flash-image", req, {}), "1:1"
        )

    def test_uppercase_size_is_normalized(self):
        req = _request("m", size="1536X1024")
        self.assertEqual(
            mc.resolve_aspect_ratio("gemini-3.1-flash-image", req, {}), "3:2"
        )


class FlattenTransparencyTests(unittest.TestCase):
    """契约 3/6：透明抹平分流（内存优化）与颜色一致性。"""

    def _compress(self, img, settings=None):
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        settings = settings or _settings()
        with mock.patch.object(
            image_processing.app_state,
            "get_setting",
            side_effect=lambda key, default=None: settings.get(key, default),
        ):
            return image_processing.optimize_image_bytes(buf.getvalue(), "image/png")

    def _open(self, data):
        return Image.open(io.BytesIO(data))

    def test_fully_opaque_rgba_skips_composite_and_keeps_pixels(self):
        img = Image.new("RGBA", (600, 500), (200, 30, 90, 255))
        out, mime = self._compress(img)
        self.assertEqual(mime, "image/jpeg")
        pixel = self._open(out).convert("RGB").getpixel((300, 250))
        self.assertTrue(all(abs(a - b) <= 12 for a, b in zip(pixel, (200, 30, 90))))

    def test_semi_transparent_rgba_flattens_onto_white(self):
        img = Image.new("RGBA", (600, 500), (0, 0, 0, 0))
        for x in range(300):
            for y in range(500):
                img.putpixel((x, y), (0, 0, 0, 128))
        out, _ = self._compress(img)
        left = self._open(out).convert("L").getpixel((150, 250))
        right = self._open(out).convert("L").getpixel((450, 250))
        # 黑、alpha=128 落在白底上 ≈ 255×(1-0.5) ≈ 127 中灰；透明区域近白。
        self.assertTrue(90 < left < 165, f"左半应为中灰，实际 {left}")
        self.assertGreater(right, 215)

    def test_fully_transparent_rgba_becomes_white(self):
        img = Image.new("RGBA", (300, 300), (0, 0, 0, 0))
        out, mime = self._compress(img)
        self.assertEqual(mime, "image/jpeg")
        pixel = self._open(out).convert("RGB").getpixel((150, 150))
        self.assertTrue(all(ch > 240 for ch in pixel))

    def test_palette_without_transparency_converts_directly(self):
        img = Image.new("P", (300, 300))
        img.putpalette([10, 20, 30] * 256)
        out, mime = self._compress(img)
        self.assertEqual(mime, "image/jpeg")
        pixel = self._open(out).convert("RGB").getpixel((150, 150))
        self.assertTrue(all(abs(a - b) <= 12 for a, b in zip(pixel, (10, 20, 30))))

    def test_palette_with_transparency_flattens_onto_white(self):
        img = Image.new("P", (300, 300))
        img.putpalette([0, 0, 0, 255, 255, 255] + [0] * 762)
        img.info["transparency"] = 1
        img.paste(0, (0, 0, 300, 300))          # 全图索引 0（黑，不透明）
        out, _ = self._compress(img)
        pixel = self._open(out).convert("L").getpixel((150, 150))
        self.assertLess(pixel, 40)

    def test_la_mode_flattens_onto_white(self):
        img = Image.new("LA", (300, 300), (0, 128))
        out, _ = self._compress(img)
        pixel = self._open(out).convert("L").getpixel((150, 150))
        # 灰 0、alpha=128 → 白底 ≈ 127 中灰偏深。
        self.assertTrue(90 < pixel < 165, f"LA 半透明应为中灰，实际 {pixel}")


class GiantFramePacingTests(unittest.TestCase):
    """契约 4/6：巨帧切片零步进；拼接逐字节一致；finish 只挂尾帧。"""

    GIANT = (1 << 20) + (1 << 18) * 5   # ~2.25MiB → 10 帧

    def _frames(self, content):
        response_dict = {
            "id": "chatcmpl-x",
            "model": "gemini-3.1-flash-image",
            "created": 1,
            "choices": [
                {"index": 0, "message": {"role": "assistant", "content": content},
                 "finish_reason": "stop"}
            ],
        }

        async def run():
            out = []
            started = time.monotonic()
            async for frame in _chunk_openai_response_dict_for_sse(response_dict):
                out.append(frame)
            return out, time.monotonic() - started

        return asyncio.run(run())

    def test_giant_slices_are_emitted_without_pacing(self):
        content = "![Image](data:image/png;base64," + "A" * self.GIANT + ")"
        frames, elapsed = self._frames(content)
        # 旧实现 10 帧 × sleep(0.01) ≥ 100ms；零步进后远低于该值
        #（真实序列化耗时 ~10ms 量级，阈值取 80ms 防抖动）。
        self.assertGreaterEqual(len(frames), 9)
        self.assertLess(elapsed, 0.08, f"巨帧发射仍带步进：{elapsed*1000:.0f}ms")

        import json as _json
        contents, finishes = [], []
        for frame in frames:
            if frame.strip() == "data: [DONE]":
                continue
            payload = _json.loads(frame.removeprefix("data: ").strip())
            choice = payload["choices"][0]
            contents.append(choice["delta"].get("content", ""))
            finishes.append(choice["finish_reason"])
        self.assertEqual("".join(contents), content)
        self.assertEqual(finishes[-1], "stop")
        self.assertTrue(all(f is None for f in finishes[:-1]))

    def test_small_text_keeps_fake_stream_pacing(self):
        content = "普通文本" * 40      # 走 10 段文本节奏分支
        frames, elapsed = self._frames(content)
        self.assertGreater(len(frames), 10)
        self.assertGreaterEqual(elapsed, 0.05)   # 10 帧以上 × sleep(0.01) 仍在


class CookieCollectReleaseTests(unittest.TestCase):
    """契约 5/6：生图整取在解析前释放 httpx 响应对象。"""

    def test_response_buffer_released_before_parsing(self):
        import upstreams.cookie_proxy as cp

        seen_refcounts = []

        real_iter = cp.iter_json_objects

        async def spying_iter(text_source):
            seen_refcounts.append(sys.getrefcount(self._fake_response))
            async for obj in real_iter(text_source):
                yield obj

        image_b64 = "QkFTRTY0" * 64
        body = {
            "results": [{
                "data": {
                    "candidates": [{
                        "content": {"parts": [
                            {"inlineData": {"mimeType": "image/png", "data": image_b64}},
                        ]},
                        "finishReason": "STOP",
                    }]
                }
            }]
        }

        class FakeResponse:
            status_code = 200
            text = cp.json.dumps(body)
            headers = {}

            async def _unused(self):
                return None

        fake_response = FakeResponse()
        self._fake_response = fake_response

        class FakeClient:
            async def post(self, *args, **kwargs):
                return fake_response

        class FakeCM:
            async def __aenter__(self):
                return FakeClient()

            async def __aexit__(self, *exc):
                return False

        fastapi_request = mock.Mock()
        fastapi_request.is_disconnected = mock.AsyncMock(return_value=False)

        async def run():
            return await cp._collect_full_response(
                "proj-1",
                "gemini-3.1-flash-image",
                mock.Mock(),
                "cookie-string",
                {},
                retry_max=0,
                backoff_sec=0,
                jitter_sec=0,
                fastapi_request=fastapi_request,
            )

        async def fake_timed_call(model, operation, *, whole_call=False, seconds=None):
            return await operation

        with mock.patch.object(cp, "open_upstream_client", return_value=FakeCM()), \
             mock.patch.object(cp, "timed_call", side_effect=fake_timed_call), \
             mock.patch.object(cp, "build_headers", return_value={"auth": "1"}), \
             mock.patch.object(cp, "build_request_body_async",
                               mock.AsyncMock(return_value={"contents": []})), \
             mock.patch.object(cp, "iter_json_objects", spying_iter), \
             mock.patch.object(cp, "stats"):
            result = asyncio.run(run())

        self.assertEqual(result["kind"], "ok", str(result))
        self.assertIn("data:image/png;base64,", result["full_text"])
        # 解析开始时（iter_json_objects 被调用）：本地 response 已置 None。
        # 此刻 fake_response 的引用 = 测试局部变量 + self._fake_response +
        # getrefcount 的临时引用 = 3；若 _collect_full_response 仍持有
        # response 局部变量则为 4。
        self.assertEqual(seen_refcounts, [3], "解析期间 httpx 响应对象未被释放")


if __name__ == "__main__":
    unittest.main(verbosity=2)
