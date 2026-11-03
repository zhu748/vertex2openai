"""生图链路全面优化回归（2026-11-03，不限于 4K）。

覆盖契约（第五轮新增 8-13）：
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
6. 别名剥离只对生图档案生效：文本模型名原样保留；
7. 分辨率后缀目录可发现性：三套模型目录（OpenAI / Anthropic /
   Gemini 原生）同源列出 -1k/-2k/-4k/-512 变体，Gemini 原生透传
   剥后缀并注入 imageConfig.imageSize（与 -search 变体同模式）；
8. 拉取式解析游标：next_object() 返回后不持有对象引用，配合消费方
   del obj 让解析出的巨型 dict（含整份 base64）在事件消费前释放；
9. 诊断采样器正文即停留存：BODY_EVENT_TYPES 事件到达后丢弃已存样本、
   停止留存但保留计数；无正文路径（含只有思考）样本照常保留；
10. 非流式整取（collect_response）与生图假流式同构：解析前释放
    httpx 响应对象；图片事件经片段表 + 一次 join 拼接；
11. 正文收集用「片段表 + join」替代逐事件 +=（导语+图片+尾声拼接
    逐字节一致）；
12. -search 与分辨率后缀叠加时两种次序都全生效（-4k-search 与
    -search-4k 均解析出档位、剥净 -search、搜索标记成立）；
13. 非流式完整正文的单对象上限按实际正文长度放行（64MB 上限不误拒
    4K 高细节 PNG）；流式解析器上限语义不变。
"""

import asyncio
import io
import json
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
    """契约 5/6/8/11：生图整取的响应释放、游标化解析与拼接一致性。"""

    def test_response_buffer_released_before_parsing(self):
        import upstreams.cookie_proxy as cp

        seen_refcounts = []

        class SpyingCursor(cp.CompleteTextObjectCursor):
            # 解析入口（游标构造）时记录 fake_response 的引用计数。
            def __init__(cursor_self, text, limit=None):
                seen_refcounts.append(sys.getrefcount(self._fake_response))
                super().__init__(text)

        image_b64 = "QkFTRTY0" * 64
        body = {
            "results": [{
                "data": {
                    "candidates": [{
                        "content": {"parts": [
                            {"text": "导语"},
                            {"inlineData": {"mimeType": "image/png", "data": image_b64}},
                            {"text": "尾声"},
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
             mock.patch.object(cp, "stats"), \
             mock.patch.object(cp, "CompleteTextObjectCursor", SpyingCursor):
            result = asyncio.run(run())

        expected_md = f"![Generated Image](data:image/png;base64,{image_b64})"
        self.assertEqual(result["kind"], "ok", str(result))
        # 契约 11（片段表 + 一次 join）：导语 + 图片 + 尾声拼接逐字节一致。
        self.assertEqual(result["full_text"], "导语" + expected_md + "尾声")
        # 解析开始时（游标构造）：本地 response 已置 None。
        # 此刻 fake_response 的引用 = 测试局部变量 + self._fake_response +
        # getrefcount 的临时引用 = 3；若 _collect_full_response 仍持有
        # response 局部变量则为 4。
        self.assertEqual(seen_refcounts, [3], "解析期间 httpx 响应对象未被释放")


class CatalogDiscoverabilityTests(unittest.TestCase):
    """契约 7/7：分辨率后缀目录可发现性与 Gemini 原生透传改写。"""

    def test_variant_ids_follow_profile_whitelist(self):
        # 目录变体单一事实源：flash 4 档全列、pro/nano 3 档、
        # 单档 lite 不列、文本模型不列。
        self.assertEqual(
            mc.image_size_variant_ids("gemini-3.1-flash-image"),
            ["-512", "-1k", "-2k", "-4k"],
        )
        self.assertEqual(
            mc.image_size_variant_ids("gemini-nano-banana-2.1"),
            ["-1k", "-2k", "-4k"],
        )
        self.assertEqual(
            mc.image_size_variant_ids("gemini-3-pro-image"),
            ["-1k", "-2k", "-4k"],
        )
        # 单档模型（flash-lite 生图档案仅 1K）与文本模型不列。
        # 注：lite 分支的判据是 "flash-lite" 子串，命中形态如
        # gemini-3.1-flash-lite-image（flash-image-lite 中间隔了 image，
        # 归 flash 档案、四档全列）。
        self.assertEqual(mc.image_size_variant_ids("gemini-3.1-flash-lite-image"), [])
        self.assertEqual(mc.image_size_variant_ids("gemini-3.8-flash"), [])

    def test_variant_ids_match_resolution_suffix_parser(self):
        # 目录列出的每个变体都必须能被请求侧 image_resolution_suffix 解析
        #（档位一致）——否则会出现“目录里能选、选了却不生效”的漂移。
        for base in ("gemini-3.1-flash-image", "gemini-3-pro-image",
                     "gemini-nano-banana-2.1"):
            for suffix in mc.image_size_variant_ids(base):
                with self.subTest(name=base + suffix):
                    stripped, tier = mc.image_resolution_suffix(base + suffix)
                    self.assertEqual(stripped, base)
                    self.assertEqual(tier, suffix[1:].upper())

    def test_native_strip_injects_size_and_creates_missing_config(self):
        from routes.gemini_native import _strip_image_size_variant

        parsed = {"contents": [{"role": "user"}]}
        base, body = _strip_image_size_variant(
            "gemini-3.1-flash-image-4k", "generateContent", parsed)
        self.assertEqual(base, "gemini-3.1-flash-image")
        rebuilt = json.loads(body)
        self.assertEqual(
            rebuilt["generationConfig"]["imageConfig"]["imageSize"], "4K")
        self.assertIn("contents", rebuilt)  # 原字段保留

    def test_native_strip_respects_explicit_request_size(self):
        from routes.gemini_native import _strip_image_size_variant

        parsed = {"generationConfig": {"imageConfig": {"imageSize": "1K"}}}
        base, body = _strip_image_size_variant(
            "gemini-3.1-flash-image-4k", "generateContent", parsed)
        self.assertEqual((base, body), ("gemini-3.1-flash-image", None))
        # 非法 imageConfig 结构：只剥后缀，交给上游报真实错误。
        parsed = {"generationConfig": {"imageConfig": "bogus"}}
        base, body = _strip_image_size_variant(
            "gemini-3.1-flash-image-4k", "generateContent", parsed)
        self.assertEqual((base, body), ("gemini-3.1-flash-image", None))

    def test_native_strip_count_tokens_and_text_models(self):
        from routes.gemini_native import _strip_image_size_variant

        # countTokens：只剥后缀（body 与模型无关）。
        base, body = _strip_image_size_variant(
            "gemini-3.1-flash-image-2k", "countTokens", {"contents": []})
        self.assertEqual((base, body), ("gemini-3.1-flash-image", None))
        # 文本模型名（即使以 -4k 结尾）：完全不动，原样透传。
        base, body = _strip_image_size_variant(
            "gemini-3.8-flash-4k", "generateContent", {"contents": []})
        self.assertEqual((base, body), ("gemini-3.8-flash-4k", None))

    def test_native_model_entry_notes_tier(self):
        from routes.gemini_native import _native_model_entry

        entry = _native_model_entry("gemini-3.1-flash-image-4k")
        self.assertEqual(entry["name"], "models/gemini-3.1-flash-image-4k")
        self.assertIn("imageSize=4K", entry["description"])
        plain = _native_model_entry("gemini-3.1-flash-image")
        self.assertNotIn("imageSize", plain["description"])


class CombinedSuffixOrderTests(unittest.TestCase):
    """契约 12：-search 与分辨率后缀叠加时两种次序都全生效（第五轮）。

    目录从不叠加两种变体，但手打名 "model-search-4k"（档位剥离后残留
    -search 发上游 404）与 "model-4k-search"（档位提示不可见，静默回落
    1K）都必须与目录变体语义一致。"""

    def test_resolution_suffix_parses_both_orders(self):
        self.assertEqual(
            mc.image_resolution_suffix("gemini-3.1-flash-image-4k-search"),
            ("gemini-3.1-flash-image", "4K"))
        name, tier = mc.image_resolution_suffix("gemini-3.1-flash-image-search-4k")
        self.assertEqual((name, tier), ("gemini-3.1-flash-image-search", "4K"))

    def test_strip_loop_removes_both_orders(self):
        for combined, want_search in (
            ("gemini-3.1-flash-image-4k-search", True),
            ("gemini-3.1-flash-image-search-4k", True),
            ("gemini-3.1-flash-image-4k", False),
        ):
            with self.subTest(name=combined):
                base, is_search = mc.strip_image_and_search_suffixes(combined)
                self.assertEqual(
                    (base, is_search), ("gemini-3.1-flash-image", want_search))
        # 文本模型的 -search 别名行为不变
        self.assertEqual(
            mc.strip_image_and_search_suffixes("gemini-3.8-flash-search"),
            ("gemini-3.8-flash", True))
        # 文本模型名（即使以 -4k 结尾）不剥离档位后缀，原样透传
        self.assertEqual(
            mc.strip_image_and_search_suffixes("gemini-3.8-flash-4k"),
            ("gemini-3.8-flash-4k", False))

    def test_express_normalize_both_orders(self):
        for combined in ("gemini-3.1-flash-image-4k-search",
                         "gemini-3.1-flash-image-search-4k"):
            with self.subTest(name=combined):
                base, is_search, err = _normalize_model_name(combined)
                self.assertEqual((base, is_search, err),
                                 ("gemini-3.1-flash-image", True, None))

    def test_resolve_image_size_reads_tier_in_both_orders(self):
        # 通道已把上游模型名剥成基础名：档位从请求原始模型名兜底解析，
        # 两种次序都要能读到（否则 -search 叠加时静默回落 1K）。
        for combined, tier in (
            ("gemini-3.1-flash-image-4k-search", "4K"),
            ("gemini-3.1-flash-image-search-4k", "4K"),
            ("gemini-3.1-flash-image-2k", "2K"),
        ):
            with self.subTest(name=combined):
                size = mc.resolve_image_size(
                    "gemini-3.1-flash-image", _request(combined), {})
                self.assertEqual(size, tier)


class CompleteTextCursorLimitTests(unittest.TestCase):
    """契约 13：非流式整取的单对象上限按实际正文长度放行；流式上限不变。"""

    def test_complete_text_lifts_per_object_cap_to_text_size(self):
        # 4K 高细节 PNG 的 base64 可达 53-93MB，超过 64MB 流式上限；非流式
        # 正文早已整体驻留，上限在此不设防任何东西，只能误拒合法响应。
        from upstreams.cookie_response import CompleteTextObjectCursor

        text = '{"data": "' + "A" * 64 + '"}   [tail]'
        cursor = CompleteTextObjectCursor(text, limit=8)  # 上限远小于对象
        self.assertEqual(cursor.next_object(), {"data": "A" * 64})
        self.assertIsNone(cursor.next_object())

    def test_streaming_parser_keeps_per_object_cap(self):
        # 流式解析器的上限语义不变：仍防解析缓冲无界增长。
        from json_stream import JsonObjects
        from response_validation import IncompleteResponseError

        parser = JsonObjects(limit=8)
        with self.assertRaises(IncompleteResponseError):
            list(parser.feed('{"data": "' + "A" * 32 + '"}'))


def _cookie_collect_harness(body_builder, patches_extra=()):
    """生图整取离线桩：返回 (run, patches, fake_response)，与释放测试同构。

    body_builder 返回 dict（单对象响应）或 list（多对象响应，逐个 dumps 拼接）。"""
    import upstreams.cookie_proxy as cp

    body = body_builder()
    if isinstance(body, list):
        response_text = "\n".join(json.dumps(item) for item in body)
    else:
        response_text = json.dumps(body)

    class FakeResponse:
        status_code = 200
        headers = {}
        text = response_text

    fake_response = FakeResponse()

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
            "proj-1", "gemini-3.1-flash-image", mock.Mock(), "cookie", {},
            retry_max=0, backoff_sec=0, jitter_sec=0,
            fastapi_request=fastapi_request)

    async def fake_timed_call(model, operation, *, whole_call=False, seconds=None):
        return await operation

    base_patches = [
        mock.patch.object(cp, "open_upstream_client", return_value=FakeCM()),
        mock.patch.object(cp, "timed_call", side_effect=fake_timed_call),
        mock.patch.object(cp, "build_headers", return_value={"auth": "1"}),
        mock.patch.object(cp, "build_request_body_async",
                          mock.AsyncMock(return_value={"contents": []})),
        mock.patch.object(cp, "stats"),
    ]
    return run, base_patches + list(patches_extra), fake_response


class CookieCollectMemoryContractTests(unittest.TestCase):
    """契约 8/9：整取收集期不再钉住解析副本与诊断样本（第五轮）。"""

    def test_parsed_object_released_before_event_consumption(self):
        # del obj + 拉取式游标：事件消费钩子（upstream_chunk）触发时，
        # 解析出的对象除测试自有引用外应无其他强引用（getrefcount == 2：
        # 测试捕获 + getrefcount 临时项）。生成器形态的旧实现会由
        # async-for 变量 + 两层生成器帧局部 + 采样器留存多钉住至少 3 份，
        # 此测试会 FAIL。（dict 实例不支持 weakref，故用 refcount 计量。）
        import upstreams.cookie_proxy as cp

        captured = []
        real_extract = cp.extract_events

        def spying_extract(obj):
            captured.append(obj)
            return real_extract(obj)

        chunk_checks = []

        def spying_chunk():
            # 用下标访问（captured[i]）而非迭代：迭代变量本身会钉住一份引用
            chunk_checks.append(
                [sys.getrefcount(captured[i]) for i in range(len(captured))])
            real_chunk()

        real_chunk = cp.upstream_chunk

        run, patches, _ = _cookie_collect_harness(lambda: {
            "results": [{"data": {"candidates": [{
                "content": {"parts": [
                    {"inlineData": {"mimeType": "image/png", "data": "QkFTRTY0" * 64}},
                ]},
                "finishReason": "STOP",
            }]}}]
        }, [
            mock.patch.object(cp, "extract_events", spying_extract),
            mock.patch.object(cp, "upstream_chunk", spying_chunk),
        ])
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        result = asyncio.run(run())

        self.assertEqual(result["kind"], "ok", str(result))
        self.assertTrue(captured, "extract_events 未被调用")
        # 每个 upstream_chunk（事件消费开始）时刻：除测试捕获与 getrefcount
        # 临时项外无其他引用（旧实现 ≥5：async-for 变量+两层生成器帧+采样器）
        self.assertTrue(all(ref == 2 for check in chunk_checks for ref in check),
                        f"解析对象在事件消费前未释放: {chunk_checks}")

    def test_sampler_stops_retaining_on_body_but_keeps_count(self):
        import upstreams.cookie_proxy as cp

        created = []

        class SpySampler(cp.RawSampler):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                created.append(self)

        run, patches, _ = _cookie_collect_harness(lambda: [
            {"results": [{"data": {"candidates": [{
                "content": {"parts": [
                    {"inlineData": {"mimeType": "image/png", "data": "QkFTRTY0" * 64}},
                ]},
                "finishReason": "STOP",
            }]}}]},
            {"results": [{"data": {"candidates": [{"finishReason": "STOP"}]}}]},
        ], [mock.patch.object(cp, "RawSampler", SpySampler)])
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        result = asyncio.run(run())

        self.assertEqual(result["kind"], "ok", str(result))
        sampler = created[0]
        # 计数保留（诊断的「共 N 个对象」口径不变），引用不再留存
        self.assertEqual(sampler.count, 2)
        self.assertEqual(sampler._head, [])
        self.assertEqual(sampler._tail, [])
        self.assertFalse(sampler._retaining)

    def test_sampler_retains_samples_when_no_body(self):
        # 无正文（只有思考）路径的 dump 诊断不受停止留存影响：样本照常保留。
        import upstreams.cookie_proxy as cp

        created = []

        class SpySampler(cp.RawSampler):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                created.append(self)

        run, patches, _ = _cookie_collect_harness(lambda: {
            "results": [{"data": {"candidates": [{
                "content": {"parts": [{"text": "内心独白", "thought": True}]},
                "finishReason": "STOP",
            }]}}]
        }, [mock.patch.object(cp, "RawSampler", SpySampler)])
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        result = asyncio.run(run())

        self.assertEqual(result["kind"], "error")
        sampler = created[0]
        self.assertTrue(sampler._retaining)
        self.assertEqual(len(sampler._head), 1)
        self.assertIn("共 1 个对象", sampler.dump())


class CookieCollectResponsePathTests(unittest.TestCase):
    """契约 10：非流式整取（collect_response）与生图假流式同构的内存行为。"""

    def test_collect_response_releases_buffer_and_joins_image_events(self):
        import upstreams.cookie_proxy as cp

        image_b64 = "QkFTRTY0" * 32
        body = {
            "results": [{"data": {"candidates": [{
                "content": {"parts": [
                    {"text": "导语"},
                    {"inlineData": {"mimeType": "image/png", "data": image_b64}},
                    {"text": "尾声"},
                ]},
                "finishReason": "STOP",
            }]}}]
        }

        class FakeResponse:
            status_code = 200
            headers = {}
            text = json.dumps(body)

        fake_response = FakeResponse()
        seen_refcounts = []

        class SpyingCursor(cp.CompleteTextObjectCursor):
            def __init__(cursor_self, text, limit=None):
                seen_refcounts.append(sys.getrefcount(fake_response))
                super().__init__(text)

        gen = cp.CookieGeneration(
            request_obj=mock.Mock(stream=False, model="gemini-3.1-flash-image",
                                  messages=[], max_tokens=None),
            fastapi_request=mock.Mock(
                is_disconnected=mock.AsyncMock(return_value=False)),
            record=None, account_id="", cookie_str="c", project_id="p",
            base_model_name="gemini-3.1-flash-image",
            model_display="gemini-3.1-flash-image",
            response_id="chatcmpl-test", client_kwargs={},
            retry_max=0, backoff_sec=0, jitter_sec=0, start_time=time.time(),
            prefill_text="", prefill_active=False, force_builtin_search=False,
            native_custom_tools=False, text_transport=None, strip_thoughts=False,
            want_usage=False, cookie_debug=False, is_stream=False, is_image=True,
        )

        class FakeClient:
            async def post(self, *args, **kwargs):
                return fake_response

        class FakeCM:
            async def __aenter__(self):
                return FakeClient()

            async def __aexit__(self, *exc):
                return False

        async def fake_timed_call(model, operation, *, whole_call=False, seconds=None):
            return await operation

        async def run():
            with mock.patch.object(cp, "open_upstream_client",
                                   return_value=FakeCM()), \
                 mock.patch.object(cp, "timed_call", side_effect=fake_timed_call), \
                 mock.patch.object(cp, "credential_manager"), \
                 mock.patch.object(cp, "build_request_body_async",
                                   mock.AsyncMock(return_value={"variables": {}})), \
                 mock.patch.object(cp, "CompleteTextObjectCursor", SpyingCursor):
                return await gen.collect_response()

        response = asyncio.run(run())
        payload = json.loads(response.body)
        expected_md = f"![Generated Image](data:image/png;base64,{image_b64})"
        # 图片事件走片段表 + join：导语 + 图片 + 尾声逐字节一致
        self.assertEqual(payload["choices"][0]["message"]["content"],
                         "导语" + expected_md + "尾声")
        # 解析开始时 httpx 响应对象已置 None（引用只剩测试局部 + getrefcount 临时项）
        self.assertEqual(seen_refcounts, [2])


if __name__ == "__main__":
    unittest.main(verbosity=2)
