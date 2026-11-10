"""4K 生图超大 content 的 SSE 分帧与内存关键路径回归测试。

契约（2026-11 修复，详见 message_processing.slice_giant_content）：
- 超过 GIANT_CONTENT_THRESHOLD（1MiB）的生图 base64 content 必须切成
  256KiB 增量帧发射——单帧发射时 json.dumps + f"data:..." + ASGI encode
  会瞬间再造三份全量拷贝，叠加转换链后 512MB 容器（Render 免费档）被
  OOMKill：进程级 SIGKILL，无 Python 异常，表现为"收到首个上游分片后
  服务直接崩溃重启"；
- 切片拼接结果与单帧输出逐字节一致（客户端按 OpenAI delta 拼接语义无感）；
- ≤1MiB 的图片保持整图单帧（既有假流式 UX 不变）；
- role 只出现在首个 content 帧（对齐官方帧序）；
- _convert_image_to_markdown 输出格式不变（b64 直接一步拼 markdown，
  峰值拷贝 4 份 → 2 份）。
"""

import asyncio
import base64
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

_STATE = tempfile.TemporaryDirectory(prefix="v2o-imgmem-test-")
os.environ.setdefault("STATE_DIR", _STATE.name)
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from message_processing import (  # noqa: E402
    GIANT_CONTENT_SLICE,
    GIANT_CONTENT_THRESHOLD,
    _convert_image_to_markdown,
    slice_giant_content,
)
from upstreams.cookie_response import (  # noqa: E402
    iter_openai_content_chunks,
    make_openai_chunk,
)
from upstreams.express_response import (  # noqa: E402
    _chunk_openai_response_dict_for_sse,
)


def _image_markdown(size_bytes: int) -> str:
    """size_bytes 随机二进制 → 与上游同构的 markdown data URL。"""
    return _convert_image_to_markdown(os.urandom(size_bytes), "image/png")


def _collect_frames(coro):
    return asyncio.run(_collect_frames_async(coro))


async def _collect_frames_async(agen):
    return [frame async for frame in agen]


def _parse_sse(frame: str):
    assert frame.startswith("data: ") and frame.endswith("\n\n"), frame[:64]
    body = frame[len("data: "):-2]
    if body == "[DONE]":
        return None
    return json.loads(body)


class SliceGiantContentTests(unittest.TestCase):
    def test_small_content_stays_single_piece(self):
        text = "x" * GIANT_CONTENT_THRESHOLD
        self.assertEqual(slice_giant_content(text), [text])

    def test_giant_content_slices_concatenate_byte_identical(self):
        text = _image_markdown(GIANT_CONTENT_SLICE * 5 + 123)
        pieces = slice_giant_content(text)
        self.assertGreater(len(pieces), 1)
        self.assertTrue(all(len(p) <= GIANT_CONTENT_SLICE for p in pieces))
        self.assertEqual("".join(pieces), text)

    def test_unicode_boundaries_survive(self):
        # 中文 + emoji 混排：切片是 Python 码点序列切片，拼接必须原样。
        text = ("🦍图片" * 300000) + "!"  # ~2MB，多码点字符
        self.assertEqual("".join(slice_giant_content(text)), text)


class ExpressFakeStreamFramingTests(unittest.TestCase):
    def _response_dict(self, content: str) -> dict:
        return {
            "id": "chatcmpl-test",
            "object": "chat.completion",
            "created": 1,
            "model": "gemini-image-test",
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": content},
                    "finish_reason": "stop",
                }
            ],
        }

    def _deltas(self, response_dict):
        frames = _collect_frames(
            _chunk_openai_response_dict_for_sse(openai_response_dict=response_dict)
        )
        payloads = [p for p in (_parse_sse(f) for f in frames) if p is not None]
        content_deltas = []
        roles = []
        finish = None
        for p in payloads:
            for choice in p.get("choices", []):
                delta = choice.get("delta") or {}
                if "content" in delta:
                    content_deltas.append(delta["content"])
                    if "role" in delta:
                        roles.append(delta["role"])
                if choice.get("finish_reason"):
                    finish = choice["finish_reason"]
        self.assertTrue(all(f.startswith("data: ") for f in frames))
        return content_deltas, roles, finish, frames

    def test_giant_image_chunks_into_incremental_deltas(self):
        content = _image_markdown(3 * 1024 * 1024)  # 3MiB 图 → ~4MiB base64
        self.assertGreater(len(content), GIANT_CONTENT_THRESHOLD)
        deltas, roles, finish, frames = self._deltas(self._response_dict(content))
        self.assertGreater(len(deltas), 1)
        self.assertEqual("".join(deltas), content)  # 字节一致
        self.assertEqual(roles, ["assistant"])       # role 只在首帧
        self.assertEqual(finish, "stop")
        # 单帧体积受控（不再出现数 MB 的巨型 SSE 事件）
        self.assertLessEqual(max(len(f) for f in frames), GIANT_CONTENT_SLICE * 2)

    def test_small_image_stays_whole(self):
        content = _image_markdown(64 * 1024)  # 64KiB 图
        deltas, _, finish, _ = self._deltas(self._response_dict(content))
        self.assertEqual(deltas, [content])
        self.assertEqual(finish, "stop")

    def test_plain_text_keeps_ten_slice_behavior(self):
        deltas, _, _, _ = self._deltas(self._response_dict("abcdefghij"))
        self.assertEqual("".join(deltas), "abcdefghij")
        self.assertGreater(len(deltas), 1)


class CookieContentChunkTests(unittest.TestCase):
    def test_giant_content_multi_frame_byte_identical(self):
        content = _image_markdown(2 * 1024 * 1024)
        frames = list(iter_openai_content_chunks(
            "chatcmpl-ck", "nano-banana", content, role="assistant"
        ))
        self.assertGreater(len(frames), 1)
        deltas = [_parse_sse(f)["choices"][0]["delta"]["content"] for f in frames]
        self.assertEqual("".join(deltas), content)
        first = _parse_sse(frames[0])["choices"][0]["delta"]
        self.assertEqual(first.get("role"), "assistant")
        # 后续帧不再重复 role
        for f in frames[1:]:
            self.assertNotIn("role", _parse_sse(f)["choices"][0]["delta"])

    def test_no_role_kept_absent(self):
        # 调用点自带独立 role 帧：不传 role 时不得擅自注入
        content = "![Image](data:image/png;base64," + "A" * (2 << 20) + ")"
        frames = list(iter_openai_content_chunks("chatcmpl-ck", "nano-banana", content))
        for f in frames:
            self.assertNotIn(
                "role", _parse_sse(f)["choices"][0]["delta"]
            )

    def test_small_content_single_frame_matches_legacy_shape(self):
        content = "![Image](data:image/png;base64,AAA)"
        frames = list(iter_openai_content_chunks("chatcmpl-ck", "nano-banana", content))
        self.assertEqual(
            frames,
            [make_openai_chunk("chatcmpl-ck", "nano-banana", content=content)],
        )


class ImageMarkdownFormatTests(unittest.TestCase):
    def test_markdown_shape_unchanged(self):
        raw = b"\x89PNG-fake-bytes"
        md = _convert_image_to_markdown(raw, "image/png")
        expected = (
            "![Image](data:image/png;base64," + base64.b64encode(raw).decode() + ")"
        )
        self.assertEqual(md, expected)


if __name__ == "__main__":
    unittest.main()
