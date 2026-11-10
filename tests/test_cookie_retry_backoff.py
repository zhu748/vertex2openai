"""Cookie 通道统一退避窗口（cookie_transport.cookie_retry_backoff*）的契约测试。

锁定 2026-11 P2 重试执行器收敛后的统一口径：
- 同一 Cookie 身份重试 → identity_rotates=False：上游 Retry-After 有约束力
  （比窗口上限长时采纳、保留 120 秒绝对上限），与 Express 通道的
  「换 Key 后钳位」语义相反；
- 统一格式重试日志（label + 尝试次数 + 延迟），原生桥走独立 log_tag；
- 静默版返回 False 表示客户端断开；心跳版逐帧转发 retry_heartbeats。
"""
import asyncio
import io
import os
from contextlib import redirect_stdout
from pathlib import Path
import unittest
from unittest import mock

os.chdir(Path(__file__).resolve().parents[1] / "app")

from upstreams import cookie_transport  # noqa: E402


class _Request:
    def __init__(self, disconnected=False):
        self.disconnected = disconnected

    async def is_disconnected(self):
        return self.disconnected


class CookieRetryBackoffTests(unittest.IsolatedAsyncioTestCase):
    async def test_retry_after_longer_than_window_is_honored(self):
        # 同一 Cookie 身份：上游提示 8s > 窗口 5s → 采纳 8s，不钳位
        request = _Request()
        with mock.patch.object(cookie_transport, "wait_for_retry", return_value=True) as wait:
            ok = await cookie_transport.cookie_retry_backoff(
                request, retry_after="8", backoff_sec=5, attempt=0, jitter_sec=0,
                error="HTTP 429: busy", label="HTTP 429（生图）")
        self.assertTrue(ok)
        self.assertEqual(wait.await_args_list[0].args[1], 8.0)

    async def test_retry_after_shorter_than_window_is_adopted(self):
        # 上游提示 2s < 窗口 5s → 照常采纳更短的服务器提示
        with mock.patch.object(cookie_transport, "wait_for_retry", return_value=True) as wait:
            ok = await cookie_transport.cookie_retry_backoff(
                _Request(), retry_after="2", backoff_sec=5, attempt=0, jitter_sec=0,
                error="HTTP 429: busy", label="429/限流")
        self.assertTrue(ok)
        self.assertEqual(wait.await_args_list[0].args[1], 2.0)

    async def test_retry_after_keeps_120s_absolute_cap(self):
        # 天文数字防御：999s 提示被 120 秒绝对上限拦住
        with mock.patch.object(cookie_transport, "wait_for_retry", return_value=True) as wait:
            await cookie_transport.cookie_retry_backoff(
                _Request(), retry_after="999", backoff_sec=5, attempt=0, jitter_sec=0,
                error="HTTP 429: busy", label="429/限流")
        self.assertEqual(wait.await_args_list[0].args[1], 120.0)

    async def test_client_disconnect_returns_false(self):
        with mock.patch.object(cookie_transport, "wait_for_retry", return_value=False) as wait:
            ok = await cookie_transport.cookie_retry_backoff(
                _Request(), retry_after=None, backoff_sec=0, attempt=1, jitter_sec=0,
                error="timeout", label="异常")
        self.assertFalse(ok)
        self.assertTrue(wait.awaited)

    async def test_uniform_log_line_and_custom_tag(self):
        with mock.patch.object(cookie_transport, "wait_for_retry", return_value=True):
            buffer = io.StringIO()
            with redirect_stdout(buffer):
                await cookie_transport.cookie_retry_backoff(
                    _Request(), retry_after="8", backoff_sec=5, attempt=0, jitter_sec=0,
                    error="HTTP 429: busy", label="HTTP 429（生图）")
            self.assertIn("⚠️ [Studio] HTTP 429（生图）（尝试 1）", buffer.getvalue())
            self.assertIn("| 8.0s 后重试...", buffer.getvalue())

            buffer = io.StringIO()
            with redirect_stdout(buffer):
                await cookie_transport.cookie_retry_backoff(
                    _Request(), retry_after="8", backoff_sec=5, attempt=2, jitter_sec=0,
                    error="quota", label="限流/拥堵", log_tag="[Cookie 原生桥]")
            self.assertIn("⚠️ [Cookie 原生桥] 限流/拥堵（尝试 3）", buffer.getvalue())


class CookieRetryBackoffHeartbeatTests(unittest.TestCase):
    def _collect(self, **kwargs):
        async def run():
            frames = []
            async for frame in cookie_transport.cookie_retry_backoff_heartbeats(**kwargs):
                frames.append(frame)
            return frames
        return asyncio.run(run())

    def test_heartbeats_are_relayed_with_binding_delay(self):
        # 心跳版同样遵循同一身份口径：8s 提示 → retry_heartbeats 收到 8s 总时长
        async def fake_heartbeats(total_sec, hb_interval=3.0, request=None):
            self.assertEqual(total_sec, 8.0)
            for _ in range(3):
                yield "HB"
        with mock.patch.object(cookie_transport, "retry_heartbeats", new=fake_heartbeats):
            frames = self._collect(request=_Request(), retry_after="8", backoff_sec=5,
                                   attempt=0, jitter_sec=0, error="busy",
                                   label="可重试拥堵/限流")
        self.assertEqual(frames, ["HB", "HB", "HB"])

    def test_empty_wait_yields_nothing(self):
        # backoff=0（立即重试）：无心跳、无等待
        async def fake_heartbeats(total_sec, hb_interval=3.0, request=None):
            self.assertEqual(total_sec, 0.0)
            return
            yield  # pragma: no cover - 保持生成器形态
        with mock.patch.object(cookie_transport, "retry_heartbeats", new=fake_heartbeats):
            frames = self._collect(request=_Request(), retry_after=None, backoff_sec=0,
                                   attempt=0, jitter_sec=0, error="busy", label="异常")
        self.assertEqual(frames, [])


if __name__ == "__main__":
    unittest.main()
