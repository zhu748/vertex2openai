"""第十六轮（PRF 域性能余项）契约测试。

覆盖：PRF-P2-2（工具流记账无 repr 物化）、PRF-P2-7（redact 先截断再
脱敏 + 异常对象安全）、PRF-P2-8（大请求体读取并发闸：快速 503 / 末块
释放 / 幂等兜底）、PRF-P3-3（日志历史 deque 淘汰、created 秒级缓存、
工具调用 ID 去排序化确定性）、PRF-P3-4（同步 resolve 阻塞告警不抛）。
（PRF-P2-1 计量改估长、PRF-P3-1 片段表 join 为行为保持型性能改动，
由既有套件回归护航；PRF-P3-2 经实测否决 join 方案，保留 BytesIO。）
"""
import asyncio
import time
from types import SimpleNamespace
import unittest
from unittest import mock

import middleware
from middleware import (
    LARGE_BODY_READ_SLOTS,
    StatsMiddleware,
    _BodyReadGate,
    _large_body_gate_needed,
)
from request_tracking import RequestTrace


class ToolStreamAccountingTests(unittest.TestCase):
    """PRF-P2-2：记账按字段估长，长参数不再走 repr 物化。"""

    def test_partial_args_accounted_without_repr(self):
        from tool_stream import ToolStreamAssembler
        asm = ToolStreamAssembler()
        part = SimpleNamespace(
            text=None, thought_signature=None, inline_data=None,
            function_call=SimpleNamespace(
                name="run_query", id="c1", args=None,
                partial_args=[SimpleNamespace(string_value="x" * 10000)],
            ),
        )
        asm._account(part)
        self.assertGreater(asm.bytes, 10000)

    def test_text_and_signature_fields_accounted(self):
        from tool_stream import ToolStreamAssembler
        asm = ToolStreamAssembler()
        part = SimpleNamespace(
            text="t" * 300, thought_signature=b"s" * 200, inline_data=None,
            function_call=None,
        )
        asm._account(part)
        self.assertGreater(asm.bytes, 500)

    def test_dict_args_use_structural_estimate(self):
        from tool_stream import ToolStreamAssembler
        asm = ToolStreamAssembler()
        part = SimpleNamespace(
            text=None, thought_signature=None, inline_data=None,
            function_call=SimpleNamespace(
                name="f", id="c1", partial_args=None,
                args={"key": "v" * 500},
            ),
        )
        asm._account(part)
        self.assertGreater(asm.bytes, 500)


class RedactTruncateFirstTests(unittest.TestCase):
    """PRF-P2-7：先截断再脱敏；异常对象与超长消息都安全。"""

    def _trace(self):
        return RequestTrace("/v1/chat/completions")

    def test_error_truncation_bounds(self):
        trace = self._trace()
        trace.fail("x" * 5000, 503)
        self.assertLessEqual(len(trace.data["last_error"]), 1200)

    def test_exception_objects_accepted_by_retry_path(self):
        trace = self._trace()
        trace.retry(0.5, RuntimeError("boom " * 2000))
        self.assertLessEqual(len(trace.data["last_error"]), 1200)
        self.assertIn("boom", trace.data["last_error"])

    def test_event_message_with_credential_in_head_still_redacted(self):
        trace = self._trace()
        trace.event("test", "Bearer sk-secret-token-123 之后是 " + "y" * 3000)
        item = list(trace.events)[-1]
        self.assertNotIn("sk-secret-token-123", item["message"])
        self.assertLessEqual(len(item["message"]), 1200)

    def test_logger_pre_truncation_keeps_marker_with_original_length(self):
        from logger import _MAX_STDOUT_CHARS, custom_print
        captured = []
        with mock.patch("logger._enqueue_stdout", lambda text, end, flush: captured.append(text)), \
             mock.patch("logger.current_trace") as trace_ctx:
            trace_ctx.get.return_value = None
            custom_print("z" * (64 * 1024))
        self.assertEqual(len(captured), 1)
        line = captured[0]
        self.assertLessEqual(len(line), _MAX_STDOUT_CHARS)
        self.assertIn(f"已截断，原长度 {64 * 1024} 字符", line)


class LargeBodyGateTests(unittest.IsolatedAsyncioTestCase):
    """PRF-P2-8：大请求体读取并发闸。"""

    def test_gate_needed_matrix(self):
        self.assertTrue(_large_body_gate_needed(""))                     # chunked / 无长度头
        self.assertTrue(_large_body_gate_needed(str(4 * 1024 * 1024 + 1)))
        self.assertFalse(_large_body_gate_needed(str(4 * 1024 * 1024)))  # 恰在阈值 = 小 body
        self.assertFalse(_large_body_gate_needed("1024"))
        self.assertTrue(_large_body_gate_needed("not-a-number"))

    async def test_body_read_gate_releases_once_on_final_chunk(self):
        sem = asyncio.Semaphore(1)
        await sem.acquire()  # 读位被占用
        gate = _BodyReadGate(sem)
        messages = [
            {"type": "http.request", "body": b"a", "more_body": True},
            {"type": "http.request", "body": b"b", "more_body": False},
        ]

        async def receive():
            return messages.pop(0)

        wrapped = gate.wrap(receive)
        self.assertEqual((await wrapped())["more_body"], True)
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(sem.acquire(), timeout=0.05)  # 未到最后一块：读位不还
        self.assertEqual((await wrapped())["more_body"], False)
        await asyncio.wait_for(sem.acquire(), timeout=0.1)       # 末块穿过即释放
        gate.release()                                           # finally 兜底幂等
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(sem.acquire(), timeout=0.05)  # 不多还

    async def test_middleware_fast_fails_503_when_slots_exhausted(self):
        for _ in range(LARGE_BODY_READ_SLOTS):
            await middleware._large_body_reads.acquire()
        try:
            sent = []

            async def send(message):
                sent.append(message)

            async def receive():
                raise AssertionError("503 快速失败不应读请求体")

            scope = {"type": "http", "method": "POST", "path": "/v1/chat/completions",
                     "headers": [(b"content-length", str(5 * 1024 * 1024).encode())]}
            stats = SimpleNamespace(increment_total=lambda: None, add_error=lambda: None)
            with mock.patch.object(middleware, "early_generation_auth",
                                   mock.AsyncMock(return_value=None)):
                await StatsMiddleware(SimpleNamespace(), lambda: stats)(scope, receive, send)
            self.assertEqual(sent[0]["status"], 503)
            self.assertIn(b"retry-after", dict(sent[0]["headers"]))
            body = sent[-1]["body"]
            self.assertIn("大请求体".encode(), body)
        finally:
            for _ in range(LARGE_BODY_READ_SLOTS):
                middleware._large_body_reads.release()


class LoggerHistoryDequeTests(unittest.TestCase):
    """PRF-P3-3：日志历史 deque(maxlen) 淘汰，seq 全局递增保持。"""

    def test_history_eviction_bounded_and_seq_monotonic(self):
        from logger import SSELogger
        log = SSELogger(max_history=3)
        for i in range(5):
            log.push(f"message-{i}")
        snapshot = log.snapshot_history()
        self.assertEqual([seq for seq, _ in snapshot], [3, 4, 5])
        self.assertEqual([msg for _, msg in snapshot][-1].endswith("message-4"), True)


class CookieChunkMicroTests(unittest.TestCase):
    """PRF-P3-3：created 秒级缓存与无 sort_keys 的确定性 ID。"""

    def test_created_cache_matches_clock(self):
        from upstreams.cookie_response import _current_created
        self.assertAlmostEqual(_current_created(), int(time.time()), delta=1)
        self.assertEqual(_current_created(), _current_created())  # 同秒稳定

    def test_tool_call_id_deterministic_across_equal_args(self):
        from upstreams.cookie_response import _stable_cookie_tool_call_id
        first = _stable_cookie_tool_call_id("resp-1", 0, "read_database",
                                            {"query": "x", "limit": 1})
        second = _stable_cookie_tool_call_id("resp-1", 0, "read_database",
                                             {"query": "x", "limit": 1})
        self.assertEqual(first, second)
        self.assertNotEqual(
            _stable_cookie_tool_call_id("resp-1", 0, "f", {}),
            _stable_cookie_tool_call_id("resp-2", 0, "f", {}),  # 跨响应必不同
        )


class SyncResolveWarnTests(unittest.IsolatedAsyncioTestCase):
    """PRF-P3-4：同步 resolve 的阻塞告警（防御性，不抛异常）。"""

    async def test_warn_helper_never_raises_on_or_off_loop(self):
        from proxy_resolver import _warn_sync_resolve_blocked_loop
        _warn_sync_resolve_blocked_loop("sing-box", time.monotonic())            # 缓存命中：静默
        _warn_sync_resolve_blocked_loop("sing-box", time.monotonic() - 30)       # 循环上阻塞：告警
        import threading
        results = []
        thread = threading.Thread(  # 工作线程（无运行循环）：静默且不抛
            target=lambda: results.append(_try(_warn_sync_resolve_blocked_loop, "proxybridge", time.monotonic() - 30)))
        thread.start()
        thread.join()
        self.assertEqual(results, [None])  # None = 无异常


def _try(fn, *args):
    try:
        fn(*args)
        return None
    except Exception as exc:  # pragma: no cover - 防御性断言
        return exc


if __name__ == "__main__":
    unittest.main()
