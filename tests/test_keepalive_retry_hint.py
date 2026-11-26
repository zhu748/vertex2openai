"""A monitored endpoint can ask the next periodic GET to wait without blocking stop."""
import asyncio
from datetime import UTC, datetime
from email.utils import format_datetime
import unittest
from unittest.mock import patch

import httpx
import keepalive
import retry_policy


class KeepaliveRetryHint(unittest.IsolatedAsyncioTestCase):
    async def run_case(self, status, hint, expected):
        now = 1_800_000_000
        real_client = httpx.AsyncClient
        calls, waits = [], []
        elapsed = 0.0
        sleeping = asyncio.Event()
        class HeadOnly(httpx.AsyncByteStream):
            async def __aiter__(self):
                raise AssertionError("probe must not read response body")
                yield b""
        def respond(request):
            calls.append(request.url)
            if len(calls) > 1:
                return httpx.Response(200, stream=HeadOnly())
            return httpx.Response(status, headers={"Retry-After": hint}, stream=HeadOnly())
        def client(**kwargs):
            return real_client(transport=httpx.MockTransport(respond), **kwargs)
        async def controlled_sleep(delay):
            nonlocal elapsed
            waits.append(delay)
            if delay == float("inf") or len(waits) == 2:
                sleeping.set()
                await asyncio.Event().wait()
            elapsed += delay
        service = keepalive.KeepAliveService()
        with (patch.object(keepalive.httpx, "AsyncClient", side_effect=client),
              patch.object(keepalive.asyncio, "sleep", side_effect=controlled_sleep),
              patch.object(keepalive, "monotonic", lambda: elapsed),
              patch.object(retry_policy.time, "time", return_value=now)):
            service._task = asyncio.create_task(service._run("https://fixture.invalid/health", 5, False))
            try:
                await asyncio.wait_for(sleeping.wait(), 1)
                if expected == float("inf"):
                    self.assertEqual((len(calls), waits), (1, [expected]))
                else:
                    self.assertEqual((len(calls), waits), (2, [expected, 5]))
            finally:
                await asyncio.wait_for(service.stop(), 1)
                self.assertFalse(service.is_running)

    async def test_next_probe_waits_only_for_actionable_server_backoff(self):
        later = format_datetime(datetime.fromtimestamp(1_800_000_060, UTC), usegmt=True)
        for status, value, wait in ((429, "60", 60), (503, later, 60), (503, "3", 5),
                                    (503, "0", 5), (503, "wrong", 5), (200, "60", 5),
                                    (404, "60", 5), (429, "9" * 400, float("inf"))):
            with self.subTest(status=status, value=value):
                await self.run_case(status, value, wait)


class KeepaliveHotReload(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.service = keepalive.KeepAliveService()
        self.config = ["https://one.invalid/health", 5, False]
        self.now = 1000.0
        self.waits = asyncio.Queue()
        self.urls = []
        real_client = httpx.AsyncClient

        def handle(request):
            self.urls.append(str(request.url))
            return self.reply(request)

        async def wait(delay):
            self.waits.put_nowait(delay)
            await asyncio.Future()  # A restart/stop must interrupt even infinite waits.

        self.enterContext(patch.object(keepalive.httpx, "AsyncClient",
                                       lambda **kw: real_client(transport=httpx.MockTransport(handle), **kw)))
        self.enterContext(patch.object(self.service, "_current_config", lambda: tuple(self.config)))
        self.enterContext(patch.object(keepalive, "monotonic", lambda: self.now))
        self.enterContext(patch.object(keepalive.asyncio, "sleep", wait))
        self.addAsyncCleanup(self.service.stop)

    async def expect_wait(self, delay):
        self.assertEqual(await asyncio.wait_for(self.waits.get(), 1), delay)

    async def test_same_wire_target_and_concurrent_reloads_keep_original_deadline(self):
        self.reply = lambda _: (httpx.Response(503, headers={"retry-after": "60"})
                                if len(self.urls) == 1 else httpx.Response(200))
        await self.service.start()
        await self.expect_wait(60)
        self.now += 13
        # Fragment, host case and default port do not identify a different endpoint.
        self.config[:] = ["HTTPS://ONE.INVALID:443/health#settings", 7, True]
        await self.service.restart()
        await self.expect_wait(47)
        await asyncio.gather(*(self.service.restart() for _ in range(4)))
        await self.expect_wait(47)
        self.assertEqual(len(self.urls), 1)
        while not self.waits.empty():
            self.assertEqual(self.waits.get_nowait(), 47)
        self.now += 47
        await self.service.restart()
        await self.expect_wait(7)
        self.assertEqual(len(self.urls), 2)

    async def test_different_target_independent_but_redirect_cannot_recontact_paused_target(self):
        target = "https://final.invalid/z"
        def response(request):
            if str(request.url) == target:
                return httpx.Response(429, headers={"Retry-After": "60"})
            if request.url.path == "/other":
                return httpx.Response(204)
            return httpx.Response(302, headers={"location": target})
        self.reply = response
        await self.service.start()
        await self.expect_wait(60)
        self.config[0] = "https://two.invalid/new"
        self.now += 10
        await self.service.restart()
        await self.expect_wait(50)
        self.assertEqual(self.urls, ["https://one.invalid/health", target, self.config[0]])
        self.config[0] = "https://two.invalid/other"
        await self.service.restart()
        await self.expect_wait(5)
        self.assertEqual(self.urls[-1], self.config[0])

    async def test_explicit_stop_clears_even_unbounded_advisory(self):
        self.reply = lambda _: (httpx.Response(503, headers={"retry-after": "9" * 400})
                                if len(self.urls) == 1 else httpx.Response(200))
        await self.service.start()
        await self.expect_wait(float("inf"))
        await self.service.restart()
        await self.expect_wait(float("inf"))
        self.assertEqual(len(self.urls), 1)
        await asyncio.wait_for(self.service.stop(), 1)
        await self.service.start()
        await self.expect_wait(5)
        self.assertEqual(len(self.urls), 2)
