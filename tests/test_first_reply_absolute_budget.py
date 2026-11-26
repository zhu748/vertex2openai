"""First-reply budget and resource ownership, using only local awaitables."""
import asyncio
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

import anyio
import httpx

import first_reply
from first_reply import FirstReplyGuard, FirstReplyTimeout
from proxy_failover import ProxyChain


class AbsoluteFirstReplyBudgetTests(unittest.IsolatedAsyncioTestCase):
    async def test_expired_guard_does_not_start_or_orphan_work(self):
        guard = FirstReplyGuard("test", seconds=1)
        guard.deadline = guard.started - 1
        started = []

        async def new_request():
            started.append(True)

        operation = new_request()
        with self.assertRaises(FirstReplyTimeout):
            await guard.wait(operation)
        self.assertEqual(started, [])
        self.assertIsNone(operation.cr_frame)  # disposed, not left unawaited

        resource = SimpleNamespace(aclose=AsyncMock())
        done = asyncio.get_running_loop().create_future()
        done.set_result(resource)
        with self.assertRaises(FirstReplyTimeout):
            await guard.wait(done)
        resource.aclose.assert_awaited_once()

        # An already-started task may suppress cancellation and hand us a resource.
        started_task = asyncio.Event()
        resource = SimpleNamespace(aclose=AsyncMock())
        async def pending():
            started_task.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                return resource
        task = asyncio.create_task(pending())
        try:
            await started_task.wait()
            with self.assertRaises(FirstReplyTimeout):
                await asyncio.wait_for(guard.wait(task), .2)
            resource.aclose.assert_awaited_once()
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_sync_result_past_deadline_is_closed_without_timer_tick(self):
        now = [100.0]
        clock = SimpleNamespace(monotonic=lambda: now[0], time=lambda: now[0])
        resource = SimpleNamespace(aclose=AsyncMock())
        with patch.object(first_reply, "time", clock):
            guard = FirstReplyGuard("test", seconds=10)
            async def immediate():
                now[0] = guard.deadline + .1  # CPU work, no time for timeout callback
                return resource
            with self.assertRaises(FirstReplyTimeout):
                await guard.wait(immediate())
        resource.aclose.assert_awaited_once()

    async def test_sync_error_checks_absolute_deadline_without_timer_tick(self):
        for elapsed, expected in ((.003, OSError), (.012, FirstReplyTimeout)):
            with self.subTest(elapsed=elapsed):
                now = [100.0]
                original = OSError("local transport failure")
                clock = SimpleNamespace(monotonic=lambda: now[0], time=lambda: now[0])
                with patch.object(first_reply, "time", clock):
                    guard = FirstReplyGuard("test", seconds=.004)
                    async def fail():
                        now[0] += elapsed  # synchronous work: timer callback has not run
                        raise original
                    with self.assertRaises(expected) as caught:
                        await guard.wait(fail())
                if expected is FirstReplyTimeout:
                    self.assertIs(caught.exception.__cause__, original)
                    self.assertEqual(caught.exception.seconds, .004)
                else:
                    self.assertIs(caught.exception, original)

    async def test_one_or_multiple_candidates_cannot_return_cancel_suppressed_result(self):
        for count in (1, 2):
            with self.subTest(candidates=count):
                attempted = []
                resource = SimpleNamespace(status_code=200, aclose=AsyncMock())
                async def call(raw, _resolved):
                    attempted.append(raw)
                    try:
                        await asyncio.Event().wait()
                    except asyncio.CancelledError:
                        return resource

                guard = FirstReplyGuard("test", seconds=.015)
                chain = ProxyChain(candidates=[""] * count)
                with self.assertRaises(FirstReplyTimeout):
                    await guard.wait(chain.run(call))
                self.assertEqual(attempted, [""])
                resource.aclose.assert_awaited_once()

    async def test_elapsed_attempt_cannot_start_another_operation(self):
        now, attempted = [100.0], []
        clock = SimpleNamespace(monotonic=lambda: now[0], time=lambda: now[0])
        async def failed(raw, _resolved):
            attempted.append(raw)
            now[0] += 1
            if len(attempted) == 1:
                raise httpx.ReadError("ordinary connection failure")
            return httpx.Response(200)

        with patch.object(first_reply, "time", clock):
            guard = FirstReplyGuard("test", seconds=.5)
            with self.assertRaises(FirstReplyTimeout):
                await guard.wait(ProxyChain(candidates=["", ""]).run(failed))
        self.assertEqual(attempted, [""])

    async def test_eof_after_suppressed_timer_is_still_a_deadline_failure(self):
        async def source():
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                return
            yield "never"
        guard = FirstReplyGuard("test", seconds=.015)
        with self.assertRaises(FirstReplyTimeout):
            _ = [item async for item in guard.iterate(source(), bool)]

    async def test_rewritten_cancellation_does_not_masquerade_as_a_transport_failure(self):
        for external in (True, False):
            with self.subTest(external=external):
                started = asyncio.Event()
                attempted = []
                async def read(raw, _resolved):
                    attempted.append(raw)
                    started.set()
                    if len(attempted) > 1:
                        return httpx.Response(200)
                    try:
                        await asyncio.Event().wait()
                    except asyncio.CancelledError as exc:
                        raise httpx.ReadError("cancelled read") from exc
                guard = FirstReplyGuard("test", seconds=1 if external else .015)
                task = asyncio.create_task(guard.wait(ProxyChain(candidates=["", ""]).run(read)))
                await started.wait()
                if external:
                    task.cancel()
                with self.assertRaises(asyncio.CancelledError if external else FirstReplyTimeout):
                    await task
                self.assertEqual(attempted, [""])

    async def test_owner_cancel_during_rejected_response_close_preserves_cleanup(self):
        closing, release = asyncio.Event(), asyncio.Event()
        calls = []
        async def close_response():
            calls.append("started")
            closing.set()
            await release.wait()
            calls.append("closed")

        resource = SimpleNamespace(aclose=close_response)
        async def operation():
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                return resource  # simulates a response returning after the timer

        guard = FirstReplyGuard("test", seconds=.015)
        owner = asyncio.create_task(guard.wait(operation()))
        try:
            await asyncio.wait_for(closing.wait(), 1)
            for _ in range(2):
                owner.cancel()
                await asyncio.sleep(0)
            release.set()
            # A stuck wait fails the test, and cancellation remains observable.
            done, _ = await asyncio.wait({owner}, timeout=1)
            self.assertIn(owner, done)
            with self.assertRaises(asyncio.CancelledError):
                owner.result()
            self.assertEqual(calls, ["started", "closed"])
        finally:
            release.set()
            if not owner.done():
                owner.cancel()
            await asyncio.gather(owner, return_exceptions=True)

    async def test_anyio_scope_cancel_is_observed_after_discarding_an_expired_result(self):
        closing, release, closed = asyncio.Event(), asyncio.Event(), asyncio.Event()
        loop = asyncio.get_running_loop()
        ready = loop.create_future()
        response = loop.create_future()

        async def close_response():
            closing.set()
            await release.wait()
            closed.set()

        response.set_result(SimpleNamespace(aclose=close_response))
        guard = FirstReplyGuard("test", seconds=1)
        guard.deadline = guard.started - 1

        async def run_scoped():
            with anyio.CancelScope() as scope:
                ready.set_result(scope)
                await guard.wait(response)
            return scope.cancelled_caught

        task = asyncio.create_task(run_scoped())
        try:
            scope = await asyncio.wait_for(ready, 1)
            await asyncio.wait_for(closing.wait(), 1)
            scope.cancel()
            await asyncio.sleep(0)
            release.set()
            self.assertTrue(await asyncio.wait_for(task, 1))
            self.assertTrue(closed.is_set())
        finally:
            release.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_eager_close_failure_is_consumed_even_when_scope_is_cancelled(self):
        consumed = []

        class FailedClose(asyncio.Future):
            def result(self):
                consumed.append(True)
                return super().result()

        closing = FailedClose()
        closing.set_exception(OSError("local cleanup failure"))
        response = asyncio.get_running_loop().create_future()
        response.set_result(SimpleNamespace(aclose=lambda: closing))
        guard = FirstReplyGuard("test", seconds=1)
        guard.deadline = guard.started - 1

        try:
            with anyio.CancelScope() as scope:
                scope.cancel()
                await guard.wait(response)
            self.assertTrue(scope.cancelled_caught)
            self.assertEqual(consumed, [True])
        finally:
            closing.exception()

    async def test_external_cancel_wins_and_cleans_a_late_owned_resource(self):
        started = asyncio.Event()
        resource = SimpleNamespace(aclose=AsyncMock())
        async def stubborn():
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                return resource
        guard = FirstReplyGuard("test", seconds=10)
        task = asyncio.create_task(guard.wait(stubborn()))
        await started.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        resource.aclose.assert_awaited_once()
