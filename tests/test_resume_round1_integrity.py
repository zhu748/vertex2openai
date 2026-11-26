"""Offline HTTP regressions for request identity and cursor-safe replay."""
import threading
import unittest
from unittest import mock

import httpx
from fastapi.responses import JSONResponse

from main import app
from resumable import ResumableStore
from routes import chat_api


class ResumeIntegrityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = ResumableStore()
        self.addAsyncCleanup(self.store.aclose)
        self.enterContext(mock.patch.object(chat_api, "resumable_store", self.store))
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test",
                                        headers={"Authorization": "Bearer test-key"})
        self.addAsyncCleanup(self.client.aclose)

    def test_inline_and_streaming_encoders_select_the_same_canonical_generation(self):
        from models import OpenAIRequest
        request = OpenAIRequest(model="m", messages=[{"role": "user", "content": '"你好" 🐈\n'}],
                                metadata={"z": ["a", 0.5, True, None], "a": {"Z": 1}})
        self.assertEqual(chat_api._request_fingerprint(request, small=True),
                         chat_api._request_fingerprint(request))

    async def test_long_prompt_middle_edit_is_distinct_and_exact_retry_replays_offloop(self):
        model = "gemini-test"
        headers = {"Idempotency-Key": "same-thread"}
        async def generate(request, _client):
            return JSONResponse({"answer": request.messages[0].content[40_000]})
        async def send(middle):
            return await self.client.post("/v1/chat/completions", headers=headers, json={
                "model": model, "messages": [{"role": "user",
                                              "content": "文" * 40_000 + middle + "B" * 40_000}]})

        main_thread = threading.get_ident()
        workers = []
        fingerprint = chat_api._request_fingerprint
        def observe(request):
            workers.append(threading.get_ident())
            return fingerprint(request)
        with mock.patch.object(chat_api, "_call_upstream", side_effect=generate) as upstream, \
             mock.patch.object(chat_api, "_request_fingerprint", side_effect=observe), \
             mock.patch.object(chat_api, "resumable_enabled", return_value=True):
            first, other, replay = await send("一"), await send("二"), await send("一")

        self.assertEqual((first.status_code, other.status_code, replay.status_code), (200, 200, 200))
        self.assertEqual([r.json()["answer"] for r in (first, other, replay)], ["一", "二", "一"])
        self.assertNotEqual(first.headers["x-resume-id"], other.headers["x-resume-id"])
        self.assertEqual(first.headers["x-resume-id"], replay.headers["x-resume-id"])
        self.assertEqual(upstream.await_count, 2)
        self.assertTrue(workers)
        self.assertNotIn(main_thread, workers)

    async def test_explicit_reconnect_never_becomes_a_fresh_generation(self):
        from routes import gemini_native
        from fastapi.responses import StreamingResponse

        # No live Express call: ASGI routes exercise the real delivery-mode selection.
        async def direct(*args):
            async def frames():
                yield 'data: {"ok":true}\n\n'
            return StreamingResponse(frames(), media_type="text/event-stream")

        self.enterContext(mock.patch.object(app.state.express_key_manager, "get_total_keys", return_value=1))
        for route, path, body, dispatch in (
            (chat_api, "/v1/chat/completions", {"model": "m", "messages": [], "stream": True}, "_call_upstream"),
            (gemini_native, "/v1beta/models/m:streamGenerateContent?alt=sse", {"contents": []}, "_passthrough_request"),
        ):
            for enabled, bypass in ((False, False), (True, True)):
                with self.subTest(route=route.__name__, enabled=enabled, bypass=bypass), \
                     mock.patch.object(route, "resumable_enabled", return_value=enabled), \
                     mock.patch.object(route, "image_bypass_enabled", return_value=bypass), \
                     mock.patch.object(route, dispatch, side_effect=direct) as upstream:
                    for headers, query in (({"Last-Event-ID": "0"}, ""), ({"X-Resume-Id": "rid-old"}, ""),
                                           ({}, "resume_from=0")):
                        url = path + (("&" if "?" in path else "?") + query if query else "")
                        failed = await self.client.post(url, json=body, headers=headers)
                        self.assertEqual(failed.status_code, 409, failed.text)
                        self.assertIn("error", failed.json())
                    contradictory = await self.client.post(path, json=body, headers={
                        "X-Resumable": "false", "Last-Event-ID": "0"})
                    self.assertEqual(contradictory.status_code, 400)
                    upstream.assert_not_awaited()
                    # Opt-in without any ID/cursor asks for a fresh run, not a replay.
                    fresh = await self.client.post(path, json=body, headers={"X-Resumable": "true"})
                    self.assertEqual(fresh.status_code, 200, fresh.text)
                    upstream.assert_awaited_once()

    async def test_native_http_resume_retains_named_events_multiline_errors_and_only_server_ids(self):
        from fastapi.responses import StreamingResponse
        from routes import gemini_native
        from upstreams import native_passthrough

        async def upstream(*args):
            async def frames():
                yield b": waiting\r\nretry: 1200\r\n\r\n"
                yield (': a comment\r\nevent: message\r\ndata: {"text":"a\u2028b"}\r\n'
                       'id: upstream-after-data\r\n\r\n').encode()
                yield (b"id: upstream-before\nevent: error\ndata: {\"error\":\n"
                       b"data:  {\"code\":503,\"message\":\"try later\"}}\n"
                       b": diagnostic\nid: upstream-after\n\n")
            return StreamingResponse(frames(), media_type="text/event-stream")

        path = "/v1beta/models/gemini-test:streamGenerateContent?alt=sse"
        body = {"contents": []}
        with mock.patch.object(app.state.express_key_manager, "get_total_keys", return_value=1), \
             mock.patch.object(gemini_native, "resumable_enabled", return_value=True), \
             mock.patch.object(native_passthrough, "resumable_store", self.store), \
             mock.patch.object(native_passthrough, "_passthrough_request", side_effect=upstream) as dispatch:
            first = await self.client.post(path, json=body, headers={"X-Resumable": "true"})
            self.assertEqual(first.status_code, 200, first.text)
            rid = first.headers["x-resume-id"]
            self.assertIn(f"id: {rid}:0\n: a comment", first.text)
            self.assertIn('data: {"text":"a\u2028b"}', first.text)
            self.assertNotIn("upstream-", first.text)
            self.assertNotIn("waiting", first.text)
            self.assertEqual(first.text.count(f"id: {rid}:"), 2)
            self.assertNotIn("[DONE]", first.text)

            replay = await self.client.post(path, json=body, headers={"Last-Event-ID": f"{rid}:0"})
            self.assertIn(f"id: {rid}:1\nevent: error", replay.text)
            self.assertNotIn("event: message", replay.text)
            poll = await self.client.get(f"/v1/resumable/{rid}")
            self.assertEqual(poll.status_code, 200, poll.text)
            record = poll.json()
            self.assertEqual(record["status"], "error")
            self.assertEqual([e["seq"] for e in record["events"]], [0, 1])
            self.assertEqual((await self.store.get_exact(rid)).status_code, 503)
            dispatch.assert_awaited_once()

    async def test_native_leading_blanks_keep_receipts_with_their_data_in_live_replay_and_poll(self):
        from routes import gemini_native
        from upstreams import native_passthrough

        first = 'event: message\r\ndata: {"candidates":[{"content":{"parts":[{"text":"ok"}]}}]}\r'
        last = 'data: {"candidates":[{"finishReason":"STOP"}]}\n\r\n'
        wire = ('\r\n' + first + 'id: upstream-one\n\n' + '\n' + 'id: upstream-two\r' + last).encode()
        handler = mock.Mock(side_effect=lambda _: httpx.Response(
            200, content=wire, headers={"content-type": "text/event-stream"}))
        upstream = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        self.addAsyncCleanup(upstream.aclose)
        manager = app.state.express_key_manager
        self.enterContext(mock.patch.object(manager, "get_total_keys", return_value=1))
        self.enterContext(mock.patch.object(manager, "get_express_record", return_value={"key": "test", "project_id": ""}))
        self.enterContext(mock.patch.object(native_passthrough.passthrough_client_pool, "get", return_value=upstream))
        self.enterContext(mock.patch.object(gemini_native, "resumable_enabled", return_value=True))
        self.enterContext(mock.patch.object(native_passthrough, "resumable_store", self.store))
        path = "/v1beta/models/gemini-test:streamGenerateContent?alt=sse"
        body = {"contents": [{"parts": [{"text": "hi"}]}]}

        direct = await self.client.post(path, json=body, headers={"X-Resumable": "0"})
        self.assertEqual(direct.status_code, 200)
        self.assertIn(wire, direct.content)
        live = await self.client.post(path, json=body, headers={"X-Resumable": "1"})
        self.assertEqual(live.status_code, 200)
        rid = live.headers["x-resume-id"]
        expected = [f"\r\nid: {rid}:0\n" + first + "\n\n", f"\nid: {rid}:1\n" + last]
        for frame in expected:
            self.assertIn(frame, live.text)
        self.assertNotIn("upstream-", live.text)
        self.assertNotIn('"error"', live.text)
        replay = await self.client.post(path, json=body, headers={"Last-Event-ID": f"{rid}:0"})
        self.assertIn(expected[1], replay.text)
        self.assertNotIn(expected[0], replay.text)
        poll = await self.client.get(f"/v1/resumable/{rid}")
        self.assertEqual(poll.status_code, 200)
        self.assertEqual(poll.json()["status"], "done")
        self.assertEqual(poll.json()["events"], [{"seq": i, "data": frame} for i, frame in enumerate(expected)])
        self.assertEqual(handler.call_count, 2)

    async def test_openai_resume_keeps_data_before_or_after_control_fields(self):
        import resumable

        buf = resumable.ResumableBuffer("rid-example")
        async def frames():
            yield ": just a heartbeat\n\n"
            yield ": preamble\nevent: message\ndata: {\"choices\":[]}\nid: legacy\n\n"
            yield 'event: failure\ndata: {"error":\ndata: {"message":"busy","code":503}}\n\n'
            yield "data: [DONE]\n: trailing comment\n\n"
        await resumable.pump_stream(buf, frames())
        self.assertEqual((buf.status, buf.status_code, buf.next_seq), ("error", 503, 3))
        self.assertEqual(sum(f.count("id: rid-example:") for f in buf.events), 3)
        self.assertNotIn("legacy", buf.events[0])
        self.assertIn(": trailing comment", buf.events[-1])
