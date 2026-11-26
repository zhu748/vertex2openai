"""Responses metrics are about public items and confirmed ASGI delivery."""
import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch

from fastapi.responses import JSONResponse
from starlette.requests import ClientDisconnect

from request_tracking import PayloadObserver, RequestTrace, request_registry
from routes import responses_api
from test_replay_http_delivery import Peer
from test_responses_api import PATH, assistant, completion, event_list, request
from test_schema_contract_fidelity import SchemaRouteHarness


class ResponsesPublicDeliveryTests(SchemaRouteHarness):
    async def test_json_and_sse_track_the_same_public_items_on_both_transports(self):
        for self.channel in ("api_key", "web_proxy"):
            for stream in (False, True):
                for text, thought, tool in (("Ready ✈", "", False), ("Ready ✈", "Review ✓", False), ("", "", True)):
                    with self.subTest(channel=self.channel, stream=stream, text=text, thought=thought, tool=tool):
                        self.reply = ([{"text": thought, "thought": True}] if thought else [])
                        self.reply += ([{"text": text}] if text else [])
                        if tool:
                            self.reply.append(assistant("count-colors-only", b"opaque signature must not count"))
                        response = await self.client.post(PATH, json=request(stream=stream))
                        self.assertEqual(response.status_code, 200)
                        if stream:
                            events = event_list(response.text)
                            self.assertEqual(events[-1]["type"], "response.completed")
                        trace = request_registry.get(response.headers["x-trace-id"]).snapshot(detail=True)
                        self.assertEqual(trace["sent_content_chars"], len(text))
                        self.assertEqual(trace["sent_reasoning_chars"], len(thought))
                        self.assertIn("first_sent_reply", trace["timings"])
                        self.assertEqual("first_sent_content" in trace["timings"], bool(text))
                        if text:
                            self.assertLessEqual(trace["timings"]["first_sent_reply"]["elapsed_ms"],
                                                 trace["timings"]["first_sent_content"]["elapsed_ms"])
                        if stream:
                            self.assertEqual(trace["sent_frames"], len(events))
                        self.assertEqual(trace["sent_bytes"], len(response.content))
                        self.assertEqual([e["kind"] for e in trace["events"]].count("first_sent_reply"), 1)

    async def test_disabled_diagnostics_still_count_frames_and_bytes(self):
        self.settings["request_diagnostics"] = False
        self.reply = [{"text": "Thought ✓", "thought": True}, {"text": "Answer ✓"}]
        for stream in (False, True):
            with self.subTest(stream=stream):
                response = await self.client.post(PATH, json=request(stream=stream))
                self.assertEqual(response.status_code, 200)
                trace = request_registry.get(response.headers["x-trace-id"]).snapshot()
                self.assertEqual(trace["sent_content_chars"], 0)
                self.assertEqual(trace["sent_reasoning_chars"], 0)
                self.assertNotIn("first_sent_reply", trace["timings"])
                self.assertNotIn("first_sent_content", trace["timings"])
                self.assertEqual(trace["sent_bytes"], len(response.content))
                self.assertEqual(trace["sent_frames"], len(event_list(response.text)) if stream else 0)


class ResponsesSentObserverTests(unittest.IsolatedAsyncioTestCase):
    def test_control_frames_and_done_snapshots_are_not_first_sent_content(self):
        trace = RequestTrace(PATH)
        observer = PayloadObserver(trace, sent=True)
        text = "你好 🌈"
        summary = "摘要"
        item = {"type": "message", "role": "assistant",
                "content": [{"type": "output_text", "text": text}]}
        response = {"object": "response", "output": [item]}
        scaffolds = (
            {"type": "response.created", "response": response},
            {"type": "response.in_progress", "response": response},
            {"type": "response.content_part.added", "part": item["content"][0]},
            {"type": "response.output_text.done", "text": text},
            {"type": "response.output_item.done", "item": item},
            {"type": "response.completed", "response": response},
            {"type": "response.failed", "response": {"error": {"message": "hidden"}}},
            {"type": "response.output_item.added", "item": {"type": "function_call", "name": "  "}},
            {"type": "response.function_call_arguments.delta", "delta": "  "},
            {"type": "response.function_call_arguments.done", "arguments": '{"name":"catalog"}'},
            {"type": "response.output_text.delta", "delta": ["not text"]},
        )
        observer.feed(b": keepalive\n\n")
        for value in scaffolds:
            observer.feed(f"data: {json.dumps(value)}\n\n")
        self.assertNotIn("first_sent_reply", trace.timings)
        self.assertNotIn("first_sent_content", trace.timings)
        self.assertEqual(trace.data["sent_content_chars"], 0)

        observer.feed(f'data: {json.dumps({"type": "response.reasoning_summary_text.delta", "delta": summary})}\n\n')
        self.assertIn("first_sent_reply", trace.timings)
        self.assertNotIn("first_sent_content", trace.timings)
        self.assertEqual(trace.data["sent_reasoning_chars"], len(summary))
        delta = {"type": "response.output_text.delta", "delta": text}
        encoded = f"event: {delta['type']}\ndata: {json.dumps(delta, ensure_ascii=False)}\n\n".encode()
        split = encoded.index("你".encode()) + 1
        observer.feed(encoded[:split])
        self.assertNotIn("first_sent_content", trace.timings)
        observer.feed(encoded[split:])
        self.assertIn("first_sent_content", trace.timings)
        self.assertEqual(trace.data["sent_content_chars"], len(text))
        self.assertNotIn("first_reply", trace.timings)
        self.assertNotIn(text, repr(trace.snapshot(detail=True)))

    async def test_refusal_counts_when_delivered_and_lost_writes_do_not_count(self):
        text, thought = "No, I cannot.", "Review ✓"
        upstream = JSONResponse(completion({"refusal": text, "reasoning_content": thought}))
        with patch.object(responses_api, "call_upstream", AsyncMock(return_value=upstream)):
            for stream in (False, True):
                for loss in (None, "dropped", "raised"):
                    with self.subTest(stream=stream, loss=loss):
                        peer = Peer(PATH, payload=request(stream=stream))
                        hit = False

                        async def send(message):
                            nonlocal hit
                            body = message.get("body", b"")
                            marker = b'event: response.refusal.delta' if stream else b'"type":"refusal"'
                            if marker not in body:
                                return
                            hit = True
                            if loss == "dropped":
                                await peer.drop()
                            elif loss == "raised":
                                raise OSError("The test peer lost the response write")

                        if loss == "raised":
                            with self.assertRaises(ClientDisconnect if stream else OSError):
                                await asyncio.wait_for(peer.run(send), 2)
                        else:
                            await asyncio.wait_for(peer.run(send), 2)
                        self.assertTrue(hit)
                        rid = dict(peer.messages[0]["headers"])[b"x-trace-id"].decode()
                        trace = request_registry.get(rid).snapshot()
                        self.assertEqual(trace["sent_content_chars"], len(text) if loss is None else 0)
                        # A refused SSE write happens after the earlier reasoning delta;
                        # rejecting one JSON write is evidence for none of its parts.
                        reasoning_sent = stream or loss is None
                        self.assertEqual(trace["sent_reasoning_chars"], len(thought) if reasoning_sent else 0)
                        self.assertEqual("first_sent_reply" in trace["timings"], reasoning_sent)
                        self.assertEqual("first_sent_content" in trace["timings"], loss is None)
