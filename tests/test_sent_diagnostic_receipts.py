"""Confirmed byte and output milestones follow the same ASGI evidence as receipts."""
import json
import unittest
from unittest.mock import patch

from fastapi.responses import JSONResponse
from request_tracking import RequestTrace, request_registry
from routes import chat_api
from test_replay_http_delivery import Peer


class SentDiagnosticReceiptTests(unittest.IsolatedAsyncioTestCase):
    async def test_observed_or_ambiguous_loss_does_not_claim_content_was_sent(self):
        payload = {"choices": [{"message": {"role": "assistant", "content": "visible text"}, "finish_reason": "stop"}]}
        body = JSONResponse(payload).body
        for mode in ("live", "headers-drop", "final-drop", "raised"):
            with self.subTest(mode=mode):
                peer = Peer("/v1/chat/completions", payload={"model": "model", "messages": []},
                            headers=[(b"x-resumable", b"false")])
                async def send(message):
                    if mode == "headers-drop" and message["type"] == "http.response.start":
                        await peer.drop()
                        await peer.scope["state"]["client_connection"].event.wait()
                    elif mode == "final-drop" and message["type"] == "http.response.body":
                        await peer.drop()
                    elif mode == "raised" and message["type"] == "http.response.body":
                        raise OSError("send failed")

                with patch.object(chat_api, "_call_upstream", return_value=JSONResponse(payload)):
                    if mode == "raised":
                        with self.assertRaises(OSError):
                            await peer.run(send)
                    else:
                        await peer.run(send)
                messages = peer.messages + peer.discarded
                rid = dict(messages[0]["headers"])[b"x-trace-id"].decode()
                trace = request_registry.get(rid)
                confirmed = mode == "live"
                self.assertEqual(trace.data["sent_bytes"], len(body) if confirmed else 0)
                self.assertEqual(trace.data["sent_content_chars"], len("visible text") if confirmed else 0)
                self.assertEqual("first_sent_content" in trace.timings, confirmed)
                self.assertEqual(peer.scope["state"]["client_connection"].delivery_possible, confirmed)
                if confirmed:
                    self.assertEqual(json.loads(messages[-1]["body"]), payload)

    def test_connection_end_only_advertises_recovery_for_a_real_buffer(self):
        for buffered, producer in ((False, True), (True, True), (True, False), (False, False)):
            with self.subTest(buffered=buffered, producer=producer):
                trace = RequestTrace("/v1/chat/completions")
                if buffered:
                    trace.configure(generation_id="rid-test", detached=producer)
                if producer:
                    trace.start_generation()
                trace.end_connection(disconnected=True)
                event = trace.events[-1]
                self.assertEqual(event["kind"], "connection_end")
                self.assertEqual("续传" in event["message"], buffered)
                self.assertEqual("后台上游" in event["message"], buffered and producer)
