"""Offline Responses API contracts, including the real Gemini wire adapters."""

import asyncio
import base64
import json
import unittest
from unittest.mock import AsyncMock, Mock, patch

from fastapi.responses import JSONResponse, Response
import httpx
from starlette.requests import ClientDisconnect

import config
import middleware
from admission import ManagedStreamingResponse, generation_limiter
from auth import get_api_key
from main import app
from request_ingress import ingress_budget
from routes import chat_api, responses_api
from test_schema_contract_fidelity import SchemaRouteHarness


PATH = "/v1/responses"
MODEL = "gemini-3-flash-preview"
TOOLS = [{"type": "function", "name": "count_colors", "parameters": {"type": "object"}}]
SIGNATURES = (b"\x00\xfb\xff-first-original", b"\x05-second-original")
AUTH = {"Authorization": "Bearer test-key"}


def event_list(text):
    result = []
    for frame in text.split("\n\n"):
        if not frame or frame.startswith(":"):
            continue
        header, body = frame.split("\n", 1)
        assert header.startswith("event: ") and body.startswith("data: ")
        payload = json.loads(body[6:])
        assert header[7:] == payload["type"]
        result.append(payload)
    assert [e["sequence_number"] for e in result] == list(range(len(result)))
    return result


def request(**kwargs):
    return {"model": MODEL, "store": False, "input": "Count the colors.", "tools": TOOLS, **kwargs}


def assistant(ident, signature=None, *, shade="blue"):
    raw = {"functionCall": {"id": ident, "name": "count_colors", "args": {"shade": shade}}}
    if signature is not None:
        raw["thoughtSignature"] = base64.b64encode(signature).decode()
    return raw


def calls_in_wire(sent, field):
    return [part for turn in sent.get("variables", sent)["contents"] for part in turn["parts"] if field in part]


def signature(part):
    value = part.get("thoughtSignature")
    return base64.urlsafe_b64decode(value) if value is not None else None


def thin_item(item):
    return {k: v for k, v in item.items() if k != "extra_content"}


def continuation(prompt, output, *, reverse=False):
    calls = [item for item in output if item["type"] == "function_call"]
    return [{"role": "user", "content": prompt}, *output, *(
        {"type": "function_call_output", "call_id": call["call_id"], "output": f"result {call['call_id']}"}
        for call in (reversed(calls) if reverse else calls)
    )]


def responses_asgi(body, send, disconnected):
    body = json.dumps(body).encode()
    first = True

    async def receive():
        nonlocal first
        if first:
            first = False
            return {"type": "http.request", "body": body, "more_body": False}
        await disconnected.wait()
        return {"type": "http.disconnect"}

    return app({"type": "http", "method": "POST", "path": PATH, "raw_path": PATH.encode(), "root_path": "",
                "headers": [(b"authorization", b"Bearer test-key"), (b"content-type", b"application/json")],
                "scheme": "http", "query_string": b"", "asgi": {"spec_version": "2.4"},
                "client": ("offline", 0), "server": ("offline", 0)}, receive, send)


class ResponsesWireTests(SchemaRouteHarness):
    async def test_text_and_real_tool_roundtrip_over_express_and_cookie(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for stream in (False, True):
                for stripped in (False, True):
                    with self.subTest(channel=channel, stream=stream, stripped=stripped):
                        label = f"{channel}/{stream}/{stripped}"
                        prompt = f"Count the colors ({label})."
                        ids = (f"opaque:{label}", f"literal:{label}__thought__YQ==")
                        # Metadata kept by the full client and a plain visible prefix
                        # reconstructed by the thin one should both bind the originals.
                        self.reply = [{"text": "公开说明 ☁"}, assistant(ids[0], SIGNATURES[0]),
                                      assistant(ids[1], SIGNATURES[1], shade="gold")]
                        self.sent.clear()
                        first = await self.client.post(PATH, json=request(input=prompt, stream=stream,
                                                       instructions="Count only.", max_output_tokens=64))
                        self.assertEqual(first.status_code, 200, first.text[:360])
                        if stream:
                            events = event_list(first.text)
                            self.assertEqual([e["type"] for e in events[:2]], ["response.created", "response.in_progress"])
                            self.assertEqual(events[-1]["type"], "response.completed")
                            self.assertNotIn("[DONE]", first.text)
                            self.assertEqual(first.headers["content-type"].split(";")[0], "text/event-stream")
                            envelope = events[-1]["response"]
                            self.assertEqual(events[0]["response"]["id"], envelope["id"])
                            self.assertEqual(events[0]["response"]["output"], [])
                            starts = [e for e in events if e["type"] == "response.output_item.added"]
                            ends = [e for e in events if e["type"] == "response.output_item.done"]
                            self.assertEqual([e["output_index"] for e in starts], [0, 1, 2])
                            self.assertEqual([e["item"] for e in ends], envelope["output"])
                            self.assertEqual([e["item"]["id"] for e in starts], [e["item"]["id"] for e in ends])
                            for done in [e for e in events if e["type"] == "response.function_call_arguments.done"]:
                                deltas = [e["delta"] for e in events if e["type"] == "response.function_call_arguments.delta"
                                          and e["item_id"] == done["item_id"]]
                                self.assertEqual("".join(deltas), done["arguments"])
                            self.assertEqual("".join(e["delta"] for e in events if e["type"] == "response.output_text.delta"),
                                             "公开说明 ☁")
                        else:
                            envelope = first.json()
                        self.assertEqual(envelope["object"], "response")
                        self.assertEqual(envelope["status"], "completed")
                        self.assertFalse(envelope["store"])
                        self.assertFalse(envelope["tools"][0]["strict"])
                        self.assertNotIn("x-resume-id", first.headers)
                        self.assertEqual(len(self.sent), 1)
                        wire = self.sent[0].get("variables", self.sent[0])
                        gen = wire["generationConfig"]
                        self.assertEqual(gen["maxOutputTokens"], 64)
                        self.assertIn("Count only.", json.dumps(wire))
                        self.assertEqual([i["type"] for i in envelope["output"]], ["message", "function_call", "function_call"])
                        items = envelope["output"]
                        for call, call_id, signed in zip(items[1:], ids, SIGNATURES, strict=True):
                            self.assertEqual(call["call_id"], call_id)
                            self.assertTrue(call["id"].startswith("fc_"))
                            self.assertNotEqual(call["id"], call_id)
                            self.assertEqual(base64.b64decode(call["extra_content"]["google"]["thought_signature"]), signed)
                        if stripped:
                            items = [thin_item(i) for i in items]
                        self.reply = {"text": "Finished."}
                        just = await self.client.post(PATH, json=request(
                            instructions="Count only.", input=continuation(prompt, items, reverse=True)))
                        self.assertEqual(just.status_code, 200, just.text[:400])
                        self.assertEqual(just.json()["output"][0]["content"][0]["text"], "Finished.")
                        self.assertEqual(len(self.sent), 2)
                        source = calls_in_wire(self.sent[1], "functionCall")
                        results = calls_in_wire(self.sent[1], "functionResponse")
                        self.assertEqual([p["functionCall"]["id"] for p in source], list(ids))
                        self.assertEqual([signature(p) for p in source], list(SIGNATURES))
                        self.assertEqual([p["functionResponse"]["id"] for p in results], list(ids))
                        self.assertEqual([p["functionResponse"]["name"] for p in results], ["count_colors", "count_colors"])
                        self.assertTrue(all(signature(part) is None for part in results))

    async def test_unproven_ids_do_not_acquire_another_turns_signature_or_a_placeholder(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            with self.subTest(channel=channel):
                self.sent.clear()
                prompt = f"original prompt ({channel})"
                old_id = f"plain-{channel}"
                self.reply = [assistant(old_id, SIGNATURES[0])]
                first = await self.client.post(PATH, json=request(input=prompt))
                self.assertEqual(first.status_code, 200, first.text[:360])
                original, = first.json()["output"]
                self.reply = {"text": "done"}
                for call_id in (old_id, f"new-{channel}", f"new-{channel}__thought__YQ=="):
                    item = {**thin_item(original), "call_id": call_id}
                    wrong = await self.client.post(PATH, json=request(input=continuation("edited prompt", [item])))
                    self.assertEqual(wrong.status_code, 200, wrong.text[:360])
                    part, = calls_in_wire(self.sent[-1], "functionCall")
                    self.assertEqual(part["functionCall"]["id"], call_id)
                    self.assertIsNone(signature(part))

                # A different verified gateway key has no claim on the first
                # client's matching history and opaque ID, even in one process.
                previous = dict(app.dependency_overrides)
                app.dependency_overrides[get_api_key] = lambda: "another-test-identity"
                try:
                    unknown = await self.client.post(PATH, json=request(input=continuation(prompt, [thin_item(original)])))
                    self.assertEqual(unknown.status_code, 200, unknown.text[:360])
                    part, = calls_in_wire(self.sent[-1], "functionCall")
                    self.assertIsNone(signature(part))
                finally:
                    app.dependency_overrides.clear()
                    app.dependency_overrides.update(previous)

                # Optional strict mode remains strict without changing the default.
                with patch.object(config, "GEMINI_STRICT_THOUGHT_SIGNATURES", True):
                    old_size = len(self.sent)
                    denied = await self.client.post(PATH, json=request(input=continuation("wrong prefix", [thin_item(original)])))
                    self.assertEqual(denied.status_code, 400, denied.text[:360])
                    self.assertIn("signature", denied.text)
                    self.assertEqual(len(self.sent), old_size)

    async def test_structured_text_results_keep_boundaries_order_and_later_thin_origins(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for stream in (False, True):
                with self.subTest(channel=channel, stream=stream):
                    label = f"structured/{channel}/{stream}"
                    prompt = [{"type": "message", "role": "user", "content": [
                        {"type": "input_text", "text": f"Count the colors ({label})."}]}]
                    first_id, second_id = f"first:{label}", f"second:{label}__thought__YQ=="
                    self.reply = [assistant(first_id, SIGNATURES[0]), assistant(second_id, SIGNATURES[1], shade="gold")]
                    started = await self.client.post(PATH, json=request(input=prompt, stream=stream))
                    self.assertEqual(started.status_code, 200, started.text[:360])
                    source = event_list(started.text)[-1]["response"] if stream else started.json()
                    thin = [thin_item(item) for item in source["output"]]

                    red_blocks = [" red 🌈\n", "", "![literal](data:image/png;base64,not-an-attachment)"]
                    gold_blocks = ["gold", "amber"]
                    def result(call_id, blocks):
                        return {"type": "function_call_output", "call_id": call_id, "output": [
                            {"type": "input_text", "text": value} for value in blocks]}

                    # Race-like reverse arrival is still correlated by the opaque call IDs.
                    results = [result(second_id, gold_blocks), result(first_id, red_blocks)]
                    history = [*prompt, *thin, *results]
                    newest_id = f"next:{label}"
                    newest_signature = b"\x00\x98\xfe-next"
                    self.reply = assistant(newest_id, newest_signature)
                    continued = await self.client.post(PATH, json=request(input=history, stream=stream))
                    self.assertEqual(continued.status_code, 200, continued.text[:360])
                    wire = self.sent[-1]
                    calls = calls_in_wire(wire, "functionCall")
                    self.assertEqual([part["functionCall"]["id"] for part in calls], [first_id, second_id])
                    self.assertEqual([signature(part) for part in calls], list(SIGNATURES))
                    replies = calls_in_wire(wire, "functionResponse")
                    self.assertEqual([part["functionResponse"]["id"] for part in replies], [first_id, second_id])
                    self.assertTrue(all("thoughtSignature" not in part and "parts" not in part["functionResponse"]
                                        for part in replies))
                    self.assertEqual([part["functionResponse"]["response"] for part in replies],
                                     [{"result": [{"type": "text", "text": value} for value in blocks]}
                                      for blocks in (red_blocks, gold_blocks)])

                    # Structured history also participates in the proof for later tool turns.
                    newest = event_list(continued.text)[-1]["response"] if stream else continued.json()
                    tail = [thin_item(newest["output"][0]), result(newest_id, [])]
                    self.reply = {"text": "done"}
                    for matching in (True, False):
                        changed = history if matching else [*prompt, *thin, results[0],
                                                           result(first_id, list(reversed(red_blocks)))]
                        done = await self.client.post(PATH, json=request(input=[*changed, *tail]))
                        self.assertEqual(done.status_code, 200, done.text[:360])
                        last_call = calls_in_wire(self.sent[-1], "functionCall")[-1]
                        self.assertEqual(last_call["functionCall"]["id"], newest_id)
                        self.assertEqual(signature(last_call), newest_signature if matching else None)
                        last_reply = calls_in_wire(self.sent[-1], "functionResponse")[-1]
                        self.assertEqual(last_reply["functionResponse"]["response"], {"result": []})

    async def test_signed_ordinary_tool_only_part_is_carried_without_a_fake_output_message(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            with self.subTest(channel=channel):
                self.sent.clear()
                opaque = f"opaque-signature-only-{channel}"
                prompt = "Collect a tool-only turn."
                self.reply = [
                    {"thoughtSignature": base64.b64encode(SIGNATURES[1]).decode()},
                    assistant(opaque, SIGNATURES[0]),
                ]
                response = await self.client.post(PATH, json=request(input=prompt))
                self.assertEqual(response.status_code, 200, response.text[:360])
                call, = response.json()["output"]
                self.assertEqual(call["type"], "function_call")
                carried = call["extra_content"]["vertex2openai"]["assistant_extra_content"]["google"]
                self.assertEqual(base64.b64decode(carried["ordinary_parts"][0]["thought_signature"]), SIGNATURES[1])
                self.reply = {"text": "done"}
                result = await self.client.post(PATH, json=request(input=continuation(prompt, [call])))
                self.assertEqual(result.status_code, 200, result.text[:380])
                wire = self.sent[-1].get("variables", self.sent[-1])
                model, = [turn for turn in wire["contents"] if turn["role"] == "model"]
                self.assertEqual([signature(p) for p in model["parts"]], [SIGNATURES[1], SIGNATURES[0]])


    async def test_delivered_unsupported_projection_fences_legacy_but_not_distinct_issued_items(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for stream in (False, True):
                for newer_result in ("signed_empty_tail", "signed_hidden_tail", "not_accepted", "not_delivered"):
                    with self.subTest(channel=channel, stream=stream, newer_result=newer_result):
                        label = f"{channel}:{stream}:{newer_result}"
                        prompt = f"count for {label}"
                        first_id, second_id = f"first:{label}", f"second:{label}"
                        body = request(input=prompt, stream=stream)
                        self.sent.clear()
                        self.reply = [{"text": "Checking colors."},
                                      assistant(first_id, b"first A"), assistant(second_id, b"second A", shade="red")]
                        accepted_a = await self.client.post(PATH, json=body)
                        self.assertEqual(accepted_a.status_code, 200, accepted_a.text[:220])
                        output_a = (event_list(accepted_a.text)[-1]["response"] if stream else accepted_a.json())["output"]
                        thin_a = [thin_item(i) for i in output_a]
                        echoed = thin_a

                        # Same opaque IDs but different visible prefix must not
                        # share the negative proof either.
                        other_prompt = f"different prefix for {label}"
                        other = await self.client.post(PATH, json={**body, "input": other_prompt})
                        self.assertEqual(other.status_code, 200, other.text[:220])
                        other_output = (event_list(other.text)[-1]["response"] if stream else other.json())["output"]
                        thin_other = [thin_item(i) for i in other_output]
                        self.reply = {"text": "done"}
                        before = await self.client.post(PATH, json=request(input=continuation(prompt, thin_a)))
                        self.assertEqual(before.status_code, 200, before.text[:220])
                        self.assertEqual([signature(p) for p in calls_in_wire(self.sent[-1], "functionCall")],
                                         [b"first A", b"second A"])

                        huge = base64.b64encode(b"B" * 300_000).decode()
                        tail = {"thoughtSignature": huge,
                                **({"thought": True} if newer_result == "signed_hidden_tail" else {"text": ""})}
                        self.reply = [{"text": "Checking colors."},
                                      assistant(first_id, b"first B"), assistant(second_id, b"second B", shade="red"), tail]
                        if newer_result == "not_accepted":
                            self.reply[1]["thoughtSignature"] = huge  # The function item itself is rejected.
                            rejected = await self.client.post(PATH, json=body)
                            if rejected.status_code == 200 and stream:
                                terminal = event_list(rejected.text)[-1]
                                self.assertEqual(terminal["type"], "response.failed")
                            else:
                                self.assertEqual(rejected.status_code, 502, rejected.text[:220])
                        elif newer_result == "not_delivered":
                            async def reject_final(message):
                                if message["type"] == "http.response.body" and not message.get("more_body", False):
                                    raise OSError("fixture rejected the final write")
                            with self.assertRaises((ClientDisconnect, OSError)):
                                await asyncio.wait_for(responses_asgi(body, reject_final, asyncio.Event()), 1)
                        else:
                            accepted_b = await self.client.post(PATH, json=body)
                            self.assertEqual(accepted_b.status_code, 200, accepted_b.text[:220])
                            output_b = (event_list(accepted_b.text)[-1]["response"] if stream else accepted_b.json())["output"]
                            thin_b = [thin_item(i) for i in output_b]
                            self.assertEqual(
                                [{k: v for k, v in i.items() if k != "id"} for i in thin_b],
                                [{k: v for k, v in i.items() if k != "id"} for i in thin_a])
                            echoed = thin_b

                        self.reply = {"text": "done"}
                        with_prefix = await self.client.post(PATH, json=request(input=continuation(prompt, echoed)))
                        self.assertEqual(with_prefix.status_code, 200, with_prefix.text[:220])
                        expected = [b"first A", b"second A"] if newer_result.startswith("not_") else [None, None]
                        self.assertEqual([signature(p) for p in calls_in_wire(self.sent[-1], "functionCall")], expected)

                        # Without item IDs this is still the old indistinguishable
                        # history, fenced by B. Retaining the actual A IDs binds A.
                        legacy = [{k: v for k, v in i.items() if k != "id"} for i in thin_a]
                        for past_prompt, items, signatures in (
                            (prompt, legacy, expected),
                            (prompt, thin_a, [b"first A", b"second A"]),
                            (other_prompt, thin_other, [b"first A", b"second A"]),
                            (other_prompt, echoed, [None, None]),
                        ):
                            follow = await self.client.post(PATH, json=request(input=continuation(past_prompt, items)))
                            self.assertEqual(follow.status_code, 200, follow.text[:220])
                            self.assertEqual([signature(p) for p in calls_in_wire(self.sent[-1], "functionCall")],
                                             signatures)

    async def test_redacted_signature_bytes_never_make_a_new_positive_public_origin(self):
        for channel in ("api_key", "web_proxy"):
            self.channel = channel
            for stream in (False, True):
                with self.subTest(channel=channel, stream=stream):
                    prompt = f"Stand-alone projection {channel}/{stream}"
                    native = b"genuine call signature"
                    self.reply = [{"text": "Checking colors.",
                                   "thoughtSignature": base64.b64encode(b"X" * 300_000).decode()},
                                  assistant(f"projection:{channel}/{stream}", native)]
                    response = await self.client.post(PATH, json=request(input=prompt, stream=stream))
                    self.assertEqual(response.status_code, 200, response.text[:220])
                    envelope = event_list(response.text)[-1]["response"] if stream else response.json()
                    self.assertEqual(envelope["status"], "completed")
                    msg, call = envelope["output"]
                    self.assertEqual(msg["content"][0]["text"], "Checking colors.")
                    self.assertIn("extra_content", msg)  # Topology may be projected without the oversized bytes.
                    self.assertEqual(base64.b64decode(call["extra_content"]["google"]["thought_signature"]), native)
                    thin = [thin_item(i) for i in envelope["output"]]
                    self.reply = {"text": "done"}
                    old = await self.client.post(PATH, json=request(input=continuation(prompt, thin)))
                    self.assertEqual(old.status_code, 200, old.text[:220])
                    self.assertEqual([signature(p) for p in calls_in_wire(self.sent[-1], "functionCall")], [None])
                    full = await self.client.post(PATH, json=request(input=continuation(prompt, envelope["output"])))
                    self.assertEqual(full.status_code, 200, full.text[:220])
                    self.assertEqual([signature(p) for p in calls_in_wire(self.sent[-1], "functionCall")], [native])


def completion(message=None, finish="stop"):
    return {"choices": [{"finish_reason": finish, "message": {
        "role": "assistant", **({"content": "test 🌈"} if message is None else message)}}],
        "usage": {"prompt_tokens": 7, "completion_tokens": 5, "total_tokens": 12,
                  "prompt_tokens_details": {"cached_tokens": 2}, "completion_tokens_details": {"reasoning_tokens": 3}}}


class ResponsesBoundaryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                        base_url="http://offline", headers=AUTH)
        self.addAsyncCleanup(self.client.aclose)

    async def test_explicitly_unsupported_requests_do_not_reach_dispatch(self):
        malformed_output = {"type": "function_call_output", "call_id": "unmatched", "output": "ok"}
        body = request()
        for bad in (
            {"store": True}, {"background": True}, {"previous_response_id": "resp_missing"},
            {"conversation": {"id": "conv_missing"}}, {"truncation": "auto"},
            {"tools": [{"type": "web_search"}]}, {"tools": [{**TOOLS[0], "strict": True}]},
            {"tool_choice": {"type": "function", "name": "not_declared"}},
            {"parallel_tool_calls": False}, {"include": ["reasoning.encrypted_content"]},
            {"input": [{"type": "item_reference", "id": "msg_missing"}]},
            {"input": [malformed_output]}, {"input": [{"role": "user", "content": [{"type": "input_file", "file_id": "x"}]}]},
            {"input": [{"role": "user", "content": "start"}, {
                "type": "function_call", "call_id": "pending", "name": "count_colors", "arguments": "{}"}]},
            {"input": [{"role": "user", "content": "start"}, {
                "type": "reasoning", "id": "rs_elsewhere", "summary": [], "encrypted_content": "not-a-signature"}]},
        ):
            with self.subTest(bad=bad), patch.object(responses_api, "call_upstream", new_callable=AsyncMock) as upstream:
                reply = await self.client.post(PATH, json={**body, **bad})
                self.assertEqual(reply.status_code, 400, reply.text[:360])
                self.assertEqual(reply.json()["error"]["type"], "invalid_request_error")
                upstream.assert_not_awaited()

    async def test_auth_before_body_and_response_size_bounds(self):
        with patch.object(responses_api, "call_upstream", new_callable=AsyncMock) as upstream:
            no_auth = await self.client.post(PATH, headers={"Authorization": ""}, content=b"notjson")
            self.assertEqual(no_auth.status_code, 401, no_auth.text[:360])
            self.assertEqual(no_auth.json()["error"]["type"], "authentication_error")
            with patch.object(middleware, "MAX_RESPONSES_REQUEST_BYTES", 25):
                large = await self.client.post(PATH, json=request())
                async def chunked():
                    yield b"{\"model\": \"gemini-3"
                    yield b"-flash-preview\", \"input\": \"value\"}"
                too_long = await self.client.post(PATH, content=chunked(),
                                                  headers={"Content-Type": "application/json"})
            self.assertEqual(large.status_code, 413)
            self.assertEqual(too_long.status_code, 413)
            upstream.assert_not_awaited()
        with patch.object(responses_api, "MAX_RESPONSE_BYTES", 10), patch.object(
                responses_api, "call_upstream", AsyncMock(return_value=JSONResponse(completion()))):
            reply = await self.client.post(PATH, json=request())
            self.assertEqual(reply.status_code, 502, reply.text[:360])
            self.assertIn("limit", reply.json()["error"]["message"])

    async def test_text_output_parts_are_bounded_and_nontext_never_reaches_dispatch(self):
        base = [
            {"role": "user", "content": "Return colors."},
            {"type": "function_call", "call_id": "known", "name": "count_colors", "arguments": "{}"},
        ]
        plain = {"type": "input_text", "text": "a"}
        for output, param in (
            ([{"type": "input_image", "image_url": "fixture"}], "input[2].output[0]"),
            ([{"type": "input_file", "file_id": "fixture"}], "input[2].output[0]"),
            ([{"type": "encrypted_content", "encrypted_content": "fixture"}], "input[2].output[0]"),
            ([{"type": "input_text", "text": ["not a scalar"]}], "input[2].output[0].text"),
            ([{"type": "input_text", "text": "a", "prompt_cache_breakpoint": {}}],
             "input[2].output[0]"),
            ([plain] * 129, "input[2].output"),
        ):
            with self.subTest(output=str(output)[:200]), patch.object(
                    responses_api, "call_upstream", new_callable=AsyncMock) as upstream:
                result = {"type": "function_call_output", "call_id": "known", "output": output}
                resp = await self.client.post(PATH, json=request(input=[*base, result], stream=True))
                self.assertEqual(resp.status_code, 400, resp.text[:360])
                self.assertEqual(resp.json()["error"]["param"], param)
                upstream.assert_not_awaited()
        with patch.object(responses_api, "call_upstream", AsyncMock(return_value=JSONResponse(completion()))) as upstream:
            result = {"type": "function_call_output", "call_id": "known", "output": [plain] * 128}
            resp = await self.client.post(PATH, json=request(input=[*base, result]))
            self.assertEqual(resp.status_code, 200, resp.text[:360])
            self.assertEqual(len(upstream.await_args.args[0].messages[-1].content), 128)

    async def test_token_usage_and_reasoning_only_replayed_if_actually_delivered(self):
        with patch.object(responses_api, "call_upstream",
                          AsyncMock(return_value=JSONResponse(completion({
                              "content": "done", "reasoning_content": "real visible summary"})))) as upstream:
            reply = await self.client.post(PATH, json=request(stream=True))
        upstream.assert_awaited_once()
        self.assertFalse(upstream.await_args.args[0].stream)  # SSE is explicitly buffered
        e = event_list(reply.text)
        result = e[-1]["response"]
        self.assertEqual(e[-1]["type"], "response.completed")
        pub, message = result["output"]
        self.assertEqual(pub["content"], [{"type": "reasoning_text", "text": "real visible summary"}])
        self.assertEqual(pub["summary"], [])
        self.assertNotIn("encrypted_content", pub)
        self.assertEqual(message["content"][0]["text"], "done")
        actual = [event for event in e if event.get("output_index") == 0 and "item_id" in event]
        self.assertEqual([event["type"] for event in actual], [
            "response.content_part.added", "response.reasoning_text.delta",
            "response.reasoning_text.done", "response.content_part.done"])
        self.assertTrue(all(event["item_id"] == pub["id"] and event["content_index"] == 0 for event in actual))
        self.assertEqual(actual[0]["part"], {"type": "reasoning_text", "text": ""})
        self.assertEqual(actual[1]["delta"], pub["content"][0]["text"])
        self.assertEqual(actual[2]["text"], pub["content"][0]["text"])
        self.assertEqual(actual[-1]["part"], pub["content"][0])
        self.assertEqual(result["usage"], {"input_tokens": 7, "output_tokens": 5, "total_tokens": 12,
                                           "input_tokens_details": {"cached_tokens": 2, "cache_write_tokens": 0},
                                           "output_tokens_details": {"reasoning_tokens": 3}})

    async def test_usage_marked_as_estimated_without_inventing_cache_writes(self):
        for reported, estimated, expected in ((4, None, 4), (None, True, 0), (True, False, 0), (-1, None, 0)):
            with self.subTest(reported=reported, estimated=estimated):
                raw = completion()
                raw["usage"]["prompt_tokens_details"]["cache_write_tokens"] = reported
                raw["usage"]["estimated"] = estimated
                with patch.object(responses_api, "call_upstream", AsyncMock(return_value=JSONResponse(raw))):
                    response = await self.client.post(PATH, json=request())
                self.assertEqual(response.status_code, 200, response.text[:360])
                usage = response.json()["usage"]
                self.assertEqual(usage["input_tokens_details"], {"cached_tokens": 2, "cache_write_tokens": expected})
                if estimated is True:
                    self.assertIs(usage["estimated"], True)
                else:
                    self.assertNotIn("estimated", usage)


    async def test_large_unicode_output_has_valid_independent_events(self):
        body = 'line\n"\u2603" 🌈 ' * 7500
        with patch.object(responses_api, "call_upstream",
                          AsyncMock(return_value=JSONResponse(completion({"content": body})))):
            reply = await self.client.post(PATH, json=request(stream=True))
        self.assertEqual(reply.status_code, 200)
        events = event_list(reply.text)
        deltas = [event["delta"] for event in events if event["type"] == "response.output_text.delta"]
        self.assertGreater(len(deltas), 1)
        self.assertEqual("".join(deltas), body)
        self.assertEqual(events[-1]["response"]["output"][0]["content"][0]["text"], body)

    async def test_dense_metadata_keeps_exact_sse_bytes_without_thousands_of_sends(self):
        ordinary = [{"kind": "text", "text": "🌈\n"} for _ in range(512)]
        order = [{"type": "ordinary", "index": index} for index in range(len(ordinary))]
        extra = {"google": {"ordinary_parts": ordinary, "part_order": order}}
        chunks = list(responses_api.ResponsesEvents().event("response.completed", response=extra))
        payload = {"type": "response.completed", "sequence_number": 0, "response": extra}
        line = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
        self.assertEqual(b"".join(chunks), b"event: response.completed\ndata: " + line + b"\n\n")
        # In this fixture every source fragment is tiny. Aggregation cannot grow
        # a large frame copy or represent every punctuation mark as an ASGI send.
        self.assertLessEqual(max(map(len, chunks)), 16 * 1024)
        frames = []

        async def send(message):
            if chunk := message.get("body"):
                frames.append(chunk)

        text = "".join(part["text"] for part in ordinary)
        raw = completion({"content": text, "extra_content": extra})
        with patch.object(responses_api, "call_upstream", AsyncMock(return_value=JSONResponse(raw))):
            await responses_asgi(request(stream=True), send, asyncio.Event())
        events = event_list(b"".join(frames).decode())
        item, = events[-1]["response"]["output"]
        self.assertEqual(item["content"][0]["text"], text)
        self.assertEqual(item["extra_content"], extra)
        self.assertLess(len(frames), 64)

    async def test_incomplete_outputs_are_not_executable(self):
        raw = {"content": "partial", "tool_calls": [{"id": "partial", "type": "function",
                                                  "function": {"name": "count_colors", "arguments": "{bogus"}}]}
        for stream in (False, True):
            for reason, expected in (("length", "max_output_tokens"), ("content_filter", "content_filter")):
                with self.subTest(stream=stream, reason=reason), patch.object(
                    responses_api, "call_upstream", AsyncMock(return_value=JSONResponse(completion(raw, reason)))
                ), patch.object(responses_api, "publish_completed_origins") as publish:
                    answer = await self.client.post(PATH, json=request(stream=stream))
                    self.assertEqual(answer.status_code, 200)
                    if stream:
                        events = event_list(answer.text)
                        self.assertEqual(events[-1]["type"], "response.incomplete")
                        self.assertNotIn("response.function_call_arguments.", answer.text)
                        out = events[-1]["response"]
                    else:
                        out = answer.json()
                    self.assertEqual(out["status"], "incomplete")
                    self.assertEqual(out["incomplete_details"], {"reason": expected})
                    item, = out["output"]
                    self.assertEqual(item["status"], "incomplete")
                    self.assertEqual(item["content"][0]["text"], "partial")
                    publish.assert_not_called()

    async def test_unsupported_or_malformed_upstream_success_is_502(self):
        cases = (
            Response(b"not json", media_type="application/json"),
            JSONResponse(completion(finish="unknown")),
            JSONResponse(completion({"tool_calls": [{"id": "x", "type": "function",
                                                    "function": {"name": "other", "arguments": "{}"}}]}, "tool_calls")),
        )
        for candidate in cases:
            with self.subTest(body=candidate.body), patch.object(
                    responses_api, "call_upstream", AsyncMock(return_value=candidate)):
                response = await self.client.post(PATH, json=request())
                self.assertEqual(response.status_code, 502)
                self.assertIn("error", response.json())

    async def test_fast_429_is_json_late_429_is_one_failed_terminal_and_advisory(self):
        upstream_error = JSONResponse({"error": {"message": "try again"}}, status_code=429, headers={"Retry-After": "9"})
        with patch.object(responses_api, "call_upstream", AsyncMock(return_value=upstream_error)):
            response = await self.client.post(PATH, json=request(stream=True))
        self.assertEqual(response.status_code, 429)
        self.assertEqual(response.headers.get("Retry-After"), "9")
        self.assertEqual(response.json()["error"]["message"], "try again")

        async def late(*args):
            await asyncio.sleep(0.04)
            return upstream_error

        with patch.object(responses_api, "call_upstream", late), patch.object(
             responses_api, "SETUP_FAST_WINDOW_SECONDS", 0.001), patch.object(
             responses_api, "IDLE_KEEPALIVE_SECONDS", 0.005):
            response = await self.client.post(PATH, json=request(stream=True))
        self.assertEqual(response.status_code, 200)
        self.assertIn(": keepalive\n\n", response.text)
        self.assertIn(": retry-after 9\n\n", response.text)
        events = event_list(response.text)
        self.assertEqual([e["type"] for e in events], ["response.created", "response.in_progress", "response.failed"])
        self.assertEqual(events[-1]["response"]["status"], "failed")
        self.assertEqual(events[-1]["response"]["error"], {"code": "rate_limit_exceeded", "message": "try again"})

    async def test_unexpected_owned_stream_is_closed_without_running_its_body(self):
        close = AsyncMock()
        async def not_read():
            self.fail("Responses never reads an unexpected Chat stream")
            yield b""
        source = ManagedStreamingResponse(not_read(), cleanup=close)
        with patch.object(responses_api, "call_upstream", AsyncMock(return_value=source)):
            response = await self.client.post(PATH, json=request(stream=True))
        self.assertEqual(response.status_code, 502)
        close.assert_awaited_once()

    async def test_disconnection_and_header_send_failure_cancel_generation_and_return_ingress_slot(self):
        for phase in ("headers", "first_event"):
            with self.subTest(phase=phase):
                started, closed, disconnected = asyncio.Event(), asyncio.Event(), asyncio.Event()
                calls = Mock()
                held, in_flight = tuple(ingress_budget.used), generation_limiter.active

                async def upstream(*args):
                    calls()
                    started.set()
                    try:
                        await asyncio.Event().wait()
                    finally:
                        closed.set()

                async def send(message):
                    if phase == "headers" and message["type"] == "http.response.start":
                        await started.wait()
                        raise OSError("client closed before first pull")
                    if phase == "first_event" and message.get("body", b"").startswith(b"event: response.created"):
                        await started.wait()
                        disconnected.set()
                        await asyncio.Event().wait()

                with patch.object(chat_api, "_dispatch_upstream", upstream), patch.object(
                     responses_api, "SETUP_FAST_WINDOW_SECONDS", 0.001):
                    try:
                        await asyncio.wait_for(responses_asgi(request(stream=True), send, disconnected), 1)
                    except ClientDisconnect:
                        self.assertEqual(phase, "headers")
                self.assertTrue(closed.is_set())
                calls.assert_called_once()
                self.assertEqual(generation_limiter.active, in_flight)
                self.assertEqual(tuple(ingress_budget.used), held)

    async def test_abort_final_body_does_not_publish_origin(self):
        disconnected = asyncio.Event()
        tool = {"id": "fixture", "type": "function", "function": {"name": "count_colors", "arguments": "{}"}}
        result = JSONResponse(completion({"tool_calls": [tool]}, "tool_calls"))

        async def reject_final(message):
            if message["type"] == "http.response.body" and not message.get("more_body", False):
                raise OSError("client lost sending final body")

        with patch.object(responses_api, "call_upstream", AsyncMock(return_value=result)), patch.object(
                responses_api, "publish_completed_origins") as publish:
            with self.assertRaises(ClientDisconnect):
                await asyncio.wait_for(responses_asgi(request(stream=True), reject_final, disconnected), 1)
            publish.assert_not_called()
