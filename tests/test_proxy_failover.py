"""Ordered proxy failover with real loopback HTTP proxies and offline SDK doubles."""

import asyncio
from contextlib import asynccontextmanager
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

_TEMP = tempfile.TemporaryDirectory(prefix="v2o-failover-")
os.environ["STATE_DIR"] = _TEMP.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import httpx
from google import genai
from google.genai import types

from client_pool import close_all_clients
from first_reply import FirstReplyGuard, FirstReplyTimeout, sdk_has_reply, timed_call
from impersonate import open_upstream_client
from proxy_config import ProxyFormatError, normalize_proxy_uri, parse_proxy_list, proxy_fields
from proxy_failover import FailoverHTTPClient, FailoverSDKClient, ProxyChain
from proxy_resolver import ProxyResolveError, mask_proxy_uri
from runtime_state import app_state
from express_key_manager import ExpressKeyManager
from routes import gemini_native
from upstreams import cookie_proxy


class FormatTests(unittest.TestCase):
    def test_supplier_formats_normalize_to_the_same_proxy(self):
        for text in ("socks5://user:pass@1.2.3.4:1080", "socks5://1.2.3.4:1080@user:pass",
                     "socks5://1.2.3.4:1080:user:pass", "socks5://user:pass:1.2.3.4:1080",
                     "1.2.3.4:1080:user:pass", "user:pass@1.2.3.4:1080", "1.2.3.4:1080@user:pass",
                     "1.2.3.4 1080 user pass", "socks://user:pass@1.2.3.4:1080"):
            with self.subTest(text=text):
                self.assertEqual(normalize_proxy_uri(text), "socks5://user:pass@1.2.3.4:1080")
        self.assertEqual(normalize_proxy_uri("http://proxy.test:8080:user:pass"), "http://user:pass@proxy.test:8080")
        self.assertEqual(normalize_proxy_uri("socks5://user:1234@proxy.test:1080"), "socks5://user:1234@proxy.test:1080")

    def test_ipv6_special_characters_and_query_credentials(self):
        uri = "socks5://u%40ser:p%3Aa%23ss@[2001:db8::1]:1080"
        fields = proxy_fields(uri)
        self.assertEqual(fields["host"], "2001:db8::1")
        self.assertEqual(fields["username"], "u@ser")
        self.assertEqual(fields["password"], "p:a#ss")
        self.assertNotIn("p%3A", mask_proxy_uri(uri))
        self.assertEqual(normalize_proxy_uri("socks5://host:1080?username=u&password=p%40ss"),
                         "socks5://u:p%40ss@host:1080")
        self.assertEqual(normalize_proxy_uri("socks5://dXNlcjpwYXNz@host:1080"), "socks5://user:pass@host:1080")
        for text in ("socks5://proxy.test:1080:user:p#ass", "socks5://proxy.test:1080:user:p@ass",
                     "socks5://user:pass:[2001:db8::1]:1080"):
            self.assertEqual(proxy_fields(text)["type"], "socks5")
        self.assertEqual(proxy_fields("socks5://proxy.test:1080:user:p#ass")["password"], "p#ass")

    def test_invalid_lists_and_ports_are_rejected_without_echoing_passwords(self):
        for value in ("socks5://user:private@host:0", "socks5://host:65536", "http://host:not-port", "socks5://host:1080\nother"):
            with self.assertRaises(ProxyFormatError) as error:
                normalize_proxy_uri(value)
            self.assertNotIn("private", str(error.exception))
        self.assertEqual(parse_proxy_list('socks5://a:1080,http://b:80\nsocks5://a:1080'),
                         ["socks5://a:1080", "http://b:80"])
        with self.assertRaises(ProxyFormatError):
            parse_proxy_list([{}])


class ChainTests(unittest.IsolatedAsyncioTestCase):
    async def test_resolution_failures_skip_to_next_proxy_and_never_add_direct(self):
        async def resolve(uri):
            if uri == "broken":
                raise ProxyResolveError("unavailable protocol")
            return uri
        calls = []
        async def operation(raw, resolved):
            calls.append(raw)
            raise httpx.ConnectError("offline")
        with mock.patch("proxy_failover.resolve_proxy_uri_async", side_effect=resolve):
            with self.assertRaises(httpx.ConnectError):
                await ProxyChain(candidates=["broken", "last"]).run(operation)
        self.assertEqual(calls, ["last"])

    async def test_auth_quota_and_local_pool_errors_do_not_switch_proxies(self):
        for status in (400, 401, 403, 404, 429):
            calls = []
            async def operation(raw, resolved):
                calls.append(raw)
                return httpx.Response(status)
            response = await ProxyChain(candidates=["http://first:80", "http://second:80"]).run(operation)
            self.assertEqual(response.status_code, status)
            self.assertEqual(calls, ["http://first:80"])
        async def exhausted(raw, resolved):
            raise httpx.PoolTimeout("busy local pool")
        chain = ProxyChain(candidates=["http://first:80", "http://second:80"])
        with self.assertRaises(httpx.PoolTimeout):
            await chain.run(exhausted)
        self.assertEqual(chain.index, 0)

    async def test_proxy_timeouts_have_individual_first_reply_windows(self):
        calls, cancelled = [], []
        async def operation(raw, resolved):
            calls.append(raw)
            if raw.endswith("first:80"):
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.append(raw)
            return httpx.Response(200)
        with mock.patch("first_reply.timeout_seconds", return_value=0.015):
            result = await timed_call("gemini-test", ProxyChain(candidates=["http://first:80", "http://second:80"]).run(operation))
        self.assertEqual(result.status_code, 200)
        self.assertEqual(len(calls), 2)
        self.assertEqual(cancelled, ["http://first:80"])
        with mock.patch("first_reply.timeout_seconds", return_value=0.01):
            with self.assertRaises(FirstReplyTimeout):
                await timed_call("gemini-test", ProxyChain(candidates=["http://first:80"]).run(operation))

    async def test_sdk_switches_before_first_chunk_but_does_not_replay_partial_stream(self):
        partial_chunk = types.GenerateContentResponse(candidates=[types.Candidate(
            content=types.Content(parts=[types.Part(text="partial")]))])
        answer_chunk = types.GenerateContentResponse(candidates=[types.Candidate(
            content=types.Content(parts=[types.Part(text="answer")]))])
        for partial in (False, True):
            calls, closed = [], []
            def factory(raw, resolved):
                async def stream(**kwargs):
                    async def chunks():
                        try:
                            calls.append(raw)
                            if raw.endswith("first:80"):
                                if partial:
                                    yield partial_chunk
                                raise httpx.ReadError("closed")
                            yield answer_chunk
                        finally:
                            closed.append(raw)
                    return chunks()
                return SimpleNamespace(aio=SimpleNamespace(models=SimpleNamespace(generate_content_stream=stream)))
            facade = FailoverSDKClient(ProxyChain(candidates=["http://first:80", "http://second:80"]), factory)
            source = await facade.generate_content_stream()
            if partial:
                self.assertEqual(await anext(source), partial_chunk)
                with self.assertRaises(httpx.ReadError):
                    await anext(source)
                self.assertEqual(len(calls), 1)
            else:
                self.assertEqual([chunk async for chunk in source], [answer_chunk])
                self.assertEqual(len(calls), 2)
            self.assertEqual(closed, calls)

    async def test_sdk_metadata_and_empty_streams_can_fail_over_before_any_reply(self):
        answer = types.GenerateContentResponse(candidates=[types.Candidate(
            content=types.Content(parts=[types.Part(text="answer")]), finish_reason="STOP")])
        metadata = types.GenerateContentResponse(usage_metadata=types.GenerateContentResponseUsageMetadata(
            prompt_token_count=10))
        for failure in ("empty", "metadata-eof", "metadata-disconnect"):
            calls, closed = [], []
            def factory(raw, resolved):
                async def stream(**kwargs):
                    async def chunks():
                        calls.append(raw)
                        try:
                            if raw.endswith("first:80"):
                                if failure != "empty":
                                    yield metadata
                                if failure == "metadata-disconnect":
                                    raise httpx.ReadError("disconnected after usage")
                            else:
                                yield answer
                        finally:
                            closed.append(raw)
                    return chunks()
                return SimpleNamespace(aio=SimpleNamespace(models=SimpleNamespace(generate_content_stream=stream)))
            facade = FailoverSDKClient(ProxyChain(candidates=["http://first:80", "http://second:80"]), factory)
            with self.subTest(failure=failure):
                source = await facade.generate_content_stream()
                self.assertEqual([chunk async for chunk in source], [answer])
                self.assertEqual(len(calls), 2)
                self.assertEqual(closed, calls)

    async def test_sdk_does_not_fail_over_a_terminal_safety_decision(self):
        blocked = types.GenerateContentResponse(prompt_feedback=types.GenerateContentResponsePromptFeedback(
            block_reason="SAFETY"))
        calls = []
        def factory(raw, resolved):
            async def stream(**kwargs):
                calls.append(raw)
                async def chunks():
                    yield blocked
                return chunks()
            return SimpleNamespace(aio=SimpleNamespace(models=SimpleNamespace(generate_content_stream=stream)))
        facade = FailoverSDKClient(ProxyChain(candidates=["http://first:80", "http://second:80"]), factory)
        source = await facade.generate_content_stream()
        self.assertEqual([chunk async for chunk in source], [blocked])
        self.assertEqual(calls, ["http://first:80"])

    async def test_sdk_timeout_after_metadata_closes_source_before_switching(self):
        closed = []
        answer = types.GenerateContentResponse(candidates=[types.Candidate(
            content=types.Content(parts=[types.Part(text="answer")]))])
        def factory(raw, resolved):
            async def stream(**kwargs):
                async def chunks():
                    try:
                        if raw.endswith("first:80"):
                            yield types.GenerateContentResponse()
                            await asyncio.Event().wait()
                        else:
                            self.assertEqual(closed, ["http://first:80"])
                            yield answer
                    finally:
                        closed.append(raw)
                return chunks()
            return SimpleNamespace(aio=SimpleNamespace(models=SimpleNamespace(generate_content_stream=stream)))
        facade = FailoverSDKClient(ProxyChain(candidates=["http://first:80", "http://second:80"]), factory)
        with mock.patch("first_reply.timeout_seconds", return_value=0.02):
            guard = FirstReplyGuard("gemini-test")
            source = await guard.wait(facade.generate_content_stream())
            self.assertEqual([chunk async for chunk in guard.iterate(source, sdk_has_reply)], [answer])
        self.assertEqual(closed, ["http://first:80", "http://second:80"])

    async def test_stream_context_closes_on_exception_and_cancellation_without_replay(self):
        closed, entered = [], []
        class Client:
            @asynccontextmanager
            async def stream(self, *args, **kwargs):
                entered.append(True)
                try:
                    yield SimpleNamespace(status_code=200)
                finally:
                    closed.append(True)
        facade = FailoverHTTPClient(ProxyChain(candidates=["http://first:80", "http://second:80"]), lambda *args: Client())
        for error in (httpx.ReadError("body stopped"), asyncio.CancelledError()):
            with self.assertRaises(type(error)):
                async with facade.stream("POST", "https://unused.test"):
                    raise error
        self.assertEqual(len(entered), 2)
        self.assertEqual(closed, entered)


class LoopbackTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        app_state._state = {}
        app_state._committed = {}
        self.servers, self.hits = [], []
        for status in (407, None, 200):
            async def serve(reader, writer, status=status):
                try:
                    header = await reader.readuntil(b"\r\n\r\n")
                    self.hits.append((status, header))
                    if status is not None:
                        candidate = {"content": {"role": "model", "parts": [{"text": "answer"}]}, "finishReason": "STOP"}
                        body = json.dumps({"ok": True, "candidates": [candidate], "results": [{"data": {"candidates": [candidate]}}]}).encode()
                        content_type = "application/json"
                        if b":streamGenerateContent" in header and status == 200:
                            body = b"data: " + body + b"\n\n"
                            content_type = "text/event-stream"
                        writer.write(f"HTTP/1.1 {status} Test\r\nContent-Length: {len(body)}\r\nContent-Type: {content_type}\r\nConnection: close\r\n\r\n".encode() + body)
                        await writer.drain()
                except (ConnectionError, asyncio.IncompleteReadError):
                    pass
                finally:
                    writer.close()
                    await writer.wait_closed()
            server = await asyncio.start_server(serve, "127.0.0.1", 0)
            self.servers.append(server)
        self.proxies = [f"http://127.0.0.1:{s.sockets[0].getsockname()[1]}" for s in self.servers]
        app_state.update_settings({"proxy_urls": [self.proxies[-1]]})

    async def asyncTearDown(self):
        await close_all_clients()
        for server in self.servers:
            server.close()
            await server.wait_closed()

    async def broken_body_proxy(self, *, sse=False, clean_eof=False):
        async def serve(reader, writer):
            try:
                header = await reader.readuntil(b"\r\n\r\n")
                self.hits.append(("empty-body", header))
                kind = "text/event-stream" if sse else "application/json"
                writer.write(f"HTTP/1.1 200 OK\r\nContent-Length: {0 if clean_eof else 100}\r\nContent-Type: {kind}\r\nConnection: close\r\n\r\n".encode())
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()
        server = await asyncio.start_server(serve, "127.0.0.1", 0)
        self.servers.append(server)
        return f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}"

    async def test_httpx_and_curl_try_account_proxies_then_global_on_real_sockets(self):
        for profile in ("off", "chrome"):
            with self.subTest(profile=profile):
                self.hits.clear()
                app_state.update_settings({"cookie_impersonate": profile})
                chain = ProxyChain({"proxies": self.proxies[:2]})
                async with open_upstream_client({"timeout": httpx.Timeout(2), "trust_env": False,
                                                 "_proxy_chain": chain}) as client:
                    response = await client.post("http://unused.invalid/test", headers={"cookie": "SAPISID=one"}, json={})
                self.assertEqual(response.status_code, 200)
                self.assertEqual([s for s, _ in self.hits], [407, None, 200])
                self.assertTrue(all(b"SAPISID=one" in header for _, header in self.hits))
                self.assertEqual(chain.index, 2)

    async def test_disabling_account_fallback_never_contacts_global_proxy(self):
        app_state.update_settings({"cookie_impersonate": "off"})
        chain = ProxyChain({"proxies": self.proxies[:2], "proxy_fallback": False})
        async with open_upstream_client({"timeout": httpx.Timeout(2), "trust_env": False,
                                         "_proxy_chain": chain}) as client:
            with self.assertRaises(httpx.RemoteProtocolError):
                await client.post("http://unused.invalid/test", json={})
        self.assertEqual([s for s, _ in self.hits], [407, None])

    async def test_http_stream_headers_fail_over_and_selected_response_is_closed(self):
        app_state.update_settings({"cookie_impersonate": "off"})
        chain = ProxyChain({"proxies": self.proxies[:2]})
        async with open_upstream_client({"timeout": httpx.Timeout(2), "trust_env": False,
                                         "_proxy_chain": chain}) as client:
            async with client.stream("POST", "http://unused.invalid/test", json={}) as response:
                self.assertEqual(response.status_code, 200)
                self.assertTrue(json.loads(await response.aread())["ok"])
        self.assertTrue(response.is_closed)
        self.assertEqual([s for s, _ in self.hits], [407, None, 200])

    async def test_real_sdk_json_and_stream_use_ordered_proxy_chain(self):
        for stream in (False, True):
            self.hits.clear()
            clients = []
            def factory(raw, resolved):
                options = {"proxy": resolved, "trust_env": False, "timeout": 2}
                client = genai.Client(vertexai=True, api_key="fake-key", http_options=types.HttpOptions(
                    base_url="http://unused.invalid", client_args=options, async_client_args=options,
                    retry_options=types.HttpRetryOptions(attempts=1)))
                clients.append(client)
                return client
            facade = FailoverSDKClient(ProxyChain({"proxies": self.proxies[:2]}), factory)
            try:
                if stream:
                    source = await facade.aio.models.generate_content_stream(model="gemini-test", contents="hello")
                    result = [chunk async for chunk in source][0]
                else:
                    result = await facade.aio.models.generate_content(model="gemini-test", contents="hello")
                self.assertEqual(result.candidates[0].content.parts[0].text, "answer")
                self.assertEqual([s for s, _ in self.hits], [407, None, 200])
            finally:
                for client in clients:
                    await client.aio.aclose()
                    client.close()

    async def test_native_request_with_zero_model_retries_uses_global_fallback(self):
        app_state.set_express_keys([{"key": "fake-key", "proxies": self.proxies[:2]}])
        app_state.update_settings({"retry_max": 0, "express_location": ""})
        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(express_key_manager=ExpressKeyManager())),
                                  is_disconnected=mock.AsyncMock(return_value=False))
        with mock.patch.object(gemini_native, "EXPRESS_BASE", "http://unused.invalid"):
            response = await gemini_native._passthrough_request(request, "gemini-test", "generateContent", "", b"{}")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(json.loads(response.body)["candidates"][0]["content"]["parts"][0]["text"], "answer")
        self.assertEqual([s for s, _ in self.hits], [407, None, 200])

    async def test_cookie_stream_can_switch_after_headers_before_first_content(self):
        for clean_eof in (False, True):
            self.hits.clear()
            proxy = await self.broken_body_proxy(clean_eof=clean_eof)
            app_state.update_settings({"cookie_impersonate": "off", "retry_max": 0})
            chain = ProxyChain({"proxies": [proxy]})
            with self.subTest(clean_eof=clean_eof), \
                 mock.patch.object(cookie_proxy, "BATCH_GRAPHQL_URL", "http://unused.invalid/test"):
                async with open_upstream_client({"timeout": httpx.Timeout(2), "trust_env": False,
                                                 "_proxy_chain": chain}) as client:
                    events = [item async for item in cookie_proxy._execute_stream_request_generator(
                        client, {}, {}, model="gemini-test")]
            self.assertIn(("text", "answer"), events)
            self.assertNotIn("fatal_error", [status for status, _ in events])
            self.assertEqual([s for s, _ in self.hits], ["empty-body", 200])

    async def test_native_stream_can_switch_after_headers_before_first_content(self):
        proxy = await self.broken_body_proxy(sse=True)
        app_state.set_express_keys([{"key": "fake-key", "proxies": [proxy]}])
        app_state.update_settings({"retry_max": 0, "express_location": ""})
        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(express_key_manager=ExpressKeyManager())),
                                  is_disconnected=mock.AsyncMock(return_value=False))
        with mock.patch.object(gemini_native, "EXPRESS_BASE", "http://unused.invalid"):
            response = await gemini_native._passthrough_request(request, "gemini-test", "streamGenerateContent", "alt=sse", b"{}")
            body = b"".join([chunk async for chunk in response.body_iterator])
        self.assertIn(b"answer", body)
        self.assertEqual([s for s, _ in self.hits], ["empty-body", 200])


if __name__ == "__main__":
    unittest.main()
