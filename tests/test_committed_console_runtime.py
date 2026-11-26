"""A console edit cannot select credentials or change model behavior before disk commit."""

import asyncio
from contextlib import asynccontextmanager
import json
from threading import Event

from google import genai
from google.genai import types
import httpx
import pytest

import channel_dispatch
import console_auth
import credential_manager
import express_endpoint
import express_key_manager
import generation_config
import main
from routes import chat_api, credentials_api, settings_api
import runtime_state
from upstreams import cookie_delivery, cookie_proxy, cookie_request, express_sdk


MODEL = "gemini-3.6-flash"
EXISTING = {"key": "saved-fixture", "project_id": "saved-project"}
REPLACEMENT = {"key": "replacement-fixture", "project_id": "replacement-project"}
COMPLETION = {
    "model": MODEL,
    "messages": [{"role": "user", "content": "Summarize the current build status."}],
    "reasoning_effort": "high",
    "tools": [{"type": "function", "function": {"name": "read_status", "parameters": {"type": "object"}}}],
}


def expected_thinking(mode, level, include):
    if mode == "api_key":
        return {"thinking_level": level, "include_thoughts": include}
    return {"thinkingLevel": level, "includeThoughts": include}


@asynccontextmanager
async def offline_server(tmp_path, monkeypatch, mode):
    monkeypatch.setattr(runtime_state, "STATE_FILE", str(tmp_path / "web_state.json"))
    state = runtime_state.AppState()
    state.set_channel_mode(mode)
    state.set_express_keys([EXISTING])
    state.set_cookie_accounts([{"id": "fixture", "cookie": "SAPISID=offline", "project_id": "cookie-project"}])
    state.update_settings({"express_location": "us-central1", "native_thinking_mode": "request"})

    for module in (runtime_state, chat_api, settings_api, credentials_api, express_key_manager,
                   express_endpoint, credential_manager, channel_dispatch, express_sdk,
                   generation_config, cookie_request, cookie_proxy):
        monkeypatch.setattr(module, "app_state", state)
    manager = express_key_manager.ExpressKeyManager()
    monkeypatch.setattr(main.app.state, "express_key_manager", manager)
    monkeypatch.setattr(cookie_delivery, "build_headers", lambda *_: {"authorization": "offline"})
    sent = []

    def provider(request):
        payload = json.loads(request.content)
        studio = "variables" in payload
        sent.append((request.headers, request.url.path, payload.get("variables", payload)))
        response = {"candidates": [{"content": {"role": "model", "parts": [{"text": "ok"}]},
                                    "finishReason": "STOP"}]}
        return httpx.Response(200, json={"results": [{"data": response}]} if studio else response)

    transport = httpx.MockTransport(provider)
    sdk = genai.Client(vertexai=True, api_key=EXISTING["key"],
                       http_options=types.HttpOptions(async_client_args={"transport": transport}))
    next_sdk = genai.Client(vertexai=True, api_key=REPLACEMENT["key"],
                            http_options=types.HttpOptions(async_client_args={"transport": transport}))
    clients = {EXISTING["key"]: sdk, REPLACEMENT["key"]: next_sdk}
    monkeypatch.setattr(express_sdk, "get_pooled_express_client", lambda key, raw: clients[key])
    token = console_auth.issue_session()
    try:
        async with (
            httpx.AsyncClient(transport=transport) as studio,
            httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://offline",
                              headers={"Authorization": "Bearer test-key", "X-Resumable": "false"},
                              cookies={console_auth.AUTH_COOKIE: token}) as client,
        ):
            @asynccontextmanager
            async def open_studio(*args, **kwargs):
                yield studio

            monkeypatch.setattr(cookie_delivery, "open_upstream_client", open_studio)
            yield client, state, sent
    finally:
        console_auth.revoke_session(token)
        for instance in clients.values():
            await instance.aio.aclose()
            instance.close()


@asynccontextmanager
async def paused_post(client, state, monkeypatch, path, payload, succeeds):
    """Block for a durable write without blocking the event loop or other readers."""
    writing, release = Event(), Event()
    write = state._write_snapshot

    def gated(snapshot):
        writing.set()
        assert release.wait(8), "test never released state write"
        if not succeeds:
            raise OSError("simulated disk failure")
        write(snapshot)

    with monkeypatch.context() as probe:
        probe.setattr(state, "_write_snapshot", gated)
        request = asyncio.create_task(client.post(path, json=payload))
        try:
            assert await asyncio.to_thread(writing.wait, 5), "write not reached"
            yield request
        finally:
            release.set()
            await asyncio.wait_for(request, 5)


async def actual_generation(client, sent):
    sent.clear()
    response = await asyncio.wait_for(client.post("/v1/chat/completions", json=COMPLETION), 5)
    assert response.status_code == 200, response.text
    assert len(sent) == 1
    return sent[0]


@pytest.mark.parametrize("mode", ["api_key", "web_proxy"])
@pytest.mark.parametrize("scope", ["global", "specific"])
@pytest.mark.parametrize("succeeds", [False, True])
def test_pending_thinking_change_never_reaches_console_or_wire(tmp_path, monkeypatch, mode, scope, succeeds):
    async def run():
        async with offline_server(tmp_path, monkeypatch, mode) as (client, state, sent):
            if scope == "specific":
                state.set_model_override(MODEL, {"native_thinking_mode": "request"})
            write_path = "/api/settings" if scope == "global" else f"/api/model-overrides/{MODEL}"
            read_path = "/api/settings" if scope == "global" else "/api/model-overrides"

            async def read_mode():
                resp = await asyncio.wait_for(client.get(read_path), 2)
                assert resp.status_code == 200
                info = resp.json()
                return (info if scope == "global" else info[MODEL])["native_thinking_mode"]

            async def think():
                _, _, body = await actual_generation(client, sent)
                return body["generationConfig"]["thinkingConfig"]

            high = expected_thinking(mode, "HIGH", True)
            off = expected_thinking(mode, "MINIMAL", False)
            # Warm the model-specific cache before staging anything.
            assert await think() == high
            assert await read_mode() == "request"
            async with paused_post(client, state, monkeypatch, write_path, {"native_thinking_mode": "off"},
                                   succeeds) as posted:
                seen_mode = await read_mode()
                seen_thinking = await think()
                assert (seen_mode, seen_thinking) == ("request", high)
            assert posted.result().status_code == (200 if succeeds else 503), posted.result().text
            assert await read_mode() == ("off" if succeeds else "request")
            assert await think() == (off if succeeds else high)

    asyncio.run(run())


@pytest.mark.parametrize("succeeds", [False, True])
def test_pending_express_key_keeps_published_revision_and_selected_identity(tmp_path, monkeypatch, succeeds):
    async def run():
        async with offline_server(tmp_path, monkeypatch, "api_key") as (client, state, sent):
            path = "/api/express-keys"
            before = (await client.get(path)).json()
            assert all(before["keys"][0][key] == value for key, value in EXISTING.items())
            async with paused_post(client, state, monkeypatch, path,
                                   {"keys": [REPLACEMENT], "revision": before["revision"]}, succeeds) as posted:
                seen = (await asyncio.wait_for(client.get(path), 2)).json()
                headers, model_path, _ = await actual_generation(client, sent)
                assert (seen["keys"], seen["revision"], headers["x-goog-api-key"]) == (
                    before["keys"], before["revision"], EXISTING["key"])
                assert f"/projects/{EXISTING['project_id']}/" in model_path
            assert posted.result().status_code == (200 if succeeds else 503), posted.result().text
            expected = REPLACEMENT if succeeds else EXISTING
            after = (await client.get(path)).json()
            assert all(after["keys"][0][key] == value for key, value in expected.items())
            assert (after["revision"] != before["revision"]) == succeeds
            if succeeds:
                payload = posted.result().json()
                assert (payload["keys"], payload["revision"]) == (after["keys"], after["revision"])
            headers, model_path, _ = await actual_generation(client, sent)
            assert headers["x-goog-api-key"] == expected["key"]
            assert f"/projects/{expected['project_id']}/" in model_path

    asyncio.run(run())


@pytest.mark.parametrize("succeeds", [False, True])
def test_dispatch_does_not_switch_channels_until_mode_is_committed(tmp_path, monkeypatch, succeeds):
    async def run():
        async with offline_server(tmp_path, monkeypatch, "api_key") as (client, state, sent):
            async with paused_post(client, state, monkeypatch, "/api/settings/mode", {"mode": "web_proxy"},
                                   succeeds) as posted:
                assert (await client.get("/api/settings/runtime")).json()["mode"] == "api_key"
                headers, _, _ = await actual_generation(client, sent)
                assert headers["x-goog-api-key"] == EXISTING["key"]
            assert posted.result().status_code == (200 if succeeds else 503), posted.result().text
            headers, _, body = await actual_generation(client, sent)
            if succeeds:
                assert "x-goog-api-key" not in headers
                assert body["model"].startswith("projects/cookie-project/")
            else:
                assert headers["x-goog-api-key"] == EXISTING["key"]

    asyncio.run(run())
