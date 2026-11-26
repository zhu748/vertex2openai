"""No immediate request-shape or pinned model downgrade across a positive wait."""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from google.genai.errors import ClientError

from models import OpenAIRequest
from test_model_retry import text_response
from upstreams import express_pipeline as sdk


@pytest.mark.parametrize("mode,kind", [("json", "pin"), ("true", "pin"), ("fake", "pin"), ("true", "args")])
@pytest.mark.parametrize("hint,hold", [("45", True), ("Fri, 01 Jan 2100 00:00:00 GMT", True),
                                      ("0", False), ("invalid date", False), (None, False)])
def test_sdk_immediate_compatibility_fallback(mode, kind, hint, hold):
    async def run():
        calls = []
        status = 404 if kind == "pin" else 400
        message = "Publisher model was not found" if kind == "pin" else "unknown field streamFunctionCallArguments"
        error = ClientError(status, {"error": {"message": message}},
                            response=httpx.Response(status, headers={"retry-after": hint} if hint else {}))
        async def generate(**kwargs):
            calls.append((kwargs["model"], bool(kwargs["config"].get("tool_config", {})
                          .get("function_calling_config", {}).get("stream_function_call_arguments"))))
            if len(calls) == 1:
                raise error
            return text_response("model output")
        async def stream(**kwargs):
            response = await generate(**kwargs)
            async def frames():
                yield response
            return frames()
        client = SimpleNamespace(aio=SimpleNamespace(models=SimpleNamespace(generate_content=generate,
                                                                            generate_content_stream=stream)))
        request = SimpleNamespace(scope={"state": {}}, is_disconnected=AsyncMock(return_value=False))
        obj = OpenAIRequest(model="gemini-3.6-flash", messages=[], stream=mode != "json")
        first = "projects/p/locations/global/publishers/google/models/" + obj.model if kind == "pin" else obj.model
        config = {"tool_config": {"function_calling_config": {"stream_function_call_arguments": True}}} if kind == "args" else {}
        with (patch.object(sdk.app_state, "get_setting", side_effect=lambda k, d=None: mode == "fake" if k == "fake_streaming" else d),
              patch.object(sdk, "get_retry_settings", return_value=(0, 0, 0))):
            async def execute():
                return await sdk.execute_gemini_call(client, first, lambda _: [], config, obj,
                            fastapi_request=request, fallback_model=obj.model if kind == "pin" else None)
            if mode == "json" and hold:
                # The outer ASGI route renders this exact failure and its headers.
                with pytest.raises(ClientError) as raised:
                    await execute()
                assert raised.value is error
            else:
                result = await execute()
                if mode == "json":
                    assert b"model output" in result.body
                else:
                    wire = "".join([frame async for frame in result.body_iterator])
                    if hold:
                        entries = [json.loads(line[6:]) for line in wire.splitlines()
                                   if line.startswith("data: ") and line[6:] != "[DONE]"]
                        assert [(e["error"]["code"], e["error"]["retry_after"]) for e in entries if "error" in e] == [(status, hint)]
                    else:
                        assert "model output" in wire
        assert calls == ([(first, kind == "args")] if hold else [(first, kind == "args"), (obj.model, False)])
        if hold and kind == "args":
            assert config["tool_config"]["function_calling_config"]["stream_function_call_arguments"] is True
    asyncio.run(run())
