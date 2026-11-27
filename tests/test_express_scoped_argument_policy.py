"""Project names cannot select an Express model's native tool capability."""

import anti_truncation as transport
from models import OpenAIRequest
from test_schema_contract_fidelity import SchemaRouteHarness


def scoped(project, leaf):
    return f"projects/{project}/locations/global/publishers/google/models/{leaf}"


def test_argument_capability_requires_a_recognizable_bare_or_typed_google_model():
    for model in ("gemini-4", "Gemini-4-Hydrogen", "models/gemini-4-hydrogen",
                  "publishers/google/models/gemini-4-hydrogen"):
        assert transport.supports_argument_stream(model), model
    for model in ("foreign-gemini-4", "gemini-experimental", "gemini-4version", "vendor/gemini-4-demo/model",
                  "projects/gemini-4-demo/locations/global/endpoints/unknown"):
        assert not transport.supports_argument_stream(model), model


def test_only_explicit_synthetic_alias_and_supported_scoped_leaf_enable_incremental_transport():
    settings = {"anti_truncation_mode": "both"}
    for leaf, project, supported in (
        ("Gemini-4-Hydrogen", "gemini-2-demo", True),
        ("gemini-2.5-flash", "gemini-4-demo", False),
        ("gemini-3.1-flash-lite-preview", "gemini-4-demo", False),
    ):
        model = scoped(project, leaf)
        req = OpenAIRequest(model=model, messages=[{"role": "user", "content": "Give a short greeting."}],
                            stream=True)
        assert transport.supports_argument_stream(model) is supported
        assert transport.prepare_transport(req, model, settings, native_stream=True) == (req, None)
        alias = req.model_copy(update={"model": model + "-antitruncation-stream"})
        wrapped, selection = transport.prepare_transport(alias, model, settings, native_stream=True)
        assert wrapped.model == alias.model  # Do not rewrite public or wire resource spelling.
        assert selection.native_stream is supported


class ExpressScopedArgumentWireTests(SchemaRouteHarness):
    async def test_real_sdk_wire_enables_argument_progress_by_leaf_only(self):
        schema = {"type": "object", "properties": {"city": {"type": "string"}}}
        definition = {"name": "quote", "parameters": schema}
        messages = [{"role": "user", "content": "Read the local weather quote."}]
        self.reply = {"functionCall": {"id": "fixture-call", "name": "quote", "args": {"city": "Paris"}}}
        for model, supported in (
            (scoped("ordinary-project", "gemini-4-hydrogen"), True),
            (scoped("gemini-2-demo", "Gemini-4-Hydrogen"), True),
            (scoped("ordinary-project", "gemini-2.5-flash"), False),
            (scoped("gemini-4-demo", "gemini-2.5-flash"), False),
            (scoped("gemini-4-demo", "gemini-3.1-flash-lite"), False),
            ("[EXPRESS] " + scoped("gemini-2-demo", "Gemini-4-Hydrogen"), True),
            ("[EXPRESS] gemini-4-hydrogen", True),
            ("models/gemini-4-hydrogen", True),
            ("publishers/google/models/gemini-4-hydrogen", True),
            ("foreign-gemini-4", False),
        ):
            for path, fields in (
                ("/v1/chat/completions", {"messages": messages,
                                          "tools": [{"type": "function", "function": definition}]}),
                ("/v1/messages", {"messages": messages, "max_tokens": 128,
                                  "tools": [{"name": "quote", "input_schema": schema}]}),
                ("/v1/responses", {"input": messages, "store": False,
                                   "tools": [{"type": "function", **definition}]}),
            ):
                with self.subTest(model=model, path=path):
                    self.sent.clear()
                    reply = await self.client.post(path, json={**fields, "model": model, "stream": True})
                    self.assertEqual(reply.status_code, 200, reply.text)
                    self.assertEqual(len(self.sent), 1)
                    wire = self.sent[0]
                    self.assertEqual([f["name"] for cfg in wire["tools"]
                                      for f in cfg["functionDeclarations"]], ["quote"])
                    cfg = wire.get("toolConfig", {}).get("functionCallingConfig", {})
                    flag = cfg.get("streamFunctionCallArguments", cfg.get("stream_function_call_arguments", False))
                    # Public Responses SSE remains an upstream collected call.
                    self.assertEqual(flag, supported and path != "/v1/responses")
                    self.assertIn("response.completed" if path == "/v1/responses" else "fixture-call", reply.text)
