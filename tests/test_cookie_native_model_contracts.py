"""Cookie's configured project/global model scope is not a redirect or a prefix to repeat."""
from unittest.mock import patch

import pytest

from schema_validation import SchemaValidationError
from upstreams import cookie_native as bridge


MODEL = "gemini-3.5-flash-lite"
PROJECT = "my-image-project"
RESOURCE = f"projects/{PROJECT}/locations/global/publishers/google/models/{MODEL}"


@pytest.mark.parametrize("model", [MODEL, f"models/{MODEL}", f"publishers/google/models/{MODEL}", RESOURCE])
def test_equivalent_models_use_one_resource_and_one_model_profile(model):
    with patch.object(bridge.app_state, "get_effective_settings", return_value={}) as settings:
        variables = bridge.native_request_to_variables({}, model, PROJECT)
    settings.assert_called_once_with(MODEL)
    assert variables["model"] == RESOURCE
    assert "responseModalities" not in variables["generationConfig"]


@pytest.mark.parametrize("model", [
    f"projects/other/locations/global/publishers/google/models/{MODEL}",
    f"projects/{PROJECT}/locations/us-central1/publishers/google/models/{MODEL}",
    f"projects/{PROJECT}/locations/global/endpoints/model",
    f"publishers/other/models/{MODEL}", f"models/extra/{MODEL}", "tunedModels/model",
    "endpoints/model", "publishers/google/models/", "models/..",
])
def test_unrepresentable_model_scopes_are_not_redirected(model):
    with pytest.raises(SchemaValidationError, match="global"):
        bridge.native_request_to_variables({}, model, PROJECT)


def thinking(config, model=MODEL):
    return bridge.native_request_to_variables(
        {"generation_config": {"thinking_config": config}}, model, PROJECT)["generationConfig"]


@pytest.mark.parametrize("model", [MODEL, "gemini-3-flash-preview", "gemini-3-pro-preview", "gemini-3-pro-image"])
@pytest.mark.parametrize("budget", [0, "0", 0.0])
def test_zero_cannot_enable_an_arbitrary_level_or_minimal(model, budget):
    with pytest.raises(SchemaValidationError, match="thinkingBudget=0"):
        thinking({"thinking_budget": budget}, model)


@pytest.mark.parametrize("model", [MODEL, "gemini-3-pro-preview"])
def test_automatic_budget_leaves_level_to_model_and_preserves_include_thoughts(model):
    assert thinking({"thinking_budget": "-1", "include_thoughts": False}, model) == {
        "thinkingConfig": {"includeThoughts": False}}
    assert "thinkingConfig" not in thinking({"thinkingBudget": -1}, model)


@pytest.mark.parametrize("budget", [None, 1024, 1024.0, "1024", "1.024e3"])
def test_proto_integer_forms_convert_equivalently(budget):
    assert thinking({"thinkingBudget": budget}) == (
        {} if budget is None else {"thinkingConfig": {"thinkingLevel": "LOW"}})


@pytest.mark.parametrize("budget", [-2, "-2", 1.1, "2.3", "", "bad", {}, [], True, False, 2**31, "1e20"])
def test_invalid_budgets_never_turn_into_paid_default_or_fractional_truncation(budget):
    with pytest.raises(SchemaValidationError, match="thinkingBudget"):
        thinking({"thinkingBudget": budget})


def test_conflicting_level_and_budget_cannot_override_disable():
    with pytest.raises(SchemaValidationError, match="thinkingBudget"):
        thinking({"thinkingLevel": "high", "thinkingBudget": 0})


def test_unavailable_budget_support_is_not_assumed():
    with pytest.raises(SchemaValidationError, match="thinkingBudget"):
        thinking({"thinkingBudget": 0}, "gemini-2.5-pro")


def test_existing_budget_wire_keeps_zero_and_automatic_distinct():
    # This profile is synthetic: it does not assert that any catalog model supports zero.
    for budget in (0, -1, 2048):
        gen = {"thinkingConfig": {"thinkingBudget": str(budget), "includeThoughts": False}}
        bridge._normalize_thinking_config(gen, {"thinking_kind": "budget"})
        assert gen == {"thinkingConfig": {"thinkingBudget": budget, "includeThoughts": False}}


@pytest.mark.parametrize("field", ["includeThoughts", "include_thoughts"])
def test_native_thought_visibility_accepts_only_json_booleans_or_null(field):
    for value in (False, True):
        assert thinking({field: value}) == {"thinkingConfig": {"includeThoughts": value}}
    assert thinking({field: None, "thinking_level": "low"}) == {"thinkingConfig": {"thinkingLevel": "LOW"}}
    for value in ("false", "true", "", 0, 1, 1.0, [], {}, [False]):
        with pytest.raises(SchemaValidationError, match="includeThoughts"):
            thinking({field: value})


def test_bad_visibility_is_a_native_400_before_cookie_dispatch():
    import asyncio
    import httpx
    from main import app

    async def exercise():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://local") as client:
            with (patch.object(app.state.express_key_manager, "get_total_keys", return_value=0),
                  patch.object(bridge.app_state, "get_cookie_accounts", return_value=[
                      {"id": "offline", "enabled": True, "cookie": "test", "project_id": PROJECT}]),
                  patch.object(bridge, "build_headers", return_value={}),
                  patch.object(bridge, "open_upstream_client", side_effect=AssertionError("unexpected dispatch")) as send):
                for action, query in (("generateContent", ""), ("streamGenerateContent", "?alt=sse")):
                    response = await client.post(f"/v1beta/models/{MODEL}:{action}{query}",
                        headers={"x-goog-api-key": "test-key"}, json={"contents": [{"parts": [{"text": "hello"}]}],
                        "generationConfig": {"thinkingConfig": {"includeThoughts": "false"}}})
                    assert response.status_code == 400
                    assert response.json()["error"]["status"] == "INVALID_ARGUMENT"
                    assert "includeThoughts" in response.json()["error"]["message"]
                send.assert_not_called()

    asyncio.run(exercise())
