"""Model-list refresh verifies usable model IDs before accepting a source."""
import asyncio

import httpx
import pytest

import model_loader
from proxy_failover import ProxyChain


PRIMARY = "https://catalog.test/models"
MIRROR = "https://mirror.test/models"
CUSTOM = "https://custom.test/models"
BLANK = {"models": [" ", "", None]}
VALID = {"vertex_express_models": [" active ", "active"]}


@pytest.mark.parametrize("override, documents, urls, expected", [
    ("", {PRIMARY: BLANK, MIRROR: VALID}, [PRIMARY, MIRROR], ["active"]),
    ("", {PRIMARY: BLANK, MIRROR: {"models": []}}, [PRIMARY, MIRROR], ["cached"]),
    (CUSTOM, {CUSTOM: BLANK}, [CUSTOM], ["cached"]),
    ("", {PRIMARY: VALID}, [PRIMARY], ["active"]),
], ids=["blank-primary-valid-mirror", "both-blank", "custom-stays-custom", "good-primary"])
def test_refresh_chooses_first_usable_catalog(monkeypatch, override, documents, urls, expected):
    seen = []

    def handle(request):
        url = str(request.url)
        seen.append(url)
        return httpx.Response(200, json=documents[url])

    real_client = httpx.AsyncClient
    monkeypatch.setattr(model_loader.httpx, "AsyncClient",
                        lambda **opts: real_client(**opts, transport=httpx.MockTransport(handle)))
    monkeypatch.setattr(model_loader, "ProxyChain", lambda: ProxyChain(candidates=[""]))
    monkeypatch.setattr(model_loader, "DEFAULT_MODELS_CONFIG_URL", PRIMARY)
    monkeypatch.setattr(model_loader, "FALLBACK_MODELS_CONFIG_URLS", [MIRROR])
    monkeypatch.setattr(model_loader.app_config, "MODELS_CONFIG_URL", override)
    monkeypatch.setattr(model_loader, "_model_cache", {"models": ["cached"]})
    monkeypatch.setattr(model_loader, "_next_refresh_allowed", 0.0)
    assert asyncio.run(model_loader.refresh_models_config_cache()) == (expected != ["cached"])
    assert seen == urls
    assert asyncio.run(model_loader.get_models_config()) == {"models": expected}
