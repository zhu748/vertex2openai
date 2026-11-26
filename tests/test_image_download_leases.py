"""Offline concurrent downloads through the production fetch entry point."""

from concurrent.futures import ThreadPoolExecutor
import threading
from unittest import mock

import httpx
import image_processing as images


def test_eviction_does_not_close_an_inflight_redirected_download():
    images.reset_image_clients()
    real_client = httpx.Client
    ready, resume = threading.Event(), threading.Event()
    clients = {}
    local = threading.local()

    class PausedImage(httpx.SyncByteStream):
        def __iter__(self):
            ready.set()
            assert resume.wait(5)
            if clients["http://config0.test"].is_closed:
                raise RuntimeError("active image client closed before download finished")
            yield b"picture"

    def make_client(**opts):
        key = opts["proxy"]

        def serve(request):
            if request.url.path == "/redirect":
                return httpx.Response(302, headers={"location": "/picture"})
            if key == "http://config0.test" and request.url.path == "/picture":
                return httpx.Response(200, headers={"content-type": "image/png"}, stream=PausedImage())
            return httpx.Response(200, headers={"content-type": "image/png"}, content=b"picture")

        clients[key] = real_client(transport=httpx.MockTransport(serve))
        return clients[key]

    def fetch(config, path):
        local.proxy = f"http://config{config}.test"
        return images.fetch_remote_image("https://images.example" + path)

    try:
        with mock.patch.object(images, "get_effective_proxy_urls", side_effect=lambda: [local.proxy]), \
             mock.patch("proxy_resolver.resolve_proxy_uri", side_effect=lambda url: url), \
             mock.patch.object(images, "_public_image_addresses", return_value=["192.0.2.1"]), \
             mock.patch.object(images.httpx, "Client", side_effect=make_client), \
             ThreadPoolExecutor(max_workers=1) as executor:
            first = executor.submit(fetch, 0, "/redirect")
            try:
                assert ready.wait(5)
                # The oldest configuration is active; every later fetch has a different pool key.
                for config in range(1, 17):
                    assert fetch(config, "/small") == (b"picture", "image/png")
            finally:
                resume.set()
            assert first.result(5) == (b"picture", "image/png")
    finally:
        images.reset_image_clients()
    assert all(client.is_closed for client in clients.values())


def test_waits_for_slot_and_retires_on_last_lease_or_error():
    from image_client_pool import ImageClientPool

    pool = ImageClientPool(capacity=1)
    first, second = mock.Mock(), mock.Mock()
    attempted = threading.Event()
    make_second = mock.Mock(return_value=second)

    def budget():
        attempted.set()
        return 2

    def fetch_second():
        with pool.lease("B", make_second, budget):
            assert first.close.call_count == 1

    with ThreadPoolExecutor(max_workers=1) as executor:
        try:
            with pool.lease("A", lambda: first, lambda: 2) as p:
                with pool.lease("A", lambda: None, lambda: 2) as q:
                    assert p is q is first
                    other = executor.submit(fetch_second)
                    assert attempted.wait(2)
                    pool.reset()
                    assert first.close.call_count == 0
                assert first.close.call_count == 0
                assert not make_second.called
                raise ValueError("simulated download failure")
        except ValueError as error:
            assert str(error) == "simulated download failure"
        other.result(2)
    with pool.lease("B", lambda: None, lambda: 2) as b:
        assert b is second
    pool.reset()
    pool.reset()
    first.close.assert_called_once()
    second.close.assert_called_once()


def test_pool_contention_respects_request_budget_without_evicting_active_client():
    import time
    import pytest
    from image_client_pool import ImageClientPool

    pool = ImageClientPool(capacity=1)
    first = mock.Mock()
    make_second = mock.Mock()
    with pool.lease("A", lambda: first, lambda: 1):
        deadline = time.monotonic() + 0.02

        def remaining():
            seconds = deadline - time.monotonic()
            if seconds <= 0:
                raise TimeoutError("request budget exhausted")
            return seconds

        with pytest.raises(TimeoutError, match="request budget exhausted"), \
             pool.lease("B", make_second, remaining):
            raise AssertionError("no client slot should be allocated")
        first.close.assert_not_called()
        make_second.assert_not_called()
    pool.reset()
    first.close.assert_called_once()
