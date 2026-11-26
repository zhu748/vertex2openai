"""Image downloads preserve bytes/budgets without a second whole-body allocation."""

from unittest import mock
import tracemalloc

import httpx
import image_processing as images
import pytest


@pytest.fixture
def fetch_chunks():
    images.reset_image_clients()
    try:
        yield _fetch_chunks
    finally:
        images.reset_image_clients()


def _fetch_chunks(chunks, *, budget=lambda: 10, mime="image/png; test=1"):
    closed = []
    class Pieces(httpx.SyncByteStream):
        def __iter__(self):
            yield from chunks

        def close(self):
            closed.append(True)

    client = httpx.Client(transport=httpx.MockTransport(lambda req: httpx.Response(
        200, headers={"content-type": mime}, stream=Pieces())))
    with mock.patch.object(images.httpx, "Client", return_value=client), \
         mock.patch.object(images, "_public_image_addresses", return_value=["192.0.2.1"]), \
         mock.patch.object(images, "check_media_budget", side_effect=budget), \
         mock.patch.object(images, "get_effective_proxy_urls", return_value=[""]):
        result = images.fetch_remote_image("https://images.example/sample.png")
    assert closed == [True]
    return result


def test_large_result_has_one_output_buffer(fetch_chunks):
    piece = bytes(range(256)) * 256
    count = 64
    size = len(piece) * count
    # Leave room for BytesIO growth, HTTPX metadata and a few chunks, not two full bodies.
    tracemalloc.start()
    try:
        result = fetch_chunks(piece for _ in range(count))
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()
    assert result == (piece * count, "image/png")
    assert peak < size * 1.7


@pytest.mark.parametrize(("chunks", "expected"), [
    ([b"abcd", b"", b"efgh"], (b"abcdefgh", "image/png")),
    ([], (b"", "image/png")),
    ([b"abcd", b"efghi"], None),
])
def test_exact_limit_and_empty_body(fetch_chunks, monkeypatch, chunks, expected):
    monkeypatch.setattr(images, "MAX_REMOTE_IMAGE_BYTES", 8)
    assert fetch_chunks(chunks) == expected


def test_finish_rechecks_deadline_and_closes_response(fetch_chunks):
    expired = False
    def chunks():
        nonlocal expired
        yield b"picture"
        expired = True

    def remaining():
        if expired:
            raise TimeoutError("test deadline")
        return 10

    assert fetch_chunks(chunks(), budget=remaining) is None
