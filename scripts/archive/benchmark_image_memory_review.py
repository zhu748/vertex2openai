"""Offline image memory measurements; excludes fixtures from Python allocations.

python scripts/benchmark_image_memory_review.py --megabytes 32 --repeats 3
PIL runs in fresh child processes and measures peak RSS above import baseline.
Its baseline function is read from the specified local Git revision only.
"""
import argparse
import asyncio
import gc
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import resource
import statistics
import subprocess
import sys
import tempfile
import time
import tracemalloc
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
_STATE = tempfile.TemporaryDirectory(prefix="v2o-image-memory-benchmark-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(ROOT / "app"))

from fastapi.responses import JSONResponse
from PIL import Image
from http_io import read_limited_bytes
from json_encoding import LargeJSONResponse, iter_json_bytes


def measure(operation, repeats):
    times = []
    for _ in range(repeats):
        started = time.perf_counter()
        result = operation()
        times.append((time.perf_counter() - started) * 1000)
        del result
    gc.collect()
    tracemalloc.start()
    result = operation()
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    del result
    return {"median_ms": round(statistics.median(times), 2), "peak_python_bytes": peak}


class Source:
    headers = {}

    def __init__(self, megabytes):
        self.chunks = megabytes * 16

    async def aiter_bytes(self, chunk_size=None):
        chunk = b"A" * (64 * 1024)
        for _ in range(self.chunks):
            yield chunk


async def old_read(source, limit):
    data = bytearray()
    async for chunk in source.aiter_bytes():
        if len(data) + len(chunk) > limit:
            raise ValueError("too large")
        data.extend(chunk)
    return bytes(data)


def emit(payload, legacy):
    digest = hashlib.sha256()
    if legacy:
        text = json.dumps(payload, ensure_ascii=False)
        frame = "data: " + text + "\n\n"
        digest.update(frame.encode())
    else:
        digest.update(b"data: ")
        for chunk in iter_json_bytes(payload):
            digest.update(chunk)
        digest.update(b"\n\n")
    return digest.hexdigest()


def pixel_child(module_path, image_path):
    spec = importlib.util.spec_from_file_location("measured_image_processing", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    image_bytes = Path(image_path).read_bytes()
    settings = {"img_compress_enabled": True, "img_compress_max_mb": 0,
                "img_compress_max_dim": 1536, "img_compress_quality": 85}
    with mock.patch.object(module.app_state, "get_setting", side_effect=lambda key, default=None: settings.get(key, default)):
        before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        started = time.perf_counter()
        output, mime = module.optimize_image_bytes(image_bytes, "image/png")
        elapsed = (time.perf_counter() - started) * 1000
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss - before
    print(json.dumps({"peak_rss_kib": peak, "ms": elapsed, "sha256": hashlib.sha256(output).hexdigest(), "mime": mime}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--megabytes", type=int, default=32)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--baseline-ref", default="HEAD")
    parser.add_argument("--pixel-child", nargs=2)
    args = parser.parse_args()
    if args.pixel_child:
        pixel_child(*args.pixel_child)
        return
    if args.megabytes < 1 or args.repeats < 1:
        parser.error("megabytes and repeats must be positive")
    part = {"inlineData": {"mimeType": "image/png", "data": "A" * (args.megabytes * 1024 * 1024)}}
    native_payload = {"candidates": [{"content": {"role": "model", "parts": [part]}, "index": 0}]}
    openai_payload = {"choices": [{"message": {"content": part["inlineData"]["data"]}}]}
    assert JSONResponse(openai_payload).body == LargeJSONResponse(openai_payload).body
    assert emit(native_payload, True) == emit(native_payload, False)
    source = Source(args.megabytes)
    limit = args.megabytes * 1024 * 1024
    result = {
        "nonstream_json": {"old": measure(lambda: JSONResponse(openai_payload), args.repeats),
                           "new": measure(lambda: LargeJSONResponse(openai_payload), args.repeats)},
        "native_sse_encoding": {"old": measure(lambda: emit(native_payload, True), args.repeats),
                                "new": measure(lambda: emit(native_payload, False), args.repeats)},
        "bounded_http_read": {"old": measure(lambda: asyncio.run(old_read(source, limit)), args.repeats),
                              "new": measure(lambda: asyncio.run(read_limited_bytes(source, limit)), args.repeats)},
    }
    with tempfile.TemporaryDirectory(dir=_STATE.name) as tmp:
        baseline = Path(tmp) / "baseline_image_processing.py"
        old = subprocess.run(["git", "show", f"{args.baseline_ref}:app/image_processing.py"],
                             cwd=ROOT, capture_output=True, text=True, check=True)
        baseline.write_text(old.stdout)
        image_path = Path(tmp) / "24mp.png"
        with Image.new("RGBA", (6000, 4000), (64, 128, 192, 128)) as image:
            image.save(image_path, format="PNG")
        pixels = {}
        digests = set()
        for name, path in (("old", baseline), ("new", ROOT / "app/image_processing.py")):
            runs = []
            for _ in range(args.repeats):
                proc = subprocess.run([sys.executable, __file__, "--pixel-child", str(path), str(image_path)],
                                      cwd=ROOT, capture_output=True, text=True, check=True)
                runs.append(json.loads(proc.stdout.strip().splitlines()[-1]))
                digests.add(runs[-1]["sha256"])
            pixels[name] = {"median_ms": round(statistics.median(r["ms"] for r in runs), 2),
                            "peak_rss_kib": statistics.median(r["peak_rss_kib"] for r in runs)}
        assert len(digests) == 1, "pixel compression output changed"
        result["input_rgba_24mp"] = pixels
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
