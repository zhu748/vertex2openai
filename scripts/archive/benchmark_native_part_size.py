"""Compare native Part size accounting; offline, excludes fixture allocation.

Run: python scripts/benchmark_native_part_size.py --megabytes 32 --repeats 5
Peak figures cover Python allocations for accounting only, not process RSS or
the actual downstream serialization (which must still produce the response).
"""
import argparse
import gc
import json
import os
from pathlib import Path
import statistics
import sys
import tempfile
import time
import tracemalloc

_STATE = tempfile.TemporaryDirectory(prefix="v2o-part-size-benchmark-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))
from upstreams.cookie_native import _part_json_size


def measure(operation, part, repeats):
    times = []
    for _ in range(repeats):
        started = time.perf_counter()
        size = operation(part)
        times.append((time.perf_counter() - started) * 1000)
    gc.collect()
    tracemalloc.start()
    assert operation(part) == size
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return {"size": size, "median_ms": round(statistics.median(times), 2), "peak_bytes": peak}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--megabytes", type=int, default=32)
    parser.add_argument("--repeats", type=int, default=5)
    args = parser.parse_args()
    if args.megabytes < 1 or args.repeats < 1:
        parser.error("megabytes and repeats must be positive")
    fixtures = {
        "base64": {"inlineData": {"mimeType": "image/png", "data": "A" * (args.megabytes * 1024 * 1024)}},
        "unicode_escaped": {"text": '中文😀\n"\\' * (args.megabytes * 1024 * 1024 // 7)},
    }
    result = {}
    for name, part in fixtures.items():
        old = measure(lambda value: len(json.dumps(value, ensure_ascii=False).encode("utf-8")), part, args.repeats)
        new = measure(_part_json_size, part, args.repeats)
        assert old["size"] == new["size"]
        result[name] = {"old": old, "new": new}
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
