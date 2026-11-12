"""Compare exact JSON byte accounting with and without whole-response copies."""

import argparse
import json
from pathlib import Path
import sys
import time
import tracemalloc

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))
from json_encoding import encoded_json_size


def measure(function, payload):
    tracemalloc.start()
    try:
        started = time.perf_counter()
        size = function(payload)
        elapsed = time.perf_counter() - started
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    return {"accounted_bytes": size, "peak_bytes": peak,
            "elapsed_seconds": round(elapsed, 4)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--megabytes", type=int, default=8)
    args = parser.parse_args()
    if args.megabytes < 1:
        parser.error("megabytes must be positive")
    payload = {"choices": [{"message": {"content": "x" * (args.megabytes * 1024 * 1024)}}]}
    before = measure(lambda value: len(json.dumps(value, ensure_ascii=False).encode("utf-8")), payload)
    after = measure(encoded_json_size, payload)
    if before["accounted_bytes"] != after["accounted_bytes"]:
        raise AssertionError("JSON byte accounting changed")
    print(json.dumps({"megabytes": args.megabytes, "whole_response": before, "bounded_chunks": after}))


if __name__ == "__main__":
    main()
