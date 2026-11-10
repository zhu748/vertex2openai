"""Offline memory comparison: retained response parts versus streaming assembly.

Usage: python scripts/benchmark_native_assembly.py --parts 10000 --part-bytes 1024
This measures the assembler only, not network throughput or total process RSS.
"""
import argparse
import gc
import json
import os
import sys
import tempfile
import time
import tracemalloc
from pathlib import Path

_STATE = tempfile.TemporaryDirectory(prefix="v2o-assembly-benchmark-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))
from upstreams.cookie_native import NativeAssembler


def measure(parts, part_bytes, *, retain):
    gc.collect()
    tracemalloc.start()
    started = time.perf_counter()
    assembler = NativeAssembler("offline-benchmark", retain_parts=retain)
    for index in range(parts):
        assembler.feed("native_part", {"text": str(index) + ":" + "x" * part_bytes})
    elapsed = time.perf_counter() - started
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return {"parts": assembler.part_count, "retained_parts": len(assembler.parts),
            "retained_bytes": current, "peak_bytes": peak, "seconds": round(elapsed, 4)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parts", type=int, default=10_000)
    parser.add_argument("--part-bytes", type=int, default=1024)
    args = parser.parse_args()
    if args.parts < 1 or args.part_bytes < 1:
        parser.error("parts and part-bytes must be positive")
    result = {"retained_assembly": measure(args.parts, args.part_bytes, retain=True),
              "streaming_assembly": measure(args.parts, args.part_bytes, retain=False)}
    sys.__stdout__.write(json.dumps(result, indent=2) + "\n")
    sys.__stdout__.flush()


if __name__ == "__main__":
    main()
