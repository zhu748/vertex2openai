"""Offline hot-path timing and allocation sample; not a production load test.

Run with the same interpreter/revision settings before and after a change.
Fixtures are outside the measured window; results are median wall time and
peak additional Python allocations for one operation, not process RSS.
"""
from contextlib import redirect_stdout
import gc
import io
import json
import os
from pathlib import Path
import statistics
import sys
import tempfile
import time
import tracemalloc

_STATE = tempfile.TemporaryDirectory(prefix="v2o-hot-paths-")
os.environ["STATE_DIR"] = _STATE.name
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from google.genai import types
from request_tracking import RequestRegistry
from tool_stream import _Arguments


def measure(operation, repeats=9):
    times = []
    for _ in range(repeats):
        start = time.perf_counter()
        result = operation()
        times.append((time.perf_counter() - start) * 1000)
        del result
    gc.collect()
    tracemalloc.start()
    result = operation()
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    del result
    return {"median_ms": round(statistics.median(times), 3), "peak_bytes": peak}


def main():
    fragments = [types.PartialArg(json_path="$.text", string_value="中文剧情喵" * 256,
                                 will_continue=i < 255) for i in range(256)]
    def assemble():
        arguments = _Arguments()
        for fragment in fragments:
            arguments.add(fragment)
        result = arguments.finish()
        assert len(result["text"]) == 327680
        return result

    registry = RequestRegistry()
    with redirect_stdout(io.StringIO()):
        for i in range(500):
            trace = registry.create("/v1/chat/completions")
            trace.configure(model=f"gemini-offline-{i % 4}")
            trace.timings = {f"stage{n}": {"at": float(n), "elapsed_ms": n} for n in range(12)}
            trace.end_connection()
    def page():
        result = registry.list(limit=25)
        assert result["total"] == 500 and len(result["items"]) == 25
        return result

    result = {"tool_327680_chars_256_fragments": measure(assemble),
              "monitor_500_records_page_25": measure(page)}
    sys.__stdout__.write(json.dumps(result, indent=2) + "\n")
    sys.__stdout__.flush()


if __name__ == "__main__":
    main()
