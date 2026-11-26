"""Cookie client wire grouping must not change history or repeatedly visit earlier Parts."""
import itertools
import json
import unittest
from copy import deepcopy

from upstreams.cookie_request import _merge_cookie_turns


def reference(contents):
    """Original grouping specification; intentionally rescans completed runs."""
    merged = []
    for turn in contents:
        previous = merged[-1] if merged else None
        parts = turn["parts"]
        if previous and previous["role"] == turn["role"]:
            old = previous["parts"]
            was_result = bool(old) and all("functionResponse" in p for p in old)
            is_result = bool(parts) and all("functionResponse" in p for p in parts)
            if was_result == is_result:
                if not is_result:
                    old.append({"text": "\n\n"})
                old.extend(parts)
                continue
        merged.append(turn)
    return merged


class TurnMerge(unittest.TestCase):
    def test_mixed_histories_keep_exact_json_including_cookie_newline_part(self):
        cases = [
            {"role": "user", "parts": []},
            {"role": "model", "parts": [{"text": "好"}]},
            {"role": "user", "parts": [{"functionResponse": {"name": "f", "id": "1"}}]},
            {"role": "user", "parts": [{"text": "ok"}, {"inlineData": {"data": "AA=="}}]},
            {"role": "user", "parts": [{"functionResponse": {}}, {"text": "mixed"}]},
        ]
        for sample in itertools.product(cases, repeat=4):
            # Each source turn owns a different parts list, as in request construction.
            expected = reference([deepcopy(t) for t in sample])
            actual = _merge_cookie_turns([deepcopy(t) for t in sample])
            self.assertEqual(json.dumps(actual, ensure_ascii=False), json.dumps(expected, ensure_ascii=False))

    def test_each_part_is_classified_once_in_long_parallel_result_history(self):
        checks = []
        class CountingPart(dict):
            def __contains__(self, key):
                if key == "functionResponse":
                    checks.append(self["id"])
                return super().__contains__(key)

        turns = [{"role": "user", "parts": [CountingPart(functionResponse={}, id=i)]} for i in range(2048)]
        result, = _merge_cookie_turns(turns)
        self.assertEqual([p["id"] for p in result["parts"]], list(range(2048)))
        self.assertEqual(checks, list(range(2048)))
