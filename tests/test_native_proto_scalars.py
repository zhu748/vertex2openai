"""ProtoJSON observations are typed and independent of the pass-through wire spelling."""
from unittest import TestCase

from protojson import int32_value
from upstreams.native_passthrough import native_expects_one_candidate
from test_schema_contract_fidelity import MODEL, SchemaRouteHarness


class ProtoScalarTests(TestCase):
    def test_first_reply_requires_one_unambiguous_candidate(self):
        for key in ("candidateCount", "candidate_count"):
            for value in (None, 1, 1.0, "1", "1.0", "1e0", "10e-1"):
                self.assertTrue(native_expects_one_candidate({"generationConfig": {key: value}}), value)
            for value in (2, True, False, "", "true", "1.1", -1, {}, "9e99999999999999999999999"):
                self.assertFalse(native_expects_one_candidate({"generationConfig": {key: value}}), value)
        self.assertTrue(native_expects_one_candidate({"generation_config": None}))
        self.assertFalse(native_expects_one_candidate({"generationConfig": None, "generation_config": {}}))
        self.assertFalse(native_expects_one_candidate({"generationConfig": {"candidate_count": None,
                                                                           "candidateCount": 1}}))
        self.assertEqual(int32_value("-2147483648"), -2147483648)
        self.assertIsNone(int32_value("2147483648"))
        self.assertIsNone(int32_value(float("nan")))


class NativeCookieScalarRoutes(SchemaRouteHarness):
    async def test_cookie_uses_int_only_and_omits_declared_null_fields(self):
        self.channel = "web_proxy"
        for action in ("generateContent", "streamGenerateContent", "streamGenerateContent?alt=sse"):
            for key in ("candidateCount", "candidate_count"):
                for value, status in (("1", 200), ("1.0", 200), (True, 400), ("2", 400), (None, 200)):
                    with self.subTest(key=key, value=value, action=action):
                        self.sent.clear()
                        config = {key: value, "logprobs": None, "response_json_schema": None}
                        r = await self.client.post(f"/v1beta/models/{MODEL}:{action}", json={
                            "contents": [{"parts": [{"text": "hi"}]}], "generation_config": config})
                        self.assertEqual(r.status_code, status, r.text[:200])
                        self.assertEqual(len(self.sent), int(status == 200))
                        if self.sent:
                            g = self.sent[0]["variables"]["generationConfig"]
                            self.assertNotIn("logprobs", g)
                            self.assertNotIn("responseJsonSchema", g)
                            if value is None:
                                self.assertNotIn("candidateCount", g)
                            else:
                                self.assertIs(type(g.get("candidateCount")), int)
                                self.assertEqual(g["candidateCount"], 1)
