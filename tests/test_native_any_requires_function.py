"""Native function mode ANY cannot be faithfully projected to text or server search."""
from test_native_cookie_continuation import ACTIONS, BASE
from test_schema_contract_fidelity import MODEL, SchemaRouteHarness


class NativeFunctionMode(SchemaRouteHarness):
    async def test_cookie_rejects_impossible_any_but_keeps_representable_modes(self):
        self.channel = "web_proxy"
        self.reply = [{"text": "normal"}]
        for action in ACTIONS:
            for mode, tools, expected in (
                ("ANY", [], 400), ("ANY", [{"googleSearch": {}}], 400),
                ("AUTO", [], 200), ("NONE", [{"googleSearch": {}}], 200),
                ("ANY", [{"functionDeclarations": [{"name": "weather"}]}], 200),
            ):
                with self.subTest(action=action, mode=mode, tools=tools):
                    self.sent.clear()
                    result = await self.client.post(f"/v1beta/models/{MODEL}:{action}", json={
                        **BASE, "tools": tools, "toolConfig": {"functionCallingConfig": {"mode": mode}}})
                    self.assertEqual(result.status_code, expected)
                    self.assertEqual(len(self.sent), int(expected == 200))
                    if expected == 400:
                        self.assertIn("ANY", result.json()["error"]["message"])
                    elif mode == "ANY":
                        self.assertEqual(self.sent[0]["variables"]["toolConfig"]["functionCallingConfig"]["mode"], mode)
                    elif tools:
                        self.assertEqual(self.sent[0]["variables"]["tools"], tools)
