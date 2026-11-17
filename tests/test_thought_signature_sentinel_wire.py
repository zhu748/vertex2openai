"""thought_signature 哨兵出站 wire 级测试（2026-11 修复回归）。

背景：缺失签名时 resolve_tool_call_signature 会生成官方哨兵
``b"skip_thought_signature_validator"``。两条出站通道对哨兵的 wire 编码
**不同**（2026-11-16 真机对比实测，勿"顺手统一"）：

* Express（google-genai SDK → Vertex REST，thoughtSignature 是 JSON string）
  ——必须发送官方**字面量** ``"skip_thought_signature_validator"``
  （官方 thought-signatures FAQ；langchain-google#1570 实证 base64 会被拒。
  2026-11-17 真机 A/B：Express 端点当日对 base64 哨兵亦放行，宽容未文档化，
  字面量仍为唯一 documented 形态——测试继续锁定字面量）。
  旧实现把哨兵 bytes 直接交给 ``types.Part``，SDK 对 bytes 统一 base64 序列化
  （``ser_json_bytes='base64'`` / ``encode_unserializable_types``），wire 上变成
  ``"c2tpcF90aG91..."``，上游拒绝跳过校验。修复：哨兵以 str 传给 Part，
  pydantic 宽松验证 base64 解码 + SDK urlsafe 重编码恰好无损还原字面量
  （哨兵被 Google 设计为 32 个 urlsafe-b64 字符）。

* Cookie（batchGraphql 私有 GraphQL，thoughtSignature 是 **Bytes 标量**）
  ——必须 base64 编码（与真实签名同一口径）。字面量中的 "_" 不是合法
  base64 字符，会被校验层拒绝（"Invalid value for variable $contents ...
  invalid value for type Bytes"，真机实测）。原实现本就正确，此处锁定行为
  防止后人误改为字面量。

本文件沿两条通道**真实的出站序列化管线**验证最终请求体：

* Express：``create_gemini_prompt`` 产出的 Part 经 SDK models.py 每次发请求
  实际调用的 ``convert_to_dict`` + ``encode_unserializable_types`` +
  ``json.dumps``（google-genai 2.31.0 models.py 中的固定两步）；
* Cookie：``_convert_messages_to_contents`` 产出的 batchGraphql contents dict。

若未来 pydantic / SDK 的 bytes 序列化策略变化破坏字面量往返，这里会最先红。
"""

import base64
import json
import unittest
import uuid

from google.genai import types
from google.genai._common import convert_to_dict, encode_unserializable_types

from message_processing import create_gemini_prompt
from models import OpenAIMessage
from signature_store import (
    SKIP_VALIDATOR_SENTINEL,
    SKIP_VALIDATOR_SENTINELS,
    signature_store,
)
from thought_signatures import (
    resolve_tool_call_signature,
    sdk_thought_signature_field,
)
from upstreams.cookie_request import _convert_messages_to_contents

G3_MODEL = "gemini-3.5-flash-lite"
LITERAL = "skip_thought_signature_validator"
CTX_LITERAL = "context_engineering_is_the_way_to_go"
# GraphQL Bytes 标量要求的哨兵出站值：标准 base64(sentinel)
GRAPHQL_B64 = base64.b64encode(SKIP_VALIDATOR_SENTINEL).decode("ascii")


def sdk_wire_body(content: types.Content) -> dict:
    """复刻 google-genai models.py 发送 generateContent 的序列化两步。"""
    request = {"contents": [convert_to_dict(content)]}
    return encode_unserializable_types(request)


def unsigned_tool_call_messages(*, extra_first=None, parallel=False):
    """user 提问 + 伪造（无签名）assistant 工具调用的最小对话。"""
    calls = [{
        "id": "call_" + uuid.uuid4().hex[:16],
        "type": "function",
        "function": {"name": "get_weather", "arguments": '{"city": "北京"}'},
    }]
    if parallel:
        calls.append({
            "id": "call_" + uuid.uuid4().hex[:16],
            "type": "function",
            "function": {"name": "get_time", "arguments": '{"tz": "UTC"}'},
        })
    if extra_first is not None:
        calls[0]["extra_content"] = {"google": {"thought_signature": extra_first}}
    messages = [
        OpenAIMessage(role="user", content="北京今天天气如何？"),
        OpenAIMessage(role="assistant", content="", tool_calls=calls),
        OpenAIMessage(role="tool", tool_call_id=calls[0]["id"],
                      name="get_weather", content='{"temp_c": 21}'),
    ]
    return messages


class HelperUnitTests(unittest.TestCase):
    """SDK 出站辅助函数的纯单元断言（含第二官方哨兵与真实签名对照）。"""

    def test_sdk_field_sentinel_becomes_literal_str(self):
        for sentinel in SKIP_VALIDATOR_SENTINELS:
            with self.subTest(sentinel=sentinel):
                value = sdk_thought_signature_field(sentinel)
                self.assertIsInstance(value, str)
                self.assertEqual(value, sentinel.decode("ascii"))

    def test_sdk_field_real_signature_and_none_pass_through(self):
        real = bytes(range(48))
        self.assertIs(sdk_thought_signature_field(real), real)
        self.assertIsNone(sdk_thought_signature_field(None))
        # 空值契约：辅助函数只翻译哨兵，不做归一（调用方以真值判断门控）。
        self.assertEqual(sdk_thought_signature_field(b""), b"")

    def test_skip_fallback_still_resolves_to_sentinel_bytes(self):
        """兜底本身不回归：缺失签名仍解析出哨兵 bytes（出站转换另做）。"""
        unknown_id = "call_" + uuid.uuid4().hex[:16]
        real_id, sig = resolve_tool_call_signature(unknown_id, require_signature=True)
        self.assertEqual(real_id, unknown_id)
        self.assertEqual(sig, SKIP_VALIDATOR_SENTINEL)
        # 并行 follower 与未要求签名的模型不产生哨兵
        _, follower = resolve_tool_call_signature(
            "call_" + uuid.uuid4().hex[:16], require_signature=False)
        self.assertIsNone(follower)


class ExpressWireTests(unittest.TestCase):
    """Express 通道：SDK 真实出站序列化管线里的 thoughtSignature 值。"""

    def _model_function_parts(self, messages):
        contents = create_gemini_prompt(messages, G3_MODEL)
        self.assertTrue(contents)
        parts = []
        for content in contents:
            if content.role == "model":
                parts.extend(content.parts)
        return [p for p in parts if p.function_call is not None]

    def test_sentinel_reaches_wire_as_official_literal(self):
        messages = unsigned_tool_call_messages()
        parts = self._model_function_parts(messages)
        self.assertEqual(len(parts), 1)
        body = sdk_wire_body(types.Content(role="model", parts=parts))
        wire_text = json.dumps(body, ensure_ascii=False)
        wire_sig = body["contents"][0]["parts"][0]["thought_signature"]
        self.assertEqual(wire_sig, LITERAL)
        # 旧 bug 形态：bytes 哨兵被 SDK base64 破坏成这个值
        self.assertNotEqual(wire_sig, GRAPHQL_B64)
        self.assertNotIn(GRAPHQL_B64, wire_text)

    def test_real_signature_stays_base64_not_literal(self):
        real = bytes(range(48))
        messages = unsigned_tool_call_messages(
            extra_first=base64.b64encode(real).decode("ascii"))
        parts = self._model_function_parts(messages)
        body = sdk_wire_body(types.Content(role="model", parts=parts))
        wire_sig = body["contents"][0]["parts"][0]["thought_signature"]
        self.assertNotEqual(wire_sig, LITERAL)
        # SDK 出站管线用 urlsafe base64；无论哪种 base64，解码后都必须还原
        # 真实签名字节，而不是被字面量/哨兵替换。
        decoded = base64.urlsafe_b64decode(wire_sig + "===")
        self.assertEqual(decoded, real)

    def test_client_supplied_second_sentinel_survives_wire(self):
        messages = unsigned_tool_call_messages(extra_first=CTX_LITERAL)
        parts = self._model_function_parts(messages)
        body = sdk_wire_body(types.Content(role="model", parts=parts))
        wire_sig = body["contents"][0]["parts"][0]["thought_signature"]
        self.assertEqual(wire_sig, CTX_LITERAL)

    def test_parallel_calls_only_first_carries_sentinel(self):
        messages = unsigned_tool_call_messages(parallel=True)
        parts = self._model_function_parts(messages)
        self.assertEqual(len(parts), 2)
        body = sdk_wire_body(types.Content(role="model", parts=parts))
        wire_parts = body["contents"][0]["parts"]
        self.assertEqual(wire_parts[0].get("thought_signature"), LITERAL)
        # 同批后续并行调用保持无签名（官方拓扑：仅第一条带签名）
        self.assertIsNone(wire_parts[1].get("thought_signature"))


class CookieWireTests(unittest.TestCase):
    """Cookie 通道：batchGraphql contents dict 里的 thoughtSignature 值。

    thoughtSignature 是 GraphQL **Bytes 标量**：哨兵与真实签名同样必须
    base64 编码。字面量会被校验层拒绝（2026-11-16 真机对比实测，见
    signature_store.SKIP_VALIDATOR_SENTINELS 的说明）。此处锁定行为，
    防止后人参照 Express 通道把这里也改成字面量。
    """

    def _contents(self, messages):
        contents, _names = _convert_messages_to_contents(messages, G3_MODEL)
        self.assertTrue(contents)
        return contents

    def _model_call_parts(self, contents):
        parts = []
        for content in contents:
            if content.get("role") == "model":
                parts.extend(p for p in content["parts"] if "functionCall" in p)
        return parts

    def test_sentinel_is_base64_in_contents(self):
        contents = self._contents(unsigned_tool_call_messages())
        call_parts = self._model_call_parts(contents)
        self.assertEqual(len(call_parts), 1)
        wire_sig = call_parts[0].get("thoughtSignature")
        # GraphQL Bytes 标量：哨兵必须 base64；字面量会被真机拒绝
        #（"invalid value for type Bytes"）。
        self.assertEqual(wire_sig, GRAPHQL_B64)
        self.assertNotEqual(wire_sig, LITERAL)
        self.assertEqual(base64.b64decode(wire_sig), SKIP_VALIDATOR_SENTINEL)

    def test_real_signature_stays_base64_in_contents(self):
        real = bytes(range(48))
        messages = unsigned_tool_call_messages(
            extra_first=base64.b64encode(real).decode("ascii"))
        contents = self._contents(messages)
        wire_sig = self._model_call_parts(contents)[0].get("thoughtSignature")
        self.assertNotEqual(wire_sig, LITERAL)
        self.assertEqual(base64.b64decode(wire_sig), real)

    def test_parallel_calls_only_first_carries_sentinel(self):
        contents = self._contents(unsigned_tool_call_messages(parallel=True))
        call_parts = self._model_call_parts(contents)
        self.assertEqual(len(call_parts), 2)
        self.assertEqual(call_parts[0].get("thoughtSignature"), GRAPHQL_B64)
        self.assertIsNone(call_parts[1].get("thoughtSignature"))


class StoreIsolation(unittest.TestCase):
    """resolve 只读存储；用随机 id 保证用例间互不串扰（无需清理全局 store）。"""

    def test_unknown_id_with_store_miss_falls_back_to_sentinel(self):
        # 显式覆盖「store 命中优先于哨兵」的顺序不被本文件其它用例污染：
        # 注册一条真实签名后，同一 id 必须还原真实签名而不是哨兵。
        call_id = "call_" + uuid.uuid4().hex[:16]
        real = bytes(range(48))
        signature_store.register_call_signature(call_id, real)
        _, sig = resolve_tool_call_signature(call_id, require_signature=True)
        self.assertEqual(sig, real)
        _, missing = resolve_tool_call_signature(
            "call_" + uuid.uuid4().hex[:16], require_signature=True)
        self.assertEqual(missing, SKIP_VALIDATOR_SENTINEL)


if __name__ == "__main__":
    unittest.main()
