"""Short-lived fallback storage for Gemini thought-signature topology.

OpenAI-compatible responses carry signatures in ``extra_content.google``. This
store is deliberately only a fallback for clients that strip that extension. A
record has three possible states because ``None`` alone is ambiguous:

* ``SIGNED``: this exact function-call part carried a signature.
* ``UNSIGNED_FOLLOWER``: this was a later call in a parallel batch and must stay
  unsigned when replayed.
* ``AMBIGUOUS``: different live turns claimed this ID; never guess either one.
* ``UNKNOWN``: signature topology was lost; validation may need the documented
  skip sentinel for the first call of a step.
"""

import hashlib
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum
from typing import Optional

SKIP_VALIDATOR_SENTINEL = b"skip_thought_signature_validator"

# Google thought-signatures 文档 FAQ：无法恢复签名时，可在 thoughtSignature
# 字段填充官方 dummy 值跳过校验，documented 的合法字面量有两个：
#   "skip_thought_signature_validator" 与 "context_engineering_is_the_way_to_go"
#
# ⚠️ 两条出站通道对哨兵的 wire 编码不同（2026-11-16 真机验证，勿“顺手统一”）：
#   * Express / REST（aiplatform JSON，string 字段）——发送官方字面量字符串
#     （唯一 documented 形态；langchain-google#1570 实证 Vertex 端点拒绝
#     base64 形态，其官方修复 #1972 亦改为发字面量），见
#     thought_signatures.sdk_thought_signature_field；
#   * Cookie / batchGraphql（GraphQL Bytes 标量）——必须 base64 编码，字面量中的
#     "_" 不是合法 base64 字符，会被校验层拒绝（"Invalid value for variable
#     $contents ... invalid value for type Bytes"，真机对比实测），走原有的
#     encode_thought_signature 即可。
#
# 2026-11-17 Express 真机 A/B（Express Key + socks5 出站 + gemini-3.5-flash-lite）：
#   修复前后全链路各 3 次伪造无签名工具调用均 200，模型均基于工具结果作答；
#   四形态同位探针——字面量哨兵 200、base64 哨兵 200（该 Express 端点当日
#   宽容）、随机 48 字节伪签名 400 "Invalid thought signature."、缺失签名
#   400 "Function call is missing a thought_signature"。验签确实在跑，
#   哨兵兜底必需；但 base64 形态的通过属未文档化宽容行为（仅本端点当日
#   如此，langchain 实测的 Vertex 端点拒绝该形态），故字面量出站不变。
SKIP_VALIDATOR_SENTINELS = (
    b"skip_thought_signature_validator",
    b"context_engineering_is_the_way_to_go",
)

DEFAULT_TTL_SECONDS = 2 * 60 * 60
DEFAULT_MAX_ENTRIES = 5000


class SignatureState(str, Enum):
    SIGNED = "signed"
    UNSIGNED_FOLLOWER = "unsigned_follower"
    UNKNOWN = "unknown"
    AMBIGUOUS = "ambiguous"


@dataclass(frozen=True)
class SignatureRecord:
    state: SignatureState
    signature: Optional[bytes] = None

    def __post_init__(self) -> None:
        if self.state is SignatureState.SIGNED and not self.signature:
            raise ValueError("signed signature records require signature bytes")
        if self.state is not SignatureState.SIGNED and self.signature is not None:
            raise ValueError("unsigned/unknown signature records cannot carry bytes")


CacheKey = str | bytes  # Public IDs are strings; private exact-origin keys are digests.


class SignatureStore:
    """Thread-safe tool_call_id -> :class:`SignatureRecord` TTL/LRU cache."""

    def __init__(self, ttl_seconds: int = DEFAULT_TTL_SECONDS, max_entries: int = DEFAULT_MAX_ENTRIES):
        self._ttl = ttl_seconds
        self._max = max_entries
        self._lock = threading.Lock()
        self._data: OrderedDict[CacheKey, tuple[float, SignatureRecord]] = OrderedDict()
        # Reads reorder the LRU, while absolute expiry follows the last write.
        self._expiry: OrderedDict[CacheKey, float] = OrderedDict()
        # An LRU miss need not mean "never issued". Remember only digests of
        # evicted live keys; beyond this second bound, fail closed on new keys
        # until the forgotten quarantine expires.
        self._displaced: OrderedDict[bytes, float] = OrderedDict()
        self._displaced_conflicts: set[bytes] = set()  # Only tags already in _displaced.
        self._unknown_until = float("-inf")

    @staticmethod
    def _tag(key: CacheKey) -> bytes:
        raw = b"s" + key.encode("utf-8", "surrogatepass") if isinstance(key, str) else b"b" + key
        return hashlib.blake2b(raw, digest_size=16).digest()

    def put_record(self, call_id: CacheKey, record: SignatureRecord) -> None:
        if not call_id:
            return
        with self._lock:
            now = time.monotonic()
            prev = self._data.get(call_id)
            if prev is not None and now - prev[0] <= self._ttl:
                if prev[1] != record:
                    record = SignatureRecord(SignatureState.AMBIGUOUS)
            elif (now <= self._unknown_until or
                  self._displaced and now <= self._displaced.get(self._tag(call_id), float("-inf"))):
                record = SignatureRecord(SignatureState.AMBIGUOUS)
            self._data[call_id] = (now, record)
            self._data.move_to_end(call_id)
            self._expiry[call_id] = now
            self._expiry.move_to_end(call_id)
            self._evict(now)

    def register_call_signature(self, call_id: CacheKey, signature: Optional[bytes],
                                *, unsigned_follower: bool = False) -> None:
        """QUA-P2-2（2026-11 审查收敛）：工具调用签名三态写入的单一入口。

        此前 thought_signatures.build_tool_call_id 与
        cookie_response.openai_tool_call 各自重复同一套
        「有签名→SIGNED / 同批后续调用无签名→UNSIGNED_FOLLOWER /
        其余→UNKNOWN」分叉；现在生产方只需提供签名字节与
        「是否同批后续调用」的判定，拓扑语义归本模块单点维护。
        """
        if signature:
            self.put_record(call_id, SignatureRecord(SignatureState.SIGNED, signature))
        elif unsigned_follower:
            self.put_unsigned_follower(call_id)
        else:
            self.put_unknown(call_id)

    def put_unsigned_follower(self, call_id: CacheKey) -> None:
        self.put_record(call_id, SignatureRecord(SignatureState.UNSIGNED_FOLLOWER))

    def put_unknown(self, call_id: CacheKey) -> None:
        self.put_record(call_id, SignatureRecord(SignatureState.UNKNOWN))

    def get_record(self, call_id: CacheKey) -> Optional[SignatureRecord]:
        if not call_id:
            return None
        with self._lock:
            now = time.monotonic()
            item = self._data.get(call_id)
            if item is None:
                if self._displaced_conflicts:
                    tag = self._tag(call_id)
                    if tag in self._displaced_conflicts and now <= self._displaced[tag]:
                        # A retained live conflict is not an unknown cache miss.
                        # Never mint a validator fallback for either the bare ID
                        # or an indistinguishable private origin.
                        return SignatureRecord(SignatureState.AMBIGUOUS)
                return None
            ts, record = item
            if now - ts > self._ttl:
                self._data.pop(call_id, None)
                self._expiry.pop(call_id, None)
                return None
            self._data.move_to_end(call_id)
            return record

    def _evict(self, now: float) -> None:
        while self._expiry:
            oldest_key, ts = next(iter(self._expiry.items()))
            if now - ts > self._ttl:
                self._expiry.popitem(last=False)
                self._data.pop(oldest_key, None)
                continue
            break
        while self._displaced and now > next(iter(self._displaced.values())):
            tag, _ = self._displaced.popitem(last=False)
            self._displaced_conflicts.discard(tag)
        while len(self._data) > self._max:
            oldest_key, (_, evicted) = self._data.popitem(last=False)
            self._expiry.pop(oldest_key, None)
            tag = self._tag(oldest_key)
            self._displaced[tag] = now + self._ttl
            self._displaced.move_to_end(tag)
            if evicted.state is SignatureState.AMBIGUOUS:
                self._displaced_conflicts.add(tag)
        while len(self._displaced) > self._max:
            tag, until = self._displaced.popitem(last=False)
            self._displaced_conflicts.discard(tag)
            self._unknown_until = max(self._unknown_until, until)


signature_store = SignatureStore()
