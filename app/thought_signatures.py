"""思考签名元数据、工具调用标识与签名拓扑恢复。"""

import base64
import binascii
import hashlib
import uuid
from typing import Any, List, Optional, Tuple

import model_capabilities as mc
from google.genai import types
from ordinary_history import exact_ordinary_slots
from schema_validation import SchemaValidationError
from signature_store import (
    SKIP_VALIDATOR_SENTINEL,
    SKIP_VALIDATOR_SENTINELS,
    SignatureState,
    signature_store,
)

LEGACY_THOUGHT_SEP = "__thought__"
_LITERAL_ID_KEY = "literal_call_id_sha256_v1"


_URL_TO_STANDARD = str.maketrans("-_", "+/")
_SIGNATURE_LITERALS = frozenset(s.decode("ascii") for s in SKIP_VALIDATOR_SENTINELS)


def _protojson_signature(value: str) -> Optional[bytes]:
    """Strict ProtoJSON bytes, in either alphabet, with optional complete padding."""
    if not value or value in _SIGNATURE_LITERALS:
        return None
    normalized = value
    if "-" in value or "_" in value:
        if "+" in value or "/" in value:
            return None
        normalized = value.translate(_URL_TO_STANDARD)
    if "=" not in normalized:
        normalized += "=" * (-len(normalized) % 4)
    try:
        raw = base64.b64decode(normalized, validate=True)
    except (ValueError, binascii.Error):
        return None
    # Reject excess/misplaced pad and nonzero unused bits instead of interpreting
    # an arbitrary literal as a different opaque byte sequence.
    return raw if base64.b64encode(raw).decode("ascii") == normalized else None


def decode_thought_signature(value: Any) -> Optional[bytes]:
    """SDK bytes are already decoded; retain historical literal fallback for text."""
    if isinstance(value, bytes):
        return value or None
    if not isinstance(value, str) or not value:
        return None
    raw = _protojson_signature(value)
    return raw if raw is not None else value.encode("utf-8")


def canonical_wire_signature(value: Any):
    """Normalize actual bytes for Cookie's Bytes field; leave legacy wire literals alone."""
    raw = _protojson_signature(value) if isinstance(value, str) else value
    return encode_thought_signature(raw) if isinstance(raw, bytes) else value


def signature_bytes(part: Any, fc: Any) -> Optional[bytes]:
    """从 part / function_call 上取出思考签名，统一成 bytes。"""
    sig = getattr(part, "thought_signature", None)
    if sig is None:
        sig = getattr(fc, "thought_signature", None)
    return decode_thought_signature(sig)


def encode_thought_signature(signature: Optional[bytes]) -> Optional[str]:
    """Encode SDK signature bytes for the documented OpenAI extension."""
    if not signature:
        return None
    return base64.b64encode(signature).decode("ascii")


def sdk_thought_signature_field(signature: Optional[bytes]):
    """把内部签名字节转换成 ``types.Part.thought_signature`` 的出站值（Express/REST）。

    Google 要求官方哨兵以 JSON **字面量字符串**抵达 REST 上游
    （thought-signatures FAQ：``skip_thought_signature_validator`` /
    ``context_engineering_is_the_way_to_go``；langchain-google#1570 实证
    base64 形态会被 Vertex 拒绝。2026-11-17 真机 A/B：Express 端点当日对
    base64 哨兵亦放行——属未文档化宽容，字面量仍是唯一 documented 形态）。
    而 google-genai SDK 对一切 ``bytes`` 值做
    base64 序列化（``BaseModel`` 的 ``ser_json_bytes='base64'`` 与出站管线
    ``encode_unserializable_types`` 的 urlsafe 编码），直接把
    ``b"skip_thought_signature_validator"`` 交给 ``types.Part`` 会在 wire 上
    变成 ``"c2tpcF90aG91..."``——严格比对字面量的端点据此拒绝跳过校验。

    因此哨兵改用 ``str`` 传入 ``types.Part``：pydantic 宽松验证把哨兵按
    base64 解码回字节，SDK 出站序列化时 urlsafe 重编码恰好还原字面量
    （哨兵本身被 Google 设计为 32 个 urlsafe-base64 字符，可无损往返）。
    真实签名保持 ``bytes`` 走正常 base64 路径。

    注意：**只适用于 Express/REST 出站**。Cookie batchGraphql 的
    thoughtSignature 是 GraphQL Bytes 标量，哨兵在那里必须 base64 编码
    （字面量会被校验层拒绝，2026-11-16 真机验证），详见 signature_store
    的通道差异说明。
    """
    if signature in SKIP_VALIDATOR_SENTINELS:
        return signature.decode("ascii")
    return signature


def thought_signature_extra(signature: Optional[bytes], part_kind: Optional[str] = None) -> Optional[dict]:
    encoded = encode_thought_signature(signature)
    if not encoded:
        return None
    google = {"thought_signature": encoded}
    # Google only standardizes thought_signature. This optional hint lets this
    # bridge put a message-level signature back on a thought/text/signature-only
    # Part instead of moving it during OpenAI flattening.
    if part_kind:
        google["thought_signature_part"] = part_kind
    return {"google": google}


def attach_signature_extra(payload: dict, signature: Optional[bytes]) -> None:
    """Attach real Google bytes and disambiguate only literal IDs that need it.

    This per-call OpenAI extension is an encoding hint, not proof of a signing
    identity. Binding its fixed-size value to the *exact* ID prevents ordinary
    client edits from silently reinterpreting a different historical carrier.
    """
    extra = thought_signature_extra(signature) or {}
    ident = payload.get("id")
    if isinstance(ident, str) and _legacy_components(ident)[1] is not None:
        extra["vertex2openai"] = {_LITERAL_ID_KEY: _literal_id_digest(ident)}
    if extra:
        payload["extra_content"] = extra


def google_metadata(value: Any) -> dict:
    extra = value.get("extra_content") if isinstance(value, dict) else getattr(value, "extra_content", None)
    return (
        (extra.get("google") or {})
        if isinstance(extra, dict) and isinstance(extra.get("google"), dict)
        else {}
    )


def signature_from_extra(value: Any) -> Tuple[Optional[bytes], Optional[str]]:
    google = google_metadata(value)
    encoded = google.get("thought_signature")
    # Preserve explicit non-string legacy values through their historic string form.
    raw = encoded if isinstance(encoded, (str, bytes)) else str(encoded) if encoded else None
    return decode_thought_signature(raw), google.get("thought_signature_part")


def standalone_message_signature(message: Any) -> Tuple[Optional[bytes], Optional[str]]:
    """A standalone assistant signature is usable; a rejected topology's duplicate is not."""
    role = message.get("role") if isinstance(message, dict) else getattr(message, "role", None)
    if role not in ("assistant", "model") or "ordinary_parts" in google_metadata(message):
        return None, None
    return signature_from_extra(message)


def ordinary_part_metadata(kind: str, text: str, signature: Optional[bytes], *, thought=False) -> dict:
    item = {"kind": kind, "text": text}
    if kind == "image" and thought:
        item["thought"] = True
    encoded = encode_thought_signature(signature)
    if encoded:
        item["thought_signature"] = encoded
    return item


def ordinary_parts_from_extra(value: Any, image_loader=None, history=None) -> Optional[List[types.Part]]:
    """Restore exact ordinary boundaries only from matching assistant history."""
    slots = exact_ordinary_slots(value, google_metadata(value).get("ordinary_parts"), history)
    if slots is None:
        return None
    parts = []
    for item, media in slots:
        kind = item["kind"]
        signature = sdk_thought_signature_field(decode_thought_signature(item.get("thought_signature")))
        if media is not None:
            part = image_loader(media) if image_loader else None
            if part is None:
                return None
            if signature is not None:
                part.thought_signature = signature
            if kind == "thought" or item.get("thought") is True:
                part.thought = True
        else:
            part = types.Part(text=item["text"], thought=True if kind == "thought" else None,
                              thought_signature=signature)
        parts.append(part)
    return parts


def merge_by_order(part_order: Any, call_parts, ordinary_parts, *, remainder_calls_first: bool = True):
    """QUA-P2-3（2026-11 审查收敛）：part_order 交错重组的单一实现。

    google.extra.part_order 记录「工具调用块与普通块」在上游响应中的
    原始交错顺序（生产者：express_response / cookie_response /
    message_processing）。此前 message_processing 与 cookie_request
    各自实现同一套「按 part_order 重放 + 越界与重复索引剔除 + 剩余块
    兜底追加」算法，仅容器类型与兜底顺序不同；现收敛为泛型单实现。
    part_order 不是 list 时返回 None，由调用方保留各自的缺省拼装。
    remainder_calls_first 保留两通道既有兜底顺序（历史行为）：
    Cookie 通道剩余调用在前，SDK 通道剩余普通块在前。
    """
    if not isinstance(part_order, list):
        return None
    ordered: list = []
    used_calls: set = set()
    used_ordinary: set = set()
    for item in part_order:
        if not isinstance(item, dict):
            continue
        idx = item.get("index")
        if (item.get("type") == "tool_call"
                and type(idx) is int
                and 0 <= idx < len(call_parts)
                and idx not in used_calls):
            ordered.append(call_parts[idx])
            used_calls.add(idx)
        elif (item.get("type") == "ordinary"
                and type(idx) is int
                and 0 <= idx < len(ordinary_parts)
                and idx not in used_ordinary):
            ordered.append(ordinary_parts[idx])
            used_ordinary.add(idx)
    if remainder_calls_first:
        ordered.extend(p for i, p in enumerate(call_parts) if i not in used_calls)
        ordered.extend(p for i, p in enumerate(ordinary_parts) if i not in used_ordinary)
    else:
        ordered.extend(p for i, p in enumerate(ordinary_parts) if i not in used_ordinary)
        ordered.extend(p for i, p in enumerate(call_parts) if i not in used_calls)
    return ordered


def merge_replayed_parts(google, calls, ordinary, *, exact_history, remainder_calls_first):
    """Invalid ordinary positions say nothing about the order of explicit calls.

    Keep the tool-only projection used by gemini_history_order to correlate a
    complete result batch. Place new public content outside the rejected order.
    """
    order = google.get("part_order")
    if not exact_history and "ordinary_parts" in google:
        sorted_calls = merge_by_order(order, calls, ())
        return None if sorted_calls is None else sorted_calls + ordinary
    return merge_by_order(order, calls, ordinary, remainder_calls_first=remainder_calls_first)


PendingCallSignatures = list[tuple[str, bytes | None, bool]]


def stage_tool_call_id(pending: PendingCallSignatures, fc: Any, signature: bytes | None,
                       *, unsigned_follower: bool) -> str:
    """Allocate the emitted ID without making a partially converted reply replayable."""
    real_id = getattr(fc, "id", None) or ""
    if not isinstance(real_id, str):
        real_id = str(real_id)
    if not real_id:
        real_id = "call_" + uuid.uuid4().hex[:16]

    pending.append((real_id, signature, unsigned_follower))
    return real_id


def publish_tool_call_signatures(pending: PendingCallSignatures) -> None:
    for call_id, signature, unsigned_follower in pending:
        signature_store.register_call_signature(call_id, signature, unsigned_follower=unsigned_follower)


def _legacy_components(call_id: str) -> tuple[str, bytes | None]:
    """Parse the old format without consulting observation/cache state."""
    base, marker, encoded = call_id.partition(LEGACY_THOUGHT_SEP)
    if not marker or not base or not encoded:
        return call_id, None
    try:
        raw = base64.b64decode(encoded, validate=True)
    except (ValueError, binascii.Error):
        return call_id, None
    if not raw or base64.b64encode(raw).decode("ascii") != encoded:
        return call_id, None
    return base, raw


def _split_legacy_call_id(call_id: str) -> tuple[str, bytes | None]:
    if LEGACY_THOUGHT_SEP in call_id and signature_store.get_record(call_id) is not None:
        return call_id, None  # Warm unmarked echoes of IDs actually emitted by the bridge.
    return _legacy_components(call_id)


def _literal_id_digest(call_id: str) -> str:
    return hashlib.sha256(call_id.encode("utf-8", errors="surrogatepass")).hexdigest()


def marked_literal_call_id(call: dict) -> str | None:
    """Only accept the bridge's exact ID-format hint, never the signature as an ID hint."""
    extra = call.get("extra_content")
    bridge = extra.get("vertex2openai") if isinstance(extra, dict) else None
    marker = bridge.get(_LITERAL_ID_KEY) if isinstance(bridge, dict) else None
    ident = call.get("id")
    if (isinstance(marker, str) and len(marker) == 64 and isinstance(ident, str)
            and _legacy_components(ident)[1] is not None and marker == _literal_id_digest(ident)):
        return ident
    return None


def replay_tool_call_id(call_id: str, literal_ids=frozenset()) -> str:
    return call_id if call_id in literal_ids else _split_legacy_call_id(call_id)[0]


def resolve_tool_call_signature(
    tool_call_id: str,
    require_signature: bool = False,
    explicit_signature: Optional[bytes] = None,
    explicit_unsigned: bool = False,
    origin_key: bytes | None = None,
    *, literal_id: bool = False,
) -> Tuple[str, Optional[bytes]]:
    """Restore ``(real id, signature)`` with explicit OpenAI metadata first.

    The skip sentinel is used only for the first call in a required Gemini 3
    step when neither explicit metadata, the legacy id, nor the fallback store
    can recover a signature. Explicitly unsigned parallel followers stay bare.
    """
    ident = str(tool_call_id or "")
    real_id, legacy_sig = (ident, None) if literal_id else _split_legacy_call_id(ident)
    sig = explicit_signature if explicit_signature is not None else legacy_sig

    if sig is None and origin_key is not None:
        exact = signature_store.get_record(origin_key) if origin_key else None
        if exact is not None:
            if exact.state is SignatureState.SIGNED:
                return real_id, exact.signature
            if exact.state in (SignatureState.UNSIGNED_FOLLOWER, SignatureState.AMBIGUOUS):
                return real_id, None
        else:
            # A bare ID from another prefix, tenant or batch is not evidence
            # for this one. In particular it cannot sign a later sibling.
            bare = signature_store.get_record(real_id)
            if bare and bare.state is not SignatureState.UNKNOWN:
                return real_id, None

    record = signature_store.get_record(real_id) if sig is None and not explicit_unsigned and origin_key is None else None
    if record and record.state is SignatureState.AMBIGUOUS:
        # Two turns claimed incompatible topology. An omitted signature may
        # be rejected upstream; a different turn's bytes or a new bypass are worse.
        return real_id, None
    if sig is None and record and record.state is SignatureState.SIGNED:
        sig = record.signature
    is_unsigned_follower = explicit_unsigned or bool(
        record and record.state is SignatureState.UNSIGNED_FOLLOWER
    )

    if sig is None and require_signature and not is_unsigned_follower:
        if literal_id:
            raise SchemaValidationError(
                "Required signature missing for a literal tool call ID; preserve the original "
                "tool_calls[].extra_content.google.thought_signature."
            )
        sig = SKIP_VALIDATOR_SENTINEL
        print(
            "⚠️ [工具调用] 必需的思考签名无法恢复，已使用官方跳过校验哨兵。请求不会失败，但模型表现可能下降。"
        )

    return real_id, sig


def requires_signature(model_name: str) -> bool:
    """该模型是否强校验思考签名（仅 Gemini 3.x 家族）。"""
    if not model_name:
        return False
    try:
        return mc.get_profile(model_name)["family"] == "g3"
    except Exception:
        return False


def is_thought_part(part: dict) -> bool:
    """健壮的思考 part 判定：兼容 true / "true" / "True" 等表示，
    避免上游把布尔编码成字符串时（"false" 为真值）把正文误判成思考。"""
    t = part.get("thought")
    if isinstance(t, str):
        return t.strip().lower() == "true"
    return bool(t)
