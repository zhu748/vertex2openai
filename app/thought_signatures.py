"""思考签名元数据、工具调用标识与签名拓扑恢复。"""

import base64
import uuid
from typing import Any, List, Optional, Tuple

import model_capabilities as mc
from google.genai import types
from signature_store import (
    SKIP_VALIDATOR_SENTINEL,
    SignatureRecord,
    SignatureState,
    signature_store,
)

LEGACY_THOUGHT_SEP = "__thought__"


def signature_bytes(part: Any, fc: Any) -> Optional[bytes]:
    """从 part / function_call 上取出思考签名，统一成 bytes。"""
    sig = getattr(part, "thought_signature", None)
    if sig is None:
        sig = getattr(fc, "thought_signature", None)
    if isinstance(sig, bytes):
        return sig or None
    if isinstance(sig, str) and sig:
        try:
            return base64.b64decode(sig)
        except Exception:
            return sig.encode("utf-8")
    return None


def encode_thought_signature(signature: Optional[bytes]) -> Optional[str]:
    """Encode SDK signature bytes for the documented OpenAI extension."""
    if not signature:
        return None
    return base64.b64encode(signature).decode("ascii")


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
    if not encoded:
        return None, google.get("thought_signature_part")
    if isinstance(encoded, bytes):
        return encoded, google.get("thought_signature_part")
    try:
        return base64.b64decode(str(encoded), validate=True), google.get("thought_signature_part")
    except Exception:
        # Preserve non-base64 callers rather than silently discarding their explicit carrier.
        return str(encoded).encode("utf-8"), google.get("thought_signature_part")


def ordinary_part_metadata(kind: str, text: str, signature: Optional[bytes]) -> dict:
    item = {"kind": kind, "text": text}
    encoded = encode_thought_signature(signature)
    if encoded:
        item["thought_signature"] = encoded
    return item


def ordinary_parts_from_extra(value: Any) -> Optional[List[types.Part]]:
    """Restore exact flattened ordinary Part boundaries from bridge metadata."""
    raw = google_metadata(value).get("ordinary_parts")
    if not isinstance(raw, list):
        return None
    parts: List[types.Part] = []
    for item in raw:
        if not isinstance(item, dict):
            return None
        kind = item.get("kind")
        text = item.get("text")
        if kind not in {"text", "thought", "signature_only"} or not isinstance(text, str):
            return None
        # Generated images are flattened to markdown in OpenAI content. Let the
        # normal content path parse them back into inline_data rather than
        # replaying the data URL as ordinary text.
        if kind == "text" and "data:image/" in text:
            return None
        encoded = item.get("thought_signature")
        signature = None
        if encoded:
            try:
                signature = base64.b64decode(str(encoded), validate=True)
            except Exception:
                signature = str(encoded).encode("utf-8")
        parts.append(
            types.Part(
                text=text,
                thought=(True if kind == "thought" else None),
                thought_signature=signature,
            )
        )
    return parts


def build_tool_call_id(fc: Any, part: Any, missing_state: SignatureState = SignatureState.UNKNOWN) -> str:
    """Return a short OpenAI call id and cache signed/unsigned topology fallback."""
    real_id = getattr(fc, "id", None) or ""
    if not isinstance(real_id, str):
        real_id = str(real_id)
    if not real_id:
        real_id = "call_" + uuid.uuid4().hex[:16]

    sig = signature_bytes(part, fc)
    if sig:
        signature_store.put_record(real_id, SignatureRecord(SignatureState.SIGNED, sig))
    elif missing_state is SignatureState.UNSIGNED_FOLLOWER:
        signature_store.put_unsigned_follower(real_id)
    else:
        signature_store.put_unknown(real_id)
    return real_id


def resolve_tool_call_signature(
    tool_call_id: str,
    require_signature: bool = False,
    explicit_signature: Optional[bytes] = None,
    explicit_unsigned: bool = False,
) -> Tuple[str, Optional[bytes]]:
    """Restore ``(real id, signature)`` with explicit OpenAI metadata first.

    The skip sentinel is used only for the first call in a required Gemini 3
    step when neither explicit metadata, the legacy id, nor the fallback store
    can recover a signature. Explicitly unsigned parallel followers stay bare.
    """
    real_id = tool_call_id or ""
    sig: Optional[bytes] = explicit_signature

    if LEGACY_THOUGHT_SEP in real_id:
        real_id, _, encoded = real_id.partition(LEGACY_THOUGHT_SEP)
        if sig is None:
            try:
                sig = base64.b64decode(encoded) or None
            except Exception:
                sig = None

    record = signature_store.get_record(real_id) if sig is None and not explicit_unsigned else None
    if sig is None and record and record.state is SignatureState.SIGNED:
        sig = record.signature
    is_unsigned_follower = explicit_unsigned or bool(
        record and record.state is SignatureState.UNSIGNED_FOLLOWER
    )

    if sig is None and require_signature and not is_unsigned_follower:
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
