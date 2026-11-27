"""One compatibility request after a native provider explicitly rejects absent signatures."""
import json
import re
from typing import Any, NoReturn
from decimal import Decimal

from signature_store import SKIP_VALIDATOR_SENTINEL


_MAX_BODY = 4 * 1024 * 1024
_MISSING = re.compile(
    r"\AFunction call (?:is missing (?:a )?thought_signature in functionCall parts\b"
    r"|`[^`\r\n]{1,256}` in the `?\d+`?\.? content block is missing (?:a )?thought_signature\b)",
    re.IGNORECASE,
)
_MODEL = re.compile(r"gemini-(?:[3-9]|[1-9][0-9]+)(?:[.-][a-z0-9][a-z0-9.-]*)?\Z", re.IGNORECASE)
_CALL = ("functionCall", "function_call")
_SIGNATURE = ("thoughtSignature", "thought_signature")


def fallback_supported(model_id: str, action: str) -> bool:
    return action in ("generateContent", "streamGenerateContent") and bool(_MODEL.fullmatch(model_id))


def missing_signature_error(document: object) -> bool:
    if not isinstance(document, dict) or not isinstance((err := document.get("error")), dict):
        return False
    code, status, message = err.get("code", 400), err.get("status", "INVALID_ARGUMENT"), err.get("message")
    return (type(code) is int and code == 400 and status == "INVALID_ARGUMENT"
            and isinstance(message, str) and bool(_MISSING.match(message)))


def _unique(pairs: list[tuple[str, Any]]) -> dict:
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON member")
        value[key] = item
    return value


def _float(text: str) -> float:
    value = float(text)
    if Decimal(str(value)) != Decimal(text):
        # Re-encoding a request must not round precision in tool arguments.
        raise ValueError("JSON number would change")
    return value


def _constant(_: str) -> NoReturn:
    raise ValueError("non-JSON number")


def with_missing_native_signatures(payload: bytes) -> tuple[bytes, int] | None:
    """Fill missing leaders only. Do not reorder or invent calls/results."""
    if len(payload) > _MAX_BODY:
        return None
    try:
        request = json.loads(payload, object_pairs_hook=_unique, parse_float=_float, parse_constant=_constant)
        contents = request.get("contents") if isinstance(request, dict) else None
        if not isinstance(contents, list):
            return None
        count = 0
        for item in contents:
            if not isinstance(item, dict) or item.get("role") != "model" or not isinstance(item.get("parts"), list):
                continue
            parts = item["parts"]
            calls = [p for p in parts if isinstance(p, dict) and any(k in p for k in _CALL)]
            if not calls:
                continue
            # Do not infer which sibling should lead when there is an ambiguous
            # call alias/shell, interleaved result, or an already signed follower.
            if (any(sum(k in p for k in _CALL) != 1
                    or not isinstance((call := p.get(_CALL[0], p.get(_CALL[1]))), dict)
                    or not isinstance(call.get("name"), str) or not call["name"].strip()
                    for p in calls)
                    or any(isinstance(p, dict) and ("functionResponse" in p or "function_response" in p)
                           for p in parts)
                    or any(any(k in p for k in _SIGNATURE) for p in calls[1:])):
                continue
            first = calls[0]
            slots = [k for k in _SIGNATURE if k in first]
            if len(slots) > 1 or (slots and first[slots[0]] not in (None, "")):
                continue
            key = slots[0] if slots else ("thoughtSignature" if _CALL[0] in first else "thought_signature")
            first[key] = SKIP_VALIDATOR_SENTINEL.decode("ascii")
            count += 1
        if count:
            wire = json.dumps(request, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
            try:
                rebuilt = wire.encode("utf-8")
            except UnicodeEncodeError:
                rebuilt = json.dumps(request, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode("ascii")
            if len(rebuilt) <= _MAX_BODY:
                return rebuilt, count
    except (ValueError, RecursionError, ArithmeticError):
        return None
    return None
