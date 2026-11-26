"""Read declared ProtoJSON scalars without coercing application JSON."""

from decimal import Decimal, InvalidOperation
import re


def int32_value(value: object) -> int | None:
    """Recognize number and short quoted integral spellings, never bool or out-of-range."""
    if isinstance(value, str):
        if len(value) > 64 or not re.fullmatch(r"-?[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?", value):
            return None
        try:
            value = Decimal(value)
        except InvalidOperation:
            return None
    if (type(value) not in (int, float, Decimal) or isinstance(value, Decimal) and not value.is_finite()
            or not -0x80000000 <= value <= 0x7fffffff):
        return None
    integer = int(value)
    return integer if integer == value else None


_BASE64_STANDARD = re.compile(r"[A-Za-z0-9+/]++")
_BASE64_URLSAFE = re.compile(r"[A-Za-z0-9_-]++")
_BASE64_TRANSLATE = str.maketrans("-_", "+/")
_BASE64_FINAL = {2: "AQgw", 3: "AEIMQUYcgkosw048"}


def standard_base64(value: object) -> str | None:
    """Canonicalize valid ProtoJSON bytes without a decoded media copy."""
    if not isinstance(value, str):
        return None
    if not value:
        return value
    end = len(value)
    padding = 2 if value.endswith("==") else 1 if value.endswith("=") else 0
    end -= padding
    remainder = end % 4
    missing = (-end) % 4
    if remainder == 1 or (padding and padding != missing):
        return None
    urlsafe = "-" in value or "_" in value
    chars = _BASE64_URLSAFE if urlsafe else _BASE64_STANDARD
    if not chars.fullmatch(value, 0, end):
        return None
    # An ignored, nonzero pad bit would silently reinterpret malformed bytes.
    if remainder and value[end - 1] not in _BASE64_FINAL[remainder]:
        return None
    normalized = value.translate(_BASE64_TRANSLATE) if urlsafe else value
    if not padding and missing:
        normalized += "=" * missing
    return normalized
