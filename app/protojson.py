"""Read declared integer-valued ProtoJSON fields without coercing application JSON."""

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
