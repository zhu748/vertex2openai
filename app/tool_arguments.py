"""One strict JSON-object parser for client tool history and protocol adapters."""

import json

from schema_validation import SchemaValidationError


def parse_tool_arguments(value, *, label="tool arguments") -> dict:
    """Missing/empty arguments mean {}; malformed input must never become {}."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return {}
    try:
        parsed = json.loads(value) if isinstance(value, str) else value
        if not isinstance(parsed, dict):
            raise SchemaValidationError(f"{label} must be a JSON object")
        # Also rejects non-finite numbers accepted by Python's JSON decoder,
        # cycles and non-JSON values in callers' already-decoded objects.
        json.dumps(parsed, allow_nan=False, ensure_ascii=False).encode("utf-8")
    except SchemaValidationError:
        raise
    except (TypeError, ValueError, UnicodeError, RecursionError, OverflowError):
        raise SchemaValidationError(f"{label} must contain a valid, finite JSON object") from None
    return parsed
