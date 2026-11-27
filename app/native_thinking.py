"""Explicit console policy and redacted diagnostics for native Gemini requests."""
import json
import re

from model_capabilities import resolve_thinking
from model_resource import parse_google_model_resource
from protojson import int32_value
from schema_validation import SchemaValidationError


_GENERATION = ("generationConfig", "generation_config")
_THINKING = ("thinkingConfig", "thinking_config")
_LEVEL = ("thinkingLevel", "thinking_level")
_BUDGET = ("thinkingBudget", "thinking_budget")
_MAX = ("maxOutputTokens", "max_output_tokens")
_LEVELS = frozenset({"MINIMAL", "LOW", "MEDIUM", "HIGH", "XHIGH", "THINKING_LEVEL_UNSPECIFIED"})
# Runs only for an explicit override, on already validated JSON. Skip whole
# quoted strings so bracket/key-shaped text inside tools never changes the scan.
_TOKEN = re.compile(rb'"[^"\\]*(?:\\.[^"\\]*)*"|[{}\[\],:]', re.DOTALL)


def _key(obj: dict, aliases: tuple[str, str]) -> str:
    present = [name for name in aliases if name in obj]
    if len(present) > 1:
        raise SchemaValidationError(f"不能同时使用 {aliases[0]} 与 {aliases[1]}。")
    return present[0] if present else aliases[0]


def _object(value: object, name: str) -> dict:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise SchemaValidationError(f"{name} 必须是对象。")
    return value


def forced_native_generation(model: str, settings: dict, body: dict) -> tuple[str, dict] | None:
    if settings.get("native_thinking_mode") != "console":
        return None
    policy = resolve_thinking(model, None, settings)
    if policy["mode"] != "level":
        return None
    gen_key = _key(body, _GENERATION)
    gen = dict(_object(body.get(gen_key), gen_key))
    thinking_key = _key(gen, _THINKING)
    original = _object(gen.get(thinking_key), thinking_key)
    # Explicitly forced level supersedes either wire spelling and the mutually
    # exclusive legacy budget. Keep client's includeThoughts / other fields.
    tc = {key: value for key, value in original.items() if key not in (*_LEVEL, *_BUDGET)}
    tc["thinkingLevel"] = policy["level"].upper()
    gen[thinking_key] = tc
    return gen_key, gen


def _unique_members(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON member")
        result[key] = value
    return result


def rewrite_native_generation(raw: bytes, gen_key: str, gen: dict, *, has_members: bool) -> bytes:
    """Replace one top-level JSON member, leaving tools/history bytes untouched."""
    try:
        encoded = json.dumps(gen, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode("ascii")
    except (ValueError, RecursionError) as exc:
        raise SchemaValidationError("generationConfig 无法编码。") from exc
    depth = 0
    key_expected = False
    selected = False
    value_start = None
    span = None
    opening = None
    for token in _TOKEN.finditer(raw):
        part = token[0]
        if key_expected and depth == 1 and part.startswith(b'"'):
            key_expected = False
            # Aliases are short; a JSON \u escape uses at most 6 bytes per letter.
            selected = len(part) <= 128 and json.loads(part) in _GENERATION
        elif depth == 1 and part == b":" and selected:
            value_start = token.end()
        elif depth == 1 and part in (b",", b"}"):
            if value_start is not None:
                if span is not None:
                    raise SchemaValidationError("原生请求重复声明 generationConfig。")
                span = (value_start, token.start())
            selected, value_start = False, None
            key_expected = part == b","
        if part in (b"{", b"["):
            if depth == 0 and part == b"{" and opening is None:
                opening = token.end()
                key_expected = True
            depth += 1
        elif part in (b"}", b"]"):
            depth -= 1
    if span is not None:
        original = raw[span[0]:span[1]]
        try:
            if json.loads(original, object_pairs_hook=_unique_members) == gen:
                return raw
        except (ValueError, RecursionError) as exc:
            raise SchemaValidationError("generationConfig 内有重复或不合法的 JSON 字段。") from exc
        view = memoryview(raw)
        return b"".join((view[:span[0]], encoded, view[span[1]:]))
    if opening is None:
        raise SchemaValidationError("原生请求必须是 JSON 对象。")
    inserted = json.dumps(gen_key).encode("ascii") + b":" + encoded + (b"," if has_members else b"")
    view = memoryview(raw)
    return b"".join((view[:opening], inserted, view[opening:]))


def native_generation_fields(body: dict, *, forced: bool = False) -> dict:
    """Report supplied wire fields, not a guess based on generated token count."""
    try:
        gen = _object(body.get(_key(body, _GENERATION)), "generationConfig")
        tc = _object(gen.get(_key(gen, _THINKING)), "thinkingConfig")
        raw_level = tc.get(_key(tc, _LEVEL))
        raw_budget = tc.get(_key(tc, _BUDGET))
        raw_max = gen.get(_key(gen, _MAX))
    except SchemaValidationError:
        return {}
    level = raw_level.upper() if isinstance(raw_level, str) and len(raw_level) <= 32 else None
    budget, maximum = int32_value(raw_budget), int32_value(raw_max)
    thinking = level if level in _LEVELS else (f"预算 {budget}" if budget is not None else None)
    if ((raw_level is not None and level not in _LEVELS)
            or (raw_budget is not None and budget is None)
            or (raw_max is not None and maximum is None)):
        return {}
    source = "console" if forced else ("client" if thinking is not None else "unspecified")
    return {"thinking_level": thinking, "max_output_tokens": maximum, "thinking_source": source}



def native_destination_fields(model: str, credential_project: str = "") -> dict:
    resource = parse_google_model_resource(model)
    return {"upstream_model": model, "project_id": (resource.project_id if resource else None) or credential_project}
