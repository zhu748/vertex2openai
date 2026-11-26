"""Per-request SDK replay positions; retries never mutate the compiled prompt."""
from google.genai import types

from signature_origin import origin_slot
from thought_signatures import sdk_thought_signature_field
from tool_history import history_call_signature


class SDKSignatureReplay:
    def __init__(self):
        self._pending = {}
        self._positions = []

    @staticmethod
    def eligible(message, index):
        return bool(origin_slot(message, index) or message._omitted_signature_key)

    def watch(self, part, message, index, required, literal_ids, first):
        self._pending[id(part)] = (message, index, required, literal_ids, first)

    def bind(self, contents):
        # Tool Parts retain identity during turn merging; record their exact
        # positions once. Reused public IDs alone cannot address a wire Part.
        if self._pending:
            for ci, content in enumerate(contents):
                for pi, part in enumerate(content.parts or ()):
                    marker = self._pending.pop(id(part), None)
                    if marker is not None:
                        self._positions.append((ci, pi, marker))
        self._pending.clear()

    def for_destination(self, contents, destination):
        result = contents
        changed = None
        for ci, pi, (message, index, required, literal_ids, first) in self._positions:
            part = contents[ci].parts[pi]
            ident, signature = history_call_signature(
                message, index, required, literal_ids, first=first, destination=destination)
            if ident != (part.function_call.id or ""):
                # If a legacy ID's interpretation changed since compilation,
                # don't send saved bytes on this differently addressed Part.
                signature = None
            value = sdk_thought_signature_field(signature)
            if isinstance(value, str):  # Preserve SDK's literal sentinel encoding.
                value = types.Part(thought_signature=value).thought_signature
            if value == part.thought_signature:
                continue
            if result is contents:
                result = list(contents)
            if changed != ci:
                result[ci] = contents[ci].model_copy(update={"parts": list(contents[ci].parts)})
                changed = ci
            result[ci].parts[pi] = part.model_copy(update={"thought_signature": value})
        return result
