"""Match private Gemini Part boundaries to the public assistant history, without I/O."""

import re

from models import normalize_content_part


# The normal attachment converter validates and sizes the image bytes *after*
# the complete history matches. A mention of data:image/ in prose is not media.
_INLINE_MARKDOWN = re.compile(r"!\[[^\]]*\]\(data:image/[a-zA-Z0-9+.-]+;base64,[^)]*\)")


class _Projection:
    """Cursor over adjacent text blocks and explicit attachment boundaries."""

    def __init__(self, value):
        self.blocks = []
        if isinstance(value, str):
            self.blocks = [value] if value else []
        elif isinstance(value, list):
            text = []
            for block in value:
                block = normalize_content_part(block)
                if isinstance(block, dict) and block.get("type") == "text" and isinstance(block.get("text"), str):
                    if block["text"]:
                        text.append(block["text"])
                elif isinstance(block, dict) and block.get("type") == "image_url":
                    if text:
                        self.blocks.append("".join(text))
                        text.clear()
                    self.blocks.append(block)
                else:
                    raise ValueError("not an exact assistant text/image projection")
            if text:
                self.blocks.append("".join(text))
        elif value is not None:
            raise ValueError("not an exact assistant projection")
        self.index = self.offset = 0

    def _current(self):
        return self.blocks[self.index] if self.index < len(self.blocks) else None

    def _advance(self, end):
        self.offset = end
        if end == len(self.blocks[self.index]):
            self.index += 1
            self.offset = 0

    def take_text(self, text):
        if not text:
            return True
        block = self._current()
        if not isinstance(block, str) or not block.startswith(text, self.offset):
            return False
        self._advance(self.offset + len(text))
        return True

    def take_image(self, history):
        block = self._current()
        if isinstance(block, dict):
            source = block.get("image_url")
            url = source.get("url") if isinstance(source, dict) else source
            if isinstance(url, str) and (url.startswith("data:image/") or history and history.name_from_url(url)):
                self.index += 1
                return block
        elif isinstance(block, str):
            match = _INLINE_MARKDOWN.match(block, self.offset)
            if not match and history:
                match = history.pattern.match(block, self.offset)
            if match:
                self._advance(match.end())
                return {"type": "text", "text": match[0]}
        return None

    def complete(self):
        return self.index == len(self.blocks)


def exact_ordinary_slots(message, raw, history=None):
    """Return (descriptor, visible media block) only when both public fields fully match.

    Signatures never justify reading media from a private descriptor, unrecognized
    host or unintended text position. Legacy [image] can still mean literal text.
    """
    field = message.get if isinstance(message, dict) else lambda k: getattr(message, k, None)
    if field("role") not in ("assistant", "model") or not isinstance(raw, list):
        return None
    content = message.replay_content() if hasattr(message, "replay_content") else field("content")
    try:
        projections = (_Projection(content), _Projection(field("reasoning_content")))
    except ValueError:
        return None
    slots = []
    for item in raw:
        if not isinstance(item, dict):
            return None
        kind, text = item.get("kind"), item.get("text")
        if (kind not in ("text", "thought", "image", "signature_only") or not isinstance(text, str)
                or kind in ("signature_only", "image") and text):
            return None
        placeholder = kind in ("text", "thought") and text == "[image]"
        thought = kind == "thought" or (kind == "image" or placeholder) and item.get("thought") is True
        projection = projections[thought]
        media = projection.take_image(history) if kind == "image" or placeholder else None
        if media is None and (kind == "image" or not projection.take_text(text)):
            return None
        slots.append((item, media))
    return slots if all(p.complete() for p in projections) else None
