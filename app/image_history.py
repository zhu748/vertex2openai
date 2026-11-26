"""Restore only this bridge's issued image Markdown in assistant chat history."""
import re
from functools import cached_property

import image_processing
from admission import GenerationCapacityError
from image_delivery import IMAGE_NAME_PATTERN, image_history_base, open_image_asset
from media_tasks import check_media_budget
from request_errors import MAX_GENERATION_BODY_BYTES
from request_ingress import IngressError, current_ingress
from schema_validation import SchemaValidationError

class ManagedImageHistory:
    """One preparation budget, shared by all turns/images (including duplicates)."""

    def __init__(self, base: str):
        self.prefix = base + "/v1/generated-images/"
        self.expanded = 0

    @cached_property
    def pattern(self):
        return re.compile(r"!\[(?:Image|Generated Image)\]\(" + re.escape(self.prefix)
                          + "(" + IMAGE_NAME_PATTERN + r")\)")

    @classmethod
    def for_request(cls):
        base = image_history_base.get()
        return cls(base) if base and base.startswith(("http://", "https://")) else None

    def name_from_url(self, url: str) -> str | None:
        name = url.removeprefix(self.prefix)
        return name if name != url and re.fullmatch(IMAGE_NAME_PATTERN, name) else None

    def segments(self, text: str):
        """Yield (text, None) or ('', artifact name) without normalizing any URL."""
        start = 0
        if "/v1/generated-images/" in text:
            for match in self.pattern.finditer(text):
                if match.start() > start:
                    yield text[start:match.start()], None
                yield "", match[1]
                start = match.end()
        if start < len(text):
            yield text[start:], None

    def read(self, name: str) -> tuple[bytes, str]:
        check_media_budget()
        try:
            asset = open_image_asset(name)
        except OSError:
            asset = None
        if asset is None:
            raise SchemaValidationError("助手历史中的生成图片不存在或已过期。请重新附上原图，或移除该历史图片。")
        with asset.file:
            size = asset.stat.st_size
            if not 0 < size <= image_processing.MAX_REMOTE_IMAGE_BYTES:
                raise SchemaValidationError("历史生成图片为空或超过单张输入图片大小上限。请重新附上较小的图片。")
            self.expanded += 4 * ((size + 2) // 3)
            if self.expanded > MAX_GENERATION_BODY_BYTES:
                raise SchemaValidationError("历史图片展开后超过请求大小上限。请减少图片数量。")
            owner = current_ingress.get()
            if owner is not None:
                try:
                    owner.reserve_expansion(self.expanded)
                except IngressError as exc:
                    if exc.status_code == 503:
                        raise GenerationCapacityError(exc.detail) from None
                    raise SchemaValidationError(exc.detail) from None
            try:
                raw = asset.file.read(size)
            except OSError:
                raise SchemaValidationError("无法读取助手历史中的生成图片。请重新附上原图。") from None
        check_media_budget()
        if len(raw) != size or not image_processing.can_replay_image_verbatim(raw):
            raise SchemaValidationError("历史生成图片无法安全还原。请重新附上较小的原图。")
        return raw, asset.mime
