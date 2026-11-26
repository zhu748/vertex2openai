"""Protocol-specific page boundaries over the already-authorized model directory."""
import base64
from hashlib import blake2s
from collections.abc import Mapping


class CatalogPageError(ValueError):
    pass


def _param(query, *names: str) -> str | None:
    values = [value for name in names if name in query
              for value in (query.getlist(name) if hasattr(query, "getlist") else [query[name]])]
    if len(values) > 1:
        raise CatalogPageError(f"请只提供一个 {names[0]}。")
    return values[0] if values else None


def _size(raw: str | None, *, google: bool) -> int:
    if raw is None:
        return 50 if google else 20
    if len(raw) <= 10 and raw.isascii() and raw.isdigit():
        value = int(raw)
        if google and value <= 0x7fffffff:
            return min(value or 50, 1000)  # zero selects the server default
        if not google and 1 <= value <= 1000:
            return value
    raise CatalogPageError("pageSize 必须是非负的 int32 整数。" if google else "limit 必须为 1 至 1000 的整数。")


def gemini_model_page(methods_by_id: Mapping[str, int], query) -> tuple[list[str], str | None]:
    ids = list(methods_by_id)
    size = _size(_param(query, "pageSize", "page_size"), google=True)
    token = _param(query, "pageToken", "page_token")
    if not token and len(ids) <= size:
        return ids, None

    # Stateless across workers. Bind the offset to the same ordered catalog,
    # advertised capabilities and effective request settings. A hot reload must
    # not silently skip/duplicate entries. This checksum is not authorization.
    digest = blake2s(digest_size=16, person=b"v2omdl02")
    digest.update(size.to_bytes(4, "big"))

    def bind(value: str):
        encoded = value.encode("utf-8", "surrogatepass")
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)

    bind(query.get("filter", ""))
    for model_id, methods in methods_by_id.items():
        bind(model_id)
        digest.update(bytes([methods]))

    def cursor(offset: int) -> str:
        raw = offset.to_bytes(4, "big")
        check = digest.copy()
        check.update(raw)
        return base64.urlsafe_b64encode(raw + check.digest()).rstrip(b"=").decode("ascii")

    start = 0
    if token:
        if len(token) == 27 and token.isascii():
            try:
                raw = base64.b64decode(token + "=", altchars=b"-_", validate=True)
            except ValueError:
                raw = b""
            start = int.from_bytes(raw[:4], "big")
        if not (0 < start < len(ids) and start % size == 0 and token == cursor(start)):
            raise CatalogPageError("pageToken 无效、模型目录已变化或请求参数不匹配。")
    end = start + size
    return ids[start:end], cursor(end) if end < len(ids) else None


def anthropic_model_page(entries: list[dict], query) -> dict:
    size = _size(_param(query, "limit"), google=False)
    before, after = _param(query, "before_id"), _param(query, "after_id")
    if before is not None and after is not None:
        raise CatalogPageError("before_id 和 after_id 不能同时提供。")
    cursor = before if before is not None else after
    start, end = 0, len(entries)
    if cursor is not None:
        index = next((i for i, entry in enumerate(entries) if entry["id"] == cursor), None)
        if index is None:
            raise CatalogPageError("分页游标不在当前模型目录中。")
        if before is None:
            start = index + 1
        else:
            end = index
    if before is None:
        end = min(end, start + size)
        more = end < len(entries)
    else:
        start = max(0, end - size)
        more = start > 0
    page = entries[start:end]
    return {"data": page, "first_id": page[0]["id"] if page else None,
            "last_id": page[-1]["id"] if page else None, "has_more": more}
