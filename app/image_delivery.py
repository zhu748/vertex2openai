"""Disk-backed image delivery for chat clients; never keep generated files in RAM.

The URL contains a random capability token so Markdown renderers need no API key.
Only OpenAI chat requests opt in; native Gemini Parts retain their wire format.
"""
import base64
from contextvars import ContextVar
import os
from pathlib import Path
import re
import secrets
import threading
import time

from starlette.requests import Request


image_base_url = ContextVar("image_base_url", default="")
_MIMES = {"image/png": ".png", "image/jpeg": ".jpg", "image/webp": ".webp", "image/gif": ".gif"}
_NAME = re.compile(r"[a-f0-9]{48}\.(?:png|jpg|webp|gif)\Z")
_LOCK = threading.Lock()
TTL_SECONDS = 7 * 86400
MAX_FILE_BYTES = 64 * 1024 * 1024
MAX_STORE_BYTES = 512 * 1024 * 1024
CHUNK = 256 * 1024  # divisible by four: independent base64 decoding


def store_directory():
    return Path(os.environ.get("STATE_DIR", ".")) / "generated-images"


def _prune(directory, incoming):
    now = time.time()
    entries = []
    for path in directory.iterdir():
        if path.name.endswith(".tmp") and _NAME.fullmatch(path.name[:-4]):
            # A process killed by Render cannot execute finally; reclaim its
            # abandoned partial writes without touching other workers' writes.
            try:
                if now - path.stat().st_mtime > 600:
                    path.unlink(missing_ok=True)
            except FileNotFoundError:
                pass
            continue
        if not _NAME.fullmatch(path.name):
            continue
        try:
            stat = path.stat()
            if now - stat.st_mtime >= TTL_SECONDS:
                path.unlink(missing_ok=True)
            else:
                entries.append((stat.st_mtime, stat.st_size, path))
        except FileNotFoundError:
            continue
    total = sum(size for _, size, _ in entries)
    for _, size, path in sorted(entries):
        if total + incoming <= MAX_STORE_BYTES:
            break
        path.unlink(missing_ok=True)
        total -= size


def image_markdown(data, mime_type, *, encoded=False, label="Image"):
    """Return a small download URL, or legacy inline Markdown outside chat scope.

    Cookie base64 is decoded in bounded slices straight to disk. SDK bytes are
    written directly: neither path creates a second whole-image base64 string.
    Storage failures propagate rather than falling back to a client-crashing blob.
    """
    base = image_base_url.get()
    extension = _MIMES.get(mime_type)
    if not base or not extension:
        value = data if encoded else base64.b64encode(data).decode("ascii")
        return f"![{label}](data:{mime_type};base64,{value})"
    padding = (2 if data.endswith("==") else 1 if data.endswith("=") else 0) if encoded else 0
    size = len(data) // 4 * 3 - padding if encoded else len(data)
    if size > MAX_FILE_BYTES:
        raise ValueError("生成图片超过 64 MiB 下载上限。")
    directory = store_directory()
    name = secrets.token_hex(24) + extension
    path = directory / name
    temporary = path.with_suffix(path.suffix + ".tmp")
    with _LOCK:
        directory.mkdir(parents=True, exist_ok=True)
        _prune(directory, size)
        try:
            with temporary.open("xb") as output:
                for offset in range(0, len(data), CHUNK):
                    piece = data[offset:offset + CHUNK]
                    output.write(base64.b64decode(piece, validate=True) if encoded else piece)
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)
    return f"![{label}]({base}/v1/generated-images/{name})"


def resolve_image(name):
    if not _NAME.fullmatch(name):
        return None
    path = store_directory() / name
    try:
        if time.time() - path.stat().st_mtime >= TTL_SECONDS:
            path.unlink(missing_ok=True)
            return None
    except FileNotFoundError:
        return None
    return path


class ImageDeliveryMiddleware:
    """Keep request URL context alive through streaming and inherited pump tasks."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        token = None
        if (scope["type"] == "http" and scope.get("method") == "POST"
                and scope.get("path") == "/v1/chat/completions"
                and os.environ.get("IMAGE_OUTPUT_MODE", "url").lower() != "inline"):
            base = (os.environ.get("IMAGE_PUBLIC_BASE_URL")
                    or os.environ.get("RENDER_EXTERNAL_URL")
                    or str(Request(scope).base_url))
            token = image_base_url.set(base.rstrip("/"))
        try:
            await self.app(scope, receive, send)
        finally:
            if token is not None:
                image_base_url.reset(token)
