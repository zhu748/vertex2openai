"""Disk-backed image delivery for chat clients; never keep generated files in RAM.

The URL contains a random capability token so Markdown renderers need no API key.
Only OpenAI chat requests opt in; native Gemini Parts retain their wire format.
"""
import base64
import errno
import os
import re
import secrets
import threading
import time
from contextvars import ContextVar
from os import fstat, stat_result
from pathlib import Path
from stat import S_ISREG
from typing import BinaryIO, NamedTuple

from config import env_int, env_str, state_dir  # QUA-P2-7：集中读取层
from starlette.requests import Request

image_base_url = ContextVar("image_base_url", default="")
image_history_base = ContextVar("image_history_base", default="")
_MIMES = {"image/png": ".png", "image/jpeg": ".jpg", "image/webp": ".webp", "image/gif": ".gif"}
IMAGE_NAME_PATTERN = r"[a-f0-9]{48}\.(?:png|jpg|webp|gif)"
_NAME = re.compile(IMAGE_NAME_PATTERN)
_IMAGE_MIME_BY_SUFFIX = {suffix: mime for mime, suffix in _MIMES.items()}
_LOCK = threading.Lock()


def _resolve_ttl_seconds() -> int:
    """SEC-P3-5：生图 URL 有效期默认从 7 天收紧到 24 小时（可配）。

    下载 URL 只含随机能力 token、不绑定请求者：聊天记录被转发后，任何
    拿到链接的人都能在有效期内反复拉图。旧默认 7 天给了泄漏链接过长的
    存活窗口；24 小时对「当轮对话回看」绰绰有余。IMAGE_URL_TTL_SECONDS
    可调（60 秒 – 7 天，越界钳位），需要旧行为设 604800。
    """
    # QUA-P2-7：环境读取走 config 集中层（非法/未设置返回默认 24h），
    # 钳位语义留在本模块。
    return max(60, min(env_int("IMAGE_URL_TTL_SECONDS", 24 * 3600), 7 * 86400))


TTL_SECONDS = _resolve_ttl_seconds()
MAX_FILE_BYTES = 64 * 1024 * 1024
MAX_STORE_BYTES = 512 * 1024 * 1024
CHUNK = 256 * 1024  # divisible by four: independent base64 decoding
# A periodic reconciliation avoids a directory scan per small publish. Within
# one server process, every publisher updates the byte upper bound under _LOCK;
# expiry/download-side deletions only make that estimate conservative.
_PRUNE_INTERVAL_SECONDS = 60.0
_PRUNE_FORCE_INCOMING_BYTES = 4 * 1024 * 1024
_last_prune_monotonic = -_PRUNE_INTERVAL_SECONDS
_usage_directory = None
_published_bytes = 0


_INLINE_IMAGE_KEYS = ("inlineData", "inline_data")


def has_inline_image(node, depth: int = 0) -> bool:
    """dict 树里是否携带内联图片（inlineData / inline_data）。

    只按容器结构递归（深度封顶），不触碰字符串内部——MB 级 base64
    数据被常数时间跳过。供流式逐对象解析路径做「是否值得把整个对象
    搬进媒体线程落盘」的廉价预检（PRF-P1-1）：纯文本对象零额外开销，
    只有真正含图的对象才支付线程切换与媒体池闸门。
    """
    if depth > 12:
        return False
    if isinstance(node, dict):
        for key, value in node.items():
            if key in _INLINE_IMAGE_KEYS:
                if value:
                    return True
                continue
            if has_inline_image(value, depth + 1):
                return True
        return False
    if isinstance(node, (list, tuple)):
        return any(has_inline_image(item, depth + 1) for item in node)
    return False


def store_directory():
    return Path(state_dir()) / "generated-images"  # QUA-P2-7：STATE_DIR 单点定义


def _prune(directory, published, incoming):
    """Reconcile published bytes and evict oldest peers after a successful rename."""
    global _last_prune_monotonic
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
        if path == published or not _NAME.fullmatch(path.name):
            continue
        try:
            stat = path.stat()
            if now - stat.st_mtime >= TTL_SECONDS:
                path.unlink(missing_ok=True)
            else:
                entries.append((stat.st_mtime, stat.st_size, path))
        except FileNotFoundError:
            continue
    total = incoming + sum(size for _, size, _ in entries)
    if total > MAX_STORE_BYTES:
        for _, size, path in sorted(entries):
            path.unlink(missing_ok=True)
            total -= size
            if total <= MAX_STORE_BYTES:
                break
    _last_prune_monotonic = time.monotonic()
    return total


def _prune_due(incoming: int) -> bool:
    """PRF-P2-4：距上次扫描不足 60 秒且非大图时跳过全目录扫描。"""
    if incoming >= _PRUNE_FORCE_INCOMING_BYTES:
        return True
    return time.monotonic() - _last_prune_monotonic >= _PRUNE_INTERVAL_SECONDS


def _publish_image(temporary, path, size):
    global _usage_directory, _published_bytes
    directory = path.parent
    with _LOCK:
        reconcile = (_usage_directory != directory or _published_bytes + size > MAX_STORE_BYTES
                     or _prune_due(size))
        # Do not evict a single working link unless the new file has already
        # been written, closed and successfully renamed to its final name.
        temporary.replace(path)
        try:
            _published_bytes = _prune(directory, path, size) if reconcile else _published_bytes + size
            _usage_directory = directory
        except OSError:
            _usage_directory = None  # next success must reconcile after a partial eviction
            path.unlink(missing_ok=True)
            raise


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
    if size > MAX_STORE_BYTES:
        raise ValueError("生成图片超过下载存储的总容量。")
    directory = store_directory()
    directory.mkdir(parents=True, exist_ok=True)
    name = secrets.token_hex(24) + extension
    path = directory / name
    temporary = path.with_suffix(path.suffix + ".tmp")
    # Staging is not visible to downloads or quota accounting. Parallel
    # callers never hold the directory lock during decode or IO of the payload.
    output = temporary.open("xb")
    try:
        with output:
            for offset in range(0, len(data), CHUNK):
                piece = data[offset:offset + CHUNK]
                if encoded and offset + CHUNK < len(data) and piece.endswith("="):
                    raise ValueError("生成图片含无效的 Base64 填充。")
                output.write(base64.b64decode(piece, validate=True) if encoded else piece)
            actual_size = output.tell()
        _publish_image(temporary, path, actual_size)
    finally:
        temporary.unlink(missing_ok=True)
    return f"![{label}]({base}/v1/generated-images/{name})"


class OpenImage(NamedTuple):
    path: Path
    file: BinaryIO
    stat: stat_result
    remaining: int

    @property
    def mime(self) -> str:
        return _IMAGE_MIME_BY_SUFFIX[self.path.suffix]


def _open_readonly(name, flags):
    # Published artifacts are immutable regular files, never symbolic links or
    # devices. Nonblocking open lets us reject a nonregular entry without waiting.
    return os.open(name, flags | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0))


def open_image_asset(name: str) -> OpenImage | None:
    """Acquire a published artifact; callers own and must close the returned file.

    A publisher can evict the path at any time. Open first and base both lifetime
    and headers on the actual descriptor; no path lookup is needed to read it.
    """
    if not _NAME.fullmatch(name):
        return None
    path = store_directory() / name
    try:
        # Windows lacks O_NOFOLLOW: verify the observed regular entry against
        # the descriptor before trusting its data or expiry instead.
        before = None if hasattr(os, "O_NOFOLLOW") else path.lstat()
        if before is not None and not S_ISREG(before.st_mode):
            return None
        file = open(path, "rb", opener=_open_readonly)
    except OSError as exc:
        if exc.errno in (errno.ENOENT, errno.ELOOP, errno.EISDIR, errno.ENXIO):
            return None
        raise
    try:
        stat = fstat(file.fileno())
        if not S_ISREG(stat.st_mode) or (before is not None and not os.path.samestat(before, stat)):
            file.close()
            return None
        remaining = stat.st_mtime + TTL_SECONDS - time.time()
        if remaining > 0:
            return OpenImage(path, file, stat, min(TTL_SECONDS, int(remaining)))
    except BaseException:
        file.close()
        raise
    file.close()
    path.unlink(missing_ok=True)
    return None


def resolve_image(name):
    """Path lookup for inspection only; transfers must use open_image_asset."""
    asset = open_image_asset(name)
    if asset is not None:
        asset.file.close()
        return asset.path
    return None


class ImageDeliveryMiddleware:
    """Keep request URL context alive through streaming and inherited pump tasks."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if (scope["type"] != "http" or scope.get("method") != "POST"
                or scope.get("path") != "/v1/chat/completions"):
            return await self.app(scope, receive, send)
        base = (env_str("IMAGE_PUBLIC_BASE_URL") or env_str("RENDER_EXTERNAL_URL")
                or str(Request(scope).base_url)).rstrip("/")
        history_token = image_history_base.set(base)
        output_token = image_base_url.set("" if env_str("IMAGE_OUTPUT_MODE", "url").lower() == "inline" else base)
        try:
            await self.app(scope, receive, send)
        finally:
            image_base_url.reset(output_token)
            image_history_base.reset(history_token)
