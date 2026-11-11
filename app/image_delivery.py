"""Disk-backed image delivery for chat clients; never keep generated files in RAM.

The URL contains a random capability token so Markdown renderers need no API key.
Only OpenAI chat requests opt in; native Gemini Parts retain their wire format.
"""
import base64
from contextvars import ContextVar
from pathlib import Path
import re
import secrets
import threading
import time

from starlette.requests import Request

from config import env_int, env_str, state_dir  # QUA-P2-7：集中读取层


image_base_url = ContextVar("image_base_url", default="")
_MIMES = {"image/png": ".png", "image/jpeg": ".jpg", "image/webp": ".webp", "image/gif": ".gif"}
_NAME = re.compile(r"[a-f0-9]{48}\.(?:png|jpg|webp|gif)\Z")
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
# PRF-P2-4：_prune 摊销——每次存图都全目录 iterdir+stat（全局锁内）在生图
# 突发下是可观的重复扫描；配额回收允许至多 60 秒的漂移窗口，且大图
#（>4MiB）始终强制扫描（突发窗口内的超配被下一个大图或下一个窗口纠正）。
_PRUNE_INTERVAL_SECONDS = 60.0
_PRUNE_FORCE_INCOMING_BYTES = 4 * 1024 * 1024
# 负值初始化：进程启动后首次写图必须真实扫描一次（目录里可能有上一
# 次运行留下的过期文件），此后才进入摊销窗口。
_last_prune_monotonic = -_PRUNE_INTERVAL_SECONDS


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


def _prune(directory, incoming):
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
    _last_prune_monotonic = time.monotonic()


def _prune_due(incoming: int) -> bool:
    """PRF-P2-4：距上次扫描不足 60 秒且非大图时跳过全目录扫描。"""
    if incoming >= _PRUNE_FORCE_INCOMING_BYTES:
        return True
    return time.monotonic() - _last_prune_monotonic >= _PRUNE_INTERVAL_SECONDS


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
    # 锁内只做目录级操作（2026-11 O-8）：mkdir + 配额回收（_prune 对他
    # 写入者的新鲜 .tmp 会跳过、对 >600s 的孤儿残片会回收，天然兼容）。
    # 写盘不再占全局锁——目标文件名是唯一随机 token，多写互不冲突；旧
    # 实现把整张图的 base64 解码与写盘（4K 可达数十 MB）关在锁里，生图
    # 并发被完全串行化，一张慢图队头阻塞全部下载。
    with _LOCK:
        directory.mkdir(parents=True, exist_ok=True)
        # PRF-P2-4：摊销扫描——不足 60 秒且非大图时跳过；写盘本体已由
        # 调用方搬进媒体线程（PRF-P1-1），这里不再有事件循环阻塞问题，
        # 摊销只为消除生图突发下的重复全目录扫描（锁内串行化）。
        if _prune_due(size):
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
                and env_str("IMAGE_OUTPUT_MODE", "url").lower() != "inline"):
            base = (env_str("IMAGE_PUBLIC_BASE_URL")
                    or env_str("RENDER_EXTERNAL_URL")
                    or str(Request(scope).base_url))
            token = image_base_url.set(base.rstrip("/"))
        try:
            await self.app(scope, receive, send)
        finally:
            if token is not None:
                image_base_url.reset(token)
