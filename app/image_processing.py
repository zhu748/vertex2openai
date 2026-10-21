"""输入图片压缩、内联解码和远程图片抓取。"""

import base64
import io
import re
from typing import Optional, Tuple

import config as app_config
import httpx
from runtime_state import app_state, get_effective_proxy_url, get_effective_proxy_urls

try:
    from PIL import Image, ImageOps
except ImportError:
    Image = None


def optimize_image_bytes(
    image_data: bytes, original_mime: str, max_size_bytes: int = None
) -> Tuple[bytes, str]:
    """输入图片压缩引擎：可在控制台配置开关/边长/质量/体积阈值。
    超过阈值的图会限制最长边并重采样，避免多轮修图卡死。"""
    if Image is None:
        return image_data, original_mime

    # 安全预检独立于压缩开关和压缩体积阈值。PIL 在打开极大图片时就会
    # 抛 DecompressionBombError；不能让普通解析失败的「回传原图」兜底
    # 把它吞掉，否则数百 MP 的低熵小文件会绕过后续像素检查。
    try:
        with Image.open(io.BytesIO(image_data)) as img:
            pixel_count = img.width * img.height
            dimensions = f"{img.width}x{img.height}"
    except Image.DecompressionBombError as exc:
        raise ValueError("输入图片像素数超过 PIL 安全上限，已拒绝处理。") from exc
    except Exception:
        return image_data, original_mime
    if pixel_count > 2 * _MAX_INPUT_PIXELS:
        raise ValueError(
            f"输入图片像素数 {dimensions} 超过安全上限"
            f"（{2 * _MAX_INPUT_PIXELS // 1_000_000}MP），已拒绝处理")

    # 逐键读取（无深拷贝）：旧实现每张图都 get_settings() 锁内深拷贝整个
    # 设置树（含全部 model_overrides），多图消息按图数放大（2026-10 优化）。
    if not app_state.get_setting("img_compress_enabled", True):
        return image_data, original_mime

    if max_size_bytes is None:
        try:
            max_size_bytes = int(float(app_state.get_setting("img_compress_max_mb", 1.5)) * 1024 * 1024)
        except (TypeError, ValueError, OverflowError):
            max_size_bytes = int(1.5 * 1024 * 1024)
    try:
        max_dim = int(app_state.get_setting("img_compress_max_dim", 1536) or 1536)
    except (TypeError, ValueError, OverflowError):
        max_dim = 1536
    try:
        quality = int(app_state.get_setting("img_compress_quality", 85) or 85)
    except (TypeError, ValueError, OverflowError):
        quality = 85

    # 在安全体积内的图片，原样发送，不损耗画质。
    # 像素炸弹守卫：低熵 PNG（如 258KB 的 9000×9000）字节数达标但像素数巨大，
    # 会绕过最长边限制原样透传，后续任何解码方部按 ~25× 解压放大。
    # 只读 header 判定（不解压全图），超过 _MAX_INPUT_PIXELS 才进入压缩路径。
    if len(image_data) <= max_size_bytes and pixel_count <= _MAX_INPUT_PIXELS:
        return image_data, original_mime

    try:
        with Image.open(io.BytesIO(image_data)) as img:
            # Compression drops EXIF metadata. Apply camera orientation first
            # so portrait photos do not silently turn sideways upstream.
            img = ImageOps.exif_transpose(img)
            # 抹平透明通道以防转成 JPEG 时报错
            if img.mode in ("RGBA", "LA", "P"):
                background = Image.new("RGB", img.size, (255, 255, 255))
                rgba = img.convert("RGBA")
                background.paste(rgba, mask=rgba.getchannel("A"))
                img = background
            elif img.mode != "RGB":
                img = img.convert("RGB")

            if img.width > max_dim or img.height > max_dim:
                img.thumbnail((max_dim, max_dim), Image.Resampling.LANCZOS)

            output = io.BytesIO()
            img.save(output, format="JPEG", quality=quality, optimize=True)
            opt_data = output.getvalue()

            # 再降低一次质量，尽量缩小；不能保证低于目标体积。
            if len(opt_data) > max_size_bytes:
                output = io.BytesIO()
                img.save(output, format="JPEG", quality=max(40, quality - 15), optimize=True)
                opt_data = output.getvalue()

            return opt_data, "image/jpeg"
    except ValueError:
        # 明确的安全拒绝（像素超限）：不落入"原图回传"兜底。
        raise
    except Image.DecompressionBombError as e:
        # PIL 自身的像素炸弹拦截：同样拒绝而非回传（belt and suspenders）。
        raise ValueError(f"输入图片像素数超过 PIL 安全上限：{e}")
    except Exception as e:
        print(f"⚠️ [图片处理] 输入图片压缩失败，已回退为原图传输：{e}")
        return image_data, original_mime


MAX_REMOTE_IMAGE_BYTES = 20 * 1024 * 1024  # 单张图上限，防止超大响应打爆内存

# 像素炸弹守卫阈值：字节数达标但像素数巨大的图（低熵 PNG）在此强制进入
# 压缩路径（最长边限制）。40MP 只拦真炸弹，常规照片/全景均不受影响。
_MAX_INPUT_PIXELS = 40_000_000


MAX_REMOTE_IMAGE_REDIRECTS = 3


MAX_DATA_URL_IMAGE_B64_CHARS = (MAX_REMOTE_IMAGE_BYTES // 3) * 4 + 4


def decode_data_url_image(b64_data: str) -> bytes:
    """解码内联 base64 图片字节，超出体积上限时抛 ValueError（调用方跳图）。"""
    if len(b64_data) > MAX_DATA_URL_IMAGE_B64_CHARS:
        raise ValueError(
            f"内联图片 base64 超过 {MAX_REMOTE_IMAGE_BYTES // (1024 * 1024)}MiB 上限（收到 "
            f"{len(b64_data) // (1024 * 1024)}MiB 级字符）"
        )
    # RFC 2045 折行仍可接受；其它非 Base64 字符不能被解码器静默丢弃。
    raw = base64.b64decode(re.sub(r"[ \t\r\n]+", "", b64_data), validate=True)
    if not raw:
        raise ValueError("内联图片内容为空。")
    if len(raw) > MAX_REMOTE_IMAGE_BYTES:
        raise ValueError(f"内联图片解码后超过 {MAX_REMOTE_IMAGE_BYTES // (1024 * 1024)}MiB 上限")
    return raw


_IMAGE_FETCH_HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"
    ),
    "accept": "image/avif,image/webp,image/png,image/jpeg,image/*;q=0.8,*/*;q=0.5",
}


def _public_image_addresses(host: str) -> list[str]:
    """Resolve and validate every address; callers connect to a validated IP."""
    import ipaddress
    import socket

    if not host:
        raise ValueError("图片地址缺少主机名")
    host = host.strip("[]")
    try:
        addresses = [ipaddress.ip_address(host)]
    except ValueError:
        addresses = [
            ipaddress.ip_address(info[4][0])
            for info in socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
        ]
    if not addresses or any(not address.is_global or address.is_multicast for address in addresses):
        raise ValueError("目标地址指向内网/环回/链路本地，已拒绝")
    return list(dict.fromkeys(str(address) for address in addresses))


def _is_blocked_host(host: str) -> bool:
    try:
        _public_image_addresses(host)
        return False
    except (OSError, ValueError):
        return True


def fetch_remote_image(url: str, timeout: float = 10.0) -> Optional[Tuple[bytes, str]]:
    """Download a bounded public image using a pinned, validated destination.

    Runs in a worker thread. Every redirect is validated, including when using
    a proxy; proxy conversion failure never causes an unexpected direct fetch.
    """
    from proxy_failover import ProxyChain
    from proxy_resolver import current_proxy_usage

    usage = current_proxy_usage.get()
    chain = getattr(usage, "route", None) if usage else None
    if chain is None:
        candidates = get_effective_proxy_urls() or [get_effective_proxy_url()]
        chain = ProxyChain(candidates=candidates)
    try:
        return chain.run_sync(lambda raw, resolved: _fetch_remote_image_once(url, timeout, resolved))
    except Exception as exc:
        print(f"⚠️ [图片处理] 获取远程图片失败，已跳过：{url[:120]}，原因：{type(exc).__name__}")
        return None


def _fetch_remote_image_once(url, timeout, proxy_url):
    from urllib.parse import urlparse

    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"不支持的 URL 协议：{parsed.scheme or '(空)'}")
    if _is_blocked_host(parsed.hostname or ""):
        raise ValueError("目标地址指向内网/环回/链路本地，已拒绝")

    client_args = {"timeout": timeout, "follow_redirects": False, "trust_env": False}
    if proxy_url:
        client_args["proxy"] = proxy_url
    if getattr(app_config, "SSL_CERT_FILE", None):
        client_args["verify"] = app_config.SSL_CERT_FILE

    with httpx.Client(**client_args) as client:
        current = url
        for _ in range(MAX_REMOTE_IMAGE_REDIRECTS + 1):
            original_url = httpx.URL(current)
            if original_url.username or original_url.password:
                raise ValueError("图片 URL 不允许包含认证凭据")
            # Pin the checked address for direct and proxy connections,
            # preserving Host and TLS SNI. DNS cannot change between the
            # safety check and the actual connection (DNS rebinding).
            address = _public_image_addresses(original_url.host)[0]
            pinned_url = original_url.copy_with(host=address)
            image_headers = dict(_IMAGE_FETCH_HEADERS, Host=original_url.netloc.decode("ascii"))
            with client.stream(
                "GET",
                pinned_url,
                headers=image_headers,
                extensions={"sni_hostname": original_url.raw_host.decode("ascii")},
            ) as resp:
                if resp.is_redirect:
                    # 每跳复查，且重定向响应体无需下载。
                    location = resp.headers.get("location", "")
                    current = str(original_url.join(location)) if location else ""
                    nxt = urlparse(current)
                    if nxt.scheme not in ("http", "https"):
                        raise ValueError(f"重定向到不支持的协议：{nxt.scheme or '(空)'}")
                    if _is_blocked_host(nxt.hostname or ""):
                        raise ValueError("重定向指向内网/环回/链路本地，已拒绝")
                    continue

                resp.raise_for_status()
                mime = resp.headers.get("content-type", "image/jpeg").split(";")[0].strip()
                if mime and not mime.startswith("image/"):
                    raise ValueError(f"响应不是图片（content-type={mime}）")
                data = bytearray()
                for chunk in resp.iter_bytes(chunk_size=64 * 1024):
                    if len(data) + len(chunk) > MAX_REMOTE_IMAGE_BYTES:
                        raise ValueError(f"图片超过 {MAX_REMOTE_IMAGE_BYTES // (1024 * 1024)}MB 上限")
                    data.extend(chunk)
                return bytes(data), mime or "image/jpeg"
        raise ValueError("重定向次数过多")
