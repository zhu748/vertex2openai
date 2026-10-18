from typing import Optional
from google.genai import types
import config as app_config
from runtime_state import get_effective_proxy_url
from express_fingerprint import build_sdk_channel_overrides
from proxy_resolver import resolve_proxy_uri


def _request_timeout(request):
    # The SDK passes timeout=None per request unless HttpOptions.timeout is set,
    # which would override a client's phase timeout. Apply phase limits after
    # that merge; this uses httpx's supported request event hook.
    request.extensions["timeout"] = {"connect": 30.0, "read": 600.0, "write": 60.0, "pool": 10.0}


async def _async_request_timeout(request):
    _request_timeout(request)


def get_http_options(
    base_url: Optional[str] = None,
    proxy_url: Optional[str] = None,
) -> Optional[types.HttpOptions]:
    """构造 google-genai HTTP 选项：代理、自定义证书、base_url 与请求头。

    代理分两级：
      - proxy_url=None → 跟随全局 get_effective_proxy_url()（控制台热改生效，
        环境变量 PROXY_URL 仅作初始值）；
      - proxy_url 显式传入 → 用它（Express 多账号场景：本次选中 Key 自己的
        代理，裁决见 runtime_state.get_effective_proxy_uri_for_key）。
    传入的是**原始 URI**：除 http/https/socks5/socks5h 外，还支持
    vless/vmess/trojan/ss/socks4/hysteria2/tuic 等分享链接——经
    proxy_resolver.resolve_proxy_uri() 转成 httpx 可消费的本地 socks5 URL
    （失败抛 ProxyResolveError，由调用方转成明确的 5xx 响应）。

    请求头指纹走 express_fingerprint.build_sdk_channel_overrides()：
    express_ua_profile != sdk 时注入接管 UA / x-goog-api-client（两个
    精确 SDK 标签齐全，SDK 的 append_library_version_headers 原样放行）；
    sdk 档案返回空 dict = 不注入，行为与历史版本一致。
    """
    raw_proxy = get_effective_proxy_url() if proxy_url is None else str(proxy_url or "").strip()
    # 空 / direct → ""（直连）；sing-box 系协议 → 本地 socks5；原生协议原样。
    resolved_proxy = resolve_proxy_uri(raw_proxy)

    client_args = {}
    if resolved_proxy:
        client_args["proxy"] = resolved_proxy
    if app_config.SSL_CERT_FILE:
        client_args["verify"] = app_config.SSL_CERT_FILE

    # Explicit network phase limits; the outer request deadline also bounds
    # repeated calls and streams which continually produce small chunks.
    import httpx
    client_args["timeout"] = httpx.Timeout(connect=30, read=600, write=60, pool=10)
    client_args["trust_env"] = False
    options = {"retry_options": types.HttpRetryOptions(attempts=1)}
    base_url = base_url or (app_config.VERTEX_BASE_URL or None)
    if base_url:
        options["base_url"] = base_url
    if client_args:
        options["client_args"] = dict(client_args, event_hooks={"request": [_request_timeout]})
        options["async_client_args"] = dict(client_args, event_hooks={"request": [_async_request_timeout]})

    headers_override = build_sdk_channel_overrides()
    if headers_override:
        options["headers"] = headers_override

    return types.HttpOptions(**options) if options else None
