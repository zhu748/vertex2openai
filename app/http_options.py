from typing import Optional
from google.genai import types
import config as app_config
from runtime_state import get_effective_proxy_url
from express_fingerprint import build_sdk_channel_overrides


def get_http_options(
    base_url: Optional[str] = None,
) -> Optional[types.HttpOptions]:
    """构造 google-genai HTTP 选项：代理、自定义证书、base_url 与请求头。

    代理走 runtime_state.get_effective_proxy_url()：控制台「通道与凭证」页
    热改后立即生效，环境变量 PROXY_URL 仅作为初始值。

    请求头指纹走 express_fingerprint.build_sdk_channel_overrides()：
    express_ua_profile != sdk 时注入接管 UA / x-goog-api-client（两个
    精确 SDK 标签齐全，SDK 的 append_library_version_headers 原样放行）；
    sdk 档案返回空 dict = 不注入，行为与历史版本一致。
    """
    proxy_url = get_effective_proxy_url()
    client_args = {}
    if proxy_url:
        client_args["proxy"] = proxy_url
    if app_config.SSL_CERT_FILE:
        client_args["verify"] = app_config.SSL_CERT_FILE

    options = {}
    base_url = base_url or (app_config.VERTEX_BASE_URL or None)
    if base_url:
        options["base_url"] = base_url
    if client_args:
        options["client_args"] = client_args
        options["async_client_args"] = client_args

    headers_override = build_sdk_channel_overrides()
    if headers_override:
        options["headers"] = headers_override

    return types.HttpOptions(**options) if options else None
