"""Apply custom trust to an HTTPS proxy's TLS connection as well as its target."""

import os
import ssl

import httpx


def build_httpx_proxy(url: str, *, verify=True):
    """HTTPX's client verify option does not configure HTTPS proxy trust."""
    if not url.lower().startswith("https://") or verify is True or verify is None:
        return url
    if isinstance(verify, ssl.SSLContext):
        context = verify
    elif verify is False:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    else:
        path = os.fspath(verify)
        context = ssl.create_default_context(**({"capath": path} if os.path.isdir(path) else {"cafile": path}))
    return httpx.Proxy(url, ssl_context=context)
