"""ASGI middleware which does not buffer or acknowledge streaming responses."""
import asyncio

import anyio
import console_auth
from request_connection import BodyTooLargeError, ClientConnection, ConnectionReceiver
from request_errors import (MAX_GENERATION_BODY_BYTES, body_too_large_response,
                            declared_body_exceeds, ingress_error_response)
from request_ingress import IngressError, IngressReceive, current_ingress, early_generation_auth
from request_tracking import PayloadObserver, current_trace, request_registry
from starlette.requests import ClientDisconnect

# 请求体体积上限：与原生透传通道（gemini_native.MAX_NATIVE_BODY_BYTES）的
# 32MiB 防护对齐。/v1/chat/completions 的 body 由 FastAPI/Pydantic 全量读入
# 解析（含巨型 messages / base64 图片），不设限等于允许认证后的内存放大。
# content-length 头在此预检；chunked 无长度头的场景由 ConnectionReceiver
# 在 receive 层边读边计数，超限抛 BodyTooLargeError → 此处统一回 413
# （旧实现依赖 native 通道「读完后检查」，据不住 chunked，2026-10 修复）。
MAX_CHAT_BODY_BYTES = MAX_GENERATION_BODY_BYTES  # QUA-P3-1：单一来源别名

# 未认证端点（/api/login）的请求体上限：Pydantic 全量解析巨型 JSON 同样
# 是内存放大面，而这里无需鉴权即可触达。口令本身就是短字符串，256KB 绰绰有余。
LOGIN_BODY_BYTES = 256 * 1024
MANAGEMENT_BODY_BYTES = 8 * 1024 * 1024

# —— PRF-P2-8：大请求体读取并发闸（2026-11）——
# 生成并发闸（upstream_max_concurrency）在 _call_upstream 内生效，位于
# Pydantic 全量解析**之后**；而 32MiB 请求体「全量读入 + 解析」本身可
# 并发 × 2-3 份副本，多个大 body 同时抵达时峰值内存瞬时可达 GB 级
# （512MB 容器直接 OOMKill）。在读体前对「声明 >4MiB 或无长度头
# （chunked）」的生成类请求加闸：最多 8 个同时读体，满时快速 503
#（与 admission 满载同口径，Retry-After 由 ingress_error_response 附上）。
LARGE_BODY_GATE_BYTES = 4 * 1024 * 1024
LARGE_BODY_READ_SLOTS = 8
_large_body_reads = asyncio.Semaphore(LARGE_BODY_READ_SLOTS)
_LARGE_BODY_GATE_MESSAGE = (
    f"同时读取中的大请求体已达上限（{LARGE_BODY_READ_SLOTS} 个），请稍后重试。"
)


def _large_body_gate_needed(declared: str) -> bool:
    """是否需要占用大请求体读位：无长度头（chunked）或声明超阈值。"""
    if not declared:
        return True
    try:
        return int(declared) > LARGE_BODY_GATE_BYTES
    except ValueError:
        return True


class _BodyReadGate:
    """大请求体读位的幂等释放器。

    释放时机是关键：middleware 的 await self.app 覆盖**整个响应生命
    周期**（含 SSE 流式到收尾），若等到 finally 才释放，读位会被首个
    请求独占到响应结束——等价于第二层生成闸，后续大 body 请求会被误
    503。在「最后一个 body 块穿过 receive」的边界点释放：FastAPI/
    Pydantic 在路由处理前读完全部请求体，该点恰为解析完成的最早时刻。
    """

    def __init__(self, semaphore: asyncio.Semaphore):
        self._semaphore = semaphore
        self._released = False

    def wrap(self, receive):
        gate = self

        async def gated_receive():
            message = await receive()
            if (message.get("type") == "http.request"
                    and not message.get("more_body")):
                gate.release()
            return message

        return gated_receive

    def release(self) -> None:
        if not self._released:
            self._released = True
            self._semaphore.release()


def _normalize_authority(value: str) -> str:
    host = value.strip().lower()
    for default in (":80", ":443"):
        if host.endswith(default):
            host = host[: -len(default)]
    return host


# —— SEC-P2-5：安全响应头（纵深防御） ——
# 当前控制台的转义纪律良好（esc() 五实体转义、无第三方脚本），但缺
# nosniff / Referrer-Policy / CSP 等响应头：一旦未来出现任何注入回归，
# 这些头是唯一能限制爆炸半径的浏览器侧防线。
# 注：模板大量使用内联 onclick/style，故 script-src / style-src 需保留
# 'unsafe-inline'——CSP 仍能拦截外源脚本/样式加载、object 嵌入、页面
# 被第三方 iframe 嵌套（frame-ancestors 'none'）等攻击面。
_SECURITY_HEADERS_ALWAYS = (
    (b"x-content-type-options", b"nosniff"),
    (b"referrer-policy", b"no-referrer"),
    (b"cross-origin-opener-policy", b"same-origin"),
)
_SECURITY_HEADERS_HTML = _SECURITY_HEADERS_ALWAYS + (
    (b"content-security-policy",
     b"default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; "
     b"img-src 'self' data:; connect-src 'self'; font-src 'self'; object-src 'none'; "
     b"base-uri 'self'; form-action 'self'; frame-ancestors 'none'"),
    (b"x-frame-options", b"DENY"),
)


class SecurityHeadersMiddleware:
    """为全部 HTTP 响应附加安全头；HTML 额外附 CSP 与 X-Frame-Options。

    已存在的同名响应头不覆盖（调用方显式设置优先）。SSE/JSON 同样适用，
    nosniff / no-referrer 对它们无副作用。
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        async def send_with_security_headers(message):
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                present = {key.lower() for key, _ in headers}
                content_type = next((value for key, value in headers
                                     if key.lower() == b"content-type"), b"")
                extra = (_SECURITY_HEADERS_HTML
                         if b"text/html" in content_type.lower()
                         else _SECURITY_HEADERS_ALWAYS)
                headers.extend(pair for pair in extra if pair[0] not in present)
                message = dict(message, headers=headers)
            await send(message)

        await self.app(scope, receive, send_with_security_headers)


def _csrf_origin_denied(scope) -> bool:
    """控制台写接口的 CSRF 防线（2026-11 S-2）。

    控制台没有 CSRF token，唯一防线是会话 Cookie 的 SameSite=Lax——部署
    在共享注册域的子域下（a.example.com 与 b.example.com）时 Lax 不拦截
    跨站 POST，被伪造的表单可改写代理与凭证配置。口径：浏览器跨站请求
    必带 Origin，要求 Origin 的主机与 Host（或可信反代传入的
    X-Forwarded-Host）一致；非浏览器客户端（curl/SDK）不带 Origin，放行。

    REQ-P2-6（2026-11）：X-Forwarded-Host 仅在 TRUST_PROXY_HEADERS 开启时
    采信。该头客户端可自带伪造——直连部署下攻击者用它即可让任意 Origin
    通过校验，改写代理/凭证配置。

    SEC-P3-2（2026-11）：显式拒绝 Origin: null（沙箱 iframe / 某些重定向
    会发出该字面值，此前它经 urlsplit 解析得到空 netloc 而被放行，仅靠
    SameSite=Lax 兜底）；并采纳 Sec-Fetch-Site: cross-site 作为独立否决
    信号——该头由浏览器强制附加且脚本不可改，比 Origin 更难绕。
    """
    origin = host = forwarded = fetch_site = ""
    for key, value in scope.get("headers", []):
        if key == b"origin":
            origin = value.decode("latin-1")
        elif key == b"host":
            host = value.decode("latin-1")
        elif key == b"x-forwarded-host":
            forwarded = value.decode("latin-1")
        elif key == b"sec-fetch-site":
            fetch_site = value.decode("latin-1").strip().lower()
    if not origin and not fetch_site:
        return False
    # SEC-P3-2：浏览器自报的跨站标记直接否决（none/same-origin/same-site 放行
    # 交由下方 Origin 一致性裁决兜底）；Origin: null 是跨源沙箱/重定向的
    # 规范字面值，同样否决。
    if fetch_site == "cross-site":
        return True
    if not origin:
        return False
    if origin.strip().lower() == "null":
        return True
    from urllib.parse import urlsplit
    origin_host = _normalize_authority(urlsplit(origin).netloc)
    if not origin_host:
        return False
    allowed = {_normalize_authority(host)}
    if forwarded and console_auth.proxy_headers_trusted():
        # 反代会重写 Host（内网服务名）：以 X-Forwarded-Host 的第一段为准。
        allowed.add(_normalize_authority(forwarded.split(",", 1)[0]))
    allowed.discard("")
    return bool(allowed) and origin_host not in allowed


class StatsMiddleware:
    def __init__(self, app, stats):
        self.app = app
        self.stats = stats

    async def __call__(self, scope, receive, send):
        stats = self.stats() if callable(self.stats) else self.stats
        path = scope.get("path", "")
        method = scope.get("method", "")
        tracked = scope["type"] == "http" and method == "POST" and (
            path == "/v1/chat/completions" or path == "/v1/messages"
            or path.startswith(("/v1/messages/", "/v1/models/", "/v1beta/models/")))
        declared_length = next((value.decode("latin-1") for key, value in scope.get("headers", [])
                                if key == b"content-length"), "")
        if (not tracked and scope["type"] == "http" and method in ("POST", "PUT", "PATCH", "DELETE")
                and path.startswith("/api/")):
            # CSRF 防线先于体积限制（2026-11 S-2）：跨站伪造请求在读体前拒绝。
            if _csrf_origin_denied(scope):
                from starlette.responses import JSONResponse
                await JSONResponse(status_code=403, content={
                    "error": "跨站请求被拒绝（Origin 与 Host 不一致）。"
                             "若部署在可信反向代理后，请设置 TRUST_PROXY_HEADERS=true 后重启。"
                })(scope, receive, send)
                return
            limit = LOGIN_BODY_BYTES if path == "/api/login" else MANAGEMENT_BODY_BYTES
            if declared_body_exceeds(declared_length, limit):
                await body_too_large_response(path, limit)(scope, receive, send)
                return
            limited = None
            ingress_token = None
            try:
                limited = IngressReceive(receive, limit)
                ingress_token = current_ingress.set(limited)
                await self.app(scope, limited, send)
            except BodyTooLargeError:
                await body_too_large_response(path, limit)(scope, receive, send)
            except IngressError as exc:
                await ingress_error_response(path, exc.status_code, exc.detail)(scope, receive, send)
            finally:
                if limited is not None:
                    limited.close()
                if ingress_token is not None:
                    current_ingress.reset(ingress_token)
            return
        if not tracked:
            return await self.app(scope, receive, send)
        stats.increment_total()
        if declared_body_exceeds(declared_length, MAX_CHAT_BODY_BYTES):
            stats.add_error()
            await body_too_large_response(path, MAX_CHAT_BODY_BYTES)(scope, receive, send)
            return
        trace = request_registry.create(path)
        token = current_trace.set(trace)
        started = False
        completed = False
        observer = None
        send_failed = False
        error_counted = False
        ingress = None
        ingress_token = None
        body_gate = None
        connection = ClientConnection()
        scope.setdefault("state", {})["client_connection"] = connection
        receiver = ConnectionReceiver(receive, connection, lambda: trace.end_connection(disconnected=True),
                                      lambda: completed, max_body_bytes=MAX_CHAT_BODY_BYTES)

        async def relay(message):
            nonlocal started, observer, completed, send_failed, error_counted
            if message["type"] == "http.response.start":
                started = True
                trace.data["http_status"] = message["status"]
                headers = list(message.get("headers", []))
                headers.append((b"x-trace-id", trace.id.encode()))
                message = dict(message, headers=headers)
                content_type = dict(headers).get(b"content-type", b"").lower()
                observer = PayloadObserver(trace, sent=True, sse=b"text/event-stream" in content_type)
                trace.milestone("response_start", f"开始响应前端：HTTP {message['status']}。")
                if message["status"] >= 400:
                    stats.add_error()
                    error_counted = True
                    trace.fail(f"HTTP {message['status']}", message["status"])
            # Await the real server send, including the stream's DONE frame.
            final_body = message["type"] == "http.response.body" and not message.get("more_body", False)
            if final_body:
                receiver.final_send_started = True
            try:
                await send(message)
            except BaseException:
                send_failed = True
                receiver.disconnect()
                raise
            finally:
                if final_body:
                    # completed is assigned below before this task next yields.
                    receiver.final_send_done.set()
            if message["type"] == "http.response.body":
                completed = not message.get("more_body", False)
                if observer:
                    observer.feed(message.get("body", b""), final=completed)

        try:
            denied = await early_generation_auth(scope)
            if denied is not None:
                await denied(scope, receive, relay)
                return
            # PRF-P2-8：大请求体读位（鉴权后占用，未认证洪峰不耗读位；
            # 满时快速 503，错误计数由 relay 按状态码统一记）。
            if _large_body_gate_needed(declared_length):
                if _large_body_reads.locked():
                    print(f"⚠️ [体积限流] {path}：{_LARGE_BODY_GATE_MESSAGE}")
                    await ingress_error_response(
                        path, 503, _LARGE_BODY_GATE_MESSAGE)(scope, receive, relay)
                    return
                await _large_body_reads.acquire()
                body_gate = _BodyReadGate(_large_body_reads)
            ingress = IngressReceive(
                body_gate.wrap(receive) if body_gate is not None else receive,
                MAX_CHAT_BODY_BYTES)
            ingress_token = current_ingress.set(ingress)
            receiver.receive = ingress
            await self.app(scope, receiver, relay)
        except IngressError as exc:
            if started or send_failed:
                raise
            await ingress_error_response(path, exc.status_code, exc.detail)(scope, receive, relay)
            return
        except BodyTooLargeError as exc:
            # chunked 请求体在 receive 层超限：响应尚未开始，统一回 413。
            # 旧实现只能在应用层全量读完后检查，拦不住无长度头的流式上传。
            # 错误计数交给 relay（status >= 400 时统一计），不在此重复计。
            if started or send_failed:
                raise
            print(f"⚠️ [体积限制] {path}：{exc}")
            response = body_too_large_response(path, MAX_CHAT_BODY_BYTES)
            await response(scope, receive, relay)
            return
        except (asyncio.CancelledError, ClientDisconnect):
            raise
        except Exception as exc:
            if send_failed:
                raise
            # 响应已开始（如 200 流中途应用层异常）同样计入错误：旧行为只在
            # 未开始时计数，这类请求在大盘上既非 success 也非 error，
            # total ≠ success + error 的差值永久累积（口径漂移，2026-10 修复）。
            # error_counted 防止 4xx/5xx start 后再抛异常的双重计数。
            if not error_counted:
                stats.add_error()
                error_counted = True
            trace.fail(str(exc))
            raise
        finally:
            if body_gate is not None:
                # 幂等：正常路径已在末块穿过 receive 时释放，这里是断连/
                # 异常路径的兑底（应用未读完请求体就终止）。
                body_gate.release()
            if ingress is not None:
                ingress.close()
            if ingress_token is not None:
                current_ingress.reset(ingress_token)
            connection.finish(send_failed or not completed)
            trace.end_connection(disconnected=send_failed or not completed)
            try:
                with anyio.CancelScope(shield=True):
                    await receiver.aclose()
            finally:
                current_trace.reset(token)
