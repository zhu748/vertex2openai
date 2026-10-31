"""Bound uploads and JSON expansion before framework body parsing."""
import asyncio
import codecs
import re
import threading
import time
from contextvars import ContextVar

from starlette.exceptions import HTTPException

UPLOAD_IDLE_SECONDS = 15.0
UPLOAD_TOTAL_SECONDS = 120.0
MAX_JSON_DEPTH = 64
MAX_JSON_ITEMS = 100_000
_SPECIAL = re.compile(rb'["\\{}\[\],:]')
_STRING_SPECIAL = re.compile(rb'["\\]')


class IngressError(HTTPException):
    pass


class IngressBudget:
    """Each lease returns all reservations, also when apps use multiple loops."""
    def __init__(self, requests=32, body_bytes=64 * 1024 * 1024, items=200_000):
        self.limits = (requests, body_bytes, items)
        self.used = [0, 0, 0]
        self.lock = threading.Lock()

    def reserve(self, amounts):
        with self.lock:
            if any(used + amount > limit for used, amount, limit in zip(self.used, amounts, self.limits)):
                raise IngressError(503, "请求入口容量已满，请稍后重试。")
            for index, amount in enumerate(amounts):
                self.used[index] += amount

    def release(self, amounts):
        with self.lock:
            for index, amount in enumerate(amounts):
                self.used[index] -= amount


ingress_budget = IngressBudget()
current_ingress = ContextVar("request_ingress", default=None)


def hold_ingress(task):
    """Detached generation tasks keep their input charged after HTTP disconnect."""
    owner = current_ingress.get()
    if owner is not None:
        release = owner.retain()
        task.add_done_callback(lambda _: release())
    return task


class JSONPreflight:
    """Linear incremental lexical guard; ordinary string bytes are skipped in C.

    Full syntax validation remains the JSON parser's job. Count structural work
    before objects are allocated and reject unpaired escaped UTF-16 surrogates.
    State is retained across arbitrary network chunk boundaries.
    """
    def __init__(self):
        self.depth = self.items = 0
        self.string = self.escape = False
        self.hex_digits = bytearray()
        self.unicode_left = 0
        self.high_surrogate = False

    def feed(self, data, final=False):
        before = self.items
        pos = 0
        while pos < len(data):
            if self.unicode_left:
                size = min(self.unicode_left, len(data) - pos)
                self.hex_digits.extend(data[pos:pos + size])
                pos += size
                self.unicode_left -= size
                if not self.unicode_left:
                    try:
                        unit = int(self.hex_digits, 16)
                    except ValueError:
                        raise IngressError(400, "JSON Unicode 转义无效。") from None
                    self.hex_digits.clear()
                    if 0xDC00 <= unit <= 0xDFFF:
                        if not self.high_surrogate:
                            raise IngressError(400, "JSON 包含未配对的 Unicode 代理项。")
                        self.high_surrogate = False
                    elif self.high_surrogate:
                        raise IngressError(400, "JSON 包含未配对的 Unicode 代理项。")
                    else:
                        self.high_surrogate = 0xD800 <= unit <= 0xDBFF
                continue
            if self.escape:
                self.items += 1
                if self.items > MAX_JSON_ITEMS:
                    raise IngressError(413, "JSON 结构复杂度超过上限。")
                self.escape = False
                char = data[pos]
                pos += 1
                if char == ord('u'):
                    self.unicode_left = 4
                elif self.high_surrogate:
                    raise IngressError(400, "JSON 包含未配对的 Unicode 代理项。")
                continue
            match = (_STRING_SPECIAL if self.string else _SPECIAL).search(data, pos)
            if self.high_surrogate and (match is None or match.start() != pos or match[0] != b'\\'):
                raise IngressError(400, "JSON 包含未配对的 Unicode 代理项。")
            if match is None:
                break
            pos = match.end()
            char = match[0]
            if self.string:
                if char == b'\\':
                    self.escape = True
                elif char == b'"':
                    self.string = False
            elif char == b'"':
                self.string = True
                self.items += 1
            elif char in (b'{', b'['):
                self.depth += 1
                self.items += 1
                if self.depth > MAX_JSON_DEPTH:
                    raise IngressError(413, "JSON 嵌套深度超过上限。")
            elif char in (b'}', b']'):
                self.depth -= 1
            elif char in (b',', b':'):
                self.items += 1
            if self.items > MAX_JSON_ITEMS:
                raise IngressError(413, "JSON 结构复杂度超过上限。")
        if final and (self.high_surrogate or self.unicode_left):
            raise IngressError(400, "JSON Unicode 转义无效或代理项未配对。")
        return self.items - before


class IngressReceive:
    def __init__(self, receive, max_bytes, budget=None):
        self.receive = receive
        self.max_bytes = max_bytes
        self.budget = budget if budget is not None else ingress_budget
        self.held = [0, 0, 0]
        self._owners = 1
        self._closed = False
        self._lock = threading.Lock()
        self.done = False
        self.deadline = time.monotonic() + UPLOAD_TOTAL_SECONDS
        self.preflight = JSONPreflight()
        self.utf8 = codecs.getincrementaldecoder("utf-8")()
        self.budget.reserve((1, 0, 0))
        self.held[0] = 1

    async def __call__(self):
        if self.done:
            return await self.receive()
        remaining = min(UPLOAD_IDLE_SECONDS, self.deadline - time.monotonic())
        if remaining <= 0:
            raise IngressError(408, "请求体上传超时。")
        try:
            async with asyncio.timeout(remaining):
                message = await self.receive()
        except TimeoutError:
            raise IngressError(408, "请求体上传超时。") from None
        if message.get("type") == "http.request":
            body = message.get("body", b"")
            if self.held[1] + len(body) > self.max_bytes:
                from request_connection import BodyTooLargeError
                raise BodyTooLargeError(self.max_bytes)
            final = not message.get("more_body", False)
            if not body.isascii() or self.utf8.getstate()[0]:
                try:
                    self.utf8.decode(body, final=final)
                except UnicodeDecodeError:
                    raise IngressError(400, "请求体必须包含有效的 UTF-8 文本。") from None
            items = self.preflight.feed(body, final=final)
            self.budget.reserve((0, len(body), items))
            self.held[1] += len(body)
            self.held[2] += items
            self.done = final
            if final:
                # Upload admission is separate from generation admission. Keep
                # input memory charged, but don't cap an administrator's larger
                # upstream concurrency setting at the upload slot count.
                with self._lock:
                    self.budget.release((self.held[0], 0, 0))
                    self.held[0] = 0
        return message

    def close(self):
        with self._lock:
            if not self._closed:
                self._closed = True
                self._release()

    def _release(self):
        self._owners -= 1
        if not self._owners:
            self.budget.release(self.held)

    def retain(self):
        with self._lock:
            if not self._owners:
                self.budget.reserve(self.held)
            self._owners += 1
        released = False

        def release():
            nonlocal released
            with self._lock:
                if not released:
                    released = True
                    self._release()
        return release


async def early_generation_auth(scope):
    """Reuse each protocol's auth before FastAPI can read/parse its body."""
    from fastapi import Request
    from auth import get_api_key
    from routes.claude_api import ClaudeAPIError, get_claude_auth
    from routes.gemini_native import NativeAPIError, get_gemini_auth
    from request_errors import ingress_error_response

    request = Request(scope)
    path, headers = scope["path"], request.headers
    if path.startswith(("/v1/models/", "/v1beta/models/")):
        dependency = get_gemini_auth
        kwargs = dict(request=request, authorization=headers.get("authorization"),
                      x_goog_api_key=headers.get("x-goog-api-key"))
    elif path == "/v1/messages" or path.startswith("/v1/messages/"):
        dependency = get_claude_auth
        kwargs = dict(request=request, authorization=headers.get("authorization"),
                      x_api_key=headers.get("x-api-key"), x_goog_api_key=headers.get("x-goog-api-key"))
    else:
        dependency = get_api_key
        kwargs = dict(authorization=headers.get("authorization"))
    # Leave replacement dependencies to FastAPI's resolver (e.g. embedded apps
    # and tests); don't run the production credential check ahead of an override.
    if dependency in getattr(scope.get("app"), "dependency_overrides", {}):
        return None
    try:
        await dependency(**kwargs)
    except (ClaudeAPIError, NativeAPIError) as exc:
        return exc.response
    except HTTPException as exc:
        return ingress_error_response(path, exc.status_code, exc.detail)
    return None
