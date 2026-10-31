"""Ordered, request-local proxy failover before a response is delivered.

Never invent a direct route. Model/auth/quota errors keep their existing retry
policy. Once streaming data has been exposed, errors propagate without replay.
"""

from contextlib import asynccontextmanager
from types import SimpleNamespace

import httpx

from proxy_resolver import ProxyResolveError, mask_proxy_uri, resolve_proxy_uri_async, resolved_proxy_scope
from runtime_state import get_effective_proxy_uris_for_account

PROXY_FAILURE_STATUSES = {407, 502, 503, 504}


def is_proxy_failure(error):
    from first_reply import FirstReplyTimeout
    from response_validation import ToolStreamError
    if isinstance(error, (FirstReplyTimeout, ToolStreamError)):
        # 首回复超时意味着上游连思考分片都没有输出（思考回传开启时思考也算
        # 首回复）：语义上与 429 一样属于“上游无响应”，应交给外层自动重试
        # （可换 Key），而不是切换出口。已解析响应的本地工具校验错误同样不能
        # 仅因携带 502/504 就被归因为坏出口。
        return False
    if isinstance(error, ProxyResolveError):
        return True
    if isinstance(error, httpx.TransportError):
        return not isinstance(error, (httpx.PoolTimeout, httpx.LocalProtocolError, httpx.UnsupportedProtocol))
    code = (getattr(error, "status_code", None) or getattr(error, "code", None)
            or getattr(getattr(error, "response", None), "status_code", None))
    if code in PROXY_FAILURE_STATUSES:
        return True
    # curl_cffi transport failures. Its exception codes are curl codes, not HTTP.
    if type(error).__module__.startswith("curl_cffi"):
        return code in {5, 6, 7, 18, 28, 35, 52, 55, 56, 60, 67, 97}
    return False


async def close_response(response):
    close = getattr(response, "aclose", None)
    if close:
        await close()


class ProxyChain:
    def __init__(self, record=None, *, candidates=None):
        self.candidates = tuple(candidates if candidates is not None else
                                get_effective_proxy_uris_for_account(record)) or ("",)
        self.index = 0

    @property
    def current(self):
        return self.candidates[self.index]

    def advance(self, error=None, *, status=None):
        if self.index + 1 >= len(self.candidates):
            return False
        if status not in PROXY_FAILURE_STATUSES and not is_proxy_failure(error):
            return False
        previous = self.current
        self.index += 1
        from logger import stats
        stats.add_retry()
        reason = f"HTTP {status}" if status is not None else type(error).__name__
        print(f"🔀 [代理切换] {mask_proxy_uri(previous) or '直连'} 失败（{reason}），"
              f"按顺序尝试 {self.index + 1}/{len(self.candidates)}："
              f"{mask_proxy_uri(self.current) or '直连'}")
        return True

    async def prepare(self, factory):
        while True:
            try:
                resolved = await resolve_proxy_uri_async(self.current)
                with resolved_proxy_scope(self.current, resolved):
                    return factory(self.current, resolved)
            except Exception as exc:
                if not self.advance(exc):
                    raise

    async def run(self, operation):
        """Operation receives (raw URI, resolved URL); resources belong to it."""
        attempted = False
        while True:
            try:
                from first_reply import proxy_attempt
                async with proxy_attempt(len(self.candidates) > 1):
                    resolved = await resolve_proxy_uri_async(self.current)
                    if attempted:
                        from request_tracking import upstream_attempt
                        upstream_attempt()
                    attempted = True
                    with resolved_proxy_scope(self.current, resolved):
                        result = await operation(self.current, resolved)
            except Exception as exc:
                if self.advance(exc):
                    continue
                raise
            if self.advance(status=getattr(result, "status_code", None)):
                await close_response(result)
                continue
            return result

    def run_sync(self, operation):
        """Worker-thread equivalent, used for bounded remote image downloads.

        与 run 不同，这里不做基于返回值状态码的切换：operation 必须对失败
        状态自行抛出异常（如 raise_for_status()），否则不会触发换出口。
        """
        from proxy_resolver import resolve_proxy_uri
        while True:
            try:
                resolved = resolve_proxy_uri(self.current)
                return operation(self.current, resolved)
            except Exception as exc:
                if not self.advance(exc):
                    raise

    def bind_to_generation(self):
        from proxy_resolver import current_proxy_usage
        usage = current_proxy_usage.get()
        if usage is not None:
            usage.route = self
        return self


class FailoverHTTPClient:
    """post/get/stream facade over pooled httpx or curl clients."""
    def __init__(self, chain, factory):
        self.chain, self.factory = chain, factory

    async def _request(self, method, *args, **kwargs):
        async def operation(raw, resolved):
            return await getattr(self.factory(raw, resolved), method)(*args, **kwargs)
        return await self.chain.run(operation)

    async def post(self, *args, **kwargs):
        return await self._request("post", *args, **kwargs)

    async def get(self, *args, **kwargs):
        return await self._request("get", *args, **kwargs)

    @asynccontextmanager
    async def stream(self, *args, **kwargs):
        attempted = False
        while True:
            try:
                from first_reply import proxy_attempt
                async with proxy_attempt(len(self.chain.candidates) > 1):
                    client = await self.chain.prepare(self.factory)
                    context = client.stream(*args, **kwargs)
                    if attempted:
                        from request_tracking import upstream_attempt
                        upstream_attempt()
                    attempted = True
                    response = await context.__aenter__()
            except Exception as exc:
                if self.chain.advance(exc):
                    continue
                raise
            if self.chain.advance(status=response.status_code):
                await context.__aexit__(None, None, None)
                continue
            break
        # Do not catch exceptions from the response consumer: it may already
        # have delivered content. Always close the chosen response on cancel.
        async with _entered_context(context, response) as selected:
            yield selected


@asynccontextmanager
async def _entered_context(context, response):
    try:
        yield response
    except BaseException as exc:
        # 不依赖 __aexit__ 的返回值决定是否重抛：带重试语义的 CM 返回 True
        # 会把消费者的异常静默吞掉。资源释放后必须无条件向上抛。
        await context.__aexit__(type(exc), exc, exc.__traceback__)
        raise
    else:
        await context.__aexit__(None, None, None)


class FailoverSDKClient:
    """SDK facade that retains the account while trying its proxy candidates."""
    def __init__(self, chain, factory):
        self.chain, self.factory = chain, factory
        self.client = None
        self.aio = SimpleNamespace(models=self)

    async def prepare(self):
        self.client = await self.chain.prepare(self.factory)
        return self

    def __getattr__(self, name):
        return getattr(self.client, name)

    async def generate_content(self, **kwargs):
        async def operation(raw, resolved):
            self.client = self.factory(raw, resolved)
            return await self.client.aio.models.generate_content(**kwargs)
        return await self.chain.run(operation)

    async def generate_content_stream(self, **kwargs):
        async def events():
            async def start(raw, resolved):
                from first_reply import sdk_has_reply
                from response_validation import IncompleteResponseError, has_finish_reason

                self.client = self.factory(raw, resolved)
                source = await self.client.aio.models.generate_content_stream(**kwargs)
                prefix, size = [], 0
                try:
                    async for chunk in source:
                        prefix.append(chunk)
                        # Metadata is not a reply. Keep it private until real
                        # output or a terminal model decision makes replay unsafe.
                        if (sdk_has_reply(chunk) or getattr(getattr(chunk, "prompt_feedback", None), "block_reason", None)
                                or any(has_finish_reason(getattr(c, "finish_reason", None))
                                       for c in getattr(chunk, "candidates", None) or [])):
                            return source, prefix
                        size += len(str(chunk).encode("utf-8"))
                        if size > 2 * 1024 * 1024:
                            raise IncompleteResponseError("首回复前的元数据缓冲超过 2 MiB。")
                    raise IncompleteResponseError("上游连接已结束，但未收到模型回复或结束标记。")
                except BaseException:
                    await source.aclose()
                    raise
            source, prefix = await self.chain.run(start)
            try:
                for chunk in prefix:
                    yield chunk
                async for chunk in source:
                    yield chunk
            finally:
                await source.aclose()
        return events()
