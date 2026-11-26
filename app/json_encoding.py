"""Encode large inline media without full-size JSON text/UTF-8 temporaries."""
import asyncio
import io
import json
import threading

from fastapi.responses import JSONResponse

JSON_STRING_CHARS = 64 * 1024

# REQ-P2-2 阈值：估算序列化规模超过 1 MiB 的 JSON 响应不再在事件循环上
# 同步 json.dumps + encode（轮询一次可拉 500 帧、单缓冲上限 64MB，生图
# 巨帧场景数百毫秒至秒级全站停顿），改在线程池构造响应。
LARGE_RESPONSE_CHARS = 1024 * 1024


def estimate_json_chars(value) -> int:
    """O(节点数) 的 JSON 序列化规模估算，不物化任何拷贝。

    str 按 len 估（转义最坏放大 6 倍，但实际负载以 base64 / 文本为主，
    len 是足够好的代理）；dict / list 递归；标量按小常数。len(str) 与
    isascii 都是 O(1)，MB 级字符串只读头部。
    """
    if isinstance(value, str):
        return len(value) + 2
    if isinstance(value, dict):
        return sum(estimate_json_chars(k) + 2 + estimate_json_chars(v) for k, v in value.items()) + 2
    if isinstance(value, (list, tuple)):
        return sum(estimate_json_chars(v) + 1 for v in value) + 2
    return 8


async def json_response_offloop(content, *, status_code: int = 200,
                                headers=None, background=None) -> JSONResponse:
    """JSON 响应构造：小负载直路；超过阈值移到线程池（REQ-P2-2）。

    大响应复用按片段编码的渲染器，避免同时持有整份 JSON 文本和编码后的字节。
    两条路径返回相同的 JSONResponse 线格式，并保留交付完成后的回调。
    """
    if estimate_json_chars(content) <= LARGE_RESPONSE_CHARS:
        return JSONResponse(status_code=status_code, content=content,
                            headers=headers, background=background)

    # Cancelling the awaiting task alone cannot interrupt an already-running
    # worker. Stop its abandoned per-reader copy at the next encoded chunk.
    cancel_render = threading.Event()
    try:
        return await asyncio.to_thread(LargeJSONResponse, status_code=status_code, content=content,
                                       headers=headers, background=background, cancel_render=cancel_render)
    except asyncio.CancelledError:
        cancel_render.set()
        raise


def iter_json_bytes(value, *, compact=False, allow_nan=True, sort_keys=False):
    """Match json.dumps(ensure_ascii=False), with bounded string encoding.

    JSONEncoder.iterencode alone still materializes each entire quoted string.
    Splitting the string before escaping avoids that copy for inline Base64.
    Each fragment is transport data; concatenation is one JSON document.
    """
    item_sep, key_sep = (b",", b":") if compact else (b", ", b": ")
    active = set()

    def string_chunks(text):
        if len(text) <= JSON_STRING_CHARS:
            yield json.dumps(text, ensure_ascii=False).encode("utf-8")
            return
        yield b'"'
        for start in range(0, len(text), JSON_STRING_CHARS):
            escaped = json.dumps(text[start:start + JSON_STRING_CHARS], ensure_ascii=False)
            yield escaped[1:-1].encode("utf-8")
        yield b'"'

    def encode(node):
        if isinstance(node, str):
            yield from string_chunks(node)
        elif isinstance(node, (dict, list, tuple)):
            ident = id(node)
            if ident in active:
                raise ValueError("Circular reference detected")
            active.add(ident)
            try:
                is_dict = isinstance(node, dict)
                yield b"{" if is_dict else b"["
                items = (sorted(node.items()) if sort_keys else node.items()) if is_dict else node
                for index, item in enumerate(items):
                    if index:
                        yield item_sep
                    if is_dict:
                        key, item = item
                        if not isinstance(key, str):
                            if key is None or isinstance(key, (bool, int, float)):
                                key = json.dumps(key, allow_nan=allow_nan)
                            else:
                                raise TypeError("JSON object keys must be str, int, float, bool or None")
                        yield from string_chunks(key)
                        yield key_sep
                    yield from encode(item)
                yield b"}" if is_dict else b"]"
            finally:
                active.remove(ident)
        else:
            yield json.dumps(node, ensure_ascii=False, allow_nan=allow_nan).encode("utf-8")

    yield from encode(value)


def encoded_json_size(value) -> int:
    """Exact json.dumps(ensure_ascii=False) byte size without a whole-body copy."""
    return sum(len(chunk) for chunk in iter_json_bytes(value))


# A dense logprob tree can have tens of thousands of small scalars but few
# string characters. Admission for inline accounting must bound node work too.
_SIZE_WORKERS = threading.BoundedSemaphore(4)
_INLINE_SIZE_NODES = 256
_INLINE_SIZE_CHARS = 64 * 1024


def _small_size_job(value) -> bool:
    nodes_left, chars_left = _INLINE_SIZE_NODES, _INLINE_SIZE_CHARS

    def visit(node):
        nonlocal nodes_left, chars_left
        nodes_left -= 1
        if nodes_left < 0:
            return False
        if isinstance(node, str):
            chars_left -= len(node)
            return chars_left >= 0
        if isinstance(node, dict):
            return len(node) * 2 <= nodes_left and all(visit(k) and visit(v) for k, v in node.items())
        if isinstance(node, (list, tuple)):
            return len(node) <= nodes_left and all(visit(v) for v in node)
        return True

    return visit(value)


def _size_worker(value, reservation):
    try:
        return encoded_json_size(value)
    finally:
        reservation.release()  # physical completion, not cancellation of its reader


async def _size_cooperatively(value):
    """Saturated: do not submit or retain another worker job. Yield in bounded batches."""
    total = fragments = since_yield = 0
    for fragment in iter_json_bytes(value):
        length = len(fragment)
        total += length
        since_yield += length
        fragments += 1
        if fragments >= 256 or since_yield >= JSON_STRING_CHARS:
            await asyncio.sleep(0)
            fragments = since_yield = 0
    return total


async def json_size_offloop(value) -> int:
    """Exact accounting with bounded loop preflight and physically capped submissions."""
    if _small_size_job(value):
        return encoded_json_size(value)
    slots = _SIZE_WORKERS
    if not slots.acquire(blocking=False):
        return await _size_cooperatively(value)
    try:
        work = asyncio.get_running_loop().run_in_executor(None, _size_worker, value, slots)
    except BaseException:
        slots.release()
        raise
    try:
        return await asyncio.shield(work)
    except asyncio.CancelledError:
        # Do not cancel a queued/active physical owner. A late encoder failure
        # must be retrieved even though the request already published its timeout.
        work.add_done_callback(lambda done: None if done.cancelled() else done.exception())
        raise


class LargeJSONResponse(JSONResponse):
    """Keep one final JSON body without also creating whole-body text/bytes copies.

    PRF-P3-2 处置记录（2026-11 实测）：曾按审查建议改 b"".join 分片拼接，
    tracemalloc 实测峰值 16.0MB（8MB 内容）——CPython 的 join(generator)
    会先把全部片段物化成列表（8MB 片段 + 8MB 结果 = 2×），反而高于
    BytesIO 递增写入的 8.76MB（内部缓冲摊销增长，getvalue 拷贝未叠加
    峰值）；join(list) 同为 16.0MB。结论：保留 BytesIO 实现，join 方案
    否决。完整流式下发（StreamingResponse + iter_json_bytes 生成器）
    才能把峰值降一档，但被 .body 契约阻塞：channel_dispatch /
    claude_api / chat_api 续传泵 / cookie_delivery / gemini_native 五处
    直接 json.loads(result.body)，改造需连同消费方一起迁移，列入后续
    迭代（benchmark 见 scripts/measure_join_peak.py）。
    """
    def __init__(self, *args, cancel_render=None, **kwargs):
        self._cancel_render = cancel_render
        super().__init__(*args, **kwargs)

    def render(self, content):
        with io.BytesIO() as output:
            for chunk in iter_json_bytes(content, compact=True, allow_nan=False):
                if self._cancel_render is not None and self._cancel_render.is_set():
                    raise asyncio.CancelledError()
                output.write(chunk)
            return output.getvalue()
