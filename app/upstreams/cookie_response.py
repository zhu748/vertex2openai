"""Cookie batchGraphql 响应解析、签名还原、诊断与 OpenAI 帧格式。"""

import hashlib
import json
import re
import time
from typing import Any, AsyncGenerator, AsyncIterator, Callable, Iterator, Optional

import httpx
from image_delivery import has_inline_image
from models import file_data_markdown
from message_processing import iter_giant_content

from response_validation import (IncompleteResponseError, StreamToolCallIds, complete_function_arguments,
                                 has_finish_reason, validate_generation_finish_reason)
from json_stream import MAX_JSON_OBJECT_BYTES, JsonObjects as _JsonObjects
from signature_store import signature_store
from upstreams.cookie_candidate_metadata import native_candidate_metadata, native_prompt_feedback
from thought_signatures import (
    attach_signature_extra,
    decode_thought_signature as signature_bytes_from_wire,
    encode_thought_signature,
    is_thought_part,
    ordinary_part_metadata,
)


def _current_created() -> int:
    """Default for standalone encoders; delivery passes the request's timestamp."""
    return int(time.time())


def _stable_cookie_tool_call_id(response_id: str, call_index: int, name: str, args: Any) -> str:
    """Generate a deterministic fallback when batchGraphql omits a call id."""
    # PRF-P3-3：去掉 sort_keys——确定性只需「同一响应字节流重解析得到
    # 同一顺序」（上游 JSON 解析顺序稳定），跨响应本就有 response_id
    # 区分；sort_keys 的全量键排序对长参数是无谓开销。
    canonical = json.dumps(args or {}, ensure_ascii=False, separators=(",", ":"))
    digest = hashlib.sha256(f"{response_id}\0{call_index}\0{name}\0{canonical}".encode()).hexdigest()[:20]
    return f"call_{digest}"


def apply_wire_signature(data: dict, ordinary_metadata: list, part_order: list) -> Optional[bytes]:
    """把 batchGraphql 的 thoughtSignature 事件并入 ordinary 元数据（流式/非流式共用）。

    上游独立的 signature_only part 始终占自己的位置，不覆盖前一块；
    伴随文本或媒体的签名才挂到相应块上。返回原始签名字节，
    空签名返回 None 且不做任何修改。
    """
    signature = signature_bytes_from_wire(data.get("value"))
    if not signature:
        return None
    if data.get("part_kind") == "signature_only" or data.get("empty_part"):
        part_order.append({"type": "ordinary", "index": len(ordinary_metadata)})
        ordinary_metadata.append(ordinary_part_metadata(data["part_kind"], "", signature))
    elif ordinary_metadata:
        ordinary_metadata[-1]["thought_signature"] = encode_thought_signature(signature)
        if data.get("part_kind") == "image":
            # The media already lives in content; metadata retains its exact
            # slot/signature instead of duplicating the base64 as plain text.
            ordinary_metadata[-1].update(kind="image", text="")
    return signature


def openai_tool_call(response_id: str, call_index: int, call: dict, *, include_arguments: bool = True) -> dict:
    name, args = call["name"], call.get("args") or {}
    call_id = call.get("id") or _stable_cookie_tool_call_id(response_id, call_index, name, args)
    signature = signature_bytes_from_wire(call.get("thought_signature"))
    # QUA-P2-2：三态拓扑写入收敛到 register_call_signature 单一入口。
    signature_store.register_call_signature(
        call_id, signature, unsigned_follower=call_index > 0)
    payload = {
        "id": call_id,
        "type": "function",
        "function": {"name": name},
    }
    if include_arguments:
        payload["function"]["arguments"] = json.dumps(args, ensure_ascii=False)
    attach_signature_extra(payload, signature)
    return payload


async def iter_json_objects(
    response: "httpx.Response", max_object_bytes: Optional[int] = None, *, on_text_chunk: Optional[Callable]=None
) -> AsyncGenerator[dict, None]:
    """解析 batchGraphql 的连续对象，忽略数组分隔符及防 XSSI 前缀。

    正则在 C 层跳过普通文本，只在引号、转义符和花括号处更新状态。
    对象边界只扫描一遍，避免对大图片的每个小块重复解析整个未完成对象。
    """
    if isinstance(response, TextSource) and max_object_bytes is None:
        # 非流式正文已完整驻留：交由拉取式游标解析（limit 语义见其注释）。
        cursor = CompleteTextObjectCursor(response.text)
        while (obj := cursor.next_object()) is not None:
            yield obj
            del obj
        return
    # A caller asking for an explicit per-object cap gets the strict byte
    # parser even for a synthetic buffered source. Ordinary wire streams
    # always retain their transport-independent per-object budget.
    parser = _JsonObjects(MAX_JSON_OBJECT_BYTES if max_object_bytes is None else max_object_bytes)
    # 提前退出（消费方错误分支 return / 异常 / 取消）时显式关闭底层文本
    # 迭代器：httpx 的 aiter_text 与 curl shim 的 _gen 均无内部 finally，
    # 此前仅靠外层 async with client.stream 的 CM 与 asyncgen GC finalization
    # 兜底，任何新调用点漏包 CM 就会漏连接（2026-10 补齐；aclose 幂等）。
    aiter = response.aiter_text()
    try:
        async for chunk in aiter:
            if chunk:
                if on_text_chunk is not None:
                    on_text_chunk(chunk)
                for obj in parser.feed(chunk):
                    yield obj
                    del obj
            del chunk  # do not retain a completed raw read while waiting for another
    finally:
        close = getattr(aiter, "aclose", None)
        if close is not None:
            await close()
    parser.finish()


# 非流式正文里这些事件类型说明响应已有正文/工具内容：诊断采样器应停止留存
# （「无正文」样本只对失败路径有价值，正文已到则 dump 永不触发；生图响应
# 的单个对象可达 30-90MB inlineData，留存即钉住整份 base64 到函数返回）。
BODY_EVENT_TYPES = frozenset({"text", "image", "thought_image", "hidden_thought", "function_call", "native_part"})


class CompleteTextObjectCursor:
    """非流式完整正文的拉取式 JSON 对象解析游标。

    为什么不用生成器（2026-11 第五轮）：生成器形态在 yield 挂起期间，其帧
    局部变量会把刚解析出的对象（含 4K inlineData 的巨型 dict）钉到消费方
    处理完整个对象之后——消费方 `del obj` 也无济于事。拉取式游标在
    next_object() 返回后不持有任何引用，消费方 del obj 即可让解析出的
    dict（含整份 base64）在事件消费（markdown 拼接/SSE 分帧）前释放，
    生图整取的收集期峰值全量驻留从 3 份降到 2 份。

    完整体已经由 HTTP 传输层在接收时按字节限流；流式解析器的单对象上限
    只约束尚在增长的缓冲。不要在此用正文字符数充当 UTF-8 字节预算，
    也不需要为逐个对象的再次计量而复制整段文本。
    """

    def __init__(self, text: str, limit: int = MAX_JSON_OBJECT_BYTES):
        self._text = text
        # limit remains accepted for compatibility with previously buffered callers.
        self._position = 0
        self._decoder = json.JSONDecoder()

    def next_object(self) -> Optional[dict]:
        """返回下一个对象；没有更多对象时返回 None；无效 JSON 抛 IncompleteResponseError。"""
        text = self._text
        start = text.find("{", self._position)
        if start < 0:
            return None
        try:
            obj, self._position = self._decoder.raw_decode(text, start)
        except (json.JSONDecodeError, RecursionError) as exc:
            raise IncompleteResponseError("上游返回无效或不完整的 JSON 对象。") from exc
        return obj


def _summ_obj(obj: dict, width: int = 700) -> str:
    """把一个响应对象压成截断的单行 JSON（诊断日志用，图片 base64 会掐掉）。"""
    try:
        s = json.dumps(obj, ensure_ascii=False)
    except Exception:
        s = str(obj)
    s = re.sub(r'"data":\s*"[A-Za-z0-9+/=]{64,}"', '"data": "<base64 已省略>"', s)
    return s[:width] + ("…" if len(s) > width else "")


class TextSource:
    """把已取回的完整文本包成异步文本迭代器（非流式路径复用流式解析器用）。

    旧实现在重试循环体内每次重建 _F / _FakeResponse 两个一次性类（解释器
    每次重新编译类体），提升为模块级单一实现。
    """

    def __init__(self, text: str):
        self.text = text

    async def aiter_text(self) -> AsyncIterator[str]:
        yield self.text


class RawSampler:
    """收集原始响应对象的首尾样本，供“无正文/空响应”时输出诊断日志。

    只存原始对象引用（上界 keep 个）；_summ_obj 的完整 json.dumps + 截断
    推迟到 dump() 才做。旧实现在 add() 里对**每个**对象都先序列化一遍、
    随即丢弃（生图响应单个对象可达数 MB，等于在事件循环上做无谓的
    全量序列化再扔掉）。

    留存停止（2026-11 第五轮）：样本只服务「无正文」诊断，正文/工具内容
    一旦到达，dump 永不触发，而留存会把含巨型 inlineData 的对象（4K
    单张 30-90MB）钉到函数返回。见到 BODY_EVENT_TYPES 事件即调
    stop_retaining()：丢弃已存引用且后续只计数不再留存；无正文路径
    （含「只有思考」）不受影响，样本照常保留。
    """

    def __init__(self, keep: int = 2):
        self.keep = keep
        self._head: list = []
        self._tail: list = []
        self.count = 0
        self._retaining = True

    def add(self, obj: dict) -> None:
        self.count += 1
        if not self._retaining:
            return
        if len(self._head) < self.keep:
            self._head.append(obj)
        else:
            self._tail.append(obj)
            if len(self._tail) > self.keep:
                self._tail.pop(0)

    def stop_retaining(self) -> None:
        """正文已到：立即丢弃已存样本并停止留存（幂等；count 继续累计）。"""
        self._retaining = False
        self._head.clear()
        self._tail.clear()

    def dump(self) -> str:
        if self.count == 0:
            return "（上游未返回任何可解析的 JSON 对象）"
        lines = [f"共 {self.count} 个对象；首尾样本："]
        lines += [f"  ▸ {_summ_obj(o)}" for o in self._head]
        if self._tail:
            lines.append(f"  …")
            lines += [f"  ▸ {_summ_obj(o)}" for o in self._tail]
        return "\n".join(lines)


def _wire_object(value: Any, label: str, *, allow_null: bool=True):
    if value is None and allow_null:
        return {}
    if not isinstance(value, dict):
        raise IncompleteResponseError(f"上游 {label} 必须是 JSON 对象。")
    return value


def _wire_list(value: Any, label: str):
    if value is None:
        return []
    if not isinstance(value, list):
        raise IncompleteResponseError(f"上游 {label} 必须是数组。")
    return value


def _complete_wire_call(raw: Any) -> Optional[dict]:
    """Ignore proto defaults on ordinary Parts, but never drop an incomplete call."""
    call = _wire_object(raw, "functionCall")
    if not call:
        return None
    name, call_id = call.get("name"), call.get("id")
    if name is not None and not isinstance(name, str):
        raise IncompleteResponseError("上游 functionCall.name 必须是字符串。")
    if call_id is not None and not isinstance(call_id, str):
        raise IncompleteResponseError("上游 functionCall.id 必须是字符串。")
    args = _wire_object(call.get("args"), "functionCall.args")
    if not (name and name.strip() or call_id or args or call.get("partialArgs")
            or call.get("partial_args") or call.get("willContinue") or call.get("will_continue")):
        return None
    complete_function_arguments(call)
    return {"name": name, "args": args, **({"id": call_id} if call_id else {})}


def _prompt_block_message(feedback: Any) -> Optional[str]:
    if isinstance(feedback, dict):
        reason = str(feedback.get("blockReason") or "")
        if reason and not reason.upper().endswith("UNSPECIFIED"):
            message = feedback.get("blockReasonMessage")
            return f"{reason}（{message}）" if message else reason
    return None


def envelope_errors(obj: dict) -> list:
    """Only protocol fields, never 'error' keys in generated model content."""
    # Proto defaults such as error: null or {} are not real errors.
    error = obj.get("error")
    return [error] if error else _wire_list(obj.get("errors"), "errors")


def has_pending_inline_image(obj: dict) -> bool:
    """Only media preceding an authoritative error could need the media worker."""
    # The parser below owns validation; this is only a cheap scheduling hint.
    if not isinstance(obj, dict) or obj.get("error") or obj.get("errors"):
        return False
    results = obj.get("results")
    if not isinstance(results, list):
        return False
    for result in results:
        if not isinstance(result, dict) or result.get("errors"):
            break
        if has_inline_image(result.get("data"), depth=3):
            return True
    return False


class CookieCandidateBoundary:
    """A Cookie attempt has exactly one candidate; remember only its first finish."""
    def __init__(self, *, check_tool_ids: bool = False) -> None:
        self.finish: str | None = None
        self.tool_ids = StreamToolCallIds() if check_tool_ids else None

    def observe(self, reason: Any, parts: list) -> None:
        finish = str(reason) if has_finish_reason(reason) else None
        if self.finish is not None:
            if parts:
                raise IncompleteResponseError("上游候选已结束，却继续返回内容。")
            if finish is not None and finish != self.finish:
                raise IncompleteResponseError("上游候选返回了相互矛盾的结束原因。")
        elif finish is not None:
            self.finish = finish


def _finish_native_part(native_part: dict, signature: Any, *, empty_text: bool, thought: bool) -> None:
    if signature:
        if not isinstance(signature, str):
            raise IncompleteResponseError("上游 thoughtSignature 必须是字符串。")
        # Signed standalone empty text is data, unlike proto defaults on
        # functionCall/media Parts. Never invent it for an absent text field.
        if not native_part and empty_text:
            native_part["text"] = ""
        native_part["thoughtSignature"] = signature
    if native_part and thought:
        native_part["thought"] = True


def extract_events(obj: dict, *, preserve_parts: bool = False, strip_thoughts: bool = False,
                   boundary: CookieCandidateBoundary | None = None) -> Iterator[tuple]:
    boundary = boundary or CookieCandidateBoundary()
    obj = _wire_object(obj, "响应", allow_null=False)
    errors = envelope_errors(obj)
    if errors:
        yield "error", errors[0]
        return

    results = _wire_list(obj.get("results"), "results")
    for result in results:
        result = _wire_object(result, "result", allow_null=False)
        errors = _wire_list(result.get("errors"), "errors")
        if errors:
            # All delivery modes terminate on the first proven provider error.
            # Do not validate or materialize discarded results after it.
            yield "error", errors[0]
            return

        data = _wire_object(result.get("data"), "data")
        if not data:
            continue

        # 提示词级拦截（promptFeedback.blockReason）：无候选、直接被挡。
        # 注意：batchGraphql 每个流式块都会带 blockReason=BLOCKED_REASON_UNSPECIFIED
        # （proto 枚举默认值 = 未拦截），必须过滤掉，只透出真实拦截。
        usage = data.get("usageMetadata")
        feedback = data.get("promptFeedback")
        blocked = native_prompt_feedback(feedback) if preserve_parts else _prompt_block_message(feedback)
        if blocked:
            # The transport ends as soon as it sees the prompt block. Deliver
            # this response's real usage first; never generate contents for it.
            if preserve_parts and isinstance(usage, dict) and usage:
                yield ("usage", usage)
            yield ("blocked", blocked)
            if preserve_parts:
                continue

        candidates = _wire_list(data.get("candidates"), "candidates")
        if len(candidates) > 1:
            raise IncompleteResponseError("Cookie 上游返回多个候选，当前桥接无法无损转换。")
        for candidate in candidates:
            if isinstance(candidate, dict) and candidate.get("index", 0) not in (None, 0):
                raise IncompleteResponseError("Cookie 上游返回非零候选索引，当前桥接无法无损转换。")
            candidate = _wire_object(candidate, "candidate", allow_null=False)
            content_obj = _wire_object(candidate.get("content"), "content")
            parts = _wire_list(content_obj.get("parts"), "parts")
            boundary.observe(candidate.get("finishReason"), parts)
            if preserve_parts:
                if (candidate.get("finishReason") == "CONTINUATION"
                        or candidate.get("continuationToken") or candidate.get("continuation_token")):
                    # A token from this private envelope cannot be replayed via
                    # the public bridge. Do not falsely promise resumable output
                    # or emit candidate parts from this known-incomplete record.
                    raise IncompleteResponseError("Cookie 原生通道无法保留上游的 continuationToken 续写契约。")
                metadata = native_candidate_metadata(candidate)
                if metadata:
                    yield "candidate_metadata", metadata
            else:
                # Validate before yielding calls from the same failing chunk.
                # Gemini-native consumers retain their original finishReason.
                validate_generation_finish_reason(candidate.get("finishReason"))

            for part in parts:
                part = _wire_object(part, "part", allow_null=False)
                text = part.get("text", "")
                if text is not None and not isinstance(text, str):
                    raise IncompleteResponseError("上游 part.text 必须是字符串。")
                native_part = {}
                ordinary_kind = "signature_only"  # changed only by an ordinary event emitted for this Part
                thought = is_thought_part(part)
                hidden = thought and strip_thoughts and not preserve_parts
                if text:
                    native_part["text"] = text
                if text and not preserve_parts and not hidden:
                    ordinary_kind = "thought" if thought else "text"
                    yield ordinary_kind, text

                function_call = _complete_wire_call(part.get("functionCall"))
                if function_call is not None:
                    if boundary.tool_ids is not None:
                        boundary.tool_ids.observe(function_call.get("id"))
                    native_part["functionCall"] = function_call
                    if not preserve_parts:
                        yield "function_call", {
                            **function_call, "thought_signature": part.get("thoughtSignature") or None,
                        }
                inline_data = _wire_object(part.get("inlineData"), "inlineData")
                if inline_data:
                    mime_type = inline_data.get("mimeType", "")
                    b64 = inline_data.get("data", "")
                    if not isinstance(mime_type, str) or not isinstance(b64, str):
                        raise IncompleteResponseError("上游 inlineData 的 mimeType / data 必须是字符串。")
                    if mime_type and b64:
                        native_part["inlineData"] = {"mimeType": mime_type, "data": b64}
                        if not preserve_parts and not hidden:
                            from image_delivery import image_markdown
                            image_md = image_markdown(b64, mime_type, encoded=True, label="Generated Image")
                            ordinary_kind = "image"
                            yield "thought_image" if thought else ordinary_kind, image_md

                file_data = _wire_object(part.get("fileData"), "fileData")
                if file_data:
                    uri, mime_type = file_data.get("fileUri", ""), file_data.get("mimeType", "")
                    uri = "" if uri is None else uri
                    mime_type = "" if mime_type is None else mime_type
                    if not isinstance(uri, str) or not isinstance(mime_type, str):
                        raise IncompleteResponseError("上游 fileData 的 fileUri / mimeType 必须是字符串。")
                    if uri:
                        native_part["fileData"] = dict(file_data)
                        if not preserve_parts and not hidden:
                            is_image = not mime_type or mime_type.startswith("image/")
                            ordinary_kind = "image" if is_image else ("thought" if thought else "text")
                            yield "thought_image" if thought and is_image else ordinary_kind, file_data_markdown(uri, mime_type)

                if hidden and any(key in native_part for key in ("text", "inlineData", "fileData")):
                    # Count real upstream activity without retaining or rendering
                    # its payload. If signed, the next event owns an opaque slot.
                    yield "hidden_thought", True

                signature = part.get("thoughtSignature")
                if signature and function_call is None and not preserve_parts:
                    # A genuine empty thought has no public delta, but its signed
                    # Part still has the thought bit and owns its own position.
                    if not native_part and thought and part.get("text") == "":
                        yield "thought_signature", {"value": signature, "part_kind": "thought", "empty_part": True}
                    else:
                        # Emit attached signatures immediately after their own
                        # text/media event; stripped thoughts stay opaque.
                        yield "thought_signature", {"value": signature, "part_kind": ordinary_kind}

                if preserve_parts:
                    _finish_native_part(native_part, signature, empty_text=part.get("text") == "", thought=thought)
                    if native_part:
                        yield "native_part", native_part

            # finishReason 全量透出（含 SAFETY/PROHIBITED_CONTENT 等），由消费方统一映射；
            # 旧实现只认 STOP/MAX_TOKENS/SAFETY 白名单，其余被静默吞掉，导致“无正文”无从排查。
            # FINISH_REASON_UNSPECIFIED 是流式中间块的枚举默认值（非真实结束），需过滤。
            finish_reason = candidate.get("finishReason")
            if finish_reason and not str(finish_reason).upper().endswith("UNSPECIFIED"):
                # 安全分只在最后一块透出：每个流式块都带 safetyRatings，
                # 逐块附加会把同一份评分重复插进正文。
                ratings = candidate.get("safetyRatings")
                if ratings:
                    yield ("safety", ratings)
                yield ("finish", str(finish_reason))

        # 尽力解析 token 用量（私有接口通常不回传；保留解析以防未来补上）
        if isinstance(usage, dict) and usage:
            yield ("usage", usage)


def make_openai_chunk(
    response_id: str,
    model: str,
    content: str = None,
    reasoning_content: str = None,
    finish_reason: str = None,
    role: str = None,
    tool_calls: Optional[list[dict]] = None,
    extra_content: Optional[dict] = None,
    *, created: Optional[int] = None,
) -> str:
    delta = {}
    if role:
        delta["role"] = role
    if content is not None:
        delta["content"] = content
    if reasoning_content is not None:
        delta["reasoning_content"] = reasoning_content
    if tool_calls is not None:
        delta["tool_calls"] = tool_calls
    if extra_content is not None:
        delta["extra_content"] = extra_content

    chunk = {
        "id": response_id,
        "object": "chat.completion.chunk",
        "created": _current_created() if created is None else created,
        "model": model,
        "choices": [
            {
                "index": 0,
                "delta": delta,
                "finish_reason": finish_reason,
            }
        ],
    }
    return f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"


def iter_openai_content_chunks(
    response_id: str,
    model: str,
    content: str,
    role: "str | None" = None,
    *, created: Optional[int] = None,
) -> Iterator[str]:
    """超大 content（4K 生图 base64）→ 按需产出的多帧 SSE 生成器。

    物化版（先建完整帧列表）会让原文/切片表/序列化帧三份全量拷贝同时
    在内存里（4K 生图 base64 单张 13-40MB，512MB 容器上叠加解析链直接
    OOMKill）。生成器版每帧只在 yield 瞬间存在一个 256KiB 切片 + 一条
    序列化帧；切片与单帧输出逐字节一致（客户端按 delta 拼接语义无感）。
    role 只注入首帧（对齐官方帧序）。需要列表形态的调用方包一层
    list(iter_openai_content_chunks(...)) 即可。
    """
    if not content:
        return
    if created is None:
        created = _current_created()
    first = True
    for piece in iter_giant_content(content):
        yield make_openai_chunk(
            response_id,
            model,
            content=piece,
            role=role if first else None,
            created=created,
        )
        first = False
