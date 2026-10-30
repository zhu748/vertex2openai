"""Cookie batchGraphql 响应解析、签名还原、诊断与 OpenAI 帧格式。"""

import base64
import hashlib
import json
import re
import time
from typing import Any, AsyncGenerator, Optional
from models import file_data_markdown

from response_validation import IncompleteResponseError, complete_function_arguments, validate_generation_finish_reason
from json_stream import MAX_JSON_OBJECT_BYTES, JsonObjects as _JsonObjects
from signature_store import SignatureRecord, SignatureState, signature_store
from thought_signatures import (
    attach_signature_extra,
    encode_thought_signature,
    is_thought_part,
    ordinary_part_metadata,
)


def _stable_cookie_tool_call_id(response_id: str, call_index: int, name: str, args: Any) -> str:
    """Generate a deterministic fallback when batchGraphql omits a call id."""
    canonical = json.dumps(args or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(f"{response_id}\0{call_index}\0{name}\0{canonical}".encode()).hexdigest()[:20]
    return f"call_{digest}"


def signature_bytes_from_wire(value: Any) -> Optional[bytes]:
    if isinstance(value, bytes):
        return value or None
    if not isinstance(value, str) or not value:
        return None
    try:
        return base64.b64decode(value, validate=True) or None
    except Exception:
        return value.encode("utf-8")


def apply_wire_signature(data: dict, ordinary_metadata: list, part_order: list) -> Optional[bytes]:
    """把 batchGraphql 的 thoughtSignature 事件并入 ordinary 元数据（流式/非流式共用）。

    已有普通块时签名挂到最后一块；尚无普通块且上游标注 signature_only 时
    补一条零长度签名块；否则丢弃（无承载位置）。返回原始签名字节，
    空签名返回 None 且不做任何修改。
    """
    signature = signature_bytes_from_wire(data.get("value"))
    if not signature:
        return None
    if ordinary_metadata:
        ordinary_metadata[-1]["thought_signature"] = encode_thought_signature(signature)
        if data.get("part_kind") == "image":
            # The media already lives in content; metadata retains its exact
            # slot/signature instead of duplicating the base64 as plain text.
            ordinary_metadata[-1].update(kind="image", text="")
    elif data.get("part_kind") == "signature_only":
        part_order.append({"type": "ordinary", "index": len(ordinary_metadata)})
        ordinary_metadata.append(ordinary_part_metadata("signature_only", "", signature))
    return signature


def openai_tool_call(response_id: str, call_index: int, call: dict, *, include_arguments: bool = True) -> dict:
    name, args = call["name"], call.get("args") or {}
    call_id = call.get("id") or _stable_cookie_tool_call_id(response_id, call_index, name, args)
    signature = signature_bytes_from_wire(call.get("thought_signature"))
    if signature:
        signature_store.put_record(call_id, SignatureRecord(SignatureState.SIGNED, signature))
    elif call_index > 0:
        signature_store.put_unsigned_follower(call_id)
    else:
        signature_store.put_unknown(call_id)
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
    response, max_object_bytes: int = MAX_JSON_OBJECT_BYTES, *, on_text_chunk=None
) -> AsyncGenerator[dict, None]:
    """解析 batchGraphql 的连续对象，忽略数组分隔符及防 XSSI 前缀。

    正则在 C 层跳过普通文本，只在引号、转义符和花括号处更新状态。
    对象边界只扫描一遍，避免对大图片的每个小块重复解析整个未完成对象。
    """
    if isinstance(response, TextSource):
        # 非流式正文已完整，直接使用 C 解码器，省去分块边界扫描。
        for obj in _complete_json_objects(response.text, max_object_bytes):
            yield obj
        return
    parser = _JsonObjects(max_object_bytes)
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
    finally:
        close = getattr(aiter, "aclose", None)
        if close is not None:
            await close()
    parser.finish()


def _complete_json_objects(text: str, limit: int):
    decoder = json.JSONDecoder()
    # 整串 ASCII 标志是 O(1)（CPython 的 str 内部位标志）：纯 ASCII 响应
    # （JSON 结构 + base64，最常见形态）下字符差即字节差，完全零拷贝；
    # 非 ASCII 文本退回逐对象切片 + 精确编码，上限语义不变。
    ascii_text = text.isascii()
    position = 0
    while (start := text.find("{", position)) >= 0:
        try:
            obj, position = decoder.raw_decode(text, start)
        except (json.JSONDecodeError, RecursionError) as exc:
            raise IncompleteResponseError("上游返回无效或不完整的 JSON 对象。") from exc
        span_bytes = (position - start) if ascii_text else len(text[start:position].encode("utf-8"))
        if span_bytes > limit:
            raise IncompleteResponseError("上游单个 JSON 对象超过字节上限。")
        yield obj


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

    def __init__(self, text):
        self.text = text

    async def aiter_text(self):
        yield self.text


class RawSampler:
    """收集原始响应对象的首尾样本，供“无正文/空响应”时输出诊断日志。

    只存原始对象引用（上界 keep 个）；_summ_obj 的完整 json.dumps + 截断
    推迟到 dump() 才做。旧实现在 add() 里对**每个**对象都先序列化一遍、
    随即丢弃（生图响应单个对象可达数 MB，等于在事件循环上做无谓的
    全量序列化再扔掉）。
    """

    def __init__(self, keep: int = 2):
        self.keep = keep
        self._head: list = []
        self._tail: list = []
        self.count = 0

    def add(self, obj: dict):
        self.count += 1
        if len(self._head) < self.keep:
            self._head.append(obj)
        else:
            self._tail.append(obj)
            if len(self._tail) > self.keep:
                self._tail.pop(0)

    def dump(self) -> str:
        if self.count == 0:
            return "（上游未返回任何可解析的 JSON 对象）"
        lines = [f"共 {self.count} 个对象；首尾样本："]
        lines += [f"  ▸ {_summ_obj(o)}" for o in self._head]
        if self._tail:
            lines.append(f"  …")
            lines += [f"  ▸ {_summ_obj(o)}" for o in self._tail]
        return "\n".join(lines)


def _wire_object(value, label, *, allow_null=True):
    if value is None and allow_null:
        return {}
    if not isinstance(value, dict):
        raise IncompleteResponseError(f"上游 {label} 必须是 JSON 对象。")
    return value


def _wire_list(value, label):
    if value is None:
        return []
    if not isinstance(value, list):
        raise IncompleteResponseError(f"上游 {label} 必须是数组。")
    return value


def extract_events(obj: dict, *, preserve_parts: bool = False):
    obj = _wire_object(obj, "响应", allow_null=False)
    # 真值判断而非 "error" in obj：该端点的 Proto JSON 会序列化默认空值
    # （同文件 thoughtSignature/functionCall 的空值过滤同理），若某个块
    # 带出 "error": null / {}，旧实现会把它当真实错误，整个回合以 502
    # "None"/"{}" 终结。真实错误对象恒为真值，此改动对其无影响。
    err = obj.get("error")
    if err:
        yield ("error", err)
        return

    results = _wire_list(obj.get("results"), "results")
    for result in results:
        result = _wire_object(result, "result", allow_null=False)
        errors = _wire_list(result.get("errors"), "errors")
        if errors:
            for err in errors:
                yield ("error", err)
            continue

        data = _wire_object(result.get("data"), "data")
        if not data:
            continue

        # 提示词级拦截（promptFeedback.blockReason）：无候选、直接被挡。
        # 注意：batchGraphql 每个流式块都会带 blockReason=BLOCKED_REASON_UNSPECIFIED
        # （proto 枚举默认值 = 未拦截），必须过滤掉，只透出真实拦截。
        pf = data.get("promptFeedback")
        if isinstance(pf, dict):
            br = str(pf.get("blockReason") or "")
            if br and not br.upper().endswith("UNSPECIFIED"):
                msg = br
                if pf.get("blockReasonMessage"):
                    msg += f"（{pf['blockReasonMessage']}）"
                yield ("blocked", msg)

        candidates = _wire_list(data.get("candidates"), "candidates")
        if len(candidates) > 1:
            raise IncompleteResponseError("Cookie 上游返回多个候选，当前桥接无法无损转换。")
        for candidate in candidates:
            if isinstance(candidate, dict) and candidate.get("index", 0) not in (None, 0):
                raise IncompleteResponseError("Cookie 上游返回非零候选索引，当前桥接无法无损转换。")
            candidate = _wire_object(candidate, "candidate", allow_null=False)
            if not preserve_parts:
                # Validate before yielding calls from the same failing chunk.
                # Gemini-native consumers retain their original finishReason.
                validate_generation_finish_reason(candidate.get("finishReason"))
            content_obj = _wire_object(candidate.get("content"), "content")
            parts = _wire_list(content_obj.get("parts"), "parts")

            for part in parts:
                part = _wire_object(part, "part", allow_null=False)
                text = part.get("text", "")
                if text is not None and not isinstance(text, str):
                    raise IncompleteResponseError("上游 part.text 必须是字符串。")
                native_part = {}
                if text:
                    native_part["text"] = text
                    if is_thought_part(part):
                        native_part["thought"] = True
                if text and not preserve_parts:
                    if is_thought_part(part):
                        yield ("thought", text)
                    else:
                        yield ("text", text)

                # Proto JSON includes empty defaults for functionCall,
                # functionResponse and thoughtSignature on ordinary Parts.  Only
                # a named call and a non-empty signature are semantically real.
                function_call = _wire_object(part.get("functionCall"), "functionCall")
                name = function_call.get("name")
                if name is not None and not isinstance(name, str):
                    raise IncompleteResponseError("上游 functionCall.name 必须是字符串。")
                if name and name.strip():
                    call_id = function_call.get("id")
                    if call_id is not None and not isinstance(call_id, str):
                        raise IncompleteResponseError("上游 functionCall.id 必须是字符串。")
                    args = function_call.get("args")
                    args = _wire_object(args, "functionCall.args")
                    complete_function_arguments(function_call)
                    native_part["functionCall"] = {"name": name, "args": args}
                    if call_id:
                        native_part["functionCall"]["id"] = call_id
                    if not preserve_parts:
                        yield (
                            "function_call",
                            {
                                "name": name,
                                "args": args,
                                **({"id": call_id} if call_id else {}),
                                "thought_signature": part.get("thoughtSignature") or None,
                            },
                        )
                inline_data = _wire_object(part.get("inlineData"), "inlineData")
                if inline_data:
                    mime_type = inline_data.get("mimeType", "")
                    b64 = inline_data.get("data", "")
                    if not isinstance(mime_type, str) or not isinstance(b64, str):
                        raise IncompleteResponseError("上游 inlineData 的 mimeType / data 必须是字符串。")
                    if mime_type and b64:
                        native_part["inlineData"] = {"mimeType": mime_type, "data": b64}
                        if not preserve_parts:
                            image_md = f"![Generated Image](data:{mime_type};base64,{b64})"
                            yield ("image", image_md)

                file_data = _wire_object(part.get("fileData"), "fileData")
                if file_data:
                    uri, mime_type = file_data.get("fileUri", ""), file_data.get("mimeType", "")
                    uri = "" if uri is None else uri
                    mime_type = "" if mime_type is None else mime_type
                    if not isinstance(uri, str) or not isinstance(mime_type, str):
                        raise IncompleteResponseError("上游 fileData 的 fileUri / mimeType 必须是字符串。")
                    if uri:
                        native_part["fileData"] = dict(file_data)
                        if not preserve_parts:
                            kind = "image" if not mime_type or mime_type.startswith("image/") else "text"
                            yield kind, file_data_markdown(uri, mime_type)

                if part.get("thoughtSignature") and not (name and name.strip()) and not preserve_parts:
                    # Signatures describe this Part. Emit them after its media
                    # event, so consumers cannot attach them to preceding text.
                    media = "inlineData" in native_part or ("fileData" in native_part
                        and (not mime_type or mime_type.startswith("image/")))
                    yield "thought_signature", {
                        "value": part["thoughtSignature"],
                        "part_kind": "thought" if is_thought_part(part)
                        else "image" if media else "text" if text else "signature_only",
                    }

                if preserve_parts:
                    signature = part.get("thoughtSignature")
                    if signature:
                        if not isinstance(signature, str):
                            raise IncompleteResponseError("上游 thoughtSignature 必须是字符串。")
                        native_part["thoughtSignature"] = signature
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
        usage = data.get("usageMetadata")
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
        "created": int(time.time()),
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
