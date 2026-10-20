"""Cookie batchGraphql 响应解析、签名还原、诊断与 OpenAI 帧格式。"""

import base64
import hashlib
import json
import re
import time
from typing import Any, AsyncGenerator, Optional

from response_validation import IncompleteResponseError
from json_stream import MAX_JSON_OBJECT_BYTES, JsonObjects as _JsonObjects
from signature_store import SignatureRecord, SignatureState, signature_store
from thought_signatures import is_thought_part, thought_signature_extra


def _stable_cookie_tool_call_id(response_id: str, call_index: int, name: str, args: Any) -> str:
    """Generate deterministic OpenAI ids because batchGraphql functionCall has no id."""
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


def openai_tool_call(response_id: str, call_index: int, call: dict) -> dict:
    name, args = call["name"], call.get("args") or {}
    call_id = _stable_cookie_tool_call_id(response_id, call_index, name, args)
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
        "function": {"name": name, "arguments": json.dumps(args, ensure_ascii=False)},
    }
    extra = thought_signature_extra(signature)
    if extra:
        payload["extra_content"] = extra
    return payload


async def iter_json_objects(
    response, max_object_bytes: int = MAX_JSON_OBJECT_BYTES
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
    async for chunk in response.aiter_text():
        if chunk:
            for obj in parser.feed(chunk):
                yield obj
    parser.finish()


def _complete_json_objects(text: str, limit: int):
    decoder = json.JSONDecoder()
    position = 0
    while (start := text.find("{", position)) >= 0:
        try:
            obj, position = decoder.raw_decode(text, start)
        except json.JSONDecodeError as exc:
            raise IncompleteResponseError("上游返回无效或不完整的 JSON 对象。") from exc
        if len(text[start:position].encode("utf-8")) > limit:
            raise IncompleteResponseError("上游单个 JSON 对象超过字节上限。")
        yield obj


FILTERED_FINISH_REASONS = {"SAFETY", "PROHIBITED_CONTENT", "RECITATION", "BLOCKLIST", "SPII", "IMAGE_SAFETY"}


def map_finish_reason(raw: str | None) -> str:
    """batchGraphql finishReason → OpenAI finish_reason。"""
    r = (raw or "").upper()
    if r == "MAX_TOKENS":
        return "length"
    if r in FILTERED_FINISH_REASONS:
        return "content_filter"
    return "stop"


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


def extract_events(obj: dict):
    # 真值判断而非 "error" in obj：该端点的 Proto JSON 会序列化默认空值
    # （同文件 thoughtSignature/functionCall 的空值过滤同理），若某个块
    # 带出 "error": null / {}，旧实现会把它当真实错误，整个回合以 502
    # "None"/"{}" 终结。真实错误对象恒为真值，此改动对其无影响。
    err = obj.get("error")
    if err:
        yield ("error", err)
        return

    results = obj.get("results", [])
    for result in results:
        if "errors" in result:
            for err in result["errors"]:
                yield ("error", err)
            continue

        data = result.get("data")
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

        candidates = data.get("candidates", [])
        for candidate in candidates:
            content_obj = candidate.get("content") or {}
            parts = content_obj.get("parts") or []

            for part in parts:
                text = part.get("text", "")
                if text:
                    if is_thought_part(part):
                        yield ("thought", text)
                    else:
                        yield ("text", text)

                # Proto JSON includes empty defaults for functionCall,
                # functionResponse and thoughtSignature on ordinary Parts.  Only
                # a named call and a non-empty signature are semantically real.
                function_call = part.get("functionCall")
                if isinstance(function_call, dict) and str(function_call.get("name") or "").strip():
                    args = function_call.get("args")
                    if not isinstance(args, dict):
                        args = {}
                    yield (
                        "function_call",
                        {
                            "name": str(function_call["name"]),
                            "args": args,
                            "thought_signature": part.get("thoughtSignature") or None,
                        },
                    )
                elif part.get("thoughtSignature"):
                    yield (
                        "thought_signature",
                        {
                            "value": part["thoughtSignature"],
                            "part_kind": "thought"
                            if is_thought_part(part)
                            else ("text" if text else "signature_only"),
                        },
                    )

                inline_data = part.get("inlineData")
                if inline_data:
                    mime_type = inline_data.get("mimeType", "")
                    b64 = inline_data.get("data", "")
                    if mime_type and b64:
                        image_md = f"![Generated Image](data:{mime_type};base64,{b64})"
                        yield ("image", image_md)

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
