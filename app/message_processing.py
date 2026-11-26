"""OpenAI 与 Gemini 消息、内容和响应格式转换。"""

import base64
import html
import json
import math
import re
import time
import uuid
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple

import config as app_config
from google.genai import types
from image_history import ManagedImageHistory
from image_processing import (
    decode_data_url_image,
    fetch_remote_image,
    optimize_image_bytes,
)
from tool_arguments import parse_tool_arguments
from tool_history import current_signature_turn_start, first_wire_call_index, gemini_history_with_ids, history_call_signature
from models import OpenAIMessage, file_data_markdown, message_text, normalize_content_part
from response_validation import (
    complete_function_arguments,
    map_finish_reason_to_openai,
    require_unique_tool_call_ids,
    sdk_function_call,
    sdk_media_payload,
)
from schema_validation import SchemaValidationError
from runtime_state import app_state
from thought_signatures import (
    replay_tool_call_id,
    attach_signature_extra,
    PendingCallSignatures,
    publish_tool_call_signatures,
    stage_tool_call_id,
    google_metadata,
    merge_replayed_parts,
    ordinary_part_metadata,
    ordinary_parts_from_extra,
    requires_signature,
    signature_bytes,
    standalone_message_signature,
    sdk_thought_signature_field,
    thought_signature_extra,
)
from usage import map_usage

if TYPE_CHECKING:
    from signature_replay import SDKSignatureReplay

SUPPORTED_ROLES = ["user", "model", "function"]

# 超大 content 的 SSE 分帧阈值（4K 生图的 base64 data URL 单张可达 13~40MB）。
# 单帧发射时 json.dumps + f"data:..." + ASGI encode 会在同一瞬间再造三份
# 全量拷贝，叠加转换链后 512MB 容器（Render 免费/入门档）实测被 OOMKill
# （进程级 SIGKILL，无任何 Python 异常，表现为"收到首个上游分片后服务
# 直接崩溃重启"）。超过阈值的 content 切成 256KiB 增量帧：客户端按
# OpenAI 语义拼接 delta 后与单帧输出逐字节一致，仅中转帧体积更小。
GIANT_CONTENT_THRESHOLD = 1 << 20  # 1 MiB：正常文本（含长思考）永不触发
GIANT_CONTENT_SLICE = 1 << 18      # 256 KiB：单帧 SSE 事件的安全体积


def iter_giant_content(
    text: str,
    *,
    threshold: int = GIANT_CONTENT_THRESHOLD,
    size: int = GIANT_CONTENT_SLICE,
) -> Iterator[str]:
    """惰性版 slice_giant_content：按需产出切片，不物化整张切片表。

    发射阶段原实现（list 版）在原文仍被 openai_response_dict 引用时又
    造一份全量拷贝（切片表 = 原文等大），4K 生图在 512MB 容器上发射
    阶段恒驻 2×content。生成器版每帧只在 yield 瞬间存在一个 256KiB
    切片，发射阶段峰值从 2× 降到 1×+256KiB。拼接语义与 list 版逐
    字节一致（见 slice_giant_content）。
    """
    if len(text) <= threshold:
        yield text
        return
    for i in range(0, len(text), size):
        yield text[i : i + size]


def slice_giant_content(
    text: str,
    *,
    threshold: int = GIANT_CONTENT_THRESHOLD,
    size: int = GIANT_CONTENT_SLICE,
) -> list[str]:
    """超大 content（生图 base64）→ 多帧切片，拼接结果与原文逐字节一致。

    小于等于阈值时原样单元素返回（保持"整图单帧"的既有假流式 UX——
    1MB 以内的图片/markdown 帧一次到达，前端无中间渲染闪烁）。切片
    总量与原文等大（一份拷贝，与旧单帧的 json.dumps 拷贝同量级），
    循环结束即释放。
    """
    return list(iter_giant_content(text, threshold=threshold, size=size))


def logprobs_payload_from_candidate(candidate: Any) -> Optional[Dict[str, Any]]:
    """把 SDK Candidate 的 logprobs_result 转成 OpenAI choices[].logprobs 结构。

    旧实现写的是 hasattr(candidate, "logprobs")，但 google-genai 的字段名
    是 logprobs_result——该分支恒为 False，客户端请求 logprobs:true 时
    上游真的返回了概率数据却在转换层被整体丢弃（2026-10 修复）。
    Cookie 通道不下发 logprobs 参数，此函数仅服务 Express/SDK 路径。
    """
    result = getattr(candidate, "logprobs_result", None)
    if result is None:
        return None
    chosen = list(getattr(result, "chosen_candidates", None) or [])
    if not chosen:
        return None
    tops = list(getattr(result, "top_candidates", None) or [])

    def _entry(item: Any) -> Optional[Dict[str, Any]]:
        token = getattr(item, "token", None)
        score = getattr(item, "log_probability", None)
        if not isinstance(token, str) or isinstance(score, bool) or not isinstance(score, (int, float)):
            return None
        try:
            score = float(score)
            if not math.isfinite(score):
                return None
            return {"token": token, "logprob": score, "bytes": list(token.encode("utf-8"))}
        except (UnicodeError, OverflowError):
            return None

    content = []
    for i, item in enumerate(chosen):
        entry = _entry(item)
        if entry is None:
            # Dropping a chosen position would shift every later score; the
            # whole optional logprobs block is then unrepresentable.
            return None
        alternatives = (getattr(tops[i], "candidates", None) or []) if i < len(tops) else ()
        entry["top_logprobs"] = [top for option in alternatives if (top := _entry(option)) is not None]
        content.append(entry)
    return {"content": content}


# 裸 data: URL（非 markdown 包裹）的统一解析正则：RFC 2045 允许 base64 折行，
# 必须 DOTALL；模式尾部是贪婪 (.+)，match 与 fullmatch 等价（2026-11 审查
# 收敛：此前三处各写一份同构 pattern）。
_DATA_URL_RE = re.compile(r"data:([^;]+);base64,(.+)", re.DOTALL)


def _extract_markdown_images_to_parts(text: str) -> Tuple[List[types.Part], str]:
    parts = []
    remaining_text = text
    # 先按 Markdown 边界取出 data URL，再由有界解码器校验编码；只匹配
    # Base64 字符会漏掉折行和坏编码，令整段图片数据残留成提示词。
    pattern = r"!\[[^\]]*\]\(data:(image/[a-zA-Z0-9+.-]+);base64,([^)]*)\)"
    matches = list(re.finditer(pattern, text))

    if matches:
        # 一次重组替代逐图「前缀+后缀」重建：多图场景下旧写法每张图都
        # 复制一遍全量剩余文本（O(n²)），数 MB 的 base64 历史会被放大成
        # 数十 MB 的 memcpy。
        kept = []
        cursor = 0
        for match in matches:
            try:
                mime_type = match.group(1)
                b64_data = match.group(2)
                raw_bytes = decode_data_url_image(b64_data)
                opt_bytes, opt_mime = optimize_image_bytes(raw_bytes, mime_type)
                parts.append(types.Part.from_bytes(data=opt_bytes, mime_type=opt_mime))
                kept.append(text[cursor : match.start()])
                cursor = match.end()
            except Exception as e:
                print(f"⚠️ [图片处理] 提取 Markdown 图片失败，已跳过该图片：{e}")
                # 失败也必须推进游标：旧实现只打日志不推进，整段 data URL
                # （数 MB base64）会残留在文本里原样发给上游——日志说"已
                # 跳过"但实际没跳过，25MB 图变成 ~33MB 提示词（2026-10 修复）。
                kept.append(text[cursor : match.start()])
                kept.append("[图片无法处理，已省略]")
                cursor = match.end()
        # 注：不可 reverse。旧实现（逐图倒序拼接防下标失效）需要 reverse
        # 恢复文档顺序；2026-10 改为正序游标重组后 parts 本就按文档序追加，
        # 残留的 reverse 会把多图消息整体倒序发给 Gemini（多图编辑时第一张
        # 图是编辑目标，顺序错乱直接改错图）。
        # 无论提取成功还是失败，都应用已重组的文本，避免全部失败时把
        # 原始 Base64 当成提示词发送。保留周围空白，代码缩进和预设排版
        # 不应因为同一消息带了图片而改变。
        kept.append(text[cursor:])
        remaining_text = "".join(kept)

    return parts, remaining_text


def _finite_result_number(token: str) -> float:
    """Tool output remains text if a numeric token cannot travel as a finite float."""
    number = float(token)
    if not math.isfinite(number):
        raise ValueError("non-finite tool result number")
    return number


def _text_media_segments(text: str, history: ManagedImageHistory | None = None, *,
                      parse_inline: bool = True) -> Iterator[str | types.Part]:
    """Keep canonical local references in position; leave legacy inline handling intact."""
    segments = history.segments(text) if history else ((text, None),)
    for fragment, name in segments:
        if name is not None:
            raw, mime = history.read(name)
            yield types.Part.from_bytes(data=raw, mime_type=mime)
        elif fragment and parse_inline:
            images, visible = _extract_markdown_images_to_parts(fragment)
            if visible:
                yield visible
            yield from images
        elif fragment:
            yield fragment


def _text_media_parts(text: str, history: ManagedImageHistory | None = None, *,
                      parse_inline: bool = True) -> Iterator[types.Part]:
    for item in _text_media_segments(text, history, parse_inline=parse_inline):
        yield types.Part.from_text(text=item) if isinstance(item, str) else item


def _thought_media_parts(text: str, history: ManagedImageHistory | None) -> Iterator[types.Part]:
    for part in _text_media_parts(text, history, parse_inline=False):
        part.thought = True
        yield part


def _coerce_tool_response(content: Any) -> Dict[str, Any]:
    """把 OpenAI 工具结果安全地转成 function_response 需要的对象。

    修复：旧实现用 `isinstance(str) and (...) or (...)` 的错误优先级，
    当 content 为 list 时会对其调用 .strip() 抛 AttributeError。
    2026-10：dict 与 str-JSON 形态对齐——旧实现 dict {"temp":20} 被二次
    序列化成 {"result":'{"temp": 20}'}，而等价的 str '{"temp":20}' 却得到
    结构化 {"temp":20}，同一份数据两种输入形状深生出不同的模型可见形状。
    """
    if content is None:
        return {"result": ""}
    if isinstance(content, dict):
        # 已是对象：直接作为 response（与 str-JSON 路径解析后的形状一致）。
        return content
    if not isinstance(content, str):
        # content 可能是 OpenAI 的分段 list 等结构：数组包 result（与
        # str '[1,2]' 的解析路径同形），其余无法结构化的再退化成文本。
        if isinstance(content, list):
            return {"result": [normalize_content_part(part) for part in content]}
        try:
            return {"result": json.dumps(content, ensure_ascii=False)}
        except Exception:
            return {"result": str(content)}
    s = content.strip()
    if (s.startswith("{") and s.endswith("}")) or (
        s.startswith("[") and s.endswith("]")
    ):
        try:
            # Python accepts NaN/Infinity and overflows ordinary tokens such as
            # 1e999. Keep the original text if either would create an invalid wire number.
            parsed = json.loads(s, parse_float=_finite_result_number, parse_constant=_finite_result_number)
            # function_response 的 response 需要是对象；数组则包一层
            return parsed if isinstance(parsed, dict) else {"result": parsed}
        except (ValueError, RecursionError):
            return {"result": content}
    return {"result": content}


def tool_response_with_media(content: Any, model_name: str = "", *, error_content: bool | None = None) -> tuple[dict, list]:
    """Replace tool image blocks with references to bounded native image blobs.

    Both Anthropic image blocks and OpenAI image_url blocks are accepted.
    Error-result wrappers are retained, and ordinary JSON results are untouched.
    Gemini 3+ supports these blobs inside FunctionResponse.parts.
    """
    response = _coerce_tool_response(content)
    # In Gemini, top-level 'error'/'output' are execution status/value fields.
    # A successful Claude tool may return either as ordinary application data.
    # Preserve the complete value (including siblings); unmarked OpenAI input
    # can already be a native envelope and must not be reinterpreted.
    if error_content is False and ("error" in response or "output" in response):
        response = {"output": response}
    media = []
    total_bytes = 0

    def convert(blocks: list):
        nonlocal total_bytes
        converted = []
        for block in blocks:
            block = normalize_content_part(block)
            if not isinstance(block, dict) or block.get("type") not in ("image", "image_url"):
                converted.append(block)
                continue
            version = re.search(r"gemini-(\d+)", model_name, re.I)
            if version and int(version[1]) < 3:
                raise SchemaValidationError("工具图片结果需要 Gemini 3 或更新模型。")
            if len(media) >= 32:
                raise SchemaValidationError("单条工具结果最多包含 32 张图片。")
            if block["type"] == "image":
                source = block.get("source") or {}
                if not isinstance(source, dict):
                    raise SchemaValidationError("工具图片 source 必须是对象。")
                if source.get("type") == "base64":
                    url = f"data:{source.get('media_type', '')};base64,{source.get('data', '')}"
                elif source.get("type") == "url":
                    url = source.get("url")
                else:
                    raise SchemaValidationError("工具图片 source.type 必须是 base64 或 url。")
            else:
                image = block.get("image_url")
                url = image.get("url") if isinstance(image, dict) else image
            if not isinstance(url, str):
                raise SchemaValidationError("工具图片缺少有效地址。")
            try:
                if url.startswith("data:"):
                    match = _DATA_URL_RE.fullmatch(url)
                    if not match:
                        raise ValueError("无效的图片 data URL。")
                    mime, encoded = match.groups()
                    raw = decode_data_url_image(encoded)
                elif url.startswith(("http://", "https://")):
                    fetched = fetch_remote_image(url)
                    if fetched is None:
                        raise ValueError("工具图片无法下载。")
                    raw, mime = fetched
                else:
                    raise ValueError("工具图片仅支持 data URL 或 HTTP(S) 地址。")
                raw, mime = optimize_image_bytes(raw, mime)
                if mime not in ("image/png", "image/jpeg", "image/webp"):
                    raise ValueError("工具图片仅支持 PNG、JPEG 或 WebP。")
            except (ValueError, TypeError) as exc:
                raise SchemaValidationError(f"工具图片无法处理：{exc}") from None
            total_bytes += len(raw)
            if total_bytes > 32 * 1024 * 1024:
                raise SchemaValidationError("单条工具结果图片总量超过 32 MiB 上限。")
            name = f"tool-image-{len(media) + 1}"
            media.append((raw, mime, name))
            converted.append({"$ref": name})
        return converted

    # Only a real content-block list or an explicitly marked Claude error
    # wrapper is multimodal. A JSON string can contain arbitrary application
    # records with type=image; parsing it must never trigger image IO.
    response = dict(response)
    keys = ("result",) if isinstance(content, list) else ("error",) if error_content else ()
    for key in keys:
        if isinstance(response.get(key), list):
            response[key] = convert(response[key])
    return response, media


def anthropic_tool_result_is_error(message: OpenAIMessage) -> bool | None:
    metadata = (message.extra_content or {}).get("anthropic")
    flag = metadata.get("tool_result_is_error") if isinstance(metadata, dict) else None
    return flag if isinstance(flag, bool) else None


def tool_observation_json(response: dict, media: list, error_content: bool | None) -> str | None:
    """Text compatibility must also keep image references and wrapped domain objects."""
    if media or (error_content is False and "output" in response):
        return json.dumps(response, ensure_ascii=False)
    return None


def _tool_result_parts(
    message: OpenAIMessage, tool_name_by_id: dict, model_name: str = "", *, literal_ids: frozenset[str] = frozenset()
) -> list[types.Part]:
    """Restore tool results as user Parts; function responses never carry signatures."""
    parts = []
    tool_call_id_str = message.tool_call_id or ""
    real_tool_id = replay_tool_call_id(tool_call_id_str, literal_ids)
    function_name = (
        message.name
        or tool_name_by_id.get(tool_call_id_str)
        or tool_name_by_id.get(real_tool_id)
    )
    error_content = anthropic_tool_result_is_error(message)
    tool_output_data, media = tool_response_with_media(message.content, model_name, error_content=error_content)

    if not function_name:
        # 既没有显式 name，也无法从前面的 assistant.tool_calls 关联时才降级。
        # content 可能是 str / 分段 list / dict：统一归一成文本，避免
        # list 直接被 f-string 嵌成 Python repr。
        fallback_text = tool_observation_json(tool_output_data, media, error_content) or message_text(message.content)
        if (
            not fallback_text
            and message.content is not None
            and not isinstance(message.content, str)
        ):
            try:
                fallback_text = json.dumps(message.content, ensure_ascii=False)
            except Exception:
                fallback_text = str(message.content)
        mock_text = f"[System Observation - Tool Result]:\n{fallback_text}"
        parts.append(types.Part.from_text(text=mock_text))
        parts.extend(types.Part.from_bytes(data=raw, mime_type=mime) for raw, mime, _name in media)
    else:
        # FunctionResponse is always a Gemini ``user`` Content and never
        # carries a thought signature. Signatures belong on model Parts.
        func_resp_kwargs = {"name": function_name, "response": tool_output_data}
        if media:
            func_resp_kwargs["parts"] = [types.FunctionResponsePart(
                inline_data=types.FunctionResponseBlob(data=raw, mime_type=mime, display_name=name)
            ) for raw, mime, name in media]
        if real_tool_id:
            func_resp_kwargs["id"] = real_tool_id

        try:
            resp_part = types.Part(
                function_response=types.FunctionResponse(**func_resp_kwargs)
            )
        except Exception as e:
            if media:
                raise SchemaValidationError("当前 SDK 无法构造工具图片结果。") from e
            print(f"⚠️ [工具调用] 构造 FunctionResponse 失败，将回退为基础形式：{e}")
            resp_part = types.Part.from_function_response(
                name=function_name, response=tool_output_data
            )

        parts.append(resp_part)
    return parts


def _assistant_tool_parts(
    message: OpenAIMessage, require_sig: bool, history: ManagedImageHistory | None = None, *,
    literal_ids: frozenset[str] = frozenset(), signature_replay: "SDKSignatureReplay | None" = None,
) -> list[types.Part]:
    """Restore parallel calls, their signatures and the original ordinary-part order."""
    parts = []
    tool_parts = []
    first = first_wire_call_index(message)
    for tool_index, tool_call in enumerate(message.tool_calls):
        function_call_data = tool_call.get("function", {})
        function_name = function_call_data.get("name", "unknown")
        arguments_str = function_call_data.get("arguments", "{}")
        parsed_arguments = parse_tool_arguments(arguments_str, label="tool_call.function.arguments")
        deferred = signature_replay is not None and signature_replay.eligible(message, tool_index)
        real_tool_id, thought_sig_bytes = history_call_signature(
            message, tool_index, require_sig, literal_ids, first=tool_index == first, defer_recovery=deferred)

        fc_kwargs = {"name": function_name, "args": parsed_arguments}
        if real_tool_id:
            fc_kwargs["id"] = real_tool_id

        try:
            part_kwargs = {"function_call": types.FunctionCall(**fc_kwargs)}
            if thought_sig_bytes:
                # 哨兵必须以 str 字面量出站：SDK 会把 bytes 哨兵 base64 编码，
                # 破坏官方要求的 skip_thought_signature_validator 字面量
                #（见 thought_signatures.sdk_thought_signature_field）。
                part_kwargs["thought_signature"] = sdk_thought_signature_field(thought_sig_bytes)
            fc_part = types.Part(**part_kwargs)
        except Exception as e:
            print(f"⚠️ [工具调用] 构造 FunctionCall 失败，将回退为基础形式：{e}")
            fc_part = types.Part.from_function_call(
                name=function_name, args=parsed_arguments
            )
        else:
            if deferred and not thought_sig_bytes:
                signature_replay.watch(fc_part, message, tool_index, require_sig, literal_ids, tool_index == first)
        tool_parts.append(fc_part)

    ordinary_parts = _restored_ordinary_parts(message, history)
    exact_history = ordinary_parts is not None
    if ordinary_parts is None and message.has_assistant_refusal():
        # The new ordered refusal form may interleave ordinary text or images.
        ordinary_parts = _ordinary_message_parts(message, history)[1]
    if ordinary_parts is None:
        ordinary_parts = []
        reasoning = getattr(message, "reasoning_content", None)
        if reasoning:
            ordinary_parts.extend(_thought_media_parts(reasoning, history))
        # content 为分段 list 时旧实现经 message_text 只提取文本，
        # image_url 分段被静默丢弃：多轮修图 agent 把上一轮 assistant
        # 的图片以分段形式回传（OpenAI 协议合法形态）时，带工具调用的
        # 回合里图片无声消失，模型"看不到图"且无日志可查。与无
        # tool_calls 分支对齐：文本仍按 "".join 归一（语义不变），
        # image_url 分段逐个摄取（2026-10 修复）。
        # 分段遍历骨架与普通消息路径、wire 路径共用（见
        # _iter_content_segments）；本路径的差异只在于文本先聚合、
        # markdown 图片在聚合文本上一次性抽取（图片后置）。
        if isinstance(message.content, list):
            text_pieces: list = []
            image_parts_collected: list = []
            _iter_content_segments(
                message.content,
                text_pieces.append,
                lambda url: image_parts_collected.append(_sdk_image_part(url, history)),
            )
            content_text = "".join(text_pieces)
        else:
            content_text = message.content
            image_parts_collected = []
        if isinstance(content_text, str) and content_text:
            ordinary_parts.extend(_text_media_parts(content_text, history))
        ordinary_parts.extend(image_parts_collected)

        message_sig, signature_kind = standalone_message_signature(message)
        if message_sig:
            _attach_message_signature(ordinary_parts, message_sig, signature_kind)

    google_extra = google_metadata(message)
    # QUA-P2-3：part_order 交错重组收敛到 thought_signatures.merge_by_order
    #（剩余块兕底顺序保留 SDK 通道既有行为：普通块在前）。
    merged = merge_replayed_parts(google_extra, tool_parts, ordinary_parts,
                                  exact_history=exact_history, remainder_calls_first=False)
    if merged is not None:
        parts.extend(merged)
    else:
        # Historical behavior emitted calls first. Preserve it unless a
        # response from this bridge supplies exact part_order metadata.
        parts.extend(tool_parts)
        parts.extend(ordinary_parts)
    return parts


def _restored_ordinary_parts(message: OpenAIMessage, history: ManagedImageHistory | None = None):
    def image(block: dict) -> types.Part | None:
        if block["type"] == "image_url":
            return _sdk_image_part(_image_part_url(block), history)
        return next((p for p in _text_media_parts(block["text"], history)
                     if p.inline_data is not None or p.file_data is not None), None)

    return ordinary_parts_from_extra(message, image, history)


def _ordinary_message_parts(message: OpenAIMessage, history: ManagedImageHistory | None = None) -> tuple[str, list[types.Part]]:
    """Convert text/images/thoughts and attach an ordinary-part signature once."""
    content = message.replay_content()
    role = message.role
    parts = []
    message_sig, signature_kind = standalone_message_signature(message)
    reasoning = message.reasoning_content if role in ("assistant", "model") else None
    if content is None and not reasoning and not message_sig and "ordinary_parts" not in google_metadata(message):
        return role, []

    current_gemini_role = role
    if current_gemini_role in ("assistant", "model"):
        current_gemini_role = "model"
    if current_gemini_role not in SUPPORTED_ROLES:
        current_gemini_role = "user"

    exact_ordinary_parts = _restored_ordinary_parts(message, history)
    if exact_ordinary_parts is not None:
        parts.extend(exact_ordinary_parts)

    if reasoning and exact_ordinary_parts is None:
        parts.extend(_thought_media_parts(reasoning, history))

    if exact_ordinary_parts is None and isinstance(content, str):
        parts.extend(_text_media_parts(content, history))

    elif exact_ordinary_parts is None and isinstance(content, list):
        def _emit_text(text_content: str):
            # F-1：pydantic 会把标准 part 解析成 ContentPartText / ContentPartImage 实例，
            # 归一成 dict 后只保留一条处理路径（原先 dict 与实例两套分支已出现行为分歧：
            # 实例分支直接 from_text，跳过了 markdown 内联图片抽取）。
            parts.extend(_text_media_parts(text_content, history))

        _iter_content_segments(
            content,
            _emit_text,
            lambda url: parts.append(_sdk_image_part(url, history)),
        )

    if message_sig and exact_ordinary_parts is None:
        _attach_message_signature(parts, message_sig, signature_kind)
    return current_gemini_role, parts


def _merge_gemini_turns(
    raw_gemini_messages: list[types.Content],
) -> list[types.Content]:
    """Merge compatible consecutive turns without merging tool results into a new user turn."""

    def kind(content: types.Content) -> str:
        parts = content.parts or ()
        if parts and all(getattr(p, "function_response", None) is not None for p in parts):
            return "results"
        if all(getattr(p, "function_call", None) is None
               and getattr(p, "function_response", None) is None
               and getattr(p, "inline_data", None) is None
               and getattr(p, "text", None) is not None for p in parts):
            return "text"
        return "other"

    merged_messages = []
    previous_kind = None
    for msg in raw_gemini_messages:
        if not merged_messages or merged_messages[-1].role != msg.role:
            merged_messages.append(msg)
            previous_kind = None  # Alternating roles never need classification.
            continue
        previous = merged_messages[-1]
        if previous_kind is None:
            previous_kind = kind(previous)
        current_kind = kind(msg)
        if (previous_kind == "results") != (current_kind == "results"):
            # A fresh user turn after parallel results is a new Content for
            # Gemini's signature validator, despite having the same role.
            merged_messages.append(msg)
            previous_kind = current_kind
            continue
        if previous_kind == current_kind == "text":
            previous.parts.append(types.Part.from_text(text="\n\n"))
        elif previous_kind != current_kind:
            previous_kind = "other"  # No separator anywhere after a non-text part.
        previous.parts.extend(msg.parts)

    if not merged_messages:
        merged_messages.append(
            types.Content(role="user", parts=[types.Part.from_text(text="继续")])
        )

    return merged_messages


def create_gemini_prompt(
    messages: List[OpenAIMessage], model_name: str = "", *, signature_replay: "SDKSignatureReplay | None" = None,
) -> List[types.Content]:
    """Convert OpenAI messages to Gemini contents, preserving tool/signature boundaries."""
    print("🔄 [消息转换] 正在将 OpenAI 格式消息转换为 Gemini contents。")
    history = ManagedImageHistory.for_request()
    require_sig = requires_signature(model_name)
    tool_name_by_id = {}
    contents = []
    turn_start = current_signature_turn_start(messages) if require_sig else 0
    for i, (message, literal_ids) in enumerate(gemini_history_with_ids(messages)):
        if message.role in ("system", "developer"):
            continue  # These instructions are sent in system_instruction.
        if message.role in ("tool", "function"):
            role, parts = "user", _tool_result_parts(message, tool_name_by_id, model_name, literal_ids=literal_ids)
        elif message.role in ("assistant", "model") and message.tool_calls:
            for call in message.tool_calls:
                call_id = str(call.get("id") or "")
                call_name = str((call.get("function") or {}).get("name") or "")
                if call_id and call_name:
                    tool_name_by_id[call_id] = call_name
                    tool_name_by_id[replay_tool_call_id(call_id, literal_ids)] = call_name
            role, parts = "model", _assistant_tool_parts(
                message, require_sig and i >= turn_start, history, literal_ids=literal_ids,
                signature_replay=signature_replay)
        else:
            role, parts = _ordinary_message_parts(message, history if message.role in ("assistant", "model") else None)
        if parts:
            contents.append(types.Content(role=role, parts=parts))
    return _merge_gemini_turns(contents)


def _rindex_by_identity(seq: list, item: Any) -> int:
    """从后往前按**对象身份**查找下标。

    list.index 按 == 匹配：parts 里存在等值重复 part 时会命中第一个，
    而思考签名的 target 是从后往前（reversed）选出的——用 index 反查
    会把签名挂到错误的 part 上，破坏 Gemini 3 “签名必须在正确 part”
    的校验。找不到时抛 ValueError（防御性，正常路径不可能发生）。
    """
    for i in range(len(seq) - 1, -1, -1):
        if seq[i] is item:
            return i
    raise ValueError("签名目标 part 不在列表中（不应发生）")


def _attach_message_signature(parts: list, message_sig: Optional[str], signature_kind: str) -> None:
    """把消息级思考签名挂到目标 part（带工具调用与普通两条恢复路径共用）。

    目标选择（与浏览器端语义对齐）：thought 签名挂最后一个思考 part；
    text 签名挂最后一个非思考文本 part；都找不到时退回最后一个 part；
    仅当 signature_only 才不退回（无 part 可挂时补一个空签名 part）。
    挂载位置按对象身份反查（见 _rindex_by_identity 注释）。
    （2026-11 审查收敛：此前 _assistant_tool_parts 与 _ordinary_message_parts
    各持一份同构实现，注释互指“同上”。）
    """
    target = None
    if signature_kind == "thought":
        target = next(
            (p for p in reversed(parts) if getattr(p, "thought", None)), None
        )
    elif signature_kind == "text":
        target = next(
            (
                p
                for p in reversed(parts)
                if getattr(p, "text", None) is not None
                and not getattr(p, "thought", None)
            ),
            None,
        )
    if target is None and signature_kind != "signature_only":
        target = parts[-1] if parts else None
    if target is None:
        parts.append(types.Part(text="", thought_signature=sdk_thought_signature_field(message_sig)))
        return
    parts[_rindex_by_identity(parts, target)] = target.model_copy(
        update={"thought_signature": sdk_thought_signature_field(message_sig)}
    )


def _iter_content_segments(content_list: list, on_text: Callable, on_image_url: Callable) -> None:
    """遍历 OpenAI 分段 content，把 text / image_url 分段派发给回调。

    三条转换管线（工具回合恢复 / 普通消息 / Cookie wire）共用的遍历骨架
    （2026-11 审查收敛：此前三处各持一份同构实现，文本空值兑底、实例形态
    归一这些口径靠注释互相指认保持同步，漏改即分叉）。
      - 裸字符串分段按文本处理（仅 wire 路径的历史输入会出现，另两条
        管线的 content 经 pydantic 校验不会产生裸 str）；
      - 归一后非 dict 的段落取 text 属性；
      - 文本分段统一 or "" 兑底（{"type":"text","text":null} 不把
        None 传给下游 markdown 抽取）；
      - image_url 分段经 _image_part_url 归一（三种历史形态）。
    on_text(text) / on_image_url(url) 自行决定聚合时机与目标形态。
    """
    for part_item in content_list:
        if isinstance(part_item, str):
            on_text(part_item)
            continue
        part_item = normalize_content_part(part_item)
        if not isinstance(part_item, dict):
            text_attr = getattr(part_item, "text", None)
            if isinstance(text_attr, str):
                on_text(text_attr)
            continue
        if part_item.get("type") == "text":
            on_text(part_item.get("text") or "")
        elif part_item.get("type") == "image_url":
            on_image_url(_image_part_url(part_item))


def _image_part_url(part_item: dict) -> str:
    """从归一后的 image_url 分段里取 URL（三种历史形态：字符串 / dict / 实例）。

    （2026-11 审查收敛：两条消息恢复管线各自复制同一三分支。）
    """
    img_url_data = part_item.get("image_url") or {}
    if isinstance(img_url_data, str):
        return img_url_data
    if isinstance(img_url_data, dict):
        return img_url_data.get("url", "")
    return getattr(img_url_data, "url", "") or ""


def _required_image_bytes(image_url: str, history: ManagedImageHistory | None = None) -> tuple[bytes, str]:
    """Resolve one explicit attachment, never degrade it to a text-only prompt.

    Client-facing errors exclude source URLs, inline payloads and decoder
    details. Only expected data/size rejections are translated here; internal
    media-worker or Part-construction failures are not malformed input.
    """
    source_help = "image_url 需要非空的 HTTP(S) 图片地址，或 data:image/...;base64,... 图片 URL。"
    if not isinstance(image_url, str):
        raise SchemaValidationError(source_help)
    if history and (name := history.name_from_url(image_url)) is not None:
        return history.read(name)
    if image_url.startswith("data:"):
        match = _DATA_URL_RE.fullmatch(image_url)
        if not match or not re.fullmatch(r"image/[a-z0-9.+-]+", match[1], re.I | re.ASCII):
            raise SchemaValidationError(source_help)
        mime = match[1]
        try:
            raw = decode_data_url_image(match[2])
        except ValueError:
            raise SchemaValidationError("image_url 的 data: 图片无法解码。请使用有效且符合大小限制的 Base64 图片。") from None
    elif image_url.startswith(("http://", "https://")):
        fetched = fetch_remote_image(image_url)
        if not fetched or not fetched[0]:
            raise SchemaValidationError(
                "无法读取 image_url 中的图片。请提供服务可访问的图片地址，或使用 data: 图片 URL。"
            )
        raw, mime = fetched
    else:
        raise SchemaValidationError(source_help)
    try:
        return optimize_image_bytes(raw, mime)
    except ValueError:
        raise SchemaValidationError("image_url 的图片无法处理。请检查图片格式、大小及像素限制后重新提供地址或 data: 图片。") from None


def _sdk_image_part(image_url: str, history: ManagedImageHistory | None = None) -> types.Part:
    raw, mime = _required_image_bytes(image_url, history)
    return types.Part.from_bytes(data=raw, mime_type=mime)


def _wire_image_part(image_url: str, history: ManagedImageHistory | None = None) -> Dict[str, Any]:
    raw, mime = _required_image_bytes(image_url, history)
    return {"inlineData": {"mimeType": mime, "data": base64.b64encode(raw).decode("ascii")}}


def openai_content_to_wire_parts(content: Any, history: ManagedImageHistory | None = None, *,
                                 thought: bool = False) -> List[Dict[str, Any]]:
    """OpenAI 的 message.content → batchGraphql 的 parts 列表（P1-2）。

    与 Express 通道行为对齐：
      - 解析正文里的 markdown data-URL 图片（多轮修图时上一张图不会再被当成巨大文本发出）
      - data: 与 http(s): 两种 image_url 都支持
      - 统一走 optimize_image_bytes 做输入图压缩（控制台开关对两条通道都生效）
    """
    parts: List[Dict[str, Any]] = []
    if content is None:
        return parts

    def _add_text_with_inline_images(text: str):
        if not text:
            return
        for item in _text_media_segments(text, history, parse_inline=not thought):
            if isinstance(item, str):
                wire = {"text": item}
            elif (blob := item.inline_data) is not None and blob.data:
                wire = {"inlineData": {"mimeType": blob.mime_type, "data": base64.b64encode(blob.data).decode("ascii")}}
            else:
                continue
            if thought:
                wire["thought"] = True
            parts.append(wire)

    if isinstance(content, str):
        _add_text_with_inline_images(content)
        return parts

    if isinstance(content, list):
        _iter_content_segments(
            content,
            _add_text_with_inline_images,
            lambda url: parts.append(_wire_image_part(url, history)),
        )
    return parts


def _rating_fields(r: Any) -> Tuple[str, str, Optional[float], Optional[float]]:
    """把一条安全评分归一成 (分类, 概率档, 概率分, 严重度分)。

    Express 通道给的是 SDK 对象（属性 + 枚举），Cookie 通道给的是 batchGraphql
    的 camelCase 字典，两边共用同一个渲染器。
    """

    def _enum_name(v: Any):
        return getattr(v, "name", None) or (str(v) if v is not None else "")

    if isinstance(r, dict):
        cat = str(r.get("category") or "")
        prob = str(r.get("probability") or "")
        ps, ss = r.get("probabilityScore"), r.get("severityScore")
    else:
        cat = _enum_name(getattr(r, "category", None))
        prob = _enum_name(getattr(r, "probability", None))
        ps, ss = (
            getattr(r, "probability_score", None),
            getattr(r, "severity_score", None),
        )
    cat = cat.replace("HARM_CATEGORY_", "").replace("_", " ").title()
    return (
        cat,
        prob,
        (ps if isinstance(ps, (int, float)) else None),
        (ss if isinstance(ss, (int, float)) else None),
    )


def _create_safety_ratings_html(safety_ratings: list) -> str:
    if not safety_ratings:
        return ""
    # 上游对部分分类只给 probability 不给 probability_score（实测 JAILBREAK 常为 None），
    # 直接 max(key=probability_score) 会拿 None 和 float 比较 → TypeError，
    # 整个请求变成 500。缺分数的按 -1 排，永远不会被选成“最高分”。
    normalized = [_rating_fields(r) for r in safety_ratings]
    highest = max(normalized, key=lambda t: t[2] if t[2] is not None else -1.0)
    highest_score = highest[2] if highest[2] is not None else -1.0

    if highest_score < 0:
        color = "#888"  # 一条分数都没有
    elif highest_score <= 0.33:
        color = "#0f8"
    elif highest_score <= 0.66:
        color = "yellow"
    else:
        color = "#bf555d"

    def _fmt(cat: str, prob: Optional[float], ps: Any, ss: Any):
        ps_s = f"{ps:.7f}" if ps is not None else "None"
        ss_s = f"{ss:.8f}" if ss is not None else "None"
        return f"{cat}: {prob} (Score: {ps_s}, Severity: {ss_s})"

    # 输出必转义（2026-11 S-7）：cat/prob 是上游枚举名，值域当前受 Google
    # 控制，但「全链路输出必转义」的可审计不变量不应有例外——换一个上
    # 游/中转（反代注入、自定义网关）即变成注入面。
    summary_line = html.escape(_fmt(*highest))
    all_ratings_str = html.escape("\n".join(_fmt(*t) for t in normalized))

    css_style = "<style>.cb{border:1px solid #444;margin:10px;border-radius:4px;background:#111}.cb summary{padding:8px;cursor:pointer;background:#222}.cb pre{margin:0;padding:10px;border-top:1px solid #444;white-space:pre-wrap}</style>"
    html_output = (
        f"{css_style}"
        f"<details class='cb'>"
        f"<summary style='color:{color}'>{summary_line} ▼</summary>"
        f"<pre>\n--- Safety Ratings ---\n{all_ratings_str}\n</pre>"
        f"</details>"
    )
    return html_output


def safety_ratings_presentation(ratings: Any, *, structured_output: bool) -> str:
    """Decorate ordinary chat only; machine-readable payloads must stay intact."""
    if structured_output or not ratings:
        return ""
    try:
        enabled = app_state.get_setting("safety_score", app_config.SAFETY_SCORE)
    except Exception:
        enabled = app_config.SAFETY_SCORE
    return _create_safety_ratings_html(ratings) if enabled else ""


def _convert_image_to_markdown(image_data: bytes, mime_type: str) -> str:
    from image_delivery import image_base_url, image_markdown
    if image_base_url.get():
        return image_markdown(image_data, mime_type)
    # 内存关键路径：4K 生图的 base64 单张可达数十 MB。旧实现先拼
    # data_url 再包 markdown（b64 bytes / b64 str / data_url / markdown
    # 四份全量拷贝同时驻留），512MB 容器（Render 免费档）叠加上游解析
    # 与 SSE 分帧会被直接 OOMKill。一步拼出最终串，b64encode 的中间
    # bytes 在 .decode() 后即刻释放，峰值驻留减半（b64 str + markdown）。
    try:
        b64_data = base64.b64encode(image_data).decode("utf-8")
        return f"![Image](data:{mime_type};base64,{b64_data})"
    except Exception as e:
        print(f"⚠️ [图片处理] 将 Gemini 图片转换为 Markdown 失败：{e}")
        return "[Image could not be displayed]"


def sdk_ordinary_part_fields(part: Any, signature: Optional[bytes]) -> Optional[tuple[str, str]]:
    """Use the same topology for completed and streamed SDK replies."""
    if sdk_media_payload(part) is not None:
        return "image", ""  # Wire content already carries the actual media.
    text = getattr(part, "text", None)
    if text is None and (getattr(part, "inline_data", None) is not None
                         or getattr(part, "file_data", None) is not None):
        return ("signature_only", "") if signature else None
    text = "" if text is None else str(text)
    if getattr(part, "thought", None) is True:
        return "thought", text
    return ("signature_only" if not text and signature else "text"), text


def parse_gemini_response_for_reasoning_and_content(
    gemini_response_candidate: Any,
) -> Tuple[str, str]:
    reasoning_text_parts = []
    normal_text_parts = []
    candidate_part_text = ""
    if (
        hasattr(gemini_response_candidate, "text")
        and gemini_response_candidate.text is not None
    ):
        candidate_part_text = str(gemini_response_candidate.text)

    gemini_candidate_content = None
    if hasattr(gemini_response_candidate, "content"):
        gemini_candidate_content = gemini_response_candidate.content

    if (
        gemini_candidate_content
        and hasattr(gemini_candidate_content, "parts")
        and gemini_candidate_content.parts
    ):
        for part_item in gemini_candidate_content.parts:
            if sdk_function_call(part_item) is not None:
                continue

            part_text = ""
            if hasattr(part_item, "text") and part_item.text is not None:
                part_text = str(part_item.text)
            elif (media := sdk_media_payload(part_item)) is not None:
                source, payload = media
                if source == "inline":
                    part_text = _convert_image_to_markdown(payload.data, getattr(payload, "mime_type", None))
                else:
                    part_text = file_data_markdown(payload.file_uri, getattr(payload, "mime_type", None))

            part_is_thought = (
                hasattr(part_item, "thought") and part_item.thought is True
            )

            if part_is_thought:
                reasoning_text_parts.append(part_text)
            elif part_text:
                normal_text_parts.append(part_text)

    elif candidate_part_text:
        normal_text_parts.append(candidate_part_text)
    elif (
        gemini_candidate_content
        and hasattr(gemini_candidate_content, "text")
        and gemini_candidate_content.text is not None
    ):
        normal_text_parts.append(str(gemini_candidate_content.text))
    elif (
        hasattr(gemini_response_candidate, "text")
        and gemini_response_candidate.text is not None
        and not gemini_candidate_content
    ):
        normal_text_parts.append(str(gemini_response_candidate.text))

    return "".join(reasoning_text_parts), "".join(normal_text_parts)


def process_gemini_response_to_openai_dict(
    gemini_response_obj: Any, request_model_str: str, *, structured_output: bool = False
) -> Dict[str, Any]:
    choices = []
    pending_signatures: PendingCallSignatures = []
    response_timestamp = int(time.time())
    base_id = f"chatcmpl-{response_timestamp}-{uuid.uuid4().hex[:12]}"

    if hasattr(gemini_response_obj, "candidates") and gemini_response_obj.candidates:
        for i, candidate in enumerate(gemini_response_obj.candidates):
            message_payload = {"role": "assistant"}

            raw_finish_reason = getattr(candidate, "finish_reason", None)
            openai_finish_reason = map_finish_reason_to_openai(
                raw_finish_reason, default="stop"
            )

            parts_in_order = list(
                getattr(getattr(candidate, "content", None), "parts", None) or []
            )
            if openai_finish_reason in ("stop", "tool_calls"):
                # A complete executable candidate must have unambiguous result IDs.
                require_unique_tool_call_ids(
                    getattr(fc, "id", None) for p in parts_in_order
                    if (fc := sdk_function_call(p)) is not None)
            reasoning_str, normal_content_str = (
                parse_gemini_response_for_reasoning_and_content(candidate)
            )
            normal_content_str += safety_ratings_presentation(
                getattr(candidate, "safety_ratings", None), structured_output=structured_output)

            tool_index = 0
            ordinary_metadata = []
            order_descriptors = []
            message_signature = None
            message_signature_kind = None
            for part in parts_in_order:
                fc = sdk_function_call(part)
                sig = signature_bytes(part, fc)
                if fc is not None:
                    arguments = complete_function_arguments(fc)
                    tool_call_id = stage_tool_call_id(
                        pending_signatures, fc, sig, unsigned_follower=tool_index > 0
                    )
                    tool_payload = {
                        "id": tool_call_id,
                        "type": "function",
                        "function": {
                            "name": fc.name,
                            "arguments": json.dumps(arguments, ensure_ascii=False, allow_nan=False),
                        },
                    }
                    attach_signature_extra(tool_payload, sig)
                    message_payload.setdefault("tool_calls", []).append(tool_payload)
                    order_descriptors.append({"type": "tool_call", "index": tool_index})
                    tool_index += 1
                    openai_finish_reason = map_finish_reason_to_openai(
                        raw_finish_reason, has_tool_calls=True
                    )
                    continue

                fields = sdk_ordinary_part_fields(part, sig)
                if fields is None:
                    continue
                ordinary_kind, text_value = fields
                is_thought = getattr(part, "thought", None) is True
                ordinary_index = len(ordinary_metadata)
                ordinary_metadata.append(
                    ordinary_part_metadata(ordinary_kind, text_value, sig, thought=is_thought)
                )
                order_descriptors.append({"type": "ordinary", "index": ordinary_index})
                if sig:
                    message_signature = sig
                    message_signature_kind = ordinary_kind

            if normal_content_str or not message_payload.get("tool_calls"):
                message_payload["content"] = normal_content_str
            else:
                message_payload["content"] = None
            if reasoning_str:
                message_payload["reasoning_content"] = reasoning_str

            if message_signature:
                message_payload["extra_content"] = thought_signature_extra(
                    message_signature, message_signature_kind
                )
            if ordinary_metadata:
                google = message_payload.setdefault("extra_content", {}).setdefault(
                    "google", {}
                )
                google["ordinary_parts"] = ordinary_metadata
            if message_payload.get("tool_calls") and order_descriptors:
                google = message_payload.setdefault("extra_content", {}).setdefault(
                    "google", {}
                )
                google["part_order"] = order_descriptors

            choice_item = {
                "index": candidate.index if getattr(candidate, "index", None) is not None else i,
                "message": message_payload,
                "finish_reason": openai_finish_reason,
            }
            logprobs_payload = logprobs_payload_from_candidate(candidate)
            if logprobs_payload is not None:
                choice_item["logprobs"] = logprobs_payload
            choices.append(choice_item)

    elif hasattr(gemini_response_obj, "text") and gemini_response_obj.text is not None:
        content_str = gemini_response_obj.text or ""
        choices.append(
            {
                "index": 0,
                "message": {"role": "assistant", "content": content_str},
                "finish_reason": "stop",
            }
        )
    else:
        choices.append(
            {
                "index": 0,
                "message": {"role": "assistant", "content": None},
                "finish_reason": "stop",
            }
        )

    usage_data = map_usage(getattr(gemini_response_obj, "usage_metadata", None))
    # A later invalid part or candidate rejects the whole reply, including its IDs.
    if pending_signatures:
        publish_tool_call_signatures(pending_signatures)
    return {
        "id": base_id,
        "object": "chat.completion",
        "created": response_timestamp,
        "model": request_model_str,
        "choices": choices,
        "usage": usage_data,
    }


def convert_to_openai_format(gemini_response: Any, model: str, *, structured_output: bool = False) -> Dict[str, Any]:
    return process_gemini_response_to_openai_dict(gemini_response, model, structured_output=structured_output)
