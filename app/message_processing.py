"""OpenAI 与 Gemini 消息、内容和响应格式转换。"""
import base64
import json
import re
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

import config as app_config
from google.genai import types
from image_processing import (
    decode_data_url_image,
    fetch_remote_image,
    optimize_image_bytes,
)
from models import OpenAIMessage, message_text, normalize_content_part
from runtime_state import app_state
from signature_store import SignatureState
from thought_signatures import (
    LEGACY_THOUGHT_SEP,
    build_tool_call_id,
    google_metadata,
    ordinary_part_metadata,
    ordinary_parts_from_extra,
    requires_signature,
    resolve_tool_call_signature,
    signature_bytes,
    signature_from_extra,
    thought_signature_extra,
)
from usage import map_usage

SUPPORTED_ROLES = ["user", "model", "function"]


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

    def _entry(item: Any) -> Dict[str, Any]:
        token = getattr(item, "token", None)
        logprob = getattr(item, "log_probability", None)
        if token is None and logprob is None:
            return {}
        entry: Dict[str, Any] = {"token": token or ""}
        if logprob is not None:
            entry["logprob"] = float(logprob)
        try:
            entry["bytes"] = list((token or "").encode("utf-8"))
        except (UnicodeError, ValueError):
            pass
        return entry

    content = []
    for i, item in enumerate(chosen):
        entry = _entry(item)
        if not entry:
            continue
        top_list = None
        if i < len(tops):
            top_list = [_entry(c) for c in (getattr(tops[i], "candidates", None) or [])]
            top_list = [t for t in top_list if t]
        if top_list:
            entry["top_logprobs"] = top_list
        content.append(entry)
    if not content:
        return None
    return {"content": content}


def _extract_markdown_images_to_parts(text: str) -> Tuple[List[types.Part], str]:
    parts = []
    remaining_text = text
    pattern = r"!\[[^\]]*\]\(data:(image/[a-zA-Z0-9+.-]+);base64,([a-zA-Z0-9+/=]+)\)"
    matches = list(re.finditer(pattern, text))
    
    if matches:
        extracted = 0
        for match in reversed(matches):
            mime_type = match.group(1)
            b64_data = match.group(2)
            try:
                raw_bytes = decode_data_url_image(b64_data)
                opt_bytes, opt_mime = optimize_image_bytes(raw_bytes, mime_type)
                parts.append(types.Part.from_bytes(data=opt_bytes, mime_type=opt_mime))
                start, end = match.span()
                remaining_text = remaining_text[:start] + remaining_text[end:]
                extracted += 1
            except Exception as e:
                print(f"⚠️ [图片处理] 提取 Markdown 图片失败，已跳过该图片：{e}")
        parts.reverse()
        # 仅在**确实抽走了图片**时才压平空白：抠掉 data URL 会留下成片空格。
        # 旧实现无条件执行，导致所有纯文本消息的缩进/多空格（代码块、ASCII 图、
        # 酒馆预设的排版）都被悄悄改写；全部提取失败时同样不该改写文本。
        # 文本保真优先。
        if extracted:
            remaining_text = re.sub(r"[ \t]+", " ", remaining_text).strip()

    return parts, remaining_text


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
            return {"result": content}
        try:
            return {"result": json.dumps(content, ensure_ascii=False)}
        except Exception:
            return {"result": str(content)}
    s = content.strip()
    if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
        try:
            parsed = json.loads(s)
            # function_response 的 response 需要是对象；数组则包一层
            return parsed if isinstance(parsed, dict) else {"result": parsed}
        except json.JSONDecodeError:
            return {"result": content}
    return {"result": content}


def create_gemini_prompt(messages: List[OpenAIMessage], model_name: str = "") -> List[types.Content]:
    """OpenAI 消息 → Gemini contents。

    model_name 用于判断是否需要对缺失的思考签名启用官方哨兵（仅 Gemini 3.x 强校验）。
    留空表示“未知模型”，此时不注入哨兵——调用方（express_sdk）总会传真实模型名。
    """
    print("🔄 [消息转换] 正在将 OpenAI 格式消息转换为 Gemini contents。")
    require_sig = requires_signature(model_name)
    raw_gemini_messages = []
    tool_name_by_id = {}
    for prior in messages:
        for tool_call in (getattr(prior, "tool_calls", None) or []):
            call_id = str(tool_call.get("id") or "")
            call_name = str((tool_call.get("function") or {}).get("name") or "")
            if call_id and call_name:
                tool_name_by_id[call_id] = call_name
                tool_name_by_id[call_id.partition(LEGACY_THOUGHT_SEP)[0]] = call_name

    for idx, message in enumerate(messages):
        role = message.role
        # OpenAI 新约定（o 系列/Responses 风格）用 developer 表达最高优先级指令，
        # 语义等同 system：统一跳过 contents，由 system_instruction 单独承载，
        # 避免被降级成 user 轮混进对话（2026-10 修复）。
        if role in ("system", "developer"):
            continue

        parts = []
        current_gemini_role = "" 

        if role in ("tool", "function"):
            # 旧版 OpenAI 协议的 role="function"（name=函数名，content=结果字符串）
            # 与 role="tool" 语义相同；旧实现会把它当成 Gemini 的 role 直发（
            # Gemini 仅接受 user/model，直接 400），这里并入 tool 分支统一转
            # functionResponse。
            tool_call_id_str = message.tool_call_id or ""
            real_tool_id = tool_call_id_str.partition(LEGACY_THOUGHT_SEP)[0]
            function_name = message.name or tool_name_by_id.get(tool_call_id_str) or tool_name_by_id.get(real_tool_id)

            if not function_name:
                # 既没有显式 name，也无法从前面的 assistant.tool_calls 关联时才降级。
                # content 可能是 str / 分段 list / dict：统一归一成文本，避免
                # list 直接被 f-string 嵌成 Python repr。
                fallback_text = message_text(message.content)
                if not fallback_text and message.content is not None and not isinstance(message.content, str):
                    try:
                        fallback_text = json.dumps(message.content, ensure_ascii=False)
                    except Exception:
                        fallback_text = str(message.content)
                mock_text = f"[System Observation - Tool Result]:\n{fallback_text}"
                parts.append(types.Part.from_text(text=mock_text))
                current_gemini_role = "user"
            else:
                # FunctionResponse is always a Gemini ``user`` Content and never
                # carries a thought signature. Signatures belong on model Parts.
                tool_output_data = _coerce_tool_response(message.content)

                func_resp_kwargs = {"name": function_name, "response": tool_output_data}
                if real_tool_id:
                    func_resp_kwargs["id"] = real_tool_id

                try:
                    resp_part = types.Part(
                        function_response=types.FunctionResponse(**func_resp_kwargs))
                except Exception as e:
                    print(f"⚠️ [工具调用] 构造 FunctionResponse 失败，将回退为基础形式：{e}")
                    resp_part = types.Part.from_function_response(name=function_name, response=tool_output_data)

                parts.append(resp_part)
                current_gemini_role = "user"

        elif role in ("assistant", "model") and message.tool_calls:
            current_gemini_role = "model"
            tool_parts = []
            for tool_index, tool_call in enumerate(message.tool_calls):
                function_call_data = tool_call.get("function", {})
                function_name = function_call_data.get("name", "unknown")
                arguments_str = function_call_data.get("arguments", "{}")
                tool_call_id_str = tool_call.get("id", "") or ""

                try:
                    parsed_arguments = json.loads(arguments_str) if isinstance(arguments_str, str) else (arguments_str or {})
                except json.JSONDecodeError as e:
                    print(f"⚠️ [工具调用] 历史 tool_call 的 arguments 不是合法 JSON（{e}），"
                          f"已按空参数下发：{str(arguments_str)[:120]}")
                    parsed_arguments = {}
                # FunctionCall.args 必须是 dict：合法 JSON 但非对象（如 "[1,2]"、"\"x\""）
                # 会让主分支与 from_function_call 回退分支**同样**抛 ValidationError
                # （旧实现两处都未兑底，异常直达客户端变 500）。与 Cookie 通道
                # _parse_function_args 的防御口径对齐：非 dict 一律归一为 {}。
                if not isinstance(parsed_arguments, dict):
                    print(f"⚠️ [工具调用] 历史 tool_call 的 arguments 不是 JSON 对象，已按空参数下发："
                          f"{str(arguments_str)[:120]}")
                    parsed_arguments = {}

                explicit_sig, _ = signature_from_extra(tool_call)
                # One assistant message is one function-calling step: only its
                # first call requires a signature. Later calls are explicitly
                # unsigned parallel followers and must not receive sentinels.
                real_tool_id, thought_sig_bytes = resolve_tool_call_signature(
                    tool_call_id_str,
                    require_signature=require_sig and tool_index == 0,
                    explicit_signature=explicit_sig,
                    explicit_unsigned=tool_index > 0,
                )

                fc_kwargs = {"name": function_name, "args": parsed_arguments}
                if real_tool_id:
                    fc_kwargs["id"] = real_tool_id

                try:
                    part_kwargs = {"function_call": types.FunctionCall(**fc_kwargs)}
                    if thought_sig_bytes:
                        part_kwargs["thought_signature"] = thought_sig_bytes
                    fc_part = types.Part(**part_kwargs)
                except Exception as e:
                    print(f"⚠️ [工具调用] 构造 FunctionCall 失败，将回退为基础形式：{e}")
                    fc_part = types.Part.from_function_call(name=function_name, args=parsed_arguments)
                tool_parts.append(fc_part)

            ordinary_parts = ordinary_parts_from_extra(message)
            if ordinary_parts is None:
                ordinary_parts = []
                reasoning = getattr(message, "reasoning_content", None)
                if reasoning:
                    ordinary_parts.append(types.Part(text=reasoning, thought=True))
                # content 为分段 list 时旧实现会静默丢弃全部文本（只处理 str），
                # 与 Cookie 通道行为不一致；这里先归一成文本再走同一条抽取链。
                content_text = message_text(message.content) if isinstance(message.content, list) else message.content
                if isinstance(content_text, str) and content_text:
                    image_parts, clean_text = _extract_markdown_images_to_parts(content_text)
                    if clean_text:
                        ordinary_parts.append(types.Part.from_text(text=clean_text))
                    ordinary_parts.extend(image_parts)

                message_sig, signature_kind = signature_from_extra(message)
                if message_sig:
                    target = None
                    if signature_kind == "thought":
                        target = next((p for p in reversed(ordinary_parts) if getattr(p, "thought", None)), None)
                    elif signature_kind == "text":
                        target = next((p for p in reversed(ordinary_parts)
                                       if getattr(p, "text", None) is not None and not getattr(p, "thought", None)), None)
                    if target is None and signature_kind != "signature_only":
                        target = ordinary_parts[-1] if ordinary_parts else None
                    if target is not None:
                        # list.index 按 == 匹配：等值重复 part 时会命中第一个，而
                        # target 是从后往前选出的那个——签名必须落在被选中的对象上。
                        ordinary_parts[_rindex_by_identity(ordinary_parts, target)] = target.model_copy(
                            update={"thought_signature": message_sig})
                    else:
                        ordinary_parts.append(types.Part(text="", thought_signature=message_sig))

            google_extra = google_metadata(message)
            part_order = google_extra.get("part_order")
            if isinstance(part_order, list):
                ordered, used_tools, used_ordinary = [], set(), set()
                for item in part_order:
                    if isinstance(item, dict) and item.get("type") == "tool_call":
                        i = item.get("index")
                        if isinstance(i, int) and 0 <= i < len(tool_parts):
                            ordered.append(tool_parts[i]); used_tools.add(i)
                    elif isinstance(item, dict) and item.get("type") == "ordinary":
                        i = item.get("index")
                        if isinstance(i, int) and 0 <= i < len(ordinary_parts):
                            ordered.append(ordinary_parts[i]); used_ordinary.add(i)
                ordered.extend(p for i, p in enumerate(ordinary_parts) if i not in used_ordinary)
                ordered.extend(p for i, p in enumerate(tool_parts) if i not in used_tools)
                parts.extend(ordered)
            else:
                # Historical behavior emitted calls first. Preserve it unless a
                # response from this bridge supplies exact part_order metadata.
                parts.extend(tool_parts)
                parts.extend(ordinary_parts)
        else:
            message_sig, signature_kind = signature_from_extra(message)
            reasoning = getattr(message, "reasoning_content", None)
            if message.content is None and not reasoning and not message_sig:
                continue

            current_gemini_role = role
            if current_gemini_role in ("assistant", "model"):
                current_gemini_role = "model"
            if current_gemini_role not in SUPPORTED_ROLES:
                current_gemini_role = "user"

            exact_ordinary_parts = ordinary_parts_from_extra(message)
            if exact_ordinary_parts is not None:
                parts.extend(exact_ordinary_parts)

            if reasoning and exact_ordinary_parts is None:
                parts.append(types.Part(text=reasoning, thought=True))

            if exact_ordinary_parts is None and isinstance(message.content, str):
                image_parts, clean_text = _extract_markdown_images_to_parts(message.content)
                if clean_text: parts.append(types.Part.from_text(text=clean_text))
                parts.extend(image_parts)

            elif exact_ordinary_parts is None and isinstance(message.content, list):
                for part_item in message.content:
                    # F-1：pydantic 会把标准 part 解析成 ContentPartText / ContentPartImage 实例，
                    # 归一成 dict 后只保留一条处理路径（原先 dict 与实例两套分支已出现行为分歧：
                    # 实例分支直接 from_text，跳过了 markdown 内联图片抽取）。
                    part_item = normalize_content_part(part_item)
                    if not isinstance(part_item, dict):
                        text_attr = getattr(part_item, "text", None)
                        if isinstance(text_attr, str):
                            parts.append(types.Part.from_text(text=text_attr))
                        continue

                    if part_item.get("type") == "text":
                        # or 兑底：{"type":"text","text":null} 会以原始 dict 留在
                        # union 里，get("text", "\n") 此时返回 None，下游 re.finditer
                        # 直接 TypeError（2026-10 修复，与 message_text 同口径）。
                        text_content = part_item.get("text") or ""
                        image_parts, clean_text = _extract_markdown_images_to_parts(text_content)
                        if clean_text: parts.append(types.Part.from_text(text=clean_text))
                        parts.extend(image_parts)

                    elif part_item.get("type") == "image_url":
                        img_url_data = part_item.get("image_url") or {}
                        if isinstance(img_url_data, dict):
                            image_url = img_url_data.get("url", "")
                        else:
                            image_url = getattr(img_url_data, "url", "") or ""

                        if image_url.startswith("data:"):
                            # re.DOTALL：RFC 2045 允许 base64 折行，含换行的
                            # data URL 也必须能解出（与 Cookie 通道实现对齐；
                            # 旧实现缺 DOTALL 会静默丢图）。解码失败不再炸整
                            # 个请求（与 Cookie 通道一致：跳过该图并留日志）。
                            mime_match = re.match(r"data:([^;]+);base64,(.+)", image_url, re.DOTALL)
                            if mime_match:
                                mime_type, b64_data = mime_match.groups()
                                try:
                                    raw_bytes = decode_data_url_image(b64_data)
                                    opt_bytes, opt_mime = optimize_image_bytes(raw_bytes, mime_type)
                                    parts.append(types.Part.from_bytes(data=opt_bytes, mime_type=opt_mime))
                                except Exception as e:
                                    print(f"⚠️ [图片处理] 解析内联图片失败，已跳过：{e}")
                        elif image_url.startswith(("http://", "https://")):
                            # 统一走加固后的 fetch_remote_image（SSRF 与体积防护都在那里，见 F-3）。
                            fetched = fetch_remote_image(image_url)
                            if fetched:
                                img_bytes, mime_type = fetched
                                opt_bytes, opt_mime = optimize_image_bytes(img_bytes, mime_type)
                                parts.append(types.Part.from_bytes(data=opt_bytes, mime_type=opt_mime))

            if message_sig and exact_ordinary_parts is None:
                target = None
                if signature_kind == "thought":
                    target = next((p for p in reversed(parts) if getattr(p, "thought", None)), None)
                elif signature_kind == "text":
                    target = next((p for p in reversed(parts)
                                   if getattr(p, "text", None) is not None and not getattr(p, "thought", None)), None)
                if target is None and signature_kind != "signature_only":
                    target = parts[-1] if parts else None
                if target is None:
                    parts.append(types.Part(text="", thought_signature=message_sig))
                else:
                    # 同上：按对象身份从后往前定位，避免等值重复 part 挂错签名。
                    parts[_rindex_by_identity(parts, target)] = target.model_copy(update={"thought_signature": message_sig})

        if not parts: continue
        raw_gemini_messages.append(types.Content(role=current_gemini_role, parts=parts))

    def _is_text_only(content: types.Content) -> bool:
        """该 Content 是否只含文本 part（没有 function_call / function_response / inline_data）。"""
        for p in content.parts or []:
            if getattr(p, "function_call", None) is not None:
                return False
            if getattr(p, "function_response", None) is not None:
                return False
            if getattr(p, "inline_data", None) is not None:
                return False
            if getattr(p, "text", None) is None:
                return False
        return True

    def _is_function_response_only(content: types.Content) -> bool:
        return bool(content.parts) and all(
            getattr(p, "function_response", None) is not None for p in content.parts)

    merged_messages = []
    for msg in raw_gemini_messages:
        if merged_messages and merged_messages[-1].role == msg.role:
            previous = merged_messages[-1]
            previous_is_results = _is_function_response_only(previous)
            current_is_results = _is_function_response_only(msg)
            # Parallel tool results are contiguous user Parts. A later fresh
            # user turn must remain a new Content because it starts a new turn
            # for Gemini's signature validator.
            if previous_is_results != current_is_results:
                merged_messages.append(msg)
                continue
            if _is_text_only(previous) and _is_text_only(msg):
                previous.parts.append(types.Part.from_text(text="\n\n"))
            previous.parts.extend(msg.parts)
        else:
            merged_messages.append(msg)

    if not merged_messages:
        merged_messages.append(types.Content(role="user", parts=[types.Part.from_text(text="继续")]))

    return merged_messages


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


def _wire_image_part(raw_bytes: bytes, mime_type: str) -> Dict[str, Any]:
    """压缩后转成 batchGraphql 的 inlineData part（camelCase）。"""
    opt_bytes, opt_mime = optimize_image_bytes(raw_bytes, mime_type)
    return {"inlineData": {"mimeType": opt_mime,
                           "data": base64.b64encode(opt_bytes).decode("utf-8")}}


def openai_content_to_wire_parts(content: Any) -> List[Dict[str, Any]]:
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
        img_parts, clean_text = _extract_markdown_images_to_parts(text)
        if clean_text:
            parts.append({"text": clean_text})
        for ip in img_parts:
            blob = getattr(ip, "inline_data", None)
            if blob is not None and getattr(blob, "data", None):
                data = blob.data
                if isinstance(data, bytes):
                    data = base64.b64encode(data).decode("utf-8")
                parts.append({"inlineData": {"mimeType": getattr(blob, "mime_type", "image/jpeg"),
                                             "data": data}})

    if isinstance(content, str):
        _add_text_with_inline_images(content)
        return parts

    if isinstance(content, list):
        for item in content:
            if hasattr(item, "model_dump"):
                item = item.model_dump()
            if isinstance(item, str):
                _add_text_with_inline_images(item)
                continue
            if not isinstance(item, dict):
                text_attr = getattr(item, "text", None)
                if isinstance(text_attr, str):
                    _add_text_with_inline_images(text_attr)
                continue

            item_type = item.get("type", "")
            if item_type == "text":
                _add_text_with_inline_images(item.get("text", ""))
            elif item_type == "image_url":
                url = item.get("image_url", {})
                if isinstance(url, dict):
                    url = url.get("url", "")
                if not isinstance(url, str) or not url:
                    continue
                if url.startswith("data:"):
                    m = re.match(r"data:([^;]+);base64,(.+)", url, re.DOTALL)
                    if m:
                        mime_type, b64_data = m.groups()
                        try:
                            parts.append(_wire_image_part(decode_data_url_image(b64_data), mime_type))
                        except Exception as e:
                            print(f"⚠️ [图片处理] 解析内联图片失败，已跳过：{e}")
                elif url.startswith("http"):
                    fetched = fetch_remote_image(url)
                    if fetched:
                        parts.append(_wire_image_part(*fetched))
    return parts


def _rating_fields(r: Any) -> Tuple[str, str, Optional[float], Optional[float]]:
    """把一条安全评分归一成 (分类, 概率档, 概率分, 严重度分)。

    Express 通道给的是 SDK 对象（属性 + 枚举），Cookie 通道给的是 batchGraphql
    的 camelCase 字典，两边共用同一个渲染器。
    """
    def _enum_name(v):
        return getattr(v, "name", None) or (str(v) if v is not None else "")

    if isinstance(r, dict):
        cat = str(r.get("category") or "")
        prob = str(r.get("probability") or "")
        ps, ss = r.get("probabilityScore"), r.get("severityScore")
    else:
        cat = _enum_name(getattr(r, "category", None))
        prob = _enum_name(getattr(r, "probability", None))
        ps, ss = getattr(r, "probability_score", None), getattr(r, "severity_score", None)
    cat = cat.replace("HARM_CATEGORY_", "").replace("_", " ").title()
    return cat, prob, (ps if isinstance(ps, (int, float)) else None), (ss if isinstance(ss, (int, float)) else None)


def _create_safety_ratings_html(safety_ratings: list) -> str:
    if not safety_ratings:
        return ""
    # 上游对部分分类只给 probability 不给 probability_score（实测 JAILBREAK 常为 None），
    # 直接 max(key=probability_score) 会拿 None 和 float 比较 → TypeError，
    # 整个请求变成 500。缺分数的按 -1 排，永远不会被选成“最高分”。
    normalized = [_rating_fields(r) for r in safety_ratings]
    highest = max(normalized, key=lambda t: t[2] if t[2] is not None else -1.0)
    highest_score = highest[2] if highest[2] is not None else -1.0

    if highest_score < 0: color = "#888"          # 一条分数都没有
    elif highest_score <= 0.33: color = "#0f8"
    elif highest_score <= 0.66: color = "yellow"
    else: color = "#bf555d"

    def _fmt(cat, prob, ps, ss):
        ps_s = f"{ps:.7f}" if ps is not None else "None"
        ss_s = f"{ss:.8f}" if ss is not None else "None"
        return f"{cat}: {prob} (Score: {ps_s}, Severity: {ss_s})"

    summary_line = _fmt(*highest)
    all_ratings_str = "\n".join(_fmt(*t) for t in normalized)

    css_style = "<style>.cb{border:1px solid #444;margin:10px;border-radius:4px;background:#111}.cb summary{padding:8px;cursor:pointer;background:#222}.cb pre{margin:0;padding:10px;border-top:1px solid #444;white-space:pre-wrap}</style>"
    html_output = (
        f"{css_style}"
        f"<details class='cb'>"
        f"<summary style='color:{color}'>{summary_line} ▼</summary>"
        f"<pre>\n--- Safety Ratings ---\n{all_ratings_str}\n</pre>"
        f"</details>"
    )
    return html_output


def _convert_image_to_markdown(image_data: bytes, mime_type: str) -> str:
    try:
        b64_data = base64.b64encode(image_data).decode("utf-8")
        data_url = f"data:{mime_type};base64,{b64_data}"
        return f"![Image]({data_url})"
    except Exception as e:
        print(f"⚠️ [图片处理] 将 Gemini 图片转换为 Markdown 失败：{e}")
        return "[Image could not be displayed]"


def parse_gemini_response_for_reasoning_and_content(gemini_response_candidate: Any) -> Tuple[str, str]:
    reasoning_text_parts = []
    normal_text_parts = []
    candidate_part_text = ""
    if hasattr(gemini_response_candidate, "text") and gemini_response_candidate.text is not None:
        candidate_part_text = str(gemini_response_candidate.text)

    gemini_candidate_content = None
    if hasattr(gemini_response_candidate, "content"):
        gemini_candidate_content = gemini_response_candidate.content

    if gemini_candidate_content and hasattr(gemini_candidate_content, "parts") and gemini_candidate_content.parts:
        for part_item in gemini_candidate_content.parts:
            if hasattr(part_item, "function_call") and part_item.function_call is not None: 
                continue
            
            part_text = ""
            if hasattr(part_item, "text") and part_item.text is not None:
                part_text = str(part_item.text)
            elif hasattr(part_item, "inline_data") and part_item.inline_data is not None:
                inline_data = part_item.inline_data
                if hasattr(inline_data, "data") and hasattr(inline_data, "mime_type"):
                    image_bytes = inline_data.data
                    mime_type = inline_data.mime_type
                    part_text = _convert_image_to_markdown(image_bytes, mime_type)
            elif hasattr(part_item, "file_data") and part_item.file_data is not None:
                file_data = part_item.file_data
                if hasattr(file_data, "file_uri"):
                    file_uri = file_data.file_uri
                    part_text = f"![Image]({file_uri})"
            
            part_is_thought = hasattr(part_item, "thought") and part_item.thought is True

            if part_is_thought: reasoning_text_parts.append(part_text)
            elif part_text: normal_text_parts.append(part_text)
            
    elif candidate_part_text: normal_text_parts.append(candidate_part_text)
    elif gemini_candidate_content and hasattr(gemini_candidate_content, "text") and gemini_candidate_content.text is not None:
        normal_text_parts.append(str(gemini_candidate_content.text))
    elif hasattr(gemini_response_candidate, "text") and gemini_response_candidate.text is not None and not gemini_candidate_content: 
        normal_text_parts.append(str(gemini_response_candidate.text))

    return "".join(reasoning_text_parts), "".join(normal_text_parts)


def process_gemini_response_to_openai_dict(gemini_response_obj: Any, request_model_str: str) -> Dict[str, Any]:
    choices = []
    response_timestamp = int(time.time())
    base_id = f"chatcmpl-{response_timestamp}-{uuid.uuid4().hex[:12]}"

    if hasattr(gemini_response_obj, "candidates") and gemini_response_obj.candidates:
        for i, candidate in enumerate(gemini_response_obj.candidates):
            message_payload = {"role": "assistant"}
            
            raw_finish_reason = getattr(candidate, "finish_reason", None)
            openai_finish_reason = "stop" 
            if raw_finish_reason:
                if hasattr(raw_finish_reason, "name"): raw_gemini_finish_reason_str = raw_finish_reason.name.upper()
                else: raw_gemini_finish_reason_str = str(raw_finish_reason).upper()

                if raw_gemini_finish_reason_str == "STOP": openai_finish_reason = "stop"
                elif raw_gemini_finish_reason_str == "MAX_TOKENS": openai_finish_reason = "length"
                elif raw_gemini_finish_reason_str in ("SAFETY", "PROHIBITED_CONTENT", "RECITATION", "BLOCKLIST", "SPII", "IMAGE_SAFETY"): openai_finish_reason = "content_filter"
                elif raw_gemini_finish_reason_str in ["TOOL_CODE", "FUNCTION_CALL"]: openai_finish_reason = "tool_calls"
            
            parts_in_order = list(getattr(getattr(candidate, "content", None), "parts", None) or [])
            reasoning_str, normal_content_str = parse_gemini_response_for_reasoning_and_content(candidate)
            if app_state.get_setting("safety_score", app_config.SAFETY_SCORE) and hasattr(candidate, "safety_ratings") and candidate.safety_ratings:
                normal_content_str += _create_safety_ratings_html(candidate.safety_ratings)

            tool_index = 0
            ordinary_metadata = []
            order_descriptors = []
            message_signature = None
            message_signature_kind = None
            for part in parts_in_order:
                fc = getattr(part, "function_call", None)
                sig = signature_bytes(part, fc)
                if fc is not None:
                    missing_state = (SignatureState.UNSIGNED_FOLLOWER if tool_index > 0
                                     else SignatureState.UNKNOWN)
                    tool_call_id = build_tool_call_id(fc, part, missing_state=missing_state)
                    tool_payload = {
                        "id": tool_call_id,
                        "type": "function",
                        "function": {
                            "name": fc.name,
                            "arguments": json.dumps(fc.args or {})
                        }
                    }
                    extra = thought_signature_extra(sig)
                    if extra:
                        tool_payload["extra_content"] = extra
                    message_payload.setdefault("tool_calls", []).append(tool_payload)
                    order_descriptors.append({"type": "tool_call", "index": tool_index})
                    tool_index += 1
                    openai_finish_reason = "tool_calls"
                    continue

                is_thought = getattr(part, "thought", None) is True
                text_value = getattr(part, "text", None)
                if text_value is None and getattr(part, "inline_data", None) is not None:
                    inline = part.inline_data
                    text_value = _convert_image_to_markdown(inline.data, inline.mime_type)
                elif text_value is None and getattr(part, "file_data", None) is not None:
                    text_value = f"![Image]({part.file_data.file_uri})"
                text_value = "" if text_value is None else str(text_value)
                if is_thought:
                    ordinary_kind = "thought"
                elif text_value == "" and sig:
                    ordinary_kind = "signature_only"
                else:
                    ordinary_kind = "text"
                ordinary_index = len(ordinary_metadata)
                ordinary_metadata.append(ordinary_part_metadata(ordinary_kind, text_value, sig))
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
                    message_signature, message_signature_kind)
            if ordinary_metadata:
                google = message_payload.setdefault("extra_content", {}).setdefault("google", {})
                google["ordinary_parts"] = ordinary_metadata
            if message_payload.get("tool_calls") and order_descriptors:
                google = message_payload.setdefault("extra_content", {}).setdefault("google", {})
                google["part_order"] = order_descriptors
            
            choice_item = {"index": i, "message": message_payload, "finish_reason": openai_finish_reason}
            logprobs_payload = logprobs_payload_from_candidate(candidate)
            if logprobs_payload is not None:
                choice_item["logprobs"] = logprobs_payload
            choices.append(choice_item)
            
    elif hasattr(gemini_response_obj, "text") and gemini_response_obj.text is not None:
         content_str = gemini_response_obj.text or ""
         choices.append({"index": 0, "message": {"role": "assistant", "content": content_str}, "finish_reason": "stop"})
    else: 
         choices.append({"index": 0, "message": {"role": "assistant", "content": None}, "finish_reason": "stop"})

    usage_data = map_usage(getattr(gemini_response_obj, "usage_metadata", None))

    return {
        "id": base_id, "object": "chat.completion", "created": response_timestamp,
        "model": request_model_str, "choices": choices,
        "usage": usage_data
    }


def convert_to_openai_format(gemini_response: Any, model: str) -> Dict[str, Any]:
    return process_gemini_response_to_openai_dict(gemini_response, model)
