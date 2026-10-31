"""Anthropic Claude Messages API 兼容层：协议转换纯逻辑（无路由、无 IO）。

设计参照 vertex-master（Go 版）的 anthropic_handler / transform 包，整体管道：

    Anthropic 请求 JSON
      → anthropic_to_openai_request()          [Anthropic → 内部 OpenAIRequest]
      → 复用 /v1/chat/completions 同款上游管线  [Express Key / Cookie 双通道]
      → openai_completion_to_anthropic()        [OpenAI 非流式 → Anthropic message]
        或 ClaudeStreamState                    [OpenAI SSE chunk → Anthropic SSE 事件]

关键兼容口径（与 vertex-master 对齐）：
- max_tokens 必填（仅 /v1/messages；count_tokens 不要求）；
- thinking.budget_tokens 必须是 >= 1024 的整数，映射到内部 reasoning_effort
  （<=1024 → low，<=8192 → medium，>8192 → high；disabled → off；adaptive → 跟随默认）；
- 优先级：thinking.enabled/disabled 钉住档位，output_config.effort 仅在 thinking
  缺失或 adaptive 时生效（对齐 vertex-master：Go 版 convertChatRequest 中
  thinking 块在 reasoning_effort 之后处理且整写覆盖 thinkingConfig）；
- thinking.display=omitted → 响应侧不回传思考块（对齐 vertex-master
  includeThoughts = display != "omitted"；模型仍思考，用量照计）；
- 历史中 assistant 的 thinking / redacted_thinking 块静默丢弃：Gemini 无法消费
  Anthropic 的思考签名，保留相邻 text / tool_use 即可；
- 响应侧 thinking 块必须携带非空 signature（此处用 SHA-256 前 32 字节 hex 伪签），
  否则 Claude Code / SDK 下一轮会把历史判为非法；
- cache_control 全程剥离（不做显式缓存计费）；上游隐式缓存命中经
  prompt_tokens_details.cached_tokens 映射为 cache_read_input_tokens，并从
  input_tokens 中扣除，避免重复计数；cache_creation 恒 0；
- 中途 system 消息（Claude Code queued steering 场景）降级为 user 轮，保留新近性；
- tool_choice: auto→auto、any→required、none→none、tool→指定函数（未声明则 400）；
- stop_reason 映射：tool_use / max_tokens / refusal / end_turn；
- 上游错误如实透传，状态码 + Anthropic 错误类型（invalid_request_error 等）。
"""

import hashlib
import json
import math
import re
import secrets
from typing import Any, Optional

from models import OpenAIMessage, OpenAIRequest
from tool_arguments import parse_tool_arguments
from response_validation import IncompleteResponseError
from thought_signatures import google_metadata, ordinary_part_metadata
from schema_validation import SchemaValidationError, validate_json_schema, sanitize_for_gemini

# ============================================================
# 错误与 ID 生成
# ============================================================

class ClaudeRequestError(Exception):
    """Anthropic 请求解析/校验失败：400 invalid_request_error（status 可覆盖）。"""

    def __init__(self, message: str, err_type: str = "invalid_request_error", status: int = 400):
        super().__init__(message)
        self.err_type = err_type
        self.status = status


def new_message_id() -> str:
    """Anthropic 消息 ID：msg_ + 24 位 hex（与官方格式同形）。"""
    return f"msg_{secrets.token_hex(12)}"


def new_request_id() -> str:
    """Anthropic 错误体携带的 request_id：req_ + 24 位 hex（对齐 vertex-master）。"""
    return f"req_{secrets.token_hex(12)}"


def new_tool_id() -> str:
    """流式中上游未给 id 时兜底生成的 tool_use ID（toolu_ 前缀）。"""
    return f"toolu_{secrets.token_hex(12)}"


def thinking_signature(thinking_text: str) -> str:
    """思考块伪签名：SHA-256(thinking) 前 32 字节的 hex（64 字符）。

    同文同签、异文异签、恒非空——满足 Claude SDK 对历史 thinking 块
    「signature 必须非空」的校验，但不与任何真实密钥相关。
    """
    return hashlib.sha256((thinking_text or "").encode("utf-8")).hexdigest()[:64]


def anthropic_error_type(status: int) -> str:
    """HTTP 状态码 → Anthropic 错误类型（对齐 vertex-master 的映射表）。"""
    return {
        400: "invalid_request_error",
        401: "authentication_error",
        403: "permission_error",
        404: "not_found_error",
        408: "invalid_request_error",
        413: "invalid_request_error",
        429: "rate_limit_error",
        500: "api_error",
        502: "api_error",
        503: "overloaded_error",
        504: "api_error",
        529: "overloaded_error",
    }.get(int(status), "api_error")


# ============================================================
# 模型映射：claude-* → 内部 Gemini 模型
# ============================================================

# claude-* → 内部 Gemini 模型的静态映射表（键为小写模型名）。
# 命不中时走家族启发（resolve_claude_model）；非 claude-* 名字不查此表。
_CLAUDE_MODEL_ALIASES = {
    # Opus（最强）→ Pro
    "claude-opus-4-1": "gemini-3.1-pro-preview",
    "claude-opus-4-1-20250805": "gemini-3.1-pro-preview",
    "claude-opus-4": "gemini-3.1-pro-preview",
    "claude-opus-4-20250514": "gemini-3.1-pro-preview",
    "claude-3-opus-20240229": "gemini-3.1-pro-preview",
    "claude-3-opus-latest": "gemini-3.1-pro-preview",
    # Sonnet（均衡）→ 最新 Flash
    "claude-sonnet-4-5": "gemini-3.8-flash",
    "claude-sonnet-4-5-20250929": "gemini-3.8-flash",
    "claude-sonnet-4": "gemini-3.8-flash",
    "claude-sonnet-4-20250514": "gemini-3.8-flash",
    "claude-3-7-sonnet-latest": "gemini-3.8-flash",
    "claude-3-7-sonnet": "gemini-3.8-flash",
    "claude-3-7-sonnet-20250219": "gemini-3.8-flash",
    "claude-3-5-sonnet-latest": "gemini-3.8-flash",
    "claude-3-5-sonnet": "gemini-3.8-flash",
    "claude-3-5-sonnet-20241022": "gemini-3.8-flash",
    "claude-3-5-sonnet-20240620": "gemini-3.8-flash",
    "claude-3-sonnet-20240229": "gemini-3.8-flash",
    "claude-sonnet-latest": "gemini-3.8-flash",
    # Haiku（快/省）→ Flash-Lite
    "claude-haiku-4-5": "gemini-3.1-flash-lite",
    "claude-haiku-4-5-20251001": "gemini-3.1-flash-lite",
    "claude-3-5-haiku-latest": "gemini-3.1-flash-lite",
    "claude-3-5-haiku": "gemini-3.1-flash-lite",
    "claude-3-5-haiku-20241022": "gemini-3.1-flash-lite",
    "claude-3-haiku-20240307": "gemini-3.1-flash-lite",
    "claude-haiku-latest": "gemini-3.1-flash-lite",
}

# 家族兜底目标（内部模型清单不可用 / 静态映射未命中时）
_CLAUDE_FAMILY_FALLBACK = {
    "opus": "gemini-3.1-pro-preview",
    "sonnet": "gemini-3.8-flash",
    "haiku": "gemini-3.1-flash-lite",
}

# 模型列表（GET /v1/messages 的 Anthropic 格式目录 / Anthropic 客户端可见别名）
CANONICAL_CLAUDE_MODELS = (
    "claude-opus-4-1",
    "claude-sonnet-4-5",
    "claude-haiku-4-5",
    "claude-3-7-sonnet-latest",
    "claude-3-5-sonnet-latest",
    "claude-3-5-haiku-latest",
)


def _model_version(name: str) -> tuple[int, int]:
    m = re.search(r"gemini-(\d+)(?:\.(\d+))?", name or "")
    if not m:
        return (0, 0)
    return (int(m.group(1)), int(m.group(2) or 0))


def _is_chat_model(name: str) -> bool:
    """排除生图 / 视频等非对话模型：claude-* 只能映射到文本对话模型。"""
    n = name.lower()
    return not any(k in n for k in ("image", "nano-banana", "imagen", "veo", "embedding", "aqa", "live"))


def _pick_family_model(family: str, models: list[str]) -> Optional[str]:
    """从内部模型清单里按家族偏好选最合适的模型（版本号大者优先）。"""
    chat = [m for m in models if _is_chat_model(m)]
    if not chat:
        return None

    def best(ms):
        return max(ms, key=_model_version)

    lites = [m for m in chat if "flash-lite" in m]
    flashes = [m for m in chat if "flash" in m and "lite" not in m]
    pros = [m for m in chat if "pro" in m]
    if family == "haiku":
        if lites:
            return best(lites)
        return best(flashes or chat)
    if family == "opus":
        if pros:
            return best(pros)
        return best(flashes or chat)
    # sonnet / 其它 claude-*：优先普通 Flash，其次 Flash-Lite，最后 Pro
    if flashes:
        return best(flashes)
    if lites:
        return best(lites)
    return best(pros or chat)


def resolve_claude_model(raw_model: str, available: Optional[list] = None) -> tuple[str, str]:
    """解析请求模型名 → (display_model, internal_model)。

    - 非 claude-* 名字（gemini-*、带 -search / 抗截断后缀的）原样透传，
      Claude 客户端可直接使用 Gemini 模型及其全部后缀变体；
    - claude-*：静态别名表 → 去日期后缀 / 去 -latest 再试 → 家族启发
      （opus→Pro、haiku→Flash-Lite、其余→Flash），内部清单可用时优先
      在清单里挑同家族最新型号（vertexModels.json 会热刷新，静态表
      允许滞后）。
    """
    name = (raw_model or "").strip()
    lowered = name.lower()
    if not lowered.startswith("claude"):
        return name, name  # 透传

    candidates = [lowered]
    stripped = re.sub(r"-\d{6}$", "", lowered)  # 形如 -20250929 的日期后缀
    if stripped != lowered:
        candidates.append(stripped)
    if stripped.endswith("-latest"):
        candidates.append(stripped[: -len("-latest")])

    static_hit = None
    for cand in candidates:
        if cand in _CLAUDE_MODEL_ALIASES:
            static_hit = _CLAUDE_MODEL_ALIASES[cand]
            break

    available = [m for m in (available or []) if isinstance(m, str)]
    if static_hit and (not available or static_hit in available):
        return name, static_hit

    family = "opus" if "opus" in lowered else ("haiku" if "haiku" in lowered else "sonnet")
    if available:
        dynamic = _pick_family_model(family, available)
        if dynamic:
            return name, dynamic
    # 清单不可用 / 家族未命中：静态映射（若有）优先于家族兜底
    return name, static_hit or _CLAUDE_FAMILY_FALLBACK[family]


# ============================================================
# 请求转换：Anthropic → 内部 OpenAIRequest
# ============================================================

def _block_dict(block: Any, label: str) -> dict:
    if isinstance(block, dict):
        return block
    if hasattr(block, "model_dump"):  # 已被 pydantic 归一的对象
        dumped = block.model_dump()
        if isinstance(dumped, dict):
            return dumped
    raise ClaudeRequestError(f"{label} must be an object")


def _instruction_text(content: Any, label: str) -> str:
    """system 参数 / system、developer 消息的文本提取（string 或 text 块数组）。"""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        texts = []
        for i, block in enumerate(content):
            block = _block_dict(block, f"{label}[{i}]")
            if block.get("type") != "text":
                raise ClaudeRequestError(f"{label}[{i}] has unsupported type {block.get('type')!r}")
            text = block.get("text")
            if text is None:  # 仅 cache_control 的占位块
                continue
            if not isinstance(text, str):
                raise ClaudeRequestError(f"{label}[{i}].text must be a string")
            texts.append(text)
        return "\n".join(texts)
    raise ClaudeRequestError(f"{label} must be a string or an array of content blocks")


def _strip_cache_control(content: Any) -> Any:
    """复杂 tool_result 内容透传前剥离 cache_control（兑现全程剥离承诺）。

    Anthropic 客户端会在 tool_result 的子块上打 cache_control 缓存标记；
    原样透传会以 JSON 形式混进 Gemini functionResponse，模型侧看到无意义
    的杂键。text/image 子块的 cache_control 只在顶层，一层剥离即可；
    非 dict 子块（字符串等）原样保留。
    """
    if not isinstance(content, list):
        return content
    cleaned: list[Any] = []
    for block in content:
        if isinstance(block, dict) and "cache_control" in block:
            block = {k: v for k, v in block.items() if k != "cache_control"}
        cleaned.append(block)
    return cleaned


def _tool_result_payload(content: Any, label: str) -> Any:
    """tool_result.content → 内部 tool 消息 content。

    纯文本块数组拼接为字符串（结构化 JSON 字符串会被下游还原成对象）；
    含图片等复杂块时剥离 cache_control 后整块透传为 JSON 值（与
    vertex-master 口径一致，Gemini functionResponse 以 {"result": [...]}
    形式可见，且不携带 Anthropic 专属的缓存标记键）。
    """
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        texts: list[str] = []
        text_only = True
        for block in content:
            block = _block_dict(block, f"{label}.content")
            if block.get("type") != "text":
                text_only = False
                continue
            text = block.get("text")
            if not isinstance(text, str):
                raise ClaudeRequestError(f"{label}.content text must be a string")
            texts.append(text)
        return "\n".join(texts) if text_only else _strip_cache_control(content)
    raise ClaudeRequestError(f"{label}.content must be a string or an array of content blocks")


def _append_user_blocks(label: str, content: list, openai_messages: list) -> None:
    """user 消息块数组 → 内部消息（text / image 累积，tool_result 独立成 tool 轮）。"""
    regular: list[dict] = []

    def flush():
        if not regular:
            return
        parts = [p for p in regular if p.get("type") != "text" or p.get("text")]
        regular.clear()
        if parts:
            openai_messages.append(OpenAIMessage(role="user", content=parts))

    for bi, block in enumerate(content):
        block_label = f"{label}.content[{bi}]"
        block = _block_dict(block, block_label)
        btype = block.get("type")
        if btype == "text":
            text = block.get("text")
            if not isinstance(text, str):
                raise ClaudeRequestError(f"{block_label}.text must be a string")
            regular.append({"type": "text", "text": text})
        elif btype == "image":
            source = block.get("source")
            if not isinstance(source, dict):
                raise ClaudeRequestError(f"{block_label}.source must be an object")
            stype = source.get("type")
            if stype == "base64":
                media_type, data = source.get("media_type"), source.get("data")
                if not isinstance(media_type, str) or not media_type or not isinstance(data, str) or not data:
                    raise ClaudeRequestError(f"{block_label}.source requires media_type and data")
                regular.append({"type": "image_url", "image_url": {"url": f"data:{media_type};base64,{data}"}})
            elif stype == "url":
                url = source.get("url")
                if not isinstance(url, str) or not url:
                    raise ClaudeRequestError(f"{block_label}.source.url is required")
                regular.append({"type": "image_url", "image_url": {"url": url}})
            else:
                raise ClaudeRequestError(f"{block_label}.source has unsupported type {stype!r}")
        elif btype == "tool_result":
            tool_use_id = block.get("tool_use_id")
            if not isinstance(tool_use_id, str) or not tool_use_id:
                raise ClaudeRequestError(f"{block_label} tool_result requires tool_use_id")
            is_error = block.get("is_error", False)
            if not isinstance(is_error, bool):
                raise ClaudeRequestError(f"{block_label}.is_error must be a boolean")
            result = _tool_result_payload(block.get("content"), block_label)
            if is_error:
                # Gemini FunctionResponse uses its error key to distinguish
                # execution failures from successful outputs. Preserve this
                # across both transports and the text compatibility fallback.
                try:
                    result = json.dumps({"error": result}, ensure_ascii=False, allow_nan=False)
                except (TypeError, ValueError, RecursionError):
                    raise ClaudeRequestError(f"{block_label}.content must contain valid JSON values") from None
            flush()  # 普通 blocks 与 tool_result 分属不同轮次
            openai_messages.append(OpenAIMessage(
                role="tool",
                content=result,
                tool_call_id=tool_use_id,
                extra_content={"anthropic": {"tool_result_is_error": True}} if is_error else None,
            ))
        else:
            raise ClaudeRequestError(f"{block_label} has unsupported type {btype!r}")
    flush()


def _append_assistant_blocks(label: str, content: list, openai_messages: list) -> None:
    """assistant 消息块数组 → 内部消息（text 拼接 + tool_calls；thinking 丢弃）。"""
    texts: list[str] = []
    tool_calls: list[dict] = []
    ordinary, order = [], []
    for bi, block in enumerate(content):
        block_label = f"{label}.content[{bi}]"
        block = _block_dict(block, block_label)
        btype = block.get("type")
        if btype == "text":
            text = block.get("text")
            if not isinstance(text, str):
                raise ClaudeRequestError(f"{block_label}.text must be a string")
            texts.append(text)
            order.append({"type": "ordinary", "index": len(ordinary)})
            ordinary.append(ordinary_part_metadata("text", text, None))
        elif btype == "tool_use":
            call_id = block.get("id")
            name = block.get("name")
            if not isinstance(call_id, str) or not call_id:
                raise ClaudeRequestError(f"{block_label} tool_use requires id")
            if not isinstance(name, str) or not name.strip():
                raise ClaudeRequestError(f"{block_label} tool_use requires name")
            args = block.get("input")
            if args is None:
                args = {}
            if not isinstance(args, dict):
                raise ClaudeRequestError(f"{block_label}.input must be an object")
            try:
                args_str = json.dumps(args, ensure_ascii=False, allow_nan=False)
            except (TypeError, ValueError, RecursionError):
                raise ClaudeRequestError(f"{block_label}.input must contain valid JSON values") from None
            order.append({"type": "tool_call", "index": len(tool_calls)})
            tool_calls.append({"id": call_id, "type": "function",
                               "function": {"name": name, "arguments": args_str}})
        elif btype in ("thinking", "redacted_thinking"):
            # Gemini 无法消费 Anthropic 思考签名；丢弃但保留相邻 text / tool_use
            continue
        else:
            raise ClaudeRequestError(f"{block_label} has unsupported type {btype!r}")
    text = "".join(texts)  # 多 text 块直接拼接（对齐 vertex-master）
    if not text and not tool_calls:
        return  # 仅含 thinking 块的历史轮：整条跳过
    openai_messages.append(OpenAIMessage(
        role="assistant",
        content=text or None,
        tool_calls=tool_calls or None,
        extra_content={"google": {"ordinary_parts": ordinary, "part_order": order}} if tool_calls else None,
    ))


def _convert_tools(raw_tools: Any) -> Optional[list[dict]]:
    if raw_tools is None:
        return None
    if not isinstance(raw_tools, list):
        raise ClaudeRequestError("tools must be an array")
    decls = []
    for ti, tool in enumerate(raw_tools):
        tool = _block_dict(tool, f"tools[{ti}]")
        name = tool.get("name")
        if not isinstance(name, str) or not name.strip():
            raise ClaudeRequestError(f"tools[{ti}].name must be a non-empty string")
        function: dict[str, Any] = {"name": name}
        description = tool.get("description")
        if description is not None and not isinstance(description, str):
            raise ClaudeRequestError(f"tools[{ti}].description must be a string")
        if isinstance(description, str) and description:
            function["description"] = description
        schema = tool.get("input_schema")
        if schema is not None and not isinstance(schema, dict):
            raise ClaudeRequestError(f"tools[{ti}].input_schema must be an object")
        function["parameters"] = schema if isinstance(schema, dict) and schema else {"type": "object", "properties": {}}
        decls.append({"type": "function", "function": function})
    return decls or None


def _convert_tool_choice(raw_choice: Any, tools: Optional[list[dict]]) -> Any:
    """Anthropic tool_choice → OpenAI tool_choice（auto/required/none/指定函数）。"""
    if raw_choice is None:
        return None
    if isinstance(raw_choice, str):
        raw_choice = {"type": raw_choice}
    if not isinstance(raw_choice, dict):
        raise ClaudeRequestError("tool_choice must be an object")
    ctype = raw_choice.get("type")
    if ctype == "auto":
        return "auto"
    if ctype == "any":
        return "required"
    if ctype == "none":
        return "none"
    if ctype == "tool":
        tool = raw_choice.get("tool") if isinstance(raw_choice.get("tool"), dict) else {}
        name = raw_choice.get("name") or tool.get("name")
        if not isinstance(name, str) or not name.strip():
            raise ClaudeRequestError("tool_choice of type 'tool' requires a name")
        if not tools:
            raise ClaudeRequestError("tool_choice references tools but none were provided")
        if not any((t.get("function") or {}).get("name") == name for t in tools):
            raise ClaudeRequestError(f"tool_choice references unknown function: {name}")
        return {"type": "function", "function": {"name": name}}
    raise ClaudeRequestError(f"unsupported tool_choice type {ctype!r}")


def _optional_number(body: dict, key: str) -> Optional[float]:
    value = body.get(key)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ClaudeRequestError(f"{key} must be a number")
    try:
        value = float(value)
    except OverflowError:
        raise ClaudeRequestError(f"{key} must be a finite number") from None
    if not math.isfinite(value):
        raise ClaudeRequestError(f"{key} must be a finite number")
    return value


def anthropic_to_openai_request(body: dict, internal_model: str,
                                require_max_tokens: bool = True) -> OpenAIRequest:
    """Anthropic Messages 请求体 → 内部 OpenAIRequest。

    max_tokens 的必填性由调用方决定（/v1/messages 必填，count_tokens 不要求）。
    解析 / 校验失败抛 ClaudeRequestError（400 invalid_request_error）。
    """
    if not isinstance(body, dict):
        raise ClaudeRequestError("request body must be a JSON object")

    # ---- max_tokens：类型校验（必填性在路由层）----
    max_tokens = body.get("max_tokens")
    if max_tokens is not None:
        if isinstance(max_tokens, bool) or not isinstance(max_tokens, (int, float)) \
                or (isinstance(max_tokens, float) and not float(max_tokens).is_integer()):
            raise ClaudeRequestError("max_tokens must be an integer >= 1")
        max_tokens = int(max_tokens)
        if max_tokens < 1:
            raise ClaudeRequestError("max_tokens must be an integer >= 1")
    elif require_max_tokens:
        raise ClaudeRequestError("max_tokens is required")

    # ---- thinking：budget_tokens → reasoning_effort ----
    extras: dict[str, Any] = {}
    # thinking.enabled/disabled 钉住档位：output_config.effort 不再覆盖
    # （对齐 vertex-master：Go 版 thinking 块后处理、整写覆盖 thinkingConfig；
    # adaptive 是合并语义——effort 设的档保留，故 effort 仍生效）
    thinking_pinned = False
    thinking = body.get("thinking")
    if thinking is not None:
        if not isinstance(thinking, dict):
            raise ClaudeRequestError("thinking must be an object")
        ttype = str(thinking.get("type") or "").strip().lower()
        if ttype == "enabled":
            budget = thinking.get("budget_tokens")
            valid = (isinstance(budget, (int, float)) and not isinstance(budget, bool)
                     and (not isinstance(budget, float) or budget.is_integer()))
            if not valid or int(budget) < 1024:
                raise ClaudeRequestError("thinking.budget_tokens must be an integer >= 1024")
            budget = int(budget)
            # Gemini 3.x 走 thinking_level：档位映射对齐 vertex-master
            # thinkingLevelForBudget（<=1024 → LOW，<=8192 → MEDIUM，其余 → HIGH）
            extras["reasoning_effort"] = "low" if budget <= 1024 else ("medium" if budget <= 8192 else "high")
            thinking_pinned = True
            # display 仅接受 summarized / omitted（对齐 vertex-master，其余 400）
            display = thinking.get("display")
            if display is not None:
                if not isinstance(display, str) or display.strip().lower() not in ("summarized", "omitted"):
                    raise ClaudeRequestError(f"unsupported thinking.display {display!r}")
        elif ttype == "disabled":
            extras["reasoning_effort"] = "off"
            thinking_pinned = True
        elif ttype == "adaptive":
            pass  # 交给控制台 / 模型默认档位动态思考（effort 仍可生效）
        else:
            raise ClaudeRequestError(f"unsupported thinking.type {ttype!r}")

    # ---- output_config.effort → reasoning_effort（仅在 thinking 未钉住时生效，对齐 vertex-master）----
    # 校验始终执行（vertex-master 在 anthropicToChatRequest 里先行校验），
    # 但 thinking.enabled/disabled 钉住档位后不再覆盖（Go 版后写覆盖的反向还原）。
    response_format = None
    output_config = body.get("output_config")
    if output_config is not None:
        if not isinstance(output_config, dict):
            raise ClaudeRequestError("output_config must be an object")
        output_format = output_config.get("format")
        if output_format is not None:
            if not isinstance(output_format, dict) or output_format.get("type") != "json_schema":
                raise ClaudeRequestError("output_config.format.type must be json_schema")
            schema = output_format.get("schema")
            try:
                validate_json_schema(schema, label="output_config.format.schema")
                sanitize_for_gemini(schema)
            except SchemaValidationError as exc:
                raise ClaudeRequestError(str(exc)) from None
            response_format = {"type": "json_schema", "json_schema": {"name": "response", "schema": schema}}
        effort = output_config.get("effort")
        if effort is not None:
            if not isinstance(effort, str) or not effort.strip():
                raise ClaudeRequestError("output_config.effort must be a non-empty string")
            normalized = effort.strip().lower()
            if normalized in ("low", "medium", "high"):
                effort_value: Optional[str] = normalized
            elif normalized in ("xhigh", "max"):
                effort_value = "high"  # 内部档位上限为 high
            elif normalized in ("auto", "default"):
                effort_value = None  # 让模型/控制台自主决定思考强度
            else:
                raise ClaudeRequestError(f"unsupported output_config.effort {normalized!r}")
            if not thinking_pinned:
                extras["reasoning_effort"] = effort_value

    # ---- 顶层 system ----
    openai_messages: list[OpenAIMessage] = []
    system_text = _instruction_text(body.get("system"), "system")
    if system_text:
        openai_messages.append(OpenAIMessage(role="system", content=system_text))

    # ---- messages ----
    raw_messages = body.get("messages")
    if not isinstance(raw_messages, list) or not raw_messages:
        raise ClaudeRequestError("messages must be a non-empty array")
    seen_turn = False
    for mi, message in enumerate(raw_messages):
        label = f"messages[{mi}]"
        message = _block_dict(message, label)
        role = message.get("role")
        content = message.get("content")
        if role in ("system", "developer"):
            # 非标准但容忍：会话中途的 system 降级为 user 轮（保留新近性，
            # 对齐 vertex-master 的 queued steering 处理）
            text = _instruction_text(content, f"{label}.content")
            if not text:
                continue
            openai_messages.append(OpenAIMessage(
                role="user" if seen_turn else "system", content=text))
            continue
        if role not in ("user", "assistant"):
            raise ClaudeRequestError(f"{label}.role must be 'user' or 'assistant'")
        seen_turn = True
        if isinstance(content, str):
            openai_messages.append(OpenAIMessage(role=role, content=content))
            continue
        if content is None:
            raise ClaudeRequestError(f"{label}.content is required")
        if not isinstance(content, list):
            raise ClaudeRequestError(f"{label}.content must be a string or an array of content blocks")
        if role == "user":
            _append_user_blocks(label, content, openai_messages)
        else:
            _append_assistant_blocks(label, content, openai_messages)
    if not openai_messages:
        raise ClaudeRequestError("messages did not contain supported content")

    # ---- tools / tool_choice ----
    tools = _convert_tools(body.get("tools"))
    tool_choice = _convert_tool_choice(body.get("tool_choice"), tools)
    if tool_choice == "required" and not tools:
        raise ClaudeRequestError("tool_choice 'any' requires at least one tool")

    # ---- stop_sequences ----
    stop_sequences = body.get("stop_sequences")
    if stop_sequences is not None:
        if not isinstance(stop_sequences, list):
            raise ClaudeRequestError("stop_sequences must be an array of strings")
        for si, s in enumerate(stop_sequences):
            if not isinstance(s, str):
                raise ClaudeRequestError(f"stop_sequences[{si}] must be a string")

    top_k = body.get("top_k")
    if top_k is not None:
        if isinstance(top_k, bool) or not isinstance(top_k, int):
            raise ClaudeRequestError("top_k must be an integer")

    stream_flag = body.get("stream")
    stream = stream_flag is True or (isinstance(stream_flag, str) and stream_flag.strip().lower() == "true")

    return OpenAIRequest(
        model=internal_model,
        messages=openai_messages,
        stream=stream,
        max_tokens=max_tokens,
        temperature=_optional_number(body, "temperature"),
        top_p=_optional_number(body, "top_p"),
        top_k=top_k,
        stop=stop_sequences or None,
        response_format=response_format,
        tools=tools,
        tool_choice=tool_choice,
        # Claude 协议的用量在 message_delta 里给出：内部流式开启 include_usage
        # 让上游在流末尾附带一个 usage-only 块（Express 真流式与假流式均支持）。
        stream_options={"include_usage": True} if stream else None,
        reasoning_effort=extras.get("reasoning_effort"),
    )


# ============================================================
# 非流式响应转换：内部 OpenAI completion dict → Anthropic message
# ============================================================

def anthropic_omit_thinking(body: dict) -> bool:
    """thinking.display == "omitted" → 响应侧不回传思考块。

    对齐 vertex-master 的 includeThoughts = display != "omitted"：
    - omitted：模型照常思考（档位不变、reasoning_tokens 照计），
      但思考文本不下发给客户端；
    - summarized：Gemini 无法摘要思考，按完整回传（与 vertex-master
      同口径的有意偏差，请求侧校验已放行）。
    请求侧已保证 display 合法，这里仍做形状防御（不抛错、不压制）。
    """
    thinking = body.get("thinking")
    if isinstance(thinking, dict):
        display = thinking.get("display")
        return isinstance(display, str) and display.strip().lower() == "omitted"
    return False


def anthropic_stop_reason(finish_reason: Optional[str], has_tool_calls: bool) -> str:
    """OpenAI finish_reason → Anthropic stop_reason。"""
    if finish_reason == "length":
        return "max_tokens"
    if finish_reason == "content_filter":
        return "refusal"
    if has_tool_calls or finish_reason == "tool_calls":
        return "tool_use"
    return "end_turn"


def _parse_tool_arguments(raw: Any) -> dict:
    """Validate upstream arguments without inventing an executable replacement."""
    try:
        return parse_tool_arguments(raw)
    except SchemaValidationError:
        raise IncompleteResponseError("上游工具参数不是有效的 JSON 对象。") from None


def _tool_use_block(call):
    if not isinstance(call, dict) or not isinstance(call.get("function"), dict):
        raise IncompleteResponseError("上游工具调用缺少 function 对象。")
    function = call["function"]
    name = function.get("name")
    ident = call.get("id")
    if not isinstance(name, str) or not name.strip():
        raise IncompleteResponseError("上游工具调用缺少函数名称。")
    if ident is not None and not isinstance(ident, str):
        raise IncompleteResponseError("上游工具调用 ID 必须是字符串。")
    return {"type": "tool_use", "id": ident or new_tool_id(), "name": name,
            "input": _parse_tool_arguments(function.get("arguments"))}


def _ordered_completion_blocks(message, tools, omit_thinking):
    """Use exact bridge topology only when it covers the entire visible message."""
    metadata = google_metadata(message)
    ordinary, order = metadata.get("ordinary_parts"), metadata.get("part_order")
    if not isinstance(ordinary, list) or not isinstance(order, list):
        return None
    blocks, texts, thoughts = [], [], []
    for item in ordinary:
        if not isinstance(item, dict) or not isinstance(item.get("text"), str):
            return None
        kind, text = item.get("kind"), item["text"]
        if kind == "text":
            texts.append(text)
            blocks.append({"type": "text", "text": text} if text else None)
        elif kind == "thought":
            thoughts.append(text)
            blocks.append({"type": "thinking", "thinking": text, "signature": thinking_signature(text)}
                          if text and not omit_thinking else None)
        elif kind == "signature_only":
            blocks.append(None)
        else:
            return None  # Image/text extensions stay intact in the fallback.
    if ("".join(texts) != (message.get("content") or "")
            or "".join(thoughts) != (message.get("reasoning_content") or "")):
        return None
    sources = {"ordinary": blocks, "tool_call": tools}
    seen, result = set(), []
    for item in order:
        if not isinstance(item, dict):
            return None
        kind, index = item.get("type"), item.get("index")
        if (not isinstance(kind, str) or kind not in sources or type(index) is not int
                or not 0 <= index < len(sources[kind]) or (kind, index) in seen):
            return None
        seen.add((kind, index))
        block = sources[kind][index]
        if block is not None:
            result.append(block)
    return result if len(seen) == len(blocks) + len(tools) else None


def _anthropic_usage(usage: Any) -> dict:
    usage = usage if isinstance(usage, dict) else {}

    def pick(*keys: str) -> int:
        for key in keys:
            value = usage.get(key)
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                try:
                    return max(0, int(value))
                except (ValueError, OverflowError):
                    continue
        return 0

    def detail(field: str, key: str) -> int:
        block = usage.get(field)
        if isinstance(block, dict):
            value = block.get(key)
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                try:
                    return max(0, int(value))
                except (ValueError, OverflowError):
                    return 0
        return 0

    total_input = pick("prompt_tokens", "input_tokens")
    cached_input = min(total_input, detail("prompt_tokens_details", "cached_tokens"))
    out = {
        # OpenAI prompt_tokens includes cache hits; Anthropic's input_tokens
        # and cache_read_input_tokens are disjoint components of total input.
        "input_tokens": total_input - cached_input,
        "output_tokens": pick("completion_tokens", "output_tokens"),
        "cache_creation_input_tokens": 0,
        "cache_read_input_tokens": cached_input,
    }
    thinking = detail("completion_tokens_details", "reasoning_tokens")
    if thinking:
        out["output_tokens_details"] = {"thinking_tokens": thinking}
    return out


def openai_completion_to_anthropic(payload: dict, display_model: str, message_id: str,
                                   omit_thinking: bool = False) -> dict:
    """内部非流式 OpenAI 响应 → Anthropic message JSON。

    omit_thinking=True（thinking.display=omitted）：跳过 thinking 块，
    正文 / 工具 / 用量不受影响（reasoning_tokens 仍计入 usage）。
    """
    payload = payload if isinstance(payload, dict) else {}
    choices = payload.get("choices") or []
    choice = choices[0] if choices and isinstance(choices[0], dict) else {}
    message = choice.get("message") if isinstance(choice.get("message"), dict) else {}

    content_blocks: list[dict] = []
    reasoning = message.get("reasoning_content")
    if isinstance(reasoning, str) and reasoning and not omit_thinking:
        content_blocks.append({
            "type": "thinking", "thinking": reasoning, "signature": thinking_signature(reasoning),
        })
    text = message.get("content")
    tool_calls = message.get("tool_calls") or []
    if isinstance(text, str) and text:
        content_blocks.append({"type": "text", "text": text})
    elif not tool_calls:
        # 无工具调用时 Anthropic 必须至少一个 text 块（可为空串）
        content_blocks.append({"type": "text", "text": text if isinstance(text, str) else ""})
    if not isinstance(tool_calls, list):
        raise IncompleteResponseError("上游 tool_calls 必须是数组。")
    tool_blocks = [_tool_use_block(call) for call in tool_calls]
    content_blocks.extend(tool_blocks)
    ordered = _ordered_completion_blocks(message, tool_blocks, omit_thinking)
    if ordered:
        content_blocks = ordered

    return {
        "id": message_id,
        "type": "message",
        "role": "assistant",
        "model": display_model,
        "content": content_blocks,
        "stop_reason": anthropic_stop_reason(choice.get("finish_reason"), bool(tool_calls)),
        "stop_sequence": None,
        "usage": _anthropic_usage(payload.get("usage")),
    }


# ============================================================
# 流式转换：内部 OpenAI SSE chunk → Anthropic SSE 事件
# ============================================================

def _sse_event(name: str, data: dict) -> str:
    return f"event: {name}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


PING_EVENT = _sse_event("ping", {"type": "ping"})


def error_event(err_type: str, message: str) -> str:
    return _sse_event("error", {"type": "error", "error": {"type": err_type, "message": message}})


MAX_TOOL_STREAM_BYTES = 32 * 1024 * 1024
MAX_STREAM_TOOL_CALLS = 1024


class ClaudeStreamState:
    """把内部 OpenAI 流式 chunk 逐帧翻译为 Anthropic SSE 事件序列。

    事件流（对齐 Anthropic 官方流式协议 / vertex-master 状态机）：

        message_start
        [content_block_start → content_block_delta* → content_block_stop]*   每块
        message_delta(stop_reason + 最终 usage)
        message_stop

    - thinking 块收尾时先发 signature_delta（伪签，见 thinking_signature）；
      累积期用增量 SHA-256 逐帧 update（不再保存全文再整体哈希：
      长思考下旧实现 buf += 是 O(n²) 拷贝且驻留整段思考文本）；
    - tool_use 块延迟到 finish_events 统一发射：分片参数先累积（list 追加，
      收尾 join 一次，避免大参数多分片时的反复拷贝），再以一条
      完整 input_json_delta 下发（对齐 vertex-master「完整 JSON 单条 delta」
      口径）。上游同一帧可能同时携带 content + tool_calls（_build_chunk_payload
      对 text part 与 functionCall part 合帧），且抗截断模式下参数跨帧分片、
      与文本块交错；立即开块会在交错时引用已关闭的块索引（协议违规），
      延迟发射对任意交错顺序都严格合法——Anthropic 客户端也要等
      content_block_stop 之后才能执行工具，无实际时序损失；
    - omit_thinking=True（thinking.display=omitted）：思考增量直接丢弃，
      不开 thinking 块也不能伪签；模型仍思考，usage.reasoning_tokens 照计
      （对齐 vertex-master includeThoughts=false 的响应侧口径）；
    - 期间上游的心跳注释帧由外层转换为 ping 事件（约 10s 一个）。
    """

    def __init__(self, display_model: str, message_id: Optional[str] = None,
                 omit_thinking: bool = False):
        self.model = display_model
        self.id = message_id or new_message_id()
        self.omit_thinking = omit_thinking
        self._next_index = 0
        self._open: Optional[dict] = None       # 当前打开的块 {"kind","index",...}
        self._tools: dict[int, dict] = {}        # OpenAI tool index → {id,name,parts}（按到达序）
        self.usage: dict = {"input_tokens": 0, "output_tokens": 0}
        self._usage_fields: dict = {}
        self.finish: Optional[str] = None
        self.has_tools = False
        self._tool_ids = {}
        self._tool_bytes = 0
        self._finished = False

    # ---- 事件构造 ----
    def message_start_event(self) -> str:
        return _sse_event("message_start", {"type": "message_start", "message": {
            "id": self.id, "type": "message", "role": "assistant", "model": self.model,
            "content": [], "stop_reason": None, "stop_sequence": None,
            "usage": {"input_tokens": 0, "output_tokens": 0,
                      "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0},
        }})

    def _start_block(self, kind: str, content_block: dict) -> list[str]:
        events = self._close_block()
        index = self._next_index
        self._next_index += 1
        block: dict[str, Any] = {"kind": kind, "index": index}
        if kind == "thinking":
            # 增量伪签：逐帧 update，收尾取 hexdigest（与 thinking_signature 同值）
            block["hash"] = hashlib.sha256()
        self._open = block
        events.append(_sse_event("content_block_start", {
            "type": "content_block_start", "index": index, "content_block": content_block,
        }))
        return events

    def _close_block(self) -> list[str]:
        events: list[str] = []
        block, self._open = self._open, None
        if block is None:
            return events
        if block["kind"] == "thinking":
            # 收尾签名：整块思考的伪签，且必须在 content_block_stop 之前
            events.append(_sse_event("content_block_delta", {
                "type": "content_block_delta", "index": block["index"],
                "delta": {"type": "signature_delta", "signature": block["hash"].hexdigest()[:64]},
            }))
        events.append(_sse_event("content_block_stop", {
            "type": "content_block_stop", "index": block["index"],
        }))
        return events

    def _reserve_tool_bytes(self, value):
        try:
            size = len(value.encode("utf-8"))
        except UnicodeError:
            raise IncompleteResponseError("上游工具调用包含无效的 Unicode。") from None
        if self._tool_bytes + size > MAX_TOOL_STREAM_BYTES:
            raise IncompleteResponseError("上游流式工具调用超过缓冲限制。")
        self._tool_bytes += size

    # ---- 喂入 ----
    def feed_openai_chunk(self, payload: dict) -> list[str]:
        """单条 OpenAI chunk → Anthropic 事件。

        处理顺序固定为 思考 → 文本 → 工具（与 Gemini parts 的自然顺序一致：
        text part 在前、functionCall part 在后）。同一 delta 同时携带
        content 与 tool_calls 时（_build_chunk_payload 合帧），先发 text 块
        再累积工具，两者都不会丢失；finish_reason 恒被采集。
        """
        if self._finished:
            raise IncompleteResponseError("上游在消息结束后继续发送内容。")
        payload = payload if isinstance(payload, dict) else {}
        self.feed_usage(payload.get("usage"))
        choices = payload.get("choices") or []
        if not choices:
            return []
        choice = choices[0] if isinstance(choices[0], dict) else {}
        delta = choice.get("delta") if isinstance(choice.get("delta"), dict) else {}
        events: list[str] = []

        # 1) 思考增量（reasoning_content → thinking_delta）：逐帧喂增量哈希；
        #    omit_thinking（display=omitted）时直接丢弃，不开块、不哈希
        reasoning = delta.get("reasoning_content")
        if isinstance(reasoning, str) and reasoning and not self.omit_thinking:
            if not (self._open and self._open["kind"] == "thinking"):
                events += self._start_block("thinking", {
                    "type": "thinking", "thinking": "", "signature": "",
                })
            self._open["hash"].update(reasoning.encode("utf-8"))
            events.append(_sse_event("content_block_delta", {
                "type": "content_block_delta", "index": self._open["index"],
                "delta": {"type": "thinking_delta", "thinking": reasoning},
            }))

        # 2) 文本增量（content → text_delta）：先于工具处理
        content = delta.get("content")
        if isinstance(content, str) and content:
            if not (self._open and self._open["kind"] == "text"):
                events += self._start_block("text", {"type": "text", "text": ""})
            events.append(_sse_event("content_block_delta", {
                "type": "content_block_delta", "index": self._open["index"],
                "delta": {"type": "text_delta", "text": content},
            }))

        # 3) 工具调用：分片参数只累积，块事件延迟到 finish_events
        #    （交错场景下立即开块会引用已关闭的块索引，见类 docstring）
        tool_calls = delta.get("tool_calls")
        if tool_calls is not None and not isinstance(tool_calls, list):
            raise IncompleteResponseError("上游流式 tool_calls 必须是数组。")
        if isinstance(tool_calls, list) and tool_calls:
            for tc in tool_calls:
                if not isinstance(tc, dict):
                    raise IncompleteResponseError("上游流式工具调用必须是对象。")
                function = tc.get("function")
                if function is None:
                    function = {}
                if not isinstance(function, dict):
                    raise IncompleteResponseError("上游流式 function 必须是对象。")
                tc_index = tc.get("index", 0)
                if type(tc_index) is not int or tc_index < 0:
                    raise IncompleteResponseError("上游工具调用索引必须是非负整数。")
                known = self._tools.get(tc_index)
                if known is None:
                    if len(self._tools) >= MAX_STREAM_TOOL_CALLS:
                        raise IncompleteResponseError("上游流式工具调用数量超过限制。")
                    known = {"id": None, "name": "", "parts": []}
                    self._tools[tc_index] = known
                tc_id, name = tc.get("id"), function.get("name")
                if tc_id is not None and not isinstance(tc_id, str):
                    raise IncompleteResponseError("上游工具调用 ID 必须是字符串。")
                if tc_id:
                    if ((known["id"] and known["id"] != tc_id)
                            or self._tool_ids.get(tc_id, tc_index) != tc_index):
                        raise IncompleteResponseError("上游流式工具调用 ID 与索引不一致。")
                    # Late IDs fill the existing slot without resetting arguments.
                    if known["id"] is None:
                        self._reserve_tool_bytes(tc_id)
                    known["id"] = tc_id
                    self._tool_ids[tc_id] = tc_index
                if name is not None and not isinstance(name, str):
                    raise IncompleteResponseError("上游工具函数名称必须是字符串。")
                if name:
                    if known["name"] and known["name"] != name:
                        raise IncompleteResponseError("同一流式工具调用的函数名称发生变化。")
                    if not known["name"]:
                        self._reserve_tool_bytes(name)
                    known["name"] = name
                arguments = function.get("arguments")
                if arguments is not None and not isinstance(arguments, str):
                    raise IncompleteResponseError("上游流式工具参数必须是 JSON 字符串分片。")
                if arguments:
                    self._reserve_tool_bytes(arguments)
                    known["parts"].append(arguments)
            self.has_tools = True

        # 4) 结束原因
        finish = choice.get("finish_reason")
        if isinstance(finish, str) and finish:
            self.finish = finish
        return events

    def feed_usage(self, usage: Any) -> None:
        if not isinstance(usage, dict):
            return
        # Usage chunks are cumulative snapshots, but an update may contain
        # only output tokens. Missing input/cache counters must not become 0.
        for canonical, alias in (("prompt_tokens", "input_tokens"), ("completion_tokens", "output_tokens")):
            value = usage.get(canonical, usage.get(alias))
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                try:
                    self._usage_fields[canonical] = max(0, int(value))
                except (ValueError, OverflowError):
                    pass
        for field, key in (("prompt_tokens_details", "cached_tokens"),
                           ("completion_tokens_details", "reasoning_tokens")):
            detail = usage.get(field)
            value = detail.get(key) if isinstance(detail, dict) else None
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                try:
                    self._usage_fields[field] = {key: max(0, int(value))}
                except (ValueError, OverflowError):
                    pass
        self.usage = _anthropic_usage(self._usage_fields)

    # ---- 收尾 ----
    def finish_events(self) -> list[str]:
        if self._finished:
            return []
        # Validate all calls before emitting any executable tool block. Truncated
        # streams retain their raw partial JSON and their max_tokens/refusal stop.
        calls = []
        for known in self._tools.values():
            if not known["name"].strip():
                raise IncompleteResponseError("上游流式工具调用缺少函数名称。")
            args = "".join(known["parts"]).strip() or "{}"
            if self.finish not in ("length", "content_filter"):
                _parse_tool_arguments(args)
            calls.append((known, args))
        self._finished = True
        events = self._close_block()
        # 延迟发射的全部 tool_use 块：start → 单条完整 input_json_delta → stop
        for known, args in calls:
            index = self._next_index
            self._next_index += 1
            events.append(_sse_event("content_block_start", {
                "type": "content_block_start", "index": index,
                "content_block": {"type": "tool_use", "id": known["id"] or new_tool_id(),
                                  "name": known["name"], "input": {}},
            }))
            events.append(_sse_event("content_block_delta", {
                "type": "content_block_delta", "index": index,
                "delta": {"type": "input_json_delta", "partial_json": args},
            }))
            events.append(_sse_event("content_block_stop", {
                "type": "content_block_stop", "index": index,
            }))
        self._tools.clear()
        self._tool_ids.clear()
        self._tool_bytes = 0
        final_usage = dict(self.usage)
        final_usage.setdefault("cache_creation_input_tokens", 0)
        final_usage.setdefault("cache_read_input_tokens", 0)
        events.append(_sse_event("message_delta", {
            "type": "message_delta",
            "delta": {"stop_reason": anthropic_stop_reason(self.finish, self.has_tools),
                      "stop_sequence": None},
            "usage": final_usage,
        }))
        events.append(_sse_event("message_stop", {"type": "message_stop"}))
        return events


# ============================================================
# count_tokens：无本地精确分词器的粗略估算
# ============================================================

# CJK 字符区间（半角假名补充 / 假名 / 扩展 A / 基本汉字 / 谚文 / 兼容 / 全角形态）。
# 预编译正则一次性匹配全部区间：逐字符 any(lo<=code<=hi) 循环在 40 万字符
# 的大上下文上要 300ms+（Claude Code 每轮都调 count_tokens），正则只需 ~10ms。
_CJK_RE = re.compile(
    "[\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff\uac00-\ud7af\uf900-\ufaff\uff00-\uffef]")


def estimate_text_tokens(text: str) -> int:
    """CJK ≈ 1 token/字，其余 ≈ 4 字符/token 的启发式估算（正则批量计数）。"""
    if not text:
        return 0
    cjk = len(_CJK_RE.findall(text))
    return max(1, round(cjk + (len(text) - cjk) / 4))


def estimate_request_tokens(body: dict) -> int:
    """按 Anthropic count_tokens 口径估算整个请求的输入 token 数。

    覆盖 system、消息文本、tool_use 入参、tool_result 文本与工具声明；
    base64 图片按数据长度 / 4 粗算。客户端（如 Claude Code 的上下文管理）
    只需要量级正确的值。
    """
    pieces: list[str] = []

    def add(value: Any):
        if isinstance(value, str) and value:
            pieces.append(value)

    system = body.get("system")
    if isinstance(system, str):
        add(system)
    elif isinstance(system, list):
        for block in system:
            if isinstance(block, dict):
                add(block.get("text"))

    for message in body.get("messages") or []:
        if not isinstance(message, dict):
            continue
        content = message.get("content")
        if isinstance(content, str):
            add(content)
            continue
        if not isinstance(content, list):
            continue
        for block in content:
            if not isinstance(block, dict):
                continue
            btype = block.get("type")
            if btype == "text":
                add(block.get("text"))
            elif btype == "tool_use":
                try:
                    add(json.dumps(block.get("input") or {}, ensure_ascii=False))
                except (TypeError, ValueError):
                    pass
            elif btype == "tool_result":
                inner = block.get("content")
                if isinstance(inner, str):
                    add(inner)
                elif isinstance(inner, list):
                    for sub in inner:
                        if isinstance(sub, dict) and sub.get("type") == "text":
                            add(sub.get("text"))
            elif btype == "image":
                source = block.get("source") or {}
                data = source.get("data") if isinstance(source, dict) else None
                if isinstance(data, str) and data:
                    pieces.append("x" * (len(data) // 4))

    for tool in body.get("tools") or []:
        if not isinstance(tool, dict):
            continue
        add(tool.get("name"))
        add(tool.get("description"))
        try:
            add(json.dumps(tool.get("input_schema") or {}, ensure_ascii=False))
        except (TypeError, ValueError):
            pass

    return estimate_text_tokens("\n".join(pieces))


# ============================================================
# Anthropic 格式模型目录（Anthropic 客户端访问 /v1/models 时切换格式）
# ============================================================

def anthropic_models_payload(available_models: list, catalog_variants: Optional[list] = None) -> dict:
    """Anthropic List Models 响应：claude 别名 + 内部 Gemini 模型并列展示。

    catalog_variants 为 None 时按旧口径直接列 available_models（单测直调）；
    生产路径由 claude_api.anthropic_models_data 传入与 OpenAI /v1/models
    同源展开的变体目录（-search / 已开启的抗截断后缀）——请求侧
    resolve_claude_model 对非 claude-* 名字原样透传，这些变体在 Claude
    协议下同样可用，目录应当如实反映。
    """
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    entries: list[dict] = []
    seen: set[str] = set()

    def add(model_id: str, display_name: str):
        if not model_id or model_id in seen:
            return
        seen.add(model_id)
        entries.append({"id": model_id, "display_name": display_name,
                         "type": "model", "created_at": now})

    for alias in CANONICAL_CLAUDE_MODELS:
        internal = _CLAUDE_MODEL_ALIASES.get(alias)
        suffix = f"（映射 {internal}）" if internal else ""
        add(alias, f"Claude 别名 {suffix}" if suffix else alias)
    for model in (catalog_variants if catalog_variants is not None else available_models):
        if isinstance(model, str):
            add(model, model)

    return {
        "data": entries,
        "first_id": entries[0]["id"] if entries else None,
        "has_more": False,
        "last_id": entries[-1]["id"] if entries else None,
    }
