from collections.abc import Callable

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr
from typing import Any, Literal

# Define data models
MAX_CANDIDATES = 8
class ImageUrl(BaseModel):
    url: str

class ContentPartImage(BaseModel):
    type: Literal["image_url"]
    image_url: ImageUrl

class ContentPartText(BaseModel):
    type: Literal["text"]
    text: str

def normalize_content_part(part: Any) -> Any:
    """把 message.content 里的单个 part 归一成普通 dict。

    F-1：`content` 的类型是 `list[ContentPartText | ContentPartImage | dict]`，
    pydantic 会优先把 `{"type": "text", ...}` 解析成 `ContentPartText` 实例，
    因此 `isinstance(p, dict)` 对标准 OpenAI 请求恒为 False——凡是靠这个判断
    筛选 part 的地方都会静默丢内容（分段 system、`--ar` 检测、markdown 图片抽取）。
    统一先过这个函数再按 dict 处理。
    """
    if isinstance(part, dict):
        return part
    if hasattr(part, "model_dump"):
        return part.model_dump()
    return part


class OpenAIMessage(BaseModel):
    _signature_slots: tuple[bytes, ...] = PrivateAttr(default=())
    _signature_scoped: bool = PrivateAttr(default=False)
    _omitted_signature_key: bytes | None = PrivateAttr(default=None)
    _claude_literal_tool_ids: bool = PrivateAttr(default=False)
    _opaque_tool_call_ids: bool = PrivateAttr(default=False)
    # Responses-only: None for omitted item IDs, empty bytes for an unusable
    # supplied vector, or a keyed digest of the complete ordered item IDs.
    _responses_item_witness: bytes | None = PrivateAttr(default=None)
    _signature_observed_reasoning: tuple[int, bytes] | None = PrivateAttr(default=None)
    role: str
    content: str | list[ContentPartText | ContentPartImage | dict[str, Any]] | None = None
    name: str | None = None
    tool_calls: list[dict[str, Any]] | None = None
    tool_call_id: str | None = None
    reasoning_content: str | None = None
    refusal: str | None = None
    # Google documents this OpenAI-compatible extension as the primary carrier
    # for thought signatures on messages and individual tool calls.
    extra_content: dict[str, Any] | None = None

    model_config = ConfigDict(extra='allow')

    def has_assistant_refusal(self) -> bool:
        # Refusal is an assistant content type, not a general text alias.
        return self.role in ("assistant", "model") and (
            bool(self.refusal) or isinstance(self.content, list) and any(
                isinstance(p, dict) and p.get("type") == "refusal" and isinstance(p.get("refusal"), str)
                and p["refusal"] for p in self.content
            )
        )

    def replay_content(self):
        """Represent OpenAI refusal history in Gemini's ordinary model parts, without mutating it."""
        if not self.has_assistant_refusal():
            return self.content
        if isinstance(self.content, list):
            parts = []
            refusals = []
            for p in self.content:
                if isinstance(p, dict) and p.get("type") == "refusal" and isinstance(p.get("refusal"), str):
                    parts.append({"type": "text", "text": p["refusal"]})
                    refusals.append(p["refusal"])
                else:
                    parts.append(p)
            # SDKs can mirror the same refusal in a scalar, one part, or a
            # sequence of parts. Keep genuinely different text exactly once.
            if (not self.refusal or self.refusal in refusals or self.refusal == "".join(refusals)
                    or any(message_text([p]) == self.refusal for p in parts)
                    or self.refusal == message_text(parts)):
                return parts
        elif not self.content or self.content == self.refusal:
            return self.refusal
        else:
            parts = [{"type": "text", "text": self.content}]
        return [*parts, {"type": "text", "text": self.refusal}]

class OpenAIRequest(BaseModel):
    _signature_output_prefix: bytes | None = PrivateAttr(default=None)
    _signature_origin_cutoff: int | None = PrivateAttr(default=None)
    _stage_signature_origin: Callable[
        [str, tuple[bytes, ...], tuple[bytes | None, ...] | None, bytes | None], None
    ] | None = PrivateAttr(default=None)
    _signature_output_destination: Callable[[bytes], None] | None = PrivateAttr(default=None)
    model: str
    messages: list[OpenAIMessage]
    # Legacy field accepted for compatibility; only an enabled model suffix
    # selects anti-truncation. This parameter no longer overrides that choice.
    anti_truncation_mode: Literal["off", "nonstream", "stream"] | None = None
    # 默认 None = 客户端未显式传入（便于区分“省略”与“显式 1.0”，从而应用控制台默认值/按模型剥离）
    temperature: float | None = None
    max_tokens: int | None = None
    max_completion_tokens: int | None = None  # 兼容最新版 OpenAI 客户端
    top_p: float | None = None
    top_k: int | None = None
    stream: bool | None = False
    # P1-3：OpenAI 允许 stop 是字符串或数组，旧定义只收数组，客户端发字符串会 422。
    stop: str | list[str] | None = None
    presence_penalty: float | None = None
    frequency_penalty: float | None = None
    seed: int | None = None
    # P1-3：OpenAI 的 logprobs 是 bool、top_logprobs 才是整数；
    # 这里两种都收下，由 create_generation_config 统一规范成 Gemini 的形状。
    logprobs: bool | int | None = None
    top_logprobs: int | None = None
    response_logprobs: bool | None = None
    response_format: dict[str, Any] | None = None # 兼容强制 JSON 格式化输出
    n: int | None = Field(default=None, ge=1, le=MAX_CANDIDATES, strict=True)
    tools: list[dict[str, Any]] | None = None
    tool_choice: str | dict[str, Any] | None = None
    # Keep absent controls out of existing resumable request fingerprints.
    parallel_tool_calls: bool | None = Field(default=None, strict=True, exclude_if=lambda v: v is None)
    # 流式用量开关（OpenAI: {"include_usage": true}）
    stream_options: dict[str, Any] | None = None

    # The server supplies this private callable; JSON extras cannot set or invoke it.
    _select_buffered_order: Callable[[dict], bool] | None = PrivateAttr(default=None)

    model_config = ConfigDict(extra='allow', allow_inf_nan=False)


def wants_json_output(request: OpenAIRequest) -> bool:
    return (request.response_format or {}).get("type") in ("json_object", "json_schema")


def message_text(content: Any) -> str:
    """从 OpenAIMessage.content（str 或分段 list）里提取纯文本。"""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for p in content:
            if isinstance(p, dict) and p.get("type") == "text":
                # `or ""`：{"type":"text","text":null} 过不了 pydantic 的
                # text:str 校验，会以原始 dict 留在 union 里；键存在但值为 None
                # 时 .get("text", "") 返回 None，拼进 "".join 直接 TypeError → 500。
                parts.append(p.get("text") or "")
            elif hasattr(p, "text") and isinstance(getattr(p, "text", None), str):
                parts.append(p.text)
        return "".join(parts)
    return ""


def file_data_markdown(file_uri: str, mime_type: str | None = None) -> str:
    """Readable OpenAI fallback for Gemini URI media; native paths keep fileData."""
    image = not mime_type or mime_type.startswith("image/")
    return f"{'!' if image else ''}[{'Image' if image else 'File'}]({file_uri})"
