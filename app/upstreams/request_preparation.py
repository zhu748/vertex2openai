"""两条 OpenAI 上游共用的消息注入与预填充处理。"""

import config
from models import OpenAIRequest
from prefill import (
    DEFAULT_IMAGE_PREFILL_NUDGE,
    apply_console_injection,
    apply_prefill_compat,
)
from runtime_state import app_state


def _prefill_template(user_template: str, is_image: bool) -> str:
    # 生图默认要求输出图片；用户自定义模板始终优先。
    return (user_template or "").strip() or (DEFAULT_IMAGE_PREFILL_NUDGE if is_image else "")


def _prefill_log(mode: str, prefill_text: str) -> str:
    length = len(prefill_text)
    if mode == "keep_turn":
        return (
            f"🩹 [预填充兼容] 预填充（{length} 字）保留为 model 轮次，其后补一句续写推动语；"
            "输出会把它拼回开头。"
        )
    if mode == "minimal":
        return (
            "🩹 [预填充兼容] 仅补占位 user 保证不报错；预填充**不会**拼回输出开头"
            "（预设的思考开标签可能因此缺失，酒馆正则会抓不到）。"
        )
    return (
        f"🩹 [预填充兼容] 预填充（{length} 字）已并入末尾 user 消息作为续写指令，并将拼回输出开头。"
        "若预填充停在半截词/半截标签且模型接不上，可试试「保留模型轮次」。"
    )


def prepare_messages(
    request: OpenAIRequest,
    profile: dict,
    settings: dict,
    *,
    has_tools: bool,
) -> tuple[OpenAIRequest, str, bool]:
    """先注入控制台消息，再兼容末尾预填充，返回请求、回拼文本与激活状态。

    has_tools 由通道判定：Cookie 还需检查历史中的函数往返，避免破坏调用顺序。
    只有消息变化时才复制请求，调用方持有的原始请求不受影响。
    """
    is_image = profile["is_image"]
    messages, notes = apply_console_injection(
        request.messages,
        system_text=settings.get("inject_system_instruction", ""),
        prefill_text=settings.get("inject_prefill", ""),
        has_tools=has_tools,
        is_image_model=is_image,
        allow_image_prefill=bool(settings.get("inject_prefill_for_image", False)),
    )
    for note in notes:
        print(note)
    if messages is not request.messages:
        request = request.model_copy(update={"messages": messages})

    mode = app_state.get_setting("prefill_mode", config.DEFAULT_SETTINGS["prefill_mode"])
    if mode == "off":
        return request, "", False

    messages, prefill_text, active = apply_prefill_compat(
        request.messages,
        mode,
        allow_model_last=not profile["requires_user_last_turn"],
        instruction_template=_prefill_template(settings.get("prefill_instruction", ""), is_image),
        cot_guard=bool(settings.get("prefill_cot_guard", True)) and not is_image,
    )
    if messages is not request.messages:
        request = request.model_copy(update={"messages": messages})
        print(_prefill_log(mode, prefill_text))
    elif prefill_text:
        print(
            f"🩹 [预填充兼容] 该模型支持 model 结尾，预填充原生透传（{len(prefill_text)} 字），模型将直接续写。"
        )
    else:
        print(
            "ℹ️ [预填充兼容] 未检测到预填充（请求最后一条不是 assistant 消息）。"
            "预设里的思维链指令属于 system/user 条目，不是预填充；"
            "若要用预设思维链顶掉原生思考，需在预设末尾放一条 assistant 条目（通常是思考块的开标签）。"
        )
    if active and app_state.get_setting("prefill_suppress_thinking", True):
        print("🧠 [预填充兼容] 已按模型压制原生思考（可在控制台关闭），让预设思维链接管。")
    return request, prefill_text, active
