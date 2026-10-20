"""消息注入、预填充兼容、思维链标签守卫与输出去重。"""

import re
from typing import List, Optional, Tuple

from models import OpenAIMessage, message_text, normalize_content_part


def _is_empty_message(msg: OpenAIMessage) -> bool:
    """判空口径：只把「无任何实质内容」的消息当空。

    message_text 只提取文本段，因此绝不能直接用它判空——否则「无文字
    说明、纯图片」的用户轮会被当成空消息，末尾图片轮随后被预填充兼容
    逻辑整条删除，上一条 assistant 历史回复被误当预填充（2026-10 修复）。
    """
    if getattr(msg, "tool_calls", None):
        return False
    content = msg.content
    if content is None:
        return True
    if isinstance(content, str):
        return not content.strip()
    if isinstance(content, list):
        for part in content:
            p = normalize_content_part(part)
            if isinstance(p, dict):
                if p.get("type") == "text":
                    if str(p.get("text", "")).strip():
                        return False
                elif p.get("type") or any(p.values()):
                    # 任何非文本载体（图片/音频/未知类型/带内容 dict）都不是空消息
                    return False
        return True
    return True


DEFAULT_PREFILL_INSTRUCTION = (
    "[继续输出] 下面是你这条回复已经写好的开头，请从断点处无缝继续，"
    "不要重复开头内容，也不要添加任何前言、解释或标注："
)


DEFAULT_KEEP_TURN_NUDGE = "[继续] 从你上一条的断点处无缝往下写，不要重复已写内容，不要任何前言或解释。"


DEFAULT_IMAGE_PREFILL_NUDGE = "[继续] 按上面说的风格与要求，直接输出图片本身，不要输出任何文字描述或字符画。"


def apply_console_injection(
    messages: List[OpenAIMessage],
    system_text: str = "",
    prefill_text: str = "",
    has_tools: bool = False,
    is_image_model: bool = False,
    allow_image_prefill: bool = False,
) -> Tuple[List[OpenAIMessage], List[str]]:
    """把控制台配置的 system 指令 / 预填充注入到消息里。

    面向 RikkaHub 这类轻量前端：它们没有酒馆的预设系统，尤其**从不发送
    assistant 预填充**，因此在这些前端下预填充这个杠杆完全用不上，
    "预填充时压制原生思考"也永远不会触发。

    注入发生在 `apply_prefill_compat` **之前**，注入完的消息与"前端自己发了
    预填充"完全同形，下游四种兼容模式原样复用，不引入新分支。

    四条护栏（缺一个就会和现有功能打架）：
      1. 客户端已经发了预填充 → 不注入，否则两段预填充叠一起（酒馆场景）；
      2. 请求带 tools → 不注入，凭空多一个 model 轮次会打乱函数调用往返；
      3. 生图模型 → 默认不注入预填充，除非 allow_image_prefill=True。
         预填充对生图确有很强的引导力（实测：同一句"画一只猫"，预填充承诺
         "纯黑白钢笔线稿"就真的输出线稿，不加则是彩色写实照片），但角色扮演
         用的预填充落到生图请求上会让模型改吐文本，所以做成开关而非默认放行；
      4. 两个字段都留空 → 整个函数是空操作。

    返回 (新消息列表, 说明做了什么的日志行列表)。
    """
    notes: List[str] = []
    system_text = (system_text or "").strip()
    prefill_text = (prefill_text or "").strip()
    if not system_text and not prefill_text:
        return messages, notes

    new_msgs = list(messages or [])

    if system_text:
        # 追加在客户端 system 之后：越靠后越不容易被前面的内容淹没，
        # 也保证前端自己的系统提示仍然在场。
        insert_at = 0
        for i, m in enumerate(new_msgs):
            if m.role == "system":
                insert_at = i + 1
        new_msgs.insert(insert_at, OpenAIMessage(role="system", content=system_text))
        notes.append(f"💉 [控制台注入] 已追加 system 指令（{len(system_text)} 字）。")

    if prefill_text:
        if has_tools:
            notes.append("💉 [控制台注入] 请求带函数调用，已跳过预填充注入（避免打乱工具往返）。")
        elif is_image_model and not allow_image_prefill:
            notes.append(
                "💉 [控制台注入] 生图模型，已跳过预填充注入"
                "（如需用预填充引导画风，请在控制台打开「生图也注入预填充」）。"
            )
        else:
            idx = len(new_msgs) - 1
            while idx >= 0 and _is_empty_message(new_msgs[idx]):
                idx -= 1
            client_has_prefill = (
                idx >= 0
                and new_msgs[idx].role == "assistant"
                and not getattr(new_msgs[idx], "tool_calls", None)
            )
            if client_has_prefill:
                notes.append("💉 [控制台注入] 客户端已自带预填充，跳过注入（不覆盖前端预设）。")
            else:
                new_msgs = new_msgs[: idx + 1]
                new_msgs.append(OpenAIMessage(role="assistant", content=prefill_text))
                notes.append(
                    f"💉 [控制台注入] 已注入预填充（{len(prefill_text)} 字），下面按预填充兼容模式处理。"
                )

    return new_msgs, notes


_TAG_OPEN_RE = re.compile(r"<([A-Za-z][A-Za-z0-9_\-~.:]*)\s*>")


def detect_unclosed_tag(text: str) -> Optional[str]:
    """找出文本里"开了但没闭合"的最后一个标签名，没有则返回 None。

    预填充卡思维链的典型形态就是停在一个**未闭合的开标签**上（标签名随预设而定）：
    模型接着写的内容理应落在标签内部（即思维链），写完再闭合、然后写正文。
    """
    if not text:
        return None
    last: Optional[str] = None
    for m in _TAG_OPEN_RE.finditer(text):
        name = m.group(1)
        if re.search(rf"</\s*{re.escape(name)}\s*>", text[m.end() :]):
            continue  # 这个标签在后面闭合了，不算
        last = name
    return last


def build_cot_guard(tag: str) -> str:
    """生成"必须先写完思维链再写正文"的强化要求（思维链守卫）。

    为什么需要它：预填充只是把话头停在开标签上，**没有任何一句话告诉模型
    "接下来必须先完成思维链"**。实测里模型经常直接跨过思考写正文，
    结果输出里只有一个孤零零的开标签、没有思考内容也没有闭合标签，
    前端按正则去抓思维链就抓不到（用户报告：多数情况没有思维链）。
    这段守卫把隐含约定写成显式指令，并点名那个具体标签。
    """
    return (
        f"\n\n（格式硬性要求）你的这条回复当前停在 <{tag}> 内部：请**先**在 <{tag}> 里"
        f"逐条写完该标签要求的全部思考内容，写完后用 </{tag}> 闭合，"
        f"**然后才**开始写正文。不允许跳过思考直接写正文，也不允许只写一个空标签。"
    )


def apply_prefill_compat(
    messages: List[OpenAIMessage],
    mode: str = "smart",
    allow_model_last: bool = False,
    instruction_template: str = "",
    cot_guard: bool = False,
) -> Tuple[List[OpenAIMessage], str, bool]:
    """
    预填充(prefill)兼容：Gemini 3.x 拒绝以 assistant/model 结尾的请求（400）。

    - mode="off"：不处理（可能 400）。
    - mode="minimal"：末尾非 user 时追加一个占位 user，仅保证不报错（不还原预填充）。
    - mode="smart"：
      * allow_model_last=True（模型允许以 model 轮次结尾，如 2.5 及更早）→ **原生透传**：
        消息保持原样发给上游，模型直接续写末尾轮次，最忠实；
      * 否则（3.x 等）→ 把末尾 assistant 预填充取出，转成末尾 user 的“续写指令”
        （模板可用 instruction_template 自定义，留空用内置默认）。
    - mode="keep_turn"（3.x 上比 smart 更贴合原意）：
      **保留** assistant 预填充作为 model 轮次，只在其后补一句极短的 user 推动语。
      3.x 拒绝的是“以 model 轮次**结尾**”，并不禁止 model 轮次出现在中间。
      smart 把预填充塞进 user 消息，模型于是把自己写的话当成“用户给的参考文本”，
      倾向另起一句；keep_turn 让预填充留在模型自己的声音里，续写是逐字接续的。
      预填充的主要用途是用预设自带的思维链顶掉原生思维链，此时预填充往往是
      `<thinking>` 这类**未闭合的开头**——它必须处在 model 轮次里，模型才会
      当作“自己已经写了一半”继续填充，而不是当成用户贴来的样例。
      2.5 系仍走原生透传，不受影响。
      以上模式都返回 prefill 文本，由上游把它拼回输出开头（配合去重）。

    返回 (处理后的消息列表, 需拼回输出开头的预填充文本, 是否检测到预填充)。
    第三项供“预填充时压制原生思考”等联动逻辑使用。
    与模型名无关：按请求形状 + 能力档案触发；新加模型 ID 自动生效。
    """
    if not messages or mode == "off":
        return messages, "", False

    # 找到最后一条“非空”消息
    idx = len(messages) - 1
    while idx >= 0 and _is_empty_message(messages[idx]):
        idx -= 1
    if idx < 0:
        return messages, "", False

    last = messages[idx]
    if last.role != "assistant" or getattr(last, "tool_calls", None):
        return messages, "", False  # 已是 user 结尾 / 末尾是工具调用 → 无需处理

    prefill = message_text(last.content).strip()
    if not prefill:
        return messages, "", False

    if mode == "minimal":
        new_msgs = list(messages)
        new_msgs.append(OpenAIMessage(role="user", content="(请继续)"))
        return new_msgs, "", True

    # 模型支持 model 结尾 → 原生预填充透传（不改消息，模型直接续写）
    if allow_model_last:
        return messages, prefill, True

    if mode == "keep_turn":
        # 保留预填充所在的 assistant 轮次，只补一句极短 user 推动语。
        # 必须截到 idx+1：预填充后面可能还跟着空消息，带上它们会再次以非 user 结尾。
        new_msgs = list(messages[: idx + 1])
        nudge = (instruction_template or "").strip() or DEFAULT_KEEP_TURN_NUDGE
        if cot_guard:
            tag = detect_unclosed_tag(prefill)
            if tag:
                nudge += build_cot_guard(tag)
        new_msgs.append(OpenAIMessage(role="user", content=nudge))
        return new_msgs, prefill, True

    # smart：丢弃末尾预填充 assistant（及其后的空消息），转成续写指令
    new_msgs = list(messages[:idx])
    intro = (instruction_template or "").strip() or DEFAULT_PREFILL_INSTRUCTION
    instruction = intro + "\n\n" + prefill
    if cot_guard:
        tag = detect_unclosed_tag(prefill)
        if tag:
            # 守卫放在预填充之后 = 模型最后读到的就是这条硬性要求
            instruction += build_cot_guard(tag)
    if new_msgs and new_msgs[-1].role == "user" and isinstance(new_msgs[-1].content, str):
        merged = new_msgs[-1].content + "\n\n" + instruction
        new_msgs[-1] = OpenAIMessage(role="user", content=merged)
    else:
        new_msgs.append(OpenAIMessage(role="user", content=instruction))
    return new_msgs, prefill, True


# 重叠搜索窗口上限：预填充复述发生在输出开头几百字符内，真实用例不会
# 超过几千字符。无上限时长预填充（角色扮演预设可达数十 KB）的循环比对
# 是 O(n²) 字符操作且在事件循环上（实测 100KB 预填充 ~237ms 纯 CPU）。
_OVERLAP_SEARCH_CAP = 4096


def strip_prefill_overlap(prefill: str, output: str, min_overlap: int = 8) -> str:
    """预填充去重：若模型无视指令、把预填充的结尾复述在输出开头，裁掉重叠部分。

    - 输出以整段预填充开头（完整复述）→ 裁掉整段；
    - 否则找 预填充结尾 与 输出开头 的最长重叠（≥ min_overlap 字符，避免误伤），
      搜索窗口受 _OVERLAP_SEARCH_CAP 钳位（超长预填充不再平方级膨胀）。
    """
    if not prefill or not output:
        return output
    if output.startswith(prefill):
        return output[len(prefill) :]
    kmax = min(len(prefill), len(output), _OVERLAP_SEARCH_CAP)
    for k in range(kmax, min_overlap - 1, -1):
        if prefill[-k:] == output[:k]:
            return output[k:]
    return output


class PrefillDeduper:
    """流式版预填充去重器。

    预填充文本由代理先行发给客户端；若模型复述了预填充开头，需要在
    流式输出的起始处裁掉重叠。做法：先攒下输出开头的一小段（窗口 =
    min(len(prefill)+32, 600) 字符），做一次去重判定后放行，之后的
    文本原样透传（不再增加任何延迟）。

    用法：out = deduper.feed(chunk_text)（可能返回空串表示还在攒）；
    流结束时调用 deduper.flush() 取回剩余文本。
    """

    def __init__(self, prefill: str, window_cap: int = 600):
        self.prefill = prefill or ""
        self.window = min(len(self.prefill) + 32, window_cap) if self.prefill else 0
        self.buffer = ""
        self.done = self.window == 0

    def feed(self, text: str) -> str:
        if self.done:
            return text
        self.buffer += text
        # P2-4：只要已经能判定「不可能是预填充的重复」，立刻放行，不必攒满窗口。
        # 旧实现固定攒到 min(len(prefill)+32, 600) 字符才放行，首 token 延迟明显，
        # 与注释里的“零额外延迟”不符。
        if len(self.buffer) >= self.window or not self._still_ambiguous():
            return self._resolve()
        return ""

    def _still_ambiguous(self) -> bool:
        """当前缓冲是否还不足以判定去重量。

        strip_prefill_overlap 只看输出的前 len(prefill) 个字符，因此：
          - 缓冲长度已达 len(prefill) → 信息完备，重叠量已确定，立即放行；
          - 否则只有当缓冲仍是预填充的某个子串时，才可能延展成更长的重叠，需要继续等。
        """
        buf, pre = self.buffer, self.prefill
        if not buf or not pre:
            return False
        if len(buf) >= len(pre):
            return False
        return buf in pre

    def flush(self) -> str:
        if self.done:
            return ""
        return self._resolve()

    def _resolve(self) -> str:
        out, self.buffer, self.done = self.buffer, "", True
        return strip_prefill_overlap(self.prefill, out)
