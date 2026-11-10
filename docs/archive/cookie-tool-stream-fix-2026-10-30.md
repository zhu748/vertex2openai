# Cookie 工具流失败修复（2026-10-30）

用户日志 `trace-554c018c85857362` 显示：普通 Cookie 对话和 Key 模式可用，Cookie 工具预设最终报“上游工具参数流：函数调用分片字段无效”，期间出现多次 HTTP 502 代理轮换和首回复超时。本轮针对 Cookie 工具转换和错误归因处理，没有调整代理配置或放宽首回复时限。

## 两处代码问题

1. `ToolStreamAssembler.restore_wire_parts` 把整个私有 GraphQL Part 交给 `google-genai` 的公开 `Part` 模型校验。私有外层元数据、无关 oneof 默认字段或不同表示方式的未选中字段也会参与校验，使真实工具调用被连带拒绝。函数参数中的数值 `nullValue=0` 同样不符合 SDK 的字符串 Literal `NULL_VALUE`，可以复现原有的通用错误。
2. Cookie 传输层将异常包装成仅带状态码的字符串，本地参数校验异常因此只剩 `502`。首回复前的故障转移按这个状态码切换代理，使相同的本地错误被重复请求；它不代表代理故障。

原日志没有保存 Pydantic 的字段位置，无法从日志确认该次请求具体命中了哪一种格式差异。上述情况已分别构造样例复现；新版遇到未覆盖的格式会直接给出安全字段位置，不再仅报告通用“字段无效”。

## 修复行为

- Cookie 普通 Part 保持 wire 结构；函数 Part 只转换 `functionCall`、`thought` 和 `thoughtSignature`，不校验其他无关的 Part 分支。
- 函数和参数片段的外层去掉 GraphQL `__typename`、未知 null 默认字段；已知字段继续校验，未知非空字段明确拒绝。`args` 内的用户数据，包括名为 `__typename` 的参数键，全部保留。
- 把数值枚举 `nullValue: 0` 转换为 SDK 接受的 `NULL_VALUE`；不把布尔 false 当作数值枚举，也不接受无效枚举和真实的多值冲突。此规范化同时支持 ProtoJSON 的 camelCase 和 snake_case 字段名。
- 专用 `ToolStreamError` 继续对外返回 502、对外层自动重试保持不可重试，但不会被认为是坏出口。仅这类本地工具错误与首回复超时被排除，已有空响应故障转移和真实 HTTP/网络故障仍有效。
- `StreamRequestError` 保留原异常来源，故障转移使用异常类型；只有实际 HTTP/上游错误事件才按其状态码处理。
- SDK 校验错误输出最多三个已知字段位置和类型，如 `functionCall.partialArgs.0.numberValue (float_type)`；不包含参数值、生成文本、签名或未知私有字段名。

真实工具名称/参数仍可结束首回复计时；心跳、签名和用量不算首回复。工具参数完整且通过校验后才作为可执行调用交付，`required`、允许函数集合与工具声明保持原意。

## 回归验证

`tests/test_cookie_tool_stream.py` 在原 13 项基础上新增 10 项，共 23 项通过，包含：

- 私有外层字段、GraphQL 元数据、数值 null、原始对象不被修改。
- 布尔 false、数值 0、签名与调用 ID 保留；用户参数内的元数据同名键不被误删。
- OpenAI 真流式与 Gemini 原生桥接在分片间隔超过首回复期限时仍能完成；只调用一次上游。
- 配置 11 个代理、12 次外层重试，本地解析失败仍只请求一次，不输出残缺工具调用。
- HTTP 502、读取超时、传输中断仍可切换到第二个代理并成功恢复。
- 非法枚举、多值冲突、未知非空字段与损坏参数仍报错；日志不含参数值。

| 检查 | 结果 |
| --- | --- |
| Cookie 工具专项 | 23 项通过 |
| Express 工具首回复专项 | 18 项通过 |
| 原有代理故障转移专项 | 30 项通过 |
| `scripts/check_project.py` 全项目检查 | 63/63 组通过 |
| `scripts/smoke_final.py` 服务冒烟 | 25 项通过，0 失败 |
| `git diff --check` | 通过 |

线上验证需更新部署后使用同一 Cookie 账号及预设重试；本地没有用户的线上 Cookie 或原始响应分片，未声称真实账号已验证成功。

协议参考：[ProtoJSON 枚举与字段表示](https://protobuf.dev/programming-guides/json/)、[Gemini Content / PartialArg](https://docs.cloud.google.com/gemini-enterprise-agent-platform/reference/rest/v1/Content)。
