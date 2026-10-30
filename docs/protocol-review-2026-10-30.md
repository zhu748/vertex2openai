# 三协议转换复查（2026-10-30）

本轮检查 OpenAI Chat Completions、Anthropic Messages 与 Gemini GenerateContent 的响应转换，覆盖 Express SDK、Cookie 私有协议和 Cookie 原生桥接。先构造失败样例复现，再修复并增加回归；未调用真实付费模型。

## 已修复的问题

| 问题 | 复现与影响 | 修复后的行为 |
| --- | --- | --- |
| 安全结束原因丢失 | `MODEL_ARMOR`、`IMAGE_PROHIBITED_CONTENT`、`IMAGE_RECITATION` 被映射为 `stop` | OpenAI 返回 `content_filter`，Claude 返回 `refusal`；共享映射覆盖流式与非流式 |
| 生成失败被当作成功 | `MALFORMED_FUNCTION_CALL`、`UNEXPECTED_TOOL_CALL`、`NO_IMAGE`、`IMAGE_OTHER` 被映射为正常完成，带函数参数时甚至变为 `tool_calls` | 转换路径报告 502；Cookie 在同一失败分片的工具调用发出前校验；Gemini 原生仍保留上游原始原因 |
| 候选索引漂移 | 上游候选索引为 `[2, 0]` 时，流式保留索引，非流式改为 `[0, 1]` | 两者均保留上游索引，只有缺省索引使用候选位置 |
| 分片用量清零 | 先有输入 100、缓存 20，后续分片只有输出 5；覆盖原对象会丢掉输入/缓存 | 源头合并 Gemini 累计快照，Claude 同样合并 OpenAI 部分用量；输出为输入 100、输出 5、缓存 20，Claude 非缓存输入为 80 |
| Claude 流结束不严谨 | 缺结束原因的 EOF / `[DONE]`、损坏事件可能被忽略后补发 `message_stop`；`[DONE]` 后还继续等连接关闭 | 不完整响应发 Anthropic `error`，不发成功结束；有效 `[DONE]` 立即终止消费并关闭生产者；尾部额外数据不再拼入回复 |
| 文件与图片元数据丢失/错位 | Cookie 丢弃 `fileData`；图片签名事件早于图片事件，可能签在前一段文字上；图片 base64 被重复放入元数据 | 原生桥接保留文件字段和签名；OpenAI 使用统一链接回退；图片签名在图片事件后关联，元数据只记录图片位置和签名 |

Gemini 用量合并按“最新累计快照”处理，不按增量求和。缺省、`null` 和非法计数字段不会清除已知值，明确的 0 会生效；分项变化且没有新总量时，旧总量不再用于 OpenAI 映射。原生透传仅更新本地诊断累计值，不修改转发的原始响应字节。

Claude SSE 解析同时补齐注释混入多行 `data:` 的处理：注释行不拆分同一个事件，也不吞掉有效数据；空 `data:` 不被当成坏 JSON。

## 通道覆盖

| 行为 | Express → OpenAI | Cookie → OpenAI | Anthropic 转换 | Gemini 原生 |
| --- | --- | --- | --- | --- |
| 安全原因映射 | 流式 / 非流式 | 流式 / 非流式 | 接收相同过滤语义 | 保留原始原因 |
| 失败结束校验 | 共享响应校验 | 事件解析前校验 | 沿用上游错误 | 保留原始原因 |
| 候选 index | 保留上游值 | 已有非零索引拒绝逻辑，避免错误合并 | 单候选接口 | Express 字节透传 |
| 部分用量 | SDK 分片合并 | 真流式、收集与假流式收集路径合并 | 保留输入 / 缓存 / 思考字段 | Cookie 重组和 Express 本地诊断合并 |
| 文件引用 | 图片/文件 Markdown 回退 | 相同显示规则 | 沿用转换后的内容 | Cookie 保留 `fileData`；Express 字节透传 |
| 图片签名与历史回放 | 接收保留的图片位置 / 签名 | 修复签名关联与元数据 | 沿用 Google 扩展元数据 | Cookie 保留原生 Part |

## 验证

| 检查 | 结果 |
| --- | --- |
| `tests/test_protocol_conversion_edges.py` | 23 项通过 |
| `scripts/check_project.py`：语法、指定静态规则、工作流、模块边界、所有测试脚本、SDK 档案兼容 | 63/63 组通过 |
| `scripts/smoke_final.py` | 25 项通过，0 失败 |
| `git diff --check` | 通过 |

新增回归包含安全/失败结束原因、候选乱序与索引缺省、部分用量和显式零、SDK 流式及 Cookie 非流式用量转换、Cookie 原生与 REST 诊断用量、Claude `[DONE]` 后保持连接与尾部数据、错误 EOF、坏 JSON、混合注释/多行事件、文件字段空值与类型、图片签名双通道历史回放。

## 保真范围

- 本轮没有改变 Gemini Express 的原生字节透传，也没有把生成失败伪装成可重试的排队超时。
- OpenAI 的文件链接是可读回退，不等于完整的原生多模态文件协议；需要保留任意 `fileData` 字段时应使用 Gemini 原生端点。已验证的完整历史回放样例是带签名的 inline 图片与前置文字。
- 验证使用锁定版本的 SDK、离线事件及 ASGI/服务测试；线上模型调度、Cookie 私有服务当前支持范围与真实账号响应仍需部署后验证。

参考官方协议：[Gemini 结束原因与用量](https://docs.cloud.google.com/gemini-enterprise-agent-platform/reference/rest/v1/GenerateContentResponse)、[Gemini Content / Part](https://docs.cloud.google.com/gemini-enterprise-agent-platform/reference/rest/v1/Content)。
