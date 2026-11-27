# DSH 切到 Gemini 后提示缺少 thought_signature

如果 DeepSeek Harness（DSH）切到 Neon 后返回 HTTP 400，并提到 `default_api:compress` 缺少 `thought_signature`，先检查当前请求是不是沿用了前一个模型的工具历史。Google 要求把必要的签名原样放回原始 `Part`。并行批次通常只有第一个调用带签名，不能给其他调用补上。详见 [Google Cloud 签名说明](https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/thinking/thought-signatures)。

DSH 的 [Gemini API 模式](https://github.com/deepseek-ai/deepseek-harness/blob/master/docs/user/guide/providers.md#add-a-custom-model-api) 使用带版本的 Base URL，例如 `https://你的网关/v1beta`。在这里复核的版本使用 [pi-ai 1.3.0](https://github.com/earendil-works/pi/blob/v1.3.0/packages/ai/src/api/transform-messages.ts#L109-L152)：同 provider、协议和模型的回执可以带回原签名，跨模型时会去掉旧签名，但可能仍把原工具调用和结果编码成 Gemini 原生结构。Vertex2OpenAI 的原生 Express 通道按原样转发这些普通请求，不知道旧调用来自哪个模型，也不能重造签名。错误里的 `compress` 自身不能识别 DSH 插件或判断历史能否迁移。

## 可选：给 DSH 客户端打兼容补丁

[本地 DSH 源码补丁](deepseek-harness-4b399c39.patch) 只适用于 DSH 上游的 `4b399c39a6d1479cd84ca591b4d7c88e24d38a96`。这是 DSH 客户端补丁，不需要修改网关，不会改动网关的原生协议、模型路径、项目、鉴权、重试或设置。已用真实 DSH AgentLoop、固定 pi-ai 和本地模拟 Gemini 验证，没有在你的 DSH 版本或 Google 线上模型验证。

在单独的干净 DSH 源码副本里核对版本并应用。命令里的补丁路径指向 **Vertex2OpenAI** 目录下的文件：

```sh
git rev-parse HEAD
git apply --check /path/to/vertex2openai/docs/deepseek-harness-4b399c39.patch
git apply /path/to/vertex2openai/docs/deepseek-harness-4b399c39.patch
```

按 [DSH 开发指南](https://github.com/deepseek-ai/deepseek-harness/blob/4b399c39a6d1479cd84ca591b4d7c88e24d38a96/docs/development.md) 在副本里验证、构建。项目依赖已安装后，可以单独运行：

```sh
pnpm exec vitest run packages/llm/llm-pi-ai/tests/gemini-foreign-history.spec.ts packages/llm/llm-pi-ai/tests/gemini-protocol.spec.ts
```

只在要切入的 DSH provider 上加一项，保留该 provider 原有的键引用、模型列表和其他设置。这是配置片段，不要用它覆盖整份 `cordis.patch.yml`：

```yaml
api: google-generative-ai
baseURL: https://你的网关/v1beta
geminiForeignHistoryHandoff: true
```

在切换前开启。它只处理 DSH 自己记下的一次明确切换，且旧模型历史中成批出现的工具调用都有相邻、唯一对应的成功文本结果。只为目标请求把已完成的旧调用参数和输出写成标明来源的历史文本，不重新执行工具，不当作新的用户指令，不写回原会话，也不生成签名或跳过校验。目标模型首次正常回答后，DSH 在既有回执元数据中记录一项核对值。同一范围、同一目的端的后续请求才能复用相同的文本投影；目标模型产生的签名、原生调用和按序返回的工具结果不改。

以下情况不会自动转换：无法确认切换、会话多次切换、历史中已有未经本补丁处理的目标模型回复、旧调用还没结束或执行失败、旧调用组混有推理或媒体、存在孤立或重复的调用/结果、压缩改掉了已核对的旧历史、超出保守边界。普通辅助压缩请求不触发本模式。这样不能替每种旧会话恢复，但不会为了绕过错误删历史或冒充原始模型状态。要撤下功能，先从 provider 删除该项；源码副本可在未混入后续改动时用 `git apply -R` 回退。已用兼容模式续过的会话建议用下述文字交接后再关闭。

## 不打补丁，继续当前工作

先切回旧模型等正在进行的工具轮完成。把**任务目标、已完成项、关键文件或结论、下一步**整理成一段自己确认过的文字，新建 DSH 会话，选 Neon，把文字和下一步作为第一条消息。不要粘贴 API Key、原始签名或把旧工具记录当作新一轮工具调用提交。DSH 自带的 [`/compact`](https://github.com/deepseek-ai/deepseek-harness/blob/master/packages/compaction/compaction-basic/README.md#what-happens-when-condensation-runs) 会保留近期记录，不能保证清掉触发错误的那一轮，不等于新建会话。

如果**同一个 Neon 会话没有切换模型**，也在 Native 工具续轮报这类错，优先检查客户端是否将原始 `Part`、`thoughtSignature`、调用 ID 和顺序完整回传。定位时只需提供 DSH 版本、API 协议，以及脱敏后的角色、工具字段名和签名字段是否存在，不需要签名内容或密钥。
