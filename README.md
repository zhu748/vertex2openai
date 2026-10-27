---
title: Vertex2OpenAI Express Adapter
emoji: 🔄
colorFrom: blue
colorTo: green
sdk: docker
app_port: 7860
---

# Vertex2OpenAI Express Adapter

Vertex2OpenAI 是一个 **OpenAI API 兼容代理**。它对外提供 OpenAI 风格的 `/v1/chat/completions` 与 `/v1/models` 接口，对内支持两条上游通道调用 **Google Agent Platform（原 Vertex AI）的 Gemini 模型**：

- **Express API Key（标准模式）**：用官方 `google-genai` SDK + `VERTEX_EXPRESS_API_KEY` 调用。
- **Cookie 直连反代模式**：用 Google Cloud 控制台 Cookie + Project ID 直连控制台私有 `batchGraphql` 接口（无需浏览器）。

> 管理控制台 UI 名为 **agentplatform2api**；仓库/镜像名仍沿用 `Vertex2OpenAI`。
>
> 📌 **说明**：Google 已将 Vertex AI 更名为 **Agent Platform**，文档表述已同步（历史标识符如环境变量 `VERTEX_EXPRESS_API_KEY`、SDK 参数 `vertexai=True`、文件名 `vertexModels.json` 为兼容性保留）。**无头浏览器（Playwright）模式已移除**，由更轻量的 Cookie 直连模式完全替代。

## 功能特性

- **多 Key / 多 Cookie 与混合调度**
  - 支持只使用单个 / 多个 Key、单个 / 多个 Cookie，或 Key + Cookie 同时启用，统一随机或轮询调度。
  - Express API Key：多 Key 随机或轮询调用官方 SDK。
  - Cookie 直连反代：Cookie + SAPISIDHASH 直连 `batchGraphql`，走网页端配额（含最新预览模型），真流式、防 60s 超时。**注意：走的是私有接口，见下方风险提示。**
- **OpenAI 兼容接口**：`GET /v1/models`、`POST /v1/chat/completions`。
- **Claude（Anthropic）兼容接口**：`POST /v1/messages`、`POST /v1/messages/count_tokens`、
  `GET /api/hello`；`GET /v1/models` 按请求头自动切换 Anthropic 目录格式。
- **管理控制台（浅色风格，本地静态资源，免前端构建）**
  - **仅密码登录**：打开根路径 `/`，输入密码（即 `API_KEY`）即可，无需账号。
  - 标准模式 / Cookie 直连 / Key + Cookie 混合模式在线切换；每个账号可独立启停。
  - 全局与账号专属代理均支持有序列表，失败后依次切换；每个账号可关闭向全局代理回退。代理弹窗支持类型与字段填写、智能粘贴识别、排序与逐条测试。
  - 在线热更新并保存 Google Cookie 与 Project ID；智能解析 `Cookie-Editor` 导出的 JSON / Header String；自动从整条控制台 URL 提取 Project ID。
  - **模型参数面板**：按所选模型显示其支持能力，并在线调整思考强度、生图分辨率与比例、采样默认值、输入图压缩、重试、假流式/轮询/安全分显示、预填充兼容模式（详见下文）。
  - **实时监控**：运行日志推流、健康度图表（成功/错误/拥堵重试）、Express 通道 Token 消耗统计（包含思考 Token；Cookie 私有接口通常不返回可靠用量）。成功计数不依赖上游是否返回 Token 用量。
- **Gemini 能力与适配**
  - **按后缀选择抗截断传输**：默认仅 3.8 Flash / 4a 提供抗截断后缀，其它模型可在设置中逐个开启；普通名称始终不启用。Express 支持原生增量函数参数流，Cookie 通道支持完整函数参数还原，详见下文。
  - 文本对话、流式（SSE）与非流式。
  - OpenAI tools / function calling ↔ Gemini function calling 适配（含 Gemini 3.x 多轮所需的 thought signature 编解码）；Express 与 Cookie 直连通道均支持自定义函数声明、调用与结果回传。
  - **安全分类对齐**：两条通道下发同一套安全设置（`HARM_CATEGORY_HATE_SPEECH`、`DANGEROUS_CONTENT`、`SEXUALLY_EXPLICIT`、`HARASSMENT`、`JAILBREAK`），阈值最宽松，避免通道间行为不一致。
    > 📌 **更正**：早期版本曾把"有思考、正文为空"归因于 Cookie 通道缺少 `HARM_CATEGORY_JAILBREAK`。这个解释是错的——按官方文档，**越狱分类器默认就是关闭的**，要打开必须显式把该分类的阈值设成具体的拦截值；不下发它不会启用任何过滤，下发 `OFF` 也只是空操作。该现象的真实成因见下方"3.6-flash 只返回思考"一节（前端恒发 `reasoning_effort=xhigh` 导致原生思考在 HIGH 档跑飞/被截断）。
  - **上游错误如实透传**：模型在当前项目/区域不可用（404）、权限不足（403）、参数非法（400）等，会以对应 HTTP 状态码 + OpenAI 错误格式返回，而非笼统的 500。
  - Google Search 增强：**文本模型**在模型名后加 `-search` 后缀按需开启。
  - 自动保留 Gemini 思考过程（Thinking），以 `reasoning_content` 返回。
  - 生图模型：输入图压缩、按模型的比例白名单校验、4K 等分辨率、图片输出转 Markdown data URL；生图强制"假流式"整块输出，避免超大 base64 卡死前端。
  - **按模型能力自动裁剪参数**：不同模型支持的参数不同，代理会自动移除目标模型不支持的参数以避免 400（详见"控制台与模型参数"）。
  - **自动重试（固定间隔 + 抖动）**：两条通道均内置 429/拥堵自动重试，**次数、间隔与抖动均可在
    控制台调整**（`retry_max` 默认 10 次、`retry_backoff_seconds` 默认 5 秒、
    `retry_jitter_seconds` 默认 1 秒；实际等待 = 间隔 ± 抖动/2，即默认 4.5~5.5 秒，
    **不随重试次数增长**——旧版指数退避会一路涨到 120 秒/次，累计可等 10 分钟以上，
    已改为固定间隔）。流式模式下，连接建立即发送 SSE 心跳、且等待期间持续发送心跳，
    避免前端因长时间无数据（如 3.1-pro 频繁 429）而**超时断开**。
    **单 Key 共享是完整支持的使用方式**：多人使用同一把 Key 遇到频繁 429 时，在原 Key 上等待
    并继续重试，各请求独立使用完整重试预算，不需要额外 Key。同一请求已退回裸模型名后会保留
    该路径，避免每次 429 重试又重打已知失败的项目路径。仅在配置了其他启用 Key 时才附带轮换，
    并使用新 Key 自己的项目路径。400/401/403/404 等明确的永久错误
    不会仅因消息含 quota 或模型名含 429 而反复重试。支持 `Retry-After` 秒数与 HTTP 日期：
    比配置窗口更短的提示照常采纳，更长的提示钳位到窗口上限（间隔 + 抖动/2，换 Key 重试时
    上一身份的等待要求对新身份无约束力）；等待中断连立即停止，仅实际执行的重试计入面板。
    流式预填充和空的用量块不会提前阻断重试；收到正文、思考或工具调用后不再重发，
    后续异常会明确返回错误事件，避免将半截答案当成成功。
  - **跨通道兜底重试（Express ↔ Cookie 互备，默认关闭）**：手动开启后，主通道在**尚未向客户端输出任何内容**前
    遇 429 拥堵、401/403 凭证问题或 5xx 服务端错误时，自动切换另一通道接力完成回复
    （Express 全部 Key 失败切 Studio Cookie；Cookie 失效/限流切 Express Key 池）。
    已输出内容绝不切换，半截答案不能重发；结构化输出在未开启 Cookie 结构化支持时
    仍锁定 Express；400/404 等请求级错误不兜底（换通道同样失败）。控制台
    「重试与退避」卡片可经「跨通道兜底重试」开关开启（`cross_channel_fallback`，默认关闭）。
    关闭时，密钥模式只调用 Express Key，Cookie 模式只调用 Cookie；保存另一类凭证不会使其参与调度。
    已保存的开关设置继续生效，升级不会覆盖管理员显式保存的选择。
  - **坏凭证自动隔离与红色告警**：Express Key 遇 `401 API_KEY_INVALID` 或项目欠费
    （`requires billing`）时，本轮请求内自动隔离该 Key 并**立即切换下一把**（跳过退避等待，
    同一把 Key 重试只会得到相同结果）；全部 Key 隔离完后交由跨通道兜底接管。同时
    在运行日志打印🔴红色醒目告警（含 Key 序号、掩码与项目 ID），提醒管理员及时
    删除或更换。单 Key + 429 场景不受影响，仍保留完整重试预算。
  - **网络截断错误可重试**：`peer closed connection (incomplete chunked read)`、
    `server disconnected`、`connection reset` 等网络层截断错误即使被上层包装成普通
    异常（类型信息丢失）也纳入可重试文本关键词，不再静默失败。
  - **模型首回复超时**：每次上游尝试默认最多等待 15 秒，思考、正文、工具调用、图片均算首回复；
    心跳、纯用量与签名不算。超时先关闭本次上游读取，再沿用现有重试预算与间隔。
    可在模型参数中调整全局默认或模型专属值，设为 0 关闭。
  - **预填充智能兼容**：自动处理"以 model 轮次结尾"被新模型拒绝（400）的问题（详见下文）。
  - **断连策略**：关闭续传时断连即停；开启续传时后台继续生成，最后一个前端断开后默认 10 秒内可续接。
    超过窗口后相同请求会取消旧任务并重新生成；窗口可调整，设为 0 即不复用断开的生成。
- **中文运行日志**：密钥轮询、上游调用、重试退避、权限报错、Token 统计等均为中文实时说明。

## 控制台配置与前端开发

模型参数按字段覆盖，运行设置在独立页保存；账号列表支持折叠、搜索和草稿撤销。近期监控指标基于当前进程保留样本。详细保存语义、开发命令及仓库恢复方式见 [控制台配置与布局更新](docs/console-usability.md)。

Cookie 短期令牌自动续期默认开启，默认每 600 秒刷新已启用账号，可在「运行设置 → Cookie 续期」调整间隔或关闭。
已有保存为关闭的设置继续保留。每个 Cookie 账号旁的「立即刷新」可刷新单个账号，运行设置中的
「立即刷新已启用账号」可刷新全部启用账号；关闭自动续期后仍可手动操作。
手动刷新使用已保存的 Cookie 和代理，先保存或撤销 Cookie 列表草稿，再执行刷新。
面板逐账号显示更新、无新令牌或失败结果，自动与手动任务不会并行轮换。
该功能只更新短期令牌，无法恢复已退出登录或已失效的会话。

管理接口 `POST /api/cookie-accounts/refresh` 使用控制台登录会话鉴权。
请求 `{}` 刷新全部启用账号，`{"id":"账号 ID"}` 刷新指定启用账号；返回逐账号结果及计数，
不返回 Cookie 内容。已有刷新任务运行时返回 HTTP 409。

## 快速开始（本地 Docker）

编辑 `docker-compose.yml`，设置初始环境变量：

```yaml
environment:
  - API_KEY=your_adapter_api_key
  - VERTEX_EXPRESS_API_KEY=your_vertex_express_api_key
```

启动：

```bash
docker compose up -d
```

默认将宿主机 `8050` 映射到容器 `7860`。浏览器打开控制台并用密码（`API_KEY`）登录：

```text
http://localhost:8050
```

> 自托管挂卷提示：镜像以非 root 用户（UID 1000）运行，`docker-compose.yml` 挂载的 `./data` 目录需对 UID 1000 可写：`chown -R 1000:1000 data` 或在 compose 里加 `user: "1000"`。

## 镜像获取与自建

```bash
# 本仓库 CI 自动构建（push main 即触发，多平台 amd64/arm64）
docker pull ghcr.io/<你的用户名>/vertex2openai:latest
```

Fork 本仓库后无需任何配置：GitHub Actions 会自动登录 GHCR（内置 `GITHUB_TOKEN`），
在每次 push `main` 时构建双平台镜像并推送 `ghcr.io/<你的用户名>/vertex2openai`，
标签策略：`latest`（默认分支）、`main-<短 SHA>`（可回溯版本）、`vX.Y.Z`（推 tag 时）。
PR 只构建验证不推送，workflow 已收敛到最小权限（`contents:read` + `packages:write`）。

发布构建使用 amd64 / arm64 原生 runner 并行执行，镜像构建与 Python/Go
检查并行；全部通过后才合并并发布双架构标签。各架构使用独立缓存，Go 编译、
sing-box 下载和 Python 依赖分别缓存。同一分支的新提交会取消旧构建。
缓存首次需要预热，实际耗时以 Actions 日志为准；详见 [镜像构建与缓存](docs/image-build.md)。

镜像内置 `HEALTHCHECK`（HEAD `/healthz`），`docker ps` / 编排平台可直接看到健康状态。

## 健康检查端点 `/healthz`

公开、无鉴权、零上游请求（纯进程内状态，开销≈0），供 Render / UptimeRobot / 负载均衡 / 保活脚本探活：

```bash
curl -fsS https://你的域名/healthz     # GET：返回 JSON 状态
curl -fsSI https://你的域名/healthz    # HEAD：只看状态码，响应无 body，适合监控平台
```

GET 返回（代理地址自动脱敏，凭据不外泄）：

```json
{
  "status": "ok",
  "service": "agentplatform2api",
  "mode": "express",
  "express_keys": { "total": 2, "enabled": 2 },
  "cookie_configured": false,
  "proxy": { "enabled": true, "url": "socks5://***@96.62.118.47:9285" },
  "time": 1760000000
}
```

> Render 部署时把 Health Check Path 设为 `/healthz` 即可，30 秒冷启动期间不会误判不健康。

### 出站保活（防平台冻结闲置实例）

Render / HuggingFace Space 等免费托管平台会休眠长时间无流量的实例。设置环境变量
`KEEPALIVE_URL=https://你的域名/healthz` 后，本服务会**默认每 60 秒**（`KEEPALIVE_INTERVAL`
可调，下限 5 秒）自己 GET 一次这个地址保持唤醒；该地址也可以换成任何外部监控端点。
留空 = 完全不启动（零开销）。保活请求直连出站、不走出站代理；也可以之后在控制台
「运行设置」页填写或修改，保存立即生效、无需重启。单请求超时 30 秒，失败只记日志，
不影响正常服务。

## Gemini 原生格式端点（透传）

除了 OpenAI 兼容接口，本服务还提供 **Gemini 原生 REST 端点**。Express 通道按字节透传普通请求，
因此 tools / function calling / `thinkingConfig` / `safetySettings` / `responseSchema` / `cachedContent`
等原生字段由目标模型和上游 API 决定支持情况。Cookie 通道对生成接口执行协议转换，能力范围见下表：

```bash
# 非流式
curl -s https://你的域名/v1beta/models/gemini-3.6-pro:generateContent \
  -H "x-goog-api-key: 你的API_KEY" -H "Content-Type: application/json" \
  -d '{"contents":[{"role":"user","parts":[{"text":"你好"}]}]}'

# SSE 流式（加 ?alt=sse，与官方 SDK 行为一致）
curl -N https://你的域名/v1beta/models/gemini-3.6-pro:streamGenerateContent?alt=sse \
  -H "x-goog-api-key: 你的API_KEY" ...

# Token 计数 / 模型列表（Gemini 原生格式）
curl -s https://你的域名/v1beta/models/gemini-3.6-pro:countTokens -H "x-goog-api-key: 你的API_KEY" -d '{"contents":[]}'
curl -s https://你的域名/v1beta/models -H "x-goog-api-key: 你的API_KEY"
```

要点：

- **鉴权三种写法任选**：`Authorization: Bearer <API_KEY>`、`x-goog-api-key: <API_KEY>`、`?key=<API_KEY>`
  （与 OpenAI 端点同一把 Key，常量时间比较校验）。
- **双前缀**：`/v1beta/...`（Generative Language 风格）与 `/v1/...`（Vertex 风格）都注册了 POST 透传；
  模型列表只在 `/v1beta/models`（`/v1/models` 保持 OpenAI 格式，供 OpenAI 生态使用）。
- **上游选择与重试**：复用 Express Key 池（轮询/随机）与 location 钉定；429/5xx 与网络异常按
  控制台 `retry_max` / `retry_backoff_seconds` / `retry_jitter_seconds` 重试（总尝试次数 =
  `retry_max + 1`，设 0 仍请求一次），间隔为**固定值 + 抖动**（默认 5±0.5 秒，不随次数增长）；
  上游 `Retry-After` 更短时采纳其提示，更长时钳位到窗口上限。每次重试按策略重新选 Key，
  单 Key 或 Key 池轮转一遍后仍可继续重试，客户端断开即停止；上游错误状态码与 body 原样透传。
  Cookie 模式下，`generateContent` / `streamGenerateContent` 转换为私有 batchGraphql 请求；
  其它模式优先使用 Express，没有 Key 而有 Cookie 时可桥接生成接口。
  `countTokens` / `embedContent` 仍需要 Express Key；仅有 Cookie 时返回明确的 400。
- **面板统计**：两个前缀的原生 POST 请求均计入总请求数，每个客户端请求只计一次；成功、
  失败、拥堵重试分别计数。流式成功在转发完成时计入，中途失败计入错误。模型列表 GET 不计数。
- **模型路径拼接与 SDK 完全一致**：裸模型名拼 `publishers/google/models/{model}`（Express 端点
  格式）；`projects/...` 完整资源路径（location 钉定生成或客户端自带）原样透传，不叠加前缀。
  > 📌 修复说明：旧版对钉定路径无条件叠加 `publishers/google/models/` 前缀，会请求
  > `…/v1beta1/publishers/google/models/projects/…` 双重前缀的畸形 URL，Google 前端直接返回
  > 404 空 body（客户端显示 `Unknown error: 404`）——这正是「OpenAI 兼容接口正常、
  > Gemini 原生格式一律 404」的根因，已按 SDK `t_model()` 语义修复。
- **上游 URL 钉 `v1beta1`**（与 google-genai SDK 在 vertex 模式的真实默认 api_version
  一致，MockTransport 实测对照）。旧版写死 `v1`：不仅与自称的 `google-genai-sdk`
  形态自相矛盾（SDK 从不产生 `/v1/` 的 URL），且 `v1` 端点的
  `streamGenerateContent` 对 `alt=sse` 的行为与 `v1beta1` 不一致——实测症状为
  **客户端无限转圈且服务端无任何报错**（选 Key 后无日志、统计不动，
  OpenAI 格式同 Key 同模型一切正常）。已修复并有防漂移回归断言。
- **出站可观测性**：上游响应一行摘要（状态码 + content-type，
  `debug_outbound` 开启时附完整上游 URL）；流式请求上游若返回非
  `text/event-stream` 立即告警；流式转发完成/中止（客户端断开）均打日志——
  不再出现「选 Key 之后零日志」无法排查的情况。
  显式请求 `alt=sse` 而上游返回非 SSE 时，返回明确的 502 错误，避免客户端一直等待事件。
- **钉定失败自动兜底**：钉定路径返回 404 `Publisher model … not found` 或 403
  `requires billing` 时，自动退回裸模型名重试一次（不换 Key），与 OpenAI 兼容通道的
  fallback 行为一致；其余错误仍原样透传。
- **客户端接入示例**（google-genai / Cherry Studio / LobeChat 等）：API Base 填
  `https://你的域名/`，API Key 填本服务 `API_KEY`，选 "Gemini / Google" 供应商类型即可。

## Claude（Anthropic）格式端点

本服务提供 **Anthropic Messages API 兼容端点**，供 Claude Code、Anthropic SDK、
Claude Code Router、Cherry Studio 等说 Claude 协议的客户端直接接入——请求被翻译成
内部 OpenAIRequest 后**完全复用** `/v1/chat/completions` 的上游管线（Express Key /
Cookie 双通道调度、重试、抗截断、预填充兼容、思考档位、安全设置），再把响应翻译回
Anthropic 格式（协议实现口径对齐 [vertex-master](https://github.com/zhu748/vertex-master) 的
anthropic_handler）：

```bash
# 非流式
 curl -s https://你的域名/v1/messages \
  -H "x-api-key: 你的API_KEY" -H "anthropic-version: 2023-06-01" -H "Content-Type: application/json" \
  -d '{"model":"claude-sonnet-4-5","max_tokens":1024,"messages":[{"role":"user","content":"你好"}]}'

# SSE 流式（stream:true，事件序列 message_start → content_block_* → message_delta → message_stop）
curl -N https://你的域名/v1/messages -H "x-api-key: 你的API_KEY" ... -d '{...,"stream":true}'

# Token 计数（Claude Code 上下文管理依赖）
curl -s https://你的域名/v1/messages/count_tokens -H "x-api-key: 你的API_KEY" \
  -d '{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":"你好"}]}'
```

要点：

- **鉴权四种写法任选**：`x-api-key: <API_KEY>`（Anthropic 标准）、`Authorization: Bearer <API_KEY>`、
  `x-goog-api-key: <API_KEY>`、`?key=<API_KEY>`（与其它端点同一把 Key，常量时间比较校验）；
  401/400 等错误一律返回 Anthropic 错误形状 `{"type":"error","error":{...},"request_id":...}`。
- **模型名映射**：`claude-opus-*` → 最新 Pro、`claude-sonnet-*` → 最新 Flash、
  `claude-haiku-*` → Flash-Lite（静态别名表 + 模型目录热启发）；`gemini-*` 名字原样透传，
  Claude 客户端也可直接用 Gemini 模型及其 `-search` / 抗截断后缀变体。
- **结构化输出**：`output_config.format = {"type":"json_schema","schema":{...}}` 转为 Gemini JSON Schema，格式或 Schema 非法时返回 400，并遵循上表通道限制。
- **思考兼容**：`thinking.budget_tokens`（整数 ≥1024）映射到内部思考档位
  （≤1024 → low、≤8192 → medium、>8192 → high；`disabled` → off、`adaptive` → 跟随默认）；
  优先级对齐 vertex-master：`thinking` 的 enabled/disabled 钉住档位，
  `output_config.effort`（low/medium/high/xhigh/max/auto/default）仅在 thinking
  缺失或 adaptive 时生效（effort 校验始终执行，非法值照样 400）；
  `thinking.display: "omitted"` 时响应侧不回传思考块（模型照常思考、
  reasoning_tokens 照计用量）；`summarized` 无法摘要、按完整回传（有意偏差）；
  历史中 assistant 的 `thinking` / `redacted_thinking` 块静默丢弃（Gemini 无法消费
  Anthropic 思考签名）；响应侧思考块携带 SHA-256 伪签名（非空，满足 SDK 历史校验）。
- **工具兼容**：`tools[].input_schema` → 函数参数 schema；`tool_choice`
  （auto/any/none/tool → OpenAI 同义档位）；`tool_use` / `tool_result` 块双向转换，
  tool_result 纯文本拼接为字符串、复杂块剥离 `cache_control` 后整体透传；流式中工具
  参数分片先累积，收尾时以一条完整 `input_json_delta` 下发（对齐 vertex-master）。
- **流式保活**：`message_start` 先行（不等上游首字节）；上游心跳注释帧与 10s 空闲窗口
  均转换为 `event: ping`——长思考场景 Gemini 可能 10-30s 不发内容，没有 ping 会导致
  Claude Code SDK 误判超时断连；上游错误帧转换为 `event: error`，不改 HTTP 状态码。
- **count_tokens 两级口径**：配置了 Express Key 时优先调 Vertex `countTokens`
  拿模型侧精确计数（消息 contents + system_instruction + 工具声明全部计入，
  与真实生成请求同一分词器；比 vertex-master 只计 contents 多算工具声明，
  对齐 Anthropic 官方口径）；纯 Cookie 通道或精确计数失败时回退启发式估算
  （CJK ≈ 1 token/字、其余 ≈ 4 字符/token），量级正确、非逐 token 精确。
- **方法约束 405**：`/v1/messages` 与 count_tokens 仅接受 POST、`/api/hello`
  仅接受 GET/HEAD，其余方法一律 405 + Anthropic 错误形状（对齐 vertex-master；
  FastAPI 默认 405 是 `{"detail":...}` 形状，Claude 客户端无法解析）。
- **模型目录双格式**：Anthropic 客户端访问 `GET /v1/models` 得 Anthropic List
  Models 形状（claude 别名 + Gemini 模型）；`GET /v1/models/{id}` 单模型检索
  同样双格式（Anthropic `models.retrieve` / OpenAI `models.retrieve`），
  未知模型分别返回 `not_found_error` / `detail` 形状的 404。
- **`cache_control` 全程剥离**（含 tool_result 复杂块，不创建显式缓存）；上游隐式缓存命中映射为
  `cache_read_input_tokens`，普通 `input_tokens` 扣除这部分，二者之和等于总输入；
  中途 system 消息（Claude Code queued steering）降级为 user 轮；`stop_reason` 映射
  tool_use / max_tokens / refusal / end_turn；`anthropic-version` / `anthropic-beta`
  头放行不校验。
- **Claude Code 接入**：`ANTHROPIC_BASE_URL=https://你的域名`、
  `ANTHROPIC_AUTH_TOKEN=你的API_KEY`、`ANTHROPIC_MODEL=claude-sonnet-4-5`（或
  `ANTHROPIC_SMALL_FAST_MODEL=claude-haiku-4-5`）。

工具转换的完整性约定：

- 历史 `tool_calls[].function.arguments` 必须是合法 JSON 对象；省略或空串按 `{}` 处理，
  非对象、非法 JSON、非有限数字返回 400。适配器不会再用空参数掩盖错误，也不会把上游
  非法参数改成额外的 `value` 入参；上游完整调用无效时返回 502（流式使用 error 事件）。
- 同一 assistant 消息内的工具 ID、声明中的函数名不能重复。未知工具类型明确拒绝；
  兼容省略 `type`、直接携带 `function` 对象的旧客户端。
- Claude 历史及带完整顺序元数据的非流式回复保留文本与工具的交错顺序；工具结果仍按
  `tool_use_id` 关联，允许同名并行调用的结果按不同顺序回传。
- Claude 流式转换按工具 `index` 累积参数，支持 ID / 名称稍后到达；冲突 ID 不会覆盖已收
  参数。累计工具内容上限为 32 MiB、调用数量上限为 1024；收尾后释放缓冲。
  `max_tokens` / 内容过滤造成的截断保留原始参数分片和结束原因，客户端应检查结束原因
  后再决定是否执行工具。普通完整回复收尾时校验参数对象。
- 普通工具通道暂不聚合 Gemini 原生 `partialArgs` / `willContinue` 参数分片，遇到这类
  不完整调用会明确报错，避免把它转换为空参数调用；防截断模式的私有文本传输不受影响。

协议依据：[Claude 流式协议](https://platform.claude.com/docs/en/build-with-claude/streaming)
及 [Gemini 函数调用](https://ai.google.dev/gemini-api/docs/function-calling)。

## Cookie 通道浏览器档案（curl_cffi）

默认使用 `chrome`；也可选择 `edge`、`firefox`、`safari`、`safari_ios` 或 `off`。
启用时，TLS 档案、User-Agent 和 client hints 都由同一个 curl 档案提供，
不再把固定旧版本的 UA 覆盖到更新的 TLS 档案上。接口请求的 fetch metadata 单独设置，
并明确移除 `Sec-Fetch-User` / `Upgrade-Insecure-Requests` 等导航头。
Cookie / SAPISIDHASH / Origin 等业务认证字段保持原有逻辑。

这些设置用于保持协议形态一致，不代表完整浏览器环境，也不保证避免 Google 的限制。
私有 batchGraphql 接口、查询签名、账号权限和配额仍可能变化。依赖版本已固定；
升级 curl_cffi 时应运行本地实际 HTTP 收包回归。

## Express 通道请求头标签

默认 `sdk` 由官方 Python SDK 管理。`node`、`gcloud`、`custom` 作为已有集成的兼容选项保留，
仅改变 HTTP 标签，不改变实际 TLS/HTTP2 栈，不保证降低风控风险。`node` 只作用于裸 REST 出站点，
SDK 主通道仍保留自身 Python 标签。自定义值支持 `{sdk}`、`{python}`、`{node}` 占位符。

`scripts/probe_sdk_channel_profiles.py` 比较 SDK 构造的 HTTP 请求；MockTransport 不经过真实 TLS，
因此结果不能用于证明真实浏览器一致性或 Google 风控结论。`429` 应先结合上游错误正文、容量及配额排查。

## 性能：上游连接池化与大响应解析

三条通道的上游 HTTP 客户端全部池化复用（`app/client_pool.py`），同配置下
请求之间、重试之间共享连接池（HTTP keep-alive），不再每请求/每重试重新
TCP+TLS 握手（每次约省 100~300ms 与相应 CPU）；429 重试场景收益最大
（默认 10 次重试从 11 次握手降到 1 次）。控制台热改代理或指纹档案后，
新请求自动使用新配置；宽限期内再次用到相同客户端时直接复用，避免 Key 数超过热池容量后反复重建。
待回收缓存上限为 256，空闲客户端最多保留约 60 秒以便复用；缓存满时优先关闭空闲客户端。
活跃请求持有连接引用直到响应结束、流关闭或取消，超过缓存窗口仍不会被关闭。
只有待回收连接全部处于使用中且预算耗尽时才返回 503；停机时统一关闭连接。

配套优化：Cookie 通道重试不再重复构建请求体（含重复下载/压缩同一批输入
图片）；batchGraphql 流式响应按对象边界增量收集，完整后只解码一次 JSON，
避免大图片被切成小块时反复扫描；完整正文直接用 C 解码器解析。单对象超过
64MiB 或 JSON 截断时返回明确错误；`/v1/models` 的
模型列表使用本地初始化和后台刷新：刷新期间仍立即返回缓存，并发请求只触发一次远程读取；
成功后 1 小时再刷，失败后 5 分钟重试，保留上一次可用缓存。服务关闭时会取消后台任务，
并释放 Express SDK、Gemini 透传、Cookie 通道及宽限期内待关闭的连接池。

环境变量 `TRUST_PROXY_HEADERS=1`（可选，默认关）：部署在可信反向代理后面时，
登录限速按 `X-Forwarded-For` 区分真实客户端 IP，避免"一个攻击者输错密码、
全站控制台被锁退避"。仅在你确认直连入口是自己的反代时开启（否则可被伪造
头绕过限速）。

## 环境变量

| 变量 | 必填 | 默认值 | 说明 |
|---|---:|---|---|
| `API_KEY` | 是 | `123456` | 保护本代理的 Key，同时也是**控制台登录密码**。客户端请求用 `Authorization: Bearer <API_KEY>`。 |
| `VERTEX_EXPRESS_API_KEY` | 否 | 空 | Gemini Express Key，支持逗号 / 换行分隔或 JSON 数组。首次启动导入，之后以控制台列表为准。 |
| `VERTEX_EXPRESS_API_KEYS` | 否 | 空 | 多 Key 配置，优先于单数变量。支持字符串数组，或带 `key`、`project_id`、`proxies`、`proxy_fallback`、`enabled`、`note` 的对象数组。 |
| `UPSTREAM_MODE` | 否 | 自动 | `api_key`（仅 Key）、`web_proxy`（仅 Cookie）、`mixed`（混合）。首次启动同时配置两类凭证时自动使用混合模式；已有控制台通道选择继续保留。 |
| `ROUNDROBIN` | 否 | `false` | Key / Cookie 账号轮询(`true`)或随机(`false`)；混合模式下两类账号共同参与。可在控制台热改。 |
| `FAKE_STREAMING` | 否 | `false` | 先非流式取回再模拟流式；生图模型始终强制启用。可在控制台热改。 |
| `FAKE_STREAMING_INTERVAL` | 否 | `1.0` | 假流式等待期间 keep-alive 间隔秒数。可在控制台热改。 |
| `MODELS_CONFIG_URL` | 否 | 仓库 `vertexModels.json` | 远程模型列表地址；改远程文件即可刷新，无需重部署。 |
| `SAFETY_SCORE` | 否 | `false` | 是否把 Gemini safety ratings 附加到输出。可在控制台热改。 |
| `PROXY_URL` | 否 | 空 | 全局出站代理（仅作初始值，控制台保存后以控制台为准）。支持 HTTP / SOCKS 原生代理、sing-box 分享链接，以及 `turn://` / `turns://` / `sstp://` 节点；镜像内置两种转换组件。保存时自动校验格式。 |
| `PROXY_URLS` | 否 | 空 | 有序全局代理列表，JSON 字符串数组或每行一条；优先于 `PROXY_URL`。每次请求先尝试第一条，失败再尝试后续条目。 |
| `PROXY_BRIDGE_BIN` | 否 | 自动探测 | TURN / SSTP 转换器路径；Docker 镜像内置，本地部署需编译 `proxybridge/`。 |
| `PROXY_BRIDGE_IDLE_MINUTES` | 否 | 30 | TURN / SSTP 空闲实例回收时间，`0` 关闭空闲回收；在用实例保留，退出进程仍清理。 |
| `SSL_CERT_FILE` | 否 | 空 | 自定义证书路径。 |
| `VERTEX_BASE_URL` | 否 | 空 | **高级**：覆盖标准模式的上游 `base_url`，正常无需设置。要钉定区域请用控制台的「标准模式 location」（详见下方「标准模式的 location」）。 |
| `GOOGLE_COOKIE` | 否 | 空 | 一个完整 Cookie 头，或每行一个账号的完整 Cookie；分号属于 Cookie 字段分隔符，不用于拆账号。兼容 Cookie-Editor JSON。 |
| `GOOGLE_COOKIES` | 否 | 空 | 优先于单数变量。JSON 字符串数组或带 `cookie`、`project_id`、`proxies`、`proxy_fallback`、`enabled`、`note` 的对象数组；也支持每行一个完整 Cookie。首次导入后以面板列表为准。 |
| `GOOGLE_PROJECT_IDS` | 否 | 空 | Cookie 使用字符串列表时，可按相同顺序填写项目列表（JSON 数组或逗号 / 换行分隔），数量须一致。对象内的 `project_id` 优先；未填时使用 `GOOGLE_PROJECT_ID`。 |
| `GOOGLE_PROJECT_ID` | 否 | 空 | Cookie 直连模式的 Project ID（初始值，后续可在控制台更新）。 |
| `EXPERIMENT_FLAGS` | 否 | 空 | 可选：batchGraphql 的 `experimentFlagsBinary`，一般无需设置。 |
| `STATE_DIR` | 否 | `.` | `web_state.json` 的存放目录。用 Docker 时请指向挂载卷，否则重建容器会丢失全部设置与 Cookie。 |
| `TRUST_PROXY_HEADERS` | 否 | 空 | 部署在可信反向代理后时设 `1`：登录限速按 `X-Forwarded-For` 区分真实客户端，避免一人失败全站被锁退避。确认入口是自己的反代才开（否则可被伪造头绕过限速）。 |
| `ALLOW_DEFAULT_KEY` | 否 | 空 | 仅用于在公开托管环境（如 HF Space）临时放行默认 `API_KEY`，正常部署不要设置。 |
| `SINGBOX_BIN` | 否 | 自动探测 | sing-box 可执行文件路径（默认依次找 `$SINGBOX_BIN` → `/usr/local/bin` 等常见路径 → 仓库根 `bin/sing-box` → `PATH`）。仅影响 vless/vmess 等分享链接协议；缺失时这些协议明确报错、不降级直连。 |
| `SINGBOX_IDLE_MINUTES` | 否 | `30` | sing-box 空闲实例回收阈值（分钟；`0` = 永不回收）。每个代理链接占一个本地端口与一个子进程，长时间不用自动回收、下次请求时秒级重启。 |
| `KEEPALIVE_URL` | 否 | 空 | 出站保活：定期 GET 该 URL（默认 60 秒一次）。填自己的 `https://你的域名/healthz` 可防止免费托管平台（Render / HF Space 等）冻结闲置实例，也可驱动外部监控。留空 = 不启动；请求直连出站、不走出站代理。仅作初始值，控制台「运行设置」页可热改。 |
| `KEEPALIVE_INTERVAL` | 否 | `60` | 保活请求间隔（秒），下限 5，上限 86400。仅作初始值，控制台可热改。 |

### 多账号与混合模式配置

控制台「通道与凭证」支持三种模式：仅 Express Key、仅 Cookie、Key + Cookie 混合。每条记录可独立启用或停用，账号数量可以为一个或多个。Cookie 的 Project ID 和代理始终跟随该账号，请勿把其他账号的 Cookie 与项目混用。结构化输出请求默认在混合模式中只选择 Express Key；Cookie 通道的结构化输出可经控制台开关 `cookie_structured_output` 显式启用（见下）。Gemini 原生生成接口同时支持 Express 透传与 Cookie 桥接；原生计数和向量接口需要 Express Key。

| 功能 | Express 通道 | Cookie 通道 |
| --- | --- | --- |
| OpenAI / Claude 文本、工具调用、图片历史 | 支持，受模型能力限制 | 支持，受模型和私有协议限制 |
| JSON Schema（含 Claude `output_config.format`） | 映射为 Gemini Schema | 需开启 `cookie_structured_output` 并验证上游支持 |
| OpenAI `n > 1` | 仅支持多候选的模型；Gemini 3.x 明确拒绝 | 明确返回 400，避免合并不同答案 |
| `logprobs` / `top_logprobs` | 受模型支持情况限制 | 明确返回 400 |
| 原生 `generateContent` / `streamGenerateContent` | 透传 | 协议桥接；只支持单候选 |
| 原生 `countTokens` / `embedContent` | 需要 Express Key | 不支持 |

混合模式下，OpenAI / Claude 请求只会分配给能保留其参数语义的通道；多候选、logprobs
以及未开启 Cookie 支持的结构化输出锁定 Express，跨通道兜底也遵循同一规则。

控制台的运行设置与全局模型默认值仅提交实际修改的字段；运行设置和模型专属参数保存
时携带字段原值作为并发条件。同字段已被另一页面修改时返回 409，并保留草稿，重新加载
后检查再保存。自定义 API 调用方可在设置 POST 中传入
`"_expected": {"retry_max": {"present": true, "value": 10}}`；模型继承字段用
`{"present": false}` 表示尚无专属值。未传 `_expected` 的旧客户端继续使用原有合并语义。

> **Cookie 通道结构化输出（`cookie_structured_output`，默认关）**：batchGraphql 私有协议对 `responseMimeType` / `responseSchema` 的接受度没有官方承诺，离线无法验证，因此默认对 `response_format` 显式 400 并提示改用 Express 通道。要启用：先在部署环境运行 `python scripts/probe_cookie_structured_output.py --model 模型名`（凭证取 `--cookie/--project-id`、环境变量或加 `--use-state` 读控制台记录），脚本直连上游跑 json_object / 严格 schema / json_patch 宽松方言三档用例并给出「完全可开 / 仅 JSON 模式 / 不可开」结论；探测通过后在控制台「运行设置」打开开关。开启后转换链与工具参数完全一致：`sanitize_for_gemini`（OpenAI 宽松方言 → Gemini 严格方言，空节点/`type` 数组/元组 `items` 自动净化）→ Studio 私有 UI Schema。若探测报告 `QUERY_SIGNATURE_NOT_FOUND` 或未知字段错误，说明 Google 已改私有协议，保持开关关闭即可，其余功能不受影响。

例如在 `.env` 中填写（以下均为占位值）：

```dotenv
API_KEY=change-me
UPSTREAM_MODE=mixed
ROUNDROBIN=true
VERTEX_EXPRESS_API_KEYS='[{"key":"key-a","project_id":"key-project-a","proxies":["socks5://user:pass@proxy-a:1080","http://backup-a:8080"],"proxy_fallback":true},{"key":"key-b","project_id":"key-project-b"}]'
GOOGLE_COOKIES='[{"cookie":"SAPISID=cookie-a; SID=sid-a","project_id":"cookie-project-a","proxies":["socks5://proxy-b:1080"],"proxy_fallback":false},{"cookie":"SAPISID=cookie-b; SID=sid-b","project_id":"cookie-project-b"}]'
PROXY_URLS='["socks5://global-first:1080","http://global-second:8080"]'
```

简单配置也可以使用 `VERTEX_EXPRESS_API_KEY=key-a,key-b`；多个 Cookie 可在 `GOOGLE_COOKIES` 中填写字符串 JSON 数组，并通过 `GOOGLE_PROJECT_IDS` 指定对应项目。

旧的单 Cookie、单代理和通道选择会自动兼容。账号列表首次导入并保存后，以面板中的持久化记录为准；清空列表后重启不会重新导入环境变量。Cookie 默认只显示配置状态；展开已保存账号后，可点击“显示 Cookie / 隐藏 Cookie”查看，或点击“复制 Cookie”复制最新保存值。查看框只读，不会生成编辑草稿；编辑框留空即可保留原 Cookie。普通 HTTP 页面无法自动复制时，会选中文本供手动复制。

### 出站代理与多协议支持

代理列表按保存顺序尝试，每次新请求从第一条开始。账号有专属代理时，顺序为 **账号代理 1 → 账号代理 2 → 全局代理 1 → 全局代理 2**；取消该账号的「专属代理全部失效后，使用全局代理」，则在专属列表耗尽后返回错误。账号未设置专属列表时直接继承全局列表。

- 连接异常、网络超时、代理协议转换失败，以及 HTTP 407 / 502 / 503 / 504 会触发下一条代理；400 / 401 / 403 / 404 / 429 等参数、鉴权和配额问题沿用模型重试策略。代理切换独立于 `retry_max`，即使模型重试设为 0 也会尝试后续代理。
- 模型首回复期限按代理尝试分别计时，整轮请求仍受 `request_timeout_seconds` 限制。已经交付流式内容后不会通过切换代理重发同一段内容。
- 全局列表为空表示直连；配置了代理并全部失败时，不会额外降级直连。账号单独填写 `direct` 可强制直连。也可在有序列表中显式加入 `direct`，自行选择直连顺位。
- **原生协议**：HTTP / HTTPS / SOCKS5 / SOCKS5H。**分享链接**：VLESS、VMess、Trojan、SS、SOCKS4 / SOCKS4A、Hysteria2、TUIC，由内置 sing-box 转成本地 SOCKS5；非 Docker 环境需安装 sing-box 或设置 `SINGBOX_BIN`。
- **TURN / TURNS / SSTP**：可直接粘贴节点链接或使用面板字段填写，由内置转换器转为本地 SOCKS5。TURN 需要 RFC 6062 TCP allocation；SSTP 支持 MS-CHAPv2 和 IPv4，不修改系统路由。目标域名在本机解析，详见 [TURN 与 SSTP 接入说明](docs/proxy-turn-sstp.md)。
- **转换边界**：支持 TCP / WS / HTTPUpgrade / gRPC / HTTP / QUIC 映射；KCP、XHTTP、TCP 伪装头、额外 QUIC 加密等无法等价转换的要求会明确报错。各协议的实际转发结果、配置校验结果与未验证范围见 [代理协议验证](docs/proxy-protocol-validation.md)。
- **格式兼容**：支持 `socks5://user:pass@host:1080`、`socks5://host:1080@user:pass`、`socks5://host:1080:user:pass`、`socks5://user:pass:host:1080`；省略协议时按 SOCKS5 识别。HTTP 同样支持这些账号位置形式。IPv6 主机可使用 `[::1]`，账号密码中的特殊字符可通过弹窗字段填写，自动编码。
- **面板操作**：「管理 / 添加代理」中分为代理列表、单条添加和批量导入。单条地址可自动识别并拆分字段，也可选择「测试并加入」。列表支持设为首选、上下移动、编辑、删除、逐条测试和测试全部；排序保留当前会话的测试结果。全局列表可直接「保存并生效」，或先应用到页面草稿。账号专属代理应用到账号草稿后，仍需保存对应的 Key / Cookie 列表。
- **批量筛选**：每次最多预览 **200 条候选 / 256 KiB**，每个保存列表最多 **32 条**。支持每行一条、完整链接逗号分隔、JSON 数组及 ICE 配置；无协议的地址按选择的类型解析。默认「先测试，仅导入通过项」，也可取消勾选后导入未测试节点。格式错误、重复项及 STUN 逐条提示，其他节点继续处理；识别后折叠原始输入，直接查看测试结果。导入时追加到末尾并保留原顺序，超过容量时可用「按剩余容量选择」勾选前面的合格节点，再按需调整。全部失败不会清空现有代理。
- **测试与结果**：从部署服务器探测所选代理，可选择 Google Vertex 或 Cookie 通道目标；不会失败后换用其他代理或直连。前端最多同时测试 3 条，服务端每个进程最多接收 4 个探测，单条总期限 55 秒；支持停止测试、重试失败或未完成项。HTTP 403 / 404 可以证明目标链路可达，**连通通过不代表账号授权或模型调用成功**。显示的 HTTPS 耗时与协议转换准备耗时分开；测试结果只保留在当前页面会话内，修改连接配置或切换目标后按对应配置显示。
- SDK、Cookie（httpx / curl）、Gemini 原生 REST、Key 探测和图片抓取均按对应账号的代理链执行；远程模型列表刷新使用全局列表。连接池按实际出口复用，切换不关闭其他请求正在使用的连接。
- `PROXY_URLS` 优先于 `PROXY_URL`；单数变量也支持换行列表和逗号分隔的完整 URL。控制台保存过代理列表后（含清空保存），以控制台为准。

> 提示：`ROUNDROBIN`、`FAKE_STREAMING(_INTERVAL)`、`SAFETY_SCORE` 等环境变量仅作为**初始值**，运行时以控制台设置为准。

---

## 行为变更说明（整改后）

按 `REFACTOR_PLAN.md` 完成整改后，以下行为与旧版本不同，升级时请注意：

| 项目 | 旧行为 | 新行为 |
|---|---|---|
| **思考档位兜底** | 非法档位一律抬到 `high`（Pro 上选 minimal 反而变 high） | 就近**向下**夹取：Pro 选 minimal → `low` |
| **`retry_max` 语义** | Express 通道当作总次数，设 0 时一次请求都不发 | 统一为「重试次数」，总请求数 = `retry_max + 1`，设 0 仍请求一次；取值钳到 0–50 |
| **并行函数调用（流式）** | 只发第一个，`index` 恒为 0 | 全部下发，`index` 跨 chunk 稳定递增 |
| **思考签名** | base64 拼进 `tool_call_id`（上千字符，易被前端截断） | 主要通过 OpenAI 扩展 `extra_content.google.thought_signature` 原样回传；进程内短期缓存、旧 id 格式与官方 `skip_thought_signature_validator` 哨兵仅作兼容兜底 |
| **Cookie 通道 + 函数调用** | 静默把 `role=tool` 折成 model，发出错乱历史 | 原生下发函数声明，按 `functionCall` / `functionResponse` 回放历史，保留调用 ID 与 thought signature；同名并行调用可按 ID 匹配结果 |
| **Cookie 通道输入图** | 不压缩、不支持 http(s) 图片、不解析正文内联图 | 与标准通道一致（压缩开关对两条通道都生效） |
| **`stop` 字段** | 只接受数组，传字符串 422 | 字符串/数组都接受 |
| **`logprobs` 字段** | 按 Gemini 语义当整数 | 兼容 OpenAI 的 `logprobs: bool` + `top_logprobs: int` |
| **Express 流式 usage** | 从不下发，客户端显示 0 | 支持 `stream_options.include_usage` |
| **控制台 Cookie 回显** | 明文返回完整 Cookie | 列表仅返回“已配置/字段数/总长度”；登录后点击单账号“显示 / 复制”才读取最新完整值，响应禁止缓存；编辑框留空＝保持原 Cookie 不变 |
| **登录** | 无限速、会话 token 为确定值 | 失败 3 次后指数退避；随机会话 token，可单独失效 |
| **状态文件** | 每次读设置都同步读盘、非原子写 | 内存优先 + 原子写 + 权限 0600 + 支持 `STATE_DIR` |
| **文本保真** | 所有消息的多空格/缩进被压平 | 仅在确实抽走内联图片时才压平 |
| **2.5 Flash-Lite 思考预算** | 下限按 0 处理 | 下限 512（`0` 仍表示关闭） |
| **生图比例白名单** | 所有生图模型共用同一套比例 | 按模型分别校验；Flash Image 支持含 `9:21` 在内的扩展比例，Pro Image 使用较小白名单 |

思考签名优先使用请求/响应中的 `extra_content.google` 携带，因此跨进程、多副本部署不依赖本地缓存；旧格式、进程内缓存与跳过校验哨兵仅用于兼容缺失显式签名的客户端。

---

## 控制台与模型参数

控制台"模型参数"面板可在线调整全局默认值，并按所选模型显示其支持情况。可调项：思考强度（按模型能力选择档位）、生图分辨率与默认比例、采样默认值（temperature/top_p/max_tokens）、输入图压缩（开关/边长/质量）、重试次数与退避、假流式/轮询/安全分显示、预填充兼容模式与"预填充时压制原生思考"开关、续写指令模板、Cookie 调试日志。

### 参数优先级

**单次请求 > 该模型专属 > 控制台全局默认 > 内置默认。**

- 有请求级写法的参数：请求体字段优先。标准字段 `temperature`/`top_p`/`max_tokens` 等，扩展字段 `reasoning_effort`、`thinking_budget`、`image_size`、`aspect_ratio`/`ar`。客户端不传时依次回退到"该模型专属 → 控制台全局 → 模型默认"。
- 两个例外：
  1. **"模型不支持"优先级最高**（最后一步裁剪）：目标模型不支持的参数，无论来自请求、专属还是全局都会被移除（避免 400）。
  2. **全局项无请求级/专属写法**：图压缩、重试、假流式、轮询、安全分显示、预填充模式与压制开关、续写模板、Cookie 调试仅由控制台全局决定。

### 按模型单独保存参数（per-model overrides）

模型参数面板顶部选择模型后，可为**当前所选模型**单独保存专属值——支持覆盖的三类：**思考（档位/预算）、生图（分辨率/比例）、采样默认（temperature/top_p/max_tokens）**。

- **保存为该模型专属**：只把上述三类字段存成该模型的专属配置；下拉框中该模型名后会显示 `★`，并出现"已有专属参数"徽章。
- **清除该模型专属**：删除该模型专属配置，回退到全局默认。
- 底部"保存设置"按钮保存**全局默认 + 基础设施项**；当所选模型已有专属配置时，它**不会**用面板里显示的专属值覆盖全局（避免误操作），只保存基础设施项。
- 基础设施项（图压缩/重试/假流式/预填充模式与压制/安全分/Cookie 调试）不支持按模型覆盖，始终全局唯一。

### 按模型能力自动裁剪（`app/model_capabilities.py`，依据官方文档）

- **采样参数弃用**：自 **Gemini 3.6 Flash / 3.5 Flash-Lite 起（及所有更新/未来模型）**，`temperature`/`top_p`/`top_k` 已废弃（现被忽略、未来返回 400），代理会**自动移除**；更早的 3.x（如 3.0–3.5 非 lite）仍可用，但官方建议保持默认。
- **`candidate_count`**：所有 Gemini 3.x 不支持，自动移除。
- **思考**：Gemini 3.x 用 `thinking_level`（`minimal`/`low`/`medium`/`high`，不可完全关闭；各模型默认不同，如 3.6-flash=medium、pro=high、flash-lite=minimal）；
  - **`gemini-3.7-flash`（2026-08 新增）**：官方默认档位 **`medium`**（取代旧 3.x 的 `high`），档位表只提供 **`low`/`medium`/`high`——没有 `minimal`**（出处：`intro_gemini_3_7_flash.ipynb`）。因此在 3.7 上选「关闭原生思考」会压到 `low`（就近合法档），而不是发一个不支持的枚举去冒 400 风险。同理，**3.7 及更新/未知型号一律不提供 `minimal`**，等真机验证后再逐个放开；`thinking_level` 与旧的 `thinking_budget` **不可同时出现**（官方明确会 400），本代理只会二选一。Gemini 2.5 用 `thinking_budget`（`-1` 动态；2.5-flash 可设 `0` 关闭，2.5-pro 最低 128）。
  - **原生思考控制（`native_thinking_mode`）**——控制台"思考强度"卡片的下拉，支持"保存为该模型专属"：
    - **跟随请求（默认）**：用前端发来的 `reasoning_effort`。⚠️ SillyTavern 等前端常在**每次请求都发 `reasoning_effort`（如 `xhigh`）**，会覆盖你在控制台设的档位。
    - **关闭原生思考（角色扮演推荐）**：忽略前端 effort，把档位压到该模型最低（3.x=`minimal`、2.5-flash 预算 `0`、2.5-pro `128`），并**不返回思考**。
    - **强制用上方档位**：忽略前端 effort，用你在卡片里选的档位（返回思考）。
  - 🎭 **酒馆预设“卡原生思维链”一键配置**：许多预设把思维链写在 **system 提示**里（不是预填充），并恒发 `reasoning_effort=xhigh`。把"原生思考控制"选 **“关闭原生思考”** 即可（可用"保存为该模型专属"只对 3.6-flash 生效），让预设自己的思维链接管。
  - ⚠️ **重要（Studio/Cookie 通道实测）**：`batchGraphql` 私有接口**会忽略 `includeThoughts=false`**——即使设了也照样回传思考。因此 Cookie 通道在"关闭原生思考"时会**在响应侧主动剥离思考块**，并**把档位压到 `minimal`**（这才是真正减少原生思考、避免重预设在思考阶段被截断/无正文的关键）。标准（Express）通道由 SDK 原生支持不返回思考。
  - 🩺 **3.6-flash 在 Studio 只返回思考、无正文（`FINISH_REASON_UNSPECIFIED`）怎么办**：真机定案——这是**原生思考在 HIGH 档跑飞/被截断**（SillyTavern 恒发 `reasoning_effort=xhigh` 覆盖了控制台档位），**不是**安全策略/`HARM_CATEGORY_JAILBREAK` 的问题（已用含/不含 jailbreak 的多组对照验证）。**解决：把"原生思考控制"设为"关闭原生思考"**（对 3.6-flash 用"保存为该模型专属"），即忽略前端 `xhigh`、压到 `minimal` 并剥离原生思考——真机验证可稳定输出正文与预设自带的思维链。仅设"思考档位=minimal"无效，因为会被前端 `xhigh` 覆盖。
- **生图**：剥离全部采样参数；`response_modalities=["TEXT","IMAGE"]`；**两个生图模型比例白名单不同**（pro-image 10 种；flash-image 15 种，含 `1:4/4:1/1:8/8:1/9:21`），控制台按所选模型过滤，后端也会校验，选到不支持的比例会**自动回退为"由模型决定"（不报错）**。
- **预填充（重要，专为 SillyTavern 等酒馆预设优化）**：Gemini 3.x 拒绝以 `model` 轮次结尾的请求（返回 `Requests ending with a model turn are not supported.`）。代理内置"预填充智能兼容"（`smart`/`minimal`/`off`，默认 `smart`，控制台可切）：
  - **按模型能力自动选策略**：2.5 及更早模型允许以 model 轮次结尾 → **原生透传**，模型直接续写你的预填充，最忠实；3.x 拒绝 → 自动把末尾 assistant 预填充转成末尾 user 的"续写指令"（模板可在控制台自定义）。两种情况都会把预填充文本**拼回输出开头**，并对模型复述的重叠部分**自动去重**。
  - **预填充时压制原生思考（"卡思维链"，默认开启）**：酒馆预设通常自带思维链，靠预填充卡掉模型原生思考、让预设的思维链接管。开启后，检测到预填充即把思考压到该模型最低并**不回传思考**：3.x 压到 `minimal`（`pro` 无 minimal 则 `low`，官方规定 3.x 无法完全关闭思考）；2.5-flash 预算设 `0` **完全关闭**、2.5-pro 降到最低 `128`。**单次请求显式传 `reasoning_effort` / `thinking_budget` 时不压制**（请求优先）。可在控制台关闭此开关恢复模型原生思考。
  - **与模型名无关，新模型自动生效**。
- **新增/未来模型**：按家族/版本模式自动归类；未知/未来型号按"最新代"前向安全处理（自动移除已废弃采样参数、走预填充兼容）。

---

## 标准模式的 location（能指定，且能修好"偶发 404 / 随机区域"）

**实测结论（2026-08-14，真机验证）**：留空时 location 由 Google 后端自选，**可能落到该模型不提供服务的区域直接 404**；显式钉定后同一个 Key、同一个模型即正常。

| 请求形态 | `gemini-2.5-pro` | 说明 |
|---|---|---|
| 裸模型名（旧行为）→ express 端点 `https://aiplatform.googleapis.com/v1beta1/publishers/google/models/{model}:generateContent` | **404** `projects/…/locations/asia-southeast1/…was not found` | 区域由后端路由，落到没有该模型的区域 |
| `projects/{project}/locations/global/publishers/google/models/{model}` | **200 正常出文** | 区域由我们钉定 ✅ |

**默认已经是 `global`**（多数 Gemini 模型只在 global 提供）。控制台「模型参数 → 全局运行设置 → 上游区域」下拉里除 `global` 外还列出了 Agent Platform 提供 Gemini 的各区域（美国 / 欧洲 / 亚太 / 其它，共 30 项）可按需切换；选「默认（后端自选）」＝回到旧行为。

**项目 ID 不需要单独配**：自动用「通道与凭证」页填的那个（或环境变量 `GOOGLE_PROJECT_ID`）——一个人通常只有一个 Express 项目。两者都没有时自动退回裸模型名（不会拼出半截路径）。

**钉定失败会自动兜底**：若钉定路径返回"模型不存在 / 需要计费"，代理会**自动退回裸模型名重试一次**（＝旧行为）并在日志说明怎么修（Express SDK 通道与 Gemini 原生透传通道均具备该兜底）。实机验证：故意把 Project ID 换成一个未开计费的项目，非流式与流式都仍能正常出文。所以把默认值设成 `global` 不会让任何既有配置变糟。

几个必须知道的边界（都是实测）：

- **项目必须是该 API Key 有权、且已开启计费的项目**。换成别的项目会 `403 requires billing to be enabled`。
- **区域端点主机不通**：`https://{location}-aiplatform.googleapis.com/...` 配 Express Key 会 404（该模型在那个区域没有）。因此钉定走的是**全局主机 + 路径里带 location**，不是换主机。
- **有的模型只在 `global` 提供**（如 `gemini-2.5-pro`），所以推荐 `global`。
- SDK 层面**不能**给 `Client` 传 `location`：`api_key` 与 `project/location` 互斥，硬传会 `ValueError`。钉定是靠**模型资源路径**实现的（`google-genai` 的 `t_model()` 对 `projects/` 开头的 model 原样透传）。
- 启动后第一次调用会打印一行端点解析结果，便于核对；钉定生效时另有一行 `🌐 [上游端点] 已钉定 location: projects/...`。

> 📌 **勘误**：本文件上一版写的"Express 模式无法指定 location、报错里的区域无从干预"**是错的**——那个结论只验证了"不能给 Client 传 location"，漏了"可以用完整资源路径钉定"这条路。已按真机结果更正。

---

## Studio(Cookie) 通道：免登录 与 非 Express 项目

两种设想都做了真机验证（2026-08-14）：

| 场景 | 结果 |
|---|---|
| **不带 Cookie（免登录）** + Express 项目 | ❌ `Permission 'aiplatform.endpoints.predict' denied` |
| **不带 Cookie（免登录）** + 非 Express 项目 | ❌ `requires billing to be enabled` |
| 不带 Cookie + 空项目 | ❌ `Request contains an invalid argument` |
| 带 Cookie + Express 项目 | ✅ 正常出文 |
| 带 Cookie + 非 Express 项目（未开计费） | ❌ `requires billing to be enabled` |

结论与代码现状：

- **本通道用的 `batchGraphql` 控制台接口不接受匿名调用**，必须有登录态（SAPISIDHASH）。浏览器里"不登录也能用 Studio"走的是**另一条**面向未登录用户的接口/额度，本项目当前不支持。若你能抓到那条请求（F12 → Network → 复制 URL、请求头与 payload），加一条上游并不难。
- **代理里并没有"检测是不是 Express 项目就拒绝"的逻辑**——能不能用只取决于：① 有登录态 Cookie；② Project ID 填的项目**该账号有权限且已开启计费**。是否 Express 项目本身不是门槛。
- 但旧版有个**误导人的 bug**：`Permission ... denied on resource //…/projects/xxx` 这类**项目级**错误会命中"Cookie 过期"关键词，于是提示你反复重取 Cookie，怎么弄都好不了。现在已按错误内容分流：点名了具体项目/计费的错误会给**项目层面**的排查指引（检查 Project ID / 开启计费 / 账号权限），只有纯会话失效才提示重取 Cookie。

---

## Cookie(Studio) 通道的原生函数调用

Cookie 直连通道会把 OpenAI `tools` 转成 batchGraphql 的原生 `functionDeclarations`。`tool_choice=none` 会直接省略全部上游工具；`auto` / `required` / 指定自定义函数分别使用 Studio 的 `AUTO` / `ANY` 模式。嵌套对象与数组参数会递归转换成 Studio 私有 UI Schema。内建 `googleSearch` 没有可放进 `allowedFunctionNames` 的声明名，因此搜索请求不附加自定义函数的 `toolConfig`。

OpenAI 入口的 `tool_choice` 字符串在两个通道都兼容大小写。强制选择未声明的自定义函数、未知选择模式，或没有任何可用工具时使用 `required`，会在请求上游前返回 400。Express 的 `[Express 工具]` 日志会列出实际函数声明、搜索启用状态和工具选择方式，便于区分工具回传引起的第二轮请求。

Cookie 原生入口会检查 `toolConfig` 和 `allowedFunctionNames` 的格式；允许名称全部未声明时返回 400，不会通过删除名称限制改成任意函数调用。禁用工具或已优先选择搜索时，会跳过不会发送的函数 Schema 转换。

| 情况 | 行为 |
|---|---|
| 自定义函数声明 | 原生下发；流式与非流式响应都返回 OpenAI `tool_calls` |
| `-search` 模型 + 默认/`auto` 工具选择 | 优先内建搜索，跳过客户端附带的可选自定义函数（如 `get_time_info`）；显式 `required` / 指定自定义函数仍优先执行自定义函数 |
| 原生搜索工具类型 `web_search` / `web_search_preview` / `google_search` | 两个通道的 OpenAI 入口均识别并映射为 Google 内建搜索；纯搜索请求不附加函数专用配置 |
| 显式声明内建搜索与自定义函数 | batchGraphql 不允许二者混用：普通模型的混合 AUTO/required 请求优先自定义函数并忽略搜索；显式选择未声明为自定义函数的搜索别名时，仅发送 `googleSearch` |
| 自定义函数恰好叫 `search` / `google_search` / `web_search` | 两通道均保留其函数声明、参数和指定调用；不再按名字替换为内建搜索。需要 Google 搜索时使用 `{"type":"web_search"}`、`{"google_search":{}}` 或 `-search` 模型后缀 |
| 历史里的 assistant `tool_calls` | 回放为 role=`model` 的 `functionCall` Parts，并从 `extra_content` 或短期缓存恢复 thought signature |
| 连续的 role=`tool` 结果 | 按 `tool_call_id` 关联函数名，合并为 role=`user` 的 `functionResponse` Parts；并行顺序为 FC1, FC2, FR1, FR2 |
| 模型或协议明确拒绝原生函数 Schema | 仅本次固定降级为文本工具观测并重试一次；没有用户可见策略开关，也不会恢复旧版严格拒绝路径 |

工具历史按消息顺序关联结果；客户端跨轮复用 ID 不会改写先前的调用名。`extra_content.google.part_order` 中重复、布尔和越界索引会被忽略，避免同一调用被重复回放。Claude `tool_result.is_error: true` 会保留为 Gemini 的工具错误响应，供模型判断失败并选择下一步。

工具调用遇到输出截断或内容过滤时，仍返回 `length` / `content_filter`（Claude 对应 `max_tokens` / `refusal`）；只有正常完成才以 `tool_calls` / `tool_use` 交给客户端执行，避免将不完整调用误报为已就绪。

---

## 思维链守卫（`prefill_cot_guard`，默认开）

针对「**预填充确实卡掉了原生思维链，但预设自己的思维链也经常不写、直接出正文**」：

- **成因**：预填充只是把话头停在你预设自己的**未闭合思维链开标签**上，请求里**没有任何一句话**告诉模型"接下来必须先完成思考再写正文"。模型于是常常跨过思考直接写正文，输出里只剩一个孤立开标签，前端正则自然抓不到思维链。
- **做法**：自动识别预填充里未闭合的标签名（**不预设任何具体标签**，只按"开了没闭合"这一形状匹配，`<thinking>`、`<CoT>`、`<plan_1>`、`<分析~>` 之类自定义标签都能识别），在续写指令**末尾**（即模型最后读到的位置）追加一条硬性要求：先逐条写完该标签内的思考 → 用对应闭合标签收尾 → 然后才写正文，且不允许空标签。`smart` 与 `keep_turn` 两种模式都生效。
- **无副作用**：预填充里没有未闭合标签时（普通句子），本项什么都不做；生图模型不适用；可按模型单独开关。
- **仍不稳定时的对照实验**：把「原生思考控制」从 **关闭原生思考** 改成 **强制用上方档位 + `low`** 再测一轮。把档位压到 `minimal` 会把模型推向"直接给答案"的行为，**有可能连带跳过预设要求的长思维链**；`low` 保留一点推理惯性、同时仍显著少于默认档。这一条是机理推断，请以你自己的 A/B 结果为准。

---

## Cookie 直连模式配置指引（支持手机与电脑）

> ⚠️ **使用前请先读这段风险提示**
>
> Cookie 直连模式的原理是：用硬编码的网页客户端 key 和固定的 `querySignature`，
> 冒充 Google Cloud 控制台前端去调用其**私有** `batchGraphql` 接口。因此：
>
> - **无兼容性承诺**：这是内部接口，Google 改一次 `querySignature` 或参数结构就会全线失效，且不会有任何公告。
> - **条款风险**：以自动化方式访问非公开接口，可能与 Google Cloud 的使用条款相冲突，存在账号被限制或处置的风险。请仅用自有账号、自担风险。
> - **凭证敏感度极高**：配置的 Cookie 含 `__Secure-1PSID` 等完整会话凭证，等价于该 Google 账号的完整访问权。请勿把本服务部署到公开可访问的地方，务必设置强 `API_KEY`。
>
> 如果你需要的是稳定、可长期依赖的方案，请使用标准模式（Express API Key）。

在控制台切换到 **Agent Platform Studio (Cookie 直连反代)**，需配置 **Cookie** 与 **Project ID**：

### 1. 获取完整 Google Cookie
关键会话凭证（如 `__Secure-1PSIDTS`、`__Secure-1PSID`）带 `HttpOnly`，无法用书签脚本提取，需按下述方式获取：
- **电脑端**：登录 [Google Cloud Console](https://console.cloud.google.com) → 按 **F12** → **Network** → 刷新页面 → 点任意成功请求 → 复制 **Request Headers** 里的 `Cookie:` 整段，粘贴到控制台。
- **手机端**：iOS(Safari) 或 Android(Kiwi) 安装 `Cookie-Editor` → 登录控制台 → 插件 **Export** 为 **Header String** 或 **JSON** → 整段粘贴，系统自动解析。

### 2. 获取 Project ID
- 从控制台顶部项目选择器复制，或直接把含 `?project=xxx` 的整条 URL 粘贴到输入框，系统自动提取。

> ⚠️ **关于 Cookie 有效期**：通常较为持久——只要不退出登录、不修改密码、Google 未主动失效会话，一般可维持**数周甚至更久**（实测可用一个月以上），**并非只有 1~2 小时**。仅当接口确实报 `Permission Denied` / `predict denied` 时，重新获取并到控制台保存激活即可。

---

## 调用示例

### 查询模型

```bash
curl http://localhost:8050/v1/models \
  -H "Authorization: Bearer your_adapter_api_key"
```

### 非流式对话

```bash
curl http://localhost:8050/v1/chat/completions \
  -H "Authorization: Bearer your_adapter_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gemini-2.5-flash",
    "messages": [
      {"role": "user", "content": "用一句话介绍 Gemini。"}
    ],
    "stream": false
  }'
```

### 流式对话

```bash
curl http://localhost:8050/v1/chat/completions \
  -H "Authorization: Bearer your_adapter_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gemini-2.5-flash",
    "messages": [
      {"role": "user", "content": "写一首短诗。"}
    ],
    "stream": true
  }'
```

### Google Search 搜索增强（文本模型）

在模型名后加 `-search` 后缀：

```json
{
  "model": "gemini-2.5-flash-search",
  "messages": [
    {"role": "user", "content": "今天有哪些 Gemini API 相关更新？"}
  ]
}
```

Cookie 通道的搜索限制：

- `-search` 启用的是 Studio 内建 `googleSearch` 工具，模型会根据问题决定是否执行搜索。
- batchGraphql 不支持内建搜索与自定义函数混用。OpenAI/Claude 请求使用 `-search` 且工具选择为默认/`auto` 时，优先内建搜索，跳过客户端自动附带的自定义工具（如 `get_time_info`）。显式 `required` / 指定自定义函数时保留自定义工具；显式强制选择搜索工具时只发送内建搜索。
- Gemini 原生入口走 Cookie 桥接时也保留搜索后缀的优先级；`functionCallingConfig.mode=ANY` 保留显式函数调用要求。原生协议的 `NONE` 只禁用函数调用，不禁用已声明的内建搜索。
- `tool_choice: "none"` 会禁用搜索，即使模型名带 `-search`。
- 与已开启的抗截断后缀组合时（如 `-search-antitruncation-stream`），保留搜索并跳过抗截断传输。

运行日志中的 `[Studio 搜索]` 会说明搜索工具已启用，或因工具冲突、`tool_choice=none` 等原因被忽略。

Express 文本通道保留普通 `type: "function"` 声明的自定义函数语义，包括名称恰好叫 `search` 的函数。使用内建搜索请选 `-search` 后缀或声明原生搜索工具类型。

### 单次请求覆盖参数（任意前端可用）

通过请求体额外字段按需覆盖控制台默认：

```json
{
  "model": "gemini-3-pro-image",
  "messages": [{"role": "user", "content": "生成一只赛博朋克猫"}],
  "image_size": "2K",
  "aspect_ratio": "16:9"
}
```

（文本模型可用 `reasoning_effort`：`low`/`medium`/`high`，或 2.5 用 `thinking_budget` 整数。）

> **注**：`temperature`/`top_p`/`max_tokens` 等标准字段、以及上述扩展字段，其优先级恒高于"该模型专属"与"控制台全局"设置。若要为某模型设持久默认值又不想每次请求都带，请用控制台的"保存为该模型专属"。

---

## 模型列表配置

> 模型列表启动时先读取镜像内的本地 `vertexModels.json`，后台再从
> `https://raw.githubusercontent.com/bad-woman/vertex2openai/main/vertexModels.json` 更新。
> 网络失败或远程空列表不会清掉当前缓存，也不会阻塞启动和模型列表请求。
> 后台更新成功后以远程列表为准；要持续提供自己的模型清单，可以二选一：
> 1. 把更新后的 `vertexModels.json` 推到 GitHub `main`（推荐，改远程即可全部部署生效、无需重部署）；
> 2. 或设环境变量 `MODELS_CONFIG_URL` 指向你自己维护的地址。

默认模型列表在远程 `MODELS_CONFIG_URL` 或本地 `vertexModels.json`：

```json
{
  "models": [
    "gemini-3.7-flash",
    "gemini-3.6-flash",
    "gemini-3.5-flash",
    "gemini-3.5-flash-lite",
    "gemini-3.1-pro-preview",
    "gemini-3.1-flash-lite",
    "gemini-3.1-flash-image",
    "gemini-3-pro-image",
    "gemini-2.5-pro",
    "gemini-2.5-flash"
  ]
}
```

> ⏰ **停用时间线（核对于 2026-07-26，请自行复核官方 deprecations 页）**
> - `gemini-2.5-pro` / `gemini-2.5-flash` / `gemini-2.5-flash-lite`：**2026-10-16 停用**。届时 `thinking_budget` 分支与"2.5 原生预填充透传"路径将没有活模型，可整体删除。
> - `gemini-3-flash-preview`：官方推荐替代为 `gemini-3.5-flash`，但**该模型目前仍可正常调用**（2026-07-26 用 Express Key 实机验证通过），因此**保留在默认清单中**；待官方正式停用后再移除。
> - `gemini-3.1-flash-image` / `gemini-3-pro-image`：GA 版本（不带 `-preview`），对应的 `-preview` 版本已于 2026-06-25 停用。

`/v1/models` 会自动为**非生图**的 Gemini 模型生成带 `-search` 后缀的别名。新增模型只需把 ID 加进此列表即可（能力自动归类，无需改代码）。

---

## 抗截断模型选项

默认仅 **`gemini-3.8-flash` 和 `gemini-4-argon`（Gemini 4a / Argon）** 在 `/v1/models` 中显示流式与非流式抗截断后缀。其它模型默认仅提供普通名称和原有搜索别名，不添加抗截断后缀。

**普通无后缀名称始终不启用抗截断，包括 3.8 Flash 和 4a。** 只有选择已开启的抗截断后缀才会使用该传输方式，无需额外运行 Node.js/Go 网关或配置客户端正则。

| 模型名称示例 | 行为 |
|---|---|
| `gemini-3.8-flash` / `gemini-4-argon` | 普通入口，不启用抗截断 |
| `gemini-3.8-flash-antitruncation-nonstream` | 上游完整返回后还原正文；客户端开启 `stream` 时仍以 SSE 交付 |
| `gemini-3.8-flash-antitruncation-stream` | 上游支持时增量还原正文；客户端关闭 `stream` 时返回普通 JSON |
| `gemini-3.6-flash` | 普通入口，不启用抗截断；在设置中开启后才添加其抗截断别名 |

在控制台「模型参数」中先选择具体模型，将「模型列表中的抗截断后缀」设为“不显示”“显示两种”“仅非流式”或“仅流式”，点「保存为该模型专属」，再刷新客户端模型列表。设置只控制该模型的后缀，不会改变普通入口，也不支持全局开启。

例如，先为 `gemini-3.6-flash` 开启流式后缀，再发送：

```json
{
  "model": "gemini-3.6-flash-antitruncation-stream",
  "stream": true,
  "messages": [{"role": "user", "content": "请完整解释这个算法"}]
}
```

配置字段 `anti_truncation_mode` 现在仅控制各模型发布的后缀：`off` 不发布，`stream` / `nonstream` 仅发布对应后缀，`both` 发布两种。已保存的模型专属值继续有效；旧全局值不再应用。请求体中的同名旧参数保留接收兼容，但不能开启抗截断或覆盖所选后缀；需要普通回复请使用无后缀名称。

手动填写 `-antitruncation` 短后缀仍对应流式模式，但同样要求该模型已开启流式后缀；未开启的后缀返回 400。别名只在本地解析，上游收到原始模型 ID。清除模型专属设置后，3.8 Flash / 4a 恢复显示两种后缀，其它模型恢复不显示。

### 实现与兼容范围

代理为每次请求声明一个随机命名的正文传输函数，要求模型把正文写入其 `content` 字符串。返回时在原生响应解析阶段将该函数还原为普通 assistant 文本；**不执行函数，也不把临时函数调用交给客户端**。真实自定义函数的声明、调用和思考签名保持独立，仍由原有工具链处理。

| 请求路径/能力 | 流式抗截断的实际交付方式 |
|---|---|
| Express + 支持增量函数参数的 Gemini 3+ 文本模型 | 发送 `streamFunctionCallArguments`，直接读取 `partialArgs` 中的字符串片段并输出 `delta.content` |
| Gemini 3.1 Flash-Lite、旧模型、旧 SDK | 等完整函数参数返回后还原；不发送不支持的增量参数字段 |
| Cookie / batchGraphql | 等完整函数参数返回后还原；不假设私有协议支持原生增量参数流 |
| 请求声明真实工具或携带工具历史 | 保留真实工具；正文函数参数完整后还原，不启用增量函数参数流 |
| 全局假流式、非流式抗截断模式 | 等上游完整响应后交付 |
| `/v1beta/models/...`、`/v1/models/...` 原生 POST 透传 | 保持原始请求和响应，不应用此选项；原生模型目录不加入本地抗截断别名 |

Google 的增量函数参数流有模型限制，详情见[官方说明](https://docs.cloud.google.com/vertex-ai/generative-ai/docs/multimodal/function-calling#streaming_function_call_arguments)。上游明确以 400 拒绝该字段时，且尚未交付内容，Express 会移除该字段再使用完整参数模式。

生图、音频、Live、嵌入等非文本生成模型不添加抗截断别名。搜索、结构化输出、多候选、旧版 `functions` 协议，以及显式 `tool_choice=none/required/指定函数` 的请求会自动跳过包装并记录原因，以保留客户端约定。搜索模型不再额外组合抗截断别名。

包装在现有控制台注入与预填充处理之后进行，原有预填充拼回和去重仍适用。429 使用原来的单 Key/多 Key 重试预算；输出已开始后不重发整轮。缺少上游结束标记、正文参数类型错误、重复正文调用等情况返回明确错误；`MAX_TOKENS` 和安全过滤结束原因分别保留为 `length`、`content_filter`，不会伪装成完整成功。

普通 OpenAI 兼容请求同样检查上游结束标记，覆盖 Express / Cookie 的真流式、假流式和非流式路径；收到部分内容后静默断流会报告 502，不计为成功，也不会自动重发整轮。原生 Gemini 接口继续保持正文透传；网络错误返回 Google 风格错误 JSON，断连和错误处理会关闭上游响应。

OpenAI 对话 SSE 在上游安静等待、长思考或完整函数参数尚未返回时，每约 10 秒发送一次注释保活。Cookie 的非流式抗截断和全局假流式也会先建立 SSE，再等待上游完成。保活不会取消或重发上游调用，不占续传事件 ID，也不会保存进响应缓冲；关闭直通连接会取消等待中的生成，开启续传时仍由后台任务继续收集回复。

### Cloudflare 橙云下的长等待

Cloudflare 当前文档列出的默认源站读取超时为 **125 秒**，上传写入超时为 **30 秒**（参见[连接限制](https://developers.cloudflare.com/fundamentals/reference/connection-limits/)与[524 说明](https://developers.cloudflare.com/support/troubleshooting/http-status-codes/cloudflare-5xx-errors/error-524/)）。
假流式应同时在客户端使用 `stream:true`：本服务在准备超过约 50ms 时先建立 SSE，随后发送心跳，
覆盖图片预处理、代理启动、上游生成和退避等待；即使将假流式专用心跳间隔设为 0，外层仍有约 10 秒的注释保活。
响应带 `Cache-Control: no-store, no-transform` 与 `X-Accel-Buffering: no`，降低缓存、转换和缓冲造成的延迟。

如果前面还有 Nginx，应让它转发每次输出，例如：

```nginx
location /v1/ {
    proxy_pass http://127.0.0.1:7860;
    proxy_http_version 1.1;
    proxy_set_header Connection "";
    proxy_buffering off;
    proxy_cache off;
    gzip off;
    proxy_read_timeout 180s;
}
```

同样给 `/v1beta/` 的原生 SSE 路由配置；Cloudflare 的自定义 Cache Rules / Worker 也应对 API 跳过缓存，
流式转发响应，避免先完整读取响应再返回。客户端自身的读取/首条内容超时也需要足够长。
本地 TCP 回归验证了持续输出，未通过真实 Cloudflare 域名验证；心跳不能保证规避平台的其它限制。

真正的 `stream:false` JSON 在完成前没有可发送的正文，不能用 SSE 注释保活。超长任务应改用流式、
已取得续传 ID 后的分片轮询，或使用可承载长请求的入口。提高本服务的总期限不会改变 Cloudflare 的限制，
响应心跳也无法解决上传或建连阶段的超时。

### 请求监控与详细日志

控制台新增“请求监控”：分别统计前端连接、上游任务、前端断开后仍在运行的后台任务及退避等待任务。
续传连接单独列出，并链接原上游请求，不把多个读者重复算成多个上游生成。支持按状态、模型与请求 ID
筛选，活跃请求置顶，分页列表与详情每 2 秒自动刷新，页面隐藏后暂停刷新。

点击请求行，或运行日志中的 `trace-…`，查看模型、通道、真/假流式、开始/结束时间、总耗时、
上游尝试与重试、无新数据时长、字节/事件数、正文与思考字符数、用量、结束原因和关联日志。
时间线分别记录上游首分片、模型首回复（包含思考、正文、工具调用、图片）、首段思考、服务端正文首字，
以及有效回复和正文通过 ASGI 发送的时刻；详情同时显示当前尝试的首回复等待倒计时、断开时间与续传窗口截止。
心跳、空内容、角色、纯用量与签名不计为有效模型首回复；ASGI 发送完成不等于客户端已收到或渲染。
首字时间是从本次 HTTP 请求到达起计算，续传读者的详情可跳转原任务查看生成阶段的时间。
服务在请求体读取完后独立监听 ASGI 断连信号，不必等下一次输出或心跳才更新连接状态。

每个 OpenAI / Gemini 原生 POST 请求的响应增加 `X-Trace-Id`，控制台和标准输出日志均带关联 ID。
管理接口 `GET /api/requests?status=active&q=gemini&offset=0&limit=50` 与
`GET /api/requests/{trace-id}` 使用控制台登录会话鉴权；响应不缓存。

登录和控制台均只使用本地静态资源，不依赖外部字体、Tailwind CDN 或图表脚本。概览成功率按成功与错误计数计算，
重试单独统计；Token 条按实际输入 / 输出占比显示。桌面采用宽屏布局，手机上的请求表格转为卡片，详情保留关键时序，
交付与上游诊断可展开查看。日志支持内容搜索、错误 / 警告筛选、自动滚动开关与连接状态提示。

模型参数与全局运行设置分别保存，模型保存栏随对应分区滚动；思维链守卫位于模型行为区，可按模型覆盖。
加载失败时可重新加载配置，未成功加载的配置禁止保存；项目探测会保留尚未保存的 Key、Project ID 与代理编辑。
切换模型会保留当前页面内各模型与全局默认的独立草稿，并标记“未保存”；刷新页面后草稿清空。保存或探测期间，
对应分区暂时禁止编辑，避免回包覆盖新输入；不同分区可分别保存，返回的数据只合并对应字段。参数错误会标记并定位到具体输入框，
采样说明同步反映当前选择。分区链接支持直接打开与刷新定位；手动关闭日志自动滚动后，滚动到底部不会自动重启该开关。
新增 Tailwind 工具样式后，可在开发环境运行 `sh scripts/build_dashboard_css.sh` 重新生成已提交的
`app/static/dashboard.css`；手写组件样式维护在 `app/static/console.css`。部署与 Docker 运行不需要 Node。

前端交互可另用 Playwright 检查（需先安装 `requirements-dev.txt`）：

```bash
npm install --no-save --prefix /tmp/vertex-console-ui playwright
NODE_PATH=/tmp/vertex-console-ui/node_modules /tmp/vertex-console-ui/node_modules/.bin/playwright install chromium
NODE_PATH=/tmp/vertex-console-ui/node_modules node scripts/verify_console_ui.cjs
```

此检查启动临时状态目录中的本地服务，使用虚构凭证与请求数据，不访问 Google 上游；覆盖登录、模型 / 全局保存、
凭证草稿、请求详情、日志筛选、错误恢复及 320–1440px 响应式布局。可通过 `PLAYWRIGHT_CHROMIUM_EXECUTABLE`
指定已有 Chromium，并通过 `UI_SCREENSHOT_DIR` 保存检查截图。该浏览器检查独立于默认 Python 离线检查。

诊断只驻留当前进程，不写入凭据文件，不保存请求正文或完整生成正文；日志按原有诊断内容脱敏、截断。
已结束记录最多 500 条、1 小时，诊断文本按 16 MiB 预算优先淘汰旧的完成记录；活跃任务不会因此取消。
每条最多 160 个阶段事件与 200 条关联日志；单个 SSE / JSON 诊断副本最大 2 MiB，超大事件仅记录字节数，
不影响原响应转发。诊断预算不等同于进程 RSS。重启后清空，多 worker / 多实例之间不共享。

### 断连缓冲与轮询

开启续传后，准备阶段前端断开也会保留同一次上游调用。新请求在准备超过约 50ms 后先建立 SSE 并保活；
此后发生的准备错误通过 SSE 错误事件交付，快速准备失败仍保留原 HTTP 错误码。

每次生成拥有独立的 `X-Resume-Id`。事件 ID 为 `<生成ID>:<序号>`；重连传回完整的 `Last-Event-ID`，
或者传 `X-Resume-Id` 加 `?resume_from=<下一帧序号>`。ID 直接查找原生成，且校验原请求体；
不存在/过期返回 404，请求体或游标冲突返回 409，无效游标返回 400。旧生成不会被新一轮覆盖。
兼容仅传数字的旧游标，但它必须结合原请求与幂等头；新客户端应使用完整事件 ID。

每次逻辑请求建议提供独立 `Idempotency-Key`，网络重试沿用该键，主动重新生成则更换。
同键同体会重放同一次生成，已完整交付的回复在 TTL 内保持幂等；未交付且最后一个读者已断开时仍受下方续传窗口约束。
未提供幂等键时保留请求体指纹兼容行为：正在生成或未交付时复用，已交付后视为新生成。
共享 API Key 的独立请求应使用不同幂等键，避免相同请求被合并。

**酒馆等客户端的取消与重新生成：** 默认前端取消只断开读者，后台生成仍继续。最后一个读者断开后，
10 秒内再次发送完全相同的请求会接回旧生成，日志显示“续传命中”；超过 10 秒后再次发送会丢弃旧缓冲、
取消旧上游任务并等待其释放名额，再开始新一轮。还有其他客户端读取时，不会因其中一个客户端断开而替换任务。
不同请求可以并发，不需要等上一轮结束。

控制台“请求与缓冲预算”的 `resumable_disconnect_grace_seconds` 可调整此窗口，0 表示断开后不再复用。
每次最后一个读者断开时采用当时的设置；服务无法区分主动取消与网络中断，两者使用相同窗口。
窗口不是后台任务的自动取消定时器：没有新的相同请求时，后台仍在总期限与容量预算内继续收集。
若重连仍携带已超窗口的旧 `X-Resume-Id` 或游标，返回 410，客户端需移除旧 ID / 游标再重新生成，
避免把旧偏移错误地用在新答案上。

若希望取消后直接重新生成，可关闭控制台“弱网续传”，或给 OpenAI 请求添加 `X-Resumable: false`
（也接受 `0` / `no`）：本次请求直通，不建立缓冲，断连时释放本次上游读取与并发名额。
该头不能与 `X-Resume-Id` / `Last-Event-ID` / `resume_from` 同时使用，冲突返回 400。
已启动的后台生成不受新请求的关闭续传头影响，仍按原期限结束。保留续传时，每次主动重新生成更换
`Idempotency-Key`，网络重试沿用原键；不要把一个固定键用于所有生成。

长提示词、高思考档位或输出上限不足都可能导致长时间没有正文；“思考与生成”token 总数不等于正文长度。
Express 真流式日志会区分首个上游分片、首段正文、正文/思考字符数与结束原因；只有结束标记或签名、
没有实际内容时返回明确错误，只有思考时保留响应并记录空正文警告。续传模式等待期间每分钟记录一次状态。
默认每次上游尝试等待模型首回复最多 15 秒，超时作为 504 进入现有重试流程；单 Key 也可重试。
计时从实际发起上游调用开始，收到思考即结束，不再要求正文在 15 秒内出现；收到有效输出后也不会因本策略自动重发半截答案。
SSE 心跳与元数据不会重置计时，退避仍由 `retry_backoff_seconds` / `retry_jitter_seconds` 控制，
受 `retry_max` 与整轮 1800 秒期限约束，不会无限重试。尚未收到响应不等于收到 429，日志会区分首回复超时与上游错误。
`first_reply_timeout_seconds` 可在“模型参数”① 区修改全局默认或模型专属值，0 表示关闭（范围 0–7200 秒）。
假流式、非流式及整块生图无法提前观察生成中的思考，计时覆盖完整上游响应；较慢模型可单独调大或关闭。
模型已回复后的读取仍受原网络超时（SDK 默认 600 秒）及总期限约束。日志中的返回事件数不代表正文 token 数量。

`GET /v1/resumable/{id}?from=<seq>&limit=200&wait=10` 返回事件分片；非流式缓冲在 `done=true` 时提供
`payload` 和上游 `status_code`。响应头已对跨域浏览器暴露。JSON、轮询和 SSE 交付标记均在实际 ASGI
发送成功后更新；这不是客户端应用层收件确认。

Gemini 原生 `streamGenerateContent?alt=sse` 默认增加安全的事件间注释保活，并检查事件和候选结束标记。
原始 JSON data 内容保持不变。需要后台缓冲/续传时加 `X-Resumable: true`；之后同样使用
`X-Resume-Id` / 完整事件 ID 或轮询恢复。原生 SSE 不添加 OpenAI 的 `[DONE]`。非 SSE 原生响应仍按原协议透传。
上游半途断流时返回错误，不把缓存中的半截内容当成完整成功，也不自动拼接另一轮生成。

默认预算（控制台“请求与缓冲预算”可调整）：

| 设置 | 默认值 | 含义 |
|---|---|---|
| `request_timeout_seconds` | 1800 秒 | 准备、退避、生成及读取的整轮期限 |
| `resumable_disconnect_grace_seconds` | 10 秒 | 最后一个读者断开后的复用窗口，0 为不复用；超过窗口的新相同请求替换旧任务 |
| `upstream_max_concurrency` | 32 | 包括断开前端后仍在运行的后台生成；满时返回 503 |
| `resumable_max_buffer_mb` | 64 MiB | 单次回复编码字节预算 |
| `resumable_max_total_mb` | 256 MiB | 已登记副本总编码字节预算 |
| `resumable_max_streams` | 200 | 缓冲数量上限 |
| `resumable_ttl_seconds` | 1800 秒 | 完成后的保留时间 |

字节预算按编码数据计数，不等同于 Python 进程 RSS；终止错误保留少量额外空间。
容量不足优先清理可回收的完成副本；没有空位时拒绝新生成，不取消其它正在运行的请求。
会话和回复副本目前位于进程内，请使用单 worker/单实例；服务重启、实例丢失或超出保留期后无法继续旧回复。
代理和客户端应关闭 SSE 缓冲，并让超时覆盖预期生成时间。心跳不能保证物理网络不断线，恢复需要客户端配合。

控制台更新的 Cookie / Project ID 优先于环境变量；环境变量只在尚未保存对应配置时作为初始值。按模型更新部分参数时会保留其它已保存字段，避免调整思考档位时把该模型明确关闭的抗截断重新打开。生图 system 指令开关在两条通道都按模型专属设置生效。

这是可选的输出传输方式，**不能保证消除所有截断或模型拒绝**，也不增加模型输出 Token 上限。临时函数声明和控制消息会增加少量输入开销。当前验证使用模拟上游，真实模型/项目支持情况需要部署后确认。输入 Unicode 转码和实验性上下文图片化未纳入本次实现。

### 思路与来源

本功能的合成函数传输思路参考：

- [ken050210/vertex-streaming-anti-truncation](https://github.com/ken050210/vertex-streaming-anti-truncation)：原生 `partialArgs` 流式还原及模型传输版本划分。
- [Xeltra233/Antigravity-gateway](https://github.com/Xeltra233/Antigravity-anti-truncation-gateway)：动态合成正文工具、真实工具隔离与回复还原；另查阅了其 [experimental 分支](https://github.com/Xeltra233/Antigravity-gateway/tree/experimental)。
- 原始讨论：[LINUX DO](https://linux.do/t/topic/2619263)、[Discord 抗截断思路](https://discord.com/channels/1134557553011998840/1543451029553545346)、[Discord 转码讨论](https://discord.com/channels/1134557553011998840/1475468066640105514)。Discord 原文需相应访问权限，本次未读取。

感谢原作者及讨论参与者。本项目根据现有 Python SDK / batchGraphql 架构独立实现，未移植输入图片化或宣称上游限制被完全消除。

---

## 关于 429 报错与并发控制

429（Resource Exhausted）常因上游限额不足或请求频率过高。项目已内置退避重试，另建议：
- 服务端默认将上游并发限制为 32，后台生成也计入；可在控制台调整。
- 重试使用固定间隔加随机抖动；有效 `Retry-After` 按配置窗口钳位，间隔不随次数增长。
- 控制客户端并发频率，重试沿用同一个幂等键。
- 适当减小最大输出 Token。
- 配置多个有效 Express Key 并开启轮询。
- 及时更新失效或权限受限的 Google Cookie。

---

## 后续升级与扩展

- **新增模型**：把模型 ID 加入 `vertexModels.json`（或远程 `MODELS_CONFIG_URL`）。`model_capabilities.py` 按**家族/版本模式**自动归类（思考方式、采样裁剪、生图比例/分辨率、预填充），**未知/未来型号按"最新代"前向安全处理**。基本即插即用。
- **迁移到 Interactions API**：代码按上游通道解耦（`app/upstreams/` 下各类实现 `BaseUpstream`；能力判定、消息转换、参数构建均可复用），新增一个 `InteractionsUpstream` 并在路由层接入即可。

  现状（核对于 2026-07-26）：
  - **Gemini Developer API 侧**：Interactions API 已于 2026-06 **GA**，官方推荐新项目使用；`generateContent` 被标为 legacy 但继续完整支持。
  - **Agent Platform 侧**：Interactions API 仍标注为 **experimental**。
  - **Express 模式**：REST 面只有 `countTokens` / `generateContent` / `streamGenerateContent` —— **这才是本项目暂不迁移的直接原因**（本项目走的正是 Express 通道）。

---

## 本地开发与检查

目录职责和扩展位置见 [代码结构与维护约定](docs/architecture.md)，
最新问题与修复见 [全面审查与优化](docs/comprehensive-review.md)，
此前记录见 [项目审查与优化记录](docs/optimization-review.md)。
模块职责与维护入口见 [架构约定](docs/architecture.md)，
本轮结构调整与复查结果见 [可维护性重构记录](docs/maintainability-2026-10-25.md)。
应用入口为 `app/main.py`，管理路由位于 `app/routes/`，页面位于 `app/templates/`；
离线回归集中在 `tests/`，`scripts/` 保留检查入口与诊断工具。

```bash
# 安装运行和开发检查依赖
pip install -r requirements-dev.txt

# 完整检查：语法、静态分析、工作流、模块边界、所有回归及 SDK 档案兼容
python scripts/check_project.py

# 控制台格式与交互检查（开发时可用 npm run format:console 自动格式化）
npm ci
npm run check:format
npm run test:console

# 单独运行一组回归
python tests/test_generation_config.py

# 本地启动
cd app
uvicorn main:app --host 0.0.0.0 --port 7860
```

测试使用隔离临时状态和模拟上游，不需要真实 Key，也不会修改部署配置。
CI 在 Python 3.11 / 3.12 上执行同一套检查，通过后才构建镜像。

请求体按实际接收字节限制：登录 256 KiB、其他管理写入 8 MiB、生成接口
32 MiB；无 Content-Length 的分块上传同样生效。错误响应保留对应 API 协议，
参数校验失败不回显密码或完整请求体。远程模型清单限制为 1 MiB；Express
项目探测最多同时执行 4 路，响应限制为 256 KiB。Cookie 会话探测同样最多
4 路并发、读取最多 256 KiB，单次总时限 45 秒（含代理切换）；批量探测保持
账号顺序，单项失败不影响其他账号。空响应、登录 HTML 或畸形结果不会被判为
有效会话。替换账号的 Cookie 或 Project ID 后，旧凭证的冷却状态自动失效，
旧请求迟到的结果不会修改新凭证的健康状态。Cookie 和 `cookie_authuser`
保存时校验请求头字符，拒绝换行、控制字符和非 ASCII 内容。
出站保活只读取响应头，
最多跟随 5 次重定向，单次总时限 30 秒，忽略环境代理并支持 `SSL_CERT_FILE`。
