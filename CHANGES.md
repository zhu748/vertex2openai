# vertex2openai 三轮重做 — 变更说明

基于原版 `vertex2openai-main`（2026-09-01），只做加法，未改原有架构。

## 第七轮：Express API Key 通道请求头指纹（User-Agent / x-goog-api-client 接管）

- **诊断**（本地 mock 上游抓包实测）：第六轮只伪装了 Cookie 通道，Express Key 通道仍有指纹盲区——
  - SDK 主通道 UA / x-goog-api-client 由 google-genai 自动管理（`google-genai-sdk/<ver> gl-python/<ver>`）；
    SDK 有强制标签机制：注入值不含**当前精确的**两个标签就会被前置/追加（实测出现双重标签）。
  - **4 处裸 httpx 出站点**发 `python-httpx/<ver>` UA（最显眼的脚本指纹）：Gemini 原生透传
    （更糟：UA 硬编码 `google-genai-sdk/vertex2openai-passthrough` 自曝代理身份）、Key 探测、
    模型配置拉取、代理连通性测试。
  - Key 探测把 API Key 放 URL query（`?key=…`），官方 SDK 的真实形态是 `x-goog-api-key` 头。
- **新模块 `app/express_fingerprint.py`**：运行时读取 google-genai 与 Python 真实版本号构造
  精确标签，让 SDK 的 `append_library_version_headers` 三个分支全部落空、原样放行注入值。
  档案（控制台「通道与凭证」页热改，默认 `sdk` = 零改动）：
  - `sdk`：官方 Python SDK 形态（SDK 自动管理，行为与历史版本一致）；
  - `node`：官方 Node.js SDK 形态（`gl-node/22.14.0`，JS 生态调用量最大；裸出站点为纯 node 形态，
    SDK 主通道会被 SDK 追加 gl-python——已知限制，标签仍各出现一次）；
  - `gcloud`：gcloud CLI 运维形态（追加 `gcloud/<ver> command/gcloud.ai-platform gccl/<ver>`）；
  - `custom`：自定义串，占位符 `{sdk}` `{python}` `{node}` 自动展开，缺标签自动补齐（防双重标签）。
- **接入点全覆盖**：`http_options.get_http_options()`（SDK 主通道注入）、`gemini_native._passthrough_once`
  （删除自曝 UA）、`express_key_probe.probe_project_id`（Key 移到 header + UA 注入）、
  `model_loader.fetch_and_parse_models_config`、`main.test_proxy`（代理测试请求头）。
- **控制台**：「通道与凭证」页新增「Express 通道请求头指纹」卡片（下拉 + 自定义框 + 状态徽标）；
  `/api/settings` 前置白名单校验（非法档案 400）；启动横幅打印当前指纹形态。
- 回归测试：`scripts/regression_test4.py` 21/21（四档案 UA 构造、SDK 放行无双重标签 E2E、透传/探测/
  代理测试抓包验证、Key 走 header、设置校验、sdk 档案零改动回归）；旧 test2 21/21、test3 33/33 不破。

## 第六轮：Gemini 原生格式透传 + Cookie 通道指纹伪装

- **Gemini 原生 REST 透传**（`app/routes/gemini_native.py` 新增）：
  - 端点：`POST /v1beta|/v1/models/{model}:generateContent` / `:streamGenerateContent`
    / `:countTokens` / `:embedContent`；`GET /v1beta/models`（Gemini 原生格式列表）与
    `GET /v1beta/models/{model}`。请求体一个字节不改地转发到 Express 端点，原生字段
    （tools / thinkingConfig / safetySettings / responseSchema / cachedContent…）100% 兼容。
  - 鉴权兼容三种原生写法：`Authorization: Bearer` / `x-goog-api-key` / `?key=`（常量时间比较）。
  - 复用 Express Key 池（轮询/随机）、location 钉定、出站代理；429/5xx 自动换 Key 重试（≤3 次）；
    上游错误状态码与 body 原样透传；流式按 `aiter_raw()` 字节级转发 SSE。
  - 路由防遮蔽：`GET /v1/models` 保持 OpenAI 格式不动，Gemini 原生列表只注册 `/v1beta/models`。
  - 流式连接生命周期：客户端与上游响应用 `build_request + send(stream=…)`，流式时由转发生成器
    的 finally 释放（修掉 `client.post()` 预读导致 `StreamConsumed`、以及 `async with` 提前关连接两个坑）。
- **Cookie 通道 TLS/浏览器指纹伪装**（`app/impersonate.py` 新增，curl_cffi 0.16 终于用上）：
  - 旧通道用 httpx：Python TLS 指纹 + 无 User-Agent，极易被风控识别为脚本流量。
  - 现按档案（chrome 默认 / edge / safari / safari_ios / firefox / off）同时伪装 TLS 指纹
    （ClientHello/cipher/扩展序）与浏览器头（UA/sec-ch-ua/sec-fetch-*/accept-language），
    Google 专用头（Authorization/Cookie/origin）不受影响。
  - `open_upstream_client()` 统一工厂替换 cookie_proxy 三处 `httpx.AsyncClient`；
    off 档位逐字节回退旧 httpx 行为。控制台「通道与凭证」页新增伪装卡片（热切换）。
  - 垫片踩坑记录（curl_cffi 0.16.3 实测）：① 流被 `aiter_content` 消费后再调 `acontent()` 会
    **永久死等**（httpx 的 aread 幂等）→ 垫片自缓冲；② 流式响应未读时 `.content` 是 `b''`
    而非 `None` → 显式 `from_stream` 标记；③ `aiter_text` 必须是"普通方法返回异步生成器"
    才与 httpx 调用约定一致。
- 回归测试 33 项全过（`scripts/regression_test3.py`：本地 mock Google 上游全链路——
  鉴权×3、字节级透传、SSE 转发、模型列表双格式、伪装开/关行为等价性、aread/aiter_text 两种
  真实用法），旧 21 项回归不破，compileall 通过。

## 第五轮：全面复查加固（安全 + CI/CD + 修复一个回退 Bug）

- **修复：清空代理被环境变量抢回**（`app/runtime_state.py`）：
  旧实现 `stored or PROXY_URL` 在用户把代理**清空保存**（存 `""`）时会因 falsy 回退到环境变量值，
  导致「恢复直连」永远做不到。新增 `get_raw_stored_setting()`（只取用户保存过的原始值，不落回默认值），
  `get_effective_proxy_url()` 据此区分「保存为空 = 强制直连」与「从未保存 = 跟随环境变量」。
- **安全：API Key 常量时间比较**（`app/auth.py`）：`==` 改 `secrets.compare_digest`，
  与控制台登录口令校验保持一致，消除时序侧信道。
- **安全：代理 URL 保存/测试前置格式校验**（`app/main.py`）：新增 `validate_proxy_url_format()`，
  仅允许 http/https/socks5/socks5h/socks4 且必须有主机地址；畸形值在 `/api/settings` 与
  `/api/proxy/test` 直接返回 400（带中文原因），不再等 8 秒超时，也防止把「存了坏代理 → 全部上游请求失败」
  的状态写进设置。前端 `saveProxy()` 同步展示后端返回的具体错误。
- **安全：Dockerfile 非 root 运行**：新增 `app` 用户（UID 1000），`COPY --chown` + `USER app`；
  加 `PYTHONDONTWRITEBYTECODE` / `PYTHONUNBUFFERED` / `PIP_NO_CACHE_DIR`。
- **运维：镜像内置 `HEALTHCHECK`**（HEAD `/healthz`，slim 镜像无 curl 故用 Python 标准库）。
- **CI/CD：重写 GitHub Actions**（`.github/workflows/docker-image.yml`）：
  权限从 `write-all` 收敛到 `contents:read + packages:write`；PR 只构建验证不推送（旧版 fork PR
  会因无权限失败、还可能误覆盖标签）；push main 打 `latest` + `main-<sha>`，推 `v*` tag 打语义化版本；
  QEMU 多平台（amd64/arm64，PR 时只验 amd64 提速）；buildx GHA 层缓存；
  新增 `test` 前置 job：compileall + 真实 `import main` 导入冒烟 + workflow 自身 YAML 语法校验
  （`.github/scripts/check_workflows.py`），把「构建成功但启动即崩」的镜像挡在门外。
- **文档**：README 新增「镜像获取与自建」「健康检查端点 /healthz」章节；`PROXY_URL` 行为说明补全；
  `.gitignore` 增加 `data/`、`web_state.json`（运行时凭据文件绝不入库）。

## 第四轮：出站代理控制台化 + /healthz 健康检查

- `app/config.py`：`DEFAULT_SETTINGS` 新增 `proxy_url`（初始值取环境变量 `PROXY_URL`），
  代理从此进入「控制台可热改 + 持久化到 web_state.json」体系（与 roundrobin 等开关同模式）。
- `app/runtime_state.py`：新增统一读取口 `get_effective_proxy_url()`（控制台设置优先、环境变量兜底）
  与脱敏函数 `mask_proxy_url()`（认证信息打码，保留协议与 host:port）。
- 代理应用点全部切换到统一读取口（5 处，两条通道 + 辅助请求全覆盖）：
  `http_options.py`（Express SDK）、`upstreams/cookie_proxy.py`（Cookie 通道）、
  `express_key_probe.py`（Key 探测）、`model_loader.py`（模型列表）、`message_processing.py`（远程图片抓取）。
  控制台改完代理后**所有出站路径立即同步，无需重启**。
- `app/main.py`：
  - **`GET/HEAD /healthz`**（公开、无鉴权、零上游请求）：返回服务状态、当前通道、Express Key 数量、
    Cookie 是否配置、代理启用状态（脱敏）。供 Render 健康检查 / UptimeRobot 保活 / 负载均衡探活。
  - **`POST /api/proxy/test`**（需登录）：代理连通性测试。优先测请求体里传入的候选值（先测后存），
    为空则测当前生效代理；两者皆空测直连。通过代理请求一次 `aiplatform.googleapis.com`，
    任何 HTTP 状态码都算链路可达，返回延迟毫秒数与脱敏后的代理地址。
  - lifespan 启动日志打印代理状态（脱敏）——「代理到底启没启用」在日志里一眼可见。
  - 控制台「通道与凭证」页新增**「出站代理（网络出口）」卡片**：
    状态徽标（已启用/直连）、代理输入框（明文可编辑）、「测试连通性」「保存代理」按钮、
    格式说明（含代理商 `socks5://IP:端口:用户名:密码` → 标准格式的转换口诀）。
- `README.md`：环境变量表 `PROXY_URL` 说明同步更新。

## 第一轮：Express Key 多账号管理

- `app/runtime_state.py`：Express Key 记录管理（增删改、启用/禁用、Project ID 与来源）；
  环境变量只做**一次性**初始导入——用户在控制台清空保存后，重启不会再把环境变量的 Key 复种回来。
- `app/express_key_manager.py`：多账号记录选择；`get_express_record()` 返回该 Key **自己**的 project_id。
- `app/express_key_probe.py`（新增）：经 Identity Toolkit 自动探测各 Key 的 Project ID；单个/批量；失败不阻断调用。
- `app/upstreams/express_sdk.py`：location 钉定优先用当前 Key 自己的项目；回退顺序：Key 自有 Project ID → 全局 Project ID → 裸模型名。
- `app/main.py`：lifespan 中一次性导入环境变量 Key、启动后台 Project ID 探测；控制台新增 Express Key 多账号卡片
  （明文显示 Key、增删改、启用/禁用、Project ID 与来源、单个探测/探测全部、保存空列表需确认）；
  新增 API：`GET /api/express-keys`、`POST /api/express-keys`、`POST /api/express-keys/probe`、`POST /api/express-keys/project`。

## 第二轮：弱网可靠交付（服务端缓冲 + 断线重放，不用 SSE 心跳）

- `app/resumable.py`（新增）：`ResumableBuffer` / `ResumableStore` / `DetachedRequest` / `compute_stream_id` /
  `pump_stream` / `sse_from_buffer`；TTL 清理、容量淘汰、并发锁。
- `app/routes/chat_api.py`：`/v1/chat/completions` 包一层可靠传输——
  - 流式：缓冲命中（未完整交付）则凭 `Last-Event-ID` / `?resume_from=` 重放/续传，不再打上游；
    否则建缓冲、后台泵抽帧（`DetachedRequest` 让上游读取与前端连接解绑），当前连接只做缓冲的读者；
    每帧带 `id: <seq>`，`X-Resume-Id` 响应头给出缓冲 ID。
  - 非流式：完整回复落缓冲；原样重发命中未交付缓冲直接重放。
  - 新增 `GET /v1/resumable/{stream_id}?from=<seq>&limit=&wait=` 分片轮询（短连接抗抖动，`wait` 为长轮询），
    返回带序号的事件分片、`next` 游标与 `done` 标志，客户端按 seq 去重、按 done 判定结束。
  - 请求 ID 优先取幂等请求头（`x-resume-id` > `x-request-id` > `x-idempotency-key`），否则用 API Key + 请求体指纹；
    幂等分支混入 API Key 与指纹，不同用户/不同请求体不会串缓冲。
- `app/config.py`：新增 `resumable_enabled`（默认开）、`resumable_ttl_seconds`（1800）、`resumable_max_streams`（200）。
- `app/main.py`：lifespan 启动缓冲清理器，停机时取消后台泵；控制台「开关」区可配三项续传参数。

## 第三轮：惩罚参数默认剥离

- `app/model_capabilities.py`：新增 `PENALTY_KEYS = {"presence_penalty", "frequency_penalty"}`，
  从 Gemini 3.x / 2.5 / legacy 全部分支的 `allowed_sampling` 剔除（`sanitize_sampling` 自动剥离兜底）。
- `app/api_helpers.py`：`create_generation_config()` 不再下发这两个字段；前端模型字段保留、仍可传；
  非零值被剥离时仅在 `debug_outbound` 开启时打一条提示。

## 验证

- 全部修改文件 `py_compile` 通过；`uvicorn main:app` 启动无报错。
- 功能测试通过：各模型档案剥离、sanitize 兜底、generationConfig 不含惩罚参数；
  缓冲落帧序号（`id == 下标`，注释帧不占序号）、`Last-Event-ID` 续传、请求 ID 优先级/隔离/稳定性、
  `DetachedRequest` 解绑、容量淘汰、TTL 配置。
- 与原包全量 md5 比对：修改 8 文件、新增 2 文件，无其他改动。

## 修复补丁（修复、补跑验证）
1. 并发去重竞态：`resumable.py` 新增 `ResumableStore.create_if_absent()`（持锁原子占位）；
   `routes/chat_api.py` 的 `_resumable_stream` / `_resumable_json` 改走原子占位，并发同体请求只打一次上游，
   上游失败/非流式响应时经 `_abort_placeholder` 撤占位（给跟读者补错误帧+终止帧）。
2. 轮询缺 `mark_delivered`：`resumable_poll` 在 `done && end >= next_seq` 时标记已交付，
   纯轮询读完后再发同一请求走新生成而非重放。

## 审计修复（2026-10-02，审计并修复，13 条断言验证通过）
1. 流式占位防僵尸：`_resumable_stream` 在 `await _call_upstream` 期间若任务被取消（客户端断开），
   原 `except Exception` 抓不到 `CancelledError`，占位缓冲会烂在 store 里无人认领——
   同指纹重试会跟读这个死缓冲直到被清扫（默认配置下最长 2 小时）。新增 `except asyncio.CancelledError`
   分支：先经 `_abort_placeholder` 撤占位（给跟读者补错误帧 + 终止帧）再重抛；无取消时行为不变。
2. 轮询坏游标误标记：`resumable_poll` 在 `from_` 越过末尾的坏游标下也会 `mark_delivered`，
   一次坏游标就挡住后续正常重放。改为仅当 `start <= buf.next_seq`（真正把本轮读到末尾）时才标记交付。

## 签名轮换修复（2026-10-04，用户抓包、换入）
- Cookie 直连通道突发全线空回复：上游 `cloudconsole-pa` 返回 `QUERY_SIGNATURE_NOT_FOUND`
  （code 5 "Requested entity was not found"），系 Google 作废了硬编码在
  `app/cookie_auth.py` 的 `STREAM_GENERATE_QUERY_SIGNATURE`（README 已预警此内部接口无兼容性承诺）。
- 新签名由用户从 Cloud Console 前端 `batchGraphql` 请求（`operationName=StreamGenerateContent`
  的 Payload）抓取：`2/Hc4FpJfYmM+gO5TB0hcjYY0Iwj0rqLIVDZOhurBbu/I=`，已替换旧值；
  旧签名在全代码中零残留。部署后需重启服务生效。
