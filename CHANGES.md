# vertex2openai 三轮重做 — 变更说明

基于原版 `vertex2openai-main`（2026-09-01），只做加法，未改原有架构。

## 第十轮：出站请求头指纹逐字节对齐官方 SDK（MockTransport 抓包实证 + 修复）

**背景**：反代项目最大的自曝风险不在 UA（第八/九轮已接管），而在**与官方 SDK
的细节偏差**——风控日志里一条请求的头集合、线序、序列化格式、压缩协商只要
有一项与「自称的 SDK 形态」对不上，就等于自我标记。本轮用 `httpx.MockTransport`
注入官方 google-genai 2.31.0（`vertexai=True + api_key`，与本项目 SDK 通道同款
构造）抓取**字节级出站基准**，与本项目全部裸 httpx 出站点逐项对照，实测发现
4 处 Google 可见偏差并全部修复（对照脚本 `scripts/probe_fingerprint_diff.py`
可随时复验）：

1. **透传通道 `accept-encoding: identity`**（最刺眼）：真实 SDK 走 httpx 默认的
   `gzip, deflate`，钉 identity 是全网 SDK 用户里不存在的异类形态。修复：
   删掉显式 identity，回归 httpx 默认；同时流式中继从 `aiter_raw` 改
   `aiter_bytes`（透明解压），非流式 `aread` 本就自动解压，两条路径行为一致，
   且本服务不透传 content-encoding 头，客户端拿到的仍是上游原始载荷。
2. **透传通道 body 重组**：旧实现 `json=payload` 把客户端请求体解析后重新
   序列化——httpx 的 `json=` 用紧凑分隔符（`{"a":1}` 无空格），而真实 Python
   SDK 用 `json.dumps` 默认分隔符（`{"a": 1}` 带空格），声称是 Python SDK 却
   发紧凑 JSON 就是自相矛盾。修复：**原始字节转发**（`content=raw_body`），
   客户端的 SDK 把 body 序列化成什么样（分隔符/键序/空白）上游收到的就是
   什么样，等价于「客户端直连 Google」。请求体仍先 parse 校验合法性。
3. **头线序与大小写**：旧实现头 dict 顺序（x-goog-api-key → content-type →
   accept-encoding → …）与 SDK 最终线序（Content-Type → x-goog-api-key →
   user-agent → x-goog-api-client）不同，且 `content-type` 小写。修复：
   `build_direct_headers(api_key)` 重构为 SDK 同款同序（含 GET 也带
   `Content-Type: application/json` 的 SDK 特性），Gemini 透传与 Key 探测
   两个出站点统一换用；Key 探测的 body 一并对齐 SDK 序列化（默认分隔符 +
   pydantic 键序 parts→role）。修复后 **透传通道与官方 SDK 抓包「完全一致
   （顺序+大小写+值）」**，Key 探测仅 content-length 因载荷不同而异。
4. **Cookie 通道浏览器头线序**：`authorization` 第一、`cookie` 第二的 dict
   插入序一眼就是脚本拼的。`apply_browser_headers` 新增 Chrome 真实 XHR 线序
   重排（client hints → content-type → authorization → Google 专用头 →
   user-agent → accept/origin/referer → sec-fetch-* → accept-language →
   cookie 殿后）；off 档案完全旧兼容，firefox 档案重排后仍是合理浏览器形态。

**顺手加固**：远程图片抓取补浏览器 UA + 图片 Accept（旧裸
`python-httpx/<ver>` 会被 imgur 等图床 403）；透传 query 参数重下发前
`quote` 再编码（旧实现裸拼 `k=v`，值含 `&`/`=`/空格会截断 alt 之外的参数）。

**验证**：回归测试扩到 11 组 48/48 通过（新增 [10] 指纹保真——线序/头值/
原始字节透传断言，[11] gzip SSE 透明解压断言）；Cookie 通道三档案线序
人工核验；`compileall` 全量通过。

## 第九轮：修复 Gemini 原生透传一律 404（OpenAI 兼容接口正常、原生格式不可用）

**现象**：客户端走 `/v1beta/models/gemini-4-argon:streamGenerateContent` 报
`Unknown error: 404`（上游 404 且**空 body**），同一模型走 `/v1/chat/completions`
正常；日志显示已正常选出 Express Key（带 Project ID）。

**根因**（真机 curl 三组对照定案）：`gemini_native._passthrough_once` 无条件给
模型路径拼 `publishers/google/models/` 前缀。而 `express_location` **默认
`global`** 且选中的 Key 带 Project ID 时，`resolve_express_model_path` 返回完整
资源路径 `projects/{p}/locations/global/publishers/google/models/{m}`，叠加后变成

```
https://aiplatform.googleapis.com/v1/publishers/google/models/projects/…/models/gemini-4-argon:…
```

这种**双重前缀畸形路径**被 Google 前端直接 404（空 body，客户端无法解析只能
显示 Unknown error: 404）。OpenAI 兼容路径走 SDK，`t_model()` 对 `projects/`
开头的 model 原样透传、URL 正确，所以不受影响。实测对照：畸形路径 404 空 body，
正确钉定路径与裸模型名路径均为 401（路径合法、假 Key 认证失败），证明 404 由
路径形态决定。

**修复**（`app/routes/gemini_native.py`）：

- 新增 `_build_upstream_url`：模型路径拼接对齐 google-genai SDK 的 `t_model()`
  语义——裸模型名拼 `publishers/google/models/{model}`（Express 端点格式）；
  `projects/`、`publishers/`、`models/`、`tunedModels/`、`endpoints/` 开头的完整
  资源路径（含 location 钉定生成的与客户端自带的）**原样拼接，不再叠加前缀**。
  客户端自带完整 `projects/…` 路径的请求（旧版同样双重前缀）一并修好。
- 补齐「钉定失败自动兜底」（对齐 SDK 通道 `execute_gemini_call` 的
  `fallback_model`）：新增 `_looks_like_pin_failure`（与
  `api_helpers.is_location_pin_failure` 同一套判据，输入换成
  (status, body)），钉定路径返回 404 `Publisher model … not found` /
  403 `requires billing` 时自动退回裸模型名重试一次（不换 Key、不占 429/5xx
  的 Key 轮换预算），彻底消除「OpenAI 接口能自动兜底、原生透传把 404 原样
  抛给客户端」的通道间行为差。
- 重试循环重构为 while + 双计数器（`key_attempts` / `pin_fallback_used`），
  行为与旧版逐项对齐：429/500/503 换 Key 退避重试最多 2 次（retry-after 优先）、
  断连即停、错误状态码与 body 原样透传（含 retry-after 头）。流式错误响应从
  流式中继改为缓冲后透传（客户端可见的状态码/body/头完全一致，错误体本来就
  是一次性小包），成功流式仍走 `aiter_raw` 字节级中继不变。
- 模块 docstring 同步更新。

**验证**：新增回归测试 9 组 38/38 通过（URL 构造 5 形态 + 双重前缀反例、
钉定+流式 200、未钉定、404 not found 兜底、403 billing 兜底、404 空 body
不兜底原样透传、429 换 Key、客户端完整路径、无 Project ID 退裸名）；
`compileall` 全量通过；真机 curl 验证三种 URL 形态的上游行为差异。

## 第八轮：性能与健壮性专项（客户端池化 + 事件循环减压 + 6 个正确性修复）

全量代码审查（约 1 万行）后按投入产出比实施的十个方向：

- **上游 HTTP 客户端全链路池化**（新增 `app/client_pool.py`，三条通道统一受益）：
  - Express SDK 通道：旧实现**每个请求** new 一个 `genai.Client` 再丢弃——每次都对
    `aiplatform.googleapis.com` 重新 TCP+TLS 握手（+100~300ms 延迟），底层 httpx 连接池
    从不关闭、靠 GC 兜底攒 fd。现按（Key, 代理, 证书, base_url, 指纹档案）签名池化
    （LRU 上限 6），控制台热改任一设置自动换新形态客户端；fallback 重试复用同实例。
  - Gemini 原生透传：每请求 new `httpx.AsyncClient` → 单槽共享池（代理/证书签名）。
  - Cookie 通道：`open_upstream_client` 保持上下文管理器签名不变（三个调用点零改动），
    底层按（伪装档案, 代理, 超时）复用 httpx / curl_cffi 连接——**重试之间也不再重新握手**
    （默认重试 10 次的 429 场景从 11 次握手降到 1 次）。已实测 curl_cffi 0.16.3 单
    session 30 并发响应无串扰。
  - 被淘汰的客户端一律**延迟 900 秒**再关闭（护住飞行中的长流式），到点同时关
    同步/异步底层连接；genai Client 跨请求并发安全性已核实（httpx 连接池语义 +
    tenacity 每调用本地 RetryCallState）。
- **Cookie 通道请求体只构建一次**：旧实现每次重试都重建 body（内含远程图片下载 +
  PIL 压缩 = 反复下载同一批图）；原生/降级两形态各自缓存，重试发同一份。
- **大 JSON 解析移出慢路径**：`_iter_json_objects` 从纯 Python 逐字符括号计数 +
  O(n²) 切片，改为 `json.JSONDecoder.raw_decode`（C 实现）增量解析——1MB 生图
  base64 对象实测 **778ms → 13ms（58×）**，不再卡住事件循环拖垮并发；另加
  64MB 失步自恢复上限。
- **Gemini 透传钉 `accept-encoding: identity`**：转发用的是原始字节且不回传
  content-encoding，上游一旦启用压缩客户端就会拿到"伪 json 乱码"；同时透传
  `retry-after` 响应头（非流式 + 流式错误 + 重试耗尽三条路径）。
- **修复 6 个正确性问题**：
  1. `-search` 后缀请求匹配不到按基础名保存的模型专属参数（采样默认/生图尺寸/比例
     全部静默落空，仅 Express 通道）→ `get_effective_settings` 查表后缀归一；
  2. Express 流式失败从不计入大盘错误数（流式 200 + SSE 错误帧，中间件按状态码
     看不见）→ 两个失败分支补 `stats.add_error()`（auto 模式仍由外层通道计数防双计）；
  3. 控制台 Key 探测消息 `rec._msg` 未转义直接 innerHTML（上游错误文本可注入
     存储型 XSS）→ 通用 `esc()` 转义，模型选项/比例选项同步加固；
  4. `/api/settings` 只校验键白名单不校验值类型（`fake_streaming_interval` 存成
     字符串 → 所有假流式请求 500 且难自救）→ 从 DEFAULT_SETTINGS 自动推导
     类型/枚举/数值范围，`/api/model-overrides` 共用同一套校验；
  5. `_login_failures` 永不过期（内存慢泄漏 + 被锁 IP 永久 300s 退避）→ 24h TTL
     + 1 万条容量上限；新增可选 `TRUST_PROXY_HEADERS=1`（反代下按
     X-Forwarded-For 区分客户端，防一人失败全站被锁）；
  6. `/v1/models` 缓存过期后每个并发请求都串行触发一次远程刷新（20s 超时排队放大）
     → 双重检查 + 成功 1h / 失败 5min 最小间隔。
- **控制台前端健壮性**：日志窗口 DOM 上限 500 条（防挂一晚页面拖卡）、通道切换
  失败自动回滚并提示、页面隐藏时暂停 3s 轮询。
- **清理与瘦身**：删除约 150 行死代码（`StreamingReasoningProcessor` 整类、
  `extract_reasoning_by_tags`、express_key_manager 四个无调用方的兼容方法、
  `has_tool_traffic`、`ProxyStats.add_request`、signature_store 四个兼容存取方法、
  无用 import）；`/v1` 流式 chunk 的 `json.dumps` 统一 `ensure_ascii=False`
  （中文体积减半，与 Cookie 通道一致）。
- **部署物**：新增 `.dockerignore`（构建上下文瘦身）；Dockerfile 去掉与
  `PIP_NO_CACHE_DIR` 重复的 `--no-cache-dir`；compose 删除已废弃的 `version:` 行、
  镜像地址更正为 `ghcr.io/zhu748/vertex2openai`。
- **验证**：新增 `scripts/regression_test5.py` 39/39（客户端池三组、生图重试
  body/客户端复用 E2E、设置校验 12 例、-search 归一、登录 TTL、刷新防风暴、
  前端修复）；既有回归 test2 21/21、test3 33/33、test4 21/21 全部不破；
  `compileall` + 真实启动冒烟（GET/HEAD /healthz 200）通过。

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
