# vertex2openai 三轮重做 — 变更说明

基于原版 `vertex2openai-main`（2026-09-01），只做加法，未改原有架构。

## 第十七轮：多协议代理层的缺陷修复与健壮性加固

- **事件循环阻塞（高优先级修复）**：sing-box 首次启动（子进程拉起 + 端口等待，
  典型 ~100ms、异常兜底 10s）原先直接跑在 asyncio 事件循环上，会卡住整个服务的
  全部并发请求。新增 `resolve_proxy_uri_async()`（`asyncio.to_thread` 包装），
  Express SDK 主调用、Gemini 原生透传（含重试换 Key）、Cookie 通道、模型列表
  刷新、Key 探测、`/api/proxy/test` 六个异步消费点全部改为线程池内完成首启；
  之后的池签名 resolve 命中缓存，零阻塞。
- **STATE_DIR 相对路径致命 bug**：`STATE_DIR` 未设置时状态目录解析为相对路径
  `./singbox`，而 sing-box 的 `-D` 参数会先 chdir——相对的 `-c` 配置路径随之
  解析到 `./singbox/singbox/…`（必然读不到，进程秒退）。任何不设 `STATE_DIR`
  的非 Docker 部署都会触发；现在统一返回绝对路径。
- **端口就绪误判（协议级校验）**：原先只做 TCP connect 判定就绪——崩溃复活的
  端口被其他进程抢走时，connect 打到的是占端口者，且 sing-box 从绑定失败到
  退出有几十毫秒窗口，存活检查恰好扑空，造成「假就绪」。现在必须完成 SOCKS5
  方法协商应答（发 `\x05\x01\x00`、收 `\x05` 开头应答）才算就绪，且子进程一旦
  退出立即结束等待（失败路径从 10s 兜底缩到 ~百毫秒）。
- **崩溃端口复活死循环**：崩溃实例重启偏好复用原端口（保池签名稳定），但原端口
  被占时会永远失败。现在换新端口重试一次；仍失败则清掉死条目再抛错，下次取用
  从全新端口开始，不会卡死在同一个坏端口上。
- **`shutdown_all` 永久砖死单例**：`_closed` 哨兵置位后无路可回，测试套件仅靠
  用例字母序侥幸通过。新增 `reopen()`（测试隔离 / 运行时重启场景），离线测试
  补齐停机-恢复语义用例。
- **fd 泄漏**：`_launch` 启动失败路径的日志文件句柄永不关闭；现在 Popen 成功后
  父进程立即关闭（子进程已继承句柄），成功/失败路径都不泄漏。
- **锁粒度**：sing-box 启动从全局锁改为 **per-URI 锁**（不同代理的启动互不
  阻塞）；空闲回收的 terminate 挪到锁外（原先最多 6s 的 terminate+kill 会阻塞
  正在取用的请求）；与停机竞态时刚拉起的进程立即回收。
- **SSRF 检查旁路**：图片抓取在「配置了代理但转换失败（如缺 sing-box）」时原先
  会直连且跳过内网地址检查——等于给了 SSRF 一条旁路。现在按**实际出口**判断：
  转换失败回落直连时照常做内网/环回/链路本地检查。
- **解析器修正**：REALITY 链接漏带 `fp` 时补默认 `chrome` 指纹（sing-box 会拒启
  缺指纹的 REALITY）；`flow` 只在 TLS/REALITY 下生效（裸 TCP 带上会被拒启，
  现静默剥离）；ss 链接带 SIP003 plugin 从「静默剥离」改为**明确报错**（静默忽略
  会产出一个永远连不上的代理）；ws/http transport 的 `path` 不再二次 unquote
  （`parse_qs` 已解码一次，`%252F` 这类字面量会被错改）；vmess base64 容忍内嵌
  换行/空白（网页复制折行）；vmess 端口补 1–65535 范围校验；`_parse_host_port`
  端口非数字时统一报可读的 `ProxyResolveError`（原先裸 `ValueError`）。
- **探测路径**：非 Docker 部署新增仓库根 `bin/sing-box` 候选路径（与
  `$SINGBOX_BIN` → 常见路径 → PATH 并列）。
- **回归验证**：离线测试 25 → **37** 项（新增异步一致性、停机/reopen、崩溃端口
  复活、死条目清理、锁外回收、REALITY 默认指纹、flow 剥离、plugin 拒绝、vmess
  容错、path 双重解码等用例）；新增 `scripts/verify_round17.py` 真机验证脚本
  （异步一致 / 真机链路 / 杀进程+占端口复活 / 全失败清理四场景）；默认
  `STATE_DIR` 下两个 e2e 脚本 + 服务级冒烟（healthz 脱敏、代理实测、非法拦截、
  Key 级代理保存/拒绝、实例日志）全部通过。

## 第十六轮：出站代理多协议支持（sing-box）与 Key 级代理

- **多协议代理**：出站代理除原有 `http/https/socks5/socks5h` 外，新增直接粘贴
  分享链接的能力——`vless://`（含 REALITY / ws / grpc / h2 传输、utls 指纹）、
  `vmess://`（base64 分享格式，ws/grpc/h2 传输）、`trojan://`、`ss://`（SIP002
  base64 userinfo、明文 userinfo 与旧式整体 base64 三种形态）、`socks4://` /
  `socks4a://`、`hysteria2://`（含 salamander 混淆）、`tuic://`。实现方式：
  `app/proxy_resolver.py` 把分享 URI 解析成 sing-box outbound，首个请求时在本地
  拉起 sing-box 子进程（127.0.0.1 动态端口 socks5 入站），httpx / curl_cffi 统一
  连本地 socks5；原生协议不经过 sing-box，零额外开销。
- **Key 级代理**：每个 Express Key 可在控制台「通道与凭证」页设置**专属代理**
  （优先于全局；留空 = 跟随全局；填 `direct` / `none` / `直连` = 该 Key 强制直连）。
  生效范围：Express SDK 通道、Gemini 原生透传（重试换 Key 时同步切换出口）、
  Key 的 Project ID 探测。SDK 客户端池与透传客户端池的签名都纳入解析后代理，
  热改代理 / 换 Key 换出口后自动重建连接池，旧客户端照旧延迟回收。
- **通道覆盖**：全局代理对 Cookie 通道（batchGraphql，含 curl_cffi 伪装形态）、
  图片抓取、模型列表刷新同样生效；vless 等分享链接在所有出站点统一转换。
- **安全策略**：配置了分享链接协议但环境缺 sing-box 时，保存有静态校验提示、
  测试按钮返回转换失败原因、真实请求返回明确的 5xx 与安装指引——**绝不静默
  直连**，防止真实出口 IP 意外泄露。
- **进程生命周期**：同一 URI 全进程只起一个 sing-box 实例且端口稳定复用（池
  签名不抖动）；崩溃实例下次取用时自动重启；空闲超 `SINGBOX_IDLE_MINUTES`
  （默认 30 分钟，0 = 不回收）自动回收；FastAPI 优雅停机与 atexit 双保险终止
  全部子进程；配置与运行日志落在 `STATE_DIR/singbox/`。
- **部署**：Docker 镜像内置 sing-box v1.14.3（amd64/arm64/armv7，构建时
  `--build-arg SINGBOX_VERSION=` 可覆盖，`SINGBOX_MIRROR` 可走镜像前缀）；
  非 Docker 部署自动探测 `$SINGBOX_BIN` → 常见路径 → PATH。
- **控制台**：代理帮助文案更新（协议清单 + 直粘分享链接说明）；Key 卡片新增
  「专属代理」输入框与协议 pill；「测试连通性」按钮对分享链接先做转换再实测
  Google 端点延迟；保存 Key 时对代理字段做前置格式校验。
- **脱敏**：`mask_proxy_url` 升级为多协议版——vless/trojan/ss 的 userinfo
  （UUID/密码）打码，vmess 退化为长度标识，解析失败退化为哈希指纹，
  日志与 `/healthz` 不泄露任何凭据。

## 第十五轮：模型请求、Key 轮换与重试边界修复

- **SDK 轮换**：非流式、真流式、假流式的拥堵重试均重新选 Key，有备选时避开上一把；
  同步更新该 Key 的项目路径。路径兜底客户端不再缓存旧 Key，避免跨项目调用串用凭据。
  全部 Key 在重试期间被禁用时停止请求，单 Key 仍可重复使用完整预算。
- **单 Key 共享优先**：SDK 三种模式及 Gemini 原生流式/非流式均验证同一 Key 连续 429 后
  执行完整十次重试；六个并发请求共用一把 Key 时，各自独立重试。保留同 Key 本轮已采用的
  裸模型兜底路径，避免每次拥堵重试额外重复一次已失败的项目路径请求。
- **流式重试时机**：预填充延迟到上游首个有效事件后发送，纯用量/空块不再禁用重试；
  去重器尚未发给客户端的内容也可以安全重试。已发出内容后出现错误时只发送错误事件和
  结束标记，不重放答案；空流不再计成功，失败流会在退避前关闭。
- **错误与等待**：结构化 HTTP 状态码优先于文本关键词，明确的 400/401/403/404 不会
  因 quota 或模型名里的 429 被误重试。统一识别 httpx / curl_cffi 的超时及连接异常，
  并保留 429、502、504 等错误码。SDK、原生透传和 Cookie 支持 Retry-After 秒数与日期；
  等待期间检测断连，实际发送下一次请求时才计重试。假流式不再因心跳间隔大而延迟处理结果。
- **Cookie 错误协议**：502/504 纳入重试；HTTP 200 内 API 错误在重试耗尽后返回真实错误，
  不再拼成普通答案并计成功；生图收集和流式中断同样保留失败状态。
- **模型路径**：拒绝空白模型名，规范化旧前缀；原生 POST 支持带斜杠的完整资源名，
  保留 publishers / tunedModels 等资源前缀；自定义上游地址的尾斜杠不再形成双斜杠 URL。
  多候选流按上游真实 candidate.index 分离正文和工具调用，不再把稀疏候选归到 0 号。
- **协议异常**：显式 alt=sse 请求收到非 SSE 响应时明确返回 502，避免只记录告警后让客户端挂起。

新增 28 项模型/重试测试，并为原生接口新增 5 项回归；全套 8 组检查通过。
真实 google-genai SDK 配合 MockTransport 验证 retry_max=0/2 时实际出站次数为 1/3，
无额外内部重试。本地验证使用 Python 3.12 和模拟上游，未调用真实 Google 服务。

## 第十四轮：运行可靠性、资源管理、用量与开发检查完善

- **续传**：`Last-Event-ID` 从最后已收帧的下一帧继续；完成缓冲的替换改为原子操作，
  避免并发重发重复创建上游请求；旧泵完成回调不会再移除同 ID 的新泵。
  容量淘汰、过期清理、删除和停机都会取消对应后台请求并唤醒读者；JSON 缓冲保留上游错误码。
- **连接生命周期**：服务关闭时等待探测、缓冲泵与刷新任务结束，关闭 SDK、原生透传和
  Cookie 当前连接池，并立即回收仍在 900 秒宽限期内的旧客户端。
- **Cookie 流式**：使用增量 UTF-8 解码，中文或 emoji 跨网络块不再损坏；空流消费与
  `aread()` 结果均有显式记录，避免二次读取 curl 流而挂起。
- **SDK 重试与统计**：三种响应模式的路径兜底独立于拥堵重试预算，`retry_max=0` 仍能
  完成一次钉定兜底；假流式也支持该兜底。超时、网络错误和 5xx 统一识别；流式迭代器和
  假流式后台调用会释放。无 usage 的成功回复也计数；SDK 与 Cookie 的用量映射统一，
  completion 包含思考 Token、prompt 包含工具输入，避免漏计。
- **启动与模型缓存**：本地清单立即初始化，后台统一刷新；网络缓慢不再阻塞启动或
  模型列表，失败或空清单保留已有缓存；模型 ID 去空白、去重。
- **输入与内存**：拒绝非法 JSON、非有限数字、小数重试次数、非法代理端口和无效请求头；
  非 ASCII 登录口令改用 UTF-8 常量时间比较。图片使用分块下载，超过上限即停止读取，
  避免先把完整超大响应载入内存后才校验。
- **开发与部署**：统一入口 `python scripts/check_project.py`，增加 30 项运行可靠性测试；
  SDK 探针改用相对路径和临时配置，避免污染部署状态。CI 执行 Python 3.11 / 3.12
  回归矩阵；Docker 构建上下文排除虚拟环境、缓存、测试脚本和本地凭据文件。

验证使用模拟上游；本地 Python 3.12 的全套检查通过。Python 3.11 已配置到 CI，
当前环境未安装该版本，未在本地重复运行；未进行真实 Google 上游调用或 Docker 构建。

## 第十三轮：Gemini 原生格式请求统计与 429 重试修复

**根因**：统计中间件只匹配 `chat/completions`，Gemini 原生 POST 不进入总请求和错误计数；
透传重试硬编码为两次，且遇到已用过的 Key 就退出，导致单 Key 收到 429 后并未真正重试，
随机选回相同 Key 或池轮转一遍后也会提前结束。钉定路径兜底后，旧逻辑还会一直复用原 Key。

**修复**：

- `/v1beta/models/...` 与 `/v1/models/...` 的 POST 接入统一总请求和错误统计；
  原生响应成功和流中失败直接计数，每个客户端请求只增加一次总数；模型列表 GET 不计数。
- 重试统一读取 `get_retry_settings()`，遵循控制台次数与间隔，429/5xx 和网络异常均使用该预算；
  不再按 Key 是否用过提前退出，单 Key 也能重试，实际执行的重试进入面板重试计数。
- 保留上游 `Retry-After` 秒数优先，非法数值回退控制台间隔，退避期间检测客户端断连。
- 钉定兜底仍复用当前 Key、不占拥堵重试预算；随后发生 429 时重新选择 Key 并解析其项目路径。
- 原有回归脚本改用仓库相对路径和隔离临时状态目录，可在任意检出位置直接运行；新增真实
  FastAPI 路由和中间件回归，覆盖双前缀、单 Key、池循环、预算边界、流式统计和响应释放等。

**验证**：新增 17 项回归测试通过；原有 53 项检查通过。所有上游请求均为 MockTransport 模拟。

## 第十二轮：Gemini 透传「选 Key 后无限转圈」修复（v1 → v1beta1）+ 出站可观测性

**用户报障**：Gemini 原生格式流式请求后客户端无限转圈、无任何输出；日志停在
`🔁 [Gemini 透传]` 与 `🔑 [密钥选择]` 之后（无重试/无错误）；OpenAI 兼容
格式同 Key 同模型完全正常；控制台「总请求」为 0（该统计只计 chat/completions，
透传路径不计入，非故障信号）。

**根因**：透传 URL 写死 `api_version = v1`，而 google-genai SDK（vertex 模式）
的默认是 **`v1beta1`**（2.31.0 `_api_client.py` vertexai 分支；
MockTransport 实测流式 URL 为
`…/v1beta1/publishers/google/models/{m}:streamGenerateContent?alt=sse`）。
这带来三层问题：

1. `v1` 端点的 `streamGenerateContent` 对 `alt=sse` 的行为与 `v1beta1`
   不一致——上游可能返回 200 + 非 SSE 载荷，客户端 SSE 解析器等不到
   `data:` 行，**无限转圈且全程无任何报错**（与服务端「选 Key 后零日志」
   的症状完全吻合）；
2. SDK 通道（OpenAI 格式）落在 `v1beta1`、透传落在 `v1`，同一服务出站
   形态自相矛盾；
3. SDK 从不产生 `/v1/` 的 URL，请求头自称 `google-genai-sdk` 却请求
   `/v1/` 本身就是指纹层面的自曝。

**修复**：

- `EXPRESS_API_VERSION` 改钉 `v1beta1`，与 SDK 真实默认一致；
- **防漂移回归断言**：回归测试新增 `[1b]` 组——直接构造官方 SDK
  （MockTransport 注入）抓其真实流式 URL，与 `_build_upstream_url` 输出
  逐段对照（scheme/host/api_version/模型路径/action），SDK 将来改默认
  版本时测试立刻报警；
- **出站可观测性**（本次排查最大痛点就是「选 Key 之后零日志」）：
  - 上游响应一行摘要：状态码 + content-type（`debug_outbound` 开启时
    附完整上游 URL）；
  - 流式请求若上游返回非 `text/event-stream` 立即告警（正是本次
    v1/v1beta1 问题 silent-fail 的形态）；
  - 流式转发完成（总字节数）/ 中止（客户端断开或上游异常，含已转发
    字节数）均打日志。

**验证**：回归测试扩到 53/53 通过（新增 [1b] 5 项 SDK URL 对照断言）；
compileall 全量通过。改动文件：`app/routes/gemini_native.py`、
`README.md`、`CHANGES.md`、`scripts/test_gemini_native_fix.py`。

## 第十一轮：Express SDK 主通道四档案线序实证 + 两处标签形态缺陷修复

**背景**：第十轮验证了「官方 SDK vs 裸 httpx 出站点」逐字节一致，但 SDK
主通道**注入 node/gcloud/custom 档案后**的最终线序从未实测。本轮补上这块
盲区（新探针 `scripts/probe_sdk_channel_profiles.py`，按 `client_pool.py`
同款构造注入 MockTransport 抓包），实测**四档案线序/大小写均与官方默认
一致**，但抓出两处标签形态缺陷并修复：

1. **node 档案混合标签**（最严重）：SDK 的 `append_library_version_headers`
   会给任何含 gl-node 的 UA 强制追加 gl-python，拼出
   `google-genai-sdk/<ver> gl-node/22.14.0 gl-python/<ver>`——Node SDK
   从不带 gl-python，真实世界没有任何客户端会发出这种组合；每条请求都
   带同一个混合标签，等于给风控留下一个**固定的异常特征**。修复：
   `build_sdk_channel_overrides()` 对 node 档案返回 `{}`（不注入 = 官方
   纯 python 形态）；纯 node 形态仍由裸 httpx 出站点（Gemini 透传 /
   Key 探测 / 模型配置）发出。同一 Key 上 python / node 两种**纯**形态
   并存（= 一个团队同时跑两种语言的应用）远比混合标签安全。
2. **custom 档案标签拼接顺序**：旧 `_ensure_sdk_labels` 把 gl-python 补
   在尾部；真实 SDK 对自定义 UA 的变换是「library 标签缺失 → sdk+python
   **整体前置**；仅 python 缺失 → 尾部追加」。已对齐为逐字符同款变换，
   保证 custom 无论怎么填，产出必然是「真实 SDK 对同样输入的输出」，
   不会出现 SDK 自己造不出来的标签排布。

**审计结论**（Express 通道指纹完善度）：sdk 档案与官方 SDK 逐字节一致；
透传/Key 探测/模型配置/代理测试四个裸出站点与官方 SDK 完全一致（线序/
大小写/值）；gcloud 档案标签组合合法；全代码检索无 `python-httpx` /
自曝 UA 残留；HTTP/1.1 与官方 SDK 一致（h2 未安装，官方同样不发
HTTP/2）；`resumable.py` 为 SSE 续传无出站请求。修复后 6 项档案策略
断言全过（探针内置），回归测试 48/48，compileall 通过。

**改动文件**：`app/express_fingerprint.py`（node 不注入 + 变换对齐 +
docstring）、`app/main.py`（控制台帮助文本）、`README.md`、`CHANGES.md`。

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
