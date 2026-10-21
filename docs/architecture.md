# 代码结构与维护约定

项目对外提供 OpenAI 兼容接口、Gemini 原生接口和管理控制台。OpenAI 请求可以选择
Express SDK 或 Cookie 上游；Gemini 原生请求通过 Express REST 透传。

## 目录与模块职责

| 位置 | 职责 |
| --- | --- |
| `app/main.py` | ASGI 入口；注册路由、中间件与异常处理 |
| `app/lifecycle.py` | 启动初始化、后台任务登记、关闭连接池与代理进程 |
| `app/routes/chat_api.py` | OpenAI 请求分流、请求期限、续传与轮询 |
| `app/routes/gemini_native.py` | Gemini 原生鉴权、REST 透传和响应交付 |
| `app/routes/models_api.py` | OpenAI 模型清单 |
| `app/routes/console.py` | 页面、登录退出、统计和日志流 |
| `app/routes/settings_api.py` | 全局设置、模型设置和能力摘要 |
| `app/routes/credentials_api.py` | Express Key、项目探测和 Cookie 凭证 |
| `app/routes/system_api.py` | 健康检查和代理连通性测试 |
| `app/templates/` | 登录和控制台 HTML；页面按文件路径加载并缓存，无需前端构建 |
| `app/static/dashboard.js`、`app/static/login.js` | 概览、配置、凭证与日志交互，以及登录表单反馈；全部使用本地资源 |
| `app/static/console.css`、`app/static/dashboard.css` | 手写组件与响应式样式，以及预编译的 Tailwind 工具样式 |
| `app/auth.py`、`app/console_auth.py` | 分别处理 API Key 鉴权、控制台会话与登录限速 |
| `app/settings_validation.py` | 管理入口的类型、范围和专项设置校验 |
| `app/runtime_state.py`、`app/config.py` | 可持久化运行状态、环境变量和内置默认值 |
| `app/credential_config.py`、`app/credential_manager.py` | 多 Key / Cookie 环境导入、混合账号选择，以及 Cookie 与项目绑定 |
| `app/proxy_config.py`、`app/proxy_failover.py` | 代理输入归一化、有序候选链、账号到全局回退及交付前切换 |
| `app/static/channel-config.js` | Cookie 账号列表、账号回退策略，以及共用代理编辑弹窗 |
| `app/console_assets.py` | 控制台资源内容版本与缓存重新验证，防止部署后混用新旧脚本 |
| `app/upstreams/` | 两种上游协议，以及共用的消息准备流程 |
| `app/upstreams/cookie_request.py` | batchGraphql 请求参数、消息及工具协议转换 |
| `app/upstreams/cookie_response.py` | 连续 JSON 对象解析、事件与签名还原、响应诊断 |
| `app/generation_config.py` | OpenAI 请求到 SDK 生成参数的映射与裁剪 |
| `app/request_validation.py`、`app/schema_validation.py` | 两通道共用的消息、工具扩展字段和 JSON Schema 校验 |
| `app/express_endpoint.py` | SDK 与原生 REST 共用的模型路径、项目区域钉定和兜底判据 |
| `app/upstream_errors.py`、`app/response_validation.py` | 共享错误映射，以及响应拦截、完整性校验 |
| `app/message_processing.py`、`app/model_capabilities.py` | SDK / OpenAI 消息及响应转换、模型能力判定 |
| `app/image_processing.py` | 图片压缩、内联图片解码与远程抓取 |
| `app/thought_signatures.py`、`app/signature_store.py` | 思考签名编码、恢复与有界 TTL/LRU 缓存 |
| `app/prefill.py` | 消息注入、预填充兼容、标签守卫与输出去重 |
| `app/api_helpers.py` | SDK 响应适配、流式输出、调用与重试编排 |
| `app/retry_policy.py`、`app/streaming.py` | 重试配置、可重试性、等待、断连检查和流式心跳 |
| `app/json_stream.py` | 有界 JSON 对象增量分帧；Cookie 连续对象与原生 JSON 文档复用 |
| `app/first_reply.py` | 每次尝试的首回复期限、有效输出识别、可重试超时及原生流预读 |
| `app/request_connection.py` | 请求体结束后独占 ASGI receive，及时广播实际断连 |
| `app/request_tracking.py` | 请求生命周期、分离连接/生成计数、首字时序、输出诊断与有界历史 |
| `app/static/request-monitor.js` | 请求监控列表、筛选、分页、详情和关联日志交互 |
| `app/usage.py` | SDK / REST 用量映射、用量开关与 OpenAI 用量尾块 |
| `app/resumable.py`、`app/admission.py` | 有界响应缓冲、后台生成、期限与并发准入 |
| `app/client_pool.py`、`app/impersonate.py`、`app/proxy_resolver.py` | 连接复用、传输适配和代理解析 |
| `proxybridge/` | TURN RFC6062 / SSTP MS-CHAPv2 转本地 SOCKS5，独立进程与用户态网络栈 |
| `tests/` | 不依赖真实凭据或远程上游的回归测试 |
| `scripts/` | 统一检查入口、SDK 请求形态探针和手动诊断 |

## 请求处理边界

OpenAI 路由负责交付与上游分流，上游负责协议适配。两条通道先调用
`upstreams/request_preparation.py` 处理控制台消息注入与预填充，再准备抗截断传输。
Cookie 通道会额外检查历史函数往返，因此共享函数显式接收 `has_tools`，保留各通道的判断。

通道模式为 `api_key` / `web_proxy` / `mixed`，旧布尔开关会映射为前两者。
混合模式在已启用的两类记录间统一随机或轮询；结构化输出只选择 Express Key。
一次 Cookie 生成使用同一条记录中的 Cookie、Project ID 和代理快照，重试时重新计算认证头，
不会重新读取另一个账号的 Cookie。Cookie 列表接口不返回原始凭证；空 Cookie 按记录 ID 保留。

代理链是请求内状态：账号列表在前，允许回退时追加全局列表并去重；没有专属代理时继承全局。
配置代理后不隐式追加直连。`ProxyChain` 在交付前的连接错误或 407/502/503/504 后
尝试下一条，独立于模型重试次数。首回复超时交给模型重试，不作为切换出口的依据；
每个实际代理尝试拥有自己的首回复期限，外层总期限继续有效。
SDK 适配器预读首片，HTTP / curl 适配器在响应头交付前切换；交付后的流错误交回原流式处理。
生成租约持有当前代理链，工作线程的远程图片抓取也继承该账号出口和 sing-box 生命周期。

Express 使用 `generation_config.py` 构造 SDK 参数，Cookie 保留 batchGraphql 专属请求体构造。
思考强度、采样策略、图片比例和尺寸的规则由 `model_capabilities.py` 统一决定。
新增规则应先放到能力模块，再由各通道转换字段名，避免两套规则逐渐偏离。

Gemini 原生请求保留自己的协议和错误体，不经过 OpenAI 消息及参数转换。
Cookie 和原生路由直接引用共享错误、重试和端点模块，无需通过 SDK 调用实现复用这些规则。
SDK 的真流式、假流式和非流式统一检查上游请求拦截；这类错误使用 400，不参与重试。

首回复期限在每次实际上游尝试时开始，默认 15 秒；思考、正文、工具调用、图片都结束计时。
流式元数据在首回复前有界暂存，心跳不参与计时；超时先关闭本次读取，再进入原有重试预算。
原生 SSE 预读有效事件时仍向客户端保活；非 SSE JSON 数组检查完整对象后继续按原字节转发。
假流式和非流式只能观察完整上游响应，按模型单独调整此期限。

中间件只在请求体结束后启动独立断连读取器，将信号缓存供应用及 Starlette 复用，避免多个消费者抢读。
缓冲登记前端连接，最后一个读者断开时快照续传窗口（默认 10 秒）；窗口外相同新请求在锁内替换占位，
锁外等旧任务清理后才启动新上游。显式旧游标超窗返回 410，不创建带旧偏移的新生成。
没有新的相同请求时继续受原总期限、TTL 与容量预算管理，断连窗口本身不触发取消。

Cookie 执行器只编排认证、重试、协议降级与交付，不再承担请求构造和响应解析。
流式 JSON 对象按边界增量收集、完整后解码一次；完整正文使用 C 解码器直接解析。
单对象按 UTF-8 字节设上限，截断和畸形对象返回明确的 502，不能丢弃后继续报告成功。
原生 JSON 流收到首回复后仍继续验证 JSON 闭合、错误对象和候选结束标记，按原字节转发。
请求体构造中的思考配置复用该次读取的设置，避免重复深拷贝和热更新期间的参数混用。
旧版思考布尔开关在状态读取时统一映射；显式保存的新模式优先，控制台与执行路径使用同一结果。

签名缓存的访问顺序和过期顺序分别维护：读取只更新 LRU，写入更新绝对过期时间。
清理先按过期顺序删除，再按 LRU 控制容量，两个索引都受同一个上限约束。

项目锁定的 `google-genai` 版本支持 `thinking_level`，请求构造直接使用该字段。
升级或更换依赖时，需要运行 SDK 请求探针和相关回归，避免只调整版本号而保留失效分支。

## 修改时从哪里开始

| 修改目标 | 主要入口 | 验证重点 |
| --- | --- | --- |
| 新增管理接口 | 对应的 `routes/*_api.py` | 使用同一个 `console_auth.require_auth`；不要依赖 `main.py` 的全局变量 |
| 新增可调设置 | `config.py`、`settings_validation.py` | 默认值、类型范围、持久化，以及是否允许模型专属覆盖 |
| 新增消息准备行为 | `upstreams/request_preparation.py` | 两条通道、工具历史、生图和原始请求隔离 |
| 调整 SDK 参数 | `generation_config.py` | 请求优先级、结构化输出、工具声明和模型能力裁剪 |
| 新增模型 | `vertexModels.json`、`model_capabilities.py` | 模型清单、思考配置、采样和生图支持 |
| 调整重试或流式交付 | `retry_policy.py`、对应上游与路由 | 单 Key 429、已输出内容、断连、清理及统计 |
| 调整请求监控 | `request_tracking.py`、`middleware.py`、`admission.py` | 并发关联、真实发送失败、后台续传、首字与历史预算 |

后台任务通过 `lifecycle.start_background_task()` 登记，生成缓冲任务由
`resumable_store` 管理。连接池中的客户端由池统一关闭，单次调用不关闭共享客户端。
管理入口的持久化写入在线程中执行；状态层负责原子落盘和失败回滚。
成功落盘的快照在同一 IO 临界区内登记，作为后续失败回滚的基准；显式重载会
作废重载前尚未落盘的快照，避免并发写者覆盖新配置。

## 本地验证

安装 `requirements-dev.txt` 后，运行 `python scripts/check_project.py`。
检查会执行语法、静态分析、工作流校验、全部 `tests/test_*.py` 和 SDK 档案探针。
每个测试文件单独启动 Python 进程，隔离模块级状态和事件循环资源。

单组测试可以直接运行，例如 `python tests/test_console_routes.py`。
`verify_round17.py` 和 `verify_singbox_e2e.py` 属于手动网络诊断，不属于离线回归。
新增离线回归请放在 `tests/test_*.py`，由统一入口自动发现。

前端浏览器回归为 `node scripts/verify_console_ui.cjs`，需另装 Playwright 和 Chromium，配置见 README。
它在临时状态中启动真实管理接口，并为上游探测提供固定结果；不需要 Google 凭证，也不触发模型调用。
修改模板、前端脚本或 CSS 后，重点检查模型保存作用域、未保存凭证编辑、自动刷新焦点及移动端溢出。
