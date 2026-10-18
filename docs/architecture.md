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
| `app/auth.py`、`app/console_auth.py` | 分别处理 API Key 鉴权、控制台会话与登录限速 |
| `app/settings_validation.py` | 管理入口的类型、范围和专项设置校验 |
| `app/runtime_state.py`、`app/config.py` | 可持久化运行状态、环境变量和内置默认值 |
| `app/upstreams/` | 两种上游协议，以及共用的消息准备流程 |
| `app/upstreams/cookie_request.py` | batchGraphql 请求参数、消息及工具协议转换 |
| `app/upstreams/cookie_response.py` | 连续 JSON 对象解析、事件与签名还原、响应诊断 |
| `app/generation_config.py` | OpenAI 请求到 SDK 生成参数的映射与裁剪 |
| `app/express_endpoint.py` | SDK 与原生 REST 共用的模型路径、项目区域钉定和兜底判据 |
| `app/upstream_errors.py`、`app/response_validation.py` | 共享错误映射，以及响应拦截、完整性校验 |
| `app/message_processing.py`、`app/model_capabilities.py` | SDK / OpenAI 消息及响应转换、模型能力判定 |
| `app/image_processing.py` | 图片压缩、内联图片解码与远程抓取 |
| `app/thought_signatures.py`、`app/signature_store.py` | 思考签名编码、恢复与有界 TTL/LRU 缓存 |
| `app/prefill.py` | 消息注入、预填充兼容、标签守卫与输出去重 |
| `app/api_helpers.py` | SDK 响应适配、流式输出、调用与重试编排 |
| `app/retry_policy.py`、`app/streaming.py` | 重试配置、可重试性、等待、断连检查和流式心跳 |
| `app/usage.py` | SDK / REST 用量映射、用量开关与 OpenAI 用量尾块 |
| `app/resumable.py`、`app/admission.py` | 有界响应缓冲、后台生成、期限与并发准入 |
| `app/client_pool.py`、`app/impersonate.py`、`app/proxy_resolver.py` | 连接复用、传输适配和代理解析 |
| `tests/` | 不依赖真实凭据或远程上游的回归测试 |
| `scripts/` | 统一检查入口、SDK 请求形态探针和手动诊断 |

## 请求处理边界

OpenAI 路由负责交付与上游分流，上游负责协议适配。两条通道先调用
`upstreams/request_preparation.py` 处理控制台消息注入与预填充，再准备抗截断传输。
Cookie 通道会额外检查历史函数往返，因此共享函数显式接收 `has_tools`，保留各通道的判断。

Express 使用 `generation_config.py` 构造 SDK 参数，Cookie 保留 batchGraphql 专属请求体构造。
思考强度、采样策略、图片比例和尺寸的规则由 `model_capabilities.py` 统一决定。
新增规则应先放到能力模块，再由各通道转换字段名，避免两套规则逐渐偏离。

Gemini 原生请求保留自己的协议和错误体，不经过 OpenAI 消息及参数转换。
Cookie 和原生路由直接引用共享错误、重试和端点模块，无需通过 SDK 调用实现复用这些规则。
SDK 的真流式、假流式和非流式统一检查上游请求拦截；这类错误使用 400，不参与重试。

Cookie 执行器只编排认证、重试、协议降级与交付，不再承担请求构造和响应解析。
流式 JSON 对象按边界增量收集、完整后解码一次；完整正文使用 C 解码器直接解析。
单对象按 UTF-8 字节设上限，截断和畸形对象返回明确的 502，不能丢弃后继续报告成功。
请求体构造中的思考配置复用该次读取的设置，避免重复深拷贝和热更新期间的参数混用。

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

后台任务通过 `lifecycle.start_background_task()` 登记，生成缓冲任务由
`resumable_store` 管理。连接池中的客户端由池统一关闭，单次调用不关闭共享客户端。
管理入口的持久化写入在线程中执行；状态层负责原子落盘和失败回滚。

## 本地验证

安装 `requirements-dev.txt` 后，运行 `python scripts/check_project.py`。
检查会执行语法、静态分析、工作流校验、全部 `tests/test_*.py` 和 SDK 档案探针。
每个测试文件单独启动 Python 进程，隔离模块级状态和事件循环资源。

单组测试可以直接运行，例如 `python tests/test_console_routes.py`。
`verify_round17.py` 和 `verify_singbox_e2e.py` 属于手动网络诊断，不属于离线回归。
新增离线回归请放在 `tests/test_*.py`，由统一入口自动发现。
