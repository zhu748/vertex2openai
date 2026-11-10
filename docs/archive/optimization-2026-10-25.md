# 前后端优化完成记录

基线：`main@2ace593`。日期：2026-10-25。实现保存在当前工作区，尚未提交、推送或部署。

本轮修复了[基线审计](audit-2ace593/README.md)确认的 8 类问题，并优化了流式内存、图片处理和辅助计数接口。依赖版本保持不变。

## 已完成的修复

| 审计项 | 修复后的行为 | 主要实现 |
| --- | --- | --- |
| B1：Cookie 原生请求绕过并发与总超时 | 与生成接口共用并发租约；额度耗尽返回 503。总预算覆盖重试、等待和持续输出；超时返回 504 或流内错误，关闭上游、释放名额。 | `app/upstreams/cookie_native.py` |
| B2：Claude 满队列取消死锁 | 取消生产者时不再向满队列追加结束哨兵；上游连接和生成名额能正常释放。 | `app/routes/claude_api.py` |
| B3：原生 SSE 丢失签名 | 原生路径按完整 Part 传递，文本、图片和独立签名均保留；SSE、JSON 数组流与非流式结果一致。 | `app/upstreams/cookie_response.py`、`cookie_proxy.py`、`cookie_native.py` |
| B4：旧 Cookie 请求污染新凭证健康状态 | 成功与认证失败均携带请求启动时的凭证快照；更换凭证后，旧结果不会使新账号进入冷却或清空新账号的失败记录。 | `app/upstreams/cookie_native.py` |
| B5：Schema 联合、引用与交集语义丢失 | 多类型转换为完整 `anyOf`；解析本地 `$ref`，保留引用约束；不支持的 `allOf`、递归或外部引用明确拒绝，不再变成任意类型。展开受深度、节点数和字节数约束。 | `app/schema_validation.py`、`app/upstreams/cookie_request.py` |
| B6：旧页面整表保存覆盖新状态 | Key/Cookie 列表返回 `revision`，保存时在状态锁内原子校验；版本冲突返回 409，前端保留草稿并显示持续提示。 | `app/runtime_state.py`、`app/routes/credentials_api.py`、`app/static/dashboard.js`、`channel-config.js` |
| B7：原生流忽略 `alt` 参数 | `alt=sse` 返回 SSE；省略或 `alt=json` 返回合法 JSON 数组流，错误也按当前格式输出。Cookie 原生续传目前明确返回 400，避免重复启动生成。 | `app/upstreams/cookie_native.py` |
| B8：HEAD 探测返回 405 | `/api/hello` 显式支持 HEAD，响应头与 GET 一致且响应体为空。 | `app/routes/claude_api.py` |

## 资源与控制台优化

- 原生流式组装不再保留所有已发送 Part。非流式累计 Part、流式单个 Part 的大小上限为 64 MiB；上游静默时发送心跳。
- 图片和提示词准备使用独立线程池：最多 8 个工作线程、32 个在途或排队任务，单次准备预算 30 秒。请求取消后立即标记取消，线程在检查点退出；容量在实际线程结束后释放，避免取消请求反复挤入无限队列。
- 图片下载的重定向和出口重试共享单张图片时间预算，读取数据时检查截止时间与取消状态；多张图片共享准备阶段预算。原有地址校验、DNS 地址固定、代理选择及图片大小限制保留。
- Claude 精确 `count_tokens` 最多并发 4 个，预算 15 秒；饱和、超时或上游失败回退估算。客户端取消继续向上传播，代理使用记录在结束时清理。
- Cookie 续期不改变编辑版本：保存备注或开关时仍保留最新短期令牌。手动更换凭证、增删账号或修改配置会改变版本。
- Express 项目探测只在服务器变化符合本次探测结果时更新保存版本；其他页面的备注、开关等修改仍触发冲突，重复探测也不能绕过。
- Cookie 健康提示采用后端实际快照，避免把网络或其他探测失败错误地累计为认证失败。Key 保存统一使用控制台错误解析。

## 验证结果

| 检查 | 结果 |
| --- | --- |
| `python scripts/check_project.py` | 53/53 组通过：语法、项目静态规则、工作流、49 个隔离测试脚本及 SDK 档案检查 |
| `python tests/test_audit_optimization.py` | 新增 23 项回归通过，包含真实 ASGI 接口、并发竞争、取消清理和资源上限 |
| `npm run test:console` | 15 组 DOM 检查通过，新增探测版本更新、重复探测冲突与草稿保留检查 |
| `PYTHON=python node scripts/verify_console_ui.cjs` | Chromium 35 组通过，包含跨标签页 Key/Cookie 保存冲突、320/390/768/1024/1440px 布局；无 JavaScript 异常或第三方资源请求 |
| `python scripts/smoke_final.py` | 25 项通过，0 失败 |
| `python -m pip check` | 无依赖冲突 |
| `git diff --check` | 通过 |

完整检查后，额外补充了长属性名展开、同时保存竞争的断言，相关 Schema 和新增回归脚本重新执行通过。

## 流式内存对照

命令：`python scripts/benchmark_native_assembly.py --parts 10000 --part-bytes 1024`。

向同一个组装器依次投喂 10,000 个具有不同文本的约 1 KiB Part，使用 `tracemalloc` 测量。完整保留模式模拟修复前的流式累积；流式模式使用本轮实现。

| 模式 | 保留 Part 数量 | 峰值分配 | 运行耗时 |
| --- | ---: | ---: | ---: |
| 完整保留 | 10,000 | 12,628,175 字节，约 12.04 MiB | 0.2986 秒 |
| 流式组装 | 0 | 5,407 字节，约 5.28 KiB | 0.2638 秒 |

[原始结果](audit-2ace593/optimization-memory.json)。这是本机组装器合成测试，不能据此推断端到端吞吐或整个服务的 RSS 降幅；网络缓冲、请求跟踪及 ASGI 发送另有开销。

## 兼容性与验证边界

1. 为兼容现有管理 API 客户端，`revision` 是可选字段；当前控制台总是提交服务器返回的版本。外部整表更新客户端也应携带该字段，才能获得冲突保护。
2. Schema 转换仍属于 Gemini 支持的子集，并非完整 JSON Schema 执行器。本轮不支持的交集、递归引用和外部引用返回明确错误。Cookie 原生断点续传尚未实现，改为明确拒绝。
3. 线程无法强制打断操作系统 DNS 查询或正在执行的原生库调用。协作取消、剩余时间检查和有界线程控制其影响；尚未进行真实慢 DNS、慢代理及大规模并发压测。
4. 本轮使用临时状态、假凭证及受控上游，未访问真实 Google 生成服务。需要有效凭证的协议验证、真实 sing-box / TURN / SSTP 链路及 Docker 镜像运行验证未执行。依赖外部环境的可选检查仍按项目原有策略跳过。
5. 凭证状态、版本校验和并发名额仍以单进程为边界；本轮没有引入多进程共享状态存储。
