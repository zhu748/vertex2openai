# 当前项目前后端审查：2ace593

> 此文保留优化前的审计证据与复现脚本。后续修复及当前验证结果见[优化完成记录](../optimization-2026-10-25.md)。复现脚本描述基线行为，不是当前版本的回归断言。

审查日期：2026-10-25。基线为 `main` 的 `2ace593`。本次是代码审查与离线验证，未修改业务实现，未提交、推送或部署。新增文件仅为本报告与复现脚本。

结论：已有测试覆盖较广，常规控制台交互表现稳定，但仍确认 **2 项 P1、5 项 P2、1 项 P3**。主要风险集中在 Gemini 原生请求进入 Cookie 桥接后的资源管理、协议元数据，以及 Claude 流式取消。控制台还存在跨页面保存覆盖的问题。

P1 表示应优先修复的可用性或资源管理问题；P2 表示特定场景下的功能错误、数据覆盖或协议不一致；P3 表示较小的兼容性问题。这里不将静态推测等同于已复现缺陷。

**检查范围与结构**

| 层 | 主要入口 | 本次重点 |
| --- | --- | --- |
| 控制台 | `templates/dashboard.html`、`static/dashboard.js`、`channel-config.js`、`console-forms.js`、`request-monitor.js`、登录页 | 保存作用域、草稿、异步交互、代理弹窗、错误状态、响应式布局 |
| 管理后端 | `routes/console.py`、`credentials_api.py`、`settings_api.py`、`runtime_state.py` | 会话鉴权、持久化、整表保存、凭证更新、并发一致性 |
| 协议入口 | `chat_api.py`、`claude_api.py`、`gemini_native.py` | 通道选择、转换、限流、超时、SSE、续传 |
| 上游执行 | `express_sdk.py`、`cookie_proxy.py`、`cookie_native.py` | 重试、签名、凭证健康、清理、响应完整性 |
| 共享设施 | `admission.py`、`streaming.py`、`resumable.py`、`client_pool.py`、Schema/图片/代理模块 | 生命周期、缓存、内存预算、取消和资源边界 |
| 部署与代理桥 | Dockerfile、Compose、CI、`proxybridge/` | 配置与静态检查；本机未执行 Go 构建或真实协议转发 |

OpenAI 和 Claude 的生成主要复用 `_call_upstream` 及其生成租约；Gemini 原生 Express 路径在 `_passthrough_request` 上使用生成租约。原生 Cookie 则提前返回 `handle_native_request`，没有经过这些公共边界。这是本次最重要的架构差异。

**已确认的问题**

| 编号 | 级别 | 问题 | 主要位置 |
| --- | --- | --- | --- |
| B1 | P1 | Cookie 原生桥接绕过生成并发限制及整轮总超时 | `app/routes/gemini_native.py:1036`、`app/upstreams/cookie_native.py:497` |
| B2 | P1 | Claude 队列满时，客户端断连导致清理等待卡住 | `app/routes/claude_api.py:210`、`:283` |
| B3 | P2 | Cookie 原生 SSE 丢失普通文本的 thoughtSignature | `app/upstreams/cookie_native.py:358`、`:538` |
| B4 | P2 | Cookie 原生请求的迟到结果污染已更换凭证的健康状态 | `app/upstreams/cookie_native.py:470`、`:546`、`:586` |
| B5 | P2 | Schema 净化改变类型联合、引用与组合约束的语义 | `app/schema_validation.py:160`、`:173` |
| B6 | P2 | 控制台跨页面整表保存会静默覆盖他人的启停和编辑 | `app/static/dashboard.js:269`、`app/runtime_state.py:375` |
| B7 | P2 | Cookie 原生 streamGenerateContent 忽略 alt 参数，固定输出 SSE | `app/upstreams/cookie_native.py:525`、`:566` |
| B8 | P3 | 声称支持 HEAD 的 `/api/hello` 实际返回 405 | `app/routes/claude_api.py:541`、`:549` |

**B1 — 生成并发与总超时不覆盖 Cookie 原生桥接**

触发：使用 Cookie 通道请求 `/v1beta/models/{model}:generateContent` 或 `:streamGenerateContent`，`/v1/models/…` 别名也进入同一路由。

`gemini_native.py:1036` 直接调用桥接并返回，发生在下面的 `asyncio.timeout(request_timeout_seconds())` 之前；`handle_native_request` 也没有 `limit_generation`。它的 HTTP 分阶段超时和首回复超时，不能替代整轮期限。

实测证据：

- 将 `upstream_max_concurrency` 设为 1，先手动占用唯一的真实生成租约，随后经过完整 ASGI 路由的原生 Cookie 生成仍返回 HTTP 200。这个请求没有申请生成槽。
- 将 `request_timeout_seconds` 设为管理接口允许的最小值 30，模拟持续返回文本而不结束的上游事件，在 30.5 秒时请求仍未结束，生成槽计数仍为 0。

影响：管理员配置的并发与执行时间边界失效；相关生成数监控不准确。在高并发、长响应或异常上游场景下，连接及内存压力可能超出预期。

修复建议：将生成租约、整轮截止时间、流式响应关闭与请求跟踪统一包在所有协议/通道执行外层；流式期限必须覆盖迭代消费，而不只是返回 `StreamingResponse` 的准备阶段。返回协议对应的容量/超时错误，防止重复申请租约。

验收：Cookie/Express × OpenAI/Claude/Gemini × 流式/非流式，覆盖并发已满、首回复后持续输出、总超时、客户端断开及最终槽位归零。

**B2 — Claude 慢消费者断连时形成清理死锁**

`_produce_frames()` 使用 128 项队列。取消时仍进入 `finally`，执行 `await frame_queue.put(_END)`。若队列已经满，且消费者已经退出，没人再读取队列；外层又在 `await producer`，因此无法执行后续的上游关闭。

实测：只消费 `message_start`，让生产者推进到第 129 帧，再关闭消费者：

```json
{"produced":129,"cleanup_completed":false,"upstream_closed":false,"occupied_slots_after_disconnect":1}
```

这里使用项目真实的 `limit_generation` 包装上游，确认不仅清理协程未完成，生成槽也仍被占用。诊断脚本最后用第二次取消结束测试；生产路径不能依赖第二次取消来解锁。

影响：慢客户端背压后断连可能留下上游与租约，累计后使后续正常生成被容量限制拒绝，也可能拖延关机。

修复建议：取消路径不再向满队列阻塞写入结束哨兵；区分正常完成通知和取消清理。无论 producer 如何结束，都应有独立、可保证执行的上游关闭路径。清理等待需要有界，且保留正常传输的背压行为。

**B3 — 普通 Part 的签名在 SSE 中丢失**

真实的 `cookie_response.extract_events()` 会把同一个上游 Part 的文本与签名拆成前后两个事件。原生桥接先序列化文本并发出；下一事件只修改 `assembler.parts[-1]`，然后返回 `None`，已经发送的 JSON 不会改变。

复现输入为同一个 Part：

```json
{"text":"answer","thoughtSignature":"c2ln"}
```

实测：非流式响应保留 `thoughtSignature`，而 SSE 正文中完全没有该字段。这不是函数调用签名问题：当前函数事件将签名放在同一事件中，属于另一条分支。

影响：相同请求在流式与非流式下得到不同的上下文元数据；客户端无法原样回传普通文本的签名，可能影响依赖签名的多轮对话。

修复建议：在生成下游帧之前保留完整 Part 边界及签名；必要时暂存一个尚未完整的 Part。不能仅修改已经序列化过的内部对象，也不能重发文本制造重复内容。补充签名与文本同 Part、独立签名 Part、图像 Part、函数 Part 的跨协议往返测试。

**B4 — 原生桥接漏传凭证快照，旧请求可隔离新 Cookie**

`CredentialManager` 已支持 `credential=record`，用 Cookie 与项目摘要区分同 ID 的旧、新凭证。原生桥接的失败和成功上报只传账号 ID，绕过了这个保护。

实测：保存旧凭证快照；将同 ID 账号更新为新 Cookie，并保留一个健康备用账号；模拟三个旧请求返回鉴权错误。新账号被标记 `cooldown_remaining=600.0`，随后选择器选中备用账号。

影响：刚更新的可用凭证可能被旧请求隔离 10 分钟；对称地，旧请求的迟到成功也能清除新凭证的失败记录。单 Cookie 池有“全部冷却时仍尝试”的回退规则，不能笼统说所有部署都会完全不可用。

修复建议：原生桥接的所有成功/失败回报都传入本次请求实际使用的 `record`，与现有 OpenAI Cookie 通道及探测路径保持一致。现有 `test_cookie_native_bridge.py` 甚至断言只传 ID，应更新其契约并加入新旧凭证交错测试。

**B5 — Schema 转换静默改变客户端约束**

三种已通过本项目 Schema 校验器、但转换后发生语义改变的输入：

| 输入约束 | 当前净化结果 | 问题 |
| --- | --- | --- |
| `{"type":["string","integer"]}` | `{"type":"string"}` | 非空联合类型只保留第一项 |
| `{"$ref":"#/$defs/Positive"}` | 全类型 `anyOf` | 对定义的引用丢失，引用约束不再保留 |
| `{"allOf":[{"type":"integer"},{"minimum":10}]}` | 全类型 `anyOf` | 组合约束被当成空 Schema |

根因：列表类型只取 `non_null[0]`；“空约束”判断没有考虑 `$ref`、`allOf` 等键。外层保留 `$defs` 也不能恢复已被替换掉的引用。

影响：工具参数和结构化输出不再表达客户端原本的类型/约束。这里已验证净化结果；没有声称这些所有 Schema 都能通过真实 Google 服务端校验。

修复建议：可转换的联合类型改用等价 `anyOf`；本地引用在深度、节点及循环限制下解析；无法安全转换的约束明确返回 400，而不是放宽或缩窄后继续。测试应比较允许/拒绝样例的语义，不能只断言“转成字典且不抛异常”。

**B6 — 多页面保存静默重新启用已停用 Key**

页面 A、B 读到同一版本的完整 Key 列表。A 关闭 Key 并保存；B 只修改备注，然后把旧的整表提交。服务端没有版本号、ETag 或字段级冲突判断。

实测两次保存均 HTTP 200，最终：

```json
{"first_save":200,"second_save":200,"account_enabled_after_disable":true,"note":"changed in another tab"}
```

影响不仅是备注覆盖，还包括启停状态、删除和代理配置。状态层的线程锁与原子落盘保证单次写入的一致性，不能识别浏览器持有的是旧版本。Cookie 列表的“提交时保留最新 Cookie”也不能保护其他元数据。

这是仓库已有审查文档提及、当前仍未解决的限制。本轮新增实际 HTTP 复现，未将其误称为本轮新引入问题。

修复建议：管理读取返回 revision；修改必须提交对应 revision，冲突返回 409 并保留草稿，展示服务器版本。也可逐账号 PATCH，并对被修改字段做乐观并发校验。前端不能收到冲突后自动整表重试覆盖。

单页面正常保存期间已有 `runAction()` 禁用对应 fieldset。浏览器验证支持这一保护，因此本报告不把“同一页面保存中仍可随意编辑并丢稿”列为已确认缺陷。

**B7 — 原生 Cookie 流式忽略响应格式选择**

同一个 `streamGenerateContent` 分别带 `?alt=sse`、不带 `alt`，实际均返回 `text/event-stream`。桥接仅按 action 判断流式，不读取 query，也没有像 Express 原生路径那样区分 JSON 流与 SSE。

影响：预期 JSON 流/JSON 数组的原生客户端会收到 `data:` 文本，不能按 JSON 解析；切换通道会改变线格式。

修复建议：按 `alt` 明确选择输出格式；如果暂不支持无 SSE 模式，在上游请求前返回明确的兼容性错误。不要接受请求后静默改变格式。同步检查续传参数的支持/拒绝策略，因为当前 Cookie 提前返回同样绕过了后面的续传分支。

**B8 — `/api/hello` 的 HEAD 未注册**

该模块说明和注释称支持 GET/HEAD，并假设 GET 会自动注册 HEAD。当前 FastAPI 路由实测 `HEAD /api/hello` 返回 405。需要 HEAD 探测的客户端会误判；普通 GET 不受影响。

修复建议：显式注册 HEAD，保持响应头与 GET 一致、响应体为空，并补充真实 ASGI HTTP 测试。

**前端与其他优化点（与已确认 bug 分开）**

| 顺序 | 优化 | 依据与建议 |
| --- | --- | --- |
| 1 | 建立跨协议公共执行边界与契约测试 | B1/B4 表明新增适配器容易遗漏已有保护；协议转换与资源调度分离，统一租约、超时、取消、健康回报，保留各协议错误体 |
| 2 | 原生 Cookie 流式避免保留整个回复 | `NativeAssembler.feed()` 会持续向 `parts` 追加，SSE 发送后仍保留。流式只保留待完成 Part、末尾元数据及统计，非流式另设整包预算；在 B1 修复前该风险更明显 |
| 3 | 远程图片增加整体预算和可取消执行 | `image_processing.py:204` 使用分阶段 timeout；逐跳/逐块持续有数据仍可耗时很长。主请求取消 `to_thread` 的等待不会停止线程中的同步下载。建议总截止时间、独立有界图片任务池、取消标志，并限制每请求图片数量/总字节；尚未做慢速服务器压力实测 |
| 4 | count_tokens 使用独立小容量和较短总期限 | `claude_api.py:484` 直接等待 SDK，且 `:489` 吞掉 CancelledError。辅助端点不宜长期占用与生成相同的网络资源；取消应向外传播，普通失败再回退估算 |
| 5 | 控制台统一保存与错误展示 | Key 保存分支在 `dashboard.js:272` 只取 `d.error`，其他分区已有 `responseError()`。可统一 401/422/503 文案与字段定位；为 B6 增加冲突比较及草稿恢复，不必先更换前端框架 |
| 6 | 凭证健康显示以服务端结果为准 | `channel-config.js:175` 后的测试结果合并自行累加失败次数，不能准确表示后端达到阈值后重置计数并进入冷却的状态。测试接口可返回健康快照，前端局部更新且保留草稿 |
| 7 | 先建立性能基线，再调整连接池和缓存 | 已有池化、签名缓存、字节预算及部分标量读取优化。优先测首回复 P50/P95、取消后槽位恢复、RSS、连接数、图片线程数和事件循环延迟，避免根据文件大小盲目重构 |

多进程/多副本部署仍需单独设计：会话、续传、凭证健康和部分统计是进程内状态。这是当前明确的架构边界，不能仅增大 Uvicorn workers 就期待登录、续传与配置一致性自动成立。

**本次验证结果**

| 检查 | 结果 |
| --- | --- |
| 按 `requirements-dev.txt` 安装依赖；`pip check` | 成功，无依赖冲突 |
| `python scripts/check_project.py` | 52/52 组通过，包含语法、静态检查、工作流、48 个测试脚本和 SDK 档案探针 |
| `npm run test:console` | 14 组 DOM 检查通过 |
| `node scripts/verify_console_ui.cjs`，真实 Chromium | 33 组通过；覆盖 320/390/768/1024/1440px、登录、保存、代理、草稿、请求监控、故障恢复；无脚本异常或第三方资源请求 |
| `python scripts/smoke_final.py` | 25 项通过、0 失败 |
| 本报告的独立复现 | 上述 8 类问题均有离线执行证据；总超时用真实 30 秒配置验证 |

“52/52”是检查脚本退出成功的组数，不代表所有可选网络集成都执行了。当前未提供 sing-box E2E 二进制，可选真实协议转发测试存在跳过；本机无 Go 工具链，因此没有执行 `go test -race`。未构建 Docker 镜像，未使用真实 Google Key/Cookie 或真实 TURN/SSTP 节点。模拟上游证明内部行为，不证明 Google 私有协议在线可用性。

**复现方式**

在仓库根目录、装好开发依赖的 Python 环境中运行：

```bash
python docs/audit-2ace593/repro_backend.py
python docs/audit-2ace593/repro_native.py
python docs/audit-2ace593/repro_console.py
python docs/audit-2ace593/repro_timeout.py
```

- `repro_backend.py`：Schema 语义与 Claude 满队列断连。
- `repro_native.py`：并发满额、签名、alt、HEAD、旧凭证健康污染。
- `repro_console.py`：两份旧快照先后保存导致 Key 重新启用。
- `repro_timeout.py`：约 31 秒，验证配置的 30 秒总期限未覆盖 Cookie 原生路径。

脚本使用临时状态目录、虚构凭证、ASGI 本地传输与模拟上游，不启动应用 lifespan，不调用 Google；输出实际行为用于诊断，而非把错误行为断言为正确契约。

建议修复顺序：B1/B2 → B3/B4 → B5/B6/B7 → B8。先补充在当前基线上能失败的行为测试，再修复共享边界，最后重跑现有离线与浏览器检查；真实上游及代理联调作为独立验收阶段。
