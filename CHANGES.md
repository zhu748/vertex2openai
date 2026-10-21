# vertex2openai 三轮重做 — 变更说明

基于原版 `vertex2openai-main`（2026-09-01），持续完善兼容性、可靠性和代码组织。

## 2026-10-21 深度复审：配置一致性、媒体保真与流完整性

- 配置持久化在同一 IO 临界区发布成功快照，修复并发写入完成顺序倒置后，下一次保存失败把内存退回旧设置的问题；显式重载会作废尚未写入的旧快照。
- 图片像素预检独立于压缩开关，拒绝在打开文件头时触发 PIL 像素炸弹限制的图片；Base64 仅允许有效编码及常规折行，空图片不再透传。
- Markdown 图片全部提取失败时仍替换原始 data URL，避免将 Base64 当成提示词；提取成功后保留原文缩进、制表符和换行。
- Cookie 工具兼容模式正确处理分段 `text: null`，旧式 `role=function` 也计入工具历史，保持预填充保护语义一致。
- Gemini 原生流只有用量或元数据时返回明确的 502，避免误计成功并给客户端返回空流；正常结束标记和上游请求拦截结果继续原样交付。重试测试使用带结束标记的完整成功样例。
- 本地模型目录按顺序尝试所有候选文件，首份损坏、结构错误或为空时继续读取随项目提供的目录。
- Key 和运行设置等敏感接口明确禁止缓存。代理草稿显示未保存状态，编辑后清除旧测试结论；校验错误定位到输入框，会话失效给出具体提示。全局设置保存后同步通道页的调度选择。
- 新增并发持久化、媒体、工具历史和原生流回归，扩充浏览器与缓存验证；心跳测试等待实际 POST 启动，消除线程调度导致的偶发失败。

## 匿名 TURN 隧道存活与 SOCKS 满载可用性复审

- 修复匿名 TURN 隧道被协议外流量误杀：裸 STUN 客户端此前「读一条就当应答」——控制连接上任何未恳求消息（公网中继被扫描时服务器随时下发的 ConnectionAttempt 指示、早前事务的重传应答）都会被当成 Refresh 结果，非成功类消息直接触发 fail-closed 杀掉健康分配，还会让真正的应答滞留流内造成后续错位。现在 `rawSTUNTransaction` 按事务 ID 匹配应答并跳过指示消息（连接期限约束循环），新增「指示注入不杀隧道 + 后续刷新正常」回归测试。
- 修复刷新节奏不遵守服务器授予的 LIFETIME（RFC 5766 §6.2 允许服务器授予比请求更短的时间）：旧实现沿用 Allocate 响应推导的固定间隔，服务器中途缩短生命周期时分配在两次刷新之间过期；固定 5 秒下限同样让授予不足 10 秒的分配首次刷新即迟到。现在每次 Refresh 应答的 LIFETIME 驱动下一次间隔（授予值一半、250ms 下限、恒早于过期），授予 0 视为分配已被回收并 fail-closed；新增「4 秒初始授予 + 2 秒再授予」节奏回归测试。
- 探测成功后立即用 Refresh(LIFETIME=0) 显式释放探测分配（RFC 5766 §7.3），不再依赖「关闭 TCP 控制连接即释放」的服务器实现差异，尽力而为不阻断模式判定。
- SOCKS 满载应答移出 accept 循环：内联执行时单个慢客户端可让整个 accept 循环停满 5 秒期限，拖住其他所有入站连接（含 Python 就绪探测）。拒答改由 64 容量的有界协程池处理，超限的直接丢弃；Python 侧区分饱和与网络故障的切换语义不变。
- SSTP 拨号失败的诊断不再被丢弃：`protocolError.Error()` 此前只返回错误码，`sstp_target_connect_failed` 携带的源地址、收发计数与底层原因从不落入日志；现在完整输出（内容仅含自产诊断文本，无对端可控凭据）。
- 桥接状态目录新增孤儿清理：同前缀清理覆盖不到 SIGKILL / OOM 后残留的其他进程 `bridge-{pid}-…` 文件，长跑部署无限积累；启动时按文件名内嵌 pid 的存活状态 + 1 小时宽限清理，pid 复用只会导致少删（安全方向），并附回归测试。

## 深度审查优化：崩溃修复、安全加固与热路径性能

- 修复匿名 TURN 桥接进程崩溃：刷新 goroutine 失败路径与正常关闭路径各自持有 `sync.Once` 关闭同一个 channel，服务器停止应答 Refresh 后必然 `close of closed channel` panic 并杀死整个桥接进程（同节点在途连接全部中断）。改为单一所有权——channel 只由分配对象的 `stop()` 关闭，数据连接独立幂等关闭；新增「刷新失败优雅关闭 + 重复关闭幂等」回归测试。
- 修复 TURN 拨号的取消/成功竞态：拨号上下文恰在成功瞬间到期时，`AfterFunc` 已关闭控制连接但连接仍被交还给调用方；现在校验 `stop()` 返回值，竞态命中按失败处理。
- SSTP 认证拒绝不再被 30 秒握手预算边缘的超时误报掩盖，先判 `ErrAuth` 再判上下文超时。
- SOCKS 满载（128 并发上限）不再裸断连接：完成方法协商后回「general SOCKS server failure」，Python 侧可区分代理饱和与网络故障，多出口切换语义恢复；拨号预算按剩余候选均分，单个黑洞 IP 不再吃光全部 30 秒，DNS 成功但无可用地址时报解析失败而非地址族错误。
- 状态文件损坏备份不再以 0644 滞留磁盘：备份落盘立即收紧到 0600（与主文件一致，内含完整 Google 会话 Cookie）。
- Docker 构建的 sing-box 下载增加 SHA256 校验（三个架构的哈希内置为 ARG，镜像源被劫持或中间人会直接构建失败；自定义版本可覆盖，留空则跳过并警告）；compose 增加 `init: true` 由 tini 兜底收尸；弱口令清单纳入 compose 占位口令 `change-me-please`，公开托管环境启动即阻断，并移除重复的第二段告警。
- 匿名 TURN 前后端口径补齐：面板的缺失字段判定此前仍对 TURN 强制凭据，开放服务器节点在 UI 中无法添加、无法测试；现在与后端同口径（仅 SSTP 的 MS-CHAPv2 协议强制），凭据「必填」标签与 TURN 提示文案同步。
- 流式去重器异常不再卡死：去重处理异常时立即置为完成态并原样透传（宁可重复，不可无界暂存），重叠裁剪失败保底放行原文（宁可重复，不可丢字），预填充拼回失败留下可排查日志。
- 热路径性能：多图消息的逐图全量文本重建（O(n²)）改为一次 join；日志脱敏四个正则提升到模块级预编译；SSE 诊断观察器改为指针增量扫描（分隔符跨块保留 3 字节重叠）；续传缓冲每帧不再进全局锁读设置（创建时快照）；Gemini 透传重试循环内不再重复解析同一 query 字符串；设置合并热路径的函数内 import 上提。
- 流式响应 ID 加入随机后缀，同秒并发的多个请求不再共享同一 ID；续传参数错误的 400 不再被兜底 except 重写成「上游错误」分类，兜底异常补打 traceback；代理空闲回收异常日志补上错误详情。
- 控制台缓存策略：带版本哈希的静态资源发一年 immutable 缓存（内容变更哈希/URL 一起变，无旧缓存风险），旧的无版本 URL 仍强制 revalidate；面板加载的两个独立接口并行拉取；日志突发时的全量 DOM 过滤改为无过滤快速路径 + 150ms 合并刷新。

## 全面复审收尾：异常响应、配置校验与启动取消

- Cookie 上游返回空 `errors` 数组时继续处理正常回复；异常响应结构、工具参数和完整正文 JSON 解码深度错误统一返回上游 502，避免内部 500 或静默丢失参数。非字符串错误消息仍保留可重试的状态码。
- 保存 Express Key / Cookie 账号前校验元数据类型和有限时间戳；旧配置中的无效时间戳归零，Cookie ID 去除首尾空格后再查找原凭据和检查重复。
- 工具 JSON Schema 拒绝嵌套 NaN / Infinity，避免无效数值进入上游请求。
- 请求结束或服务关闭后及时中止仍在等待就绪的代理转换器，并清理进程和临时配置；同节点还有其他请求使用时继续启动。
- 新增异常上游、管理配置写入边界与启动取消回归，完成后端离线检查、Go race / vet 和浏览器交互验证。

## TURN / SSTP 接入复审与多格式识别

- 面板允许先识别缺少凭据的 TURN / TURNS / SSTP 地址并填入字段；混合批量导入保留待补齐条目，补齐后才可应用。识别草稿与运行配置分别校验，添加后保留所选协议。
- TURN / TURNS / SSTP 共用供应商简写识别，支持正反 `@`、两种四段式、空格 / 竖线分隔和 Base64 凭据；新增 WebRTC ICE、节点 JSON 与混合数组导入。
- 保留节点备注和高级连接参数，补齐默认端口与本机 DNS 提示；直接加入 / 应用新粘贴内容时先识别，避免误用旧表单。编辑原始多节点 JSON 时先展开，防止只保留首条。
- 拒绝重复 JSON 字段、畸形 URL 编码、无效 SNI / MTU、OAuth 和 UDP TURN；只有 STUN 的配置明确报错，不能变成空代理列表。
- 修复取用与回收的竞态：在锁内刷新使用时间，旧监听器终止前不启动同节点的新进程；启动锁用完即释放，等价节点复用一个隧道。
- 原生代理和已启动转换器的异步解析使用缓存快路径，减少线程池调度；SOCKS5 就绪检查支持分片应答，SSTP 提前占用本地端口且各握手阶段共用总期限，双向转发正确处理半关闭失败。
- 补充协议输入、进程竞态、资源复用、半关闭错误与浏览器交互回归，并更新接入文档。

## TURN / TURNS / SSTP 节点接入

- 新增独立的 Go 转换组件：TURN / TURNS 使用 RFC 6062，SSTP 使用 MS-CHAPv2 和用户态 IPv4 栈；可直接填写节点链接或面板字段。
- 转换器与 sing-box 共用进程复用、在用保护、稳定端口、退出重启和空闲回收；缺少组件或节点失败时明确报错，不添加直连出口。
- 面板新增协议类型与 SNI / CA / Realm / MTU 高级设置；支持标准 TURN URI、混合代理列表、特殊字符编码与凭据脱敏。
- Docker 多阶段编译并内置转换器，无需新增运行权限；临时配置收紧权限并在启动后删除，日志只输出受控错误代码。
- 增加 Python 配置 / 进程回归、Go 协议 / 并发检查、SSTP 服务联调入口和浏览器验证，补齐部署与兼容性说明。

## 全面审查：配置迁移与流边界修复

- 日志历史重放后推进事件游标，消除与订阅队列重叠时的重复；多线程发布按事件序号入队，避免去重遗漏乱序日志；重启后的旧游标不会屏蔽新日志。
- 旧版 `hide_thoughts` / `thinking_force_console` 在后端统一兼容，显式保存的新思考模式优先；前端不再用旧开关改写新模式。
- 显式重载状态时替换磁盘快照，已从文件删除的字段不再残留；保存失败仍回滚到最近成功加载的快照。
- Cookie 生图假流式完整传递预填充和搜索状态，并遵守思考隐藏设置。
- 两通道共用嵌套消息 / 工具选择校验，畸形字段在准备连接前返回 400；保留未知扩展与旧式图片 URL 字符串。
- 修复分段工具结果包含 Pydantic 对象时无法 JSON 序列化，以及异常 Google 扩展元数据导致的转换崩溃；拒绝非有限请求数值。
- 提取共用有界 JSON 分帧器，原生 JSON 流每个对象只解码一次；首回复后继续检查截断、错误对象与候选结束标记。
- 原生 JSON 生图预读按对象上限管理，合法大图片不再被误判为超过 2MiB 的元数据。
- 新增 Cookie 账号由后端生成 ID，兼容普通 HTTP 页面没有 `crypto.randomUUID` 的情况。
- 前端代理文本框与环境变量一致支持逗号分隔的完整 URL；图片压缩应用 EXIF 方向并正确处理调色板透明背景。
- 新增离线回归并扩充浏览器兼容验证，同步修正代理首回复超时的架构说明。

## 首回复超时改为上游重试（不再切换出口）

- 首回复超时（`first_reply_timeout_seconds`）语义修正：上游连思考分片都未输出（思考回传开启时思考也算首回复），与 429 一样属于"上游无响应"，直接交给外层自动重试（退避 / 换 Key），不再切换代理出口——出口没有故障证据，不应被冤枉。
- 真传输错误（连接重置等）与 HTTP 407/502/503/504 仍按原顺序切换出口；代理切换回归其本职场景。
- 恢复多出口路径丢失的超时说明：`proxy_attempt` 窗口超时同样发出请求跟踪事件「等待模型首回复超过 N 秒（思考、正文、工具调用或图片均算回复），本次上游读取已中断，稍后自动重试」；该说明此前仅存在于单出口路径。
- 多出口超时的异常文案与单出口逐字一致（「等待模型首回复超过 N 秒（思考、正文、工具调用或图片均算回复）。」）：外层重试日志、最终 504 错误响应与 cookie 通道共用该文案，多出口不再显示"当前出口等待…"的缩水版本。
- 自动重试日志对首回复超时直接展示中文说明（含"思考、正文、工具调用或图片均算回复"），不再只报英文类名 `FirstReplyTimeout`。
- 修复计时归因：「第 N 次尝试等待 X 秒」随每次上游尝试（含换出口 / 换 Key 重试）重新起算，不再把已失败尝试的等待时间累加进成功尝试。

## 部署升级后控制台配置加载修复

- 修复代理编辑器脚本缺失或弹窗初始化异常时，后续 Key、代理、参数和模型加载全部中断的问题。
- 配置读写独立于代理连通性和弹窗组件，代理认证失败时仍能查看原代理并直接修改保存。
- 控制台资源使用统一内容版本，HTML 禁止缓存，静态资源强制重新验证，避免部署后混用新旧脚本。
- 账号组件支持自动重试和手动重载，重复加载不再冲突全局变量；加载失败显示明确提示。
- 浏览器回归覆盖旧脚本缓存、编辑器不可用、代理离线时修改保存及弹窗初始化失败。

## 多 Key / Cookie 混合调度与代理列表

- 增加多 Cookie 账号，每个账号独立绑定项目、启用状态、备注和代理；保留旧单 Cookie 配置与接口。
- 新增混合通道，两类账号共同随机或轮询；结构化输出在混合模式中只选择 Express Key。
- 新增多 Key / Cookie / 代理环境变量和 JSON 对象配置，支持账号级代理与回退策略；列表清空后不重新导入。
- 全局和账号代理均支持有序故障切换，每个账号可关闭向全局回退；代理重试独立于模型重试预算。
- 支持常见 SOCKS / HTTP 账号前置、主机前置和四段格式；IPv6 与特殊字符通过统一归一化处理。
- 代理编辑弹窗提供字段填写、智能识别、批量导入、排序和逐条测试；账号草稿随列表保存后热生效。
- 覆盖 SDK、原生 REST、Cookie httpx / curl、Key 探测、图片与模型列表出站；交付后的流不会重放。
- 收尾修复仅启用 Cookie 的混合模式模型目录、Cookie 导出格式识别、代理认证 / 网关错误测试结果，及首回复前空流 / 元数据断流切换。
- 代理弹窗排序保留未提交字段并标记编辑目标，重复导入按去重后的条数校验；补充真实套接字与浏览器回归。
- 弹窗内容独立滚动，标题和操作栏不遮挡表单；修复旧式 SS 分享链接在代理摘要、日志和健康检查中的脱敏。

## 控制台布局、响应式与配置交互收尾

- 统一登录页与控制台的卡片、表单、导航和配色；登录、字体与成功率显示均无需外部 CDN。
- 概览成功率按成功 / 错误计数计算，重试单独展示；输入 / 输出 Token 条按实际占比显示。
- 参数分区分别保存，模型保存栏随分区滚动；切换模型保留当前页面内的独立草稿并标记未保存，刷新后清空。
- 保存 / 探测时保护对应分区的编辑；阻止与保存冲突的重载，并修复不同分区并行保存时旧响应回滚默认值。
- 思维链守卫归入模型行为，采样提示跟随当前选择；校验错误保留完整原因并定位具体字段。
- 凭证按字段分组，项目探测保留未保存的 Key / Project ID / 代理草稿；长请求头档案仅在提示中展示完整值。
- 手机请求列表使用卡片布局，详情区分关键时序与展开诊断；刷新保留焦点、展开状态、滚动位置与文本选择。
- 日志增加搜索、级别筛选、连接状态与自动滚动控制；修复手动关闭后被滚动事件重新开启的问题。
- 分区链接支持直接打开和刷新定位；退出失败保留当前页面并明确提示。
- 前端脚本与组件样式独立维护；补充临时状态下的真实浏览器回归，覆盖 320–1440px 布局、慢响应、并行保存和异常恢复。
- 压缩请求头测试改为对照运行环境的 httpx 默认值，兼容可选 Brotli / Zstandard 解码器。

## 首回复超时重试与断连续传窗口

- 新增 `first_reply_timeout_seconds`，默认 15 秒，可设置全局默认或模型专属值，0 关闭。
  思考、正文、工具调用和图片均算首回复，心跳/纯用量/签名不算；每次尝试独立计时。
- Express 真/假流式、Cookie、原生 Gemini 复用现有重试次数、间隔与总期限，超时先关闭本次读取再重试。
  有效输出后不自动重发；非流式、假流式和整块生图以完整上游响应为界。
- 独立监听 ASGI 断连，及时更新连接与后台计数；不与请求体或流式断连监听争抢 receive。
- 新增 `resumable_disconnect_grace_seconds`，默认 10 秒，0 不复用。最后一个读者断开时开始计时；
  窗口内相同请求复用，窗口外新请求原子替换旧任务并等待旧任务释放名额，其他读者仍在线时不替换。
- 已超窗口的显式续传 ID / 游标返回 410，不把旧偏移应用到新答案；无新请求时后台仍按原期限运行。
- 监控区分模型首回复与正文首字，展示首回复等待倒计时、断开时间、续传截止以及替换原因。
- 离线回归覆盖单 Key 超时重试、思考/工具首回复、元数据排除、断连窗口、多读者、替换竞态、资源清理及设置持久化。
- 收尾修复原生 SSE 首回复前错误的状态码、错误体与 Retry-After 保留；区分底层网络超时与首回复期限。
  首回复期限设为 0 时仍保留元数据隔离与输出前重试，不改变其它重试行为。

## 请求监控、阶段耗时与首字诊断

- 新增请求监控列表与可点击详情，按状态/模型/ID 筛选、分页；运行日志中的请求 ID 可直接打开详情。
- 分离前端连接、上游执行、断开后后台生成和退避等待计数，续传读者关联原任务，不重复占用生成计数。
- Express 真/假流式、Cookie 及 Gemini 原生通道记录尝试、重试、首分片、首思考、正文首字、发送时刻与结束状态。
- 响应暴露 X-Trace-Id；async 任务及线程预处理沿用关联上下文，日志不再混淆并发请求。
- 历史数量、TTL、诊断文本与单次解析均有上限，接口受控制台会话保护，浏览器隐藏后暂停监控轮询。
- 回归覆盖发送失败、后台继续生成、续传关联、并发隔离、线程日志、SDK/原生重试与诊断预算。

## 酒馆长等待与重新生成诊断

- OpenAI 请求支持 `X-Resumable: false` 单次关闭续传，取消与重新发送不再因请求体相同接回旧缓冲；
  与续传 ID / 游标冲突时返回 400。保留默认的后台续传与显式幂等行为。
- 续传命中日志补充运行状态、等待时长和事件数；后台无新数据每分钟输出等待状态。
- Express 真流式增加首分片、首正文、思考/正文字数和结束原因诊断，重试日志显示实际错误码与等待时间。
- 修复仅有结束标记或签名被当成有效回复的问题；只有思考时保留协议响应并记录空正文警告，安全拦截仍保留原结束原因。

## 代码整理：职责拆分与公共逻辑复用

- `main.py` 仅组装应用、路由、中间件和异常处理；启动及资源清理归入 `lifecycle.py`，
  控制台会话归入 `console_auth.py`。
- 控制台页面移入 `templates/`，管理路由按页面、设置、凭证、系统检查分类；设置校验集中维护。
- Express 与 Cookie 复用消息注入和预填充处理；生成参数映射独立为 `generation_config.py`，
  按系统指令、请求参数、结构化输出、工具和生图职责拆成短函数。
- 离线回归移至 `tests/`，统一检查入口与 Docker 上下文同步调整；补充会话、模板、
  凭证热更新及参数优先级回归，静态检查增加未使用导入与重复定义规则。
- 现有接口路径、配置格式及页面内容保持兼容；维护入口说明见 `docs/architecture.md`。

### 后续审查：共享规则和错误边界

- 错误映射、重试配置与可重试状态、Express 路径解析分别归入独立模块，
  Cookie 和 Gemini 原生路由不再为这些规则导入 SDK 调用模块。
- 统一两条通道的用量尾块和开关读取，复用心跳常量，删除无调用方的字符串去重接口。
- SDK 三种交付模式共用请求拦截校验：修复假流式将安全拦截标为 500 的不一致，
  统一返回 400，并保留上游详细说明，不重试、不计为成功。
- 旧配置中的超大整数无法转为浮点数时，重试间隔与抖动回退到内置默认，避免读取时溢出。
- 移除与锁定 SDK 版本无关的旧版本探测分支和未使用的生图形参，补充对应回归。

### 循环复审：Cookie 协议、消息分类和缓存边界

- Cookie 通道按执行编排、请求构造、响应解析拆分；消息模块按消息转换、图片处理、
  思考签名、预填充分类，调用方直接引用对应职责模块。
- 分块 JSON 按边界收集，完整后解码一次；完整正文保留 C 解码器快路径。
  修复末尾半截对象被静默丢弃，以及完整对象绕过体积限制的问题，统一按 UTF-8 字节设限。
- 思考签名缓存分别维护 LRU 和写入过期顺序，修复读取旧签名后导致有效签名先被淘汰的问题。
- 图片压缩读取旧的无穷值或超大整数时回退默认值，避免参数转换溢出。
- Cookie 请求体中的思考配置复用同一份设置快照；Express 思考配置复用已读取的基础模型设置。
- 增加 JSON 分块、转义、截断、体积限制和缓存边界回归，静态检查增加未使用局部变量规则。

### 收尾审查：配置边界与系统诊断

- 全局和模型默认参数均校验正整数 token 上限、非负 temperature 和 [0, 1] 的 top_p，
  无效配置在保存时返回 400；保留 null 清除默认值和零采样值。
- 代理连通性测试不再隐式读取系统代理，遵循直连哨兵及自定义 CA 配置，耗时改用单调时钟。
  健康检查正确识别直连配置，HEAD 保留 GET 头部并在 ASGI 层省略正文。
- 增加管理接口的合法/非法参数持久化、健康检查和出站诊断离线回归。

## 第二十四轮：最终审查与 Cloudflare 长等待保活

- 真/假流式在代理启动、图片预处理等准备超过 50ms 时也提前建立 SSE，等待、生成和退避期间
  继续心跳；直通与续传均带 no-store/no-transform 和 X-Accel-Buffering:no。未开始读取即断连
  也会取消准备任务、关闭迭代器并归还并发名额。非流式 JSON 保持原协议，不能插入 SSE 心跳。
- sing-box 按生成跟踪代理使用（含线程预处理和重试换出口），请求结束前不会被空闲回收误停。
- 每轮总期限在准入时固定，非流式等待使用剩余预算；调低缓存容量不再取消已运行的请求。
  后台异常会写入明确错误并唤醒读者；停机等待已移出缓存但尚在清理的任务，不重复取消清理。
- 负数 Last-Event-ID 返回 400；无效 JSON、空原生 SSE 和畸形候选返回 502；原生客户端池满
  返回 Google 风格 503。自定义模型清单失败保留原缓存，不切换到其它维护者的远程清单。
- 项目探测写回使用条件更新，避免网络等待期间的旧结果覆盖刚保存的人工 Project ID；写盘在线程执行。
- 增加实际 TCP 长等待心跳回归，覆盖 Express/Cookie 假流式和续传开启/关闭，及取消/代理回收回归。

## 第二十三轮：按模型后缀选择抗截断

- 默认仅 Gemini 3.8 Flash / 4 Argon（4a）显示两种抗截断后缀，其它模型不再自动添加。
- 设置改为逐模型控制后缀显示（隐藏、两种、仅流式、仅非流式），不再提供全局开启。
  旧模型专属设置继续控制对应后缀，旧全局值不再应用；清除专属设置恢复内置后缀列表。
- 普通模型名始终不启用抗截断；请求参数不能开启或覆盖后缀。未开启的后缀明确返回 400，
  列表过滤与 Express / Cookie 的执行条件一致；后缀不发送到 Google 上游。
- 更新设置提示、接入示例与回归，覆盖默认列表、专属设置热更新、普通入口隔离和两通道实际请求。

## 第二十二轮：生成隔离、断流恢复与出站可靠性

- 每轮生成分配独立 ID，事件游标包含生成 ID；X-Resume-Id 直接查找已绑定原请求的副本。
  显式 Idempotency-Key 在 TTL 内保持幂等，旧生成可重放；统计改为纯 ASGI 中间件，
  结束帧发送失败不会提前标记交付。补齐 CORS 暴露与准备阶段保活。
- 普通 Express SSE 仅在最终候选块发送完整 Part 拓扑，保留签名并消除累计元数据的平方级传输。
  缓冲增加单次/总字节预算、运行期限和不打断其它生成的容量准入；全通道生成并发默认 32。
- SDK 请求通过实际 httpx 请求钩子设置连接/读取/写入/池等待超时，禁用重复内部重试。
  三种 Express 输出模式在换 Key 和裸模型兜底时保留正确代理，新代理启动也走异步路径。
  淘汰连接在宽限期内可重新复用，待回收队列有上限。
- Cookie 浏览器身份头由 curl 档案配套提供，移除导航头混入；尊重证书和重定向设置。
  图片对所有出口执行公有地址检查并固定连接 IP、保留 Host/SNI，代理失败不意外直连。
  未验证的 Cookie 结构化输出明确返回 400。运行/开发依赖固定版本。
- 原生 alt=sse 增加完整事件分帧、注释保活、错误和结束标记检查；可用 X-Resumable:true
  启用后台缓冲/续传，保持原生 JSON 格式且不添加 OpenAI DONE。其它原生响应保持透传。
- 退避增加指数增长和随机抖动，仍尊重 Retry-After。配置原子保存失败返回 503 并回滚内存，
  Cookie 和项目字段一起提交，管理写入移到线程执行。
- 新增真实 ASGI 发送失败、代际游标、预算、超时、实际 SDK 出站、本地浏览器头收包、
  代理隔离、连接复用、DNS 重绑定及持久化回滚回归；不使用真实 Google 凭据。

## 第二十一轮：收尾审查与上游资源回收

- 普通 Express / Cookie 回复也检查结束标记，覆盖真流式、假流式、非流式及
  Cookie 生图；缺失标记报告 502，不将半截内容计为成功，也不自动重复生成。
  SDK 多候选分别检查，Token 上限与过滤结束仍保留 length / content_filter。
- Gemini 原生通道在读取错误正文被取消、发送响应头失败、发送正文失败时，
  都显式关闭上游响应；清理屏蔽 Starlette 的持续取消，避免连接留在池中。
  正常正文和压缩解码后的 SSE 仍按原字节透传。
- 原生网络异常和错误正文中断返回 Google 风格 JSON；保留已知错误状态码，
  Key 池在退避期间被清空时，HTTP 状态与错误体的 code 也保持一致。
- Cookie 非流式异常保留真实状态码；流式出错、重试或工具降级时立即关闭
  旧事件迭代器，先释放被拒请求，再启动替代请求。
- 代理测试结果统一使用 HTML 转义；补充 14 项回归测试，覆盖无结束标记、
  多候选、ASGI 发送失败、取消作用域、错误格式及 Cookie 降级清理。

## 第二十轮：续传一致性与断连交付修复

- 显式 `Last-Event-ID` / `resume_from` 重连始终读取原缓冲，即使此前已标记交付，
  也不会重新生成再丢弃前几帧。缓冲过期返回 404，游标超过末尾返回 409。
- 流式初始化也由受管理的后台任务持有；准备阶段前端取消不再取消上游，
  同请求重连仍共享一次生成。容量淘汰与停机可以取消准备任务并唤醒跟读者。
- 非流式 JSON 和轮询在 ASGI 发送完成后标记交付；发送抛错时保留原回复供重放。
  JSON 轮询完成时增加 `payload` 和 `status_code`，实际交付正文及上游状态码。
- 缓冲完成或被淘汰后禁止追加/覆盖；上游取消后迟到的内容不会把 503 改为成功，
  流泵发现副本已结束即停止读取并关闭迭代器。直接替换同 ID 也会回收旧任务。
- 请求指纹递归按 JSON 对象键排序，字段顺序变化不再重复生成；数组顺序仍参与指纹。
  初始化异常给并发跟读者保留真实错误码，直接 HTTP 错误保持原响应。
- 新增 17 项离线回归，覆盖重连、ASGI 发送失败、迟到结果、准备阶段取消、
  停机回收及错误状态传递；默认抗截断模型设置保持不变。

## 第十九轮：流式等待、配置热更新与错误处理优化

- SSE 静默等待时发送注释保活，维持单个上游读取任务；关闭连接时取消等待并释放
  上游迭代器。续传读者同样保活，注释不入缓冲、不占事件序号。
- Cookie 非流式抗截断和全局假流式提前建立 SSE，等待期间保活；全局假流式开关
  对 Cookie 文本请求生效，错误按 SSE 错误事件返回且只计一次失败。
- 修复 Cookie / Project ID 环境变量覆盖控制台热更新的问题，Key 自己的项目仍优先。
- Cookie 流式按结构化 HTTP 错误码判断重试，避免 429 未重试、400 被关键词误判；
  错误事件保留真实状态码，不再追加正常 stop。未捕获的流异常也会明确报告并计数。
- 修复 keep_turn 无法保存、Express 生图 system 开关忽略模型专属设置、Cookie 生图
  始终发送 system 的问题。模型参数的局部更新保留其它已保存字段。
- 工具名非法时在请求入口返回 400；抗截断还原拒绝未声明的函数调用，随机名称冲突
  有次数上限。新增静默流、取消、续传、配置优先级和真实 HTTP 模拟回归测试。

## 第十八轮：按模型选择抗截断传输

- 文本模型在 OpenAI 模型列表中自动提供 `-antitruncation-stream` 和
  `-antitruncation-nonstream` 两种别名；控制台支持全局/模型专属默认值，
  单次请求可用 `anti_truncation_mode` 覆盖。Gemini 4 Argon / 3.8 Flash
  的普通入口默认开启流式抗截断，其它模型默认关闭。模型专属内置默认值
  与持久化设置按字段合并，清除专属设置后恢复内置默认，控制台与执行一致。
- Express SDK 使用原生增量函数参数流实时还原正文；旧模型、Cookie 通道、
  混合真实工具请求使用完整参数还原，不向客户端暴露临时函数。
- 保留预填充、真实工具签名、用量、重试、断线续传等现有处理；不兼容的
  搜索、结构化输出、强制工具选择等请求保留原协议。
- 缺失结束标记、重复调用或畸形参数返回错误；Token 上限/过滤结束原因保留。
  续传缓冲识别 SSE 错误帧，轮询/重放不再把失败结果标为成功完成。
- 新增离线通道与流式交付回归测试；思路来源及兼容边界见 README。

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

## 全项目四路深度审查（API/流式、投递恢复、内容模型、基础设施）

- 修复多图消息图片倒序：`_extract_markdown_images_to_parts` 在改为正序游标重组后仍残留旧实现的 `parts.reverse()`，多图消息整体倒序发给 Gemini（多图编辑时第一张图是编辑目标，顺序错乱直接改错图）。
- 修复生图/非流式请求被首回复窗口封顶永不成功：生图请求被服务强制走假流式/整取路径（用户无法规避），默认 15s 首回复窗口把整个生成封顶，FirstReplyTimeout 可重试导致空烧全部重试后 504。现在生图模型的整调用预算抬到 max(窗口, 300s)（文本模型维持配置窗口语义，测试契约不变）；首回复窗口每请求只读一次设置（重试不再重复深拷贝整棵设置树）；多代理出口不再把首回复窗口放大成 N×（钳制到原始剩余预算）。
- 修复 TRUST_PROXY_HEADERS 解析反转：黑名单只认 `""/"0"/"false"/"False"`，`FALSE/OFF/no` 等自然写法全被当成启用 XFF 信任——管理员明确关闭反而打开，客户端可伪造 X-Forwarded-For 逐请求换 IP 绕过登录限速。改为显式白名单（1/true/yes/on）。
- 修复像素炸弹守卫对最大的炸弹失效：低熵超大图（如 258KB 的 900MP PNG）解码到 PIL 的 DecompressionBombError 后被兜底 except 捕获**原样回传**，恰好绕过守卫。现在头部像素预检（2×上限直接拒绝）+ 炸弹异常单独拒绝（不落入原图回退）。
- 修复提取失败的图片残留整段 data URL：失败路径只打日志不推进游标，数 MB base64 留在文本里原样发给上游（日志却说"已跳过"）。现在失败也推进游标并留占位符。
- 修复控制台 413 响应形状分裂：BodyTooLargeError 继承 HTTPException 被内层 ExceptionMiddleware 先行转成 Starlette 默认 `{"detail":...}` 形状，StatsMiddleware 的 except 分支永不命中。注册显式处理器渲染 OpenAI 错误形状；256KB 登录上限的"约 1MiB"误标文案改为准确 KiB 单位。
- 修复重试标记数字误匹配：裸子串 `"429" in msg` 会命中 "port 4292"/"elapsed 42912ms"，把不可重试错误判成可重试（最多空烧 50 次重试）。改用带数字边界的正则。
- 修复状态落盘全程握状态锁：json.dump+fsync+replace 在 Docker overlay/网络卷上单次可达数百毫秒，事件循环上所有请求路径的 get_setting() 全部停摆。重构为「状态锁内快照（微秒级）+ 专用 IO 锁内落盘」，写入序号防旧盖新，并消除锁序倒置死锁风险；12 处调用点全部移出锁块。
- 修复 SDK 客户端池同签名竞态：冷启动并发时败者把胜者刚登记的客户端退役进 7230s 延迟回收池，N 路并发平白多建 N-1 个 SDK 客户端。对齐姊妹池的正确写法（复用胜者、退役自己）。
- 修复跳帧诊断恢复丢失：超限帧跳过后的分隔符检测只查当前块且不认 `\r\r`，分隔符被拆开时 skipping 永不复位，后续所有帧的里程碑/用量统计静默丢失；触发块自带终结分隔符的场景同样漏检。重写为「3 字节尾拼接检测 + 分隔符后残余回灌」。
- 修复 stdout 日志绕过脱敏：无 trace 上下文（启动期/线程池 worker）的 print 拷贝原样输出，Docker 日志/采集端恰在这里拿到明文凭据。与 SSE 流同口径 redact。
- 修复预填充流式去重 600 字符窗口截断：酒馆预填充普遍超 600 字符，复述进行中被强制判定导致整段复述漏裁（与非流式路径行为分叉）。复述状态（缓冲仍是前缀）时窗口放宽到 min(len(prefill), 4096)。
- 修复生图分辨率非法值放大到最高档：未知 image_size（客户端可控 extra 字段，如 "1024x1024"）回落到 4K 而非默认档 1K，一个越界值买到 16× 像素成本。
- 修复两条通道安全设置漂移：Express 恒发 BLOCK_NONE（评分返回后被丢弃），Cookie 跟随 safety_score 开关（OFF=关分类器）。现统一跟随开关。
- 修复 thought_signature 两侧解码策略不一致：入口宽松解码（非 base64 值变成乱码字节回传 Google → 400），出口严格校验。统一 validate=True + UTF-8 回退。
- 修复 stop 序列成员级空串下发：stop=[""] 的空串成员对 Gemini 是非法入参（整体 truthiness 检查挡不住），上游 400。补成员级过滤。
- 修复 curl 流式响应丢弃后再读死等：aread 漏检 _dropped 状态，半消费流上重新进入 aiter_content 永久死等到 60s 超时。
- 修复 impersonate 客户端池 verify 签名塌缩：verify=False 与未传（默认 True）同签名，先到者决定池化客户端的 TLS 校验行为。改用可区分的规范 repr。
- 修复温度默认值无上界：default_temperature 保存任意非负值（如 22），此后每个注入默认值的请求上游 400（不可重试），报错点离保存点极远。补 [0,2] 边界。
- 生图元数据不再重复编码整图 base64：ordinary_parts 的消费端拒绝 data: 图片条目，两处重复编码只让每个生图回合响应体积翻倍且无人使用。改留占位符。
- 关机任务排空加固：关机窗口内新注册的后台任务立即取消（此前单次快照会漏掉收尾期间新注册的任务成为孤儿）；sing-box 版本子进程探测移出事件循环（此前挂起的 sing-box 可拖慢启动接收流量最长 10s）。
- SSE 帧切分与错误包裹优化：帧缓冲改延迟压实（每帧 memmove 剩余字节的平方级搬运只在过半时发生一次）；prepare_sse 快窗扩到 250ms（立即拒绝的错误以真实状态码返回的比例更高），慢路径把上游状态码与 Retry-After 以 SSE 注释行透出（此前两者皆丢）；首个 SSE 帧前的元数据不再重复序列化（测量与下发共用一份）。
- n>1 流式预填充先行帧补齐：此前只发 choice 0，与非流式的全 choice 行为分叉。
- 其他：抗截断代理项筛查改 C 级正则快速路径（每 token 的逐字符 Python 循环只在异常输入触发）；续传全局预算读取加 1s TTL 缓存（每帧不再进状态锁）；请求注册表字节预算只统计已结束 trace（活动集合超限时不再清空全部控制台历史）；apply_sampling_policy 返回缓存副本防未来调用方污染；已死的 create_final_chunk 移除。
