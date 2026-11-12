# vertex2openai 三轮重做 — 变更说明

基于原版 `vertex2openai-main`（2026-09-01），持续完善兼容性、可靠性和代码组织。

## 2026-11-13：性能与可维护性复审

- 日志投递从 `logger.py` 分离到 `log_stream.py`；线程到事件循环的交接缓冲同样有界，每订阅者最多一个待执行回调，避免日志风暴在 asyncio 回调队列中持续积压。
- 文本准备与媒体处理的准入名额跟随执行器独立管理；取消请求继续持有名额到实际线程退出。
- 续传 JSON 字节计量改用精确的分块编码计数，大负载在线程执行，避免仅为计量就物化整份 JSON 文本和字节串；关闭诊断时非流式观察器不再保留正文副本。
- 搜索、生图分辨率和抗截断模型目录规则归属 `model_catalog.py`，三协议共用标准变体展开；历史导入路径兼容，新增模块反向依赖检查。
- 增补按职责组织的测试索引与日志微基准，修正架构文档中已过时的 pytest 执行方式。完整审查记录见 `docs/optimization-review-2026-11-13.md`。

## 2026-11-13：修复后台模型刷新覆盖本仓库新增模型

- 默认远程模型清单和备用 CDN 改为 `zhu748/vertex2openai`，避免后台刷新时被原仓库不含 `gemini-4-hydrogen` 的清单覆盖。
- 文档补充 `MODELS_CONFIG_URL` 的优先级、成功刷新间隔及现有部署的修复方法。

## 2026-11-12（第二十轮）：收录 gemini-4-hydrogen；抗截断默认收敛到 3.5 Flash-Lite；首回复超时默认关闭

- 新模型：`vertexModels.json` 收录 `gemini-4-hydrogen`（用户确认 2026-11-12 已发布）。能力档案经 `gemini-(\d+)` 家族识别自动归入 4 系规则（思考档 low/medium/high、默认 medium、剥离 temperature/top_p/top_k），目录自动展开 `-search` 变体；按用户决策默认不开抗截断，如上游实际 API ID 与此不同只需改该文件一行。
- 抗截断默认集合收敛（用户决策 2026-11-12）：内置 `model_overrides` 仅保留 `gemini-3.5-flash-lite: both`，移除 `gemini-4-argon` / `gemini-4-carbon` / `gemini-3.8-flash` 三个内置条目——目录默认只发布 3.5 Flash-Lite 的 `-antitruncation-stream` / `-antitruncation-nonstream` 两个后缀，其余模型（含 4 Hydrogen）默认仅普通名称与搜索别名，可在控制台逐个开启；裸模型名永不启用抗截断、未开启的后缀返回 400 的语义不变。
- 首回复超时默认 15.0 → 0.0（关闭）：`DEFAULT_SETTINGS.first_reply_timeout_seconds` 归零，`first_reply.timeout_seconds()` 三处兜底（缺键 / 非有限数 / 类型错误）与控制台模型参数表单空值回退同步从 15 改 0——真流式请求不再因首回复超时被断开，需要时仍可在「模型参数」设置全局或模型专属秒数（0–7200）；假流式/非流式/整块生图整取路径不参与首回复计时的既有语义不变，启用后超时仍沿用现有重试预算。
- 文案与文档同步：README（能力清单、抗截断模型选项、重试与超时章节）、docs/architecture.md、dashboard「模型参数」帮助文本与抗截断默认提示全部改为新默认口径。
- 测试同步：test_anti_truncation 内置默认集合断言收敛到 3.5 Flash-Lite（含目录 HTTP 展开与回退语义），test_console_usability / test_streaming_optimizations / test_tool_stream_deadline 的沿用模型从 4 Argon 换为 3.5 Flash-Lite。

## 2026-11-11（第十九轮）：首推 GitHub 后 CI 红构建修复（pytest 缺失 + Go 1.27.4）

- 背景：第十八轮收尾推送 GitHub 后 "Build & Publish Docker Image" 首跑双红。test 矩阵挂在 check_project「单元测试」组：`No module named pytest`——P2-8 把单测切到 `python -m pytest` 单进程模式时 pytest 未进 requirements-dev.txt，本地沙箱常驻 pytest 掩盖了缺口，CI 干净环境当场暴露（pip-audit 与其余 8 组检查均通过，publish 因 test / proxy-bridge 未过而跳过，amd64 / arm64 双架构镜像构建本身成功）。proxy-bridge 挂在 govulncheck：GO-2026-6859（net/http MaxBytesReader 头部竞态导致 HTTP/1 server crash），go1.27.3 受影响、1.27.4 修复，SSTP 侧 `init → http.HandleFunc` 等三条符号路径实测可达。同批 Dependabot 自动开的 5 个 PR（actions 升级 ×4 + pion）基于修复前 main，同因红，待本轮合入后重定基。
- CI 依赖补齐：requirements-dev.txt 补 `pytest==9.0.2`（与十八轮验证版本一致；conftest 三层隔离只用 pytest 核心，无 asyncio 插件依赖，pytest.ini 维持最小配置）。
- Go 工具链升级：go.mod `go 1.27.3`→`1.27.4`（setup-go 按 go-version-file 跟随）、Dockerfile 构建段 `golang:1.27.3-bookworm`→`1.27.4-bookworm`（镜像内编译产物同步换修复版标准库）、docs/proxy-turn-sstp.md 非 Docker 最低版本同步；proxy-protocol-validation.md 的 1.27.3 为历史验证记录，保留。
- SSTP await() 双就绪竞态（升 1.27.4 后 `go test -race` 暴露）：`await()` 的 select 同时等 `closed`/`ready`，attach 之后再 Close 的场景两 channel 双双就绪，select 在就绪分支间伪随机选择，约一半概率把已 Close 的 tun 当可用返回——1.27.3 的调度时序恰好总走 closed 分支，1.27.4 下 CI 稳定踩中另一侧（`TestSSTPPacketsCloseIsIdempotentAndConcurrent` 红）。生产路径同样受影响：Close 与 Read/Write 的 await 竞态时「已关闭必错」契约不成立。修复：命中 ready 分支后复检 closed（标准 select 优先级模式），语义不再依赖调度时序；全仓排查其余 select，relayIdleWatchdog（finished vs ticker）与 TURN 刷新循环（closed vs timer）两处双就绪均为良性（多跑一拍 / 多刷一次，无契约破坏）。
- 验收：干净 venv（Python 3.12.16，与 CI 3.12 矩阵同版本）仅装 requirements-dev.txt 后 `scripts/check_project.py` **9/9 组**全绿、全程 20s（单测组 16.7s，远低于该组 120s 子进程超时）——1,427 项测试 + 1,083 子测试 + 2 跳过，与第十八轮基线一致；Go 侧本地 1.27.4 实测：gofmt / vet 零告警、全量 `go test -race` 18.4s 通过、SSTP 六测 20 连跑全绿（修复前单次约 50% 失败率，20 连跑几乎必挂）、govulncheck「No vulnerabilities found」exit 0。

## 2026-11-11（第十八轮・收尾轮）：QUA/ENG 域清零，审查 85 项全部处置完毕

- 背景：第十七轮清完安全域后，本轮处置审查报告最后剩余的 QUA 域 7 项与 ENG 域 7 项，含一项按审查预案**实测否决**（ENG-P3-5，net.Pipe 复现实验证明与看门狗契约冲突）。至此 2026-11 全面审查的 85 项发现全部闭环：83 项落地、2 项实测否决并留档（PRF-P3-2 join 峰值翻倍、ENG-P3-5 计数契约冲突）。验收：1,427 项 Python 测试（+14 契约）+ 1,083 子测试、check_project **9/9 组**全绿（新增 print 基线闸）、Go 侧 gofmt/vet 零告警 + `go test -race` 全绿（+6 SSTP 状态机离线单测）。
- QUA-P2-1（错误形状三处分派）：`request_errors` 的 ingress / body_too_large / validation 三函数各自重复同一套 OpenAI/Anthropic/Gemini 路径判断；收敛为 `_error_builder_for(path)` 单点分派（返回协议名 + 构造器，`_PARAM_UNSET` 哨兵区分「param 键缺省」与「param: null」），三函数只留各自 HTTP 语义；Gemini 分支改经 `native_passthrough._native_error` 复用唯一构造点，错误形状与旧实现逐字段一致（契约测试对齐）。
- QUA-P2-2（签名三态双实现）：`thought_signatures.build_tool_call_id` 与 `cookie_response.openai_tool_call` 各自重复「有签名→SIGNED / 同批后续→UNSIGNED_FOLLOWER / 其余→UNKNOWN」分叉；收敛为 `SignatureStore.register_call_signature(call_id, signature, *, unsigned_follower)` 单点，两个生产方一行调用（express_response 经 build_tool_call_id 自动受益）。
- QUA-P2-3（part_order 重组双实现）：`message_processing` 与 `cookie_request` 各自约 25 行同构的「按 part_order 重放 + 越界/重复索引剔除 + 剩余块兜底」算法；收敛为 `thought_signatures.merge_by_order()` 泛型单实现，`remainder_calls_first` 参数保留两通道既有兜底顺序差异（Cookie 剩余调用在前 / SDK 剩余普通块在前），part_order 非 list 返回 None 由调用方保留缺省拼装。
- QUA-P2-4（CJK token 舍入分叉）：`claude_protocol.estimate_text_tokens` 自带银行家舍入 + 下限 1，与 `usage.estimate_token_count` 的向上取整分叉（正则已在早前轮收敛，公式仍两个行为）；统一走 usage 单实现，count_tokens 场景的「非空至少 1」语义在 claude 层包装。
- QUA-P2-7（环境变量半集中）：STATE_DIR 默认值「.」曾在 4 个文件各写一遍、SINGBOX_*/PROXY_BRIDGE_*/IMAGE_* 等运行时开关散落使用点；`config` 新增集中读取层（`env_raw/env_str/env_flag/env_int/env_float/state_dir`，布尔口径统一 1/true/yes/on），13 个文件 22 处读取点全部迁入；顺带消除 `FORCE_SECURE_COOKIE` 在 lifecycle（非空即真）与 console_auth（布尔字面量）的口径分叉（统一为布尔，`ALLOW_DEFAULT_KEY` 同时接受 on）。
- QUA-P2-10（日志无级别体系）：`logger` 新增 `log` 门面（info/warn/error/debug，只加文本级别前缀、完整复用 custom_print 脱敏→trace 关联→SSE 推流→有界 stdout 队列管线）；新增 `scripts/check_print_baseline.py` **print 基线闸**（39 文件 / 262 处存量只降不升，字符串字面量与注释不计，`--update` 随迁移下调），纳入 check_project 第 9 组检查——裸 print 禁止新增，存量逐步迁移到 log 门面。
- QUA-P3-4（私有名 re-export 期限）：`architecture.md` 标注 `api_helpers` 兼容层 6 个 `_` 前缀私有名为**带期限过渡 re-export**（计划 2026-12 清理轮移除，新增测试禁止 patch 兼容层私有名，公共名不在移除范围）。
- ENG-P3-1（MTU 校验钳位错配）：`main.go` 校验从 576–9000 收紧到 576–1400，与 sstp.go 的 TUN 钳位上限对齐——旧口径下 1400–9000 的用户配置被无声改写为 1400；sstp.go 钳位注释更新（只剩 mtu=0 默认路径）。
- ENG-P3-2（turn.go 裸断言）：`raw.(*net.TCPConn)` 改 comma-ok + 受控错误码 `server_transport_unsupported`（未来 dialer 返回包装 conn 时不再 panic）；新错误码同步登记 Python 侧 `known` 映射。
- ENG-P3-3（诊断上限永不复位）：`diagnostic()` 从「进程生命周期 128 条硬上限」改为「每类 code 至多 8 条 + 全局 128 条兜底」——长运行进程早期噪声不再耗尽配额导致新错误模式全部静默；`os.Setenv(SSL_CERT_FILE)` 错误不再静默（受控码 `ca_setenv_failed`，Python 侧同步登记）。`TestDiagnosticTruncatesAndIsBounded` 更新为双上限断言。
- ENG-P3-5（ReadFrom 零拷贝，**实测否决**）：实现「解包对侧 countingConn → 委托底层 conn.ReadFrom → 返回时计数」后，net.Pipe 复现实验证明其与看门狗契约冲突——splice/内层拷贝的字节数只在**整个拷贝返回时**可得，慢速长传输期间 activity 恒 0，firstByte 分支会把活跃连接误杀（`TestRelayKeepsActiveSlowConnectionOpen` 立即失败）；「src 包 countingReader」又会破坏 splice 的原始 TCPConn 类型要求。回退并按 PRF-P3-2 先例把实测结论留档（重试前提：看门狗先改「心跳 + 字节数」双信号）。
- ENG-P3-6（SSTP 零离线覆盖）：新增 `proxybridge/sstp_packets_test.go` 6 项离线单测（可离线构造的 LightweightNetTun，net.Pipe 风格时序）：await 阻塞/放行、Close 先于 attach 的 await/Read/Write 全错且计数不误增、attach 撞已 Close 的竞态守卫、Close 幂等且 8 goroutine 并发不 panic、Close 解除挂起 Read、真实包往返（Stack().WriteUDP → Read 计 sent → Write 注回计 received）。
- ENG-P3-11（Python 侧 NOTICE）：新增 `NOTICE-Python.txt`（10 项直接依赖 + 13 项传递依赖的许可清单，读自安装元数据；curl_cffi 捆绑的 libcurl/BoringSSL 与 cryptography 的 OpenSSL 双授权单列），Dockerfile 将其随镜像分发到 `/usr/local/share/`；附 pip-licenses 再生口径。
- ENG-P3-13（ruff 扩 B/UP 族 + Go 静态检查）：静态检查组从 8 条规则扩至 19 条（+B023/B007/B017/B904/B905 与 UP009/UP012/UP015/UP017/UP034/UP037/UP041）；app/ 的 5 处 B023 闭包循环变量捕获补默认参数绑定（均同迭代调用、无真实缺陷，防延迟绑定回归），11 处 B904 补 `from e`/`from None`，5 处 B017 盲异常断言收窄到实测类型（ClientDisconnect/HTTPException/ServerError），3 处 B905 zip 补 strict=；tests/ 的 92 处 B023 豁免（夹具合法模式）。CI proxy-bridge 作业新增 `gofmt -l` 硬闸 + `go vet ./...`（全仓 Go 已统一 gofmt -w，旧 8 空格缩进清零）。UP006/UP035/UP045（约 390 处注解风格改写）按审查「逐步扩」意见继续分批。
- 测试：新增 `tests/test_qua_round18_contracts.py` 14 项契约（merge_by_order 重放/去重/越界/两通道兜底顺序、register 三态、估算统一公式、env 层矩阵、STATE_DIR 单点、三协议错误形状逐字段对齐、log 门面前缀、基线闸计数口径）；Go 侧 +6 项 SSTP 状态机测试。
- 遗留（后续迭代，均非审查项）：LargeJSONResponse 完整流式化（`.body` 契约五消费方迁移）、复杂度闸下一目标 35（extract_events 40 / _claude_stream_response 37 待拆）、UP006/UP035/UP045 注解风格分批迁移、存量 262 处 print 向 log 门面迁移（基线闸护航）、api_helpers 私有名 re-export 2026-12 清理。

## 2026-11-11（第十七轮）：SEC 域收尾（安全余项 10 项 + PRF-P2-5 补齐）

- 背景：本轮处置审查报告安全域剩余全部 10 项（SEC-P2-2/3/4/6、SEC-P3-1/2/3/5/6/7）并补齐 PRF-P2-5（第十六轮记录的「已在早期轮落地」实为 O-4 的折叠本身——折叠后摘要仍逐块处理整串，弱化窗口并未落地，本轮修正归档）。验收：1,413 项测试（+43 契约与子测试）+ 1,083 子测试、check_project 8/8 组全绿、prettier + jsdom 18 组通过。
- SEC-P2-2（状态静态加密）：新增 `state_crypto`（可选依赖 `cryptography`，未设密钥零开销）——`STATE_ENCRYPTION_KEY` 设置后 web_state.json 以 Fernet（AES-128-CBC + HMAC-SHA256）+ PBKDF2-HMAC-SHA256（200k 轮、每文件随机盐）密文落盘；明文文件设置密钥后首次保存自动升级；**密文信封 + 无密钥/密钥错误一律拒绝启动**（RuntimeError，不进损坏备份路径——静默降级会把加密状态覆盖丢失）；盐进程级认领复用，PBKDF2 每进程至多派生一次。README 标注该口令与状态文件等同 Google 账号密码。
- SEC-P2-3（会话双窗口）：控制台会话从固定 30 天改为「签发后 7 天绝对上限 + 闲置 2 小时超时」双窗口，鉴权成功滑动续期；泄漏 token 的完全控制期从最长 30 天收敛到 min(7d, 2h)。新增 `POST /api/sessions/revoke-all` 与控制台「撤销全部会话」按钮（口令疑似泄漏时的一键止损）。
- SEC-P2-4（明文 HTTP 告警）：登录成功且请求经明文 HTTP（且未设 FORCE_SECURE_COOKIE）时打印进程级一次性醒目告警；已存凭证（Cookie/Express Key）的部署启动时附 TLS 反代提醒；新增 `FORCE_SECURE_COOKIE` 环境变量（反代终止 TLS 但未传 x-forwarded-proto 时强制 Secure Cookie）。
- SEC-P2-6（保活私网确认）：环回/私网保活目标默认拒绝（被劫持的控制台会话可借保活任务对内网管理面重放 GET 并经 SSE 日志回读状态码——探测 oracle）；新增控制台设置 `keepalive_allow_private` 显式放行（「自我保活」127.0.0.1 与内网监控用户），设置保存校验支持同补丁先开开关再填私网 URL，重定向复检同口径。
- SEC-P3-1（CORS 同源默认）：`allow_origins=["*"]` 改为默认不装 CORS 中间件（同源部署对 SDK/服务端调用与自带控制台零影响）；新增 `ALLOWED_ORIGINS` 环境变量（逗号分隔显式放行，`*` 恢复旧行为）。两个断言 expose-headers 的既有测试改为「默认缺失 + 组件级 CORSMiddleware 包裹验证」双语义。
- SEC-P3-2（CSRF Fetch Metadata）：显式拒绝 `Origin: null`（沙箱 iframe 字面值，此前经 urlsplit 得空 netloc 被放行）；采纳 `Sec-Fetch-Site: cross-site` 作为独立否决信号（浏览器强制附加、脚本不可改）；无浏览器头的 curl/SDK 请求不受影响。
- SEC-P3-3（429 设置指引）：未开 TRUST_PROXY_HEADERS 时登录 429 附「所有来访者共享同一限速桶」的设置指引（反代部署下个别人的爆破会锁死管理员登录）。
- SEC-P3-5（生图 URL TTL）：下载 URL 有效期从 7 天收紧为默认 24 小时（`IMAGE_URL_TTL_SECONDS` 可调 60s–7d；URL 只含随机 token 不绑定请求者，聊天记录转发后任何人可拉）。
- SEC-P3-6（代理测试内网闸门）：连通性测试的代理地址为字面私网/环回 IP（含 IPv6 括号字面量与 userinfo 前缀解析）时默认拒绝（错误分类与延迟差异可区分内网端口状态）；新增控制台设置 `proxy_test_allow_private` 与环境变量 `DISABLE_PROXY_TEST`（整体禁用端点）。
- SEC-P3-7（工程加固）：compose 增加 `cap_drop: [ALL]`、`no-new-privileges`、`mem_limit: 1g`；CI 增加 `pip-audit -r app/requirements.txt` 依赖漏洞审计步骤（出现已知 CVE 即红）；顺带修复 `scripts/measure_join_peak.py` 存量未用导入（静态检查组因此 8/8）。
- PRF-P2-5（指纹弱化窗口）：超长字符串（>64KB）折叠摘要从逐块编码整串改为「首尾各 4KB 窗口 + 长度」——20MB 内嵌图在事件循环上的 40-80ms 重算降为微秒级；同首尾同长度的中段差异共享缓冲（文档化取舍：指纹是去重键非安全边界，语义等同重复请求）；64KB 以下仍全量摘要（中段单字符差异可区分）。
- 测试：新增 `tests/test_security_round17.py` 43 项契约（+17 子测试）——加密往返/迁移/拒绝降级启动/盐复用/缺依赖、会话双窗口边界/滑动续期/revoke-all、明文告警一次性/Secure Cookie 三态、CORS 解析矩阵、CSRF Origin:null 与 Sec-Fetch-Site 六态、429 指引、TTL 钳位、保活校验三态、代理私网闸门（含 IPv6 与开关放行、无网络推进证明）、指纹窗口四态。更新 5 个受新默认值影响的既有测试文件。
- 遗留（后续迭代）：QUA 域余项（QUA-P2-1/2/3/4/7/10、QUA-P3-4、QUA-P1-4 的 dashboard 命名空间部分）、ENG 域余项（ENG-P3-1/2/3/6/11/13 的 go vet/gofmt CI 部分）、LargeJSONResponse 完整流式化、复杂度闸下一目标 35。

## 2026-11-11（第十六轮）：性能余项收尾（PRF 域 9 项，1 项实测否决）

- 背景：第十五轮清完核心请求链路域后，本轮处置审查报告性能域剩余 9 项（PRF-P2-1/2/2/7/8、PRF-P3-1/2/3/4；PRF-P2-5 请求指纹弱化折叠已在早期轮落地）。其中 PRF-P3-2 按审查建议实施后经 tracemalloc 实测**否决**（join 方案峰值反而翻倍），处置记录写入代码注释。验收：1,370 项测试（+14 契约）+ 1,062 子测试、check_project 8/8 组全绿。
- PRF-P2-1（元数据计量物化）：Cookie 首回复前元数据暂存 `len(str(data).encode())` 在 native_part 含数十 MB base64 时先拼整串再编码只为算字节数；改 str/bytes 直接 len、容器走 `estimate_json_chars` 无拷贝结构估算（上限闸粗估口径不变）。
- PRF-P2-2（工具流记账）：`ToolStreamAssembler._account` 的 `repr(part)` 递归物化整个 Part 字符串（CJK 六倍展开、base64 全量渲染）；改按字段增量估长（text/signature/inline_data/partial_args 各取 len，dict args 走结构估算）。
- PRF-P2-7（redact 全文扫描）：custom_print 与 request_tracking 九处「先 4 正则全文扫描、再砍到上限」全部改为先截断再脱敏（logger 预截留 trace 前缀与原长标记的头寸；trace 侧 1200/2000/400/600 上限配 2000/3000/600/900 预截）；顺带修复 `retry(delay, error)` 收到异常对象时 `error[:N]` 下标崩溃（统一 `str()` 强转——此前 redact 内部兜底掩盖了这一点，先截断后必须显式强转）。
- PRF-P2-8（大请求体读取并发闸）：32MiB 请求体「全量读入 + Pydantic 解析」发生在生成并发闸**之前**，多路大 body 并发时峰值内存瞬时 GB 级。middleware 在鉴权后对「声明 >4MiB 或无长度头（chunked）」的生成类请求加 8 读位信号量：满时快速 503（Retry-After）；释放时机是关键——middleware 的 await 覆盖整个响应生命周期（含 SSE 流式），读位经 receive 包装器在「最后一个 body 块穿过」时释放（FastAPI 解析完成的最早时刻），finally 兜底幂等，不会退化成第二层生成闸。
- PRF-P3-1（JSON 分帧双拷贝）：`JsonObjects` 累计容器从 StringIO+getvalue 改片段表 + join——getvalue 把内部缓冲整份再拷一次，4K 生图单对象 30-90MB 收集期三份瞬时驻留降为两份。
- PRF-P3-2（**实测否决**）：按审查建议把 `LargeJSONResponse.render` 改 `b"".join` 后，tracemalloc 实测峰值 16.0MB（8MB 内容）vs BytesIO 的 8.76MB——CPython 的 join(generator) 先物化全部片段列表（2×），join(list) 同为 16.0MB。回退保留 BytesIO 并把实测结论写入 docstring（benchmark 脚本 `scripts/measure_join_peak.py` 留档）；真正能降一档的完整流式下发被 `.body` 契约阻塞（channel_dispatch / claude_api / chat_api 续传泵 / cookie_delivery / gemini_native 五处直接 `json.loads(result.body)`），列入后续迭代。
- PRF-P3-3（三处微观热点）：控制台日志历史从锁内 `list.pop(0)`（每条 O(n) 前移）改 `deque(maxlen)`；SSE 帧的 `created` 时间戳改秒级缓存（同秒复用一次取值，输出值与逐帧 time.time() 逐值一致）；Cookie 工具调用 ID 的 canonical dumps 去掉 `sort_keys`（确定性只需「同一响应字节流重解析得到同一顺序」，跨响应本就有 response_id 区分）。
- PRF-P3-4（同步 resolve 防御告警）：`resolve_proxy_uri` 的 sing-box/proxybridge 分支在**实际阻塞超过 50ms 且处于事件循环线程**时打印归因告警（当前调用图已被异步入口钉住，这是防新增调用方误用的护栏；按耗时判定，子进程缓存命中零误报）。
- 测试：新增 `tests/test_prf_round16_contracts.py` 14 项契约——工具流记账矩阵、redact 截断界与异常对象安全、logger 预截原长标记、读位闸（阈值矩阵 / 末块释放一次性 / 幂等兜底 / 满载快速 503 端到端）、日志历史 deque 淘汰、created 缓存、无 sort_keys 的 ID 确定性、阻塞告警不抛。
- 遗留（后续迭代）：SEC 域余项（SEC-P2-2/3/6、SEC-P3-1/2/5/6/7）、QUA 域余项（QUA-P2-1/2/3/4/7/10）、LargeJSONResponse 完整流式化（连同五个 .body 消费方迁移）、复杂度闸下一目标 35。

## 2026-11-11（第十五轮）：核心请求链路正确性收尾（REQ 域 11 项）

- 背景：第十二轮四阶段修复覆盖了全部 P1 与高优先 P2，本轮把审查报告核心请求链路域剩余的 11 项发现（REQ-P2-1/2/7、REQ-P3-3~10）全部处置完毕——除 REQ-P2-2/REQ-P3-3 外均为行为修正型（非重构），每项附特征回归。验收：1,356 项测试（+24 契约）+ 1,062 子测试、check_project 8/8 组全绿。
- REQ-P2-1（content_filter 丢签名）：Express 真流式 content_filter 等空响应路径首回复从未就绪，`pending_metadata` 暂存的思考签名帧随流结束静默丢弃——Gemini 3.x 多轮工具对话强校验签名，丢签名导致下一轮 400。收尾段与首回复分支同口径：先按原顺序冲刷暂存元数据再下发延迟结束帧（正常流该 drain 恒为空：`first_reply.iterate` 的 ready 先于 yield 置位）。
- REQ-P2-2（大负载离环序列化）：`json_encoding` 新增 `estimate_json_chars`（O(节点数) 结构估算，len(str)/isascii 均 O(1)，不物化拷贝）与 `json_response_offloop`（超 1MiB 在线程池内构造 JSONResponse，复用同一渲染器保证与直路字节级一致）；`resumable_poll` 与 `_resumable_json` 的三处大 payload 返回点全部接入——轮询一次可拉 500 帧、生图 payload 可达 30-90MB，此前同步 dumps 会拖停全站事件循环数百毫秒。
- REQ-P2-7（兜底锁死）：跨通道兜底中主通道 error 帧后**迟到的 content 帧**此前仍置 `saw_content`，流既不切备用通道、又因错误帧被吞而无 [DONE] 终止语义。修正后 `saw_content` 与 `fallback_started` 互斥（内容先于错误则错误分支本就不触发），迟到内容归入「已判死尝试」与收尾帧一同吞掉，兜底照常接力。
- REQ-P3-3（count_tokens 图片口径）：O-10 重构引入双重除法（累计时 `//4`、收尾再 `/4`），实际 len/16 与 docstring 承诺的 len/4 不符，Claude Code 依此裁剪上下文每张图低估 4 倍；`tool_result` 内层图片（多模态工具结果，协议支持）此前完全不计数。统一为 `note_image` 计数据长度、收尾一次 /4。
- REQ-P3-4（取消归因）：Express 三条 CancelledError 路径（真流式/假流式/非流式）此前一律打印「客户端已断开」——服务停机排空与续传超窗取消同样走这里，归因误导排障。新增 `_cancel_attribution`：先查 ASGI 断连，否则明示「服务端取消（停机排空 / 超窗 / 超时）」。
- REQ-P3-5（轮询参数形状）：`/v1/resumable/{id}` 的 from/limit/wait 改 str 接收 + 手工解析，类型错误统一 400 `invalid_resume_cursor`（不再漏成 FastAPI 422 形状）；顺带拒绝 `inf/nan`（旧 float 解析默默收下 nan，`min(nan,25)` 直接写进断连计时器）。
- REQ-P3-6（安全分丢失）：Cookie 流式四个错误终点（transport / api_error_text / cookie_error / retryable-fatal）此前丢弃已暂存的 `pending_safety_html`，安全拦截诊断线索消失。新增 `_safety_before_error_frames`：与正常收尾同口径先冲刷安全分（未建连时补 role 帧）再发错误帧。
- REQ-P3-7（心跳污染续传缓冲）：假流式心跳从 `chatcmpl-keepalive` data 帧改为 `: keep-alive` 注释帧（与真流式同口径）——`pump_stream` 天然过滤注释帧，心跳不再入库占事件序号、断线重放不再收到大量空 delta；`_sse_frame_kind` 两处 keepalive 特判随之删除（旧形状防御性归 other）。
- REQ-P3-8（断连 502 呈现）：生图收集 `_collect_full_response` 的六处断连错误字典统一带 `code: 499`（`_CLIENT_GONE_RESULT` 常量）——消费方按 `res.get("code", 502)` 取码，缺 code 时客户端断开被呈现为 502 上游错误，污染归因与大盘计数。
- REQ-P3-9（prefill 丢失）：预填充先行帧从 `has_yielded` 隐式折叠中拆出独立闸门 `_ensure_prefill_frames`（`st.prefill_sent`）——签名/工具事件先到时置真 has_yielded（重试闸语义保留）但从未发过 prefill，后续正文分支被挡住、预填充整段丢失，与 Express 通道（签名暂存 pending_gate、首内容前冲刷后发 prefill）行为分叉；现在签名先到也能正确发出 prefill，且恰好一次。
- REQ-P3-10（usage 清洗统一）：`_anthropic_usage` 的 pick/detail 与 `ClaudeStreamState.feed_usage` 三处手写 try/int/max 收敛为 `_count_value` 单实现——溢出 float（inf/nan）、bool、非数值一律语义为「字段缺失」（pick 跳到下一别名 / detail 归零 / feed 保留上一快照），同名概念不再两个行为。
- 测试：新增 `tests/test_req_round15_contracts.py` 24 项契约（+3 子测试）：签名冲刷顺序、大负载字节级一致、迟到内容兜底/内容优先不兜底双路径、len/4 与 tool_result 图片计数、取消归因、轮询 400 形状、安全分先于错误帧、注释帧心跳、生图 499、prefill 独立闸（签名先到/正常/恰好一次）、usage 清洗矩阵。同步更新 `test_request_boundaries_and_cleanup` / `test_upstream_alignment` 中两处 keepalive 分类契约。
- 遗留（后续迭代）：PRF 域性能余项（PRF-P2-1/2/5/7/8、PRF-P3-1/2/3/4）、SEC 域余项（SEC-P2-2/3/6、SEC-P3-1/2/5/6/7）、QUA 域余项（QUA-P2-1/2/3/4/7/10）、复杂度闸下一目标 35（extract_events 40 / _claude_stream_response 37 待拆）。

## 2026-11-11（第十四轮）：复杂度闸清零基线 + 注解/覆盖铺开

- 背景：第十三轮遗留清单中「deliver_collect_response / _passthrough_request_impl / gemini_fake_stream_generator 再拆分」与「QUA-P2-5 第二阶段」全部落地，另完成 ENG-P3-7 三个低覆盖模块的直接契约测试。四个 C901 超标函数全部拆分完毕，**复杂度闸 ≤40 零基线运行**（审查原计划「逐年下调」，两轮即达成）；≥250 行函数 3 → 0 个。验收：1,332 项测试 + 1,059 子测试、check_project 8/8 组全绿（均为行为保持型重构，测试全程护航）。
- deliver_collect_response 拆分（337 → 151 行，C901 47 → 过闸）：事件收集（`_collect_whole_body` 游标循环 + 媒体线程分流 + `_apply_whole_body_event` 单事件分发，片段表 + join 的 O(n²) 防护语义保真保留在 `_WholeBodyState`）、结果装配（`_assemble_whole_response`：成功/无正文双路径 + message_obj/payload）、重试编排壳。
- _passthrough_request_impl 拆分（287 → 253 行，C901 41 → 过闸）：错误分类（`_classify_attempt_error`：NativeStreamError/超时/不完整/连接失败四象限）、流式预取（`_prefetch_stream_reply`：SSE/JSON 数组双形态 + 首帧预取）、缓冲透传（`_buffered_upstream_result`：读取关闭 + 空正文防冒充）。三处拆分均不跨越异常生命周期边界（上游连接的收尸仍归编排壳）。
- gemini_fake_stream_generator 拆分（261 → 210 行）：重选目标与失败路径记忆（`_fake_stream_retry_target`）、响应发射（`_fake_stream_response_frames`：校验/转换/巨帧 GC——调用方在进入子生成器前置空 `raw_gemini_response`/`api_call_task` 引用，4K 生图内存修复的释放时序逐点保真）、外层错误帧构造（`_fake_stream_error_payload`）。
- anthropic_to_openai_request 拆分（173 → 105 行，C901 43 → 过闸）：max_tokens 校验、thinking 块（budget→档位映射 + display 白名单 + 钉住语义）、output_config（response_format + effort 意图，`_EFFORT_UNSET` 哨兵区分「未提供」与「auto=None」）各自独立成纯函数；thinking 钉住与 effort 覆盖的交互在调用方一处收口。
- QUA-P2-5 第二阶段（注解铺开）：first_reply / streaming / tool_stream / message_processing 四个核心管线工具 88 处 ANN001/ANN201 缺口清零（usage.py 复查已全注解）；注解闸自 upstreams 扩至「upstreams + 管线工具」。
- ENG-P3-7（低覆盖模块补测）：新增 `tests/test_fingerprint_and_tls_contracts.py` 33 项契约——express_fingerprint 62% → **100%**（UA 档案四分支 / 标签补齐三分支 / importlib 三级回退 / 通道覆盖与直连头 / describe 四分支），proxy_tls 73% → **100%**（HTTPS 代理信任六分支：明文原样 / 默认不换 / SSLContext 复用 / verify=False 关校验 / capath / cafile），proxy_resolver 75% → 78%（二进制发现缓存三元组 / 版本缓存与子进程回退 / 收割线程幂等；进程管理内部按审查记录维持 mock 现状）。
- 复杂度闸注释更新：40 零基线；下一目标 35（extract_events 40 / _claude_stream_response 37 待拆，均为 yield 深嵌套的协议解析器，需独立特征测试护航）。

## 2026-11-11（第十三轮）：审查遗留清单的结构性工程全部落地（七项）

- 背景：第十二轮四阶段修复后，审查报告第四阶段「随迭代持续」清单还剩 7 项结构性债务（QUA-P1-1/2/4、QUA-P2-5/6/9、ENG-P3-4），另顺手完成 QUA-P3-2 复杂度闸锁住拆分成果。全部为行为保持型重构（测试与 patch seam 全量迁移验证），唯一有意的规范化：Cookie 流式四个终点路径的错误帧统一走 `create_openai_error_response`（拿到凭据脱敏 + 标准形状 + 1024 截断，此前两处内联 dict 无脱敏）。验收：1,299 项测试 + 1,059 子测试、`check_project.py` 扩至 8/8 组、jsdom 18 组 + Playwright 浏览器 38 组、prettier 全绿；≥250 行函数 5 → 3 个，无 400+ 行函数。
- QUA-P1-1（巨型交付函数拆分）：Express `stream_frames` 451 → 164 行编排壳 + `_emit_attempt_frames` 单次尝试子生成器 + `_StreamMeters` 计量类（usage 累计短路 / 候选标记 / 增量计数，仿 PendingMetadataGate 先例）+ `_prepare_candidate_payload` 载荷准备；预填充先行帧构造移入 `prefill.first_frames`。Cookie `deliver_stream_events` 439 → 91 行编排壳 + `_stream_attempt_frames` + `_finalize_cookie_stream` + `_StreamAttemptState`，错误帧与 role 首帧收敛为 `_error_done_frames` / `_role_frames`。拆分中发现并保真还原了「空响应先行的结束帧延迟发射」等三处隐式契约（回归测试全程护航）。
- QUA-P1-2（重试骨架收敛）：`cookie_delivery` 四条交付路径的重复样板收敛为 `_body_cache_for` / `_attempt_context`（body 双形态 + 降级锁定 + 请求头 + 调试日志）/ `_BackoffPolicy`（六处 `cookie_retry_backoff` 调用的参数包）。
- QUA-P1-4（前端拆分）：`channel-config.js` 1465 → 429 行（仅 Cookie 账号域），代理编辑器约 1,000 行拆为 `proxy-editor.js`，仿 `credential-list.js` 先例以 `window.proxyEditorKit` 显式命名空间协作（`mount(deps)` 注入 Cookie 侧闭包，5 处跨段引用收口）；HTML onclick 全局契约不变；`ensureChannelConfig` 懒加载改为双脚本顺序补载；顺手修复 dashboard.js 的 prettier 存量漂移（check:format 恢复全绿）。
- QUA-P2-9（gemini_native 拆分）：1,257 行单文件按职责拆为 `upstreams/native_catalog.py`（目录 / 变体剥离 / URL，197 行）+ `upstreams/native_passthrough.py`（透传编排引擎 + Google 风格错误形状，752 行）+ 路由层（鉴权 + 端点装配 + models 路由，411 行，兼容 re-export 保测试面）。14 个测试文件约 30 处 patch seam 按「引擎内部依赖 → native_passthrough、URL 常量 → native_catalog、stats 双打」原则迁移；对象级 patch（app_state / pool.get）与端点直驱 patch 不受影响。
- QUA-P2-6（AppState 拆解）：Express Key 与 Cookie 账号的 14 个 CRUD 方法拆出 `CredentialsRepository`（存储 / 持久化经宿主动态取用——reload 会整体替换 `_state`，不能快照引用）；AppState 保留同名委托转发与静态方法别名，36 个依赖方与 `patch.object(app_state, ...)` 测试面零改动。
- QUA-P2-5（类型注解）：`app/upstreams/` 十个模块 178 处 ANN001/ANN201 缺口全部补齐（含本轮拆分引入的新函数；`CookieGeneration` 经 TYPE_CHECKING 解环）；`check_project.py` 新增「注解闸（upstreams）」防回退。
- QUA-P3-2（复杂度闸）：`check_project.py` 新增「复杂度闸（C901≤40）」，存量 3 处超限函数（claude_protocol.anthropic_to_openai_request 43 / deliver_collect_response 47 / _passthrough_request_impl 41）逐函数 noqa 基线化，只挡新增、逐年下调；`check_architecture.py` 学会跳过 TYPE_CHECKING 守卫块（类型注解解环的标准手段不再误报环）。
- ENG-P3-4（Go 依赖治理）：CI 的 proxy-bridge 作业新增 govulncheck 漏洞扫描步骤（对 go.sum 锁定依赖树）；新增 `.github/dependabot.yml`（gomod 每周 + pion 家族分组、actions 每周）——yuhaiin 间接拖入的 gvisor / tailscale fork 依赖链自此有双通道安全更新面；go.sum 锁定语义不变。
- 文档同步：`docs/architecture.md` 模块表更新 gemini_native / runtime_state / channel-config / proxy-editor 四处职责描述。
- 遗留（后续迭代）：`deliver_collect_response`（337 行，复杂度 47）与 `_passthrough_request_impl`（287 行，41）的再拆分——两者本轮已通过骨架收敛与引擎隔离降低耦合，进一步拆分需独立特征测试护航；QUA-P2-5 第二三阶段（纯逻辑模块注解铺开 + mypy --strict 模块白名单）；`gemini_fake_stream_generator`（261 行）；ENG-P3-4 长期项（MS-CHAPv2 + PPP 最小自研替代 yuhaiin）。

## 2026-11-11（第十二轮）：按《全面技术审查报告》路线图完成四阶段优化（四提交）

- 背景：五维深度审查（安全 / 核心链路 / 性能 / 质量 / Go 与工程化，85 项发现）给出的四阶段路线图在本轮落地：阶段一安全止血、阶段二正确性与韧性、阶段三性能解锁全部完成，阶段四完成 8 项收敛类（巨型函数拆分与前端模块化两项结构性工程列入后续迭代）。每阶段提交前全量 pytest + `check_project.py` 验收，最终 1,299 项测试、6/6 组检查、jsdom 18 组 + Playwright 浏览器 38 组、`go build/test/vet` 全绿。
- 阶段一（安全，SEC-P1-1/2、SEC-P2-1/5、SEC-P3-4、QUA-P2-8）：出厂弱口令（`123456` / compose 占位值）默认拒绝启动，保留 `ALLOW_DEFAULT_KEY=true` 显式放行；新增 `ADMIN_PASSWORD` 与 `API_KEY` 拆分（未设置时回退），分享 API Key 不再等于交出控制台；Cookie reveal 接口加二次口令验证（复用登录指数退避限速）并逐次审计留痕，前端 prompt 交互同步；新增 `SecurityHeadersMiddleware`（全响应 nosniff / no-referrer / COOP，HTML 加 CSP + X-Frame-Options）；Dockerfile 追加 `--no-access-log`（uvicorn 访问日志泄 `?key=`）；compose 端口默认绑 `127.0.0.1` 并附 TLS 反代指引；出厂口令常量收敛单一来源。
- 阶段二（正确性与韧性，REQ-P1-1、REQ-P2-3/4/6、ENG-P2-1/2/3/4）：Express「location 钉定失败 → 裸模型名」免费回退改为按失败路径记忆——换 Key 重选到已失败的钉定路径时直接改用裸模型名，多 Key 轮询下配额烧毁最多放大 3 倍的问题消除（真流式/非流式/假流式三路径 + 轮询回归测试）；续传取消路径的 `buf.fail` 收尸用 `CancelScope(shield=True)` 包裹（二次取消不再留僵尸缓冲）；`x-request-id` 移出幂等键候选（GCP LB / Nginx 每次重试换新值，击穿续传语义）；CSRF 防线仅在 `TRUST_PROXY_HEADERS` 开启时采信 `X-Forwarded-Host`；Go 桥 accept 瞬时错误（EMFILE/ENFILE/ECONNABORTED）有界退避重试不再整个进程退出、SOCKS 成功/失败回包前刷新 deadline（拨号耗尽预算不再吞掉按码 failover 契约）；conftest 单例快照兜底显式告警；宽限窗口测试改事件驱动断言（消除 CI 墙钟 flake）。
- 阶段三（性能，PRF-P1-1..5、PRF-P2-3/4/6）：媒体池拆分为 prep（prompt 构建，每请求必经）与 media（图片解码 / 落盘 / Cookie 请求体）双执行器，慢 PIL 操作不再队头阻塞全站首字；Express 假流式/非流式的响应转换链（含生图 base64 解码 + 落盘）整体搬媒体线程，Cookie 流式对象按「含内联图片」廉价预检选择性搬运——事件循环上 100-500ms/张的同步落盘停顿消除；`_prune` 全目录扫描按 60 秒窗口摊销（大图强制扫描）；SDK 客户端池上限跟随 Key 数（`SDK_POOL_MAX` 可钉住），7+ Key 轮询不再被固定容量 LRU 误淘汰；`get_effective_settings` 增加修订号 + 每模型缓存（每请求 2-4 次整树 deepcopy 消除，失效矩阵覆盖 mutator / reload / 直写 / 原地 pop）；新增 `request_diagnostics` 热开关可关闭逐帧载荷检查；远程图片抓取按（代理, 证书）共享同步 httpx.Client；已完整交付且无幂等键的续传缓冲改 120 秒短窗口保留（`resumable_delivered_ttl_seconds`），稳态 RSS 基线回落。
- 阶段四（质量，QUA-P1-3、REQ-P2-5、REQ-P3-1/2、QUA-P3-1/3、ENG-P3-8/9/10/12）：API Key 提取五处平行实现收敛为 `auth.extract_candidate_key`（协议差异只留在错误形状）；`RETRYABLE_MARKERS` 从单词级（internal / unavailable）收窄为短语级（internal error / service unavailable），确定性 4xx 包装文本不再白烧重试；Claude SSE data 行按规范只剥一个前导空格（多行 data 拼接不再损坏缩进）、收尸 `except BaseException` 收窄；32MiB 请求体上限收敛单一来源；architecture.md 失效脚本引用修正、proxy-turn-sstp.md 补记桥内看门狗窗口；coverage 锁 7.13.1、新增最小 pytest.ini（rootdir 发现稳定）、check_project 入口目录预检与 FileNotFoundError 兜底。
- 兼容性说明：弱口令拒绝启动与 compose 回环绑定是**预期的行为收紧**（升级前请设置强 `API_KEY`）；reveal 需带 `password` 字段（前端已同步，第三方调用方需跟随）；未设置 `ADMIN_PASSWORD` 时控制台口令仍回退 `API_KEY`，存量部署零破坏。
- 遗留（后续迭代）：QUA-P1-1 两个 400+ 行交付函数拆分、QUA-P1-2 cookie_delivery 重试骨架收敛、QUA-P1-4 前端 proxy-editor 拆分与显式命名空间、QUA-P2-5 类型注解与 mypy 白名单、QUA-P2-6 AppState 拆解、QUA-P2-9 gemini_native 下沉、ENG-P3-4 Go 依赖树削减——均为审查报告第四阶段「随迭代持续」项。

## 2026-11-11（第十一轮）：按审查路线图完成 P2/P3 结构优化（两批十六提交）

- 背景：代码审查报告（P0→P3 路线图）指出的结构性债务在本轮全部处置完毕：上一批（835936c..ebc50d7）落地 P0 缺陷修复、P1 机械合并（常量收敛、helper 化、死代码清除、冒烟脚本与历史文档归档、重试退避窗口 8 处合一、Go 假 TURN 服务器合并、轮次命名测试改名）；本批完成 P2 全部八项与 P3 两项，每步 `check_project.py` 全绿验收。
- 模块归位（P2-2/P2-3/P2-5/P2-7）：`api_helpers.py` 的 Express 本体迁入 `upstreams/express_pipeline.py`（与 cookie 家族对称，原文件留兼容 re-export 层）；`cookie_proxy.py` 从 1,691 行拆出四条交付路径（真流式/非流式/生图假流式/缓冲假流式）到 `upstreams/cookie_delivery.py`，原文件只剩每请求上下文类、凭证装配与分发（357 行）；`chat_api.py` 的跨通道兜底引擎下沉 `channel_dispatch.py`（479 行），claude_api 改走公共 `call_upstream` 门面；Go `proxybridge/main.go`（555 行）拆为 main/socks/relay 三文件并补齐 `refuseOverloaded` 与 `diagnostic()` 的零覆盖测试。审查点名的三处跨模块私有 import 契约（claude_api→chat_api._call_upstream、claude_api→generation_config 下划线函数、cookie_native→cookie_request 私有函数）全部以公共别名收口。
- 重复收敛（P2-4/P2-6）：message_processing 三份同构的「分段 content 遍历骨架」合一（`_iter_content_segments`，含 text:null 兜底、实例归一、image_url 三形态统一，附三管线契约特征测试）；前端六对 Key/Cookie CRUD 平行函数参数化为 `credential-list.js` 共享编排，七处 `/api/settings` POST 样板收敛进 `postSettings()`（代理编辑器因 throw 契约例外保留）；`dashboard.js` 全局基座并入 IIFE 命名空间体系（函数静态挂载 + 可变状态 getter/setter 双向同步）。
- 测试基建（P2-8）：`check_project.py` 从「75 组逐文件子进程」改为 pytest 单进程运行（conftest 三层隔离生效），68 个测试文件头部的 STATE_DIR + sys.path 样板（434 行）删除，全套 1,274 项测试 16 秒跑完。
- 循环依赖（P3-2）：后缀表/`split_model_alias`/`supports_transport` 归位 model_capabilities（anti_truncation 单向 re-export）；impersonate 的上游池关闭改为向 client_pool 注册关闭器，`close_all_clients` 遍历注册表，client_pool 不再反向引用 impersonate。
- 覆盖率（P3-5）：引入 `.coveragerc` + coverage 依赖，全套基线 88%（15,790 语句 / 1,867 未命中），为后续测试合并决策提供逐模块数据。
- 遗留（后续评估）：`_stream_events` 的 status 链 handler 表化（446 行、控制流重排需独立特征测试护航）；gemini_native 透传引擎下沉（~30 处 patch seam 分布在 14 个测试文件）；runtime_state 五合一拆解（36 个依赖方）；SSTP 依赖树削减（产品决策）。

## 2026-11-07（第八轮）：Express 非流式请求跟踪上报思考档位与输出上限

- 背景：用户核实「3.5-flash-lite 开启全局最高思考强度并强制跟随上方档位（`native_thinking_mode=console`）后，非流式请求监控仍显示思考档位=默认，流式却正常显示 high」。属实——Express 通道的 `track_configure` 只在流式分支（`SDKGeneration.response()`）上报 `thinking_level` / `max_output_tokens`，非流式整取 `collect_response()` 仅上报 `mode` / `upstream_model`，请求监控对 json 模式永远显示「默认 / 默认」，让人误以为控制台强制档位没生效。实际上 `thinking_config` 在 `chat_completions` 阶段就已写入 `gen_config_dict`，流式与非流式共用同一份配置，档位照常下发（用户 trace 里非流式请求生成 2,645 字符思考即为佐证：`gemini-3.5-flash-lite` 家族默认档位是 minimal，强制 high 未生效不可能产出该量级思考）；Cookie 通道两种模式均上报，唯 Express json 模式漏报。
- 修复：`collect_response` 入口对齐流式路径与 Cookie 通道口径，从 `gen_config_dict` 读取 `thinking_config.thinking_level` 与 `max_output_tokens` 一并上报；无思考配置（legacy 模型）时上报 None，监控侧维持「默认」显示。纯诊断字段补齐，上游请求与响应行为零变化。
- 测试：`test_express_json_tracking` 新增 2 项契约（上报档位与上限 / 无配置时上报 None，用「客户端已断开」短路重试循环免去上游桩）；全量套件打补丁前后失败集合逐条一致（17 项既有跨文件隔离失败，原始 HEAD 同批次同样失败，零新增）。

## 2026-11-03（第七轮）：断开超窗后直接取消上游（不再后台空烧）

- 背景：用户指出「断开连接之后，超过了断连续传的时间，已经连不上了，应该直接取消请求上游，而不是在后台浪费请求」。属实——旧实现里 `resume_expired()` 只在懒检查点生效（重连 410 / 同指纹新请求替换 / 面板 meta），断开超窗后无人再来时，后台泵任务会把上游跑到自然结束，最长吃满整轮 `request_timeout_seconds`（默认 1800 秒）：慢思考/长生成请求的配额就这样替一个永远不会回来的客户端烧掉（README 旧文甚至明说「窗口不是后台任务的自动取消定时器」）。非流式路径最典型：HTTP 处理器挂在 `buf.wait_for` 等整取结果，连接早已死亡，上游却照常跑完。
- 修复：`ResumableBuffer` 在最后一个读者断开时启动续传窗口计时器（`_arm_grace_timer`，宽限取断开时刻快照、晚挂连接按观测断开时间回算 remaining）；到点复核 `resume_expired()` 仍成立且生成仍在运行时，经 store 注入的 `on_grace_expired` 钩子调用新增的 `ResumableStore.cancel_generation`：先给缓冲落 410 终态（`error_type=superseded`，trace 记「已取消」，面板可见），再取消在飞泵任务——setup 阶段直接取消 `_call_upstream`，泵阶段经 `pump_stream` finally / ManagedStreamingResponse 清理链关闭上游连接。缓冲本体保留：晚到重连仍是既有 410 懒检查语义，容量回收仍归 TTL 清扫；同指纹新请求的替换路径不受影响（泵已弹出，无需再等 5 秒释放）。
- 轮询防误伤：`/v1/resumable/{id}` 轮询是断线后的合法续接方式（短连接天生抗抖动），新增 `ResumableBuffer.note_client_activity(horizon)` 把窗口从最后一次轮询向后滑动（长轮询按声明的 wait 预支在途时间，封顶 25 秒），仍在轮询的客户端不会被超窗取消；客户端消失后，最后一次活动 + 窗口即为取消时点。重连（`attach_client`）、完整交付（`mark_delivered`）与生成完成（`finish`/`fail`）都会撤销计时器（`_cancel_grace_timer` 带 `current_task` 守卫，防止取消路径把「正在执行的自己」标成待取消而误伤收尾 await）。
- 文档：README「取消与重新生成」一节与设置表、控制台「断连续传窗口」帮助文案改写为新语义（窗口到点即取消，轮询滑动续期）；`resumable.py` 模块 docstring 同步。
- 测试：`test_resumable_delivery` 新增 `GraceExpiryTests` 六项契约（超窗取消并保留缓冲 / 窗口内重连撤销计时 / 轮询滑动窗口 / 长轮询预支 / 完成后不取消 / 非流式断开超窗端到端：上游挂起→断开→单次调用即取消、无重试风暴→410）；`test_review_round2` 轮询桩缓冲补 `note_client_activity` 接口。相关 13 个套件 272 项通过，与改动前仅有的 6 项跨文件状态污染失败（原始 HEAD 同批次同样失败，与本次无关）完全一致。

## 2026-11-03（第六轮）：整取路径退出首回复计时（假流式 / 非流式）

- 背景：用户核实「当前项目的假流式和非流式好像都会计入首字超时，这个不应该」。属实，且比表象更广——全部 6 条整取路径（Express 非流式/假流式、Cookie 非流式/假流式/生图整取、Gemini 原生非流式透传）都把**整段生成时间**套进 15s 首回复窗口，而真流式只计到首个有效分片：同一「慢但有效」的思考模型（首思考分片立即到、总生成超窗口），真流式成功、非流式/假流式必然 `FirstReplyTimeout` → 判可重试 → 同一请求原样重发仍超不过窗口 → `retry_max` 次空烧配额后 504。旧版只给生图模型放宽到 300s（`WHOLE_CALL_MIN_SECONDS`），文本思考模型照挂，且假流式/生图保护是服务端强制路径，客户端无法通过改请求规避（`scripts/repro_first_reply_wholecall.py` 三模式对照复现）。
- 修复（语义修正，非调参）：`first_reply.timed_call` 的 `whole_call` 分支不再计时（窗口强制置 0，`guard.wait` 直接放行），首回复窗口回归本职——只约束真流式（`guard.iterate` / `prefetch` 首个有效分片）。生图 300s 特例（`WHOLE_CALL_MIN_SECONDS` / `_is_image_model`）随之移除；`gemini_native` 非流式透传同规则（guard seconds 置 0）。整取路径挂起兜底不变：传输层读超时（Express httpx read=600s / Cookie read=180s）+ 整轮 `request_timeout_seconds`（默认 1800s）+ 断连检测。
- trace / 面板口径：整取请求 trace 记录 `first_reply_timeout_seconds=0`（请求监控不再显示首回复倒计时），成功仍记 `first_reply` 里程碑（完整响应到达即首回复）；控制台 FAQ 与设置标签、README 均注明「窗口仅约束真流式」。
- 调用点清理：`api_helpers`（假流式/非流式）与 `cookie_proxy`（生图整取/非流式/降级重发）不再预读 `fr_seconds`、不再传 `seconds`（每请求少一次设置树读取，`cookie_proxy` 的 `timeout_seconds` 导入随之移除）；真流式路径（`stream_frames` / `cookie_transport.stream_events` / 原生流式 prefetch / `proxy_attempt` 出口窗口）语义不变。
- 测试：`test_retry_windows` 旧契约「小窗口可约束挂起的非流式尝试（超时→重试→成功）」按新语义重写为「窗口小于上游耗时时不超时、不重试、一次成功」；`test_proxy_failover` 直接驱动 `timed_call`（不带 `whole_call`）的代理出口窗口契约不受影响；复现脚本三模式（真流式/假流式/非流式）全部通过。

## 2026-11-03（第五轮）：整取收集期驻留治理 + 后缀次序无关化

- 背景：第四轮收尾后按「开始进行全面优化」再通读生图全链路，深挖前三轮未覆盖的**响应收集段**与请求侧后缀组合次序。详见 `docs/archive/optimization-2026-11-03-image.md` 第五轮一节。
- 拉取式解析游标（内存）：完整正文路径旧用生成器消费——生成器在 yield 挂起期间其帧局部仍持有刚解析出的巨型 dict（4K inlineData 单张 30-90MB），消费方 `del obj` 无济于事；叠加采样器无差别留存，收集期恒驻 3 份全量。新增 `cookie_response.CompleteTextObjectCursor`：`next_object()` 返回后不持任何引用，配合消费方 `del obj` 让解析副本在事件消费前释放。解析语义与旧实现逐字节一致；`iter_json_objects` 的 TextSource 分支委托游标，其余调用方不变。
- 64MB 单对象上限误拒修正（正确性）：非流式完整正文沿用流式解析器的 `MAX_JSON_OBJECT_BYTES=64MB`——完整正文早已整体驻留，上限在此不设防任何东西，却把 base64 可达 53-93MB 的 4K 高细节 PNG 整单拒绝（用户可见失败）。游标取 `max(limit, len(text))`；**流式解析器上限语义不变**（那里真正防解析缓冲无界增长，回归测试固定分叉）。
- 诊断采样器「正文即停留存」（内存）：`RawSampler` 只服务「无正文」日志，却在成功路径把含巨型 inlineData 的对象钉到函数返回。新增 `stop_retaining()`（幂等，计数继续累计），`BODY_EVENT_TYPES`（text/image/function_call/native_part）到达即丢弃样本；无正文路径（含只有思考）样本照常保留、`dump()` 不变。生图整取、非流式整取、流式传输器（原生桥共享）三处接入；流式侧不 del obj（流式解析器仍是生成器，帧局部钉住，注释记录）。
- 收集拼接改「片段表 + 一次 join」（内存/CPU）：`full_text += data` 逐事件重建整串——生图响应常见「导语 + 巨型图片 + 尾文本」，尾文本一次多拷一份 40MB 级全量、多图 O(n²)。两条整取路径均改片段表 + join，拼接逐字节一致（回归保证）。
- collect_response 响应释放（内存，第三轮遗漏）：第三轮只修了生图假流式路径；非流式生图请求（`stream=false`）走 `collect_response`，httpx bytes 缓冲与解码 str 仍全程双驻留。补上同构的「text 取本地引用后置 None」。
- -search 与分辨率后缀任意次序生效（正确性）：目录从不叠加两种变体，但手打名两种次序都必须全生效——旧实现固定「先 -search 后档位」单次剥离：`model-search-4k` 残留 `-search` 发上游 404、`model-4k-search` 档位提示不可见（搜索生效但静默回落 1K）。`image_resolution_suffix` 允许档位后再跟 -search（resolve_image_size 的原名兜底两种次序都能读到档位）；新增 `strip_image_and_search_suffixes` 循环剥离，Express `_normalize_model_name` 与 Cookie `_prepare_cookie_generation` 统一接入；文本模型 -search 行为不变。
- 基准：`benchmark_image_path.py` 新增「Cookie 生图整取收集期新增峰值」子进程高水位差测量——48MB base64（4K 高细节档）收集期新增峰值 145.9 → 97.3MB（-33%，3 份→2 份）。
- 测试：`test_image_path_optimization` 新增契约 8-13 共 10 项（游标化释放 refcount 计量、采样器停留存/无正文保留、拼接逐字节一致、两种后缀次序的解析/剥离/Express 归一/档位兜底、64MB 上限分叉、collect_response 集成释放与拼接）；释放与采样器两条行为测试经「临时禁用→FAIL」验证非空转；`test_model_retry` 的 Cookie 桩迁移到游标（36 项全绿）。

## 2026-11-03（第四轮）：分辨率后缀目录可发现性 + 原生透传支持 + 测试隔离治本

- 背景：用户核实「分辨率此前只在控制台调、`/v1/models` 里生图模型后面没有分辨率」。属实：请求侧 `-1k/-2k/-4k/-512` 模型名后缀第三轮起已端到端生效，但三套模型目录（OpenAI / Anthropic / Gemini 原生）都不列出这些变体——GUI 客户端下拉选不到、只能手打，与 `-search` 变体「既支持调用、也列目录」的口径漂移；且 Gemini 原生透传对该后缀会直接 404（透传侧从不剥离）。详见 `docs/archive/optimization-2026-11-03-image.md` 第四轮一节。
- 目录可发现性：新增 `mc.image_size_variant_ids`（按模型白名单的目录变体单一事实源：flash 生图 4 档含 -512、pro/nano 3 档、单档模型与文本模型不列），`anti_truncation.catalog_variant_ids`（OpenAI + Anthropic 共用）与 `gemini_native._native_catalog_ids`（Gemini 原生）同源展开，原生目录条目附「等价 imageConfig.imageSize 档位」注记；与 -search / 抗截断后缀互不叠加。
- -search 规则生图误判修正（既有缺陷，顺手修复）：`search_variant_available` 只按 `"image" not in name` 排除生图，nano-banana 家族（名字不含 image）漏网——目录一直误列 `gemini-nano-banana-2.1-search`，原生透传侧还会给生图请求注入搜索工具。改为按模型档案判定（与分辨率变体/家族识别同源），文本模型的 -search 不受影响。
- Gemini 原生透传支持：新增 `_strip_image_size_variant`（与 -search 同模式的最小改写）——剥后缀并把档位注入 `generationConfig.imageConfig.imageSize`；客户端已显式设置时不覆盖（对齐 resolve_image_size 优先级链）；缺 `generationConfig` 时创建后注入（极简调用不丢档位意图）；countTokens 等只剥后缀；结构非法只剥后缀交上游报真实错误；Cookie 桥接路径经 parsed 原地注入自动携带；文本模型名（即使以 -4k 结尾）原样透传不变。
- 测试隔离治本（既有缺陷，顺手修复）：`test_audit_optimization.py::NativeLifecycleTests` 的 asyncSetUp 设置 `web_proxy`+Cookie 记录不恢复，与 `test_concurrent_console_updates.py` 泄漏的 Cookie 账号组合后，同进程后续的原生透传测试被改道 Cookie 桥接（Express Mock 桩失效）而失败——该泄漏在 HEAD 上即可复现，波及 `test_gemini_native_stats_retry` 18 项与 `test_protocol_alignment` 3 项。两处 setUp 改为快照-恢复全局通道/凭据/并发设置；全量失败 34 → 13，剩余 13 项与 HEAD 基线逐条一致、零新增。
- 测试：`test_image_path_optimization` 新增契约 7（变体白名单、目录变体与解析器口径一致性、原生改写注入/不覆盖显式值/countTokens 只剥/文本模型不动/条目注记）；`test_protocol_alignment` 目录断言更新并新增 `ImageSizeVariantPassthroughTests` 端到端 4 项（注入、显式优先、countTokens、文本模型逐字节透传）。

## 2026-11-03（第三轮）：生图链路全面优化（不限 4K）

- 背景：前两轮解决 4K 输出 OOMKill 后，通读生图全链路（输入摄取/压缩 → 档位与比例解析 → 两通道请求构建 → 响应收集/发射），覆盖 512/1K/2K/4K 全档位。详见 `docs/archive/optimization-2026-11-03-image.md`。
- 分辨率别名生效（正确性）：`-1k/-2k/-4k/-512` 模型名后缀此前只用于家族识别，从未转成 `imageConfig.imageSize` 也未从上游模型名剥离（目录无此 ID → 404）。新增 `mc.image_resolution_suffix`（仅生图档案生效），Express（`_normalize_model_name`，Claude 路由共用）与 Cookie（解析处 + `build_request_body` 幂等二次剥离）上游模型名剥离后缀；`resolve_image_size` 优先级改为 请求字段 > 模型名后缀 > size 档位意图 > 控制台 > 1K。
- OpenAI size 映射补全（正确性）：宽高比补上 dall-e-3/gpt-image-1 的竖横版（1536x1024→3:2、1024x1536→2:3、1792x1024→16:9、1024x1792→9:16，旧实现静默丢弃）；档位仅接受无歧义意图（长边 ≥2048→2K/4K、≤640→512，1024/1536/1792 保持默认 1K 不放大成本）。
- 输入透明抹平分流（内存）：旧实现对 RGBA/LA/P 一律 convert("RGBA")+白底+掩码 paste，24MP RGBA 输入抹平段新增峰值 RSS 206MB。现按模式与 alpha 极值分流（极值探测不物化掩码）：全不透明仅丢通道、全透明直接白底、无透明调色板走 LUT、真半透明才合成（颜色逐像素回归一致）。实测抹平段峰值 206→92MB（全不透明/全透明）、206→115MB（真半透明，行为不变）。
- Express 巨帧零步进（时延）：假流式 >1MiB 生图切片不再逐帧 sleep(0.01)（data URL 不可增量渲染，步进是纯延迟，Cookie 通道同路径从不步进），改 sleep(0) 让出循环；文本 10 段节奏不变。2.25MiB 发射 120ms 下限→4.2ms，4K 节省 0.5-1.6s、2K 节省 ~160ms，切片拼接逐字节一致。
- Cookie 整取提前释放（内存）：生图整取在 JSON 解析前释放 httpx 响应对象（bytes 缓冲不再与 .text 双驻留，4K 单图收集期少 ~30-50MB，所有档位均少一份），引用计数级断言验证。
- 基准：`scripts/benchmark_image_path.py`（PIL 像素缓冲 tracemalloc 不可见，抹平用子进程 ru_maxrss 高水位差测量）。测试：新增 `tests/test_image_path_optimization.py` 20 项回归（别名解析/优先级链/梯级回落/两通道请求体、size 映射、抹平四分支颜色一致性、巨帧零步进与拼接、整取释放），释放与步进两条行为测试经「临时还原旧实现→FAIL」验证非空转；统一检查 69/69 组通过。

## 2026-11-03（第二轮）：4K 生图崩溃的第二层根因——续传缓冲驻留 + 并发叠加 + 发射期碎片

- 背景：第一轮把单请求转换链峰值从 +250MB 压到 +150MB 后，用户场景（Render 免费档 512MB）仍会「成功几张图后突然崩溃、之后一段时间持续闪崩」。第二轮排查定位了三个叠加层：
- 根因四（续传缓冲全量驻留）：`resumable` 弱网可靠传输默认把**每条**响应完整缓冲在进程内（单流预算 64MB / 全局 256MB / TTL 30 分钟）。一张 4K 图（25MB+ base64）成功交付后仍要在 ResumableStore 里驻留半小时，几张开下来叠加转换链直接 OOMKill——这解释了「有时能出几张图，随后连一张都出不来」的时间累积特征。修复：生图模型默认绕过续传缓冲直通交付（`resumable.image_bypass_enabled`，新设置 `resumable_image_streams` 默认 false，内存充裕的部署可打开恢复生图断线重连）；chat 通道与 Gemini 原生透传通道同口径。文本流维持续传能力不变。
- 根因五（并发叠加）：`upstream_max_concurrency`（默认 32）对生图没有任何区分——两路 4K 并发生图即 +300MB 叠加。新增生图专用并发闸 `image_max_concurrency`（默认 2，控制台可热调，满时 503 快速失败）：`admission.GenerationLimiter` 参数化（设置键/默认值/满员消息/是否记 trace），`chat_api._call_upstream` 拆为闸门层 + `_dispatch_upstream` 分发层，生图流式响应把生图名额挂到最内层 `body_iterator`（随流 EOF/断开/取消释放，与全局闸同生命周期语义），非流式与异常路径立即释放；Claude 协议复用同闸（`traced=False` 避免双层租约重复记里程碑/双计 payload 观测）。
- 根因六（发射期驻留碎片）：第一轮的切片分帧仍先物化整张切片帧表（原文 + 切片表 ≈ 2×content 同瞬驻留），且实测**每张图发射完成后 RSS 残留 +25MB 不归还 OS**（glibc 对大量 256KiB 中小块的 arena 碎片），连发数张 RSS 阶梯式爬升直到限额。修复：切片全部惰性化——`message_processing.iter_giant_content` 生成器（`slice_giant_content` 保留为 list 包装，逐字节等价）；Express 假流式与 Cookie 两处热路径（`iter_openai_content_chunks`，`make_openai_content_chunks` 保留 list 形态）改按需产帧，每帧只在 yield 瞬间存在一份 256KiB 切片；发射完毕置空转换产物引用并针对巨帧触发一次 `gc.collect()`（毫秒级，回收 SDK/pydantic 引用环）。
- 附带（真流式巨帧防护）：`split_giant_chunk_payload` 把任何 >1MiB 的真流式 delta.content（文本模型叠加 responseModalities、未来直出 inline_data 的路径）拆成 256KiB 增量帧——finish_reason 只挂尾帧、其余 delta 键保留首帧、结构异常原样回退；`stream_frames` 的直接下发与首回复暂存（pending_metadata）两条路径都已接入。
- 评估后放弃（记录结论）：非流式「预序列化替代 JSONResponse」实测无收益——CPython json C 编码器的内部累积缓冲峰值本身就 ≈ 2.2×content，提前释放 dict 不改变峰值，保持 JSONResponse（`scripts/measure_emission_memory.py` 可复测）。
- 实测（77MB 基线，25MB base64 档）：发射阶段增量 +48MB → +1MB（tracemalloc）；全链路 RSS 峰值 164 → 145MB，且回收后 **102 → 77MB（零残留，完全回落基线）**——旧链路每张图 +25MB 的 RSS 阶梯爬升被消除；真流式巨帧单帧路径 78 → 54MB。叠加根因四/五的修复，512MB 容器上 4K 生图从「必崩/几张后崩」变为单请求峰值 ~220MB 且请求结束后内存完全回收。
- 测试：新增 `tests/test_image_lifecycle_hardening.py` 19 项回归（惰性切片与 list 版逐字节等价、Cookie 惰性帧等价、真流式巨帧拆分拓扑与回退、生图绕过续传缓冲的三态谓词、生图并发闸的非流式/流式/异常/文本不占名额/满员 503/计数幂等）；全项目测试与基线对比无新增失败（34 项环境依赖型失败与 HEAD 完全一致）。

## 2026-11-03：修复 4K 生图在 512MB 容器（Render 等）被 OOMKill 的崩溃

- 症状：Express Key 通道请求 4K 生图，等待 30-120 秒后监控面板出现「收到首个上游分片」里程碑，随即整个进程死亡重启（渲染页面刷新即见服务崩溃）；1K/2K 正常，4K 一张都出不来，全程无任何 Python 异常或错误日志。
- 根因：4K 图片单张 10-25MB，响应落地后的应用层转换链对其做 6 份以上全量驻留拷贝——SDK 解析（body bytes + base64 str + 解码 bytes）→ `_convert_image_to_markdown` 的 4 连拷贝（b64 bytes/str、data_url、markdown）→ 整图单帧 SSE 发射的 `json.dumps` + `f"data:..."` + ASGI `encode` 三连拷贝。实测 25MB 图旧链路峰值 RSS +250MB（忠实生命周期模拟，25MB → 328MB），叠加 FastAPI/uvicorn/SDK 基线后突破 512MB 容器限额，内核 OOMKill 直接 SIGKILL 进程——这正是"首个分片到达即崩溃、无异常日志"的机制。
- 修复一（转换拷贝减半）：`_convert_image_to_markdown` 不再先拼 `data_url` 再包 markdown，base64 一步拼出最终串，峰值驻留从 4 份降到 2 份（b64 str + markdown）；输出格式逐字节不变。
- 修复二（超大 content 分帧）：新增 `message_processing.slice_giant_content`（阈值 1MiB、切片 256KiB，拼接结果与原文逐字节一致）。Express 假流式 `_chunk_openai_response_dict_for_sse` 与 Cookie 通道（流式 `image` 事件 + 整取假流式，新增 `make_openai_content_chunks`）对超过 1MiB 的生图 content 改发 256KiB 增量帧：客户端按 OpenAI delta 拼接语义无感，单帧 `json.dumps`/`encode` 不再产生全量拷贝，同时消除了此前 13-33MB 的巨型单 SSE 事件对客户端/代理缓冲的压力。≤1MiB 的小图保持整图单帧，假流式 UX 不变。
- 修复三（提前释放 SDK 响应）：Express 假流式与非流式路径在 `convert_to_openai_format` + 用量记录完成后，立即置空 `raw_gemini_response`/`api_call_task`/`response_obj_call` 引用，让响应持有的图片原始 bytes（0.75× base64 体积）先行释放，不与后续分帧字符串叠加驻留。
- 实测（忠实生命周期模拟，含 SDK 解析，基准 77MB）：12MB 图 206→157MB；25MB 图 328→227MB（-31%），且分帧发射阶段不再新增峰值。512MB 容器叠加 ~150MB 应用基线后，25MB 4K 单请求峰值约 280MB，恢复生存空间；多请求并发或图生图（历史含图回传）仍建议 ≥1GB 实例。
- 测试：新增 `tests/test_image_memory_framing.py` 10 项回归（切片字节一致性、Unicode 边界、Express/Cookie 分帧拼接、role 只在首帧、小图单帧不变、markdown 格式不变）；全项目测试与 lint 基线对比无新增失败。

## 2026-10-31：请求稳定性与资源回收

- 修复 curl_cffi 在响应头到达前取消请求时遗留后台下载任务与连接的问题。按请求追踪底层句柄，取消或首回复超时后等待生产者归还连接，保留共享会话及其他活跃请求；覆盖流式、非流式和等待连接池时的取消。
- 生成接口在读取 JSON 前执行对应协议的鉴权；保留 Bearer、Claude x-api-key、Gemini x-goog-api-key 与已有查询参数写法及依赖替换语义。
- 为生成与管理写入增加上传期限（空闲 15 秒、总计 120 秒）、JSON 预检（64 层、10 万结构/转义工作单位）、并发上传限额（每进程 32 个）与输入内存总预算（64 MiB 原始正文、20 万工作单位）。上传完成即归还上传名额，生成并发仍按控制台设置；输入预算随实际工作结束释放。超时返回 408、复杂度超限返回 413、容量占满返回 503 与 Retry-After；不改变原始正文转发字节。
- 拒绝请求中的无效 UTF-8 与未配对 Unicode 代理项；诊断文本也做容错，避免单次异常输入污染请求监控。合法 emoji、转义文本与网络分块仍正常接收。
- 将 OpenAI 候选数 n 限制在 1–8，并保留模型能力限制；没有预填充时不再分配候选先行帧。
- 代理客户端构造使用本次异步解析出的出口，消除预热后进程退出导致同步重启阻塞事件循环的竞态；下一次尝试仍重新检查代理存活并在线程中重启。
- 普通工具 JSON 中的 image/image_url 业务记录不再被误当图片；Claude 错误工具结果通过明确的来源标记保留多模态图片处理。Claude SSE 错误保留 401/429/503 分类；Gemini 原生流拒绝结束后同一候选继续产生内容，允许其他候选和尾部用量。
- 前端断开后，续传任务与仍在工作的媒体线程继续持有输入预算，直至实际工作结束再归还，防止反复取消绕过总内存保护。
- 新增有界故障回归，使用本地真实 curl/HTTP 连接验证取消、超时、后续连接复用与活跃请求隔离，并覆盖 ASGI 入口、分块 Unicode、总预算回收和协议边界。
- 验证：本轮新增 30 项稳定性回归；全项目 65/65 组、18 组控制台 DOM 检查与前端格式检查通过。真实 Uvicorn 冒烟确认慢上传返回 408、未认证请求不读正文返回 401、随后健康检查正常且入口预算清零；另通过 1000 组随机 JSON / 分块边界检查。未使用真实上游账号进行长期并发压测。

## 2026-10-30：工具协议完整性与响应读取加固

- 修复 Express 生图请求携带工具声明时的旧关键字参数错误；保留明确声明的内建搜索，自定义函数与禁用工具不再触发异常。
- Cookie 流式重试同时认可顶层 HTTP 状态与 `extensions.status.code`：429/5xx 不再因文案变化漏重试，400/404 不因包含 quota/internal 而误重试。
- Cookie 原生桥接转换 `parametersJsonSchema` / `responseJsonSchema`，保留属性、必填字段与本地引用，冲突或无法转换的 Schema 返回 400；结构化输出开关继续生效。
- Gemini 3+ 的 Claude / OpenAI 图片工具结果转换为原生 `functionResponse.parts`，保留调用 ID、文本顺序、错误标记和并行结果分组。使用现有图片下载、SSRF、解码及压缩防护，另限制单条工具结果最多 32 张、32 MiB。私有协议明确拒绝原生工具字段时仅降级一次，仍保留视觉输入。参考 [Google 多模态工具结果文档](https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/tools/function-calling#multimodal_function_responses)；Cookie 实际接受能力仍需真实账号验证。
- Cookie 的 httpx / curl 整取请求和 Gemini 原生非流式请求改为边读边限额：解压后普通正文最多 64 MiB、错误正文最多 1 MiB，超限关闭响应；不改变正常生成流的分块交付。缓冲后的响应移除压缩头，避免二次解压。
- Cookie 原生非流式错误交由 HTTP 中间件统一计数，修复一次失败计为两次；删除 Cookie 非流式成功路径重复的账号健康更新。控制台非法输入测试只统计设置写请求，避免后台轮询干扰断言。
- 新增隔离回归，覆盖工具图片往返、私有协议降级、gRPC 分类、Schema 保真、HTTP / curl 读取预算、gzip 与真实 ASGI 统计。

## 2026-10-30：移除 Cookie 通道工具参数分片管线（上游实测不支持）

- 用真实 Cookie 直连 batchGraphql 实测（gemini-3.5-flash-lite / gemini-3.8-flash，identity 与 gzip 编码、ANY 强制工具、正文+工具混合共 6 组对照）：**正文逐块流式（约 100-150ms/帧），但工具参数从不分片**——`streamFunctionCallArguments` 被静默接受，`partialArgs`/`willContinue` 一帧未发，完整 `functionCall` 仅在生成结束后整块到达。结论：Cookie 端的分片接收/合并/增量交付管线是永远走不到的死代码，予以移除；Key/Express 通道的参数分片（官方 SDK 支持）完整保留。
- 移除内容：请求侧 `enable_argument_stream`（不再发送 `streamFunctionCallArguments`）、`ToolStreamAssembler` 的 Cookie wire 适配（`restore_wire_parts`/`_wire_*`）与 OpenAI 增量交付（`emit_deltas`/`tool_delta`/`tool_end`）、`CookieStreamDiagnostics` 分层诊断、字段被拒降级重试（`argument_stream_unsupported`）。Cookie 完整工具调用回归直连解析：`extract_events` 直接产出 `function_call` 事件，GraphQL 私有字段照旧忽略；若上游未来真发分片，按"参数未完成"明确报错而非静默丢弃。
- `tool_stream.py` 瘦身为 Express（官方 SDK）专用的分片合并器（`restore_sdk`），Key 通道的参数分片、首回复期限与文本传输行为不变（`test_tool_stream_deadline` 18 项全过）。
- 修复直连解析回归：Cookie 收到 200 后的本地解析/校验错误在传输层包装为 `ToolStreamError`，避免被 `PROXY_FAILURE_STATUSES`（502）误判为坏出口而空转全部代理（沿用 852a840 的排除语义，Key 空流故障转移不受影响）。
- 测试与文档：删除 `test_cookie_tool_deltas.py`；重写 `test_cookie_tool_stream.py`（保留完整调用解析、故障转移、静默排队超时，新增直连解析与"分片明确报错"断言）；README / architecture 同步更新；真实 Cookie 端到端实测两模型完整工具调用交付正常；`scripts/check_project.py` 63/63 组通过。

## 2026-10-30：Cookie OpenAI 工具参数增量交付

- 补齐 Cookie 原生工具到 OpenAI `delta.tool_calls` 的真正参数增量。此前仅把工具分片作为首回复进度，交付仍等待完整参数；现在可在上游生成结束前发送函数名称及 JSON 参数前缀，完成后不再重发整份参数。
- 参数 JSON 采用追加式序列化，保留字符串转义、UTF-16 代理项、零值、并行索引、晚到 ID 和签名；无法确认已关闭的嵌套容器保守缓冲，避免后续路径补写破坏已发送的 JSON。
- 参数完成和结束标记继续校验；一旦已向客户端发送工具增量，中途失败返回错误，不再重新生成另一轮。Gemini 原生桥接、Express 与内部文本传输继续使用原有完整调用组装路径。
- 增加 Cookie 每次上游调用的分层诊断：是否请求参数流式、有效首回复超时配置、响应头、首次正文数据、首个完整 JSON、真实工具分片数量和完整工具对象数量；诊断数据不结束首回复计时，不记录参数正文。
- 上游只返回完整工具对象时明确记录，不能据本地拆帧声称上游支持参数流式。新增 13 项回归、全项目 64/64 组检查及服务冒烟 25 项通过；详见 `docs/archive/cookie-tool-deltas-2026-10-30.md`。

## 2026-10-30：Cookie 工具分片兼容与错误归因修复

- 修复 Cookie 工具流把完整 GraphQL Part 当作 SDK Part 校验的问题；只转换函数调用及其签名/思考标记，避免无关 oneof 默认字段和私有元数据导致工具调用失败，普通 Part 继续原样保留。
- 兼容 `PartialArg.nullValue=0` 的数值枚举表示，保留原有 `null` / `NULL_VALUE`、路径关闭、布尔 false、数值 0 的语义；只移除协议外层的 `__typename` 与未知 null 默认字段，不修改用户参数，也不忽略实际未知的非空字段。
- 工具参数校验使用专用 `ToolStreamError`；Cookie 错误事件保留原始异常来源，避免本地 502 触发全代理轮换。真实 HTTP 网关错误、网络异常及已有空响应故障转移继续生效。
- 字段校验错误携带安全的字段位置和错误类型，例如 `functionCall.partialArgs.0.nullValue (literal_error)`，不记录生成内容或参数值。
- 新增 10 项 Cookie 回归，覆盖 OpenAI / Gemini 原生桥接、签名、11 个代理与 12 次重试下只请求一次、真实网络错误切换、非法字段边界及源对象不变。Cookie 专项 23 项、全项目 63/63 组检查、服务冒烟 25 项通过；首回复期限与完整参数交付策略不变。详见 `docs/archive/cookie-tool-stream-fix-2026-10-30.md`。

## 2026-10-30：三协议转换边界与流式完整性

- 补齐 Gemini `MODEL_ARMOR`、`IMAGE_PROHIBITED_CONTENT`、`IMAGE_RECITATION` 的内容过滤映射；无效工具调用和失败的图片生成明确报错，避免被 OpenAI / Claude 转换成正常结束或可执行调用。Gemini 原生路径继续保留原始结束原因。
- 非流式 OpenAI 响应保留上游 candidate index，与真流式对齐；索引缺省时才使用候选位置。
- Express、Cookie、Cookie 原生桥接及原生透传诊断按实际出现字段合并累计用量，补齐 Claude 输出专用用量更新的输入/缓存保留；显式零值可更新，累计值不重复相加，过期总量重新按分项计算。
- Claude SSE 收到 `[DONE]` 即结束并回收生产者；缺少结束原因、损坏 JSON 和非对象事件返回错误，不再补发成功结束。兼容注释与多行 data 混合、空 data 保活。
- Cookie 解析保留 `fileData`；原生桥接保留 URI、MIME、displayName 和签名，OpenAI 使用图片/文件链接展示，与 Express 流式和非流式采用同一规则。
- 修复 Cookie 图片签名误挂到前面文字、图片元数据重复保存 base64 的问题，覆盖图片与文字交错后的双通道历史回放。
- 新增 23 项转换边界回归；全项目 63/63 组检查、25/25 项服务冒烟通过。范围与保真边界见 `docs/archive/protocol-review-2026-10-30.md`。

## 2026-10-30：请求热路径与控制台性能优化

- 工具参数按字符串处理 Unicode 代理项，移除整棵参数树的 ASCII JSON 往返；重复 JSONPath 只缓存最近一次解析，继续检查未闭合字符串、数组缺口与非有限数值。
- 兼容 Cookie/GraphQL 分片中未使用 oneof 字段的 `null` 默认值，同时保留实际多值冲突校验，避免正常字符串、数值、布尔值被误报为冲突。
- 请求监控先用标量完成筛选与汇总，只为当前页生成完整快照；全局注册表锁不再覆盖逐条快照，慢分页不会阻塞新请求登记。
- Express 随机选 Key 直接使用安全随机选择，省去全池打乱；硬排除、失败 Key 软回避与单 Key 重试行为保持一致。
- 图片下载每跳只解析并验证一次 DNS，随后连接已验证 IP，继续保留原 Host/SNI、私网拦截、代理与逐跳检查。
- 控制台日志按 50ms 窗口合并 DOM 写入、过滤及滚动；待渲染队列与 DOM 分别最多保留最新 500 条，清空时同时清除待渲染消息。
- 增加热路径基准、9 项 Python 回归与突发日志 DOM 回归，详细数据见 `docs/archive/optimization-2026-10-30.md`。

## 2026-10-30：双通道工具生成与首回复超时

- Express 普通工具调用开启原生参数分片，真实函数名称/参数分片结束首回复计时；工具参数有界合并、校验完整后交付，避免长参数生成反复触发首回复超时。
- Cookie 的 OpenAI 真流式与 Gemini 原生流式桥接补齐相同的进度识别、参数合并、调用 ID/签名与 Part 顺序保留；空 Proto 字段不再误结束未完成的调用。
- Cookie 私有协议明确拒绝 `streamFunctionCallArguments` 时，只移除该字段并关闭被拒请求后重试，不降级原生工具、不改 `required`/允许函数集合，不重置首回复期限；该回退对本次请求后续重试持续有效。
- 心跳、用量、签名不算首回复，真正静默排队仍按原期限取消/重试；未完成参数不作为可执行工具调用返回。Cookie 私有接口实际支持情况尚需线上凭证验证。

## 2026-10-27：协议转换与工具往返审查

- Cookie 收到上游 `functionCall.id` 时完整保留，OpenAI / Gemini 原生响应与后续 `functionResponse.id` 一致；缺少 ID 的旧上游继续生成稳定回退 ID，兼容旧式签名 ID。
- Express 与 Cookie 均按历史消息顺序解析调用名，避免后续轮次复用 ID 时改写先前工具结果；排序元数据忽略重复、布尔和越界索引，防止同一调用被重复回放。
- Claude 的 `tool_result.is_error` 转换为 Gemini 工具错误响应，空错误和复杂结果同样保留失败语义；非法结果、标记、Schema、非有限采样参数和无法序列化的工具参数明确返回请求错误。
- Claude `input_tokens` 扣除缓存读取，避免客户端把缓存命中重复计入总输入；修复流式收尾清零 `cache_read_input_tokens`，真流式与非流式采用同一口径。
- 带工具调用的回复保留截断和过滤结束原因，避免被改写为工具就绪；统一 Express/Cookie 的真流式、假流式、非流式及 Claude 映射。
- 新增双通道协议往返回归，覆盖同名并行调用逆序回填、原生桥接、签名历史、重复元数据、工具错误、参数边界及 Claude 路由 400。

## 2026-10-27：控制台 Cookie 显示与复制

- 已保存的 Cookie 账号增加“显示 Cookie / 隐藏 Cookie”和“复制 Cookie”按钮，停用账号也可查看；每次读取当前保存值，支持自动续期后复制最新 Cookie。
- 默认列表继续只返回配置状态；新增登录鉴权的单账号读取接口，响应禁止缓存。显示框只读且不参与草稿和保存，隐藏后清空，避免查看操作覆盖原 Cookie。
- 复制优先使用剪贴板 API，普通 HTTP 页面回退到文本复制；无法自动复制时展示并选中文本，提示手动复制。

## 2026-10-27：工具、搜索和响应边界修复

- Cookie 的 `-search` 请求在默认/`auto` 模式下优先内建搜索，避免客户端附带的 `get_time_info` 等可选函数覆盖搜索；显式函数选择继续保留。Gemini 原生 Cookie 桥接同步保留搜索后缀意图。
- 统一内建搜索工具识别；Express 支持 `web_search` 等原生搜索声明，并保留普通自定义函数的原有语义。`tool_choice` 字符串兼容大小写，非法模式和未声明的强制函数在出站前返回 400。
- 新增 Express 工具选择日志，记录函数声明、内建搜索状态和选择模式；Cookie 记录搜索启用或忽略的原因。
- SDK 的 `BLOCKED_REASON_UNSPECIFIED` 不再被误判为安全拦截，覆盖非流式、真流式和假流式；真实拦截继续返回 400 且不重试。
- Cookie 原生 `allowedFunctionNames` 全部无效时明确拒绝，不再删除限制后扩大为任意函数调用；参数格式错误返回请求错误，避免内部 TypeError。
- 元数据缓冲清空后同步释放字节计数，避免后续有效分片被误报超限。
- 禁用工具时跳过无用的函数 Schema 转换；Cookie 优先搜索和生图路径也跳过不会发送的自定义函数转换。
- 修正首回复超时测试中 SDK/Cookie 函数引用的 mock，使毫秒级超时配置实际生效；取消测试等待上游真正启动后再验证关闭，避免线程调度引起的偶发误报。

## 2026-10-24 修复：curl_cffi 路径 SOCKS5 域名解析位置错误（测试通过但实战失败）

**现象**：面板「测试代理」通过的两条 socks5 代理，在 Cookie 通道实战请求里全部
失败：`curl: (97) cannot complete SOCKS5 connection to cloudconsole-pa.clients6.google.com. (4)`
（SOCKS5 reply 4 = host unreachable），或被包装为 `ProxyError` 触发代理切换，
两条出口耗尽后整请求 500。

**根因**：DNS 解析位置在两条路径上不一致——

- 面板测试走 **httpx**：httpcore 的 SOCKS 实现把目标**域名**原样发给代理，
  由**代理端**解析（`httpcore/_async/socks_proxy.py` 以 `_remote_origin.host`
  构造 CONNECT 请求）；
- Cookie 通道开启浏览器伪装后走 **curl_cffi**：libcurl 对 `socks5://` 的语义是
  **本机**先解析域名、再把解析出的 IP 交给代理。

容器本地 DNS 对 Google 域名被污染或不可达时（典型：大陆部署环境），代理会收到
一个连不上的假 IP，回 SOCKS5 reply(4) host unreachable。两条「好」代理都失败，
是因为它们被要求连接的是**同一个坏 IP**——代理本身没有任何问题。

**修复**（`app/impersonate.py`）：

- 新增 `_curl_remote_dns_proxy()`：curl_cffi 分支把代理 scheme `socks5://`
  统一改写为 `socks5h://`（域名交给代理解析），`socks5h/http/https` 原样透传；
- 改写发生在 `_create_upstream_client` 单点（生产路径唯一的 curl_cffi 会话
  构造处），httpx 分支与面板测试路径保持原行为；
- sing-box 本地入站（`socks5://127.0.0.1:port`，vless/vmess/trojan/ss/
  hysteria2/tuic/socks4 转换）同样受益：域名交给 sing-box 出站节点解析，
  不再受容器本机解析结果影响。

改写后「面板测试」与「实战请求」的 DNS 行为完全一致（都由代理解析），
测试结果恢复对实战的预测力。

## 2026-10-24 三协议对齐：模型目录变体统一 + 运行日志补齐（Gemini 原生 / Claude）

Gemini 原生（/v1beta）与 Claude（/v1/messages）端点是后加的，此前与 OpenAI 端点
存在模型目录与运行日志两类不对齐，本次统一。

- **目录变体展开单一事实源**（`app/anti_truncation.py`）：
  - 新增 `search_variant_available()`（-search 变体规则：Gemini 文本模型）与
    `catalog_variant_ids()`（完整展开：base / base-search / 已开启的抗截断后缀）；
  - OpenAI `/v1/models`（`models_api`）改为复用，行为逐字不变。
- **Anthropic 目录对齐**（`claude_api.anthropic_models_data`、`claude_protocol`）：
  - claude 别名之后同时列出与 OpenAI 目录同源展开的变体（-search 与按模型设置
    开启的抗截断后缀）——请求侧 `resolve_claude_model` 本就原样透传这些变体，
    目录从「少列」修正为与实际能力一致；
  - `anthropic_models_payload()` 新增可选 `catalog_variants` 参数，单参调用兼容。
- **Gemini 原生通道支持 -search 变体**（`gemini_native`）：
  - 透传侧新增 `_strip_search_variant()`：模型名带 -search 后缀时剥后缀并注入
    `{"google_search": {}}` 工具（仅对显式后缀请求做 body 最小重组；普通模型仍
    字节级透传，原注释「转发原始字节」的形态保真原则不变——该后缀是本服务
    扩展名，Google 无此模型，不重组只会 404）；
  - countTokens / embedContent 只剥后缀；客户端已带搜索工具时不重复注入；
    不支持变体的模型（生图等）回 400 明确报错；监控按客户端原始模型名记录；
  - `/v1beta/models` 目录列出 -search 变体（条目描述注明），`models/{id}` 检索
    同步接受变体名；抗截断后缀仍不列出（其传输语义只在 OpenAI 兼容管线实现）。
- **Gemini 透传运行日志对齐**（`gemini_native`）：
  - 流式完成日志从「共 N 字节」升级为 `✅ [Gemini 透传] {模型} | 流式转发完成 |
    耗时 X.Xs | 共 N 字节`，中止日志同样带模型与耗时；
  - 非流式成功路径此前零完成日志，补同构 ✅ 行；
  - `_NativeStreamState` 捕获尾帧 `usageMetadata`，完成时打 `💰 [算力消耗统计]`
    行（提示词 / 生成 / 思考 / 总计），非流式从缓冲 body 提取（子串存在才解析），
    与 OpenAI 通道的算力日志同口径；请求监控的 usage 此前已由中间件观察器捕获。
- **Claude 运行日志对齐**（`claude_api`）：
  - 流式翻译完整发出 message_stop 后打 `✅ [Claude] 模型 {显示名} | 流式翻译完成 |
    耗时 X.Xs | 输入 X / 输出 Y tokens`（客户端断开 / 失败不打，避免误导）；
  - 非流式响应补同构 ✅ 行；此前的 🔵 开始 / ❌ 失败日志不变。
- **请求监控面板**（`static/request-monitor.js`）：
  - 行内与详情标签从「通道 · 模式（协议被吞）」改为 `协议 · 通道 · 模式` 三段
    （OpenAI / Claude / Gemini 原生 · Express / Cookie / 混合 · 真流式/假流式/非流式），
    三协议共存的部署可分辨请求来自哪套端点。

测试：新增 `tests/test_protocol_alignment.py` 23 项（变体展开 / 三目录 / 透传
-search 改写与字节级透传不破坏 / 用量提取与日志 / Claude 完成日志）；全量
717 通过（基线固有的 2 个顺序敏感失败不变）。

## 2026-10-24 对齐上游 bad-woman/vertex2openai：跨通道兜底、坏凭证隔离与重试关键词加固

对比上游仓库 2026-10-23 批次改动（多 Key 轮换容灾、Key ↔ Cookie 跨通道互备、
500 智能重试体系），把其中当前仓库缺失的三项核心优化按本仓库架构对齐移植；
上游的统一调度引擎整体替换不引入（会破坏 first_reply/keepalive/resumable/
proxy_failover 等深度集成），仅提取其语义。

- **跨通道兜底重试（Express ↔ Cookie 互备）**（`app/routes/chat_api.py`）：
  - 新增 `cross_channel_fallback` 设置（默认开，控制台「重试与退避」卡片开关）：
    主通道在尚未向客户端输出任何内容前，遇 429 / 401 / 403 / 5xx 失败时自动切换
    另一通道接力完成回复；已输出内容绝不切换（半截答案不能重发，与通道内重试
    安全线同口径）。
  - 流式路径：包装主通道帧流，识别"错误帧前置"（真实内容帧之前的 `data: {"error": ...}`），
    吞掉错误帧与收尾帧后无缝切换备用通道；客户端只看到接力输出，无重复 [DONE]。
  - 非流式路径：主通道返回可兜底错误时用另一通道再试一次，失败保留主通道错误
    （不误导排查方向）。
  - 门控：结构化输出在未开启 `cookie_structured_output` 时锁定 Express；
    400/404/422 等请求级错误不兜底；备用通道需有可用凭证（Key 或 Cookie+项目）；
    兜底只一层不递归，受整轮请求期限约束。
- **坏凭证自动隔离与红色告警**（`app/retry_policy.py`、`app/express_key_manager.py`、
  `app/upstreams/express_sdk.py`、`app/api_helpers.py`）：
  - 新增 `is_auth_or_billing_error()` 分类：401/403、`API_KEY_INVALID`、
    `requires billing`、`account_suspended` 等命中即为坏凭证；`ValueError` 等
    本地参数错误不算（对齐上游）。
  - `get_express_record()` 新增 `exclude_keys` 硬排除参数：坏凭证当轮隔离，
    全部 Key 隔离完后返回 None 交由跨通道兜底接管；`exclude_key` 软排除语义
    不变（单 Key + 429 仍吃满完整重试预算）。
  - `retry_target_factory(error=...)`：把失败错误交给工厂做隔离标记；
  `_call_retry_factory` 用签名探测兼容旧式无参工厂（测试桩不受影响）。
  - Express 三条重试循环（真流式/假流式/非流式）：坏凭证**跳过退避等待立即切换**
    下一把 Key（优于上游的照常等待）；红色告警（🔴 含 Key 序号、掩码、项目 ID）
    每凭证每请求只打一次。
- **可重试关键词扩充**（`app/retry_policy.py`）：补充
  `incomplete chunked read`、`peer closed connection`、`server disconnected`、
  `connection reset`、`deadline_exceeded`、`backend error`、`pooltimeout` 等网络
  截断与服务端 5xx 文本短语——异常被上层包装成普通 Exception（类型信息丢失，
  isinstance 白名单失效）时的最后防线；既有数字边界正则（防 "4292ms" 误报）
  与永久状态码不重试语义保持不变。
- **Express 完成日志**（对齐上游）：真流式首帧「⚡ 连接建立，正在实时流式输出…」、
  完毕「✅ 流式传输顺利完毕 | 耗时 Xs」、假流式与非流式完成日志，便于观测。
- 不对齐项（核实后确认当前仓库已等价或更优）：Cookie `batchGraphql` 签名
  （两边一致）；Gemini 2.5 清理（模型列表已一致）；非流式错误双倍计数
  （已有 is_auto_attempt 外层统一计数机制）；`has_yielded_output` 细化
  （当前 first_reply guard + deferred finishes 实现更精细）。
- 新增 `tests/test_upstream_alignment.py`：48 个回归（关键词、分类、隔离、
  帧解析、兜底切换安全线、设置校验）；全量 694 通过，与基线一致
  （基线本身有 2 个与本次无关的顺序敏感失败）。

## 2026-10-23 代理面板与批量测试导入

- 代理管理拆分为列表、单条添加、批量导入，扩展列表宽度与移动端操作区；全局代理可在弹窗内保存生效，账号专属代理明确提示保存账号列表。
- 新增有界、逐条容错的导入预览：200 条候选、256 KiB 内容上限，保留原始行号或 JSON 路径；格式错误不阻断其他节点，重复连接忽略备注差异，STUN 与直连条目单独提示跳过。正式保存仍严格校验并限制 32 条。
- 批量导入默认先测试并勾选通过项，可取消测试筛选、手动选择、停止测试及重试失败项；追加时保留输入顺序，保存失败保留输入、选择和现有配置。
- 测试支持 Vertex / Cookie 两个固定目标，限制前后端并发和总时长；浏览器中止后取消实际探测，回收无人使用的临时转换进程，保留业务已接管的实例。
- 测试结果跟随节点与测试目标保存于内存，排序不丢结果；界面区分已配置、未测试和连通通过，报告 HTTPS 与转换准备耗时。
- 增加导入解析、并发准入、取消清理、保存回滚及手机端布局回归，保留编辑器失败时原始配置可读可改的降级路径。

## 2026-10-22 出站保活服务（移植自 gcli2api）

定期 GET 配置的 URL（默认 60 秒一次），防止免费托管平台冻结闲置实例，或驱动外部监控：

- 新增 `app/keepalive.py`：后台任务式保活（`KeepAliveService`，启动即打第一发、
  失败只记日志不重试、停机幂等）；未配置 URL 时不创建任何任务，零开销。
- 环境变量 `KEEPALIVE_URL`（留空 = 关闭，须 http/https）与 `KEEPALIVE_INTERVAL`
  （默认 60 秒，下限 5、上限 86400）仅作初始值；控制台「运行设置」页新增保活卡片，
  保存立即热重启任务（清空 URL 即停止），无需重启服务。
- 保活请求**直连**出站、不走出站代理（目标是公网监控端点或自身，跟随 Google
  上游代理反而会在代理不可用时连带失效）；单请求超时 30 秒。
- 保存时校验 URL scheme 与间隔范围；新增 13 个回归（含本地 HTTP 回路的真实 GET
  命中、API 热重启钩子与环境变量初始值）。

## 2026-10-22 Claude（Anthropic）Messages API 兼容层

对齐 vertex-master anthropic_handler 的完整协议兼容，Claude Code / Anthropic SDK /
Claude Code Router 等客户端可直连本服务：

- 新增 `POST /v1/messages`（流式 + 非流式）、`POST /v1/messages/count_tokens`、
  `GET /api/hello` 三端点；协议转换纯逻辑独立于 `app/claude_protocol.py`，
  上游完全复用 `/v1/chat/completions` 管线（双通道调度、重试、抗截断、预填充、思考档位）。
- 鉴权兼容 `x-api-key` / `Bearer` / `x-goog-api-key` / `?key=`；全部错误（含 413、
  上游透传、鉴权）返回 Anthropic 形状 `{"type":"error","error":{...},"request_id":...}`。
- 思考兼容：`thinking.budget_tokens` → 内部档位（≤1024 low / ≤8192 medium / >8192 high）、
  `disabled` → off、`adaptive` → 默认；优先级对齐 vertex-master——`thinking` 的
  enabled/disabled 钉住档位，`output_config.effort` 仅在 thinking 缺失或 adaptive 时生效
  （effort 校验始终执行）；`thinking.display: "omitted"` 响应侧不回传思考块（模型照常
  思考、用量照计，对齐 includeThoughts 口径）；历史
  thinking 块静默丢弃；响应侧思考块带 SHA-256 伪签名（流式增量哈希）。
- 流式状态机：`message_start` 先行、10s 空闲补 `event: ping` 保活（防 Claude Code SDK
  误判超时）、工具块延迟到收尾统一发射并单条完整 `input_json_delta` 下发；
  有界帧队列背压防慢客户端堆内存。
- 用量保真：`cache_read_input_tokens`（上游隐式缓存命中）与
  `output_tokens_details.thinking_tokens` 细分透传，ccusage 成本统计可用。
- count_tokens 两级口径：有 Express Key 时调 Vertex `countTokens` 拿模型侧精确计数
  （contents + system_instruction + 工具声明，与生成同分词器；模型路径按 Key 项目钉定）；
  失败或纯 Cookie 通道回退启发式估算（CJK ≈ 1 token/字、其余 ≈ 4 字符/token，
  正则批量计数，38 万字符 ~11ms）。
- 方法约束 405 Anthropic 形状：`/v1/messages` / count_tokens 仅 POST、`/api/hello` 仅
  GET/HEAD，其余方法 405（FastAPI 默认 405 是 `{"detail":...}` 形状，Claude 客户端无法解析）。
- `GET /v1/models` 按请求头切换 Anthropic / OpenAI 目录格式；新增
  `GET /v1/models/{id}` 单模型检索双格式（Anthropic / OpenAI `models.retrieve`），
  404 按客户端协议分别返回 `not_found_error` / `detail` 形状。
- `cache_control` 全程剥离（含 tool_result 复杂块透传路径）；中途 system 消息降级
  user 轮；tool_choice / stop_reason / 错误类型表全量映射。
- 测试卫生：multi_credentials 测试的状态污染（`channel_mode=mixed` 泄漏到同进程
  后续测试文件）修复为快照回滚，全量套件在单进程下恢复可信信号（原先 6 个
  顺序依赖失败）；新增 62 个 Claude 协议测试与 `scripts/smoke_claude_round3.py` E2E 冒烟。

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
