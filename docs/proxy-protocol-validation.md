# 代理协议与转换验证（2026-10-25）

基线 `ddabde1`，审查当前工作区的代理导入、分享链接转换、SDK HTTPX、Cookie
HTTPX / curl、sing-box 和 TURN / SSTP bridge。以下结果区分解析、引擎配置检查和
真实数据转发；配置能保存或引擎能解析，不等于远端代理已经可用。

## 本轮验证结果

| 协议 / 路径 | 本轮证据 | 尚未验证的边界 |
| --- | --- | --- |
| HTTP、HTTPS、SOCKS5、SOCKS5H | 真实 loopback 代理；4 种入口 × SDK HTTPX / Cookie HTTPX / curl × HTTP / HTTPS 目标，共 24 条数据路径，每条逐字节校验 128 KiB | 供应商节点的认证策略、出口路由和到 Google 的连通性 |
| HTTPS 代理的自定义 CA | 上述 3 种后端均校验私有 CA；实际 google-genai 客户端经代理取得模拟模型结果；不信任该 CA 时拒绝连接 | 生产证书链和证书轮换 |
| TURN / TURNS | 实际 Pion TURN 服务器、认证和 TLS、并发数据转发；匿名 TURN 测试服务器的数据往返；完整 Go `-race` 通过 | 供应商是否支持 RFC 6062 TCP allocation、出口限制及临时凭证有效期 |
| TURN / TURNS 域名节点和域名目标 | 新增本地 DNS 服务，把进程默认解析器替换为必定失败的解析器，确认 bridge 明确使用 systemResolver 解析服务器与目标并完成数据往返 | DNS 在转换进程本机执行，不宣称经隧道远程解析 |
| SOCKS4 / SOCKS4A | 实际 sing-box 1.14.3 校验服务端和应用生成的 outbound | 本环境未执行真实 sing-box 转发；4 / 4A 的 DNS 差异仍需完整联调 |
| VLESS、VMess、Trojan | 各自 TCP / WS / HTTPUpgrade / gRPC / HTTP / QUIC 共 18 组，实际引擎校验服务端和应用生成的 outbound | 本环境未执行真实 sing-box 握手或转发；本矩阵不覆盖 REALITY、Vision、全部指纹与全部参数组合 |
| Shadowsocks | SIP002 Base64 userinfo、旧式整体 Base64 两组实际引擎配置校验；另有明文百分号编码凭据回归 | 本环境未执行真实 sing-box 加解密转发；未覆盖所有加密方法和 AEAD 2022 多用户组合 |
| Hysteria2 / HY2、TUIC | 实际引擎配置校验；HY2 验证 user:pass 和缺省端口转换 | QUIC / UDP 网络转发、混淆及供应商互操作未执行 |
| SSTP | 本轮 Go 测试验证未受信任 TLS 拒绝、端口冲突前不发起 VPN 握手 | 未提供真实 SSTP 测试配置，完整认证、PPP 协商及 VPN 内转发测试跳过 |

sing-box 矩阵合计 **24 组转换协议组合 + 4 组原生协议服务端配置 = 28 组**。
实际固定版本的 `sing-box check` 全部通过。启动引擎时，当前环境返回：

```text
initialize network: create netlink socket: operation not permitted
```

这是运行环境阻止网络初始化；不能把配置检查结果写成端到端转发成功，也不能据此认定
远端协议有故障。测试保留完整转发路径，可在允许该网络初始化的部署主机运行。
本轮不提升权限或修改主机网络策略。

`docs/proxy-turn-sstp.md` 中已有的 SoftEther 联调记录属于项目历史验证；本轮没有
重新取得该服务端配置，不能将历史记录计为这次执行的 SSTP 成功测试。

## 已复现并修复的转换缺陷

| 问题 | 原行为 | 修复 |
| --- | --- | --- |
| Base64 凭据二次 URL 解码 | SS、原生 HTTP / SOCKS 的 Base64 密码含字面量 `%2F` / `%23` 时被改成 `/` / `#`，认证失败 | 区分 Base64 原始凭据与 URL 百分号编码字段；只对后者解码一次，用户名同样保留 |
| HY2 user:pass 截断 | `hy2://alice:password@host` 仅保留 `alice`，丢失密码部分 | 合并解码后的两部分，等价保留整体编码的 `alice:password` |
| HY2 缺省端口不兼容 | 合法的省略端口链接被拒绝 | 缺省使用 URI 规范规定的 443 |
| HTTPS 代理 CA 未生效 | HTTPX 的客户端 `verify` 设置只用于目标 TLS，私有 CA 的 HTTPS 代理仍报证书错误 | 用显式 `httpx.Proxy(..., ssl_context=...)` 配置代理 TLS，应用所有 HTTPX 代理入口；默认仍严格校验证书 |
| HTTPUpgrade / QUIC 未转换 | 没有映射传输对象，静默改为 TCP | 增加对应 transport 映射，保留 HTTPUpgrade host / path |
| 不支持的传输被忽略 | KCP、XHTTP、SplitHTTP 或未知值被当普通 TCP | 保存校验和运行解析都明确报不支持，不产生错误协议连接 |
| TCP 伪装头被丢弃 | TCP + HTTP header 伪装被当作普通 TCP，VMess 的 `type` 也未参与转换 | 明确拒绝当前无法等价转换的 TCP 伪装要求 |
| 加密要求被静默丢弃 | 未知 TLS security、VLESS 额外 encryption 或没有 TLS 的 flow 可能产生不同语义的连接 | 未支持的要求直接报错；flow 缺少 TLS / REALITY 不再静默剥离 |
| QUIC 加密参数丢失 | 分享链接的额外加密、key、headerType 可能被忽略 | 检查并拒绝不能由 sing-box QUIC 等价表达的参数；VMess 的 host / path / type 按 QUIC 语义映射后检查 |
| HY2 TLS 校验要求被忽略 | pinSHA256 / ECH 未转换仍继续连接 | 暂不实现时明确拒绝，避免假装执行了指定的证书约束 |
| SS 错误消息泄露凭据 | 非法端口等错误拼接原始 URI 前缀 | 改成不包含原始链接或凭据的端点错误；空主机同样拒绝 |

HTTPS 代理信任修复覆盖 SDK、Cookie 的 HTTPX 分支、Key 探测、模型加载、图片获取、
Gemini 原生透传和控制台代理测试。curl 分支保留既有 CA 配置和连接池复用策略。

## 明确的兼容性限制

- sing-box 不支持 mKCP；XHTTP / SplitHTTP 当前没有转换实现，不自动映射成其他传输。
- TCP HTTP 伪装不等同于所有 TLS 场景下的 sing-box HTTP transport，因此暂时拒绝，
  不做可能改变握手协议的猜测性转换。
- VLESS 本转换器仅支持 `encryption=none`；额外加密需要独立实现及互操作测试。
- sing-box 的 V2Ray QUIC transport 不支持额外的 QUIC 加密层；本项目同步拒绝此类参数。
- Shadowsocks SIP003 插件仍不支持，保留明确拒绝；并非识别 `ss://` 就支持所有插件节点。
- HY2 当前不转换端口跳跃列表、pinSHA256 或 ECH。省略端口已支持；多端口列表会被拒绝。
- TURN 的 TCP 控制连接不代表服务器支持 TCP 目标中继；需要 RFC 6062，普通 WebRTC
  UDP allocation 或单纯 STUN 服务不能代替 TCP 代理。
- bridge 只提供 SOCKS5 CONNECT，目标域名使用转换进程本机 DNS；没有 BIND、
  UDP ASSOCIATE 或系统 VPN 接管。
- SSTP 完整成功联调、REALITY / Vision 与实际供应商节点仍是后续验证重点。

## 回归与复现

最终结果：项目检查 **50/50 组通过**（显式启用引擎配置矩阵），应用冒烟 **25/25**，
控制台 DOM **14 组通过**，Go 全套 `-race` 通过。sing-box 的配置校验与实际转发
边界按上表记录；未启用的 SSTP 完整联调不计作成功。

新增文件：

- `tests/test_proxy_conversion_integrity.py`：12 项，覆盖凭据完整性、不支持参数拒绝和错误脱敏。
- `tests/test_native_proxy_relays.py`：4 项，覆盖上述 24 条实际数据路径、google-genai
  集成、证书拒绝，以及 CONNECT / SOCKS 隧道内不泄露 Proxy-Authorization。
- `tests/test_proxy_protocol_e2e.py`：2 个矩阵测试入口，缺少引擎时明确跳过；可选择
  仅配置校验或真正运行服务端、应用转换器和两种后端。
- `proxybridge/dns_test.go`：实际 DNS + TURN / TURNS 域名启动和目标转发回归。

在仓库根目录执行普通离线检查：

```bash
python scripts/check_project.py
python scripts/smoke_final.py
npm run test:console
```

启用固定版本 sing-box 的矩阵配置校验：

```bash
PROXY_E2E_SINGBOX_BIN=/absolute/path/sing-box \
PROXY_E2E_CHECK_ONLY=1 python tests/test_proxy_protocol_e2e.py
```

在允许引擎网络初始化的主机上执行实际 loopback 转发：

```bash
PROXY_E2E_SINGBOX_BIN=/absolute/path/sing-box \
python tests/test_proxy_protocol_e2e.py
```

完整 Go 并发与数据转发检查：

```bash
cd proxybridge
go test -race -p=2 -count=1 -timeout=90s ./...
```

本次使用项目固定的 sing-box **1.14.3** 和 Go **1.27.3**，未变更版本或依赖。
sing-box 下载包 SHA256 与 Dockerfile 固定值相同；Go 下载包与官方发布清单校验一致。
本轮没有真实 Google Key / Cookie 或供应商代理节点，因此未把本地测试记作 Google
在线生成成功。完整 SSTP 可选联调命令见 `docs/proxy-turn-sstp.md`。

此前确认的设置继续保留：跨通道兜底默认关闭，Cookie 自动刷新默认开启，支持手动刷新。

## 协议依据

- [Shadowsocks SIP002](https://shadowsocks.org/doc/sip002.html)：Base64 与明文百分号编码 userinfo。
- [Hysteria2 URI](https://v2.hysteria.network/docs/developers/URI-Scheme/)：userpass、缺省端口和 TLS 参数。
- [sing-box V2Ray transport](https://sing-box.sagernet.org/configuration/shared/v2ray-transport/)：支持类型与 QUIC 限制。
- [sing-box VLESS](https://sing-box.sagernet.org/configuration/outbound/vless/)：支持的 outbound 字段与 flow。
- [RFC 6062](https://www.rfc-editor.org/rfc/rfc6062.html)：TURN TCP allocation、Connect 与 ConnectionBind。
