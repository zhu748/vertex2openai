# TURN、TURNS 与 SSTP 代理

项目可以直接保存 TURN / TURNS / SSTP 节点，在首次使用时通过独立的 `vertex-proxy-bridge` 进程转换为本地 SOCKS5。全局代理、Key 专属代理和 Cookie 专属代理均可使用，沿用现有的排序、故障切换和连接复用。

| 类型 | 默认端口 | 客户端实现 | 服务器要求 |
|---|---|---|---|
| TURN | 3478 | Pion TURN，TCP 控制和数据连接 | RFC 6062 TCP allocation、有效账号密码、允许访问目标地址 |
| TURNS | 5349 | Pion TURN，TLS 控制和数据连接 | 同上，并提供可校验的 TLS 证书 |
| SSTP | 443 | yuhaiin 用户态 SSTP / TCP/IP 栈 | MS-CHAPv2 认证、IPv4 地址协商和目标网络访问权限 |

SSTP 客户端不创建系统 TUN / PPP 设备，不修改宿主机路由，也不需要 `NET_ADMIN`、`privileged` 或 `/dev/ppp`。Docker 应用继续以普通用户运行。

## 在面板填写

在「通道与凭证」中打开「管理 / 添加代理」，选择 TURN、TURNS 或 SSTP，填写服务器主机名、端口、账号和密码。端口留空时使用上表默认值。账号密码含特殊字符时，字段方式会自动完成 URL 编码。

「高级连接设置」可填写证书名称（SNI）、自定义 CA 文件、TURN Realm 或 SSTP MTU。默认校验 TLS 证书，只有明确勾选「允许未受信任的服务器证书」才会跳过校验。

加入列表、应用草稿后，点击对应的「保存代理」「保存 Key 列表」或「保存 Cookie 列表」生效。配置解析与保存不连接远端服务器，转换器未安装或代理离线时仍能编辑已有配置。

## 链接与环境变量

```text
turn://username:password@turn.example.com:3478
turns://username:password@turn.example.com:5349?sni=turn.example.com
sstp://username:password@vpn.example.com:443?sni=vpn.example.com&mtu=1400
```

也支持 RFC 7065 的无斜线 TURN URI，凭据放在扩展查询参数中：

```text
turn:turn.example.com:3478?transport=tcp&username=username&password=password
turns:turn.example.com:5349?transport=tcp&username=username&password=password
```

用户名和密码中的 `@`、`:`、`#`、`&` 等保留字符须进行 URL 编码，例如密码 `p@ss:word` 应写成 `p%40ss%3Aword`。面板字段方式无需手动编码。标准 TURN URI 本身不定义凭据参数，这里的 `username` / `password` 是本项目用于传入凭据的扩展。

环境变量可混合不同类型并指定故障切换顺序：

```bash
PROXY_URLS='["turns://user:pass@turn.example.com:5349","sstp://user:pass@vpn.example.com:443","socks5://backup.example.com:1080"]'
```

`PROXY_URLS` 优先于 `PROXY_URL`，也支持每行一条或逗号分隔的完整地址。已有控制台保存记录时，以控制台为准；修改环境变量不会覆盖已保存的列表。

| 查询参数 | 适用类型 | 默认行为 |
|---|---|---|
| `transport=tcp` | TURN / TURNS | 使用 TCP，拒绝 UDP |
| `realm` | TURN / TURNS | 使用服务器返回的 Realm |
| `sni` | TURNS / SSTP | 使用服务器主机名校验证书 |
| `ca` | TURNS / SSTP | 使用系统信任；可指定进程可读的 PEM 文件路径 |
| `insecure=true` | TURNS / SSTP | 默认 `false`；设为 `true` 才跳过证书验证 |
| `mtu` | SSTP | 1400，可指定 576–9000，作为内部 MTU 上限 |

重复参数、未知参数、缺少凭据及不支持的传输方式会在保存时拒绝。自定义 CA 需要挂载到应用容器，例如将宿主机证书目录以只读方式挂载到 `/app/certificates`，然后填写 `/app/certificates/company.pem`。SSTP 自定义 CA 目前支持 Linux。

## 安装转换器

Dockerfile 通过独立 Go 构建阶段编译转换器并复制到镜像，运行阶段不需要 Go。需要使用包含此次代码的新构建；旧镜像不会因填写新链接自动获得该组件。

非 Docker 部署需安装 Go 1.27.3 或更新的兼容版本，然后在仓库根目录运行：

```bash
mkdir -p bin
cd proxybridge
go build -trimpath -o ../bin/vertex-proxy-bridge .
```

程序自动查找 `/usr/local/bin/vertex-proxy-bridge`、仓库 `bin/vertex-proxy-bridge` 或 PATH 中的同名可执行文件，也可用 `PROXY_BRIDGE_BIN` 指定其他位置。二进制源码与固定的依赖版本位于 `proxybridge/`，第三方许可保存在 `proxybridge/NOTICE.txt`，镜像同时携带该文件。

| 环境变量 | 默认值 | 作用 |
|---|---|---|
| `PROXY_BRIDGE_BIN` | 自动查找 | TURN / SSTP 转换器路径 |
| `PROXY_BRIDGE_IDLE_MINUTES` | 30 | 空闲实例回收时间；`0` 关闭空闲回收，已退出的进程仍会清理 |

每个节点独立监听 `127.0.0.1` 动态端口，同一节点复用现有进程。正在服务请求的实例不会因空闲计时被回收；进程退出后，下次取用会重新启动。SSTP 会话断开时转换器停止服务，不会把流量改走普通网络。已配置的代理全部失败后，项目不会额外增加直连出口；显式配置的 `direct` 条目仍按顺序执行。

节点凭据通过权限 `0600` 的临时配置文件传给转换器，不出现在命令行参数中；读取配置并完成启动后删除临时文件。诊断日志位于 `STATE_DIR/proxy-bridge/`，仅记录受控错误代码，不输出协议凭据。

## 兼容性边界

- **TURN TCP 与 TCP 中继不同**：`transport=tcp` 只说明客户端到服务器使用 TCP；服务器还必须支持 RFC 6062，才能直接连接 HTTPS 目标。仅有 WebRTC UDP allocation 的服务器不可用于这种接入。
- **SSTP 认证**：本组件支持 MS-CHAPv2；PAP-only、EAP、SSO / MFA 和 IPv6CP 不在当前实现范围内。SSTP 与 SoftEther 原生 SSL-VPN 是不同协议，服务器必须启用 SSTP 兼容模式。
- **域名解析**：目标域名由转换进程使用本机 DNS 解析。当前没有通过 TURN 或 SSTP 隧道远程查询 DNS；不会把 SOCKS5H 标注成这两种协议的能力。
- **TCP 请求**：本地代理只支持 SOCKS5 CONNECT，适用于项目的 HTTP / HTTPS 请求；不提供 BIND、UDP ASSOCIATE 或全局 VPN 接管。
- **凭据续期**：支持服务器提供的用户名密码，包括尚未过期的 TURN REST 临时凭据；不会自动调用供应商接口刷新过期凭据。

## 验证

Python 离线回归覆盖 URI / 高级参数、面板接口、配置文件权限、缺少二进制时的明确错误、端口复用、在用实例保留及失败清理：

```bash
python scripts/check_project.py
```

Go 测试会在 loopback 上启动实际 Pion TURN 服务器，验证 TURN / TURNS、分片响应、并发、证书验证、错误凭据、取消与 SOCKS5 转发：

```bash
cd proxybridge
go test -race -count=1 -timeout=90s ./...
```

真实 SSTP 服务联调是可选测试。`SSTP_TEST_CONFIG` 指向使用转换器 JSON 格式的私密配置文件；`SSTP_TEST_TARGET` 指向 VPN 可达的 TCP echo 服务。若填 `peer`，测试会额外建立一条 SSTP 会话，在 VPN 内创建 echo 服务，要求服务器允许客户端互访；可用 `SSTP_TEST_PEER_USER` 指定第二个账号（密码与测试配置一致）。

```bash
SSTP_TEST_CONFIG=/private/sstp-test.json SSTP_TEST_TARGET=peer \
SSTP_TEST_LONG_STREAM=1 go test -race -run '^TestSSTPInteroperability$' -v -timeout=90s .
```

额外设置 `SSTP_TEST_PYTHON` 为已安装项目依赖的 Python 路径，可同时验证应用管理器拉起实际转换进程、SOCKS5 转发、端口复用、错误密码拒绝与停机清理；运行前需先构建 `bin/vertex-proxy-bridge`，也可设置 `PROXY_BRIDGE_BIN`。

本轮在 Linux amd64 完成 Pion TURN / TURNS 与 SoftEther 4.41 SSTP 实测，SSTP 覆盖自定义 CA 严格校验、MS-CHAPv2、TCP 数据往返和超过 30 秒后的继续传输。另完成 Linux arm64 / armv7 交叉编译；交叉编译不代表已在对应硬件运行。

实际供应商兼容性还受服务器策略、证书、路由和到 Google 上游的访问权限影响，需要使用对应节点与有效上游凭据进行部署联调。

## 协议与实现依据

- [Pion TURN](https://github.com/pion/turn)
- [yuhaiin SSTP](https://github.com/yuhaiin/yuhaiin/tree/c3ac7d8eb930dcd044d40b0648c621f2b0289374/pkg/net/proxy/sstp)
- [RFC 6062](https://www.rfc-editor.org/rfc/rfc6062.html)、[RFC 7065](https://www.rfc-editor.org/rfc/rfc7065.html)
- [Microsoft MS-SSTP](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-sstp/c50ed240-56f3-4309-8e0c-1644898f0ea8)
