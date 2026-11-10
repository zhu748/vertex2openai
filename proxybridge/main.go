// proxybridge exposes one TURN or SSTP node as a loopback-only SOCKS5 proxy.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type config struct {
	Scheme     string `json:"scheme"`
	Server     string `json:"server"`
	Port       int    `json:"server_port"`
	Username   string `json:"username"`
	Password   string `json:"password"`
	ServerName string `json:"tls_server_name"`
	CAFile     string `json:"ca_file"`
	Insecure   bool   `json:"insecure"`
	Realm      string `json:"realm"`
	MTU        int    `json:"mtu"`
	ListenPort int    `json:"listen_port"`
	ReadyFile  string `json:"ready_file"`
}

type dialFunc func(context.Context, string) (net.Conn, error)

type protocolError struct {
	code  string
	cause error
}

func (e protocolError) Error() string {
	// The SSTP wrapper only carries its own diagnostics (assigned IP, packet
	// counters, dial error text); nothing peer-supplied flows in here, so the
	// cause is safe to surface. Returning the bare code discarded it.
	if e.cause == nil {
		return e.code
	}
	return e.code + ": " + e.cause.Error()
}
func (e protocolError) Unwrap() error { return e.cause }

var diagnostics atomic.Uint32

// Diagnostic messages are controlled codes: peer-provided errors can contain
// echoed credentials. Bound output even when an offline node is retried often.
// protocolError renders as "code: cause" where the cause may carry TURN reason
// phrases — keep only the code token, and collapse anything else to a single
// line defensively (newlines enable log forgery; 2026-11 G-4).
func diagnostic(message string) {
	if idx := strings.Index(message, ": "); idx > 0 {
		message = message[:idx]
	}
	if idx := strings.IndexAny(message, "\r\n\x00"); idx >= 0 {
		message = message[:idx]
	}
	if diagnostics.Add(1) <= 128 {
		fmt.Fprintln(os.Stderr, message)
	}
}

func main() {
	path := flag.String("config", "", "path to private JSON configuration")
	version := flag.Bool("version", false, "show supported protocols")
	flag.Parse()
	if *version {
		fmt.Println("vertex2openai-proxy-bridge 1 (TURN RFC6062; SSTP MS-CHAPv2)")
		return
	}
	// Dependencies use slog too; protocol diagnostics must never print secrets.
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := run(*path); err != nil {
		diagnostic(err.Error())
		os.Exit(1)
	}
}

func run(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return errors.New("config_read_failed")
	}
	var cfg config
	err = json.NewDecoder(io.LimitReader(f, 65536)).Decode(&cfg)
	f.Close()
	if err != nil || cfg.Server == "" || cfg.Port < 1 || cfg.Port > 65535 ||
		cfg.ListenPort < 1 || cfg.ListenPort > 65535 {
		return errors.New("invalid_config")
	}
	// SSTP authenticates with MS-CHAPv2, so it always needs credentials.
	// TURN/TURNS may omit them on open servers that accept unauthenticated
	// allocations; those use the anonymous dialer chosen below.
	if cfg.Scheme == "sstp" && (cfg.Username == "" || cfg.Password == "") {
		return errors.New("invalid_config")
	}
	if cfg.Scheme == "sstp" && cfg.MTU != 0 && (cfg.MTU < 576 || cfg.MTU > 9000) {
		return errors.New("invalid_config")
	}
	if cfg.ServerName == "" {
		cfg.ServerName = cfg.Server
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer cancel()
	// Reserve the SOCKS port before an expensive SSTP handshake. Otherwise a
	// different process can claim it during TLS/PPP negotiation, wasting a VPN
	// session and up to 30 seconds before we discover the bind failure.
	listener, err := net.Listen("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(cfg.ListenPort)))
	if err != nil {
		return errors.New("listen_failed")
	}
	defer listener.Close()
	var dial dialFunc
	var closeTunnel func()
	switch cfg.Scheme {
	case "turn", "turns":
		tlsConfig, err := cfg.tlsConfig()
		if err != nil {
			return err
		}
		// One probe decides the client flavor: open servers that accept
		// unauthenticated allocations break the Pion client (it expects
		// a 401 challenge first), so they get the raw dialer instead.
		mode, err := detectTURNMode(ctx, cfg, tlsConfig)
		if err != nil {
			return errors.New("turn_probe_failed_check_server")
		}
		dial = func(ctx context.Context, target string) (net.Conn, error) {
			return dialResolved(ctx, target, false, func(ctx context.Context, addr *net.TCPAddr) (net.Conn, error) {
				if mode == "anonymous" {
					return cfg.dialTURNAnonymous(ctx, addr, tlsConfig)
				}
				return cfg.dialTURN(ctx, addr, tlsConfig)
			})
		}
		closeTunnel = func() {}
	case "sstp":
		dial, closeTunnel, err = cfg.startSSTP(ctx, cancel)
		if err != nil {
			return err
		}
	default:
		return errors.New("unsupported_protocol")
	}
	defer closeTunnel()
	if cfg.ReadyFile != "" {
		if err := os.WriteFile(cfg.ReadyFile, []byte("ready\n"), 0600); err != nil {
			listener.Close()
			return errors.New("ready_write_failed")
		}
		defer os.Remove(cfg.ReadyFile)
	}
	return serveSOCKS(ctx, listener, dial)
}

// Resolve locally and pass only numeric targets to protocol clients. The helper
// never uses the ordinary TCP dialer for a target connection.
func dialResolved(ctx context.Context, target string, ipv4Only bool, dial func(context.Context, *net.TCPAddr) (net.Conn, error)) (net.Conn, error) {
	host, portText, err := net.SplitHostPort(target)
	if err != nil {
		return nil, errors.New("invalid_target")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return nil, errors.New("invalid_target")
	}
	// Must not use net.DefaultResolver: yuhaiin's netapi package replaces it
	// with an uninitialized bootstrap resolver (see dns.go).
	ips, err := systemResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, errors.New("target_dns_failed")
	}
	// Prefer IPv4 for common IPv4-only relays and SSTP IPCP networks.
	var ordered []net.IPAddr
	for _, ipv4 := range []bool{true, false} {
		for _, ip := range ips {
			if (ip.IP.To4() != nil) == ipv4 && (!ipv4Only || ipv4) {
				ordered = append(ordered, ip)
			}
		}
	}
	if len(ordered) == 0 {
		if len(ips) == 0 {
			// DNS succeeded but returned no usable A/AAAA record.
			return nil, errors.New("target_dns_failed")
		}
		// DNS 本身成功，但 ipv4Only（SSTP IPCP 只承载 IPv4）把全部
		// 地址过滤掉了（如仅有 AAAA 记录的目标）——这不是解析失败
		// （2026-11 G-1：旧实现统一误报 target_dns_failed，误导排障）。
		return nil, errors.New("target_address_family_unsupported")
	}
	err = errors.New("target_dial_failed")
	for i, ip := range ordered {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// Spread the remaining dial budget across the remaining
		// candidates so one black-holed address cannot starve the
		// rest; a single candidate keeps the whole budget.
		dialCtx := ctx
		var cancelDial context.CancelFunc
		if deadline, ok := ctx.Deadline(); ok && len(ordered)-i > 1 {
			if per := time.Until(deadline) / time.Duration(len(ordered)-i); per > 0 {
				dialCtx, cancelDial = context.WithTimeout(ctx, per)
			}
		}
		var conn net.Conn
		conn, err = dial(dialCtx, &net.TCPAddr{IP: ip.IP, Zone: ip.Zone, Port: port})
		if cancelDial != nil {
			cancelDial()
		}
		if err == nil {
			return conn, nil
		}
	}
	return nil, err
}

func serveSOCKS(ctx context.Context, listener net.Listener, dial dialFunc) error {
	ctx, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	defer func() { cancel(); listener.Close(); workers.Wait() }()
	stop := context.AfterFunc(ctx, func() { listener.Close() })
	defer stop()
	slots := make(chan struct{}, 128)
	// Refusals run on their own bounded pool: answering inline in the
	// accept loop let one slow client stall the loop (and thus every
	// other incoming connection) for the full 5-second refusal deadline.
	// Beyond the pool, saturated connections are dropped without a reply.
	refusals := make(chan struct{}, 64)
	var refusers sync.WaitGroup
	defer refusers.Wait()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return errors.New("accept_failed")
		}
		select {
		case slots <- struct{}{}:
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer func() { <-slots }()
				defer conn.Close()
				stop := context.AfterFunc(ctx, func() { conn.Close() })
				defer stop()
				handleSOCKS(ctx, conn, dial)
			}()
		default:
			// Overloaded: finish enough of the SOCKS exchange to
			// reply with a real failure code instead of a bare
			// reset, so the Python side can tell proxy saturation
			// apart from a network error and fail over correctly.
			select {
			case refusals <- struct{}{}:
				refusers.Add(1)
				go func() {
					defer refusers.Done()
					defer func() { <-refusals }()
					defer conn.Close()
					refuseOverloaded(conn)
				}()
			default:
				conn.Close()
			}
		}
	}
}

func socksReply(conn net.Conn, code byte) error {
	_, err := conn.Write([]byte{5, code, 0, 1, 0, 0, 0, 0, 0, 0})
	return err
}

// refuseOverloaded answers a CONNECT with "general SOCKS server failure"
// when the bridge is at its concurrency limit. Best effort: any parse
// error just drops the connection.
func refuseOverloaded(conn net.Conn) {
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	header := make([]byte, 2)
	if _, err := io.ReadFull(conn, header); err != nil || header[0] != 5 || header[1] == 0 {
		return
	}
	methods := make([]byte, int(header[1]))
	if _, err := io.ReadFull(conn, methods); err != nil {
		return
	}
	if _, err := conn.Write([]byte{5, 0}); err != nil {
		return
	}
	request := make([]byte, 4)
	if _, err := io.ReadFull(conn, request); err != nil {
		return
	}
	socksReply(conn, 1)
	diagnostic("socks_overloaded")
}

func handleSOCKS(ctx context.Context, conn net.Conn, dial dialFunc) {
	conn.SetDeadline(time.Now().Add(30 * time.Second))
	header := make([]byte, 2)
	if _, err := io.ReadFull(conn, header); err != nil || header[0] != 5 || header[1] == 0 {
		return
	}
	methods := make([]byte, int(header[1]))
	if _, err := io.ReadFull(conn, methods); err != nil {
		return
	}
	found := false
	for _, method := range methods {
		found = found || method == 0
	}
	if !found {
		conn.Write([]byte{5, 255})
		return
	}
	if _, err := conn.Write([]byte{5, 0}); err != nil {
		return
	}
	request := make([]byte, 4)
	if _, err := io.ReadFull(conn, request); err != nil || request[0] != 5 || request[2] != 0 {
		return
	}
	if request[1] != 1 {
		socksReply(conn, 7) // CONNECT only: no BIND or UDP ASSOCIATE.
		return
	}
	var host string
	switch request[3] {
	case 1, 4:
		size := 4
		if request[3] == 4 {
			size = 16
		}
		addr := make([]byte, size)
		if _, err := io.ReadFull(conn, addr); err != nil {
			return
		}
		host = net.IP(addr).String()
	case 3:
		length := make([]byte, 1)
		if _, err := io.ReadFull(conn, length); err != nil || length[0] == 0 {
			return
		}
		addr := make([]byte, int(length[0]))
		if _, err := io.ReadFull(conn, addr); err != nil {
			return
		}
		host = string(addr)
	default:
		socksReply(conn, 8)
		return
	}
	port := make([]byte, 2)
	if _, err := io.ReadFull(conn, port); err != nil {
		return
	}
	dialCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	upstream, err := dial(dialCtx, net.JoinHostPort(host, strconv.Itoa(int(port[0])<<8|int(port[1]))))
	cancel()
	if err != nil {
		diagnostic(err.Error())
		socksReply(conn, 1)
		return
	}
	defer upstream.Close()
	stop := context.AfterFunc(ctx, func() { upstream.Close() })
	defer stop()
	if err := socksReply(conn, 0); err != nil {
		return
	}
	conn.SetDeadline(time.Time{})
	upstream.SetDeadline(time.Time{})
	relay(conn, upstream)
}

// relay 空闲回收窗口（2026-11 P2-7，测试可缩短）：
//
// 双向拷贝此前只在第一个方向「带错误」返回时才关闭两侧连接。半关闭语义下
// （对端正常 EOF、CloseWrite 成功、返回 nil），另一方向的 io.Copy 在静默
// 目标上会无限阻塞——组件文档自己就警告过「部分开放 TURN 服务器连接建立
// 后无响应」。每个挂死连接占住 1 个 goroutine、上游数据连接、控制连接以及
// 匿名路径的持续续期任务，128 个槽位被逐个耗尽后新连接全部进入拒绝路径，
// 进程无法自愈，表现为难以定位的「代理越来越慢直到不可用」。
var (
	// 半关闭后剩余方向的无流量回收窗口。
	relayHalfCloseIdle = 5 * time.Minute
	// 从未搬运过任何字节的连接回收窗口：真实客户端在 SOCKS 应答后立即
	// 发送请求，长时间零字节即死连接。
	relayFirstByteIdle = 10 * time.Minute
	// 看门狗轮询间隔。
	relayIdleCheck = 15 * time.Second
)

// countingConn 统计经手字节数：任一方向有真实流量即视为活跃。
type countingConn struct {
	net.Conn
	counter *atomic.Int64
}

func (c countingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.counter.Add(int64(n))
	}
	return n, err
}

func (c countingConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if n > 0 {
		c.counter.Add(int64(n))
	}
	return n, err
}

// CloseWrite 必须转发到底层连接（若支持）：包装会遮蔽具体的
// CloseWrite 方法，copyTo 的类型断言将改走整连接关闭分支，破坏
// 「首个方向 EOF 后另一方向仍可传输」的半关闭语义。底层不支持时
// 回退为整连接关闭，与 relay 原有的 else 分支行为一致。
func (c countingConn) CloseWrite() error {
	if closer, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return closer.CloseWrite()
	}
	c.Conn.Close()
	return nil
}

// relayIdleWatchdog 只回收「确认静默」的连接：从未有过流量
// （relayFirstByteIdle），或半关闭后长时间无新字节（relayHalfCloseIdle）。
// 两侧都打开且有过流量的静默是合法场景——非流式 4K 生图在响应就绪前
// 可静默 10 分钟以上——不做中途闲置回收，避免误杀慢生成。
func relayIdleWatchdog(a, b net.Conn, activity *atomic.Int64, started time.Time, halfClosed bool, finished <-chan struct{}) {
	ticker := time.NewTicker(relayIdleCheck)
	defer ticker.Stop()
	lastCount := activity.Load()
	lastChange := started
	for {
		select {
		case <-finished:
			return
		case now := <-ticker.C:
			cur := activity.Load()
			if cur != lastCount {
				lastCount, lastChange = cur, now
				continue
			}
			if cur == 0 && now.Sub(started) >= relayFirstByteIdle {
				a.Close()
				b.Close()
				return
			}
			if halfClosed && now.Sub(lastChange) >= relayHalfCloseIdle {
				a.Close()
				b.Close()
				return
			}
		}
	}
}

func relay(a, b net.Conn) {
	var activity atomic.Int64
	ca := countingConn{Conn: a, counter: &activity}
	cb := countingConn{Conn: b, counter: &activity}
	done := make(chan error, 2)
	copyTo := func(dst, src net.Conn) {
		_, err := io.Copy(dst, src)
		if closer, ok := dst.(interface{ CloseWrite() error }); ok {
			if closeErr := closer.CloseWrite(); err == nil {
				err = closeErr
			}
		} else {
			dst.Close()
		}
		done <- err
	}
	go copyTo(ca, cb)
	go copyTo(cb, ca)
	first := <-done
	if first != nil {
		a.Close()
		b.Close()
	}
	finished := make(chan struct{})
	defer close(finished)
	go relayIdleWatchdog(a, b, &activity, time.Now(), first == nil, finished)
	<-done
}
