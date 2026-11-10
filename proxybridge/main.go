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
//
// ⚠️ Cross-process contract (kept in sync with the Python side): every code
// reachable during startup (before the ready file is written) must have a
// matching entry in the `known` map of app/proxy_resolver.py (_launch).
// Per-connection codes below (turn_*, target_*, socks_*) are diagnostics
// only and need no Python mapping. Renaming a startup code without updating
// Python silently degrades it to the generic "startup timeout" message.
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
