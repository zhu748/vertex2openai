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

func (e protocolError) Error() string { return e.code }
func (e protocolError) Unwrap() error { return e.cause }

var diagnostics atomic.Uint32

// Diagnostic messages are controlled codes: peer-provided errors can contain
// echoed credentials. Bound output even when an offline node is retried often.
func diagnostic(code string) {
	if diagnostics.Add(1) <= 128 {
		fmt.Fprintln(os.Stderr, code)
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
	if err != nil || cfg.Server == "" || cfg.Username == "" || cfg.Password == "" ||
		cfg.Port < 1 || cfg.Port > 65535 || cfg.ListenPort < 1 || cfg.ListenPort > 65535 {
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
	var dial dialFunc
	var closeTunnel func()
	switch cfg.Scheme {
	case "turn", "turns":
		tlsConfig, err := cfg.tlsConfig()
		if err != nil {
			return err
		}
		dial = func(ctx context.Context, target string) (net.Conn, error) {
			return dialResolved(ctx, target, false, func(ctx context.Context, addr *net.TCPAddr) (net.Conn, error) {
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
	listener, err := net.Listen("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(cfg.ListenPort)))
	if err != nil {
		return errors.New("listen_failed")
	}
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
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
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
	err = errors.New("target_address_family_unsupported")
	for _, ip := range ordered {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		var conn net.Conn
		conn, err = dial(ctx, &net.TCPAddr{IP: ip.IP, Zone: ip.Zone, Port: port})
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
			conn.Close()
		}
	}
}

func socksReply(conn net.Conn, code byte) error {
	_, err := conn.Write([]byte{5, code, 0, 1, 0, 0, 0, 0, 0, 0})
	return err
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

func relay(a, b net.Conn) {
	done := make(chan error, 2)
	copyTo := func(dst, src net.Conn) {
		_, err := io.Copy(dst, src)
		if closer, ok := dst.(interface{ CloseWrite() error }); ok {
			closer.CloseWrite()
		} else {
			dst.Close()
		}
		done <- err
	}
	go copyTo(a, b)
	go copyTo(b, a)
	if err := <-done; err != nil {
		a.Close()
		b.Close()
	}
	<-done
}
