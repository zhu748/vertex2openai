package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/pion/logging"
	"github.com/pion/transport/v5/stdnet"
	"github.com/pion/turn/v5"
)

// minDeadline returns the earlier of the caller's context deadline and
// now+floor. Binding exchanges and probes use it so a context without a
// deadline cannot block forever on a silent server (2026-11 review
// convergence: this "take-the-earlier" glue was previously duplicated in
// withTimeout / dialTURN / dialTURNAnonymous).
func minDeadline(ctx context.Context, floor time.Duration) time.Time {
	deadline := time.Now().Add(floor)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	return deadline
}

func (c config) tlsConfig() (*tls.Config, error) {
	if c.Scheme == "turn" {
		return nil, nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: c.ServerName, InsecureSkipVerify: c.Insecure}
	if c.CAFile != "" {
		pem, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, errors.New("ca_read_failed")
		}
		cfg.RootCAs, err = x509.SystemCertPool()
		if err != nil {
			cfg.RootCAs = x509.NewCertPool()
		}
		if !cfg.RootCAs.AppendCertsFromPEM(pem) {
			return nil, errors.New("ca_invalid_pem")
		}
	}
	return cfg, nil
}

func dialServer(ctx context.Context, address string, cfg *tls.Config) (net.Conn, *net.TCPConn, error) {
	// dialer() attaches systemResolver: the server address may be a
	// hostname, and net.DefaultResolver is hijacked by yuhaiin (dns.go).
	raw, err := dialer().DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, nil, errors.New("server_connect_failed")
	}
	// ENG-P3-2（2026-11 审查）：comma-ok 断言——若未来 dialer 返回包装
	// conn（代理链 / 测试注入），旧的首接断言会让辅助进程直接 panic；
	// 改为受控错误码，启动路径经 Python known 映射展示。
	tcp, ok := raw.(*net.TCPConn)
	if !ok {
		raw.Close()
		return nil, nil, errors.New("server_transport_unsupported")
	}
	if cfg == nil {
		return raw, tcp, nil
	}
	conn := tls.Client(raw, cfg)
	if err := conn.HandshakeContext(ctx); err != nil {
		raw.Close()
		return nil, nil, errors.New("server_tls_failed_check_ca_sni")
	}
	return conn, tcp, nil
}

func (c config) dialTURN(ctx context.Context, peer *net.TCPAddr, tlsConfig *tls.Config) (net.Conn, error) {
	control, _, err := dialServer(ctx, net.JoinHostPort(c.Server, strconv.Itoa(c.Port)), tlsConfig)
	if err != nil {
		return nil, err
	}
	logger := logging.NewDefaultLoggerFactory()
	logger.DefaultLogLevel = logging.LogLevelDisabled
	family := turn.RequestedAddressFamilyIPv4
	if peer.IP.To4() == nil {
		family = turn.RequestedAddressFamilyIPv6
	}
	client, err := turn.NewClient(&turn.ClientConfig{
		TURNServerAddr: control.RemoteAddr().String(), Conn: turn.NewSTUNConn(control),
		Username: c.Username, Password: c.Password, Realm: c.Realm,
		RequestedAddressFamily: family, LoggerFactory: logger,
		// Bound carrier sockets need no interface inventory. Avoid netlink
		// enumeration, which is unavailable in some unprivileged sandboxes.
		Net: &stdnet.Net{},
	})
	if err != nil {
		control.Close()
		return nil, errors.New("turn_client_failed")
	}
	success := false
	defer func() {
		if !success {
			client.Close()
			control.Close()
		}
	}()
	stop := context.AfterFunc(ctx, func() { client.Close(); control.Close() })
	defer stop()
	if err := client.Listen(); err != nil {
		return nil, errors.New("turn_control_failed")
	}
	allocation, err := client.AllocateTCPWithContext(ctx)
	if err != nil {
		return nil, errors.New("turn_allocate_failed_check_credentials_and_rfc6062")
	}
	defer func() {
		if !success {
			allocation.Close()
		}
	}()
	// RFC 6062 requires the same server transport address for both connections.
	data, tcp, err := dialServer(ctx, control.RemoteAddr().String(), tlsConfig)
	if err != nil {
		return nil, err
	}
	stopData := context.AfterFunc(ctx, func() { data.Close() })
	defer stopData()
	// 绑定期 deadline 下限（2026-11 G-3，对齐匿名路径）：ctx 无 Deadline 时，
	// 静默服务器会让 DialTCPWithConn 无限挂起；给 10 秒下限。
	data.SetDeadline(minDeadline(ctx, 10*time.Second))
	adapter := &turnDataConn{TCPConn: tcp, stream: data, binding: true}
	bound, err := allocation.DialTCPWithConn(adapter, "tcp", peer)
	if err != nil || ctx.Err() != nil {
		data.Close()
		// 具体错误保留在受控格式里（2026-11 G-3）：诊断只打印 code 前缀，
		// cause 经 Unwrap 供调用方排障，不再被统一错误码吞掉。
		return nil, protocolError{"turn_connect_failed_check_peer_policy", err}
	}
	adapter.binding = false
	data.SetDeadline(time.Time{})
	// Cancel/success race: if the dial context fired while DialTCPWithConn
	// was completing, the AfterFunc callbacks already closed control and
	// data (stop reports false). Fail instead of handing the caller a dead
	// connection that the cleanup path would close a second time.
	if ctx.Err() != nil || !stop() || !stopData() {
		success = false
		data.Close()
		return nil, errors.New("turn_dial_cancelled")
	}
	success = true
	return &managedConn{Conn: bound, cleanup: func() {
		allocation.Close()
		client.Close()
		control.Close()
	}}, nil
}

// Pion's TCP allocation expects TCP socket methods, including for a TLS data
// connection. During ConnectionBind, ReadFull also preserves fragmented STUN
// replies without consuming following application bytes.
type turnDataConn struct {
	*net.TCPConn
	stream  net.Conn
	binding bool
}

func (c *turnDataConn) Read(p []byte) (int, error) {
	if c.binding {
		return io.ReadFull(c.stream, p)
	}
	return c.stream.Read(p)
}
func (c *turnDataConn) Write(p []byte) (int, error)        { return c.stream.Write(p) }
func (c *turnDataConn) Close() error                       { return c.stream.Close() }
func (c *turnDataConn) SetDeadline(t time.Time) error      { return c.stream.SetDeadline(t) }
func (c *turnDataConn) SetReadDeadline(t time.Time) error  { return c.stream.SetReadDeadline(t) }
func (c *turnDataConn) SetWriteDeadline(t time.Time) error { return c.stream.SetWriteDeadline(t) }
func (c *turnDataConn) ReadFrom(r io.Reader) (int64, error) {
	return io.Copy(struct{ io.Writer }{c.stream}, r)
}
func (c *turnDataConn) CloseWrite() error {
	if tlsConn, ok := c.stream.(*tls.Conn); ok {
		return tlsConn.CloseWrite()
	}
	return c.TCPConn.CloseWrite()
}

type managedConn struct {
	net.Conn
	once    sync.Once
	cleanup func()
}

func (c *managedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.cleanup)
	return err
}
func (c *managedConn) CloseWrite() error {
	if conn, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return conn.CloseWrite()
	}
	return c.Close()
}
