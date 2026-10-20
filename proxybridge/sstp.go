package main

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Asutorufa/yuhaiin/pkg/net/proxy/sstp"
	"github.com/Asutorufa/yuhaiin/pkg/net/proxy/vpnkit/dataplane"
	client "github.com/Asutorufa/yuhaiin/pkg/net/proxy/vpnkit/session"
	"github.com/Asutorufa/yuhaiin/pkg/net/proxy/wireguard"
)

func (c config) startSSTP(ctx context.Context, cancel context.CancelFunc) (dialFunc, func(), error) {
	// Go's Unix system roots honor SSL_CERT_FILE. Set it before the first TLS
	// operation in this dedicated process; other nodes have separate processes.
	if c.CAFile != "" {
		if runtime.GOOS != "linux" {
			return nil, nil, errors.New("sstp_custom_ca_requires_linux")
		}
		pem, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, nil, errors.New("ca_read_failed")
		}
		if !x509.NewCertPool().AppendCertsFromPEM(pem) {
			return nil, nil, errors.New("ca_invalid_pem")
		}
		os.Setenv("SSL_CERT_FILE", c.CAFile)
	}
	packets := newSSTPPackets()
	name := "v2o-sstp-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	release, err := dataplane.RegisterVirtualTUN(name, packets)
	if err != nil {
		return nil, nil, errors.New("sstp_packet_bridge_failed")
	}
	defer release()
	handshakeCtx, stopHandshake := context.WithTimeout(ctx, 30*time.Second)
	defer stopHandshake()
	var carrier net.Conn
	// Cancel the full handshake on process shutdown, including library calls
	// that do not themselves attach the caller's context to every read.
	carrierDial := func(dialCtx context.Context, _, _ string) (net.Conn, error) {
		// dialer() attaches systemResolver: net.DefaultResolver is
		// hijacked by yuhaiin's netapi package (see dns.go), and the
		// carrier address may be a hostname.
		raw, err := dialer().DialContext(dialCtx, "tcp", net.JoinHostPort(c.Server, strconv.Itoa(c.Port)))
		if err != nil {
			return nil, err
		}
		// The timeout covers DNS/TCP/TLS/HTTP/PPP together, rather than giving
		// HTTP a fresh 30 seconds after a slow TCP connection. The library's
		// HTTP handshake does not itself watch the context.
		deadline, _ := handshakeCtx.Deadline()
		raw.SetDeadline(deadline)
		stop := context.AfterFunc(ctx, func() { raw.Close() })
		carrier = &managedConn{Conn: raw, cleanup: func() { stop() }}
		return carrier, nil
	}
	session, result, err := sstp.Dial(handshakeCtx, sstp.Config{
		Server: c.ServerName, Port: c.Port, Username: c.Username, Password: c.Password,
		TUNName: name, SkipVerify: c.Insecure, DialContext: carrierDial,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		if carrier != nil {
			carrier.Close()
		}
		packets.Close()
		// Auth refusal must win over the deadline check: a server that
		// rejects credentials right at the 30s edge would otherwise be
		// misreported as a timeout and misdirect debugging.
		if errors.Is(err, client.ErrAuth) {
			return nil, nil, errors.New("sstp_auth_failed_requires_mschapv2")
		}
		var timeout net.Error
		if handshakeCtx.Err() != nil || (errors.As(err, &timeout) && timeout.Timeout()) {
			return nil, nil, errors.New("sstp_handshake_timeout")
		}
		return nil, nil, errors.New("sstp_handshake_failed_check_tls_credentials_and_mschapv2")
	}
	if handshakeCtx.Err() != nil {
		session.Close()
		packets.Close()
		return nil, nil, errors.New("sstp_handshake_timeout")
	}
	carrier.SetDeadline(time.Time{})
	ip, valid := netip.AddrFromSlice(result.AssignedIP)
	if !valid || !ip.Unmap().Is4() {
		session.Close()
		packets.Close()
		return nil, nil, errors.New("sstp_ipv4_address_missing")
	}
	mtu := c.MTU
	if mtu == 0 || mtu > 1400 {
		mtu = 1400
	}
	network, err := wireguard.CreateLightweightNetTUN([]netip.Prefix{netip.PrefixFrom(ip.Unmap(), 32)}, mtu)
	if err != nil {
		session.Close()
		packets.Close()
		return nil, nil, errors.New("sstp_network_stack_failed")
	}
	packets.attach(network)
	go func() {
		session.Wait(ctx)
		cancel() // Fail closed: the helper exits when the VPN session ends.
	}()
	dial := func(ctx context.Context, target string) (net.Conn, error) {
		conn, err := dialResolved(ctx, target, true, func(ctx context.Context, peer *net.TCPAddr) (net.Conn, error) {
			return network.DialContextTCP(ctx, peer)
		})
		if err != nil {
			return nil, protocolError{"sstp_target_connect_failed", fmt.Errorf("source %s, packets sent %d / received %d: %w", result.AssignedIP, packets.sent.Load(), packets.received.Load(), err)}
		}
		return conn, nil
	}
	return dial, func() { session.Close(); packets.Close() }, nil
}

type sstpPackets struct {
	ready          chan struct{}
	closed         chan struct{}
	network        atomic.Pointer[wireguard.LightweightNetTun]
	once           sync.Once
	sent, received atomic.Uint64
}

func newSSTPPackets() *sstpPackets {
	return &sstpPackets{ready: make(chan struct{}), closed: make(chan struct{})}
}
func (p *sstpPackets) attach(n *wireguard.LightweightNetTun) {
	p.network.Store(n)
	select {
	case <-p.closed:
		n.Close()
	default:
	}
	close(p.ready)
}
func (p *sstpPackets) await() (*wireguard.LightweightNetTun, error) {
	select {
	case <-p.closed:
		return nil, io.ErrClosedPipe
	case <-p.ready:
		return p.network.Load(), nil
	}
}
func (p *sstpPackets) Read(b []byte) (int, error) {
	n, err := p.await()
	if err != nil {
		return 0, err
	}
	size, err := n.ReadPacket(b)
	if size > 0 {
		p.sent.Add(1)
	}
	return size, err
}
func (p *sstpPackets) Write(b []byte) (int, error) {
	n, err := p.await()
	if err != nil {
		return 0, err
	}
	err = n.InjectPacket(b)
	if err != nil {
		return 0, err
	}
	p.received.Add(1)
	return len(b), nil
}
func (p *sstpPackets) Close() error {
	p.once.Do(func() {
		close(p.closed)
		if n := p.network.Load(); n != nil {
			n.Close()
		}
	})
	return nil
}
