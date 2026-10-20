package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/Asutorufa/yuhaiin/pkg/net/proxy/sstp"
	"github.com/Asutorufa/yuhaiin/pkg/net/proxy/vpnkit/dataplane"
	"github.com/Asutorufa/yuhaiin/pkg/net/proxy/wireguard"
)

// An optional real-server test. The configuration file uses the same private
// JSON schema as the helper; the target must run a TCP echo service via the VPN.
func TestSSTPInteroperability(t *testing.T) {
	path, target := os.Getenv("SSTP_TEST_CONFIG"), os.Getenv("SSTP_TEST_TARGET")
	if path == "" || target == "" {
		t.Skip("set SSTP_TEST_CONFIG and SSTP_TEST_TARGET for real-server checks")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var cfg config
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatal(err)
	}
	if target == "peer" {
		target = sstpEchoPeer(t, cfg)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	dial, closeTunnel, err := cfg.startSSTP(ctx, cancel)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTunnel()
	dialCtx, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()
	conn, err := dial(dialCtx, target)
	if err != nil {
		t.Fatalf("%v (%v), target %s", err, errors.Unwrap(err), target)
	}
	defer conn.Close()
	payload := bytes.Repeat([]byte("sstp-stream"), 8192)
	check := func() {
		t.Helper()
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		written := make(chan error, 1)
		go func() { _, err := conn.Write(payload); written <- err }()
		got := make([]byte, len(payload))
		if _, err := io.ReadFull(conn, got); err != nil {
			t.Fatal(err)
		}
		if err := <-written; err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatal("SSTP payload changed")
		}
	}
	check()
	if python := os.Getenv("SSTP_TEST_PYTHON"); python != "" {
		root, err := filepath.Abs("..")
		if err != nil {
			t.Fatal(err)
		}
		command := exec.CommandContext(t.Context(), python, filepath.Join(root, "scripts", "verify_proxy_bridge_sstp.py"))
		command.Env = append(os.Environ(), "SSTP_TEST_TARGET="+target)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("process integration: %v\n%s", err, output)
		} else {
			t.Logf("%s", output)
		}
	}
	// A startup deadline must not later terminate a successful streaming tunnel.
	if os.Getenv("SSTP_TEST_LONG_STREAM") == "1" {
		select {
		case <-time.After(31 * time.Second):
		case <-ctx.Done():
			t.Fatal("SSTP session ended before long-stream check")
		}
		check()
	}
	closeTunnel()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("closed VPN did not terminate the helper context")
	}
}

// Two real SSTP sessions exchange TCP through the server's virtual hub, so this
// fixture also works on a host that has only loopback networking and no TUN.
func sstpEchoPeer(t *testing.T, cfg config) string {
	t.Helper()
	pipe := newSSTPPackets()
	name := "v2o-peer-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	release, err := dataplane.RegisterVirtualTUN(name, pipe)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	username := os.Getenv("SSTP_TEST_PEER_USER")
	if username == "" {
		username = cfg.Username
	}
	var carrier net.Conn
	session, result, err := sstp.Dial(ctx, sstp.Config{Server: cfg.ServerName, Port: cfg.Port,
		Username: username, Password: cfg.Password, TUNName: name, SkipVerify: cfg.Insecure,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort(cfg.Server, strconv.Itoa(cfg.Port)))
			if err == nil {
				conn.SetDeadline(time.Now().Add(10 * time.Second))
				carrier = conn
			}
			return conn, err
		}})
	if err != nil {
		pipe.Close()
		t.Fatal(err)
	}
	t.Logf("SSTP echo peer assigned address %s", result.AssignedIP)
	t.Cleanup(func() { session.Close() })
	carrier.SetDeadline(time.Time{})
	ip, ok := netip.AddrFromSlice(result.AssignedIP)
	if !ok {
		t.Fatal("peer received no IPv4 address")
	}
	network, err := wireguard.CreateLightweightNetTUN([]netip.Prefix{netip.PrefixFrom(ip.Unmap(), 32)}, 1400)
	if err != nil {
		t.Fatal(err)
	}
	pipe.attach(network)
	// Publish the peer's IPv4 source to SoftEther's virtual-hub IP table before
	// asking a second PPP session to connect to its listener.
	probeIP := net.ParseIP("192.168.30.1")
	if len(result.DNS) > 0 {
		probeIP = result.DNS[0]
	}
	udp, err := network.DialUDP(nil, &net.UDPAddr{IP: probeIP, Port: 9})
	if err != nil {
		t.Fatal(err)
	}
	udp.Write([]byte("peer-online"))
	udp.Close()
	listener, err := network.ListenTCP(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() { defer conn.Close(); io.Copy(conn, conn) }()
		}
	}()
	return net.JoinHostPort(ip.Unmap().String(), strconv.Itoa(listener.Addr().(*net.TCPAddr).Port))
}
