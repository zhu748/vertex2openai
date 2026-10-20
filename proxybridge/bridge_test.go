package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pion/logging"
	"github.com/pion/transport/v5/stdnet"
	"github.com/pion/turn/v5"
)

func testCertificate(t *testing.T) (tls.Certificate, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "fixture.test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		DNSNames: []string{"fixture.test"}, IsCA: true, BasicConstraintsValid: true,
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	cert, err := tls.X509KeyPair(certPEM, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, certPEM, 0600); err != nil {
		t.Fatal(err)
	}
	return cert, path
}

type fragmentedListener struct{ net.Listener }
type fragmentedConn struct {
	net.Conn
	first bool
}

func (l fragmentedListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &fragmentedConn{Conn: c, first: true}, nil
}
func (c *fragmentedConn) Write(p []byte) (int, error) {
	if !c.first {
		return c.Conn.Write(p)
	}
	c.first = false
	total := 0
	for len(p) > 0 {
		n, err := c.Conn.Write(p[:min(3, len(p))])
		total += n
		p = p[n:]
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func turnFixture(t *testing.T, secure bool) config {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg := config{Scheme: "turn", Server: "127.0.0.1", Port: ln.Addr().(*net.TCPAddr).Port,
		Username: "fixture-user", Password: "fixture-password", ServerName: "fixture.test"}
	if secure {
		cert, ca := testCertificate(t)
		cfg.Scheme = "turns"
		cfg.CAFile = ca
		ln = tls.NewListener(ln, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	}
	logger := logging.NewDefaultLoggerFactory()
	logger.DefaultLogLevel = logging.LogLevelDisabled
	server, err := turn.NewServer(turn.ServerConfig{Realm: "fixture", LoggerFactory: logger,
		AuthHandler: func(r *turn.RequestAttributes) (string, []byte, bool) {
			return r.Username, turn.GenerateAuthKey(cfg.Username, "fixture", cfg.Password), r.Username == cfg.Username
		}, ListenerConfigs: []turn.ListenerConfig{{Listener: fragmentedListener{ln},
			RelayAddressGenerator: &turn.RelayAddressGeneratorStatic{RelayAddress: net.ParseIP("127.0.0.1"), Address: "127.0.0.1", Net: &stdnet.Net{}}}}})
	if err != nil {
		ln.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	return cfg
}

func echoFixture(t *testing.T) *net.TCPAddr {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { defer c.Close(); io.Copy(c, c) }()
		}
	}()
	return ln.Addr().(*net.TCPAddr)
}

func TestTURNAndTURNSConcurrentTCPRelay(t *testing.T) {
	for _, secure := range []bool{false, true} {
		name := "TURN"
		if secure {
			name = "TURNS"
		}
		t.Run(name, func(t *testing.T) {
			cfg := turnFixture(t, secure)
			peer := echoFixture(t)
			tlsConfig, err := cfg.tlsConfig()
			if err != nil {
				t.Fatal(err)
			}
			var workers sync.WaitGroup
			for i := 0; i < 4; i++ {
				workers.Add(1)
				go func() {
					defer workers.Done()
					ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
					defer cancel()
					conn, err := cfg.dialTURN(ctx, peer, tlsConfig)
					if err != nil {
						t.Error(err)
						return
					}
					defer conn.Close()
					conn.SetDeadline(time.Now().Add(3 * time.Second))
					payload := bytes.Repeat([]byte("streamed relay payload"), 4096)
					written := make(chan error, 1)
					go func() { _, err := conn.Write(payload); written <- err }()
					received := make([]byte, len(payload))
					_, err = io.ReadFull(conn, received)
					if err != nil {
						t.Error(err)
						return
					}
					if err := <-written; err != nil {
						t.Error(err)
					}
					if !bytes.Equal(payload, received) {
						t.Error("payload changed")
					}
				}()
			}
			workers.Wait()
		})
	}
}

func TestTURNRejectsWrongCredentialsAndUntrustedTLS(t *testing.T) {
	peer := echoFixture(t)
	for _, secure := range []bool{false, true} {
		cfg := turnFixture(t, secure)
		if secure {
			cfg.CAFile = ""
		} else {
			cfg.Password = "wrong-password"
		}
		tlsConfig, err := cfg.tlsConfig()
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		conn, err := cfg.dialTURN(ctx, peer, tlsConfig)
		cancel()
		if conn != nil {
			conn.Close()
		}
		if err == nil {
			t.Fatal("rejected configuration connected")
		}
		if bytes.Contains([]byte(err.Error()), []byte(cfg.Password)) {
			t.Fatal("credential in error")
		}
	}
}

func TestTURNCancellationStopsSilentServer(t *testing.T) {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		c, err := ln.Accept()
		if err == nil {
			defer c.Close()
			io.Copy(io.Discard, c)
		}
	}()
	cfg := config{Server: "127.0.0.1", Port: ln.Addr().(*net.TCPAddr).Port, Username: "user", Password: "password"}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	conn, err := cfg.dialTURN(ctx, &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 443}, nil)
	if conn != nil {
		conn.Close()
	}
	if err == nil || time.Since(started) > time.Second {
		t.Fatalf("timeout was not bounded: %v", err)
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("control connection leaked")
	}
}

func TestBindingReadPreservesFollowingApplicationBytes(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	go func() {
		for _, p := range [][]byte{[]byte("frag"), []byte("mented"), []byte("application")} {
			b.Write(p)
		}
	}()
	adapter := turnDataConn{stream: a, binding: true}
	first := make([]byte, 10)
	if n, err := adapter.Read(first); err != nil || n != 10 || string(first) != "fragmented" {
		t.Fatalf("binding read: %q %v", first, err)
	}
	adapter.binding = false
	last := make([]byte, 11)
	if _, err := io.ReadFull(&adapter, last); err != nil || string(last) != "application" {
		t.Fatalf("application read: %q %v", last, err)
	}
}

func TestSOCKSOnlyConnectsAfterAValidRequest(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	called := false
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer a.Close()
		handleSOCKS(t.Context(), a, func(context.Context, string) (net.Conn, error) { called = true; return nil, errors.New("unexpected") })
	}()
	b.Write([]byte{5, 1, 2})
	reply := make([]byte, 2)
	io.ReadFull(b, reply)
	if !bytes.Equal(reply, []byte{5, 255}) {
		t.Fatalf("auth reply: %v", reply)
	}
	<-done
	if called {
		t.Fatal("dialed before a valid CONNECT")
	}
}

func TestSOCKSRelaysBytesAndStopsOnShutdown(t *testing.T) {
	peer := echoFixture(t)
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- serveSOCKS(ctx, ln, func(ctx context.Context, target string) (net.Conn, error) {
			if target != peer.String() {
				return nil, errors.New("unexpected_target")
			}
			return (&net.Dialer{}).DialContext(ctx, "tcp", target)
		})
	}()
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(time.Second))
	c.Write([]byte{5, 1, 0})
	reply := make([]byte, 2)
	io.ReadFull(c, reply)
	port := peer.Port
	c.Write([]byte{5, 1, 0, 1, 127, 0, 0, 1, byte(port >> 8), byte(port)})
	reply = make([]byte, 10)
	io.ReadFull(c, reply)
	if reply[1] != 0 {
		t.Fatalf("CONNECT: %v", reply)
	}
	c.Write([]byte("stream"))
	reply = make([]byte, 6)
	io.ReadFull(c, reply)
	if string(reply) != "stream" {
		t.Fatalf("relay: %q", reply)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown retained a connection")
	}
}

func TestSSTPRejectsUntrustedServerBeforePPP(t *testing.T) {
	cert, _ := testCertificate(t)
	ln, err := tls.Listen("tcp4", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err == nil {
			defer c.Close()
			io.Copy(io.Discard, c)
		}
	}()
	host, portText, _ := net.SplitHostPort(ln.Addr().String())
	port, _ := strconv.Atoi(portText)
	cfg := config{Scheme: "sstp", Server: host, Port: port, ServerName: "fixture.test", Username: "user", Password: "password", MTU: 1400}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	dial, closeTunnel, err := cfg.startSSTP(ctx, cancel)
	if closeTunnel != nil {
		closeTunnel()
	}
	if err == nil || dial != nil {
		t.Fatal("untrusted SSTP server was accepted")
	}
}

type failedHalfClose struct{ net.Conn }

func (c failedHalfClose) CloseWrite() error { return io.ErrClosedPipe }

func TestRelayHalfCloseErrorStopsBothDirections(t *testing.T) {
	left, leftPeer := net.Pipe()
	right, rightPeer := net.Pipe()
	defer left.Close()
	defer leftPeer.Close()
	defer right.Close()
	defer rightPeer.Close()
	done := make(chan struct{})
	go func() {
		relay(left, failedHalfClose{right})
		close(done)
	}()
	leftPeer.Close() // EOF in the upload direction, followed by a failed FIN.
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("failed half-close left the opposite copy goroutine blocked")
	}
}

func TestSSTPPortCollisionDoesNotStartVPNHandshake(t *testing.T) {
	occupied, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	cert, _ := testCertificate(t)
	vpn, err := tls.Listen("tcp4", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatal(err)
	}
	defer vpn.Close()
	connected := make(chan struct{}, 1)
	go func() {
		conn, err := vpn.Accept()
		if err == nil {
			defer conn.Close()
			connected <- struct{}{}
			conn.(*tls.Conn).Handshake()
		}
	}()
	cfg := config{Scheme: "sstp", Server: "127.0.0.1", Port: vpn.Addr().(*net.TCPAddr).Port,
		ListenPort: occupied.Addr().(*net.TCPAddr).Port, Username: "fixture", Password: "password"}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := run(path); err == nil || err.Error() != "listen_failed" {
		t.Fatalf("port collision reached the VPN handshake: %v", err)
	}
	select {
	case <-connected:
		t.Fatal("contacted VPN despite occupied SOCKS port")
	default:
	}
}
