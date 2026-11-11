package main

// Tests for the anonymous-allocation dialer. The stock Pion server always
// demands MESSAGE-INTEGRITY, so it cannot represent the open servers that
// reply to an unauthenticated Allocate with success; a minimal fake covers
// the wire behavior instead.

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/pion/stun/v4"
)

// readSTUNRaw reads one framed STUN message over TCP.
func readSTUNRaw(c net.Conn) (*stun.Message, error) {
	head := make([]byte, 20)
	if _, err := io.ReadFull(c, head); err != nil {
		return nil, err
	}
	body := make([]byte, binary.BigEndian.Uint16(head[2:4]))
	if _, err := io.ReadFull(c, body); err != nil {
		return nil, err
	}
	msg := &stun.Message{Raw: append(head, body...)}
	return msg, msg.Decode()
}

func writeSTUNRaw(c net.Conn, m *stun.Message) error {
	_, err := c.Write(m.Raw)
	return err
}

func successReply(request *stun.Message, additional ...stun.Setter) *stun.Message {
	setters := append([]stun.Setter{
		stun.NewTransactionIDSetter(request.TransactionID),
		stun.NewType(request.Type.Method, stun.ClassSuccessResponse),
	}, additional...)
	msg, err := stun.Build(setters...)
	if err != nil {
		panic(err)
	}
	return msg
}

// fakeAnonymousTURN / startFakeAnonymousTURN 已合并为 turn_refresh_test.go
// 的 fakeTURN / startFakeTURN（2026-11 P2 测试去重）：两者本就是同一份
// 「无鉴权 Allocate/ConnectionBind/Connect/Refresh + 拨号对端中继」骨架，
// 仅 lifetime 配置方式与失败注入开关不同，现统一为原子开关。

func errorReply(request *stun.Message, code stun.ErrorCode) *stun.Message {
	msg, err := stun.Build(
		stun.NewTransactionIDSetter(request.TransactionID),
		stun.NewType(request.Type.Method, stun.ClassErrorResponse),
		&stun.ErrorCodeAttribute{Code: code},
	)
	if err != nil {
		panic(err)
	}
	return msg
}

func TestDetectTURNModeSelectsAnonymousAndPion(t *testing.T) {
	_, openServer := startFakeTURN(t)
	openCfg := config{Scheme: "turn", Server: "127.0.0.1", Port: openServer.Port}
	mode, err := detectTURNMode(t.Context(), openCfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if mode != "anonymous" {
		t.Fatalf("open server detected as %q", mode)
	}

	authCfg := turnFixture(t, false)
	mode, err = detectTURNMode(t.Context(), authCfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if mode != "pion" {
		t.Fatalf("authenticated server detected as %q", mode)
	}
}

func TestAnonymousTURNRelayRoundTrip(t *testing.T) {
	peer := echoFixture(t)
	_, serverAddr := startFakeTURN(t)
	cfg := config{Scheme: "turn", Server: "127.0.0.1", Port: serverAddr.Port,
		Username: "", Password: ""}
	tlsConfig, err := cfg.tlsConfig()
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	for i := 0; i < 2; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			conn, err := cfg.dialTURNAnonymous(ctx, peer, tlsConfig)
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(3 * time.Second))
			payload := bytes.Repeat([]byte("anonymous relay payload"), 2048)
			written := make(chan error, 1)
			go func() { _, err := conn.Write(payload); written <- err }()
			received := make([]byte, len(payload))
			if _, err := io.ReadFull(conn, received); err != nil {
				t.Error(err)
				return
			}
			if err := <-written; err != nil {
				t.Error(err)
				return
			}
			if !bytes.Equal(payload, received) {
				t.Error("payload changed")
			}
		}()
	}
	workers.Wait()
}

func TestAnonymousTURNCancellationStopsSilentServer(t *testing.T) {
	addr, closed := startSilentServer(t)
	cfg := config{Scheme: "turn", Server: "127.0.0.1", Port: addr.Port}
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	started := time.Now()
	conn, err := cfg.dialTURNAnonymous(ctx, &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 443}, nil)
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

// Regression: the refresh-failure path used to call stop() followed by kill(),
// where each guarded the SAME closed channel with its own sync.Once — a
// guaranteed "close of closed channel" panic that killed the whole bridge
// process. The failure must now close cleanly: the connection starts
// erroring, the process survives, and repeated kills stay idempotent.
func TestAnonymousTURNRefreshFailureFailsClosed(t *testing.T) {
	peer := echoFixture(t)
	fake, serverAddr := startFakeTURN(t)
	fake.allocateLife.Store(6) // 6s → first refresh after 5s
	fake.failRefresh.Store(true)

	cfg := config{Scheme: "turn", Server: "127.0.0.1", Port: serverAddr.Port}
	tlsConfig, err := cfg.tlsConfig()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	conn, err := cfg.dialTURNAnonymous(ctx, peer, tlsConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// The refresh goroutine fails within ~5s; the kill path must close the
	// data connection instead of panicking.
	conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	buf := make([]byte, 16)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("connection stayed open after refresh failure")
	}
	// Idempotency: closing again (managedConn.Close on a failed allocation)
	// must not panic either.
	if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatalf("second close: %v", err)
	}
}
