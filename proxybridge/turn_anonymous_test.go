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
	"sync/atomic"
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

// fakeAnonymousTURN answers Allocate/Refresh with success (no credentials)
// and relays bound data connections to the stored peer address.
type fakeAnonymousTURN struct {
	t           *testing.T
	ln          net.Listener
	mu          sync.Mutex
	cids        map[uint32]*net.TCPAddr
	next        uint32
	active      sync.WaitGroup
	failRefresh atomic.Bool
	lifetime    [4]byte
}

func startFakeAnonymousTURN(t *testing.T, target *net.TCPAddr) (*fakeAnonymousTURN, *net.TCPAddr) {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeAnonymousTURN{t: t, ln: ln, cids: map[uint32]*net.TCPAddr{}, next: 0x1234,
		lifetime: [4]byte{0, 0, 2, 88}}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			fake.active.Add(1)
			go func() { defer fake.active.Done(); defer conn.Close(); fake.serve(conn) }()
		}
	}()
	t.Cleanup(func() { ln.Close(); fake.active.Wait() })
	return fake, ln.Addr().(*net.TCPAddr)
}

func (f *fakeAnonymousTURN) serve(conn net.Conn) {
	msg, err := readSTUNRaw(conn)
	if err != nil {
		return
	}
	switch msg.Type.Method {
	case stun.MethodAllocate:
		relayed := stun.XORMappedAddress{IP: net.ParseIP("127.0.0.1"), Port: 1}
		f.mu.Lock()
		lifetime := f.lifetime
		f.mu.Unlock()
		if err := writeSTUNRaw(conn, successReply(msg, stun.RawAttribute{
			Type: stun.AttrLifetime, Value: lifetime[:]})); err != nil {
			return
		}
		_ = relayed // relayed address is not used by outbound dials
		f.relayControl(conn)
	case stun.MethodConnectionBind:
		value, err := msg.Get(stun.AttrConnectionID)
		if err != nil {
			return
		}
		f.mu.Lock()
		peer, matched := f.cids[binary.BigEndian.Uint32(value)]
		f.mu.Unlock()
		if !matched {
			writeSTUNRaw(conn, errorReply(msg, 400))
			return
		}
		if err := writeSTUNRaw(conn, successReply(msg)); err != nil {
			return
		}
		upstream, err := net.DialTimeout("tcp4", peer.String(), 5*time.Second)
		if err != nil {
			return
		}
		defer upstream.Close()
		done := make(chan struct{}, 2)
		go func() { io.Copy(upstream, conn); done <- struct{}{} }()
		go func() { io.Copy(conn, upstream); done <- struct{}{} }()
		<-done
	default:
		writeSTUNRaw(conn, errorReply(msg, 400))
	}
}

// relayControl answers Connect and Refresh transactions on the control
// connection until it closes.
func (f *fakeAnonymousTURN) relayControl(conn net.Conn) {
	for {
		msg, err := readSTUNRaw(conn)
		if err != nil {
			return
		}
		switch msg.Type.Method {
		case stun.MethodConnect:
			peerAddr := &stun.XORMappedAddress{}
			if err := peerAddr.GetFromAs(msg, stun.AttrXORPeerAddress); err != nil {
				writeSTUNRaw(conn, errorReply(msg, 400))
				return
			}
			f.mu.Lock()
			f.next++
			cid := f.next
			f.cids[cid] = &net.TCPAddr{IP: peerAddr.IP, Port: peerAddr.Port}
			f.mu.Unlock()
			value := make([]byte, 4)
			binary.BigEndian.PutUint32(value, cid)
			if err := writeSTUNRaw(conn, successReply(msg,
				stun.RawAttribute{Type: stun.AttrConnectionID, Value: value})); err != nil {
				return
			}
		case stun.MethodCreatePermission:
			// Some servers require a permission before Connect; accept it.
			if err := writeSTUNRaw(conn, successReply(msg)); err != nil {
				return
			}
		case stun.MethodRefresh:
			if f.failRefresh.Load() {
				writeSTUNRaw(conn, errorReply(msg, 437))
				return
			}
			if err := writeSTUNRaw(conn, successReply(msg, stun.RawAttribute{
				Type: stun.AttrLifetime, Value: []byte{0, 0, 2, 88}})); err != nil {
				return
			}
		default:
			writeSTUNRaw(conn, errorReply(msg, 400))
		}
	}
}

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
	peer := echoFixture(t)
	_, openServer := startFakeAnonymousTURN(t, peer)
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
	_, serverAddr := startFakeAnonymousTURN(t, peer)
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
	cfg := config{Scheme: "turn", Server: "127.0.0.1", Port: ln.Addr().(*net.TCPAddr).Port}
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
	fake, serverAddr := startFakeAnonymousTURN(t, peer)
	fake.mu.Lock()
	fake.lifetime = [4]byte{0, 0, 0, 6} // 6s → first refresh after 5s
	fake.mu.Unlock()
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
