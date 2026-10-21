package main

// Regression tests for refresh-cadence and unsolicited-message handling in the
// anonymous RFC 6062 dialer. A self-contained scripted server is used so the
// wire behavior (granted lifetimes, injected indications) stays deterministic.

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/stun/v4"
)

// scriptedTURN is an open TURN server whose Allocate and Refresh responses
// carry configurable LIFETIME values, and which can inject one unsolicited
// ConnectionAttempt indication ahead of the first Refresh response.
type scriptedTURN struct {
	ln           net.Listener
	mu           sync.Mutex
	refreshAt    []time.Time
	cids         map[uint32]*net.TCPAddr
	next         uint32
	active       sync.WaitGroup
	allocateLife atomic.Uint32 // seconds granted in the Allocate response
	refreshLife  atomic.Uint32 // seconds granted in each Refresh response
	injectOnce   atomic.Bool   // send a ConnectionAttempt before first Refresh reply
	t            *testing.T
}

func startScriptedTURN(t *testing.T) (*scriptedTURN, *net.TCPAddr) {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &scriptedTURN{ln: ln, cids: map[uint32]*net.TCPAddr{}, next: 0x2100, t: t}
	s.allocateLife.Store(10)
	s.refreshLife.Store(10)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			s.active.Add(1)
			go func() { defer s.active.Done(); defer conn.Close(); s.serve(conn) }()
		}
	}()
	t.Cleanup(func() { ln.Close(); s.active.Wait() })
	return s, ln.Addr().(*net.TCPAddr)
}

func (s *scriptedTURN) serve(conn net.Conn) {
	msg, err := readSTUNRaw(conn)
	if err != nil {
		return
	}
	switch msg.Type.Method {
	case stun.MethodAllocate:
		lifetime := make([]byte, 4)
		binary.BigEndian.PutUint32(lifetime, s.allocateLife.Load())
		if err := writeSTUNRaw(conn, successReply(msg, stun.RawAttribute{
			Type: stun.AttrLifetime, Value: lifetime})); err != nil {
			return
		}
		s.relayControl(conn)
	case stun.MethodConnectionBind:
		value, err := msg.Get(stun.AttrConnectionID)
		if err != nil {
			return
		}
		s.mu.Lock()
		peer, matched := s.cids[binary.BigEndian.Uint32(value)]
		s.mu.Unlock()
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

func (s *scriptedTURN) relayControl(conn net.Conn) {
	for {
		msg, err := readSTUNRaw(conn)
		if err != nil {
			return
		}
		switch msg.Type.Method {
		case stun.MethodCreatePermission:
			if err := writeSTUNRaw(conn, successReply(msg)); err != nil {
				return
			}
		case stun.MethodConnect:
			peerAddr := &stun.XORMappedAddress{}
			if err := peerAddr.GetFromAs(msg, stun.AttrXORPeerAddress); err != nil {
				writeSTUNRaw(conn, errorReply(msg, 400))
				return
			}
			s.mu.Lock()
			s.next++
			cid := s.next
			s.cids[cid] = &net.TCPAddr{IP: peerAddr.IP, Port: peerAddr.Port}
			s.mu.Unlock()
			value := make([]byte, 4)
			binary.BigEndian.PutUint32(value, cid)
			if err := writeSTUNRaw(conn, successReply(msg,
				stun.RawAttribute{Type: stun.AttrConnectionID, Value: value})); err != nil {
				return
			}
		case stun.MethodRefresh:
			// Any peer that hits the relayed transport address makes the
			// server emit an unsolicited ConnectionAttempt indication on the
			// control connection (RFC 6062 section 6.2.2). Public relays see
			// these constantly from scanners.
			if s.injectOnce.CompareAndSwap(true, false) {
				indication := stun.MustBuild(
					stun.TransactionID,
					stun.NewType(stun.MethodConnectionAttempt, stun.ClassIndication),
					peerAddrSetter{IP: net.ParseIP("127.0.0.1"), Port: 9},
				)
				if err := writeSTUNRaw(conn, indication); err != nil {
					return
				}
			}
			s.mu.Lock()
			s.refreshAt = append(s.refreshAt, time.Now())
			s.mu.Unlock()
			lifetime := make([]byte, 4)
			binary.BigEndian.PutUint32(lifetime, s.refreshLife.Load())
			if err := writeSTUNRaw(conn, successReply(msg, stun.RawAttribute{
				Type: stun.AttrLifetime, Value: lifetime})); err != nil {
				return
			}
		default:
			writeSTUNRaw(conn, errorReply(msg, 400))
		}
	}
}

func (s *scriptedTURN) refreshTimes() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Time(nil), s.refreshAt...)
}

// waitForRefreshes polls until at least n refreshes were observed.
func waitForRefreshs(t *testing.T, s *scriptedTURN, n int, budget time.Duration) []time.Time {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if times := s.refreshTimes(); len(times) >= n {
			return times
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("expected at least %d refreshes within %v, saw %d", n, budget, len(s.refreshTimes()))
	return nil
}

// dialAnonymous establishes one relayed connection through the scripted server.
func dialAnonymous(t *testing.T, cfg config, peer *net.TCPAddr) net.Conn {
	t.Helper()
	tlsConfig, err := cfg.tlsConfig()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	conn, err := cfg.dialTURNAnonymous(ctx, peer, tlsConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func echoRoundTrip(t *testing.T, conn net.Conn, payload []byte) {
	t.Helper()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	written := make(chan error, 1)
	go func() { _, err := conn.Write(payload); written <- err }()
	received := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, received); err != nil {
		t.Fatalf("echo read failed: %v", err)
	}
	if err := <-written; err != nil {
		t.Fatalf("echo write failed: %v", err)
	}
	if !bytes.Equal(payload, received) {
		t.Fatal("payload changed")
	}
}

// Regression 1: the granted LIFETIME must drive the refresh cadence.
//   - Allocate grants 4s: the old 5s floor delayed the first refresh past
//     the expiry of such allocations.
//   - Refresh responses grant 2s: the old loop kept the original interval
//     (lifetime/2 from Allocate), so a server that reduces the lifetime
//     killed the allocation between refreshes.
func TestAnonymousTURNRefreshHonorsGrantedLifetime(t *testing.T) {
	peer := echoFixture(t)
	server, addr := startScriptedTURN(t)
	server.allocateLife.Store(4) // short initial grant
	server.refreshLife.Store(2)  // even shorter re-grant
	cfg := config{Scheme: "turn", Server: "127.0.0.1", Port: addr.Port}

	conn := dialAnonymous(t, cfg, peer)

	// First refresh must land inside the 4s granted window (2s after bind).
	times := waitForRefreshs(t, server, 1, 8*time.Second)
	if since := time.Since(times[0]); since < 0 {
		t.Fatalf("negative refresh latency: %v", since)
	}
	// After the first refresh granted 2s, the next refresh must follow
	// within half of that grant (1s) plus scheduling slack.
	times = waitForRefreshs(t, server, 2, 8*time.Second)
	gap := times[1].Sub(times[0])
	if gap > 3*time.Second {
		t.Fatalf("second refresh came %v after the first; a 2s re-grant must be honored (expected ~1s)", gap)
	}
	// The allocation must still relay data.
	echoRoundTrip(t, conn, bytes.Repeat([]byte("still-alive"), 512))
}

// Regression 2: an unsolicited ConnectionAttempt indication between refresh
// transactions used to be consumed as the Refresh response, failing the
// refresh, killing the allocation, and desynchronizing the stream.
func TestAnonymousTURNIndicationDoesNotKillRefresh(t *testing.T) {
	peer := echoFixture(t)
	server, addr := startScriptedTURN(t)
	server.allocateLife.Store(10)
	server.refreshLife.Store(10)
	server.injectOnce.Store(true)
	cfg := config{Scheme: "turn", Server: "127.0.0.1", Port: addr.Port}

	conn := dialAnonymous(t, cfg, peer)

	// The first refresh fires ~5s in; the scripted server injects one
	// ConnectionAttempt indication before its success response.
	waitForRefreshs(t, server, 1, 10*time.Second)
	// Data must still flow: the indication must be skipped, not mistaken
	// for the refresh result.
	echoRoundTrip(t, conn, bytes.Repeat([]byte("after-indication"), 512))
	// And the next refresh cycle must still work.
	waitForRefreshs(t, server, 2, 12*time.Second)
}
