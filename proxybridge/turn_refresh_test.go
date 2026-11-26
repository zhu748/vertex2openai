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
	"testing/synctest"
	"time"

	"github.com/pion/stun/v4"
)

// fakeTURN is the shared scriptable open TURN server for the anonymous-relay
// and refresh-lifetime tests. Responses need no credentials; optional
// behaviors are toggled via atomics:
//   - allocateLife / refreshLife: seconds granted in Allocate / Refresh
//     responses (default 600, the 0x00000288 the relay fake used to hardcode)
//   - failRefresh: Refresh replies 437 allocation-quota-exceeded
//   - injectConnectionAttempt: one unsolicited ConnectionAttempt indication
//     ahead of the first Refresh response (RFC 6062 section 6.2.2)
//   - refreshTimes(): timestamps of every observed Refresh transaction
type fakeTURN struct {
	ln                      net.Listener
	mu                      sync.Mutex
	grantedAt               time.Time
	refreshAt               []time.Time
	cids                    map[uint32]*net.TCPAddr
	next                    uint32
	active                  sync.WaitGroup
	allocateLife            atomic.Uint32 // seconds granted in the Allocate response
	omitAllocateLife        atomic.Bool   // omit optional lifetime in compatibility fixture
	refreshLife             atomic.Uint32 // seconds granted in each Refresh response
	connectDelay            atomic.Int64  // simulated time spent completing Connect
	failRefresh             atomic.Bool   // Refresh replies 437 instead of success
	injectConnectionAttempt atomic.Bool   // send a ConnectionAttempt before first Refresh reply
}

func startFakeTURN(t *testing.T) (*fakeTURN, *net.TCPAddr) {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &fakeTURN{ln: ln, cids: map[uint32]*net.TCPAddr{}, next: 0x2100}
	s.allocateLife.Store(600)
	s.refreshLife.Store(600)
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

func (s *fakeTURN) serve(conn net.Conn) {
	msg, err := readSTUNRaw(conn)
	if err != nil {
		return
	}
	switch msg.Type.Method {
	case stun.MethodAllocate:
		s.mu.Lock()
		s.grantedAt = time.Now()
		s.mu.Unlock()
		response := successReply(msg)
		if !s.omitAllocateLife.Load() {
			lifetime := binary.BigEndian.AppendUint32(nil, s.allocateLife.Load())
			response = successReply(msg, stun.RawAttribute{Type: stun.AttrLifetime, Value: lifetime})
		}
		if err := writeSTUNRaw(conn, response); err != nil {
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

func (s *fakeTURN) relayControl(conn net.Conn) {
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
			time.Sleep(time.Duration(s.connectDelay.Load()))
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
			if s.injectConnectionAttempt.CompareAndSwap(true, false) {
				indication := stun.MustBuild(
					stun.TransactionID,
					stun.NewType(stun.MethodConnectionAttempt, stun.ClassIndication),
					peerAddrSetter{IP: net.ParseIP("127.0.0.1"), Port: 9},
				)
				if err := writeSTUNRaw(conn, indication); err != nil {
					return
				}
			}
			if s.failRefresh.Load() {
				writeSTUNRaw(conn, errorReply(msg, 437))
				return
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

func (s *fakeTURN) grantTime() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.grantedAt
}

func (s *fakeTURN) refreshTimes() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Time(nil), s.refreshAt...)
}

// waitForRefreshes polls until at least n refreshes were observed.
func waitForRefreshs(t *testing.T, s *fakeTURN, n int, budget time.Duration) []time.Time {
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
	server, addr := startFakeTURN(t)
	server.allocateLife.Store(4) // short initial grant
	server.refreshLife.Store(2)  // even shorter re-grant
	cfg := config{Scheme: "turn", Server: "127.0.0.1", Port: addr.Port}

	conn := dialAnonymous(t, cfg, peer)

	// First refresh must land inside the 4s window granted by Allocate.
	times := waitForRefreshs(t, server, 1, 8*time.Second)
	if age := times[0].Sub(server.grantTime()); age < 0 || age >= 4*time.Second {
		t.Fatalf("first refresh outside the 4s grant window: %v", age)
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
	server, addr := startFakeTURN(t)
	server.allocateLife.Store(10)
	server.refreshLife.Store(10)
	server.injectConnectionAttempt.Store(true)
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

// Setup has exclusive use of the control socket until ConnectionBind returns.
// A slow successful setup does not grant another lifetime at handoff.
func TestAnonymousTURNSlowSetupGrantWindow(t *testing.T) {
	for _, tt := range []struct {
		name     string
		lifetime uint32
		delay    time.Duration
		expired  bool
	}{
		{"refresh-overdue-but-still-live", 2, 1250 * time.Millisecond, false},
		{"expired-before-handoff", 1, 1250 * time.Millisecond, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			peer := echoFixture(t)
			server, addr := startFakeTURN(t)
			server.allocateLife.Store(tt.lifetime)
			server.connectDelay.Store(int64(tt.delay))
			cfg := config{Scheme: "turn", Server: "127.0.0.1", Port: addr.Port}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			conn, err := cfg.dialTURNAnonymous(ctx, peer, nil)
			if conn != nil {
				defer conn.Close()
			}
			if tt.expired {
				if conn != nil || err == nil || err.Error() != "turn_allocation_expired_during_setup" {
					t.Fatalf("expired grant: handed off connection=%v, error=%v", conn != nil, err)
				}
				if n := len(server.refreshTimes()); n != 0 {
					t.Fatalf("started renewal of expired allocation: %d refreshes", n)
				}
				return // fixture cleanup also waits for both owned sockets to close
			}
			if err != nil {
				t.Fatal(err)
			}
			first := waitForRefreshs(t, server, 1, 3*time.Second)[0]
			age := first.Sub(server.grantTime())
			if age < tt.delay || age >= time.Duration(tt.lifetime)*time.Second {
				t.Fatalf("first refresh at grant age %v after setup delay %v (lifetime %ds)", age, tt.delay, tt.lifetime)
			}
			t.Logf("first refresh at grant age %v after setup delay %v", age, tt.delay)
			echoRoundTrip(t, conn, []byte("alive"))
		})
	}
}

func TestAnonymousTURNInitialZeroGrantIsNotMissing(t *testing.T) {
	for _, test := range []struct {
		name string
		omit bool
	}{{"explicit-zero", false}, {"omitted", true}} {
		t.Run(test.name, func(t *testing.T) {
			peer := echoFixture(t)
			server, addr := startFakeTURN(t)
			server.allocateLife.Store(0)
			server.omitAllocateLife.Store(test.omit)
			cfg := config{Scheme: "turn", Server: "127.0.0.1", Port: addr.Port}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			conn, err := cfg.dialTURNAnonymous(ctx, peer, nil)
			if conn != nil {
				defer conn.Close()
			}
			if test.omit {
				if err != nil {
					t.Fatal(err)
				}
				echoRoundTrip(t, conn, []byte("existing default"))
				return
			}
			if err == nil || err.Error() != "turn_allocation_zero_lifetime" || conn != nil {
				t.Fatalf("zero grant handed off connection=%v, error=%v", conn != nil, err)
			}
			server.mu.Lock()
			defer server.mu.Unlock()
			if len(server.cids) != 0 || len(server.refreshAt) != 0 {
				t.Fatal("continued setup or refreshed an already expired allocation")
			}
		})
	}
}

// Deterministic boundary coverage: unused setup time is subtracted, not a
// fresh half-life added. A zero re-grant must still close the control socket.
func TestFirstRefreshUsesRemainingGrantWindow(t *testing.T) {
	for _, delay := range []time.Duration{0, 400 * time.Millisecond, time.Second, 1250 * time.Millisecond, 2 * time.Second} {
		t.Run(delay.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				control, server := net.Pipe()
				defer server.Close()
				allocation := &rawAllocation{control: control, lifetime: 2 * time.Second, closed: make(chan struct{})}
				defer allocation.stop()
				granted := time.Now()
				time.Sleep(delay)
				allocation.startRefresh(granted, allocation.stop)
				if delay >= allocation.lifetime {
					// A goroutine delayed until expiry must not even send Refresh.
					synctest.Wait()
					select {
					case <-allocation.closed:
					default:
						t.Fatal("stalled owner attempted to renew the expired grant")
					}
					return
				}
				request, err := readSTUNRaw(server)
				if err != nil {
					t.Fatal(err)
				}
				if request.Type.Method != stun.MethodRefresh {
					t.Fatalf("expected refresh, got %s", request.Type)
				}
				if age, want := time.Since(granted), max(time.Second, delay); age != want {
					t.Fatalf("first refresh at %v; want %v", age, want)
				}
				if err := writeSTUNRaw(server, successReply(request, stun.RawAttribute{
					Type: stun.AttrLifetime, Value: make([]byte, 4),
				})); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				select {
				case <-allocation.closed:
				default:
					t.Fatal("zero re-grant did not close the allocation")
				}
			})
		})
	}
}

// A refresh request by itself is not a new grant. Only a confirmed reply
// starts the next window; silence must stop both sockets at the earlier limit.
func TestRefreshSilenceStopsWithinConfirmedWindow(t *testing.T) {
	for _, tc := range []struct {
		name      string
		lifetime  time.Duration
		renew     bool
		wantClose time.Duration
	}{
		{"short grant", 2 * time.Second, false, 2 * time.Second},
		{"ordinary transaction limit", 30 * time.Second, false, 25 * time.Second},
		{"confirmed new grant", 2 * time.Second, true, 5500 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				control, server := net.Pipe()
				defer server.Close()
				allocation := &rawAllocation{control: control, lifetime: tc.lifetime, closed: make(chan struct{})}
				defer allocation.stop()
				killed := make(chan struct{})
				started := time.Now()
				allocation.startRefresh(started, func() { allocation.stop(); close(killed) })
				request, err := readSTUNRaw(server)
				if err != nil || request.Type.Method != stun.MethodRefresh {
					t.Fatalf("refresh: %v, err=%v", request, err)
				}
				if tc.renew {
					time.Sleep(500 * time.Millisecond)
					if err := writeSTUNRaw(server, successReply(request, stun.RawAttribute{
						Type: stun.AttrLifetime, Value: binary.BigEndian.AppendUint32(nil, 4),
					})); err != nil {
						t.Fatal(err)
					}
					if _, err := readSTUNRaw(server); err != nil {
						t.Fatal(err)
					}
					if age := time.Since(started); age != 3500*time.Millisecond {
						t.Fatalf("next refresh at %v", age)
					}
				}
				<-killed
				if age := time.Since(started); age != tc.wantClose {
					t.Fatalf("terminated at %v, want %v", age, tc.wantClose)
				}
				if _, err := readSTUNRaw(server); err == nil {
					t.Fatal("control stayed open")
				}
			})
		})
	}
}
