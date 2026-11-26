package main

// Anonymous-allocation TURN support (RFC 6062 over raw STUN).
//
// The Pion TURN client always expects the RFC 5766 long-term-credential
// dance: it treats the first Allocate response as a 401 challenge and
// unconditionally reads NONCE/REALM from it (client.go sendAllocateRequest).
// Open TURN servers that accept unauthenticated allocations reply with a
// bare success response, so Pion fails with an attribute error even though
// the allocation succeeded.
//
// At startup the bridge probes the server with one raw Allocate request.
// Success selects the raw dialer below; an error response (401/438/...)
// selects the Pion client. The raw dialer mirrors Pion's RFC 6062 flow
// without credentials: Allocate, Connect(peer), then a second connection
// running ConnectionBind, followed by application bytes and periodic
// Refresh requests that fail closed.

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/pion/stun/v4"
)

const stunHeaderSize = 20
const rawControlIOBudget = 10 * time.Second

// peerAddrSetter writes XOR-PEER-ADDRESS (same wire format as
// XORMappedAddress, different attribute type).
type peerAddrSetter stun.XORMappedAddress

func (s peerAddrSetter) AddTo(m *stun.Message) error {
	return stun.XORMappedAddress(s).AddToAs(m, stun.AttrXORPeerAddress)
}

// rawSTUNTransaction writes one request and reads the response carrying the
// same transaction ID. STUN-over-TCP framing: 20-byte header, then the
// announced length.
//
// The control connection is not request/response exclusive: any peer that
// hits the relayed transport address makes the server emit an unsolicited
// ConnectionAttempt indication (RFC 6062 section 6.2.2), and retransmitted
// responses to earlier transactions can still arrive. Reading exactly one
// message used to consume such traffic as the reply, failing the Refresh,
// killing the allocation and desynchronizing the stream. Unsolicited
// messages are therefore read past; the connection deadline bounds the loop.
func rawSTUNTransaction(conn net.Conn, request *stun.Message) (*stun.Message, error) {
	if _, err := conn.Write(request.Raw); err != nil {
		return nil, err
	}
	head := make([]byte, stunHeaderSize)
	for {
		if _, err := io.ReadFull(conn, head); err != nil {
			return nil, err
		}
		if !stun.IsMessage(head) {
			return nil, errors.New("not a STUN message")
		}
		body := make([]byte, binary.BigEndian.Uint16(head[2:4]))
		if _, err := io.ReadFull(conn, body); err != nil {
			return nil, err
		}
		response := &stun.Message{Raw: append(head, body...)}
		if err := response.Decode(); err != nil {
			return nil, err
		}
		if response.Type.Class == stun.ClassIndication {
			continue
		}
		if response.TransactionID != request.TransactionID {
			continue
		}
		return response, nil
	}
}

// withTimeout gives the setup owner both the bounded I/O deadline and the
// ability to interrupt an already-open socket on cancellation. The returned
// release must succeed before handing the socket to another owner.
func withTimeout(ctx context.Context, conn net.Conn) (func() error, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := conn.SetDeadline(minDeadline(ctx, rawControlIOBudget)); err != nil {
		return nil, err
	}
	clear := func() error { return conn.SetDeadline(time.Time{}) }
	if ctx.Done() == nil {
		return sync.OnceValue(clear), nil
	}
	interrupted := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		// SetDeadline interrupts reads and writes without racing the owner's
		// regular Close. If the socket cannot update its deadline, close it.
		if err := conn.SetDeadline(time.Now()); err != nil {
			conn.Close()
		}
		close(interrupted)
	})
	return sync.OnceValue(func() error {
		if !stop() {
			<-interrupted // never clear a deadline concurrently with the callback
		}
		if err := ctx.Err(); err != nil {
			return err // cancellation won; a caller must not transfer this socket
		}
		return clear()
	}), nil
}

// requestedFamily encodes REQUESTED-ADDRESS-FAMILY (0x0017): 0x01 = IPv4,
// 0x02 = IPv6, plus three reserved zero bytes.
func requestedFamily(ipv4 bool) stun.RawAttribute {
	value := []byte{2, 0, 0, 0}
	if ipv4 {
		value[0] = 1
	}
	return stun.RawAttribute{Type: stun.AttrRequestedAddressFamily, Value: value}
}

func allocateRequest(ipv4 bool) *stun.Message {
	return stun.MustBuild(
		stun.TransactionID,
		stun.NewType(stun.MethodAllocate, stun.ClassRequest),
		stun.RawAttribute{Type: stun.AttrRequestedTransport, Value: []byte{6, 0, 0, 0}}, // 6 = TCP (RFC 6062)
		requestedFamily(ipv4),
		stun.Fingerprint,
	)
}

func stunError(response *stun.Message) error {
	var code stun.ErrorCodeAttribute
	if err := code.GetFrom(response); err == nil {
		return fmt.Errorf("stun error %d (%s)", code.Code, code.Reason)
	}
	return fmt.Errorf("%s", response.Type)
}

// detectTURNMode probes the server once: a success response to an
// unauthenticated Allocate selects the anonymous dialer; any error response
// (401/438/300/...) selects the Pion client, which handles the standard
// credential flow.
func detectTURNMode(parent context.Context, c config, tlsConfig *tls.Config) (mode string, err error) {
	// Tight budget: the Python manager marks the bridge as failed when the
	// ready marker does not appear within 10 seconds of launch.
	ctx, cancel := context.WithTimeout(parent, 8*time.Second)
	defer cancel()
	control, _, err := dialServer(ctx, net.JoinHostPort(c.Server, strconv.Itoa(c.Port)), tlsConfig)
	if err != nil {
		return "", err
	}
	defer control.Close()
	clear, err := withTimeout(ctx, control)
	if err != nil {
		return "", err
	}
	defer func() {
		clear() // probe sockets are not transferred
		// An internal timeout during best-effort release does not erase the
		// observed mode, but explicit caller shutdown must not report it as ready.
		if cause := parent.Err(); cause != nil {
			mode, err = "", cause
		}
	}()
	// Most open relays are IPv4-only; probe the IPv4 family first and fall
	// back to the Pion client whenever the server answers with an error.
	response, err := rawSTUNTransaction(control, allocateRequest(true))
	if err != nil {
		return "", err
	}
	if response.Type.Class == stun.ClassSuccessResponse {
		// The probe just created a real allocation. Closing the TCP control
		// connection releases it on RFC-compliant servers; an explicit
		// deallocation (RFC 5766 section 7.3: Refresh with LIFETIME 0) also
		// covers servers that keep relayed ports reserved until expiry.
		// Best effort: the mode decision does not depend on it.
		release := stun.MustBuild(
			stun.TransactionID,
			stun.NewType(stun.MethodRefresh, stun.ClassRequest),
			stun.RawAttribute{Type: stun.AttrLifetime, Value: make([]byte, 4)},
			stun.Fingerprint,
		)
		_, _ = rawSTUNTransaction(control, release)
		return "anonymous", nil
	}
	return "pion", nil
}

type rawAllocation struct {
	control  net.Conn
	lifetime time.Duration
	closed   chan struct{}
	once     sync.Once
}

func (a *rawAllocation) stop() {
	a.once.Do(func() { close(a.closed); a.control.Close() })
}

// refreshInterval schedules the next refresh inside the granted window:
// half the granted lifetime with a small floor, and strictly before the
// expiry. The old fixed floor of 5s delayed the first refresh past the
// expiry of allocations granted less than 10 seconds.
func refreshInterval(granted time.Duration) time.Duration {
	if granted <= 0 {
		return 0
	}
	interval := granted / 2
	if interval < 250*time.Millisecond {
		interval = 250 * time.Millisecond
	}
	if interval >= granted { // sub-second grants
		interval = granted / 3
	}
	if interval <= 0 {
		interval = time.Millisecond
	}
	return interval
}

// startRefresh keeps the allocation alive; a failed refresh fails closed by
// closing the control connection and notifying the data side.
//
// RFC 5766 section 6.2 lets the server grant less time than requested, so
// the next refresh follows the LIFETIME returned by each response. The old
// loop kept the interval derived from the Allocate response: a server that
// reduced the lifetime let the allocation expire between refreshes.
func (a *rawAllocation) startRefresh(grantedAt time.Time, kill func()) {
	go func() {
		// The control socket was owned by setup until now. The initial grant
		// already ran during Permission / Connect / Bind, not from handoff.
		timer := time.NewTimer(time.Until(grantedAt.Add(refreshInterval(a.lifetime))))
		defer timer.Stop()
		granted := a.lifetime
		for {
			select {
			case <-a.closed:
				return
			case <-timer.C:
			}
			if time.Since(grantedAt) >= granted {
				// A stalled scheduler can wake after the known grant has expired.
				kill()
				return
			}
			// A pending Refresh is not a confirmed grant. The existing I/O
			// budget may be shorter, but must not extend the previous lifetime.
			expires := grantedAt.Add(granted)
			if err := a.control.SetDeadline(cappedIODeadline(expires, rawControlIOBudget)); err != nil {
				a.stop()
				kill()
				return
			}
			refresh := stun.MustBuild(
				stun.TransactionID,
				stun.NewType(stun.MethodRefresh, stun.ClassRequest),
				stun.RawAttribute{Type: stun.AttrLifetime,
					Value: binary.BigEndian.AppendUint32(nil, uint32(a.lifetime/time.Second))},
				stun.Fingerprint,
			)
			response, err := rawSTUNTransaction(a.control, refresh)
			grantedAt = time.Now()
			clearErr := a.control.SetDeadline(time.Time{})
			if err != nil || clearErr != nil || !grantedAt.Before(expires) || response.Type.Class != stun.ClassSuccessResponse {
				a.stop()
				kill()
				return
			}
			if value, err := response.Get(stun.AttrLifetime); err == nil && len(value) == 4 {
				granted = time.Duration(binary.BigEndian.Uint32(value)) * time.Second
			}
			if next := refreshInterval(granted); next <= 0 {
				// The server granted a zero lifetime: the allocation is gone.
				a.stop()
				kill()
				return
			} else {
				timer.Reset(time.Until(grantedAt.Add(next)))
			}
		}
	}()
}

// dialTURNAnonymous implements outbound RFC 6062 relaying without
// credentials: Allocate → Connect(peer) → ConnectionBind on a second
// connection, then plain relayed bytes.
func (c config) dialTURNAnonymous(ctx context.Context, peer *net.TCPAddr, tlsConfig *tls.Config) (_ net.Conn, err error) {
	serverAddr := net.JoinHostPort(c.Server, strconv.Itoa(c.Port))
	control, _, err := dialServer(ctx, serverAddr, tlsConfig)
	if err != nil {
		return nil, err
	}
	var data net.Conn
	complete := false
	defer func() {
		if !complete {
			if data != nil {
				data.Close()
			}
			control.Close()
			if cause := ctx.Err(); cause != nil {
				err = cause
			}
		}
	}()
	clearControl, err := withTimeout(ctx, control)
	if err != nil {
		return nil, err
	}
	defer clearControl()

	allocate := allocateRequest(peer.IP.To4() != nil)
	response, err := rawSTUNTransaction(control, allocate)
	if err != nil {
		return nil, err
	}
	if response.Type.Class != stun.ClassSuccessResponse {
		return nil, fmt.Errorf("turn_allocate_failed: %w", stunError(response))
	}
	grantedAt := time.Now()
	lifetime := 600 * time.Second
	if value, err := response.Get(stun.AttrLifetime); err == nil && len(value) == 4 {
		seconds := binary.BigEndian.Uint32(value)
		if seconds == 0 {
			// Unlike an omitted LIFETIME, an explicit zero means there is
			// no live allocation. The setup owner still owns the control socket.
			return nil, errors.New("turn_allocation_zero_lifetime")
		}
		lifetime = time.Duration(seconds) * time.Second
	}

	// CreatePermission for the peer before Connect, matching the Pion client
	// and the edgetunnel Worker implementation: some servers reject a Connect
	// for a peer without an installed permission.
	permission := stun.MustBuild(
		stun.TransactionID,
		stun.NewType(stun.MethodCreatePermission, stun.ClassRequest),
		peerAddrSetter{IP: peer.IP, Port: peer.Port},
		stun.Fingerprint,
	)
	response, err = rawSTUNTransaction(control, permission)
	if err != nil {
		return nil, err
	}
	if response.Type.Class != stun.ClassSuccessResponse {
		return nil, fmt.Errorf("turn_permission_failed: %w", stunError(response))
	}

	connect := stun.MustBuild(
		stun.TransactionID,
		stun.NewType(stun.MethodConnect, stun.ClassRequest),
		peerAddrSetter{IP: peer.IP, Port: peer.Port},
		stun.Fingerprint,
	)
	response, err = rawSTUNTransaction(control, connect)
	if err != nil {
		return nil, err
	}
	if response.Type.Class != stun.ClassSuccessResponse {
		return nil, fmt.Errorf("turn_connect_failed: %w", stunError(response))
	}
	cid, err := response.Get(stun.AttrConnectionID)
	if err != nil || len(cid) != 4 {
		return nil, errors.New("turn_connection_id_missing")
	}

	// RFC 6062 requires the same server transport address for both
	// connections, exactly like the Pion path.
	data, tcpData, err := dialServer(ctx, control.RemoteAddr().String(), tlsConfig)
	if err != nil {
		return nil, err
	}
	// Keep a floor on the binding exchange even when the caller passes a
	// context without a deadline: a silent server would otherwise block
	// this dial forever instead of failing within seconds.
	clearData, err := withTimeout(ctx, data)
	if err != nil {
		return nil, err
	}
	defer clearData()
	adapter := &turnDataConn{TCPConn: tcpData, stream: data, binding: true}
	bind := stun.MustBuild(
		stun.TransactionID,
		stun.NewType(stun.MethodConnectionBind, stun.ClassRequest),
		stun.RawAttribute{Type: stun.AttrConnectionID, Value: cid},
		stun.Fingerprint,
	)
	if _, err := adapter.Write(bind.Raw); err != nil {
		return nil, err
	}
	head := make([]byte, stunHeaderSize)
	if _, err := adapter.Read(head); err != nil || !stun.IsMessage(head) {
		return nil, errors.New("connection bind: invalid response")
	}
	body := make([]byte, binary.BigEndian.Uint16(head[2:4]))
	if _, err := adapter.Read(body); err != nil {
		return nil, err
	}
	bindResponse := &stun.Message{Raw: append(head, body...)}
	if err := bindResponse.Decode(); err != nil {
		return nil, err
	}
	if bindResponse.Type.Class != stun.ClassSuccessResponse {
		return nil, fmt.Errorf("turn_connection_bind_failed: %w", stunError(bindResponse))
	}
	// Transaction ID 校验（2026-11 G-2）：行为异常的服务器可用任意成功响应
	// 让绑定「成功」；与同文件 rawSTUNTransaction 的语义对齐，ID 不匹配
	// 即拒绝。（数据连接不做 indication 跳过循环：绑定期内不该有旁路流量。）
	if bindResponse.TransactionID != bind.TransactionID {
		return nil, errors.New("connection bind: transaction mismatch")
	}
	adapter.binding = false
	// Stop and join both setup callbacks before starting connection-owned work.
	// A later cancel of the dial context must not touch either transferred socket.
	if err := clearData(); err != nil {
		return nil, err
	}
	if err := clearControl(); err != nil {
		return nil, err
	}
	if time.Since(grantedAt) >= lifetime {
		// The setup owner still owns both sockets; never hand off a stale grant.
		return nil, errors.New("turn_allocation_expired_during_setup")
	}
	complete = true

	// Single ownership: rawAllocation.stop() is the ONLY closer of the
	// closed channel (and of control). kill() delegates there and closes
	// the data connection exactly once. Previously kill guarded the same
	// channel with a second sync.Once, so the refresh-failure path
	// (stop followed by kill) panicked with "close of closed channel"
	// and crashed the whole bridge process.
	allocation := &rawAllocation{control: control, lifetime: lifetime, closed: make(chan struct{})}
	var dataOnce sync.Once
	kill := func() {
		allocation.stop()
		dataOnce.Do(func() { data.Close() })
	}
	allocation.startRefresh(grantedAt, kill)
	// The dial context is cancelled by handleSOCKS right after this returns
	// (same as the Pion path): the connection must survive it, ownership is
	// purely cleanup-driven from here on.
	return &managedConn{Conn: adapter, cleanup: kill}, nil
}
