// SOCKS5 front-end: bounded accept loop, greeting/request parsing and the
// overloaded-refusal path. Split out of main.go (2026-11 P2-5, zero code
// changes — the process entry point stays in main.go).
package main

import (
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"syscall"
	"time"
)

// ENG-P2-2（2026-11 审查）：高负载下 fd 耗尽（EMFILE/ENFILE）或连接被
// 中止（ECONNABORTED）时 Accept 返回错误，但这类错误是**可自愈**的——
// 描述符释放后即可恢复。若直接退出整个桥进程，所有在飞中继陪葬，
// Python 重启期间该节点全部请求中断。
func isTransientAcceptError(err error) bool {
	if errors.Is(err, syscall.EINTR) || errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.EMFILE) || errors.Is(err, syscall.ENFILE) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func serveSOCKS(ctx context.Context, listener net.Listener, dial dialFunc) error {
	ctx, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	defer func() { cancel(); listener.Close(); workers.Wait() }()
	stop := context.AfterFunc(ctx, func() { listener.Close() })
	defer stop()
	slots := make(chan struct{}, 128)
	// Refusals run on their own bounded pool: answering inline in the
	// accept loop let one slow client stall the loop (and thus every
	// other incoming connection) for the full 5-second refusal deadline.
	// Beyond the pool, saturated connections are dropped without a reply.
	refusals := make(chan struct{}, 64)
	var refusers sync.WaitGroup
	defer refusers.Wait()
	// ENG-P2-2：瞬时 accept 错误有界退避重试（100ms × 10 次，成功即复位
	// 计数）；仅持续性错误（listener 真正不可用）才终止进程。
	transientAccepts := 0
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if isTransientAcceptError(err) && transientAccepts < 10 {
				transientAccepts++
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(100 * time.Millisecond):
				}
				continue
			}
			return errors.New("accept_failed")
		}
		transientAccepts = 0
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
			// Overloaded: finish enough of the SOCKS exchange to
			// reply with a real failure code instead of a bare
			// reset, so the Python side can tell proxy saturation
			// apart from a network error and fail over correctly.
			select {
			case refusals <- struct{}{}:
				refusers.Add(1)
				go func() {
					defer refusers.Done()
					defer func() { <-refusals }()
					defer conn.Close()
					refuseOverloaded(conn)
				}()
			default:
				conn.Close()
			}
		}
	}
}

func socksReply(conn net.Conn, code byte) error {
	_, err := conn.Write([]byte{5, code, 0, 1, 0, 0, 0, 0, 0, 0})
	return err
}

// readSOCKSGreeting consumes the SOCKS5 client greeting (version + method
// list) and returns the offered methods. Shared by handleSOCKS and
// refuseOverloaded so both parse the wire format identically (2026-11
// review convergence: previously duplicated). The caller is responsible for
// having set a deadline on conn.
func readSOCKSGreeting(conn net.Conn) ([]byte, bool) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(conn, header); err != nil || header[0] != 5 || header[1] == 0 {
		return nil, false
	}
	methods := make([]byte, int(header[1]))
	if _, err := io.ReadFull(conn, methods); err != nil {
		return nil, false
	}
	return methods, true
}

// refuseOverloaded answers a CONNECT with "general SOCKS server failure"
// when the bridge is at its concurrency limit. Best effort: any parse
// error just drops the connection.
func refuseOverloaded(conn net.Conn) {
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, ok := readSOCKSGreeting(conn); !ok {
		return
	}
	if _, err := conn.Write([]byte{5, 0}); err != nil {
		return
	}
	request := make([]byte, 4)
	if _, err := io.ReadFull(conn, request); err != nil {
		return
	}
	socksReply(conn, 1)
	diagnostic("socks_overloaded")
}

func handleSOCKS(ctx context.Context, conn net.Conn, dial dialFunc) {
	conn.SetDeadline(time.Now().Add(30 * time.Second))
	methods, ok := readSOCKSGreeting(conn)
	if !ok {
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
		// ENG-P2-1（2026-11 审查）：握手期设的 30 秒 deadline 从 Accept
		// 后起算，拨号叉把预算耗尽时连接 deadline 已过期，失败码写不
		// 出去——Python 只能看到裸 reset，无法按错误码 failover。
		// 写回包前刷新 deadline，保住「按失败码切换节点」的契约。
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		socksReply(conn, 1)
		return
	}
	defer upstream.Close()
	stop := context.AfterFunc(ctx, func() { upstream.Close() })
	defer stop()
	// 同理：拨号接近预算上限时成功回包也可能撞上过期 deadline。
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	if err := socksReply(conn, 0); err != nil {
		return
	}
	conn.SetDeadline(time.Time{})
	upstream.SetDeadline(time.Time{})
	relay(conn, upstream)
}
