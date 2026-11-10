// Byte-relay plumbing: bidirectional copy with half-close semantics and the
// idle-recycle watchdog. Split out of main.go (2026-11 P2-5, zero code
// changes).
package main

import (
        "io"
        "net"
        "sync/atomic"
        "time"
)

// relay 空闲回收窗口（2026-11 P2-7，测试可缩短）：
//
// 双向拷贝此前只在第一个方向「带错误」返回时才关闭两侧连接。半关闭语义下
// （对端正常 EOF、CloseWrite 成功、返回 nil），另一方向的 io.Copy 在静默
// 目标上会无限阻塞——组件文档自己就警告过「部分开放 TURN 服务器连接建立
// 后无响应」。每个挂死连接占住 1 个 goroutine、上游数据连接、控制连接以及
// 匿名路径的持续续期任务，128 个槽位被逐个耗尽后新连接全部进入拒绝路径，
// 进程无法自愈，表现为难以定位的「代理越来越慢直到不可用」。
var (
        // 半关闭后剩余方向的无流量回收窗口。
        relayHalfCloseIdle = 5 * time.Minute
        // 从未搬运过任何字节的连接回收窗口：真实客户端在 SOCKS 应答后立即
        // 发送请求，长时间零字节即死连接。
        relayFirstByteIdle = 10 * time.Minute
        // 看门狗轮询间隔。
        relayIdleCheck = 15 * time.Second
)

// relayIdleTimeouts 把看门狗的三个时间参数按值传递（2026-11 修复）：
// 旧测试直接改写上面的包级变量来缩窗，而上一测试遗留的看门狗
// goroutine 仍在读同一变量，`go test -race` 实锤数据竞争。
// 参数只在本结构体里流转，测试注入缩窗值，包级变量回归纯只读。
type relayIdleTimeouts struct {
        check     time.Duration
        firstByte time.Duration
        halfClose time.Duration
}

var defaultRelayIdleTimeouts = relayIdleTimeouts{
        check:     relayIdleCheck,
        firstByte: relayFirstByteIdle,
        halfClose: relayHalfCloseIdle,
}

// countingConn 统计经手字节数：任一方向有真实流量即视为活跃。
type countingConn struct {
        net.Conn
        counter *atomic.Int64
}

func (c countingConn) Read(p []byte) (int, error) {
        n, err := c.Conn.Read(p)
        if n > 0 {
                c.counter.Add(int64(n))
        }
        return n, err
}

func (c countingConn) Write(p []byte) (int, error) {
        n, err := c.Conn.Write(p)
        if n > 0 {
                c.counter.Add(int64(n))
        }
        return n, err
}

// CloseWrite 必须转发到底层连接（若支持）：包装会遮蔽具体的
// CloseWrite 方法，copyTo 的类型断言将改走整连接关闭分支，破坏
// 「首个方向 EOF 后另一方向仍可传输」的半关闭语义。底层不支持时
// 回退为整连接关闭，与 relay 原有的 else 分支行为一致。
func (c countingConn) CloseWrite() error {
        if closer, ok := c.Conn.(interface{ CloseWrite() error }); ok {
                return closer.CloseWrite()
        }
        c.Conn.Close()
        return nil
}

// relayIdleWatchdog 只回收「确认静默」的连接：从未有过流量
// （idle.firstByte），或半关闭后长时间无新字节（idle.halfClose）。
// 两侧都打开且有过流量的静默是合法场景——非流式 4K 生图在响应就绪前
// 可静默 10 分钟以上——不做中途闲置回收，避免误杀慢生成。
func relayIdleWatchdog(a, b net.Conn, activity *atomic.Int64, halfClosed *atomic.Bool,
        started time.Time, finished <-chan struct{}, idle relayIdleTimeouts) {
        ticker := time.NewTicker(idle.check)
        defer ticker.Stop()
        lastCount := activity.Load()
        // 半关闭静默窗口的起点：首次观察到 halfClosed 的那轮 tick。
        // 新字节到达会清零重计——「半关闭后又活跃」的慢生成不受影响。
        halfClosedSince := time.Time{}
        for {
                select {
                case <-finished:
                        return
                case now := <-ticker.C:
                        cur := activity.Load()
                        if cur != lastCount {
                                lastCount = cur
                                halfClosedSince = time.Time{}
                                continue
                        }
                        if cur == 0 && now.Sub(started) >= idle.firstByte {
                                a.Close()
                                b.Close()
                                return
                        }
                        if halfClosed.Load() {
                                if halfClosedSince.IsZero() {
                                        halfClosedSince = now
                                        continue
                                }
                                if now.Sub(halfClosedSince) >= idle.halfClose {
                                        a.Close()
                                        b.Close()
                                        return
                                }
                        }
                }
        }
}

func relay(a, b net.Conn) {
        relayWithIdleTimeouts(a, b, defaultRelayIdleTimeouts)
}

func relayWithIdleTimeouts(a, b net.Conn, idle relayIdleTimeouts) {
        var activity atomic.Int64
        var halfClosed atomic.Bool
        ca := countingConn{Conn: a, counter: &activity}
        cb := countingConn{Conn: b, counter: &activity}
        done := make(chan error, 2)
        copyTo := func(dst, src net.Conn) {
                _, err := io.Copy(dst, src)
                if closer, ok := dst.(interface{ CloseWrite() error }); ok {
                        if closeErr := closer.CloseWrite(); err == nil {
                                err = closeErr
                        }
                } else {
                        dst.Close()
                }
                done <- err
        }
        go copyTo(ca, cb)
        go copyTo(cb, ca)
        finished := make(chan struct{})
        defer close(finished)
        // 看门狗必须在首次 done 之前启动：两侧都打开但从未有流量的死连接
        // 正是 done 永远不来的场景——旧实现先等 first 再启动看门狗，恰好
        // 回收不了它存在的唯一理由（never-started 死连接）。
        go relayIdleWatchdog(a, b, &activity, &halfClosed, time.Now(), finished, idle)
        first := <-done
        if first == nil {
                halfClosed.Store(true)
        } else {
                a.Close()
                b.Close()
        }
        <-done
}
