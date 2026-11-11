package main

// ENG-P3-6（2026-11 收尾轮）：sstpPackets 状态机的离线单测。
//
// 此前 SSTP 正向路径零离线覆盖——ready/closed 双通道、attach 与 Close
// 的竞态守卫、Close 幂等（sync.Once 防双 close panic）只被环境门控的
// 集成测试间接经过。本文件仿照 relay.go 看门狗测试的参数注入风格，
// 用可离线构造的 LightweightNetTun（用户态栈，无内核依赖）逐状态验证：
//
//  1. await 在 attach 前阻塞、attach 后放行（ready 通道语义）
//  2. Close 先于 attach：await/Read/Write 全部 io.ErrClosedPipe
//  3. attach 撞上已 Close：attach 内联关闭 TUN（竞态守卫），后续读必错
//  4. Close 幂等且可并发（8 个 goroutine 同时 Close 不 panic）
//  5. Close 解除阻塞中的 Read（半途启动的读不被永久挂死）
//  6. 真实包往返：栈发出 → Read 计 sent；Write 注回 → 计 received

import (
	"errors"
	"io"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/Asutorufa/yuhaiin/pkg/net/proxy/wireguard"
)

func newTestTun(t *testing.T) *wireguard.LightweightNetTun {
	t.Helper()
	network, err := wireguard.CreateLightweightNetTUN(
		[]netip.Prefix{netip.MustParsePrefix("192.0.2.1/32")}, 1400)
	if err != nil {
		t.Fatalf("create lightweight tun: %v", err)
	}
	t.Cleanup(func() { _ = network.Close() })
	return network
}

func TestSSTPPacketsAwaitBlocksUntilAttach(t *testing.T) {
	p := newSSTPPackets()
	ready := make(chan *wireguard.LightweightNetTun, 1)
	go func() {
		network, err := p.await()
		if err != nil {
			close(ready)
			return
		}
		ready <- network
	}()
	select {
	case <-ready:
		t.Fatal("await returned before attach")
	case <-time.After(50 * time.Millisecond):
	}
	network := newTestTun(t)
	p.attach(network)
	select {
	case got := <-ready:
		if got != network {
			t.Fatal("await returned a different network instance")
		}
	case <-time.After(time.Second):
		t.Fatal("await did not return after attach")
	}
}

func TestSSTPPacketsCloseBeforeAttachFailsEveryOp(t *testing.T) {
	p := newSSTPPackets()
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := p.await(); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("await after close = %v, want io.ErrClosedPipe", err)
	}
	if _, err := p.Read(make([]byte, 128)); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("read after close = %v, want io.ErrClosedPipe", err)
	}
	if _, err := p.Write([]byte("x")); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("write after close = %v, want io.ErrClosedPipe", err)
	}
	// 计数器不得在失败路径上误增。
	if p.sent.Load() != 0 || p.received.Load() != 0 {
		t.Fatalf("counters moved on closed packets: sent=%d received=%d",
			p.sent.Load(), p.received.Load())
	}
}

func TestSSTPPacketsAttachAfterCloseClosesTun(t *testing.T) {
	// 竞态守卫：Close 先落地、attach 后到时，attach 必须内联关掉 TUN，
	// 否则这个 net.Conn 形态的对象会绕过 once 泄漏（历史 double-close
	// panic 的姊妹场景）。
	p := newSSTPPackets()
	p.Close()
	network := newTestTun(t)
	p.attach(network)

	done := make(chan error, 1)
	go func() {
		_, err := p.Read(make([]byte, 128))
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("read on close-then-attach must fail (either ErrClosedPipe or net.ErrClosed)")
		}
	case <-time.After(time.Second):
		t.Fatal("read hung after close-then-attach")
	}
}

func TestSSTPPacketsCloseIsIdempotentAndConcurrent(t *testing.T) {
	p := newSSTPPackets()
	p.attach(newTestTun(t))
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = p.Close() // 双 close 会 panic；once 保护下必须全部安全
		}()
	}
	wg.Wait()
	_ = p.Close()
	if _, err := p.await(); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("await after repeated close = %v, want io.ErrClosedPipe", err)
	}
}

func TestSSTPPacketsCloseUnblocksPendingRead(t *testing.T) {
	p := newSSTPPackets()
	p.attach(newTestTun(t))
	done := make(chan error, 1)
	go func() {
		_, err := p.Read(make([]byte, 1500))
		done <- err
	}()
	select {
	case <-done:
		t.Fatal("read returned before any packet existed")
	case <-time.After(50 * time.Millisecond):
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("pending read must fail after close, not return empty")
		}
	case <-time.After(time.Second):
		t.Fatal("close did not unblock the pending read")
	}
}

func TestSSTPPacketsPacketRoundtripAndCounters(t *testing.T) {
	p := newSSTPPackets()
	network := newTestTun(t)
	p.attach(network)

	buf := make([]byte, 1500)
	readDone := make(chan int, 1)
	go func() {
		// 先就位：packetPipe 的 write 会同步等待读端接手（上游
		// lightweight_close_test 同款时序），没有读者 WriteUDP 会挂死。
		n, err := p.Read(buf)
		if err != nil {
			readDone <- -1
			return
		}
		readDone <- n
	}()
	if _, err := network.Stack().WriteUDP(
		netip.MustParseAddrPort("192.0.2.1:1000"),
		netip.MustParseAddrPort("192.0.2.2:2000"),
		[]byte("hello-roundtrip")); err != nil {
		t.Fatalf("stack WriteUDP: %v", err)
	}
	select {
	case n := <-readDone:
		if n <= 0 {
			t.Fatal("engine did not hand the routed packet to Read")
		}
	case <-time.After(time.Second):
		t.Fatal("routed packet never reached Read")
	}
	if p.sent.Load() != 1 {
		t.Fatalf("sent counter = %d, want 1", p.sent.Load())
	}
	if _, err := p.Write(buf[:20]); err != nil {
		t.Fatalf("write back into the stack: %v", err)
	}
	if p.received.Load() != 1 {
		t.Fatalf("received counter = %d, want 1", p.received.Load())
	}
}
