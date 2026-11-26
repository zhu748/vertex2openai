package main

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"testing/synctest"
	"time"
)

func TestSetupSocketCancelWithoutDeadline(t *testing.T) {
	for _, operation := range []string{"read", "write"} {
		t.Run(operation, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			conn, peer := net.Pipe()
			defer conn.Close()
			defer peer.Close()
			release, err := withTimeout(ctx, conn)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			started, ended := make(chan struct{}), make(chan error, 1)
			go func() {
				close(started)
				var err error
				if operation == "read" {
					_, err = conn.Read(make([]byte, 1))
				} else {
					_, err = conn.Write([]byte{1})
				}
				ended <- err
			}()
			<-started
			cancel()
			select {
			case err := <-ended:
				if err == nil {
					t.Fatal("cancelled setup I/O succeeded")
				}
			case <-time.After(500 * time.Millisecond):
				t.Fatal("cancelled setup I/O remained blocked")
			}
		})
	}
}

func TestSetupSocketRejectsCancelledOwner(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	release, err := withTimeout(ctx, conn)
	if err == nil {
		release()
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-cancelled setup returned %v", err)
	}
}

func TestSetupSocketSurvivesOwnerCancelAfterHandoff(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	release, err := withTimeout(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	if err := release(); err != nil {
		t.Fatal(err)
	}
	cancel() // a completed setup no longer owns the socket
	if err := peer.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	wrote := make(chan error, 1)
	go func() {
		_, err := peer.Write([]byte{42})
		wrote <- err
	}()
	var b [1]byte
	if _, err := io.ReadFull(conn, b[:]); err != nil || b[0] != 42 {
		t.Fatalf("transferred socket was interrupted: %v", err)
	}
	if err := <-wrote; err != nil {
		t.Fatal(err)
	}
}

// Hold just the cancellation write to SetDeadline so the release/interrupt race
// can be exercised without scheduler sleeps or a live transport.
type heldSetupInterrupt struct {
	net.Conn
	started, allow chan struct{}
}

func (c *heldSetupInterrupt) SetDeadline(d time.Time) error {
	if !d.IsZero() && !d.After(time.Now()) {
		close(c.started)
		<-c.allow
	}
	return c.Conn.SetDeadline(d)
}

func TestSetupSocketHandoffJoinsConcurrentInterrupt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		conn, peer := net.Pipe()
		defer conn.Close()
		defer peer.Close()
		socket := &heldSetupInterrupt{conn, make(chan struct{}), make(chan struct{})}
		release, err := withTimeout(ctx, socket)
		if err != nil {
			t.Fatal(err)
		}
		cancel()
		<-socket.started
		result := make(chan error, 1)
		go func() { result <- release() }()
		synctest.Wait() // release is blocked waiting for the running callback
		select {
		case <-result:
			close(socket.allow)
			t.Fatal("handoff completed while the cancelled owner was still modifying the socket")
		default:
		}
		close(socket.allow)
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("handoff succeeded despite a concurrent cancellation: %v", err)
		}
		if err := release(); !errors.Is(err, context.Canceled) {
			t.Fatalf("releasing again lost the setup outcome: %v", err)
		}
	})
}
