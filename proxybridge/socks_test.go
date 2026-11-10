package main

import (
        "bytes"
        "io"
        "net"
        "os"
        "strings"
        "testing"
)

// refuseOverloaded and diagnostic() had zero direct coverage (2026-11 P2-5):
// they only ran indirectly through saturated serveSOCKS runs that the suite
// never triggered.

func TestRefuseOverloadedRepliesGeneralFailure(t *testing.T) {
        a, b := net.Pipe()
        defer b.Close()
        done := make(chan struct{})
        go func() {
                defer close(done)
                refuseOverloaded(a)
                a.Close()
        }()
        // SOCKS5 greeting offering exactly the no-auth method.
        if _, err := b.Write([]byte{5, 1, 0}); err != nil {
                t.Fatalf("greeting write: %v", err)
        }
        choice := make([]byte, 2)
        if _, err := io.ReadFull(b, choice); err != nil || !bytes.Equal(choice, []byte{5, 0}) {
                t.Fatalf("method choice: %v %v", choice, err)
        }
        // CONNECT 127.0.0.1:8080 — the dialer must never be consulted. Written
        // from a goroutine: refuseOverloaded consumes only the 4-byte command
        // head, and net.Pipe writes block until fully consumed.
        requestErr := make(chan error, 1)
        go func() {
                _, err := b.Write([]byte{5, 1, 0, 1, 127, 0, 0, 1, 0x1f, 0x90})
                requestErr <- err
        }()
        reply := make([]byte, 10)
        if _, err := io.ReadFull(b, reply); err != nil {
                t.Fatalf("reply read: %v", err)
        }
        if reply[0] != 5 || reply[1] != 1 {
                t.Fatalf("want SOCKS5 general failure (5,1), got %v", reply[:2])
        }
        <-done
}

func TestRefuseOverloadedDropsInvalidGreeting(t *testing.T) {
        a, b := net.Pipe()
        defer b.Close()
        done := make(chan struct{})
        go func() {
                defer close(done)
                refuseOverloaded(a)
                a.Close()
        }()
        // Zero offered methods: malformed greeting, must be dropped without a reply.
        if _, err := b.Write([]byte{5, 0}); err != nil {
                t.Fatalf("greeting write: %v", err)
        }
        if _, err := b.Read(make([]byte, 1)); err != io.EOF {
                t.Fatalf("want connection drop (EOF), got %v", err)
        }
        <-done
}

func TestDiagnosticTruncatesAndIsBounded(t *testing.T) {
        diagnostics.Store(0)
        r, w, err := os.Pipe()
        if err != nil {
                t.Fatalf("pipe: %v", err)
        }
        saved := os.Stderr
        os.Stderr = w
        // ": " suffix (TURN reason phrases may echo credentials), newline and
        // NUL (log forgery) must all be stripped to the leading code token.
        diagnostic("turn_probe_failed: password=secret\nsecond line\x00tail")
        for i := 0; i < 300; i++ {
                diagnostic("socks_overloaded")
        }
        w.Close()
        os.Stderr = saved
        out, err := io.ReadAll(r)
        if err != nil {
                t.Fatalf("read stderr: %v", err)
        }
        lines := strings.Split(strings.TrimRight(string(out), "\n"), "\n")
        if len(lines) != 128 {
                t.Fatalf("diagnostic output must be bounded at 128 lines, got %d", len(lines))
        }
        if lines[0] != "turn_probe_failed" {
                t.Fatalf("first line must be the bare code token, got %q", lines[0])
        }
        if strings.Contains(string(out), "secret") || strings.Contains(string(out), "second line") {
                t.Fatalf("cause text leaked into diagnostics: %q", lines[0])
        }
}
