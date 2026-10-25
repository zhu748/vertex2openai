package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

func resolverFixture(t *testing.T) *net.Resolver {
	t.Helper()
	server, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	go func() {
		buffer := make([]byte, 2048)
		for {
			n, peer, err := server.ReadFrom(buffer)
			if err != nil {
				return
			}
			var parser dnsmessage.Parser
			header, err := parser.Start(buffer[:n])
			if err != nil {
				continue
			}
			question, err := parser.Question()
			if err != nil {
				continue
			}
			builder := dnsmessage.NewBuilder(nil, dnsmessage.Header{ID: header.ID, Response: true,
				RecursionDesired: true, RecursionAvailable: true})
			builder.StartQuestions()
			builder.Question(question)
			builder.StartAnswers()
			if question.Type == dnsmessage.TypeA {
				builder.AResource(dnsmessage.ResourceHeader{Name: question.Name, Type: dnsmessage.TypeA,
					Class: dnsmessage.ClassINET, TTL: 60}, dnsmessage.AResource{A: [4]byte{127, 0, 0, 1}})
			}
			response, err := builder.Finish()
			if err == nil {
				server.WriteTo(response, peer)
			}
		}
	}()
	return &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "udp4", server.LocalAddr().String())
	}}
}

func TestTURNHostnameServerAndTargetUseSystemResolver(t *testing.T) {
	// Dependencies replace net.DefaultResolver. Neither server bootstrap nor
	// target lookup may depend on that process-global resolver being usable.
	original := net.DefaultResolver
	originalSystem := systemResolver
	systemResolver = resolverFixture(t)
	net.DefaultResolver = &net.Resolver{PreferGo: true, Dial: func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("default_resolver_must_not_be_used")
	}}
	t.Cleanup(func() { net.DefaultResolver = original; systemResolver = originalSystem })
	for _, secure := range []bool{false, true} {
		name := "TURN"
		if secure {
			name = "TURNS"
		}
		t.Run(name, func(t *testing.T) {
			cfg := turnFixture(t, secure)
			cfg.Server = "server.proxy.test"
			peer := echoFixture(t)
			tlsConfig, err := cfg.tlsConfig()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			conn, err := dialResolved(ctx, net.JoinHostPort("target.proxy.test", strconv.Itoa(peer.Port)), false,
				func(ctx context.Context, addr *net.TCPAddr) (net.Conn, error) {
					return cfg.dialTURN(ctx, addr, tlsConfig)
				})
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(2 * time.Second))
			payload := []byte("hostname server and hostname target")
			if _, err := conn.Write(payload); err != nil {
				t.Fatal(err)
			}
			got := make([]byte, len(payload))
			if _, err := io.ReadFull(conn, got); err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("hostname relay failed: %v", err)
			}
		})
	}
}
