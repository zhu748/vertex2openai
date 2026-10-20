package main

// DNS bootstrap repair.
//
// yuhaiin's netapi package replaces net.DefaultResolver in an init() hook
// with its own bootstrap resolver, which fails every lookup with
// "bootstrap resolver is not initialized" until netapi.SetBootstrap is
// called. This bridge resolved targets with net.DefaultResolver, so after
// the yuhaiin dependency was added, any hostname target (and any hostname
// TURN/SSTP server) failed with target_dns_failed. Loopback-only Go tests
// never noticed because they dial numeric IPs.
//
// Two independent safeguards:
//  1. systemResolver is a standalone resolver that reads /etc/resolv.conf
//     directly. It never consults net.DefaultResolver, so all bridge dial
//     paths use it explicitly.
//  2. init() installs a systemDNS adapter as the netapi bootstrap, restoring
//     net.DefaultResolver for anything else in this process that still uses
//     it (dependency internals included).

import (
	"context"
	"net"
	"net/netip"
	"strings"
	"time"

	"codeberg.org/miekg/dns"
	"codeberg.org/miekg/dns/rdata"
	"github.com/Asutorufa/yuhaiin/pkg/net/netapi"
)

// systemResolver resolves through the system resolver (resolv.conf) without
// touching the hijacked net.DefaultResolver.
var systemResolver = &net.Resolver{PreferGo: true}

// dialer returns a net.Dialer bound to systemResolver. Every bridge dial that
// may receive a hostname must use this instead of a bare net.Dialer.
func dialer() *net.Dialer {
	return &net.Dialer{KeepAlive: 30 * time.Second, Resolver: systemResolver}
}

type systemDNS struct{}

func (systemDNS) Name() string { return "system" }
func (systemDNS) Close() error { return nil }

func (systemDNS) LookupIP(ctx context.Context, domain string,
	_ ...func(*netapi.LookupIPOption)) (*netapi.IPs, error) {
	addrs, err := systemResolver.LookupIPAddr(ctx, strings.TrimSuffix(domain, "."))
	if err != nil {
		return nil, err
	}
	ips := &netapi.IPs{}
	for _, addr := range addrs {
		if addr.IP.To4() != nil {
			ips.A = append(ips.A, addr.IP)
		} else {
			ips.AAAA = append(ips.AAAA, addr.IP)
		}
	}
	return ips, nil
}

// Raw answers the wire-format queries that the hijacked net.DefaultResolver
// forwards through netapi's dnsConn. Only A/AAAA are ever requested by this
// process; other types get REFUSED instead of fabricated answers.
func (s systemDNS) Raw(ctx context.Context, req netapi.DNSQuestion) (*dns.Msg, error) {
	msg := netapi.NewDNSMsg(req)
	switch req.Qtype {
	case dns.TypeA, dns.TypeAAAA:
		ips, err := s.LookupIP(ctx, req.Name)
		if err != nil {
			return nil, err
		}
		if req.Qtype == dns.TypeA {
			for _, ip := range ips.A {
				addr, ok := netip.AddrFromSlice(ip.To4())
				if !ok {
					continue
				}
				msg.Answer = append(msg.Answer, &dns.A{
					Hdr: dns.Header{Name: req.Name, Class: dns.ClassINET, TTL: 60},
					A:   rdata.A{Addr: addr},
				})
			}
			return msg, nil
		}
		for _, ip := range ips.AAAA {
			addr, ok := netip.AddrFromSlice(ip.To16())
			if !ok {
				continue
			}
			msg.Answer = append(msg.Answer, &dns.AAAA{
				Hdr:  dns.Header{Name: req.Name, Class: dns.ClassINET, TTL: 60},
				AAAA: rdata.AAAA{Addr: addr.Unmap()},
			})
		}
		return msg, nil
	default:
		msg.Rcode = dns.RcodeRefused
		return msg, nil
	}
}

func init() { netapi.SetBootstrap(systemDNS{}) }
