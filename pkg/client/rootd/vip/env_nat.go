package vip

import (
	"context"
	"net/netip"
	"regexp"
	"sort"
)

var (
	ipV4Rx = regexp.MustCompile(`(?:\d{1,3}\.){3}\d{1,3}`)                                                              //nolint:gochecknoglobals // constant
	ipV6Rx = regexp.MustCompile(`(?:[0-9a-fA-F]{0,4}:){1,7}(?:[0-9a-fA-F]{0,4}%[0-9a-zA-Z]+|(?:\d{1,3}\.){3}\d{1,3}|)`) //nolint:gochecknoglobals // constant
)

type LocalIPProvider interface {
	MapsIPv4() bool
	MapsIPv6() bool
	GetLocalIP(ctx context.Context, remoteIP netip.Addr) (netip.Addr, error)
}

func replaceIP(ctx context.Context, provider LocalIPProvider, rx *regexp.Regexp, s string) string {
	return rx.ReplaceAllStringFunc(s, func(s string) string {
		if ip, err := netip.ParseAddr(s); err == nil {
			if rip, err := provider.GetLocalIP(ctx, ip); err == nil {
				return rip.String()
			}
		}
		return s
	})
}

func TranslateEnvironmentIPs(ctx context.Context, env map[string]string, provider LocalIPProvider) {
	ks := make([]string, len(env))
	i := 0
	for k := range env {
		ks[i] = k
		i++
	}
	sort.Strings(ks)
	if provider.MapsIPv4() {
		for _, k := range ks {
			env[k] = replaceIP(ctx, provider, ipV4Rx, env[k])
		}
	}
	if provider.MapsIPv6() {
		for _, k := range ks {
			env[k] = replaceIP(ctx, provider, ipV6Rx, env[k])
		}
	}
}
