package iputil

import (
	"fmt"
	"net"
	"net/netip"
)

// Parse is like net.ParseIP but converts an IPv4 in 16 byte form to its 4 byte form.
func Parse(ipStr string) (ip netip.Addr) {
	if ip, err := netip.ParseAddr(ipStr); err == nil {
		return ip.Unmap()
	}
	return
}

// SplitToIPPort splits the given address into an IP and a port number. It's
// an error if the address is based on a hostname rather than an IP.
func SplitToIPPort(netAddr net.Addr) (netip.AddrPort, error) {
	ipAddr, ok := netAddr.(interface{ AddrPort() netip.AddrPort })
	if !ok {
		return netip.AddrPort{}, fmt.Errorf("address %q is not an IP:port address", netAddr)
	}
	return ipAddr.AddrPort(), nil
}
