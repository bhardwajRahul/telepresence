package docker

import (
	"fmt"
	"net/netip"
	"strconv"
	"strings"
)

type PublishedPort struct {
	HostAddrPort  netip.AddrPort
	Protocol      string
	ContainerPort uint16
}

func parsePort(s string) (uint16, error) {
	pn, err := strconv.ParseUint(s, 10, 16)
	if err != nil {
		return 0, fmt.Errorf("%q is not a valid port number", s)
	}
	return uint16(pn), nil
}

func parsePublishedPort(pp string) (PublishedPort, error) {
	pc := PublishedPort{}
	mapping, proto, found := strings.Cut(pp, "/")
	if !found {
		pc.Protocol = "tcp"
	} else {
		pc.Protocol = strings.ToLower(proto)
		if pc.Protocol != "tcp" && pc.Protocol != "udp" {
			return PublishedPort{}, fmt.Errorf("%q is not a valid protocol", proto)
		}
	}

	var hostPort uint16
	if lastColon := strings.LastIndexByte(mapping, ':'); lastColon >= 0 {
		p := mapping[lastColon+1:]
		var err error
		pc.ContainerPort, err = parsePort(p)
		if err != nil {
			return PublishedPort{}, err
		}

		mapping = mapping[:lastColon]
		if strings.ContainsRune(mapping, ':') {
			pc.HostAddrPort, err = netip.ParseAddrPort(mapping)
			if err != nil {
				return PublishedPort{}, err
			}
			return pc, nil
		}
		hostPort, err = parsePort(mapping)
		if err != nil {
			return PublishedPort{}, err
		}
	}
	pc.HostAddrPort = netip.AddrPortFrom(netip.IPv4Unspecified(), hostPort)
	return pc, nil
}

func writePort(sb *strings.Builder, port uint16) {
	sb.WriteString(strconv.FormatUint(uint64(port), 10))
}

func (c PublishedPort) writeTo(sb *strings.Builder) {
	if !c.HostAddrPort.Addr().IsUnspecified() {
		sb.WriteString(c.HostAddrPort.String())
		sb.WriteByte(':')
	} else if c.HostAddrPort.Port() != 0 {
		writePort(sb, c.HostAddrPort.Port())
		sb.WriteByte(':')
	}
	writePort(sb, c.ContainerPort)
	if c.Protocol != "tcp" {
		sb.WriteByte('/')
		sb.WriteString(c.Protocol)
	}
}

func (c PublishedPort) String() string {
	sb := strings.Builder{}
	c.writeTo(&sb)
	return sb.String()
}

type PublishedPorts []PublishedPort

func (p *PublishedPorts) String() string {
	sb := strings.Builder{}
	sb.WriteByte('[')
	for i, config := range *p {
		if i > 0 {
			sb.WriteByte(',')
		}
		config.writeTo(&sb)
		sb.WriteString(config.String())
	}
	sb.WriteByte(']')
	return sb.String()
}

func (p *PublishedPorts) Set(s string) error {
	return p.Append(s)
}

func (p *PublishedPorts) Type() string {
	return "list"
}

func (p *PublishedPorts) Append(s string) error {
	c, err := parsePublishedPort(s)
	if err == nil {
		*p = append(*p, c)
	}
	return err
}

func (p *PublishedPorts) Replace(vals []string) error {
	pcs := make([]PublishedPort, len(vals))
	for i, val := range vals {
		pc, err := parsePublishedPort(val)
		if err != nil {
			return err
		}
		pcs[i] = pc
	}
	*p = pcs
	return nil
}

func (p *PublishedPorts) GetSlice() []string {
	vals := make([]string, len(*p))
	for i, pc := range *p {
		vals[i] = pc.String()
	}
	return vals
}
