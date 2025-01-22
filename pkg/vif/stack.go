package vif

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"time"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv6"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/icmp"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"gvisor.dev/gvisor/pkg/tcpip/transport/udp"
	"gvisor.dev/gvisor/pkg/waiter"

	"github.com/datawire/dlib/dlog"
	"github.com/telepresenceio/telepresence/v2/pkg/iputil"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
)

func NewStack(ctx context.Context, ep stack.LinkEndpoint, streamCreator tunnel.StreamCreator) (*stack.Stack, error) {
	s := stack.New(stack.Options{
		NetworkProtocols: []stack.NetworkProtocolFactory{
			ipv4.NewProtocol,
			ipv6.NewProtocol,
		},
		TransportProtocols: []stack.TransportProtocolFactory{
			icmp.NewProtocol4,
			icmp.NewProtocol6,
			tcp.NewProtocol,
			udp.NewProtocol,
		},
		HandleLocal: false,
	})
	if err := setDefaultOptions(s); err != nil {
		return nil, err
	}
	_, err := setNIC(s, ep)
	if err != nil {
		return nil, err
	}
	setTCPHandler(ctx, s, streamCreator)
	setUDPHandler(ctx, s, streamCreator)
	return s, nil
}

const (
	myWindowScale    = 6
	maxReceiveWindow = 4 << (myWindowScale + 14) // 4MiB
)

// maxInFlight specifies the max number of in-flight connection attempts.
const maxInFlight = 16

// keepAliveIdle is used as the very first alive interval. Subsequent intervals
// use keepAliveInterval.
const keepAliveIdle = 60 * time.Second

// keepAliveInterval is the interval between sending keep-alive packets.
const keepAliveInterval = 30 * time.Second

// keepAliveCount is the max number of keep-alive probes that can be sent
// before the connection is killed due to lack of response.
const keepAliveCount = 9

type idStringer stack.TransportEndpointID

func (i idStringer) String() string {
	return fmt.Sprintf("%s -> %s",
		iputil.JoinIpPort(i.RemoteAddress.AsSlice(), i.RemotePort),
		iputil.JoinIpPort(i.LocalAddress.AsSlice(), i.LocalPort))
}

func setDefaultOptions(s *stack.Stack) error {
	sa := tcpip.TCPSACKEnabled(true)
	if err := s.SetTransportProtocolOption(tcp.ProtocolNumber, &sa); err != nil {
		return fmt.Errorf("SetTransportProtocolOption(tcp, TCPSACKEnabled(%t): %s", sa, err)
	}

	if runtime.GOOS == "windows" {
		// Windows w/RACK performs poorly. ACKs do not appear to be handled in a
		// timely manner, leading to spurious retransmissions and a reduced
		// congestion window.
		tr := tcpip.TCPRecovery(0)
		if err := s.SetTransportProtocolOption(tcp.ProtocolNumber, &tr); err != nil {
			return fmt.Errorf("SetTransportProtocolOption(tcp, TCPRecovery(%d): %s", tr, err)
		}
	}

	// Enable Receive Buffer Auto-Tuning, see:
	// https://github.com/google/gvisor/issues/1666
	mo := tcpip.TCPModerateReceiveBufferOption(true)
	if err := s.SetTransportProtocolOption(tcp.ProtocolNumber, &mo); err != nil {
		return fmt.Errorf("SetTransportProtocolOption(tcp, TCPModerateReceiveBufferOption(%t): %s", mo, err)
	}

	cco := tcpip.CongestionControlOption("cubic")
	if err := s.SetTransportProtocolOption(tcp.ProtocolNumber, &cco); err != nil {
		return fmt.Errorf("SetTransportProtocolOption(tcp, CongestionControlOption(%s): %s", cco, err)
	}

	if err := s.SetForwardingDefaultAndAllNICs(ipv4.ProtocolNumber, true); err != nil {
		return fmt.Errorf("SetForwardingDefaultAndAllNICs(ipv4, %t): %s", true, err)
	}
	if err := s.SetForwardingDefaultAndAllNICs(ipv6.ProtocolNumber, true); err != nil {
		return fmt.Errorf("SetForwardingDefaultAndAllNICs(ipv6, %t): %s", true, err)
	}
	ttl := tcpip.DefaultTTLOption(64)
	if err := s.SetNetworkProtocolOption(ipv4.ProtocolNumber, &ttl); err != nil {
		return fmt.Errorf("SetDefaultTTL(ipv4, %d): %s", ttl, err)
	}
	if err := s.SetNetworkProtocolOption(ipv6.ProtocolNumber, &ttl); err != nil {
		return fmt.Errorf("SetDefaultTTL(ipv6, %d): %s", ttl, err)
	}
	return nil
}

func setNIC(s *stack.Stack, ep stack.LinkEndpoint) (tcpip.NICID, error) {
	nicID := s.NextNICID()
	if tcpErr := s.CreateNIC(nicID, ep); tcpErr != nil {
		return 0, fmt.Errorf("create NIC failed: %s", tcpErr)
	}
	if err := s.SetPromiscuousMode(nicID, true); err != nil {
		return 0, fmt.Errorf("SetPromiscuousMode(%d, %t): %s", nicID, true, err)
	}
	if err := s.SetSpoofing(nicID, true); err != nil {
		return 0, fmt.Errorf("SetSpoofing(%d, %t): %s", nicID, true, err)
	}
	s.SetRouteTable([]tcpip.Route{
		{
			Destination: header.IPv4EmptySubnet,
			NIC:         nicID,
		},
	})
	return nicID, nil
}

func forwardTCP(ctx context.Context, streamCreator tunnel.StreamCreator, fr *tcp.ForwarderRequest) {
	var ep tcpip.Endpoint
	var err tcpip.Error
	id := fr.ID()
	defer func() {
		if err != nil {
			msg := fmt.Sprintf("forward TCP %s: %s", idStringer(id), err)
			dlog.Error(ctx, msg)
		}
	}()

	var wq waiter.Queue
	if ep, err = fr.CreateEndpoint(&wq); err != nil {
		fr.Complete(true)
		dlog.Error(ctx, err)
		return
	}
	fr.Complete(false)

	so := ep.SocketOptions()
	so.SetKeepAlive(true)

	idle := tcpip.KeepaliveIdleOption(keepAliveIdle)
	if err = ep.SetSockOpt(&idle); err != nil {
		dlog.Error(ctx, err)
		return
	}

	ivl := tcpip.KeepaliveIntervalOption(keepAliveInterval)
	if err = ep.SetSockOpt(&ivl); err != nil {
		dlog.Error(ctx, err)
		return
	}

	if err = ep.SetSockOptInt(tcpip.KeepaliveCountOption, keepAliveCount); err != nil {
		dlog.Error(ctx, err)
		return
	}
	dispatchToStream(ctx, newConnID(header.TCPProtocolNumber, id), gonet.NewTCPConn(&wq, ep), streamCreator)
}

func setTCPHandler(ctx context.Context, s *stack.Stack, streamCreator tunnel.StreamCreator) {
	f := tcp.NewForwarder(s, maxReceiveWindow, maxInFlight, func(fr *tcp.ForwarderRequest) {
		forwardTCP(ctx, streamCreator, fr)
	})
	s.SetTransportProtocolHandler(tcp.ProtocolNumber, f.HandlePacket)
}

var blockedUDPPorts = map[uint16]bool{ //nolint:gochecknoglobals // constant
	137: true, // NETBIOS Name Service
	138: true, // NETBIOS Datagram Service
	139: true, // NETBIOS
}

func forwardUDP(ctx context.Context, streamCreator tunnel.StreamCreator, fr *udp.ForwarderRequest) {
	id := fr.ID()
	if _, ok := blockedUDPPorts[id.LocalPort]; ok {
		return
	}

	wq := waiter.Queue{}
	ep, err := fr.CreateEndpoint(&wq)
	if err != nil {
		msg := fmt.Sprintf("forward UDP %s: %s", idStringer(id), err)
		dlog.Error(ctx, msg)
		return
	}
	dispatchToStream(ctx, newConnID(udp.ProtocolNumber, id), gonet.NewUDPConn(&wq, ep), streamCreator)
}

func setUDPHandler(ctx context.Context, s *stack.Stack, streamCreator tunnel.StreamCreator) {
	f := udp.NewForwarder(s, func(fr *udp.ForwarderRequest) {
		forwardUDP(ctx, streamCreator, fr)
	})
	s.SetTransportProtocolHandler(udp.ProtocolNumber, f.HandlePacket)
}

func newConnID(proto tcpip.TransportProtocolNumber, id stack.TransportEndpointID) tunnel.ConnID {
	return tunnel.NewConnID(int(proto), id.RemoteAddress.AsSlice(), id.LocalAddress.AsSlice(), id.RemotePort, id.LocalPort)
}

func dispatchToStream(ctx context.Context, id tunnel.ConnID, conn net.Conn, streamCreator tunnel.StreamCreator) {
	ctx, cancel := context.WithCancel(ctx)
	stream, err := streamCreator(ctx, id)
	if err != nil {
		dlog.Errorf(ctx, "forward %s: %s", id, err)
		cancel()
		return
	}
	ep := tunnel.NewConnEndpoint(stream, conn, cancel, nil, nil)
	ep.Start(ctx)
}
