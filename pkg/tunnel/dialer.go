package tunnel

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/datawire/dlib/dlog"
	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/ipproto"
)

// The idleDuration controls how long a dialer for a specific proto+from-to address combination remains alive without
// reading or writing any messages. The dialer is normally closed by one of the peers.
const (
	tcpConnTTL = 2 * time.Hour // Default tcp_keepalive_time on Linux
	udpConnTTL = 1 * time.Minute
)

const (
	notConnected = int32(iota)
	connecting
	connected
)

// streamReader is implemented by the dialer and udpListener so that they can share the
// readLoop function.
type streamReader interface {
	Idle() <-chan time.Time
	ResetIdle() bool
	Stop(context.Context)
	getStream() Stream
	reply([]byte) (int, error)
	startDisconnect(context.Context, string)
}

// The dialer takes care of dispatching messages between gRPC and UDP connections.
type dialer struct {
	TimedHandler
	stream    Stream
	cancel    context.CancelFunc
	conn      net.Conn
	connected int32
	done      chan struct{}

	ingressBytesProbe *CounterProbe
	egressBytesProbe  *CounterProbe
}

// NewDialer creates a new handler that dispatches messages in both directions between the given gRPC stream
// and the given connection.
func NewDialer(
	stream Stream,
	cancel context.CancelFunc,
	ingressBytesProbe, egressBytesProbe *CounterProbe,
) Endpoint {
	return NewConnEndpoint(stream, nil, cancel, ingressBytesProbe, egressBytesProbe)
}

// NewDialerTTL creates a new handler that dispatches messages in both directions between the given gRPC stream
// and the given connection. The TTL decides how long the connection can be idle before it's closed.
//
// The handler remains active until it's been idle for the ttl duration, at which time it will automatically close
// and call the release function it got from the tunnel.Pool to ensure that it gets properly released.
func NewDialerTTL(stream Stream, cancel context.CancelFunc, ttl time.Duration, ingressBytesProbe, egressBytesProbe *CounterProbe) Endpoint {
	return NewConnEndpointTTL(stream, nil, cancel, ttl, ingressBytesProbe, egressBytesProbe)
}

func NewConnEndpoint(stream Stream, conn net.Conn, cancel context.CancelFunc, ingressBytesProbe, egressBytesProbe *CounterProbe) Endpoint {
	ttl := tcpConnTTL
	if stream.ID().Protocol() == ipproto.UDP {
		ttl = udpConnTTL
	}
	return NewConnEndpointTTL(stream, conn, cancel, ttl, ingressBytesProbe, egressBytesProbe)
}

func NewConnEndpointTTL(
	stream Stream,
	conn net.Conn,
	cancel context.CancelFunc,
	ttl time.Duration,
	ingressBytesProbe, egressBytesProbe *CounterProbe,
) Endpoint {
	state := notConnected
	if conn != nil {
		state = connecting
	}
	return &dialer{
		TimedHandler: NewTimedHandler(stream.ID(), ttl, nil),
		stream:       stream,
		cancel:       cancel,
		conn:         conn,
		connected:    state,
		done:         make(chan struct{}),

		ingressBytesProbe: ingressBytesProbe,
		egressBytesProbe:  egressBytesProbe,
	}
}

func (h *dialer) Start(ctx context.Context) {
	go func() {
		defer close(h.done)

		id := h.stream.ID()
		tag := h.stream.Tag()

		switch h.connected {
		case notConnected:
			// Set up the idle timer to close and release this handler when it's been idle for a while.
			h.connected = connecting

			dlog.Tracef(ctx, "   %s %s, dialing", tag, id)
			d := net.Dialer{Timeout: h.stream.DialTimeout()}
			conn, err := d.DialContext(ctx, id.DestinationProtocolString(), id.Destination().String())
			if err != nil {
				dlog.Errorf(ctx, "!> %s %s, failed to establish connection: %v", tag, id, err)
				if err = h.stream.Send(ctx, NewMessage(DialReject, nil)); err != nil {
					dlog.Errorf(ctx, "!> %s %s, failed to send DialReject: %v", tag, id, err)
				}
				if err = h.stream.CloseSend(ctx); err != nil {
					dlog.Errorf(ctx, "!> %s %s, stream.CloseSend failed: %v", tag, id, err)
				}
				h.connected = notConnected
				return
			}
			if err = h.stream.Send(ctx, NewMessage(DialOK, nil)); err != nil {
				_ = conn.Close()
				dlog.Errorf(ctx, "!> %s %s, failed to send DialOK: %v", tag, id, err)
				return
			}
			dlog.Tracef(ctx, "<- %s %s, dial answered", tag, id)
			h.conn = conn

		case connecting:
		default:
			dlog.Errorf(ctx, "!! %s %s, start called in invalid state", tag, id)
			return
		}

		// Set up the idle timer to close and release this endpoint when it's been idle for a while.
		h.TimedHandler.Start(ctx)
		h.connected = connected

		wg := sync.WaitGroup{}
		wg.Add(2)
		go h.connToStreamLoop(ctx, &wg)
		go h.streamToConnLoop(ctx, &wg)
		wg.Wait()
		h.Stop(ctx)
	}()
}

func (h *dialer) Done() <-chan struct{} {
	return h.done
}

// Stop will close the underlying TCP/UDP connection.
func (h *dialer) Stop(ctx context.Context) {
	h.startDisconnect(ctx, "explicit close")
	h.cancel()
}

func (h *dialer) startDisconnect(ctx context.Context, reason string) {
	if !atomic.CompareAndSwapInt32(&h.connected, connected, notConnected) {
		return
	}
	id := h.stream.ID()
	dlog.Tracef(ctx, "   CONN %s closing connection: %s", id, reason)
	if err := h.conn.Close(); err != nil {
		dlog.Tracef(ctx, "!! CONN %s, Close failed: %v", id, err)
	}
}

func (h *dialer) connToStreamLoop(ctx context.Context, wg *sync.WaitGroup) {
	var endReason string
	endLevel := dlog.LogLevelTrace
	id := h.stream.ID()
	tag := h.stream.Tag()

	outgoing := make(chan Message, 50)
	defer func() {
		if !h.ResetIdle() {
			// Hard close of peer. We don't want any more data
			select {
			case outgoing <- NewMessage(Disconnect, nil):
			default:
			}
		}
		close(outgoing)
		dlog.Logf(ctx, endLevel, "<- %s %s conn-to-stream loop ended because %s", tag, id, endReason)
		wg.Done()
	}()

	wg.Add(1)
	WriteLoop(ctx, h.stream, outgoing, wg, h.egressBytesProbe)

	buf := make([]byte, 0x100000)
	dlog.Tracef(ctx, "-> %s %s conn-to-stream loop started", tag, id)
	for {
		n, err := h.conn.Read(buf)
		if n > 0 {
			dlog.Tracef(ctx, "-> %s %s, read len %d from conn", tag, id, n)
			select {
			case <-ctx.Done():
				endReason = ctx.Err().Error()
				return
			case outgoing <- NewMessage(Normal, buf[:n]):
			}
		}

		if err != nil {
			switch {
			case errors.Is(err, io.EOF):
				endReason = "EOF was encountered"
			case errors.Is(err, net.ErrClosed):
				endReason = "the connection was closed"
				h.startDisconnect(ctx, endReason)
			case strings.Contains(err.Error(), "connection aborted"):
				endReason = "the connection was aborted"
			default:
				endReason = fmt.Sprintf("a read error occurred: %T %v", err, err)
				endLevel = dlog.LogLevelError
			}
			return
		}

		if !h.ResetIdle() {
			endReason = "it was idle for too long"
			return
		}
	}
}

func (h *dialer) getStream() Stream {
	return h.stream
}

func (h *dialer) reply(data []byte) (int, error) {
	return h.conn.Write(data)
}

func (h *dialer) streamToConnLoop(ctx context.Context, wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
	}()
	readLoop(ctx, h.stream.Tag(), h, h.ingressBytesProbe)
}

func handleControl(ctx context.Context, h streamReader, cm Message) {
	switch cm.Code() {
	case DialReject, Disconnect: // Peer wants to hard-close. No more messages will arrive
		h.Stop(ctx)
	case KeepAlive:
		h.ResetIdle()
	case DialOK:
		// So how can a dialer get a DialOK from a peer? Surely, there cannot be a dialer at both ends?
		// Well, the story goes like this:
		// 1. A request to the service is made on the workstation.
		// 2. This agent's listener receives a connection.
		// 3. Since an intercept is active, the agent creates a tunnel to the workstation
		// 4. A new dialer is attached to that tunnel (reused as a tunnel endpoint)
		// 5. The dialer at the workstation dials and responds with DialOK, and here we are.
	default:
		dlog.Errorf(ctx, "!! CONN %s: unhandled connection control message: %s", h.getStream().ID(), cm)
	}
}

func readLoop(ctx context.Context, tag Tag, h streamReader, trafficProbe *CounterProbe) {
	var endReason string
	endLevel := dlog.LogLevelTrace
	id := h.getStream().ID()
	defer func() {
		h.startDisconnect(ctx, endReason)
		dlog.Logf(ctx, endLevel, "<- %s %s stream-to-conn loop ended because %s", tag, id, endReason)
	}()

	incoming, errCh := ReadLoop(ctx, h.getStream(), trafficProbe)
	dlog.Tracef(ctx, "<- %s %s stream-to-conn loop started", tag, id)
	for {
		select {
		case <-ctx.Done():
			endReason = ctx.Err().Error()
			return
		case <-h.Idle():
			endReason = "it was idle for too long"
			return
		case err, ok := <-errCh:
			if ok {
				dlog.Error(ctx, err)
			}
		case dg, ok := <-incoming:
			if !ok {
				// h.incoming was closed by the reader and is now drained.
				endReason = "there was no more input"
				return
			}
			if !h.ResetIdle() {
				endReason = "it was idle for too long"
				return
			}
			if dg.Code() != Normal {
				handleControl(ctx, h, dg)
				continue
			}
			payload := dg.Payload()
			pn := len(payload)
			for n := 0; n < pn; {
				wn, err := h.reply(payload[n:])
				if err != nil {
					endReason = fmt.Sprintf("a write error occurred: %v", err)
					endLevel = dlog.LogLevelError
					return
				}
				dlog.Tracef(ctx, "<- %s %s, len %d", tag, id, wn)
				n += wn
			}
		}
	}
}

// DialWaitLoop reads from the given dialStream. A new goroutine that creates a Tunnel to the manager and then
// attaches a dialer Endpoint to that tunnel is spawned for each request that arrives. The method blocks until
// the dialStream is closed.
func DialWaitLoop(
	ctx context.Context,
	tag Tag,
	tunnelProvider Provider,
	dialStream rpc.Manager_WatchDialClient,
	sessionID SessionID,
) error {
	// create ctx to cleanup leftover dialRespond if waitloop dies
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for ctx.Err() == nil {
		dr, err := dialStream.Recv()
		if err == nil {
			go dialRespond(ctx, tag, tunnelProvider, dr, sessionID)
			continue
		}
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || status.Code(err) == codes.NotFound {
			return nil
		}
		switch status.Code(err) {
		case codes.NotFound, codes.Unavailable:
			return nil
		}
		return fmt.Errorf("dial request stream recv: %w", err)
	}
	return nil
}

func dialRespond(ctx context.Context, tag Tag, tunnelProvider Provider, dr *rpc.DialRequest, sessionID SessionID) {
	id := ConnID(dr.ConnId)
	ctx, cancel := context.WithCancel(ctx)
	mt, err := tunnelProvider.Tunnel(ctx)
	if err != nil {
		dlog.Errorf(ctx, "!! %s %s, call to manager Tunnel failed: %v", tag, id, err)
		cancel()
		return
	}
	s, err := NewClientStream(ctx, tag, mt, id, sessionID, time.Duration(dr.RoundtripLatency), time.Duration(dr.DialTimeout))
	if err != nil {
		dlog.Error(ctx, err)
		cancel()
		return
	}
	d := NewDialer(s, cancel, nil, nil)
	d.Start(ctx)
	<-d.Done()
}
