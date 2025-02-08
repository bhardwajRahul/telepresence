package server

import (
	"context"
	"net"
	"runtime"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"google.golang.org/grpc"

	"github.com/datawire/dlib/dcontext"
	"github.com/datawire/dlib/dlog"
)

type mergedCtx struct {
	context.Context
	valCtx context.Context
}

func (m *mergedCtx) Value(i any) any {
	if v := m.valCtx.Value(i); v != nil {
		return v
	}
	return m.Context.Value(i)
}

type mergedStream struct {
	grpc.ServerStream
	valCtx context.Context
}

func (s *mergedStream) Context() context.Context {
	return &mergedCtx{Context: s.ServerStream.Context(), valCtx: s.valCtx}
}

// New creates a gRPC server which has no service registered and has not started to accept requests yet. Values
// in the provided context will be included in the context passed to both unary and stream calls.
func New(valCtx context.Context, options ...grpc.ServerOption) *grpc.Server {
	requestCount := uint64(0)
	unaryInterceptor := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return handler(&mergedCtx{Context: ctx, valCtx: callCtx(valCtx, info.FullMethod, &requestCount)}, req)
	}
	streamInterceptor := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(srv, &mergedStream{
			ServerStream: ss,
			valCtx:       callCtx(valCtx, info.FullMethod, &requestCount),
		})
	}
	if dlog.MaxLogLevel(valCtx) >= dlog.LogLevelDebug {
		opts := []logging.Option{
			logging.WithLogOnEvents(logging.StartCall, logging.FinishCall),
			// Add any other option (check functions starting with logging.With).
		}
		options = append(
			options,
			grpc.ChainUnaryInterceptor(
				unaryInterceptor,
				logging.UnaryServerInterceptor(interceptorLogger(), opts...),
			),
			grpc.ChainStreamInterceptor(
				streamInterceptor,
				logging.StreamServerInterceptor(interceptorLogger(), opts...),
			),
		)
	} else {
		options = append(options, grpc.UnaryInterceptor(unaryInterceptor), grpc.StreamInterceptor(streamInterceptor))
	}
	return grpc.NewServer(options...)
}

// Serve accepts incoming connections on the listener lis, creating a new ServerTransport and service goroutine for each.
// The service goroutines read gRPC requests and then call the registered handlers to reply to them. Serve returns when
// lis.Accept fails with fatal errors.
//
// Serve waits until ctx.Done is closed. The svc.GracefulStop function will be called if the context has soft-cancel
// enabled. The svc.Stop function will be called if no soft-cancel is enabled or when the GracefulStop doesn't finish
// until the hard context is done.
func Serve(ctx context.Context, svc *grpc.Server, lis net.Listener) error {
	dlog.Debug(ctx, "gRPC server started")
	go Wait(ctx, svc)
	err := svc.Serve(lis)
	if err != nil {
		dlog.Errorf(ctx, "gRPC server ended with error: %v", err)
	} else {
		dlog.Debug(ctx, "gRPC server ended")
	}
	return err
}

// Stop the service by calling svc.Stop and give it maxTime to complete before returning.
// If the maxTime expires and the current debug loglevel >= debug, then a stack trace of all goroutines will
// be logged.
func Stop(ctx context.Context, svc *grpc.Server, maxTime time.Duration) {
	dead := make(chan struct{})
	dlog.Debug(ctx, "Initiating hard shutdown")
	go func() {
		defer close(dead)
		svc.Stop()
		dlog.Debug(ctx, "Hard shutdown complete")
	}()
	select {
	case <-dead:
	case <-time.After(maxTime):
		// Hard shutdown is stuck! This shouldn't happen, and we need to find out why
		if dlog.MaxLogLevel(ctx) >= dlog.LogLevelDebug {
			buf := make([]byte, 1024*256)
			n := runtime.Stack(buf, true)
			dlog.Debug(ctx, string(buf[:n]))
		}
	}
}

// Wait waits until the given contexts Done channel is closed. The server's GracefulStop function will be called
// if the context has soft-cancel enabled. The server's Stop function will be called if no soft-cancel is enabled or
// when the GracefulStop doesn't finish until the Done channel of the hard context closed.
func Wait(ctx context.Context, svc *grpc.Server) {
	hardCtx := dcontext.HardContext(ctx)
	if hardCtx != ctx {
		<-ctx.Done()
		dead := make(chan struct{})
		go func() {
			dlog.Debug(ctx, "Initiating soft shutdown")
			svc.GracefulStop()
			close(dead)
			dlog.Debug(ctx, "Soft shutdown complete")
		}()
		select {
		case <-dead:
			// GracefulStop did the job.
		case <-hardCtx.Done():
			Stop(ctx, svc, 5*time.Second)
		}
	} else {
		Stop(ctx, svc, 5*time.Second)
	}
}
