package vif

import (
	"context"
	"time"

	"github.com/hashicorp/go-multierror"
	"gvisor.dev/gvisor/pkg/tcpip/stack"

	"github.com/datawire/dlib/dlog"
	"github.com/telepresenceio/telepresence/v2/pkg/routing"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
)

type TunnelingDevice struct {
	stack  *stack.Stack
	Device Device
	Router *Router
	table  routing.Table
}

func NewTunnelingDevice(ctx context.Context, tunnelStreamCreator tunnel.StreamCreator) (*TunnelingDevice, error) {
	routingTable, err := routing.OpenTable(ctx)
	if err != nil {
		return nil, err
	}
	dev, err := OpenTun(ctx)
	if err != nil {
		return nil, err
	}
	ep, err := dev.NewLinkEndpoint()
	if err != nil {
		return nil, err
	}
	netStack, err := NewStack(ctx, ep, tunnelStreamCreator)
	if err != nil {
		return nil, err
	}
	router := NewRouter(dev, routingTable)
	return &TunnelingDevice{
		stack:  netStack,
		Device: dev,
		Router: router,
		table:  routingTable,
	}, nil
}

func (vif *TunnelingDevice) Close(ctx context.Context) error {
	var result error
	vif.stack.Close()
	vif.Router.Close(ctx)
	vif.Device.Close()
	if err := vif.table.Close(ctx); err != nil {
		result = multierror.Append(result, err)
	}
	return result
}

func (vif *TunnelingDevice) Run(ctx context.Context) (err error) {
	defer func() {
		dlog.Debug(ctx, "vif ended")
	}()

	// The stack.Wait gets stuck at times, even though the stack is closed, so we ensure that the
	// Run terminates anyway here.
	// See https://github.com/google/gvisor/issues/11456
	stackDoneCh := make(chan struct{})
	go func() {
		vif.stack.Wait()
		close(stackDoneCh)
	}()
	<-ctx.Done()
	select {
	case <-stackDoneCh:
	case <-time.After(2 * time.Second):
	}
	return nil
}
