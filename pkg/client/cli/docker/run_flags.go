package docker

import (
	"context"
	"fmt"
	"net/netip"
	"strings"

	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"

	"github.com/datawire/dlib/dlog"
	"github.com/telepresenceio/telepresence/v2/pkg/client/cli/flags"
	docker2 "github.com/telepresenceio/telepresence/v2/pkg/client/docker"
)

type RunFlags struct {
	PublishedPorts PublishedPorts // --publish Port mappings that the container will expose on localhost
	Networks       []string
}

func ParseRunFlags(args []string) (*RunFlags, []string, error) {
	f := RunFlags{}
	var v string
	var found bool
	var err error
	for {
		v, found, args, err = flags.ConsumeUnparsedValue("publish", 'p', false, args)
		if err != nil {
			return nil, nil, err
		}
		if !found {
			break
		}
		var pp PublishedPort
		pp, err = parsePublishedPort(v)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid port format for --publish: %w", err)
		}
		f.PublishedPorts = append(f.PublishedPorts, pp)
	}
	for {
		v, found, args, err = flags.ConsumeUnparsedValue("expose", 0, false, args)
		if err != nil {
			return nil, nil, err
		}
		if !found {
			break
		}
		// Convert --expose values to --publish values
		if strings.Contains(v, ":") {
			return nil, nil, fmt.Errorf("invalid port format for --expose: %s", v)
		}
		proto, portRange := nat.SplitProtoPort(v)
		start, end, err := nat.ParsePortRange(portRange)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid argument for --expose: %s, error: %s", v, err)
		}
		if start != end {
			return nil, nil, fmt.Errorf("invalid argument for --expose: %s, error: a range not supported", v)
		}
		port := uint16(start)
		f.PublishedPorts = append(f.PublishedPorts, PublishedPort{
			HostAddrPort:  netip.AddrPortFrom(netip.IPv4Unspecified(), port),
			Protocol:      proto,
			ContainerPort: port,
		})
	}
	for {
		v, found, args, err = flags.ConsumeUnparsedValue("network", 0, false, args)
		if err != nil {
			return nil, nil, err
		}
		if !found {
			break
		}
		f.Networks = append(f.Networks, v)
	}
	return &f, args, nil
}

func ConnectNetworksToDaemon(ctx context.Context, networks []string, daemonName string) (context.CancelFunc, error) {
	cancel := func() {}
	if len(networks) == 0 {
		return cancel, nil
	}

	cli, err := docker2.GetClient(ctx)
	if err != nil {
		return cancel, err
	}

	var ds []string
	cancel = func() {
		disconnectDaemons(ctx, cli, ds, daemonName)
	}
	for _, n := range networks {
		connected, err := connectDaemon(ctx, cli, n, daemonName)
		if err != nil {
			return cancel, err
		}
		if connected {
			ds = append(ds, n)
		}
	}
	return cancel, nil
}

// connectDaemon connects the given network to the containerized daemon. It will
// return false if the daemon already had this network attached.
func connectDaemon(ctx context.Context, cli *client.Client, network, daemonName string) (bool, error) {
	dlog.Debugf(ctx, "Connecting network %s to container %s", network, daemonName)
	if err := cli.NetworkConnect(ctx, network, daemonName, nil); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return false, nil
		}
		return false, fmt.Errorf("failed to connect network %s to container %s: %v", network, daemonName, err)
	}
	return true, nil
}

// disconnectDaemons disconnects the given networks from the containerized daemon.
func disconnectDaemons(ctx context.Context, cli *client.Client, networks []string, daemonName string) {
	ctx = context.WithoutCancel(ctx)
	for _, n := range networks {
		dlog.Debugf(ctx, "Disconnecting network %s from container %s", n, daemonName)
		err := cli.NetworkDisconnect(ctx, n, daemonName, false)
		if err != nil {
			dlog.Warnf(ctx, "failed to disconnect network %s from daemon: %v", n, err)
		}
	}
}
