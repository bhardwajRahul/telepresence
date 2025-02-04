package vif

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"runtime"
	"slices"

	"github.com/datawire/dlib/dlog"
	"github.com/telepresenceio/telepresence/v2/pkg/errcat"
	"github.com/telepresenceio/telepresence/v2/pkg/routing"
	"github.com/telepresenceio/telepresence/v2/pkg/subnet"
)

type Router struct {
	// The vif device that packets will be routed through
	device Device
	// The routing table that will be used to route packets
	routingTable routing.Table
	// A list of never proxied routes that have already been added to routing table
	staticOverrides []*routing.Route
	// The subnets that are currently being routed
	routedSubnets []netip.Prefix
	// The subnets that are allowed to be routed even in the presence of conflicting routes
	whitelistedSubnets []netip.Prefix
}

func NewRouter(device Device, table routing.Table) *Router {
	return &Router{device: device, routingTable: table}
}

func (rt *Router) GetRoutedSubnets() []netip.Prefix {
	return rt.routedSubnets
}

func (rt *Router) UpdateWhitelist(whitelist []netip.Prefix) {
	rt.whitelistedSubnets = whitelist
}

func (rt *Router) ValidateRoutes(ctx context.Context, routes []netip.Prefix) error {
	// We need the entire table because we need to check for any overlaps, not just "is this IP already routed"
	table, err := routing.GetRoutingTable(ctx)
	if err != nil {
		return err
	}

	nonWhitelisted := slices.DeleteFunc(slices.Clone(routes), func(r netip.Prefix) bool {
		for _, w := range rt.whitelistedSubnets {
			if subnet.Covers(w, r) {
				return true
			}
		}
		for _, er := range table {
			if r == er.RoutedNet && er.Interface.Name == rt.device.Name() {
				// Route is already in the routing table.
				return true
			}
		}
		return false
	})

	// Slightly awkward nested loops, since they can both continue (i.e., there are probably wasted iterations), but it's
	// okay, there's not going to be hundreds of routes.
	// In any case, we really wanna run over the table as the outer loop, since it's bigger.
	for _, tr := range table {
		dlog.Tracef(ctx, "checking for overlap with route %q", tr)
		if (tr.RoutedNet.Bits() == 0 || tr.Default) || // Default route, overlapped if needed
			subnet.IsHalfOfDefault(tr.RoutedNet) || // OpenVPN covers half the address space with a /1 route and the other half with another. This is its way of doing a default route.
			tr.Interface.Name == rt.device.Name() { // This is the interface we're routing through, so we can overlap it
			continue
		}
		for _, r := range nonWhitelisted {
			if tr.RoutedNet.Overlaps(r) {
				return errcat.Config.New(fmt.Sprintf(
					"subnet %s overlaps with existing route %q. Please see %s for more information",
					r, tr, "https://www.telepresence.io/docs/reference/vpn",
				))
			}
		}
	}
	return nil
}

func (rt *Router) UpdateRoutes(ctx context.Context, pleaseProxy, dontProxy, dontProxyOverrides []netip.Prefix) error {
	// Remove all current static routes so that they don't affect the routes for subnets
	// that we're about to add.
	rt.dropStaticOverrides(ctx)

	// Remove all no longer desired subnets from the routedSubnets
	var removed []netip.Prefix
	rt.routedSubnets, removed = subnet.Partition(rt.routedSubnets, func(_ int, sn netip.Prefix) bool {
		for _, d := range pleaseProxy {
			if sn == d {
				return true
			}
		}
		return false
	})

	// Remove already routed subnets from the pleaseProxy list
	added := slices.DeleteFunc(pleaseProxy, func(sn netip.Prefix) bool {
		for _, d := range rt.routedSubnets {
			if sn == d {
				return true
			}
		}
		return false
	})

	// Add pleaseProxy subnets to the currently routed subnets
	rt.routedSubnets = append(rt.routedSubnets, added...)

	for _, sn := range removed {
		if err := rt.device.RemoveSubnet(ctx, sn); err != nil {
			dlog.Errorf(ctx, "failed to remove subnet %s: %v", sn, err)
		}
	}

	var staticNets []netip.Prefix
	for _, sn := range added {
		var err error
		bits := sn.Bits()
		if sn.Addr().Is4() && bits > 30 {
			staticNets = append(staticNets, sn)
			continue
		}

		// On linux, this adds a link, so it's still relevant after adding a static route.
		if err = rt.device.AddSubnet(ctx, sn); err != nil {
			dlog.Errorf(ctx, "failed to add subnet %s: %v", sn, err)
			continue
		}

		if runtime.GOOS == "linux" {
			// On linux, we use static routes for conflicting subnets, because those subnets will then belong
			// to our own routing table.
			if slices.ContainsFunc(rt.whitelistedSubnets, func(r netip.Prefix) bool { return r.Overlaps(sn) }) {
				dlog.Debugf(ctx, "Using static route for %s because it is an override", sn)
				staticNets = append(staticNets, sn)
			}
		}
	}
	return rt.addStaticOverrides(ctx, dontProxy, dontProxyOverrides, staticNets)
}

func (rt *Router) addStaticOverrides(ctx context.Context, neverProxy, neverProxyOverrides, staticNets []netip.Prefix) (err error) {
	desired := make([]*routing.Route, 0, len(neverProxy)+len(neverProxyOverrides))
	dr, err := routing.DefaultRoute(ctx)
	if err != nil {
		return err
	}
	for _, sn := range neverProxy {
		// All subnets in neverProxy have been verified as being routed by the TUN-device, so we
		// route them to the default route instead.
		desired = append(desired, &routing.Route{
			LocalIP:   dr.LocalIP,
			RoutedNet: sn,
			Interface: dr.Interface,
			Gateway:   dr.Gateway,
			Default:   false,
		})
	}

	for _, sn := range neverProxyOverrides {
		r, err := routing.GetRoute(ctx, sn)
		if err != nil {
			dlog.Error(ctx, err)
		} else {
			desired = append(desired, &routing.Route{
				LocalIP:   r.LocalIP,
				RoutedNet: sn,
				Interface: r.Interface,
				Gateway:   r.Gateway,
				Default:   r.Default,
			})
		}
	}

	if len(staticNets) > 0 {
		ifd, err := net.InterfaceByIndex(int(rt.device.Index()))
		if err != nil {
			return err
		}
		var pr *routing.Route
		if dr.Interface.Index == ifd.Index {
			pr = dr
		} else {
			addrs, err := ifd.Addrs()
			if err != nil {
				return err
			}
			for _, addr := range addrs {
				pfx, err := netip.ParsePrefix(addr.String())
				if err != nil {
					return err
				}
				pr, err = routing.GetRoute(ctx, pfx)
				if err != nil {
					return err
				}
				if pr.Gateway.IsValid() {
					break
				}
			}
			if pr == nil {
				pr = &routing.Route{
					Interface: ifd,
				}
			}
		}

		for _, sn := range staticNets {
			desired = append(desired, &routing.Route{
				LocalIP:   pr.LocalIP,
				Gateway:   pr.Gateway,
				RoutedNet: sn,
				Interface: ifd,
			})
		}
	}

	for _, r := range desired {
		dlog.Debugf(ctx, "Adding static route %s", r)
		if err = rt.routingTable.Add(ctx, r); err != nil {
			dlog.Errorf(ctx, "failed to add static route %s: %v", r, err)
		}
	}
	rt.staticOverrides = desired
	return nil
}

func (rt *Router) dropStaticOverrides(ctx context.Context) {
	// Remove all current static routes so that they don't affect the routes for subnets
	// that we're about to add.
	for _, c := range rt.staticOverrides {
		if err := rt.routingTable.Remove(ctx, c); err != nil {
			dlog.Errorf(ctx, "failed to remove static route %s: %v", c, err)
		}
	}
	rt.staticOverrides = nil
}

func (rt *Router) Close(ctx context.Context) {
	for _, sn := range rt.routedSubnets {
		if err := rt.device.RemoveSubnet(ctx, sn); err != nil {
			dlog.Errorf(ctx, "failed to remove subnet %s: %v", sn, err)
		}
	}
	rt.dropStaticOverrides(ctx)
}
