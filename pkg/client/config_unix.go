//go:build !windows

package client

import "net/netip"

// defaultVirtualSubnet A randomly chosen class E subnet.
var defaultVirtualSubnet = netip.MustParsePrefix("246.246.0.0/16") //nolint:gochecknoglobals // constant

type OSSpecificConfig struct{}

func GetDefaultOSSpecificConfig() OSSpecificConfig {
	return OSSpecificConfig{}
}

func (c *OSSpecificConfig) Merge(o *OSSpecificConfig) {
}
