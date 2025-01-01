package remotefs

import (
	"context"
	"net/netip"
)

// A Mounter is responsible for mounting a remote filesystem in a local directory or drive letter.
type Mounter interface {
	// Start mounts the remote directory given by mountPoint on the local directory or drive letter
	// given ty clientMountPoint. The podIP and port is the address to the remote FTP or SFTP server.
	// The id is just used for logging purposes.
	Start(ctx context.Context, workload, container, clientMountPoint, mountPoint string, podAddrPort netip.AddrPort, ro bool) error
}
