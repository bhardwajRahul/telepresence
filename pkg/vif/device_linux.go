package vif

import (
	"context"
	"fmt"
	"net/netip"
	"unsafe"

	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"
	"gvisor.dev/gvisor/pkg/tcpip/link/fdbased"
	"gvisor.dev/gvisor/pkg/tcpip/stack"

	"github.com/telepresenceio/telepresence/v2/pkg/subnet"
)

const devicePath = "/dev/net/tun"

type device struct {
	fd             int
	name           string
	interfaceIndex uint32
}

func openTun(_ context.Context) (*device, error) {
	// https://www.kernel.org/doc/html/latest/networking/tuntap.html

	fd, err := unix.Open(devicePath, unix.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open TUN device %s: %w", devicePath, err)
	}
	unix.CloseOnExec(fd)
	defer func() {
		if err != nil {
			_ = unix.Close(fd)
		}
	}()

	ifr, err := unix.NewIfreq("tel%d")
	ifr.SetUint16(unix.IFF_TUN | unix.IFF_NO_PI)

	err = unix.IoctlSetInt(fd, unix.TUNSETIFF, int(uintptr(unsafe.Pointer(ifr))))
	if err != nil {
		return nil, fmt.Errorf("failed to set TUN device flags: %w", err)
	}

	// Retrieve the name that was generated based on the "tel%d" template.
	name := ifr.Name()

	// Set non-blocking so that ReadPacket() doesn't hang for several seconds when the
	// fd is Closed. ReadPacket() will still wait for data to arrive.
	//
	// See: https://github.com/golang/go/issues/30426#issuecomment-470044803
	_ = unix.SetNonblock(fd, true)

	var index uint32
	err = withSocket(unix.AF_INET, func(provisioningSocket int) error {
		// Bring the device up. This is how it's done in ifconfig.
		if err = ioctl(provisioningSocket, unix.SIOCGIFFLAGS, unsafe.Pointer(ifr)); err != nil {
			return fmt.Errorf("failed to get flags for %s: %w", name, err)
		}

		ifr.SetUint16(ifr.Uint16() | unix.IFF_UP | unix.IFF_RUNNING)
		if err = ioctl(provisioningSocket, unix.SIOCSIFFLAGS, unsafe.Pointer(ifr)); err != nil {
			return fmt.Errorf("failed to set flags for %s: %w", name, err)
		}

		if err = ioctl(provisioningSocket, unix.SIOCGIFINDEX, unsafe.Pointer(ifr)); err != nil {
			return fmt.Errorf("get interface index on %s failed: %w", name, err)
		}
		index = ifr.Uint32()
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &device{fd: fd, name: name, interfaceIndex: index}, nil
}

func (d *device) addSubnet(_ context.Context, pfx netip.Prefix) error {
	link, err := netlink.LinkByIndex(int(d.interfaceIndex))
	if err != nil {
		return fmt.Errorf("failed to find link for interface %s: %w", d.name, err)
	}
	addr := &netlink.Addr{IPNet: subnet.PrefixToIPNet(pfx)}
	if err := netlink.AddrAdd(link, addr); err != nil {
		return fmt.Errorf("failed to add address %s to interface %s: %w", pfx, d.name, err)
	}
	return nil
}

func (d *device) removeSubnet(ctx context.Context, pfx netip.Prefix) error {
	link, err := netlink.LinkByIndex(int(d.interfaceIndex))
	if err != nil {
		return err
	}
	addr := &netlink.Addr{IPNet: subnet.PrefixToIPNet(pfx)}
	return netlink.AddrDel(link, addr)
}

func (d *device) index() uint32 {
	return d.interfaceIndex
}

func (d *device) getMTU() (mtu uint32, err error) {
	err = withSocket(unix.AF_INET, func(fd int) error {
		ifr, err := unix.NewIfreq(d.name)
		if err == nil {
			err = ioctl(fd, unix.SIOCGIFMTU, unsafe.Pointer(ifr))
			if err == nil {
				mtu = ifr.Uint32()
			}
		}
		return err
	})
	return mtu, err
}

func (d *device) createLinkEndpoint() (stack.LinkEndpoint, error) {
	mtu, err := d.getMTU()
	if err != nil {
		return nil, err
	}
	return fdbased.New(&fdbased.Options{
		FDs:                []int{d.fd},
		MTU:                mtu,
		PacketDispatchMode: fdbased.RecvMMsg,
	})
}

func (d *device) Close() {
	_ = unix.Close(d.fd)
}

func (d *device) WaitForDevice() {
}
