//go:build docker

package remotefs

import (
	"context"
	"errors"
	"sync"

	"github.com/telepresenceio/go-fuseftp/rpc"
)

// NewFTPMounter returns nil. It's here to satisfy the linker.
func NewFTPMounter(rpc.FuseFTPClient, *sync.WaitGroup) Mounter {
	return nil
}

type fuseFtpMgr struct{}

type FuseFTPManager interface {
	DeferInit(context.Context) error
	GetFuseFTPClient(context.Context) rpc.FuseFTPClient
}

func NewFuseFTPManager() FuseFTPManager {
	return &fuseFtpMgr{}
}

func (s *fuseFtpMgr) DeferInit(context.Context) error {
	return errors.New("fuseftp client is not available")
}

func (s *fuseFtpMgr) GetFuseFTPClient(context.Context) rpc.FuseFTPClient {
	return nil
}
