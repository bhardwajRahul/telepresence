package docker

import (
	"context"
	"fmt"
	"time"

	"github.com/docker/docker/api/types/container"

	"github.com/datawire/dlib/dlog"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
)

func StopContainer(ctx context.Context, nameOrID string) error {
	cli, err := GetClient(ctx)
	if err != nil {
		return err
	}
	opts := container.StopOptions{}
	timeout := client.GetConfig(ctx).Timeouts().Get(client.TimeoutContainerShutdown)
	if timeout > 0 {
		secs := int(timeout / time.Second)
		opts.Timeout = &secs
		dlog.Debugf(ctx, "Stopping container %s with a grace period of %d seconds", nameOrID, secs)
	} else {
		dlog.Debugf(ctx, "Stopping container %s with default grace period", nameOrID)
	}
	err = cli.ContainerStop(ctx, nameOrID, opts)
	if err != nil {
		return fmt.Errorf("failed to stop container %s: %v", nameOrID, err)
	}
	dlog.Debugf(ctx, "Container %s stopped", nameOrID)
	return nil
}
