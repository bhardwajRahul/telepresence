package k8sclient

import (
	"context"

	"k8s.io/client-go/rest"
)

type k8sConfigKey struct{}

func GetK8sConfig(ctx context.Context) *rest.Config {
	if c, ok := ctx.Value(k8sConfigKey{}).(*rest.Config); ok {
		return c
	}
	return nil
}

func WithK8sConfig(ctx context.Context, k8sConfig *rest.Config) context.Context {
	return context.WithValue(ctx, k8sConfigKey{}, k8sConfig)
}
