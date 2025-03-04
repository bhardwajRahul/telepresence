package informer

import (
	"context"

	"github.com/puzpuzpuz/xsync/v3"
	"k8s.io/client-go/informers"

	argorolloutsinformer "github.com/datawire/argo-rollouts-go-client/pkg/client/informers/externalversions"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

type factoryKey struct{}

func getOpts(ns string) (k8sOpts []informers.SharedInformerOption, argoOpts []argorolloutsinformer.SharedInformerOption) {
	if ns != "" {
		k8sOpts = append(k8sOpts, informers.WithNamespace(ns))
		argoOpts = append(argoOpts, argorolloutsinformer.WithNamespace(ns))
	}

	return k8sOpts, argoOpts
}

func WithFactory(ctx context.Context, ns string) context.Context {
	if _, ok := ctx.Value(factoryKey{}).(*xsync.MapOf[string, GlobalFactory]); !ok {
		ctx = context.WithValue(ctx, factoryKey{}, xsync.NewMapOf[string, GlobalFactory]())
		if ns == "" {
			// The cluster wide informer must be created when it is requested as the initial informer because it will act as a
			// proxy for all other requested informers.
			GetFactory(ctx, ns)
		}
	}
	return ctx
}

func GetFactory(ctx context.Context, ns string) GlobalFactory {
	fm, ok := ctx.Value(factoryKey{}).(*xsync.MapOf[string, GlobalFactory])
	if !ok {
		return nil
	}
	gf, _ := fm.LoadOrCompute(ns, func() GlobalFactory {
		if ns != "" {
			// Return the cluster wide factory if one exists.
			if cw, ok := fm.Load(""); ok {
				return cw
			}
		}
		k8sOpts, argoOpts := getOpts(ns)
		i := k8sapi.GetJoinedClientSetInterface(ctx)
		k8sFactory := informers.NewSharedInformerFactoryWithOptions(i, 0, k8sOpts...)
		argoRolloutFactory := argorolloutsinformer.NewSharedInformerFactoryWithOptions(i, 0, argoOpts...)
		return NewDefaultGlobalFactory(k8sFactory, argoRolloutFactory)
	})
	return gf
}

func GetK8sFactory(ctx context.Context, ns string) informers.SharedInformerFactory {
	f := GetFactory(ctx, ns)
	if f != nil {
		return f.GetK8sInformerFactory()
	}
	return nil
}

func GetArgoRolloutsFactory(ctx context.Context, ns string) argorolloutsinformer.SharedInformerFactory {
	f := GetFactory(ctx, ns)
	if f != nil {
		return f.GetArgoRolloutsInformerFactory()
	}
	return nil
}
