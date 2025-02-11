package workload

import (
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

func FromAny(obj any) (k8sapi.Workload, bool) {
	if ro, ok := obj.(runtime.Object); ok {
		if wl, err := k8sapi.WrapWorkload(ro); err == nil {
			return wl, true
		}
	}
	return nil, false
}

func RpcKind(s k8sapi.Kind) manager.WorkloadInfo_Kind {
	switch s {
	case k8sapi.DeploymentKind:
		return manager.WorkloadInfo_DEPLOYMENT
	case k8sapi.ReplicaSetKind:
		return manager.WorkloadInfo_REPLICASET
	case k8sapi.StatefulSetKind:
		return manager.WorkloadInfo_STATEFULSET
	case k8sapi.RolloutKind:
		return manager.WorkloadInfo_ROLLOUT
	default:
		return manager.WorkloadInfo_UNSPECIFIED
	}
}
