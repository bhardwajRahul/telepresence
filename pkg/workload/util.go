package workload

import (
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

const (
	DomainPrefix           = "telepresence.getambassador.io/"
	InjectAnnotation       = DomainPrefix + "inject-" + agentconfig.ContainerName
	ServiceNameAnnotation  = DomainPrefix + "inject-service-name"
	ManualInjectAnnotation = DomainPrefix + "manually-injected"
	AnnRestartedAt         = DomainPrefix + "restartedAt"
)

func FromAny(obj any) (k8sapi.Workload, bool) {
	if ro, ok := obj.(runtime.Object); ok {
		if wl, err := k8sapi.WrapWorkload(ro); err == nil {
			return wl, true
		}
	}
	return nil, false
}
