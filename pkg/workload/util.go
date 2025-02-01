package workload

import (
	"k8s.io/apimachinery/pkg/runtime"

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
