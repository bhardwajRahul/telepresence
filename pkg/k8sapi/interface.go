package k8sapi

import (
	"k8s.io/client-go/kubernetes"

	argoRollouts "github.com/datawire/argo-rollouts-go-client/pkg/client/clientset/versioned"
	typedArgoRollouts "github.com/datawire/argo-rollouts-go-client/pkg/client/clientset/versioned/typed/rollouts/v1alpha1"
)

type JoinedClientSetInterface interface {
	kubernetes.Interface
	argoRollouts.Interface
}

type joinedClientSetInterface struct {
	kubernetes.Interface
	ari argoRollouts.Interface
}

func (j joinedClientSetInterface) ArgoprojV1alpha1() typedArgoRollouts.ArgoprojV1alpha1Interface {
	return j.ari.ArgoprojV1alpha1()
}
