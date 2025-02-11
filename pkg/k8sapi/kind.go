package k8sapi

import "slices"

type Kind string

const (
	ServiceKind     Kind = "Service"
	PodKind         Kind = "Pod"
	DeploymentKind  Kind = "Deployment"
	StatefulSetKind Kind = "StatefulSet"
	ReplicaSetKind  Kind = "ReplicaSet"
	RolloutKind     Kind = "Rollout"
)

type Kinds []Kind

func (k Kinds) Contains(kind Kind) bool {
	return slices.Contains(k, kind)
}

var (
	KnownKinds         = Kinds{ServiceKind, PodKind, DeploymentKind, StatefulSetKind, ReplicaSetKind, RolloutKind} //nolint:gochecknoglobals // constant
	KnownWorkloadKinds = Kinds{DeploymentKind, ReplicaSetKind, StatefulSetKind, RolloutKind}                       //nolint:gochecknoglobals // constant
)

func (w Kind) IsValid() bool {
	return KnownKinds.Contains(w)
}
