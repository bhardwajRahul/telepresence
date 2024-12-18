package workload

import (
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"

	argorollouts "github.com/datawire/argo-rollouts-go-client/pkg/apis/rollouts/v1alpha1"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

type State int

const (
	StateUnknown State = iota
	StateProgressing
	StateAvailable
	StateFailure
)

func deploymentState(d *appsv1.Deployment) State {
	conds := d.Status.Conditions
	sort.Slice(conds, func(i, j int) bool {
		return conds[i].LastTransitionTime.Compare(conds[j].LastTransitionTime.Time) > 0
	})
	for _, c := range conds {
		switch c.Type {
		case appsv1.DeploymentProgressing:
			if c.Status == core.ConditionTrue {
				return StateProgressing
			}
		case appsv1.DeploymentAvailable:
			if c.Status == core.ConditionTrue {
				return StateAvailable
			}
		case appsv1.DeploymentReplicaFailure:
			if c.Status == core.ConditionTrue {
				return StateFailure
			}
		}
	}
	if len(conds) == 0 {
		return StateProgressing
	}
	return StateUnknown
}

func replicaSetState(d *appsv1.ReplicaSet) State {
	for _, c := range d.Status.Conditions {
		if c.Type == appsv1.ReplicaSetReplicaFailure && c.Status == core.ConditionTrue {
			return StateFailure
		}
	}
	return StateAvailable
}

func statefulSetState(_ *appsv1.StatefulSet) State {
	return StateAvailable
}

func rolloutSetState(r *argorollouts.Rollout) State {
	conds := r.Status.Conditions
	sort.Slice(conds, func(i, j int) bool {
		return conds[i].LastTransitionTime.Compare(conds[j].LastTransitionTime.Time) > 0
	})
	for _, c := range conds {
		switch c.Type {
		case argorollouts.RolloutProgressing:
			if c.Status == core.ConditionTrue {
				return StateProgressing
			}
		case argorollouts.RolloutAvailable:
			if c.Status == core.ConditionTrue {
				return StateAvailable
			}
		case argorollouts.RolloutReplicaFailure:
			if c.Status == core.ConditionTrue {
				return StateFailure
			}
		}
	}
	if len(conds) == 0 {
		return StateProgressing
	}
	return StateUnknown
}

func (ws State) String() string {
	switch ws {
	case StateProgressing:
		return "Progressing"
	case StateAvailable:
		return "Available"
	case StateFailure:
		return "Failure"
	default:
		return "Unknown"
	}
}

func GetWorkloadState(wl k8sapi.Workload) State {
	if d, ok := k8sapi.DeploymentImpl(wl); ok {
		return deploymentState(d)
	}
	if r, ok := k8sapi.ReplicaSetImpl(wl); ok {
		return replicaSetState(r)
	}
	if s, ok := k8sapi.StatefulSetImpl(wl); ok {
		return statefulSetState(s)
	}
	if rt, ok := k8sapi.RolloutImpl(wl); ok {
		return rolloutSetState(rt)
	}
	return StateUnknown
}

func StateFromRPC(s manager.WorkloadInfo_State) State {
	switch s {
	case manager.WorkloadInfo_AVAILABLE:
		return StateAvailable
	case manager.WorkloadInfo_FAILURE:
		return StateFailure
	case manager.WorkloadInfo_PROGRESSING:
		return StateProgressing
	default:
		return StateUnknown
	}
}
