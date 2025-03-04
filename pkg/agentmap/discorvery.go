package agentmap

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"sort"

	core "k8s.io/api/core/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	apps "k8s.io/client-go/informers/apps/v1"

	argorollouts "github.com/datawire/argo-rollouts-go-client/pkg/client/informers/externalversions/rollouts/v1alpha1"
	"github.com/datawire/dlib/dlog"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/informer"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

var ReplicaSetNameRx = regexp.MustCompile(`\A(.+)-[a-f0-9]+\z`)

type WorkloadOwnerNotFoundError struct {
	*k8sErrors.StatusError
}

func FindOwnerWorkload(ctx context.Context, obj k8sapi.Object, supportedWorkloadKinds k8sapi.Kinds) (k8sapi.Workload, error) {
	dlog.Tracef(ctx, "FindOwnerWorkload(%s,%s,%s)", obj.GetName(), obj.GetNamespace(), obj.GetKind())
	lbs := obj.GetLabels()
	if wlName, ok := lbs[agentconfig.WorkloadNameLabel]; ok {
		kind, ok := lbs[agentconfig.WorkloadKindLabel]
		if ok && !supportedWorkloadKinds.Contains(k8sapi.Kind(kind)) {
			return nil, fmt.Errorf("unable to find %s owner for %s.%s (annotation controlled)",
				kind, obj.GetName(), obj.GetNamespace())
		}
		return GetWorkload(ctx, wlName, obj.GetNamespace(), k8sapi.Kind(kind))
	}
	refs := obj.GetOwnerReferences()
	ns := obj.GetNamespace()
	for i := range refs {
		if or := &refs[i]; or.Controller != nil && *or.Controller {
			kind := k8sapi.Kind(or.Kind)
			if kind == k8sapi.ReplicaSetKind && supportedWorkloadKinds.Contains(k8sapi.DeploymentKind) {
				// Try the common case first. Strip replicaset's generated hash and try to
				// get the deployment. If this succeeds, we have saved us a replicaset
				// lookup.
				if m := ReplicaSetNameRx.FindStringSubmatch(or.Name); m != nil {
					if wl, err := GetWorkload(ctx, m[1], ns, k8sapi.DeploymentKind); err == nil {
						return wl, nil
					}
				}
			}
			if supportedWorkloadKinds.Contains(kind) {
				wl, err := GetWorkload(ctx, or.Name, ns, kind)
				if err != nil {
					return nil, err
				}
				return FindOwnerWorkload(ctx, wl, supportedWorkloadKinds)
			}
			// A controller owner of unsupported workload kind is treated as "no owner".
			break
		}
	}
	if wl, ok := obj.(k8sapi.Workload); ok {
		return wl, nil
	}
	return nil, &WorkloadOwnerNotFoundError{StatusError: k8sErrors.NewNotFound(
		obj.GetGroupResource(), fmt.Sprintf("%s.%s", obj.GetName(), obj.GetNamespace()))}
}

func GetWorkload(ctx context.Context, name, namespace string, workloadKind k8sapi.Kind) (obj k8sapi.Workload, err error) {
	dlog.Tracef(ctx, "GetWorkload(%s,%s,%s)", name, namespace, workloadKind)
	i := informer.GetFactory(ctx, namespace)
	if i == nil {
		dlog.Debugf(ctx, "fetching %s %s.%s using direct API call", workloadKind, name, namespace)
		return k8sapi.GetWorkload(ctx, name, namespace, workloadKind)
	}
	ai, ri := i.GetK8sInformerFactory().Apps().V1(), i.GetArgoRolloutsInformerFactory().Argoproj().V1alpha1().Rollouts()
	return getWorkload(ai, ri, name, namespace, workloadKind)
}

func getWorkload(ai apps.Interface, ri argorollouts.RolloutInformer, name, namespace string, kind k8sapi.Kind) (obj k8sapi.Workload, err error) {
	switch kind {
	case k8sapi.DeploymentKind:
		return getDeployment(ai, name, namespace)
	case k8sapi.ReplicaSetKind:
		return getReplicaSet(ai, name, namespace)
	case k8sapi.StatefulSetKind:
		return getStatefulSet(ai, name, namespace)
	case k8sapi.RolloutKind:
		return getRollout(ri, name, namespace)
	case "":
		for _, wk := range k8sapi.KnownWorkloadKinds {
			if obj, err = getWorkload(ai, ri, name, namespace, wk); err == nil {
				return obj, nil
			}
			if !k8sErrors.IsNotFound(err) {
				return nil, err
			}
		}
		return nil, k8sErrors.NewNotFound(core.Resource("workload"), name+"."+namespace)
	default:
		return nil, k8sapi.UnsupportedWorkloadKindError(kind)
	}
}

func getDeployment(ai apps.Interface, name, namespace string) (wl k8sapi.Workload, err error) {
	dep, err := ai.Deployments().Lister().Deployments(namespace).Get(name)
	if err != nil {
		return nil, err
	}
	return k8sapi.Deployment(dep), nil
}

func getRollout(ri argorollouts.RolloutInformer, name, namespace string) (wl k8sapi.Workload, err error) {
	if ri == nil {
		return nil, k8sapi.UnsupportedWorkloadKindError("Rollout")
	}
	rollout, err := ri.Lister().Rollouts(namespace).Get(name)
	if err != nil {
		return nil, err
	}
	return k8sapi.Rollout(rollout), nil
}

func getReplicaSet(ai apps.Interface, name, namespace string) (k8sapi.Workload, error) {
	rs, err := ai.ReplicaSets().Lister().ReplicaSets(namespace).Get(name)
	if err != nil {
		return nil, err
	}
	return k8sapi.ReplicaSet(rs), nil
}

func getStatefulSet(ai apps.Interface, name, namespace string) (k8sapi.Workload, error) {
	ss, err := ai.StatefulSets().Lister().StatefulSets(namespace).Get(name)
	if err != nil {
		return nil, err
	}
	return k8sapi.StatefulSet(ss), nil
}

func FindServicesForPod(ctx context.Context, pod *core.PodTemplateSpec, svcName string) ([]k8sapi.Object, error) {
	switch {
	case svcName != "":
		var svc *core.Service
		var err error
		if f := informer.GetK8sFactory(ctx, pod.Namespace); f != nil {
			svc, err = f.Core().V1().Services().Lister().Services(pod.Namespace).Get(svcName)
		} else {
			// This shouldn't happen really.
			dlog.Debugf(ctx, "fetching service %s.%s using direct API call", svcName, pod.Namespace)
			svc, err = k8sapi.GetK8sInterface(ctx).CoreV1().Services(pod.Namespace).Get(ctx, svcName, meta.GetOptions{})
		}
		if err != nil {
			if k8sErrors.IsNotFound(err) {
				return nil, fmt.Errorf(
					"unable to find service %s specified by annotation %s declared in pod %s.%s",
					svcName, ServiceNameAnnotation, pod.Name, pod.Namespace)
			}
			return nil, err
		}
		return []k8sapi.Object{k8sapi.Service(svc)}, nil
	case len(pod.Labels) > 0:
		return findServicesSelecting(ctx, pod.Namespace, labels.Set(pod.Labels))
	default:
		return nil, fmt.Errorf("unable to find a service using pod %s.%s because it has no labels", pod.Name, pod.Namespace)
	}
}

type objectsStringer []k8sapi.Object

func (os objectsStringer) String() string {
	b := bytes.Buffer{}
	l := len(os)
	if l == 0 {
		return "no services"
	}
	for i, o := range os {
		if i > 0 {
			if l != 2 {
				b.WriteString(", ")
			}
			if i == l-1 {
				b.WriteString(" and ")
			}
		}
		b.WriteString(o.GetName())
	}
	return b.String()
}

// findServicesSelecting finds all services that has a selector that matches the given labels.
func findServicesSelecting(ctx context.Context, namespace string, lbs labels.Labels) ([]k8sapi.Object, error) {
	var ms []k8sapi.Object
	var scanned int
	if f := informer.GetK8sFactory(ctx, namespace); f != nil {
		ss, err := f.Core().V1().Services().Lister().Services(namespace).List(labels.Everything())
		if err != nil {
			return nil, err
		}
		scanned = len(ss)
		for _, s := range ss {
			sel := s.Spec.Selector
			if len(sel) > 0 && labels.SelectorFromValidatedSet(sel).Matches(lbs) {
				ms = append(ms, k8sapi.Service(s))
			}
		}
	} else {
		// This shouldn't happen really.
		dlog.Tracef(ctx, "Fetching services in %s using direct API call", namespace)
		l, err := k8sapi.GetK8sInterface(ctx).CoreV1().Services(namespace).List(ctx, meta.ListOptions{})
		if err != nil {
			return nil, err
		}
		items := l.Items
		scanned = len(items)
		for i := range items {
			s := &items[i]
			sel := s.Spec.Selector
			if len(sel) > 0 && labels.SelectorFromValidatedSet(sel).Matches(lbs) {
				ms = append(ms, k8sapi.Service(s))
			}
		}
	}
	// Ensure predictable order of found services
	sort.Slice(ms, func(i, j int) bool {
		return ms[i].GetName() < ms[j].GetName()
	})
	dlog.Tracef(ctx, "Scanned %d services in namespace %s and found that %s selects labels %v", scanned, namespace, objectsStringer(ms), lbs)
	return ms, nil
}

// findContainerMatchingPort finds the container that matches the given ServicePort. The match is
// made using Protocol, and the Name or the ContainerPort field of each port in each container
// depending on if  the service port is symbolic or numeric. The first container with a matching
// port is returned along with the index of the container port that matched.
//
// The first container with no ports at all is returned together with a port index of -1, in case
// no port match could be made and the service port is numeric. This enables intercepts of containers
// that indeed do listen a port but lack a matching port description in the manifest, which is what
// you get if you do:
//
//	kubectl create deploy my-deploy --image my-image
//	kubectl expose deploy my-deploy --port 80 --target-port 8080
func findContainerMatchingPort(port *core.ServicePort, cns []core.Container) (*core.Container, int) {
	// The protocol of the targetPort must match the protocol of the containerPort because it is
	// not illegal to listen with both TCP and UDP on the same port.
	proto := core.ProtocolTCP
	if port.Protocol != "" {
		proto = port.Protocol
	}
	protoEqual := func(p core.Protocol) bool {
		return p == proto || p == "" && proto == core.ProtocolTCP
	}

	if port.TargetPort.Type == intstr.String {
		portName := port.TargetPort.StrVal
		for ci := range cns {
			cn := &cns[ci]
			for pi := range cn.Ports {
				p := &cn.Ports[pi]
				if p.Name == portName && protoEqual(p.Protocol) {
					return cn, pi
				}
			}
		}
	} else {
		portNum := port.TargetPort.IntVal
		if portNum == 0 {
			// The targetPort default is the value of the port field.
			portNum = port.Port
		}
		for ci := range cns {
			cn := &cns[ci]
			for pi := range cn.Ports {
				p := &cn.Ports[pi]
				if p.ContainerPort == portNum && protoEqual(p.Protocol) {
					return cn, pi
				}
			}
		}
		// As a last resort, also consider containers that don't expose their ports at all. Those
		// containers match all ports because it's unknown what they might be listening to.
		for ci := range cns {
			cn := &cns[ci]
			if len(cn.Ports) == 0 {
				return cn, -1
			}
		}
	}
	return nil, 0
}

// IsPodRunning returns true if at least one container has state Running and a non-zero StartedAt.
func IsPodRunning(pod *core.Pod) bool {
	for _, cn := range pod.Status.ContainerStatuses {
		if r := cn.State.Running; r != nil && !r.StartedAt.IsZero() {
			// At least one container is running.
			return true
		}
	}
	return false
}

// AgentContainer returns the pod's traffic-agent container, or nil if the pod doesn't have a traffic-agent.
func AgentContainer(pod *core.Pod) *core.Container {
	return containerByName(agentconfig.ContainerName, pod.Spec.Containers)
}

// InitContainer returns the pod's tel-agent-init init-container, or nil if the pod doesn't have a tel-agent-init.
func InitContainer(pod *core.Pod) *core.Container {
	return containerByName(agentconfig.InitContainerName, pod.Spec.InitContainers)
}

func containerByName(name string, cns []core.Container) *core.Container {
	for i := range cns {
		cn := &cns[i]
		if cn.Name == name {
			return cn
		}
	}
	return nil
}
