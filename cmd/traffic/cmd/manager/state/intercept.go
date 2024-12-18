package state

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	core "k8s.io/api/core/v1"
	events "k8s.io/api/events/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/datawire/dlib/derror"
	"github.com/datawire/dlib/dlog"
	managerrpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/managerutil"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/mutator"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/agentmap"
	"github.com/telepresenceio/telepresence/v2/pkg/errcat"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

// PrepareIntercept ensures that the given request can be matched against the intercept configuration of
// the workload that it references. It returns a PreparedIntercept where all intercepted ports have been
// qualified with a container port and if applicable, with service name and a service port name.
//
// The first step is to find the requested Workload and the agent config for that workload. This step will
// create the initial ConfigMap for the namespace if it doesn't exist yet, and also generate the actual
// intercept config if it doesn't exist.
//
// The second step matches all PortIdentifiers in the request to the intercepts of the agent config.
//
// It's expected that the client that makes the call will update any unqualified port identifiers
// with the ones in the returned PreparedIntercept.
func (s *state) PrepareIntercept(
	ctx context.Context,
	cr *managerrpc.CreateInterceptRequest,
) (pi *managerrpc.PreparedIntercept, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = derror.PanicToError(r)
			dlog.Errorf(ctx, "%+v", err)
		}
	}()

	interceptError := func(err error) (*managerrpc.PreparedIntercept, error) {
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		dlog.Errorf(ctx, "PrepareIntercept error %v", err)
		return &managerrpc.PreparedIntercept{Error: err.Error(), ErrorCategory: int32(errcat.GetCategory(err))}, nil
	}

	spec := cr.InterceptSpec
	wl, err := agentmap.GetWorkload(ctx, spec.Agent, spec.Namespace, spec.WorkloadKind)
	if err != nil {
		if k8sErrors.IsNotFound(err) {
			err = errcat.User.New(err)
		}
		dlog.Error(ctx, err)
		return interceptError(err)
	}

	ac, _, err := s.ensureAgent(ctx, wl, s.isExtended(spec), spec)
	if err != nil {
		return interceptError(err)
	}
	cn, ic, err := findIntercept(ac, spec)
	if err != nil {
		return interceptError(err)
	}
	return &managerrpc.PreparedIntercept{
		Namespace:       ac.Namespace,
		ServiceUid:      string(ic.ServiceUID),
		ServiceName:     ic.ServiceName,
		ServicePortName: ic.ServicePortName,
		ContainerName:   cn.Name,
		Protocol:        string(ic.Protocol),
		ContainerPort:   int32(ic.ContainerPort),
		ServicePort:     int32(ic.ServicePort),
		AgentImage:      ac.AgentImage,
		WorkloadKind:    ac.WorkloadKind,
	}, nil
}

func (s *state) EnsureAgent(ctx context.Context, n, ns string) (as []*managerrpc.AgentInfo, err error) {
	var wl k8sapi.Workload
	wl, err = agentmap.GetWorkload(ctx, n, ns, "")
	if err != nil {
		if k8sErrors.IsNotFound(err) {
			err = errcat.User.New(err)
		}
		return nil, err
	}
	_, as, err = s.ensureAgent(ctx, wl, false, nil)
	return as, err
}

func (s *state) ValidateCreateAgent(context.Context, k8sapi.Workload, agentconfig.SidecarExt) error {
	return nil
}

// sortAgents will sort the given AgentInfo based on pod name.
func sortAgents(as []*managerrpc.AgentInfo) {
	sort.Slice(as, func(i, j int) bool {
		return as[i].PodName < as[j].PodName
	})
}

func (s *state) ensureAgent(parentCtx context.Context, wl k8sapi.Workload, extended bool, spec *managerrpc.InterceptSpec) (
	ac *agentconfig.Sidecar, as []*managerrpc.AgentInfo, err error,
) {
	if agentmap.TrafficManagerSelector.Matches(labels.Set(wl.GetLabels())) {
		msg := fmt.Sprintf("deployment %s.%s is the Telepresence Traffic Manager. It can not have a traffic-agent", wl.GetName(), wl.GetNamespace())
		dlog.Error(parentCtx, msg)
		return nil, nil, status.Error(codes.FailedPrecondition, msg)
	}

	if !managerutil.AgentInjectorEnabled(parentCtx) {
		sce, err := mutator.GetMap(parentCtx).Get(parentCtx, wl.GetName(), wl.GetNamespace())
		if err != nil {
			return nil, nil, err
		}
		if sce != nil {
			ac = sce.AgentConfig()
			am := s.agents.LoadAllMatching(func(_ string, ai *managerrpc.AgentInfo) bool {
				return ai.Name == ac.AgentName && ai.Namespace == ac.Namespace
			})
			as = make([]*managerrpc.AgentInfo, len(am))
			i := 0
			for _, found := range am {
				as[i] = found
				i++
			}
			sortAgents(as)
			return ac, as, nil
		}
		msg := fmt.Sprintf("agent-injector is disabled and no agent has been added manually for %s.%s", wl.GetName(), wl.GetNamespace())
		return nil, nil, status.Error(codes.FailedPrecondition, msg)
	}
	ctx, cancel := context.WithTimeout(parentCtx, managerutil.GetEnv(parentCtx).AgentArrivalTimeout)
	defer cancel()

	failedCreateCh, err := watchFailedInjectionEvents(ctx, wl.GetName(), wl.GetNamespace())
	if err != nil {
		return nil, nil, err
	}

	sce, err := s.getOrCreateAgentConfig(ctx, wl, extended, spec, false)
	if err != nil {
		return nil, nil, err
	}
	ac = sce.AgentConfig()
	if as, err = s.waitForAgents(ctx, ac.AgentName, ac.Namespace, failedCreateCh); err != nil {
		// If no agent arrives, then drop its entry from the configmap. This ensures that there
		// are no false positives the next time an intercept is attempted.
		if dropErr := s.dropAgentConfig(parentCtx, wl); dropErr != nil {
			dlog.Errorf(ctx, "failed to remove configmap entry for %s.%s: %v", wl.GetName(), wl.GetNamespace(), dropErr)
		}
		return nil, nil, err
	}
	sortAgents(as)
	return ac, as, nil
}

func (s *state) isExtended(spec *managerrpc.InterceptSpec) bool {
	return spec.Mechanism != "tcp"
}

func (s *state) ValidateAgentImage(agentImage string, extended bool) (err error) {
	if agentImage == "" {
		err = errcat.User.Newf(
			"intercepts are disabled because the traffic-manager is unable to determine what image to use for injected traffic-agents.")
	} else if extended {
		err = errcat.User.New("traffic-manager does not support intercepts that require an extended traffic-agent")
	}
	return err
}

func (s *state) dropAgentConfig(
	ctx context.Context,
	wl k8sapi.Workload,
) error {
	return mutator.GetMap(ctx).Delete(ctx, wl.GetName(), wl.GetNamespace())
}

func (s *state) RestoreAppContainer(ctx context.Context, ii *managerrpc.InterceptInfo) (err error) {
	dlog.Debugf(ctx, "Restoring app container for %s", ii.Id)
	spec := ii.Spec
	n := spec.Agent
	ns := spec.Namespace
	mm := mutator.GetMap(ctx)
	return mm.Update(ctx, ns, func(cm *core.ConfigMap) (changed bool, err error) {
		y, ok := cm.Data[n]
		if !ok {
			return false, nil
		}
		sce, err := unmarshalConfigMapEntry(y, n, ns)
		if err != nil {
			return false, err
		}
		cn, _, err := findIntercept(sce.AgentConfig(), spec)
		if !(err == nil && cn.Replace) {
			return false, nil
		}
		cn.Replace = false

		// The pods for this workload will be killed once the new updated sidecar
		// reaches the configmap. We remove them now, so that they don't continue to
		// review intercepts.
		for sessionID, ai := range s.getAgentsByName(n, ns) {
			if as, ok := s.GetSession(sessionID).(*agentSessionState); ok {
				as.active.Store(false)
			}
			mm.Blacklist(ai.PodName, ns)
		}
		return updateSidecar(sce, cm, n)
	})
}

func updateSidecar(sce agentconfig.SidecarExt, cm *core.ConfigMap, n string) (bool, error) {
	yml, err := sce.Marshal()
	if err != nil {
		return false, err
	}
	oldYaml := cm.Data[n]
	newYaml := string(yml)
	if oldYaml != newYaml {
		cm.Data[n] = newYaml
		return true, nil
	}
	return false, nil
}

func (s *state) waitForAgentDepartures(ctx context.Context, wl k8sapi.Workload) error {
	filter := func(s string, info *managerrpc.AgentInfo) bool {
		return info.Kind == wl.GetKind() && info.Name == wl.GetName() && info.Namespace == wl.GetNamespace()
	}
	if len(s.agents.LoadAllMatching(filter)) == 0 {
		return nil
	}
	dlog.Debugf(ctx, "Waiting for deleted %s.%s agents to depart", wl.GetName(), wl.GetNamespace())
	agCh := s.agents.SubscribeSubset(ctx, filter)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case as, ok := <-agCh:
			if ok && len(as.State) > 0 {
				continue
			}
		}
		return nil
	}
}

func (s *state) GetOrGenerateAgentConfig(ctx context.Context, name, namespace string) (agentconfig.SidecarExt, error) {
	wl, err := agentmap.GetWorkload(ctx, name, namespace, "")
	if err != nil {
		code := codes.Internal
		if k8sErrors.IsNotFound(err) {
			code = codes.NotFound
		}
		return nil, status.Error(code, err.Error())
	}
	return s.getOrCreateAgentConfig(ctx, wl, false, nil, true)
}

func (s *state) getOrCreateAgentConfig(
	ctx context.Context,
	wl k8sapi.Workload,
	extended bool,
	spec *managerrpc.InterceptSpec,
	dryRun bool,
) (sce agentconfig.SidecarExt, err error) {
	enabled, err := checkInterceptAnnotations(wl)
	if err != nil {
		return nil, err
	}
	if !enabled {
		return nil, errcat.User.Newf("%s %s.%s is not interceptable", wl.GetKind(), wl.GetName(), wl.GetNamespace())
	}

	agentImage := managerutil.GetAgentImage(ctx)
	if err = s.self.ValidateAgentImage(agentImage, extended); err != nil {
		return nil, err
	}
	err = mutator.GetMap(ctx).Update(ctx, wl.GetNamespace(), func(cm *core.ConfigMap) (changed bool, err error) {
		doUpdate := false
		y, cmFound := cm.Data[wl.GetName()]
		if cmFound {
			if sce, err = unmarshalConfigMapEntry(y, wl.GetName(), wl.GetNamespace()); err != nil {
				return false, err
			}
			ac := sce.AgentConfig()
			// If the agentImage has changed, and the extended image is requested, then update
			if ac.AgentImage != agentImage && extended {
				ac.AgentImage = agentImage
				doUpdate = true
			}
		} else {
			if cm.Data == nil {
				cm.Data = make(map[string]string)
			}
			var gc agentmap.GeneratorConfig
			if gc, err = agentmap.GeneratorConfigFunc(agentImage); err != nil {
				return false, err
			}
			if sce, err = gc.Generate(ctx, wl, nil); err != nil {
				return false, err
			}

			// If we don't have an entry for the workload in the config-map, then all current agents for that
			// workload are invalid, and we'll have to wait for them to be removed.
			if err = s.waitForAgentDepartures(ctx, wl); err != nil {
				return false, err
			}
			doUpdate = true
		}

		ac := sce.AgentConfig()
		if spec != nil {
			cn, _, err := findIntercept(ac, spec)
			if err != nil {
				return false, err
			}
			if cn.Replace != agentconfig.ReplacePolicy(spec.Replace) {
				cn.Replace = agentconfig.ReplacePolicy(spec.Replace)
				doUpdate = true
			}
		}
		if dryRun {
			return false, nil
		}

		if doUpdate {
			if cmFound {
				// The pods for this workload be killed once the new updated sidecar
				// reaches the configmap. We remove them now, so that they don't continue to
				// review intercepts.
				for sessionID := range s.getAgentsByName(wl.GetName(), wl.GetNamespace()) {
					if as, ok := s.GetSession(sessionID).(*agentSessionState); ok {
						as.active.Store(false)
					}
				}
			} else {
				if err = s.self.ValidateCreateAgent(ctx, wl, sce); err != nil {
					return false, err
				}
			}
			return updateSidecar(sce, cm, wl.GetName())
		}
		return false, nil
	})
	return sce, err
}

func checkInterceptAnnotations(wl k8sapi.Workload) (bool, error) {
	pod := wl.GetPodTemplate()
	a := pod.Annotations
	if a == nil {
		return true, nil
	}

	webhookEnabled := true
	manuallyManaged := a[mutator.ManualInjectAnnotation] == "true"
	ia := a[mutator.InjectAnnotation]
	switch ia {
	case "":
		webhookEnabled = !manuallyManaged
	case "enabled":
	case "false", "disabled":
		webhookEnabled = false
	default:
		return false, errcat.User.Newf(
			"%s is not a valid value for the %s.%s/%s annotation",
			ia, wl.GetName(), wl.GetNamespace(), mutator.ManualInjectAnnotation)
	}

	if !manuallyManaged {
		return webhookEnabled, nil
	}
	cns := pod.Spec.Containers
	var an *core.Container
	for i := range cns {
		cn := &cns[i]
		if cn.Name == agentconfig.ContainerName {
			an = cn
			break
		}
	}
	if an == nil {
		return false, errcat.User.Newf(
			"annotation %s.%s/%s=true but pod has no traffic-agent container",
			wl.GetName(), wl.GetNamespace(), mutator.ManualInjectAnnotation)
	}
	return true, nil
}

func watchFailedInjectionEvents(ctx context.Context, name, namespace string) (<-chan *events.Event, error) {
	// A timestamp with second granularity is needed here, because that's what the event creation time uses.
	// Finer granularity will result in relevant events seemingly being created before this timestamp because
	// they have the fraction of seconds trimmed off (which is odd, given that the type used is a MicroTime).
	start := time.Unix(time.Now().Unix(), 0)

	ei := k8sapi.GetK8sInterface(ctx).EventsV1().Events(namespace)
	w, err := ei.Watch(ctx, meta.ListOptions{
		FieldSelector: fields.OneTermNotEqualSelector("type", "Normal").String(),
	})
	if err != nil {
		return nil, err
	}
	nd := name + "-"
	ec := make(chan *events.Event)
	go func() {
		defer w.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case eo, ok := <-w.ResultChan():
				if !ok {
					return
				}
				// Using a negated Before when comparing the timestamps here is relevant. They will often be equal and still relevant
				if e, ok := eo.Object.(*events.Event); ok &&
					!e.CreationTimestamp.Time.Before(start) &&
					!strings.HasPrefix(e.Note, "(combined from similar events):") {
					n := e.Regarding.Name
					if strings.HasPrefix(n, nd) || n == name {
						dlog.Infof(ctx, "%s %s %s", e.Type, e.Reason, e.Note)
						ec <- e
					}
				}
			}
		}
	}()
	return ec, nil
}

func (s *state) waitForAgents(ctx context.Context, name, namespace string, failedCreateCh <-chan *events.Event) ([]*managerrpc.AgentInfo, error) {
	dlog.Debugf(ctx, "Waiting for agent %s.%s", name, namespace)
	snapshotCh := s.WatchAgents(ctx, func(sessionID string, agent *managerrpc.AgentInfo) bool {
		return agent.Name == name && agent.Namespace == namespace
	})
	failedContainerRx := regexp.MustCompile(`restarting failed container (\S+) in pod ([0-9A-Za-z_-]+)_` + namespace)
	mm := mutator.GetMap(ctx)

	// fes collects events from the failedCreatedCh and is included in the error message in case
	// the waitForAgents call times out.
	var fes []*events.Event
	for {
		select {
		case fe, ok := <-failedCreateCh:
			if !ok {
				return nil, errors.New("failed create channel closed")
			}
			msg := fe.Note
			// Terminate directly on known fatal events. No need for the user to wait for a timeout
			// when one of these are encountered.
			switch fe.Reason {
			case "BackOff":
				// The traffic-agent container was injected, but it fails to start
				if rr := failedContainerRx.FindStringSubmatch(msg); rr != nil {
					cn := rr[1]
					pod := rr[2]
					rq := k8sapi.GetK8sInterface(ctx).CoreV1().Pods(namespace).GetLogs(pod, &core.PodLogOptions{
						Container: cn,
					})
					if rs, err := rq.Stream(ctx); err == nil {
						if log, err := io.ReadAll(rs); err == nil {
							dlog.Infof(ctx, "Log from failing pod %q, container %s\n%s", pod, cn, string(log))
						} else {
							dlog.Errorf(ctx, "failed to read log stream from pod %q, container %s\n%s", pod, cn, err)
						}
						_ = rs.Close()
					} else {
						dlog.Errorf(ctx, "failed to read log from pod %q, container %s\n%s", pod, cn, err)
					}
				}
				msg = fmt.Sprintf("%s\nThe logs of %s %s might provide more details", msg, fe.Regarding.Kind, fe.Regarding.Name)
			case "Failed", "FailedCreate", "FailedScheduling":
				// The injection of the traffic-agent failed for some reason, most likely due to resource quota restrictions.
				if fe.Type == "Warning" && (strings.Contains(msg, "waiting for ephemeral volume") ||
					strings.Contains(msg, "unbound immediate PersistentVolumeClaims") ||
					strings.Contains(msg, "skip schedule deleting pod") ||
					strings.Contains(msg, "nodes are available")) {
					// This isn't fatal.
					fes = append(fes, fe)
					continue
				}
				msg = fmt.Sprintf(
					"%s\nHint: if the error mentions resource quota, the traffic-agent's requested resources can be configured by providing values to telepresence helm install",
					msg)
			default:
				// Something went wrong, but it might not be fatal. There are several events logged that are just
				// warnings where the action will be retried and eventually succeed.
				fes = append(fes, fe)
				continue
			}
			return nil, errcat.User.New(msg)
		case snapshot, ok := <-snapshotCh:
			if !ok {
				// The request has been canceled.
				return nil, status.Error(codes.Canceled, fmt.Sprintf("channel closed while waiting for agent %s.%s to arrive", name, namespace))
			}
			if len(snapshot.State) == 0 {
				continue
			}
			as := make([]*managerrpc.AgentInfo, 0, len(snapshot.State))
			for _, a := range snapshot.State {
				if mm.IsBlacklisted(a.PodName, a.Namespace) {
					dlog.Debugf(ctx, "Pod %s.%s is blacklisted", a.PodName, a.Namespace)
				} else {
					dlog.Debugf(ctx, "Agent %s.%s is ready", a.Name, a.Namespace)
					as = append(as, a)
				}
			}
			if len(as) > 0 {
				return as, nil
			}
		case <-ctx.Done():
			v := "canceled"
			if ctx.Err() == context.DeadlineExceeded {
				v = "timed out"
			}
			bf := &strings.Builder{}
			fmt.Fprintf(bf, "request %s while waiting for agent %s.%s to arrive", v, name, namespace)
			if len(fes) > 0 {
				bf.WriteString(": Events that may be relevant:\n")
				writeEventList(bf, fes)
			}
			return nil, errcat.User.New(bf.String())
		}
	}
}

func writeEventList(bf *strings.Builder, es []*events.Event) {
	now := time.Now()
	age := func(e *events.Event) string {
		return now.Sub(e.CreationTimestamp.Time).Truncate(time.Second).String()
	}
	object := func(e *events.Event) string {
		or := e.Regarding
		return strings.ToLower(or.Kind) + "/" + or.Name
	}
	ageLen, typeLen, reasonLen, objectLen := len("AGE"), len("TYPE"), len("REASON"), len("OBJECT")
	for _, e := range es {
		if l := len(age(e)); l > ageLen {
			ageLen = l
		}
		if l := len(e.Type); l > typeLen {
			typeLen = l
		}
		if l := len(e.Reason); l > reasonLen {
			reasonLen = l
		}
		if l := len(object(e)); l > objectLen {
			objectLen = l
		}
	}
	ageLen += 3
	typeLen += 3
	reasonLen += 3
	objectLen += 3
	fmt.Fprintf(bf, "%-*s%-*s%-*s%-*s%s\n", ageLen, "AGE", typeLen, "TYPE", reasonLen, "REASON", objectLen, "OBJECT", "MESSAGE")
	for _, e := range es {
		fmt.Fprintf(bf, "%-*s%-*s%-*s%-*s%s\n", ageLen, age(e), typeLen, e.Type, reasonLen, e.Reason, objectLen, object(e), e.Note)
	}
}

func unmarshalConfigMapEntry(y string, name, namespace string) (agentconfig.SidecarExt, error) {
	scx, err := agentconfig.UnmarshalYAML([]byte(y))
	if err != nil {
		return nil, fmt.Errorf("failed to parse entry for %s in ConfigMap %s.%s: %w", name, agentconfig.ConfigMap, namespace, err)
	}
	return scx, nil
}

// findIntercept finds the intercept configuration that matches the given InterceptSpec's service/service port or container port.
func findIntercept(ac *agentconfig.Sidecar, spec *managerrpc.InterceptSpec) (foundCN *agentconfig.Container, foundIC *agentconfig.Intercept, err error) {
	pi := agentconfig.PortIdentifier(spec.PortIdentifier)
	for _, cn := range ac.Containers {
		for _, ic := range cn.Intercepts {
			if !(spec.ServiceName == "" || spec.ServiceName == ic.ServiceName) {
				continue
			}
			if pi != "" {
				if ic.ServiceUID != "" {
					if !agentconfig.IsInterceptForService(pi, ic) {
						continue
					}
				} else if !agentconfig.IsInterceptForContainer(pi, ic) {
					continue
				}
			}
			if foundIC == nil {
				foundCN = cn
				if spec.ContainerName != "" {
					for _, cx := range ac.Containers {
						if cx.Name == spec.ContainerName {
							foundCN = cx
							break
						}
					}
				}
				foundIC = ic
				continue
			}
			var msg string
			switch {
			case spec.ServiceName == "" && pi == "":
				msg = fmt.Sprintf("%s %s.%s has multiple interceptable ports.\n"+
					"Please specify the service and/or port you want to intercept "+
					"by passing the --service=<svc> and/or --port=<local:portName/portNumber> flag.",
					ac.WorkloadKind, ac.WorkloadName, ac.Namespace)
			case spec.ServiceName == "":
				msg = fmt.Sprintf("%s %s.%s has multiple interceptable services with port %s.\n"+
					"Please specify the service you want to intercept by passing the --service=<svc> flag.",
					ac.WorkloadKind, ac.WorkloadName, ac.Namespace, pi)
			case pi == "":
				msg = fmt.Sprintf("%s %s.%s has multiple interceptable ports in service %s.\n"+
					"Please specify the port you want to intercept by passing the --port=<local:svcPortName> flag.",
					ac.WorkloadKind, ac.WorkloadName, ac.Namespace, spec.ServiceName)
			default:
				msg = fmt.Sprintf("%s %s.%s intercept config is broken. Service %s, port %s is declared more than once\n",
					ac.WorkloadKind, ac.WorkloadName, ac.Namespace, spec.ServiceName, pi)
			}
			return nil, nil, errcat.User.New(msg)
		}
	}
	if foundIC != nil {
		return foundCN, foundIC, nil
	}

	ss := ""
	if spec.ServiceName != "" {
		if pi != "" {
			ss = fmt.Sprintf(" matching service %s, port %s", spec.ServiceName, pi)
		} else {
			ss = fmt.Sprintf(" matching service %s", spec.ServiceName)
		}
	} else if pi != "" {
		ss = fmt.Sprintf(" matching port %s", pi)
	}
	return nil, nil, errcat.User.Newf("%s %s.%s has no interceptable port%s", ac.WorkloadKind, ac.WorkloadName, ac.Namespace, ss)
}

type InterceptFinalizer func(ctx context.Context, interceptInfo *managerrpc.InterceptInfo) error

type interceptState struct {
	sync.Mutex
	lastInfoCh  chan *managerrpc.InterceptInfo
	finalizers  []InterceptFinalizer
	interceptID string
}

func newInterceptState(interceptID string) *interceptState {
	is := &interceptState{
		lastInfoCh:  make(chan *managerrpc.InterceptInfo),
		interceptID: interceptID,
	}
	return is
}

func (is *interceptState) addFinalizer(finalizer InterceptFinalizer) {
	is.Lock()
	defer is.Unlock()
	is.finalizers = append(is.finalizers, finalizer)
}

func (is *interceptState) terminate(ctx context.Context, interceptInfo *managerrpc.InterceptInfo) {
	is.Lock()
	defer is.Unlock()
	for i := len(is.finalizers) - 1; i >= 0; i-- {
		f := is.finalizers[i]
		if err := f(ctx, interceptInfo); err != nil {
			dlog.Errorf(ctx, "finalizer for intercept %s failed: %v", interceptInfo.Id, err)
		}
	}
}
