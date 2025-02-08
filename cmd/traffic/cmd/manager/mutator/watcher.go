package mutator

import (
	"context"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/puzpuzpuz/xsync/v3"
	"google.golang.org/protobuf/types/known/durationpb"
	core "k8s.io/api/core/v1"
	v1 "k8s.io/api/policy/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"

	"github.com/datawire/dlib/derror"
	"github.com/datawire/dlib/dlog"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/managerutil"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/namespaces"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/agentmap"
	"github.com/telepresenceio/telepresence/v2/pkg/informer"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
	"github.com/telepresenceio/telepresence/v2/pkg/workload"
)

type Map interface {
	Get(string, string) agentconfig.SidecarExt
	Store(agentconfig.SidecarExt)
	Start(context.Context)
	StartWatchers(context.Context) error
	Wait(context.Context) error
	OnAdd(context.Context, k8sapi.Workload, agentconfig.SidecarExt) error
	OnDelete(context.Context, string, string) error
	DeleteMapsAndRolloutAll(context.Context)
	IsInactive(podID types.UID) bool
	Inactivate(podID types.UID)
	EvictPodsWithAgentConfig(ctx context.Context, wl k8sapi.Workload) error
	EvictPodsWithAgentConfigMismatch(ctx context.Context, scx agentconfig.SidecarExt) error
	EvictAllPodsWithAgentConfig(ctx context.Context, namespace string) error

	RegenerateAgentMaps(ctx context.Context, s string) error

	Delete(name, namespace string)
	Update(name, namespace string, updater func(cm agentconfig.SidecarExt) (agentconfig.SidecarExt, error)) (agentconfig.SidecarExt, error)
}

var NewWatcherFunc = NewWatcher //nolint:gochecknoglobals // extension point

type mapKey struct{}

func WithMap(ctx context.Context, m Map) context.Context {
	return context.WithValue(ctx, mapKey{}, m)
}

func GetMap(ctx context.Context) Map {
	if m, ok := ctx.Value(mapKey{}).(Map); ok {
		return m
	}
	return nil
}

func Load(ctx context.Context) Map {
	cw := NewWatcherFunc()
	cw.Start(ctx)
	return cw
}

// RegenerateAgentMaps load the telepresence-agents config map, regenerates all entries in it,
// and then, if any of the entries changed, it updates the map.
func (c *configWatcher) RegenerateAgentMaps(ctx context.Context, agentImage string) error {
	gc, err := agentmap.GeneratorConfigFunc(agentImage)
	if err != nil {
		return err
	}
	nss := namespaces.GetOrGlobal(ctx)
	for _, ns := range nss {
		if err = c.regenerateAgentMaps(ctx, ns, gc); err != nil {
			return err
		}
	}
	return nil
}

// regenerateAgentMaps load the telepresence-agents config map, regenerates all entries in it,
// and then, if any of the entries changed, it updates the map.
func (c *configWatcher) regenerateAgentMaps(ctx context.Context, ns string, gc agentmap.GeneratorConfig) error {
	dlog.Debugf(ctx, "regenerate agent maps %s", whereWeWatch(ns))
	pods, err := podList(ctx, "", "", ns)
	if err != nil {
		return err
	}

	dbpCmp := cmp.Comparer(func(a, b *durationpb.Duration) bool {
		return a.AsDuration() == b.AsDuration()
	})

	wls := make(map[workloadKey]agentconfig.SidecarExt, len(pods))
	for _, pod := range pods {
		cfgJSON, ok := pod.ObjectMeta.Annotations[agentconfig.ConfigAnnotation]
		if !ok {
			continue
		}
		sce, err := agentconfig.UnmarshalJSON(cfgJSON)
		if err != nil {
			dlog.Errorf(ctx, "unable to unmarshal agent config from annotation in pod %s.%s: %v", pod.Name, pod.Namespace, err)
			continue
		}
		ac := sce.AgentConfig()
		key := workloadKey{
			name:      ac.WorkloadName,
			namespace: ac.Namespace,
			kind:      ac.WorkloadKind,
		}
		newSce, ok := wls[key]
		if !ok && managerutil.GetEnv(ctx).EnabledWorkloadKinds.Contains(ac.WorkloadKind) {
			wl, err := agentmap.GetWorkload(ctx, ac.WorkloadName, ac.Namespace, ac.WorkloadKind)
			if err != nil {
				dlog.Errorf(ctx, "unable to load %s %s.%s", ac.WorkloadKind, ac.WorkloadName, ac.Namespace)
				continue
			}
			newSce, err = gc.Generate(ctx, wl, sce)
			if err != nil {
				dlog.Errorf(ctx, "unable to update config for %s %s.%s", ac.WorkloadKind, ac.WorkloadName, ac.Namespace)
				continue
			}
			wls[key] = newSce
			c.Store(newSce)
		}
		if newSce == nil || !cmp.Equal(newSce, sce, dbpCmp) {
			go func() {
				evictPod(ctx, pod)
			}()
		}
	}
	return nil
}

type workloadKey struct {
	name      string
	namespace string
	kind      k8sapi.Kind
}

const (
	serviceWatcher = iota
	deploymentWatcher
	replicaSetWatcher
	statefulSetWatcher
	rolloutWatcher
	watcherMax
)

type informersWithCancel struct {
	cancel    context.CancelFunc
	informers [watcherMax]cache.SharedIndexInformer
	eventRegs [watcherMax]cache.ResourceEventHandlerRegistration
}

type inactivation struct {
	time.Time
	deleted bool
}

type configWatcher struct {
	cancel       context.CancelFunc
	agentConfigs *xsync.MapOf[string, map[string]agentconfig.SidecarExt]
	informers    *xsync.MapOf[string, *informersWithCancel]
	inactivePods *xsync.MapOf[types.UID, inactivation]
	startedAt    time.Time
	running      atomic.Bool

	self Map // For extension
}

func (c *configWatcher) Delete(name, namespace string) {
	c.agentConfigs.Compute(namespace, func(sceMap map[string]agentconfig.SidecarExt, loaded bool) (map[string]agentconfig.SidecarExt, bool) {
		if loaded {
			delete(sceMap, name)
			return sceMap, len(sceMap) == 0
		}
		return nil, true
	})
}

func (c *configWatcher) Update(name, namespace string, updater func(agentconfig.SidecarExt) (agentconfig.SidecarExt, error)) (agentconfig.SidecarExt, error) {
	var err error
	var sce agentconfig.SidecarExt
	c.agentConfigs.Compute(namespace, func(sceMap map[string]agentconfig.SidecarExt, loaded bool) (map[string]agentconfig.SidecarExt, bool) {
		if loaded {
			var ok bool
			sce, ok = sceMap[name]
			if ok {
				sce = sce.Clone()
			}
			sce, err = updater(sce)
			if err == nil {
				if sce == nil {
					delete(sceMap, name)
				} else {
					sceMap[name] = sce
				}
			}
			return sceMap, false
		} else {
			sce, err = updater(nil)
			if err == nil && sce != nil {
				sceMap = map[string]agentconfig.SidecarExt{name: sce}
				return sceMap, false
			}
			return nil, true
		}
	})
	return sce, err
}

func (c *configWatcher) Store(sce agentconfig.SidecarExt) {
	ag := sce.AgentConfig()
	c.agentConfigs.Compute(ag.Namespace, func(sceMap map[string]agentconfig.SidecarExt, loaded bool) (map[string]agentconfig.SidecarExt, bool) {
		if loaded {
			sceMap[ag.AgentName] = sce
		} else {
			sceMap = map[string]agentconfig.SidecarExt{ag.AgentName: sce}
		}
		return sceMap, false
	})
}

func NewWatcher() Map {
	w := &configWatcher{
		informers:    xsync.NewMapOf[string, *informersWithCancel](),
		inactivePods: xsync.NewMapOf[types.UID, inactivation](),
		agentConfigs: xsync.NewMapOf[string, map[string]agentconfig.SidecarExt](),
	}
	w.self = w
	return w
}

func (c *configWatcher) SetSelf(self Map) {
	c.self = self
}

func (c *configWatcher) startInformers(ctx context.Context, ns string) (iwc *informersWithCancel, err error) {
	defer c.running.Store(true)
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		if err != nil {
			cancel()
		}
	}()

	ifns := [watcherMax]cache.SharedIndexInformer{}
	ifns[serviceWatcher] = c.startServices(ctx, ns)
	for _, wlKind := range managerutil.GetEnv(ctx).EnabledWorkloadKinds {
		switch wlKind {
		case k8sapi.DeploymentKind:
			ifns[deploymentWatcher] = workload.StartDeployments(ctx, ns)
		case k8sapi.ReplicaSetKind:
			ifns[replicaSetWatcher] = workload.StartReplicaSets(ctx, ns)
		case k8sapi.StatefulSetKind:
			ifns[statefulSetWatcher] = workload.StartStatefulSets(ctx, ns)
		case k8sapi.RolloutKind:
			ifns[rolloutWatcher] = workload.StartRollouts(ctx, ns)
		}
	}
	c.startPods(ctx, ns)
	kf := informer.GetK8sFactory(ctx, ns)
	kf.Start(ctx.Done())
	kf.WaitForCacheSync(ctx.Done())
	if ifns[rolloutWatcher] != nil {
		rf := informer.GetArgoRolloutsFactory(ctx, ns)
		rf.Start(ctx.Done())
		rf.WaitForCacheSync(ctx.Done())
	}

	return &informersWithCancel{
		cancel:    cancel,
		informers: ifns,
	}, nil
}

func (c *configWatcher) startWatchers(ctx context.Context, iwc *informersWithCancel) (err error) {
	ifns := iwc.informers
	iwc.eventRegs[serviceWatcher], err = c.watchServices(ctx, ifns[serviceWatcher])
	if err != nil {
		return err
	}
	for i := deploymentWatcher; i < watcherMax; i++ {
		if ifn := ifns[i]; ifn != nil {
			iwc.eventRegs[i], err = c.watchWorkloads(ctx, ifn)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *configWatcher) StartWatchers(ctx context.Context) error {
	defer c.running.Store(true)
	c.startedAt = time.Now()
	ctx, c.cancel = context.WithCancel(ctx)
	var errs []error
	c.informers.Range(func(ns string, iwc *informersWithCancel) bool {
		if err := c.startWatchers(ctx, iwc); err != nil {
			errs = append(errs, err)
			return false
		}
		return true
	})
	if len(errs) > 0 {
		return derror.MultiError(errs)
	}
	return nil
}

func (c *configWatcher) Wait(ctx context.Context) error {
	if err := c.StartWatchers(ctx); err != nil {
		return err
	}
	return c.namespacesChangeWatcher(ctx)
}

func (c *configWatcher) OnAdd(ctx context.Context, wl k8sapi.Workload, acx agentconfig.SidecarExt) error {
	return nil
}

func (c *configWatcher) OnDelete(context.Context, string, string) error {
	return nil
}

// Get returns the Sidecar configuration that for the given key and namespace.
// If no configuration is found, this function returns nil, nil.
// An error is only returned when the configmap holding the configuration could not be loaded for
// other reasons than it did not exist.
func (c *configWatcher) Get(key, ns string) (ac agentconfig.SidecarExt) {
	c.agentConfigs.Compute(ns, func(sceMap map[string]agentconfig.SidecarExt, loaded bool) (map[string]agentconfig.SidecarExt, bool) {
		if loaded {
			ac = sceMap[key]
		}
		return sceMap, !loaded
	})
	return ac
}

func whereWeWatch(ns string) string {
	if ns == "" {
		return "cluster wide"
	}
	return "in namespace " + ns
}

func (c *configWatcher) startPods(ctx context.Context, ns string) cache.SharedIndexInformer {
	f := informer.GetK8sFactory(ctx, ns)
	ix := f.Core().V1().Pods().Informer()
	_ = ix.SetTransform(func(o any) (any, error) {
		if pod, ok := o.(*core.Pod); ok {
			pod.ManagedFields = nil
			pod.Finalizers = nil

			ps := &pod.Status
			// We're just interested in the podIP/podIPs
			ps.Conditions = nil

			// Strip everything but the State from the container statuses. We need
			// the state to determine if a pod is running.
			cns := pod.Status.ContainerStatuses
			for i := range cns {
				cns[i] = core.ContainerStatus{
					State: cns[i].State,
				}
			}
			ps.EphemeralContainerStatuses = nil
			ps.HostIPs = nil
			ps.HostIP = ""
			ps.InitContainerStatuses = nil
			ps.Message = ""
			ps.ResourceClaimStatuses = nil
			ps.NominatedNodeName = ""
			ps.Reason = ""
			ps.Resize = ""
		}
		return o, nil
	})
	_ = ix.SetWatchErrorHandler(func(_ *cache.Reflector, err error) {
		dlog.Errorf(ctx, "Watcher for pods %s: %v", whereWeWatch(ns), err)
	})
	return ix
}

func (c *configWatcher) gcInactivated(now time.Time) {
	c.inactivePods.Range(func(key types.UID, value inactivation) bool {
		if now.Sub(value.Time) > time.Minute {
			c.inactivePods.Delete(key)
		}
		return true
	})
}

func (c *configWatcher) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case now := <-ticker.C:
				c.gcInactivated(now)
			}
		}
	}()

	for _, ns := range namespaces.GetOrGlobal(ctx) {
		dlog.Debugf(ctx, "Adding watchers for namespace %s", ns)
		iwc, err := c.startInformers(ctx, ns)
		if err != nil {
			dlog.Errorf(ctx, "Failed to create watchers namespace %s: %v", ns, err)
			continue
		}
		c.informers.Store(ns, iwc)
	}
}

func (c *configWatcher) namespacesChangeWatcher(ctx context.Context) error {
	sid, nsChanges := namespaces.Subscribe(ctx)
	defer namespaces.Unsubscribe(ctx, sid)
	for {
		select {
		case <-ctx.Done():
			return nil
		case _, ok := <-nsChanges:
			if !ok {
				return nil
			}
			nss := namespaces.GetOrGlobal(ctx)

			// Start informers for added namespaces
			for _, ns := range nss {
				c.informers.Compute(ns, func(iwc *informersWithCancel, loaded bool) (*informersWithCancel, bool) {
					if loaded {
						return iwc, false
					}
					dlog.Debugf(ctx, "Adding watchers for namespace %s", ns)
					iwc, err := c.startInformers(ctx, ns)
					if err != nil {
						dlog.Errorf(ctx, "Failed to create watchers namespace %s: %v", ns, err)
						return nil, true
					}
					if err = c.startWatchers(ctx, iwc); err != nil {
						dlog.Errorf(ctx, "Failed to start watchers namespace %s: %v", ns, err)
						return nil, true
					}
					return iwc, false
				})
			}

			// Stop informers for namespaces that are no longer managed
			c.informers.Range(func(ns string, iwc *informersWithCancel) bool {
				if !slices.Contains(nss, ns) {
					c.deleteMapsAndRolloutNS(ctx, ns, iwc)
				}
				return true
			})
		}
	}
}

func (c *configWatcher) DeleteMapsAndRolloutAll(ctx context.Context) {
	if c.running.CompareAndSwap(true, false) {
		c.cancel() // No more updates from watcher
		c.informers.Range(func(ns string, iwc *informersWithCancel) bool {
			c.deleteMapsAndRolloutNS(ctx, ns, iwc)
			return true
		})
	}
}

func (c *configWatcher) deleteMapsAndRolloutNS(ctx context.Context, ns string, iwc *informersWithCancel) {
	defer c.informers.Delete(ns)

	dlog.Debugf(ctx, "Cancelling watchers for namespace %s", ns)
	for i := 0; i < watcherMax; i++ {
		if reg := iwc.eventRegs[i]; reg != nil {
			_ = iwc.informers[i].RemoveEventHandler(reg)
		}
	}
	iwc.cancel()

	err := c.EvictAllPodsWithAgentConfig(ctx, ns)
	if err != nil {
		dlog.Errorf(ctx, "unable to delete agents in namespace %s: %v", ns, err)
	}
}

func (c *configWatcher) EvictPodsWithAgentConfigMismatch(ctx context.Context, scx agentconfig.SidecarExt) error {
	ac := scx.AgentConfig()
	pods, err := podList(ctx, ac.WorkloadKind, ac.AgentName, ac.Namespace)
	if err != nil {
		return err
	}
	cfgJSON, err := agentconfig.MarshalTight(scx)
	if err != nil {
		return err
	}

	for _, pod := range pods {
		err = c.evictPodWithAgentConfigMismatch(ctx, pod, cfgJSON)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *configWatcher) EvictPodsWithAgentConfig(ctx context.Context, wl k8sapi.Workload) error {
	pods, err := podList(ctx, wl.GetKind(), wl.GetName(), wl.GetNamespace())
	if err != nil {
		return err
	}

	for _, pod := range pods {
		err = c.evictPodWithAgentConfigMismatch(ctx, pod, "")
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *configWatcher) EvictAllPodsWithAgentConfig(ctx context.Context, namespace string) error {
	c.agentConfigs.Delete(namespace)
	pods, err := podList(ctx, "", "", namespace)
	if err != nil {
		return err
	}
	for _, pod := range pods {
		err = c.evictPodWithAgentConfigMismatch(ctx, pod, "")
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *configWatcher) evictPodWithAgentConfigMismatch(ctx context.Context, pod *core.Pod, cfgJSON string) error {
	podID := pod.UID
	if c.isEvicted(podID) {
		dlog.Debugf(ctx, "Skipping pod %s because it is already deleted", pod.Name)
		return nil
	}
	a := pod.ObjectMeta.Annotations
	if v, ok := a[agentconfig.ManualInjectAnnotation]; ok && v == "true" {
		dlog.Tracef(ctx, "Skipping pod %s because it is managed manually", pod.Name)
		return nil
	}
	if a[agentconfig.ConfigAnnotation] == cfgJSON {
		dlog.Tracef(ctx, "Keeping pod %s because its config is still valid", pod.Name)
		return nil
	}
	var err error
	c.inactivePods.Compute(podID, func(v inactivation, loaded bool) (inactivation, bool) {
		if loaded && v.deleted {
			dlog.Debugf(ctx, "Skipping pod %s because it was deleted by another goroutine", pod.Name)
			return v, false
		}
		evictPod(ctx, pod)
		return inactivation{Time: time.Now(), deleted: true}, false
	})
	return err
}

func evictPod(ctx context.Context, pod *core.Pod) {
	dlog.Debugf(ctx, "Evicting pod %s", pod.Name)
	err := k8sapi.GetK8sInterface(ctx).CoreV1().Pods(pod.Namespace).EvictV1(ctx, &v1.Eviction{
		ObjectMeta: meta.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace},
	})
	if err != nil {
		dlog.Errorf(ctx, "Failed to evict pod %s: %v", pod.Name, err)
	}
}

func (c *configWatcher) Inactivate(podID types.UID) {
	c.inactivePods.LoadOrCompute(podID, func() inactivation {
		return inactivation{Time: time.Now()}
	})
}

func (c *configWatcher) IsInactive(podID types.UID) bool {
	_, ok := c.inactivePods.Load(podID)
	return ok
}

func (c *configWatcher) isEvicted(podID types.UID) bool {
	v, ok := c.inactivePods.Load(podID)
	return ok && v.deleted
}

func podIsRunning(pod *core.Pod) bool {
	switch pod.Status.Phase {
	case core.PodPending, core.PodRunning:
		return true
	default:
		return false
	}
}

func podList(ctx context.Context, kind k8sapi.Kind, name, namespace string) ([]*core.Pod, error) {
	var lister interface {
		List(selector labels.Selector) (ret []*core.Pod, err error)
	}
	api := informer.GetK8sFactory(ctx, namespace).Core().V1().Pods().Lister()
	if namespace == "" {
		lister = api
	} else {
		lister = api.Pods(namespace)
	}
	pods, err := lister.List(labels.Everything())
	if err != nil {
		return nil, fmt.Errorf("error listing pods in namespace %s: %v", namespace, err)
	}
	enabledWorkloads := managerutil.GetEnv(ctx).EnabledWorkloadKinds
	var podsOfInterest []*core.Pod
	for _, pod := range pods {
		if !podIsRunning(pod) {
			continue
		}
		if podKind, ok := pod.Labels[agentconfig.WorkloadKindLabel]; ok && !enabledWorkloads.Contains(k8sapi.Kind(podKind)) {
			// Pod's label indicates a workload kind that has been disabled.
			podsOfInterest = append(podsOfInterest, pod)
			continue
		}
		wl, err := agentmap.FindOwnerWorkload(ctx, k8sapi.Pod(pod), enabledWorkloads)
		if err != nil {
			if !k8sErrors.IsNotFound(err) {
				return nil, err
			}
		} else if (kind == "" || wl.GetKind() == kind) && (name == "" || wl.GetName() == name) {
			podsOfInterest = append(podsOfInterest, pod)
		}
	}
	return podsOfInterest, nil
}
