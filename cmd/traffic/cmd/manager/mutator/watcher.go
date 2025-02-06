package mutator

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/puzpuzpuz/xsync/v3"
	"google.golang.org/protobuf/types/known/durationpb"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/retry"

	"github.com/datawire/dlib/derror"
	"github.com/datawire/dlib/dlog"
	"github.com/datawire/dlib/dtime"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/managerutil"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/namespaces"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/agentmap"
	"github.com/telepresenceio/telepresence/v2/pkg/informer"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
	"github.com/telepresenceio/telepresence/v2/pkg/maps"
	"github.com/telepresenceio/telepresence/v2/pkg/workload"
)

type Map interface {
	Get(context.Context, string, string) (agentconfig.SidecarExt, error)
	Start(context.Context)
	StartWatchers(context.Context) error
	Wait(context.Context) error
	OnAdd(context.Context, k8sapi.Workload, agentconfig.SidecarExt) error
	OnDelete(context.Context, string, string) error
	DeleteMapsAndRolloutAll(context.Context)
	Blacklist(podName, namespace string)
	Whitelist(podName, namespace string)
	IsBlacklisted(podName, namespace string) bool
	DisableRollouts()

	store(ctx context.Context, acx agentconfig.SidecarExt) error
	remove(ctx context.Context, name, namespace string) error

	RegenerateAgentMaps(ctx context.Context, s string) error

	Delete(ctx context.Context, name, namespace string) error
	Update(ctx context.Context, namespace string, updater func(cm *core.ConfigMap) (bool, error)) error

	registerPrematureInjectEvent(wl k8sapi.Workload)
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

func (e *entry) workload(ctx context.Context) (agentconfig.SidecarExt, k8sapi.Workload, error) {
	scx, err := agentconfig.UnmarshalYAML([]byte(e.value))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to decode ConfigMap entry %q into an agent config", e.value)
	}
	ac := scx.AgentConfig()
	wl, err := agentmap.GetWorkload(ctx, ac.WorkloadName, ac.Namespace, ac.WorkloadKind)
	if err != nil {
		return nil, nil, err
	}
	return scx, wl, nil
}

// isRolloutNeeded checks if the agent's entry in telepresence-agents matches the actual state of the
// pods. If it does, then there's no reason to trigger a rollout.
func (c *configWatcher) isRolloutNeeded(ctx context.Context, wl k8sapi.Workload, ac *agentconfig.Sidecar) bool {
	if c.rolloutDisabled {
		return false
	}
	podMeta := wl.GetPodTemplate().GetObjectMeta()
	if wl.GetDeletionTimestamp() != nil {
		return false
	}
	if ia, ok := podMeta.GetAnnotations()[agentconfig.InjectAnnotation]; ok {
		// Annotation controls injection, so no explicit rollout is needed unless the deployment was added before the
		// traffic-manager or the traffic-manager already received an injection event but failed due to the lack
		// of an agent config.
		if c.running.Load() {
			if c.receivedPrematureInjectEvent(wl) {
				dlog.Debugf(ctx, "Rollout of %s.%s is necessary. Pod template has inject annotation %s and a premature injection event was received",
					wl.GetName(), wl.GetNamespace(), ia)
				return true
			}
			if wl.GetCreationTimestamp().After(c.startedAt) {
				dlog.Debugf(ctx, "Rollout of %s.%s is not necessary. Pod template has inject annotation %s",
					wl.GetName(), wl.GetNamespace(), ia)
				return false
			}
		}
	}
	podLabels := podMeta.GetLabels()
	if len(podLabels) == 0 {
		// Have never seen this, but if it happens, then rollout only if an agent is desired
		dlog.Debugf(ctx, "Rollout of %s.%s is necessary. Pod template has no pod labels",
			wl.GetName(), wl.GetNamespace())
		return true
	}

	selector := labels.SelectorFromValidatedSet(podLabels)
	podsAPI := informer.GetK8sFactory(ctx, wl.GetNamespace()).Core().V1().Pods().Lister().Pods(wl.GetNamespace())
	pods, err := podsAPI.List(selector)
	if err != nil {
		dlog.Debugf(ctx, "Rollout of %s.%s is necessary. Unable to retrieve current pods: %v",
			wl.GetName(), wl.GetNamespace(), err)
		return true
	}

	runningPods := 0
	okPods := 0
	var rolloutReasons []string
	for _, pod := range pods {
		if c.IsBlacklisted(pod.Name, pod.Namespace) {
			dlog.Debugf(ctx, "Skipping blacklisted pod %s.%s", pod.Name, pod.Namespace)
			continue
		}
		if !agentmap.IsPodRunning(pod) {
			continue
		}
		runningPods++
		if ror := isRolloutNeededForPod(ctx, ac, wl.GetName(), wl.GetNamespace(), pod); ror != "" {
			if !slices.Contains(rolloutReasons, ror) {
				rolloutReasons = append(rolloutReasons, ror)
			}
		} else {
			okPods++
		}
	}
	// Rollout if there are no running pods
	if runningPods == 0 {
		if ac != nil {
			dlog.Debugf(ctx, "Rollout of %s.%s is necessary. An agent is desired and there are no pods",
				wl.GetName(), wl.GetNamespace())
			return true
		}
		return false
	}
	if okPods == 0 {
		// Found no pods out there that match the desired state
		for _, ror := range rolloutReasons {
			dlog.Debug(ctx, ror)
		}
		return true
	}
	if ac == nil {
		if okPods < runningPods {
			dlog.Debugf(ctx, "Rollout of %s.%s is necessary. At least one pod still has an agent",
				wl.GetName(), wl.GetNamespace())
			return true
		}
		return false
	}
	dlog.Debugf(ctx, "Rollout of %s.%s is not necessary. At least one pod have the desired agent state",
		wl.GetName(), wl.GetNamespace())
	return false
}

func isRolloutNeededForPod(ctx context.Context, ac *agentconfig.Sidecar, name, namespace string, pod *core.Pod) string {
	podAc := agentmap.AgentContainer(pod)
	if ac == nil {
		if podAc == nil {
			dlog.Debugf(ctx, "Rollout check for %s.%s is found that no agent is desired and no agent config is present for pod %s", name, namespace, pod.GetName())
			return ""
		}
		return fmt.Sprintf("Rollout of %s.%s is necessary. No agent is desired but the pod %s has one", name, namespace, pod.GetName())
	}
	if podAc == nil {
		// Rollout because an agent is desired but the pod doesn't have one
		return fmt.Sprintf("Rollout of %s.%s is necessary. An agent is desired but the pod %s doesn't have one",
			name, namespace, pod.GetName())
	}
	desiredAc, anns := agentconfig.AgentContainer(ctx, pod, ac)
	if !(containerEqual(ctx, podAc, desiredAc) && maps.Equal(anns, pod.ObjectMeta.Annotations)) {
		return fmt.Sprintf("Rollout of %s.%s is necessary. The desired agent is not equal to the existing agent in pod %s",
			name, namespace, pod.GetName())
	}
	podIc := agentmap.InitContainer(pod)
	if podIc == nil {
		if needInitContainer(ac) {
			return fmt.Sprintf("Rollout of %s.%s is necessary. An init-container is desired but the pod %s doesn't have one",
				name, namespace, pod.GetName())
		}
	} else {
		if !needInitContainer(ac) {
			return fmt.Sprintf("Rollout of %s.%s is necessary. No init-container is desired but the pod %s has one",
				name, namespace, pod.GetName())
		}
	}
	for _, cn := range ac.Containers {
		var found *core.Container
		cns := pod.Spec.Containers
		for i := range cns {
			if cns[i].Name == cn.Name {
				found = &cns[i]
				break
			}
		}
		if cn.Replace {
			if found != nil {
				return fmt.Sprintf("Rollout of %s.%s is necessary. The %s container must be replaced",
					name, namespace, cn.Name)
			}
		} else if found == nil {
			return fmt.Sprintf("Rollout of %s.%s is necessary. The %s container should not be replaced",
				name, namespace, cn.Name)
		}
	}
	return ""
}

func (c *configWatcher) triggerRollout(ctx context.Context, wl k8sapi.Workload, ac *agentconfig.Sidecar) {
	lck := c.getRolloutLock(wl)
	if !lck.TryLock() {
		// A rollout is already in progress, doing it again once it is complete wouldn't do any good.
		return
	}
	defer lck.Unlock()

	if !c.isRolloutNeeded(ctx, wl, ac) {
		return
	}

	switch wl.GetKind() {
	case "StatefulSet", "ReplicaSet":
		triggerScalingRollout(ctx, wl)
	default:
		restartAnnotation := generateRestartAnnotationPatch(wl.GetPodTemplate())
		if err := wl.Patch(ctx, types.JSONPatchType, []byte(restartAnnotation)); err != nil {
			err = fmt.Errorf("unable to patch %s %s.%s: %v", wl.GetKind(), wl.GetName(), wl.GetNamespace(), err)
			dlog.Error(ctx, err)
			return
		}
		dlog.Infof(ctx, "Successfully rolled out %s.%s", wl.GetName(), wl.GetNamespace())
	}
}

// generateRestartAnnotationPatch generates a JSON patch that adds or updates the annotation
// We need to use this particular patch type because argo-rollouts do not support strategic merge patches.
func generateRestartAnnotationPatch(podTemplate *core.PodTemplateSpec) string {
	basePointer := "/spec/template/metadata/annotations"
	pointer := fmt.Sprintf(
		basePointer+"/%s",
		strings.ReplaceAll(workload.AnnRestartedAt, "/", "~1"),
	)

	if _, ok := podTemplate.Annotations[workload.AnnRestartedAt]; ok {
		return fmt.Sprintf(
			`[{"op": "replace", "path": "%s", "value": "%s"}]`, pointer, time.Now().Format(time.RFC3339),
		)
	}

	if len(podTemplate.Annotations) == 0 {
		return fmt.Sprintf(
			`[{"op": "add", "path": "%s", "value": {}}, {"op": "add", "path": "%s", "value": "%s"}]`, basePointer, pointer, time.Now().Format(time.RFC3339),
		)
	}

	return fmt.Sprintf(
		`[{"op": "add", "path": "%s", "value": "%s"}]`, pointer, time.Now().Format(time.RFC3339),
	)
}

func triggerScalingRollout(ctx context.Context, wl k8sapi.Workload) {
	// Rollout of a replicatset/statefulset will not recreate the pods. In order for that to happen, the
	// set must be scaled down to zero replicas and then up again.
	dlog.Debugf(ctx, "Performing %s rollout of %s.%s using scaling", wl.GetKind(), wl.GetName(), wl.GetNamespace())
	replicas := wl.Replicas()
	if replicas == 0 {
		dlog.Debugf(ctx, "%s %s.%s has zero replicas so rollout was a no-op", wl.GetKind(), wl.GetName(), wl.GetNamespace())
		return
	}

	waitForReplicaCount := func(count int) error {
		for retryCount := 0; retryCount < 200; retryCount++ {
			if nwl, err := k8sapi.GetWorkload(ctx, wl.GetName(), wl.GetNamespace(), wl.GetKind()); err == nil {
				if rp := nwl.Replicas(); rp == count {
					wl = nwl
					return nil
				}
			}
			dtime.SleepWithContext(ctx, 300*time.Millisecond)
		}
		return fmt.Errorf("%s %s.%s never scaled down to zero", wl.GetKind(), wl.GetName(), wl.GetNamespace())
	}

	patch := `{"spec": {"replicas": 0}}`
	if err := wl.Patch(ctx, types.StrategicMergePatchType, []byte(patch)); err != nil {
		err = fmt.Errorf("unable to scale %s %s.%s to zero: %w", wl.GetKind(), wl.GetName(), wl.GetNamespace(), err)
		dlog.Error(ctx, err)
		return
	}
	if err := waitForReplicaCount(0); err != nil {
		dlog.Error(ctx, err)
		return
	}
	dlog.Debugf(ctx, "%s %s.%s was scaled down to zero. Scaling back to %d", wl.GetKind(), wl.GetName(), wl.GetNamespace(), replicas)
	patch = fmt.Sprintf(`{"spec": {"replicas": %d}}`, replicas)
	if err := wl.Patch(ctx, types.StrategicMergePatchType, []byte(patch)); err != nil {
		err = fmt.Errorf("unable to scale %s %s.%s to %d: %v", wl.GetKind(), wl.GetName(), wl.GetNamespace(), replicas, err)
		dlog.Error(ctx, err)
	}
	if err := waitForReplicaCount(replicas); err != nil {
		dlog.Error(ctx, err)
		return
	}
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
	lister := tpAgentsInformer(ctx, ns).Lister()
	cml, err := lister.List(labels.Everything())
	if err != nil {
		return err
	}
	dbpCmp := cmp.Comparer(func(a, b *durationpb.Duration) bool {
		return a.AsDuration() == b.AsDuration()
	})

	n := len(cml)
	for i := 0; i < n; i++ {
		cm := cml[i]
		changed := false
		ns := cm.Namespace
		err = c.Update(ctx, ns, func(cm *core.ConfigMap) (bool, error) {
			dlog.Debugf(ctx, "regenerate: checking namespace %s", ns)
			data := cm.Data
			for n, d := range data {
				e := &entry{name: n, namespace: ns, value: d}
				acx, wl, err := e.workload(ctx)
				if err != nil {
					if !errors.IsNotFound(err) {
						return false, err
					}
					dlog.Debugf(ctx, "regenereate: no workload found %s", n)
					delete(data, n) // Workload no longer exists
					changed = true
					continue
				}
				ncx, err := gc.Generate(ctx, wl, acx)
				if err != nil {
					return false, err
				}
				if cmp.Equal(acx, ncx, dbpCmp) {
					dlog.Debugf(ctx, "regenereate: agent %s is not modified", n)
					continue
				}
				yml, err := ncx.Marshal()
				if err != nil {
					return false, err
				}
				dlog.Debugf(ctx, "regenereate: agent %s was regenerated", n)
				data[n] = string(yml)
				changed = true
			}
			if changed {
				dlog.Debugf(ctx, "regenereate: updating regenerated agents")
			}
			return changed, nil
		})
	}
	return err
}

type podKey struct {
	name      string
	namespace string
}

type workloadKey struct {
	name      string
	namespace string
	kind      string
}

const (
	configMapWatcher = iota
	serviceWatcher
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

func newWorkloadKey(wl k8sapi.Workload) workloadKey {
	return workloadKey{
		name:      wl.GetName(),
		namespace: wl.GetNamespace(),
		kind:      wl.GetKind(),
	}
}

type configWatcher struct {
	cancel                   context.CancelFunc
	rolloutLocks             *xsync.MapOf[workloadKey, *sync.Mutex]
	nsLocks                  *xsync.MapOf[string, *sync.RWMutex]
	blacklistedPods          *xsync.MapOf[podKey, time.Time]
	prematureInjectionEvents *xsync.MapOf[workloadKey, time.Time]
	informers                *xsync.MapOf[string, *informersWithCancel]
	startedAt                time.Time
	rolloutDisabled          bool
	running                  atomic.Bool

	self Map // For extension
}

// Blacklist will prevent the pod from being used when determining if a rollout is necessary, and
// from participating in ReviewIntercept calls. This is needed because there's a lag between the
// time when a pod is deleted and its agent announces its departure during which the pod must be
// considered inactive.
func (c *configWatcher) Blacklist(podName, namespace string) {
	c.blacklistedPods.Store(podKey{name: podName, namespace: namespace}, time.Now())
}

func (c *configWatcher) Whitelist(podName, namespace string) {
	c.blacklistedPods.Delete(podKey{name: podName, namespace: namespace})
}

func (c *configWatcher) registerPrematureInjectEvent(wl k8sapi.Workload) {
	c.prematureInjectionEvents.Store(newWorkloadKey(wl), time.Now())
}

func (c *configWatcher) receivedPrematureInjectEvent(wl k8sapi.Workload) bool {
	_, yes := c.prematureInjectionEvents.Load(newWorkloadKey(wl))
	return yes
}

func (c *configWatcher) DisableRollouts() {
	c.rolloutDisabled = true
}

func (c *configWatcher) IsBlacklisted(podName, namespace string) bool {
	_, yes := c.blacklistedPods.Load(podKey{name: podName, namespace: namespace})
	return yes
}

func (c *configWatcher) Delete(ctx context.Context, name, namespace string) error {
	return c.remove(ctx, name, namespace)
}

func (c *configWatcher) Update(ctx context.Context, namespace string, updater func(cm *core.ConfigMap) (bool, error)) error {
	api := k8sapi.GetK8sInterface(ctx).CoreV1().ConfigMaps(namespace)
	return retry.RetryOnConflict(retry.DefaultRetry, func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = derror.PanicToError(r)
				dlog.Errorf(ctx, "%+v", err)
			}
		}()
		lock := c.getNamespaceLock(namespace)
		lock.Lock()
		defer lock.Unlock()
		cm, err := tpAgentsConfigMap(ctx, namespace)
		if err != nil {
			return err
		}
		cm = cm.DeepCopy() // Protect the cached cm from updates
		create := cm == nil
		if create {
			cm = &core.ConfigMap{
				TypeMeta: meta.TypeMeta{
					Kind:       "ConfigMap",
					APIVersion: "v1",
				},
				ObjectMeta: meta.ObjectMeta{
					Name:      agentconfig.ConfigMap,
					Namespace: namespace,
				},
			}
		}

		changed, err := updater(cm)
		if err == nil && changed {
			if create {
				_, err = api.Create(ctx, cm, meta.CreateOptions{})
				if err != nil && errors.IsAlreadyExists(err) {
					// Treat AlreadyExists as a Conflict so that this attempt is retried.
					err = errors.NewConflict(schema.GroupResource{
						Group:    "v1",
						Resource: "ConfigMap",
					}, cm.Name, err)
				}
			} else {
				_, err = api.Update(ctx, cm, meta.UpdateOptions{})
			}
		}
		return err
	})
}

func NewWatcher() Map {
	w := &configWatcher{
		nsLocks:                  xsync.NewMapOf[string, *sync.RWMutex](),
		rolloutLocks:             xsync.NewMapOf[workloadKey, *sync.Mutex](),
		informers:                xsync.NewMapOf[string, *informersWithCancel](),
		blacklistedPods:          xsync.NewMapOf[podKey, time.Time](),
		prematureInjectionEvents: xsync.NewMapOf[workloadKey, time.Time](),
	}
	w.self = w
	return w
}

type entry struct {
	name      string
	namespace string
	value     string
	oldValue  string
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
	ifns[configMapWatcher] = c.startConfigMap(ctx, ns)
	ifns[serviceWatcher] = c.startServices(ctx, ns)
	for _, wlKind := range managerutil.GetEnv(ctx).EnabledWorkloadKinds {
		switch wlKind {
		case workload.DeploymentKind:
			ifns[deploymentWatcher] = workload.StartDeployments(ctx, ns)
		case workload.ReplicaSetKind:
			ifns[replicaSetWatcher] = workload.StartReplicaSets(ctx, ns)
		case workload.StatefulSetKind:
			ifns[statefulSetWatcher] = workload.StartStatefulSets(ctx, ns)
		case workload.RolloutKind:
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
	iwc.eventRegs[configMapWatcher], err = c.watchConfigMap(ctx, ifns[configMapWatcher])
	if err != nil {
		return err
	}
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
	c.startedAt = time.Now()
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = func() {
		c.terminating.Store(true)
		cancel()
	}
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
	c.triggerRollout(ctx, wl, acx.AgentConfig())
	return nil
}

func (c *configWatcher) OnDelete(context.Context, string, string) error {
	return nil
}

func (c *configWatcher) handleAddOrUpdateEntry(ctx context.Context, e entry) {
	switch e.oldValue {
	case e.value:
		return
	case "":
		dlog.Debugf(ctx, "add %s.%s", e.name, e.namespace)
	default:
		dlog.Debugf(ctx, "update %s.%s", e.name, e.namespace)
	}
	scx, wl, err := e.workload(ctx)
	if err != nil {
		if !errors.IsNotFound(err) {
			dlog.Error(ctx, err)
		}
		return
	}
	ac := scx.AgentConfig()
	if ac.Manual {
		// Manually added, just ignore
		return
	}
	if err = c.self.OnAdd(ctx, wl, scx); err != nil {
		dlog.Error(ctx, err)
	}
}

func (c *configWatcher) handleDeleteEntry(ctx context.Context, e entry) {
	dlog.Debugf(ctx, "del %s.%s", e.name, e.namespace)
	scx, wl, err := e.workload(ctx)
	if err != nil {
		if !errors.IsNotFound(err) {
			dlog.Error(ctx, err)
			return
		}
	} else {
		ac := scx.AgentConfig()
		if ac.Create || ac.Manual {
			// Deleted before it was generated or manually added, just ignore
			return
		}
	}
	if err = c.self.OnDelete(ctx, e.name, e.namespace); err != nil {
		dlog.Error(ctx, err)
	}
	if wl != nil {
		c.triggerRollout(ctx, wl, nil)
	}
}

func (c *configWatcher) getNamespaceLock(ns string) *sync.RWMutex {
	lock, _ := c.nsLocks.LoadOrCompute(ns, func() *sync.RWMutex {
		return &sync.RWMutex{}
	})
	return lock
}

func (c *configWatcher) getRolloutLock(wl k8sapi.Workload) *sync.Mutex {
	lock, _ := c.rolloutLocks.LoadOrCompute(newWorkloadKey(wl), func() *sync.Mutex {
		return &sync.Mutex{}
	})
	return lock
}

// Get returns the Sidecar configuration that for the given key and namespace.
// If no configuration is found, this function returns nil, nil.
// An error is only returned when the configmap holding the configuration could not be loaded for
// other reasons than it did not exist.
func (c *configWatcher) Get(ctx context.Context, key, ns string) (agentconfig.SidecarExt, error) {
	lock := c.getNamespaceLock(ns)
	lock.RLock()
	defer lock.RUnlock()

	data, err := data(ctx, ns)
	if err != nil {
		return nil, err
	}
	v, ok := data[key]
	if !ok {
		return nil, nil
	}
	return agentconfig.UnmarshalYAML([]byte(v))
}

// remove will delete an agent config from the agents ConfigMap for the given namespace. It will
// also update the current snapshot.
// An attempt to delete a manually added config is a no-op.
func (c *configWatcher) remove(ctx context.Context, name, namespace string) error {
	return c.Update(ctx, namespace, func(cm *core.ConfigMap) (bool, error) {
		yml, ok := cm.Data[name]
		if !ok {
			return false, nil
		}
		scx, err := agentconfig.UnmarshalYAML([]byte(yml))
		if err != nil {
			return false, err
		}
		if scx.AgentConfig().Manual {
			return false, nil
		}
		delete(cm.Data, name)
		dlog.Debugf(ctx, "Deleting %s from ConfigMap %s.%s", name, agentconfig.ConfigMap, namespace)
		return true, nil
	})
}

// store an agent config in the agents ConfigMap for the given namespace.
func (c *configWatcher) store(ctx context.Context, acx agentconfig.SidecarExt) error {
	js, err := acx.Marshal()
	yml := string(js)
	if err != nil {
		return err
	}
	ac := acx.AgentConfig()
	ns := ac.Namespace
	return c.Update(ctx, ns, func(cm *core.ConfigMap) (bool, error) {
		if cm.Data == nil {
			cm.Data = make(map[string]string)
		} else {
			if oldYml, ok := cm.Data[ac.AgentName]; ok {
				if oldYml == yml {
					return false, nil
				}
				dlog.Debugf(ctx, "Modifying configmap entry for sidecar %s.%s", ac.AgentName, ac.Namespace)
				scx, err := agentconfig.UnmarshalYAML([]byte(oldYml))
				if err == nil && scx.AgentConfig().Manual {
					dlog.Warnf(ctx, "avoided an attempt to overwrite manually added Config entry for %s.%s", ac.AgentName, ns)
					return false, nil
				}
			}
		}
		cm.Data[ac.AgentName] = yml
		dlog.Debugf(ctx, "updating agent %s in %s.%s", ac.AgentName, agentconfig.ConfigMap, ns)
		return true, nil
	})
}

func data(ctx context.Context, ns string) (map[string]string, error) {
	cm, err := tpAgentsConfigMap(ctx, ns)
	if err != nil || cm == nil {
		return nil, err
	}
	return cm.Data, nil
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
			pod.OwnerReferences = nil
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

func (c *configWatcher) gcBlacklisted(now time.Time) {
	const maxAge = time.Minute
	maxCreated := now.Add(-maxAge)
	c.blacklistedPods.Range(func(key podKey, created time.Time) bool {
		if created.Before(maxCreated) {
			c.blacklistedPods.Delete(key)
		}
		return true
	})
	c.prematureInjectionEvents.Range(func(key workloadKey, created time.Time) bool {
		if created.Before(maxCreated) {
			c.prematureInjectionEvents.Delete(key)
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
				c.gcBlacklisted(now)
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
	defer func() {
		if err := recover(); err != nil {
			dlog.Errorf(ctx, "%+v", derror.PanicToError(err))
		}
	}()
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
	lock := c.getNamespaceLock(ns)
	lock.Lock()
	defer func() {
		lock.Unlock()
		c.nsLocks.Delete(ns)
		c.informers.Delete(ns)
	}()

	dlog.Debugf(ctx, "Cancelling watchers for namespace %s", ns)
	for i := 0; i < watcherMax; i++ {
		if reg := iwc.eventRegs[i]; reg != nil {
			_ = iwc.informers[i].RemoveEventHandler(reg)
		}
	}
	iwc.cancel()

	now := meta.NewDeleteOptions(0)
	api := k8sapi.GetK8sInterface(ctx).CoreV1()
	wlm, err := data(ctx, ns)
	if err != nil {
		dlog.Errorf(ctx, "unable to get configmap %s.%s: %v", agentconfig.ConfigMap, ns, err)
		return
	}
	for k, v := range wlm {
		e := &entry{name: k, namespace: ns, value: v}
		scx, wl, err := e.workload(ctx)
		if err != nil {
			if !errors.IsNotFound(err) {
				dlog.Errorf(ctx, "unable to get workload for %s.%s %s: %v", k, ns, v, err)
			}
			continue
		}
		ac := scx.AgentConfig()
		if ac.Create || ac.Manual {
			// Deleted before it was generated or manually added, just ignore
			continue
		}
		c.triggerRollout(ctx, wl, nil)
	}
	if err := api.ConfigMaps(ns).Delete(ctx, agentconfig.ConfigMap, *now); err != nil {
		if !errors.IsNotFound(err) {
			dlog.Errorf(ctx, "unable to delete ConfigMap %s-%s: %v", agentconfig.ConfigMap, ns, err)
		}
	}
}
