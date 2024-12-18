package k8sapi

import (
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"

	"github.com/datawire/dlib/dlog"
)

// resyncPeriod controls how often the controller goes through all items in the cache and fires an update func again.
// Resyncs are made to periodically check if updates were somehow missed (due to network glitches etc.). They consume
// a fair amount of resources on a large cluster and shouldn't run too frequently.
// TODO: Probably a good candidate to include in the cluster config.
const resyncPeriod = 2 * time.Minute

// Watcher watches some resource and can be cancelled.
type Watcher[T runtime.Object] struct {
	sync.Mutex
	cancel         context.CancelFunc
	resource       string
	namespace      string
	getter         cache.Getter
	cond           *sync.Cond
	controller     cache.Controller
	store          cache.Store
	equals         func(T, T) bool
	stateListeners []*StateListener
	labelSelector  string
	fieldSelector  string
}

type StateListener struct {
	Cb func()
}

func newListerWatcher(c context.Context, getter cache.Getter, resource, namespace, labelSelector, fieldSelector string) cache.ListerWatcher {
	// need to dig into how a ListerWatcher is created in order to pass the correct context
	listFunc := func(options meta.ListOptions) (runtime.Object, error) {
		options.FieldSelector = fieldSelector
		options.LabelSelector = labelSelector
		return getter.Get().
			Namespace(namespace).
			Resource(resource).
			VersionedParams(&options, meta.ParameterCodec).
			Do(c).
			Get()
	}
	watchFunc := func(options meta.ListOptions) (watch.Interface, error) {
		options.FieldSelector = fieldSelector
		options.LabelSelector = labelSelector
		options.Watch = true
		return getter.Get().
			Namespace(namespace).
			Resource(resource).
			VersionedParams(&options, meta.ParameterCodec).
			Watch(c)
	}
	return &cache.ListWatch{ListFunc: listFunc, WatchFunc: watchFunc}
}

// NewWatcher returns a new watcher. It will not do anything until it is started.
//
//	resource string: the kind of resouce you want to watch, like "pods"
//	getter cache.Getter: a kubernetes rest.Interface client, like clientset.CoreV1().RESTClient()
//	objType T: an object of the resource type, like &corev1.Pod{}
//	cond *sync.Cond: cond will broadcast when the cache changes. Use k8sapi.Subscribe to subscribe to events
func NewWatcher[T runtime.Object](resource string, getter cache.Getter, cond *sync.Cond, opts ...WatcherOpt[T]) *Watcher[T] {
	watcher := &Watcher[T]{
		resource: resource,
		getter:   getter,
		cond:     cond,
	}

	for _, f := range opts {
		f(watcher)
	}

	return watcher
}

// AddStateListener adds a listener function that will be called when the watcher
// changes its state (starts or is cancelled).
func (w *Watcher[T]) AddStateListener(l *StateListener) {
	w.Lock()
	w.stateListeners = append(w.stateListeners, l)
	w.Unlock()
}

// RemoveStateListener removes a listener function.
func (w *Watcher[T]) RemoveStateListener(rl *StateListener) {
	w.Lock()
	sls := w.stateListeners
	last := len(sls) - 1
	for i, sl := range sls {
		if rl == sl {
			if i < last {
				sls[i] = sls[last]
			}
			w.stateListeners = sls[:last]
			break
		}
	}
	w.Unlock()
}

func (w *Watcher[T]) Cancel() {
	w.Lock()
	if w.cancel != nil {
		w.cancel()
		w.cancel = nil
	}
	w.callStateListeners()
	w.Unlock()
}

func (w *Watcher[T]) callStateListeners() {
	sl := make([]*StateListener, len(w.stateListeners))
	copy(sl, w.stateListeners)
	w.Unlock()
	defer w.Lock()
	for _, l := range sl {
		l.Cb()
	}
}

// HasSynced returns true if this Watcher's controller has synced, or if this watcher hasn't started yet.
func (w *Watcher[T]) HasSynced() bool {
	w.Lock()
	defer w.Unlock()
	if w.controller != nil {
		w.controller.HasSynced()
	}
	return true
}

// obj T: an object that would generate the same key as the object you
// want. For our keyfunc, you need to populate name and, if the object is
// scoped to a namespace, namespace.
func (w *Watcher[T]) Get(c context.Context, obj T) (T, bool, error) {
	w.Lock()
	defer w.Unlock()
	var zeroValue T
	if w.store == nil {
		if err := w.startOnDemand(c); err != nil {
			return zeroValue, false, err
		}
	}
	t, b, e := w.store.Get(obj)
	if t == nil {
		return zeroValue, b, e
	}
	return t.(T), b, e
}

func (w *Watcher[T]) EnsureStarted(c context.Context, cb func(bool)) error {
	w.Lock()
	defer w.Unlock()
	if w.store == nil {
		if cb != nil {
			var rl *StateListener
			rl = &StateListener{Cb: func() {
				cb(true)
				w.RemoveStateListener(rl)
			}}
			w.stateListeners = append(w.stateListeners, rl)
		}
		return w.startOnDemand(c)
	}
	if cb != nil {
		cb(false)
	}
	return nil
}

func (w *Watcher[T]) List(c context.Context) ([]T, error) {
	w.Lock()
	defer w.Unlock()
	if w.store == nil {
		if err := w.startOnDemand(c); err != nil {
			return nil, err
		}
	}
	ls := w.store.List()
	ts := make([]T, len(ls))
	for i, l := range ls {
		ts[i] = l.(T)
	}
	return ts, nil
}

// Active returns true if the watcher has been started and not yet cancelled.
func (w *Watcher[T]) Active() bool {
	w.Lock()
	active := w.cancel != nil
	w.Unlock()
	return active
}

func (w *Watcher[T]) Watch(c context.Context, ready *sync.WaitGroup) error {
	var errCh <-chan error
	func() {
		w.Lock()
		defer w.Unlock()
		c, errCh = w.createConfig(c)
	}()
	w.run(c, ready)
	select {
	case err := <-errCh:
		return err
	default:
	}
	return nil
}

func (w *Watcher[T]) startOnDemand(c context.Context) error {
	c, errCh := w.createConfig(c)
	rdy := sync.WaitGroup{}
	rdy.Add(1)
	go w.run(c, &rdy)
	rdy.Wait()
	// We're about to sit waiting for the cache. It's a bad idea to do so while we're holding the lock.
	// WaitForCacheSync isn't gonna touch any state that the lock cares about anyway, and there are
	// things going on in the background of startLocked and run that will want that lock
	w.Unlock()
	cache.WaitForCacheSync(c.Done(), w.controller.HasSynced)
	w.Lock()
	select {
	case err := <-errCh:
		if err != nil {
			return err
		}
	default:
	}
	w.callStateListeners()
	return nil
}

func (w *Watcher[T]) createConfig(c context.Context) (context.Context, <-chan error) {
	c, w.cancel = context.WithCancel(c)
	eventCh := make(chan struct{}, 10)
	go w.handleEvents(c, eventCh)

	errCh := make(chan error, 1)

	w.store = cache.NewStore(cache.DeletionHandlingMetaNamespaceKeyFunc)
	fifo := cache.NewDeltaFIFOWithOptions(cache.DeltaFIFOOptions{
		KnownObjects:          w.store,
		EmitDeltaTypeReplaced: true,
	})

	// Just creating an informer won't do, because then we cannot set the WatchErrorHandler of
	// its Config. So we create it from a Config instead, which actually plays out well because
	// we get immediate access to the Process function and can skip the ResourceEventHandlerFuncs
	var zeroValue T
	config := cache.Config{
		Queue:         fifo,
		ListerWatcher: newListerWatcher(c, w.getter, w.resource, w.namespace, w.labelSelector, w.fieldSelector),
		Process: func(obj any, _ bool) error {
			return w.process(c, obj.(cache.Deltas), eventCh)
		},
		ObjectType:       zeroValue,
		FullResyncPeriod: resyncPeriod,
		WatchErrorHandler: func(_ *cache.Reflector, err error) {
			w.errorHandler(c, err)
			if !errors.Is(err, context.Canceled) {
				select {
				case errCh <- err:
				default:
				}
			}
		},
	}
	w.controller = cache.New(&config)
	return c, errCh
}

func (w *Watcher[T]) run(c context.Context, ready *sync.WaitGroup) {
	defer dlog.Debugf(c, "Watcher for %s in %s stopped", w.resource, w.namespace)
	dlog.Debugf(c, "Watcher for %s in %s started", w.resource, w.namespace)

	// Give the Run a short time to get going so that HasSynced doesn't return true
	// because the controller hasn't started yet.
	time.AfterFunc(10*time.Millisecond, func() {
		dlog.Debug(c, "signalling done")
		ready.Done()
	})
	w.controller.Run(c.Done())
}

func (w *Watcher[T]) process(c context.Context, ds cache.Deltas, eventCh chan<- struct{}) error {
	// from oldest to newest
	for _, d := range ds {
		var verb string
		switch d.Type {
		case cache.Deleted:
			if err := w.store.Delete(d.Object); err != nil {
				return err
			}
			verb = "delete"
		default:
			old, exists, err := w.store.Get(d.Object)
			if err != nil {
				return err
			}
			if exists {
				if err = w.store.Update(d.Object); err != nil {
					return err
				}
				if w.equals != nil && w.equals(old.(T), d.Object.(T)) {
					continue
				}
				verb = "update"
			} else {
				if err = w.store.Add(d.Object); err != nil {
					return err
				}
				verb = "add"
			}
		}
		dlog.Tracef(c, "%s %s in %s (%s)", verb, w.resource, w.namespace, d.Type)
		eventCh <- struct{}{}
	}
	return nil
}

const idleTriggerDuration = 50 * time.Millisecond

// handleEvents reads the channel and broadcasts on the condition once the channel has
// been quiet for idleTriggerDuration.
func (w *Watcher[T]) handleEvents(c context.Context, eventCh <-chan struct{}) {
	idleTrigger := time.NewTimer(time.Duration(math.MaxInt64))
	idleTrigger.Stop()
	for {
		select {
		case <-c.Done():
			return
		case <-idleTrigger.C:
			idleTrigger.Stop()
			w.cond.Broadcast()
		case <-eventCh:
			idleTrigger.Reset(idleTriggerDuration)
		}
	}
}

func (w *Watcher[T]) errorHandler(c context.Context, err error) {
	switch {
	case errors.Is(err, context.Canceled):
		// Just ignore. This happens during a normal shutdown
	case apierrors.IsResourceExpired(err) || apierrors.IsGone(err):
		// Don't set LastSyncResourceVersionUnavailable - LIST call with ResourceVersion=RV already
		// has a semantic that it returns data at least as fresh as provided RV.
		// So first try to LIST with setting RV to resource version of last observed object.
		dlog.Errorf(c, "Watcher for %s in %s closed with: %v", w.resource, w.namespace, err)
	case errors.Is(err, io.EOF):
		// watch closed normally
	case errors.Is(err, io.ErrUnexpectedEOF):
		dlog.Errorf(c, "Watcher for %s in %s closed with unexpected EOF: %v", w.resource, w.namespace, err)
	default:
		se := &apierrors.StatusError{}
		if errors.As(err, &se) {
			// There's no happy ending here. The API has told us that they can't serve our request, so cancel the watcher.
			defer w.Cancel()
			st := se.Status()
			if st.Code == http.StatusForbidden {
				dlog.Errorf(c, "Watcher for %s in %s was denied access: %s", w.resource, w.namespace, st.Message)
				return
			}
			_, i := se.DebugError()
			dlog.Errorf(c, "Watcher for %s in %s failed: %v", w.resource, w.namespace, i[0])
		} else {
			dlog.Errorf(c, "Watcher for %s in %s failed: %v", w.resource, w.namespace, err)
		}
		utilruntime.HandleError(err)
	}
}

func (w *Watcher[T]) Subscribe(ctx context.Context) <-chan struct{} {
	return Subscribe(ctx, w.cond)
}
