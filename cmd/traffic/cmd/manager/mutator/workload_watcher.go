package mutator

import (
	"context"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/datawire/dlib/dlog"
	"github.com/datawire/k8sapi/pkg/k8sapi"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/managerutil"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/agentmap"
	"github.com/telepresenceio/telepresence/v2/pkg/workload"
)

func (c *configWatcher) watchWorkloads(ctx context.Context, ix cache.SharedIndexInformer) error {
	_, err := ix.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj any) {
				if wl, ok := workload.FromAny(obj); ok && len(wl.GetOwnerReferences()) == 0 {
					c.updateWorkload(ctx, wl, nil, workload.GetWorkloadState(wl))
				}
			},
			DeleteFunc: func(obj any) {
				if wl, ok := workload.FromAny(obj); ok {
					if len(wl.GetOwnerReferences()) == 0 {
						c.deleteWorkload(ctx, wl)
					}
				} else if dfsu, ok := obj.(*cache.DeletedFinalStateUnknown); ok {
					if wl, ok = workload.FromAny(dfsu.Obj); ok && len(wl.GetOwnerReferences()) == 0 {
						c.deleteWorkload(ctx, wl)
					}
				}
			},
			UpdateFunc: func(oldObj, newObj any) {
				if wl, ok := workload.FromAny(newObj); ok && len(wl.GetOwnerReferences()) == 0 {
					if oldWl, ok := workload.FromAny(oldObj); ok {
						c.updateWorkload(ctx, wl, oldWl, workload.GetWorkloadState(wl))
					}
				}
			},
		})
	return err
}

func (c *configWatcher) deleteWorkload(ctx context.Context, wl k8sapi.Workload) {
	scx, err := c.Get(ctx, wl.GetName(), wl.GetNamespace())
	if err != nil {
		dlog.Errorf(ctx, "Failed to get sidecar config: %v", err)
	} else if scx != nil {
		err = c.Delete(ctx, wl.GetName(), wl.GetNamespace())
		if err != nil {
			dlog.Errorf(ctx, "Failed to delete sidecar config: %v", err)
		}
	}
}

func (c *configWatcher) updateWorkload(ctx context.Context, wl, oldWl k8sapi.Workload, state workload.State) {
	if state == workload.StateFailure {
		return
	}
	tpl := wl.GetPodTemplate()
	ia, ok := tpl.Annotations[workload.InjectAnnotation]
	if !ok {
		return
	}
	if oldWl != nil && cmp.Equal(oldWl.GetPodTemplate(), tpl,
		cmpopts.IgnoreFields(meta.ObjectMeta{}, "Namespace", "UID", "ResourceVersion", "CreationTimestamp", "DeletionTimestamp"),
		cmpopts.IgnoreMapEntries(func(k, _ string) bool {
			return k == workload.AnnRestartedAt
		})) {
		return
	}

	switch ia {
	case "enabled":
		img := managerutil.GetAgentImage(ctx)
		if img == "" {
			return
		}
		cfg, err := agentmap.GeneratorConfigFunc(img)
		if err != nil {
			dlog.Error(ctx, err)
			return
		}
		var scx agentconfig.SidecarExt
		if oldWl != nil {
			scx, err = c.Get(ctx, wl.GetName(), wl.GetNamespace())
			if err != nil {
				dlog.Errorf(ctx, "Failed to get sidecar config: %v", err)
				return
			}
		}
		action := "Generating"
		if scx == nil {
			action = "Regenerating"
		}
		dlog.Debugf(ctx, "%s config entry for %s %s.%s", action, wl.GetKind(), wl.GetName(), wl.GetNamespace())

		scx, err = cfg.Generate(ctx, wl, scx)
		if err != nil {
			if strings.Contains(err.Error(), "unable to find") {
				if err = c.remove(ctx, wl.GetName(), wl.GetNamespace()); err != nil {
					dlog.Error(ctx, err)
				}
			} else {
				dlog.Error(ctx, err)
			}
		}
		if err = c.store(ctx, scx); err != nil {
			dlog.Error(ctx, err)
		}
	case "false", "disabled":
		c.deleteWorkload(ctx, wl)
	}
}
