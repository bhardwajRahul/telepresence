package config

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-json-experiment/json"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/yaml"

	"github.com/datawire/dlib/dlog"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/managerutil"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

const (
	clientConfigFileName   = "client.yaml"
	agentEnvConfigFileName = "agent-env.yaml"
	cfgConfigMapName       = "traffic-manager"
)

type WatcherCallback func(watch.EventType, runtime.Object) error

type Watcher interface {
	Run(ctx context.Context) error
	GetClientConfigYaml() []byte
	GetAgentEnv() AgentEnv
}

type AgentEnv struct {
	Excluded []string `json:"excluded,omitempty"`
}

type config struct {
	sync.RWMutex
	namespace string

	clientYAML []byte
	agentEnv   AgentEnv
}

func NewWatcher(namespace string) Watcher {
	return &config{
		namespace: namespace,
	}
}

func (c *config) Run(ctx context.Context) error {
	dlog.Infof(ctx, "Started watcher for ConfigMap %s", cfgConfigMapName)
	defer dlog.Infof(ctx, "Ended watcher for ConfigMap %s", cfgConfigMapName)

	// The WatchConfig will perform a http GET call to the kubernetes API server, and that connection will not remain open forever
	// so when it closes, the watch must start over. This goes on until the context is cancelled.
	api := k8sapi.GetK8sInterface(ctx).CoreV1()
	for ctx.Err() == nil {
		w, err := api.ConfigMaps(c.namespace).Watch(ctx, meta.SingleObject(meta.ObjectMeta{Name: cfgConfigMapName}))
		if err != nil {
			return fmt.Errorf("unable to create configmap watcher for %s.%s: %v", cfgConfigMapName, c.namespace, err)
		}
		if !c.configMapEventHandler(ctx, w.ResultChan()) {
			return nil
		}
	}
	return nil
}

func (c *config) configMapEventHandler(ctx context.Context, evCh <-chan watch.Event) bool {
	for {
		select {
		case <-ctx.Done():
			return false
		case event, ok := <-evCh:
			if !ok {
				return true // restart watcher
			}
			switch event.Type {
			case watch.Deleted:
				if m, ok := event.Object.(*core.ConfigMap); ok {
					dlog.Debugf(ctx, "%s %s", event.Type, m.Name)
					c.refreshFile(ctx, nil)
				}
			case watch.Added, watch.Modified:
				if m, ok := event.Object.(*core.ConfigMap); ok {
					dlog.Debugf(ctx, "%s %s", event.Type, m.Name)
					c.refreshFile(ctx, m.Data)
				}
			}
		}
	}
}

var AmendClientConfigFunc = AmendClientConfig //nolint:gochecknoglobals // extension point

func AmendClientConfig(ctx context.Context, cfg client.Config) bool {
	env := managerutil.GetEnv(ctx)
	if len(env.ManagedNamespaces) > 0 {
		dlog.Debugf(ctx, "Checking if Augment mapped namespaces with %d managed namespaces", len(env.ManagedNamespaces))
		if len(cfg.Cluster().MappedNamespaces) == 0 {
			dlog.Debugf(ctx, "Augment mapped namespaces with %d managed namespaces", len(env.ManagedNamespaces))
			cfg.Cluster().MappedNamespaces = env.ManagedNamespaces
		}
		return true
	}
	return false
}

func (c *config) refreshFile(ctx context.Context, data map[string]string) {
	c.Lock()
	if yml, ok := data[clientConfigFileName]; ok {
		c.clientYAML = []byte(yml)
		cfg, err := client.ParseConfigYAML(ctx, clientConfigFileName, c.clientYAML)
		if err != nil {
			dlog.Errorf(ctx, "failed to unmarshal YAML from %s: %v", clientConfigFileName, err)
		} else if AmendClientConfigFunc(ctx, cfg) {
			c.clientYAML = []byte(cfg.String())
			dlog.Debugf(ctx, "Refreshed client config: %s", yml)
		}
	} else {
		c.clientYAML = nil
		dlog.Debugf(ctx, "Cleared client config")
	}

	c.agentEnv = AgentEnv{}
	if yml, ok := data[agentEnvConfigFileName]; ok {
		data, err := yaml.YAMLToJSON([]byte(yml))
		if err == nil {
			err = json.Unmarshal(data, &c.agentEnv)
		}
		if err != nil {
			dlog.Errorf(ctx, "failed to unmarshal YAML from %s: %v", agentEnvConfigFileName, err)
		}
		dlog.Debugf(ctx, "Refreshed agent-env: %s", yml)
	} else {
		dlog.Debugf(ctx, "Cleared agent-env")
	}
	c.Unlock()
}

func (c *config) GetAgentEnv() AgentEnv {
	return c.agentEnv
}

func (c *config) GetClientConfigYaml() (ret []byte) {
	c.RLock()
	ret = c.clientYAML
	c.RUnlock()
	return
}
