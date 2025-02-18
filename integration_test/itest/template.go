package itest

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/Masterminds/sprig/v3"
	core "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

type ContainerPort struct {
	Number   int
	Name     string
	Protocol core.Protocol
}

type ServicePort struct {
	Number     int
	Name       string
	Protocol   core.Protocol
	TargetPort string
}

type Generic struct {
	Name           string
	Annotations    map[string]string
	Labels         map[string]string
	Environment    []core.EnvVar
	TargetPort     string
	ServicePorts   []ServicePort
	ContainerPort  int
	ContainerPorts []ContainerPort
	Image          string
	Registry       string
	ServiceAccount string
}

type PersistentVolume struct {
	// Deployment and service name
	Name string

	// MountDirectory in the pod
	MountDirectory string
}

func OpenTemplate(ctx context.Context, name string, data any) (io.Reader, error) {
	b, err := ReadTemplate(ctx, name, data)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(b), nil
}

func ReadTemplate(ctx context.Context, name string, data any) ([]byte, error) {
	fnMap := sprig.FuncMap()
	fnMap["toYaml"] = toYAML
	tpl, err := template.New("").Funcs(fnMap).ParseFiles(filepath.Join(GetWorkingDir(ctx), name))
	if err != nil {
		return nil, err
	}
	wr := bytes.Buffer{}
	if err = tpl.ExecuteTemplate(&wr, filepath.Base(name), data); err != nil {
		return nil, err
	}
	return wr.Bytes(), nil
}

func EvalTemplate(content string, data any) ([]byte, error) {
	fnMap := sprig.FuncMap()
	fnMap["toYaml"] = toYAML
	tpl, err := template.New("embedded").Funcs(fnMap).Parse(content)
	if err != nil {
		return nil, err
	}
	wr := bytes.Buffer{}
	if err = tpl.ExecuteTemplate(&wr, "embedded", data); err != nil {
		return nil, err
	}
	return wr.Bytes(), nil
}

// toYAML is direct copy of toYaml in the helm.sh/helm/v3/pkg/engine package.
func toYAML(v interface{}) string {
	data, err := yaml.Marshal(v)
	if err != nil {
		// Swallow errors inside of a template.
		return ""
	}
	return strings.TrimSuffix(string(data), "\n")
}
