package k8sapi

import "k8s.io/client-go/tools/clientcmd"

type ClientConfigProvider interface {
	ClientConfig() (clientcmd.ClientConfig, error)
}
