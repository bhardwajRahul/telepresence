package mutator

import (
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
)

const (
	InjectAnnotation      = agentconfig.DomainPrefix + "inject-" + agentconfig.ContainerName
	ServiceNameAnnotation = agentconfig.DomainPrefix + "inject-service-name"
)
