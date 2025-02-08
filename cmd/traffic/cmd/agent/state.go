package agent

import (
	"context"
	"net/http"

	"github.com/blang/semver/v4"
	"github.com/puzpuzpuz/xsync/v3"
	core "k8s.io/api/core/v1"

	"github.com/datawire/dlib/dlog"
	"github.com/telepresenceio/telepresence/rpc/v2/agent"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/forwarder"
	"github.com/telepresenceio/telepresence/v2/pkg/restapi"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
)

// State reflects the current state of the agent.
type State interface {
	Config
	agent.AgentServer
	tunnel.ClientStreamProvider
	AddInterceptState(is InterceptState)
	AgentState() restapi.AgentState
	InterceptStates() []InterceptState
	HandleIntercepts(ctx context.Context, cepts []*manager.InterceptInfo) []*manager.ReviewInterceptRequest
	ManagerClient() manager.ManagerClient
	ManagerVersion() semver.Version
	SessionInfo() *manager.SessionInfo
	SetFileSharingPorts(ftp uint16, sftp uint16)
	SetManager(ctx context.Context, sessionInfo *manager.SessionInfo, manager manager.ManagerClient, version semver.Version)
	FtpPort() uint16
	SftpPort() uint16
	NewInterceptState(forwarder forwarder.Interceptor, target InterceptTarget, container string) InterceptState
	AddContainerState(containerName string, containerState ContainerState)
}

type ContainerState interface {
	State
	Name() string
	ReplaceContainer() bool
	MountPoint() string
	Env() map[string]string
}

// An InterceptState implements what's needed to intercept one target port.
type InterceptState interface {
	State
	Target() InterceptTarget
	InterceptInfo(ctx context.Context, callerID, path string, containerPort uint16, headers http.Header) (*restapi.InterceptInfo, error)
}

// State of the Traffic Agent.
type state struct {
	Config
	ftpPort          uint16
	sftpPort         uint16
	dialWatchers     *xsync.MapOf[tunnel.SessionID, chan *manager.DialRequest]
	awaitingForwards *xsync.MapOf[tunnel.SessionID, *xsync.MapOf[tunnel.ConnID, *awaitingForward]]

	// The sessionInfo and manager client are needed when forwarders establish their
	// tunnel to the traffic-manager.
	sessionInfo *manager.SessionInfo
	manager     manager.ManagerClient
	mgrVer      semver.Version

	interceptStates []InterceptState
	containerStates map[string]ContainerState
	agent.UnimplementedAgentServer
}

func (s *state) ManagerClient() manager.ManagerClient {
	return s.manager
}

func (s *state) ManagerVersion() semver.Version {
	return s.mgrVer
}

func (s *state) SetFileSharingPorts(ftp uint16, sftp uint16) {
	s.ftpPort = ftp
	s.sftpPort = sftp
}

func (s *state) SessionInfo() *manager.SessionInfo {
	return s.sessionInfo
}

func NewState(config Config) State {
	return &state{
		Config:           config,
		containerStates:  make(map[string]ContainerState),
		dialWatchers:     xsync.NewMapOf[tunnel.SessionID, chan *manager.DialRequest](),
		awaitingForwards: xsync.NewMapOf[tunnel.SessionID, *xsync.MapOf[tunnel.ConnID, *awaitingForward]](),
	}
}

func (s *state) AddInterceptState(is InterceptState) {
	s.interceptStates = append(s.interceptStates, is)
}

func (s *state) AddContainerState(containerName string, containerState ContainerState) {
	s.containerStates[containerName] = containerState
}

func (s *state) AgentState() restapi.AgentState {
	return s
}

func (s *state) InterceptStates() []InterceptState {
	return s.interceptStates
}

func (s *state) HandleIntercepts(ctx context.Context, iis []*manager.InterceptInfo) []*manager.ReviewInterceptRequest {
	var rs []*manager.ReviewInterceptRequest

	// Keep track of all InterceptInfos handled by interceptStates
	handled := make([]bool, len(iis))
	for _, ist := range s.interceptStates {
		ms := make([]*manager.InterceptInfo, 0, len(iis))
		for i, ii := range iis {
			if !handled[i] {
				ic := ist.Target()
				if ic.MatchForSpec(ii.Spec) {
					dlog.Debugf(ctx, "intercept id %s svc=%q, portId=%q matches target protocol=%s, agentPort=%d, containerPort=%d",
						ii.Id, ii.Spec.ServiceName, ii.Spec.PortIdentifier, ic.Protocol(), ic.AgentPort(), ic.ContainerPort())
					ms = append(ms, ii)
					handled[i] = true
				}
			}
		}
		rs = append(rs, ist.HandleIntercepts(ctx, ms)...)
	}

	// Collect InterceptInfos weren't handled by interceptStates
	var unhandled []*manager.InterceptInfo
	for i, ok := range handled {
		if !ok {
			unhandled = append(unhandled, iis[i])
		}
	}
	if len(unhandled) > 0 {
		// Let containerStates handle the rest.
		for _, cn := range s.containerStates {
			rs = append(rs, cn.HandleIntercepts(ctx, unhandled)...)
		}
	}
	return rs
}

func (s *state) InterceptInfo(ctx context.Context, callerID, path string, containerPort uint16, headers http.Header) (*restapi.InterceptInfo, error) {
	if containerPort == 0 && len(s.interceptStates) == 1 {
		containerPort = s.interceptStates[0].Target().ContainerPort()
	}
	for _, is := range s.interceptStates {
		ic := is.Target()
		if containerPort == ic.ContainerPort() && ic.Protocol() == core.ProtocolTCP {
			return is.InterceptInfo(ctx, callerID, path, containerPort, headers)
		}
	}

	return &restapi.InterceptInfo{}, nil
}

func (s *state) SetManager(_ context.Context, sessionInfo *manager.SessionInfo, manager manager.ManagerClient, version semver.Version) {
	s.manager = manager
	s.sessionInfo = sessionInfo
	s.mgrVer = version
}

func (s *state) FtpPort() uint16 {
	return s.ftpPort
}

func (s *state) SftpPort() uint16 {
	return s.sftpPort
}
