package state

import (
	"context"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/datawire/dlib/dlog"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/mutator"
	testdata "github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/test"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/watchable"
	"github.com/telepresenceio/telepresence/v2/pkg/log"
	"github.com/telepresenceio/telepresence/v2/pkg/workload"
)

type suiteState struct {
	suite.Suite

	ctx   context.Context
	state *state
}

func (s *suiteState) SetupTest() {
	s.ctx = dlog.NewTestContext(s.T(), false)
	s.state = &state{
		backgroundCtx:    s.ctx,
		intercepts:       watchable.NewMap[string, *Intercept](interceptEqual, time.Millisecond),
		agents:           watchable.NewMap[string, *manager.AgentInfo](agentsEqual, time.Millisecond),
		clients:          xsync.NewMapOf[string, *manager.ClientInfo](),
		workloadWatchers: xsync.NewMapOf[string, workload.Watcher](),
		sessions:         xsync.NewMapOf[string, SessionState](),
		timedLogLevel:    log.NewTimedLevel("debug", log.SetLevel),
		llSubs:           newLoglevelSubscribers(),
	}
}

type FakeClock struct {
	When int
}

func (fc *FakeClock) Now() time.Time {
	base := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	offset := time.Duration(fc.When) * time.Second
	return base.Add(offset)
}

func getAllAgents(st *state) []*manager.AgentInfo {
	agents := make([]*manager.AgentInfo, 0, st.agents.Size())
	st.agents.Range(func(_ string, a *manager.AgentInfo) bool {
		agents = append(agents, a)
		return true
	})
	return agents
}

func getAgentsByName(st *state, name, namespace string) (agents []*manager.AgentInfo) {
	st.agents.Range(func(_ string, a *manager.AgentInfo) bool {
		if a.Name == name && a.Namespace == namespace {
			agents = append(agents, a)
		}
		return true
	})
	return agents
}

func (s *suiteState) TestStateInternal() {
	ctx := context.Background()

	testAgents := testdata.GetTestAgents(s.T())
	testClients := testdata.GetTestClients(s.T())

	s.T().Run("agents", func(t *testing.T) {
		a := assertNew(t)

		helloAgent := testAgents["hello"]
		helloProAgent := testAgents["helloPro"]
		demoAgent1 := testAgents["demo1"]
		demoAgent2 := testAgents["demo2"]

		clock := &FakeClock{}
		m := mutator.NewWatcher()
		ctx = mutator.WithMap(ctx, m)
		st := NewState(ctx).(*state)

		h, err := st.AddAgent(ctx, helloAgent, clock.Now())
		require.NoError(t, err)
		hp, err := st.AddAgent(ctx, helloProAgent, clock.Now())
		require.NoError(t, err)
		d1, err := st.AddAgent(ctx, demoAgent1, clock.Now())
		require.NoError(t, err)
		d2, err := st.AddAgent(ctx, demoAgent2, clock.Now())
		require.NoError(t, err)

		a.Equal(helloAgent, st.GetAgent(h))
		a.Equal(helloProAgent, st.GetAgent(hp))
		a.Equal(demoAgent1, st.GetAgent(d1))
		a.Equal(demoAgent2, st.GetAgent(d2))

		agents := getAllAgents(st)
		a.Len(agents, 4)
		a.Contains(agents, helloAgent)
		a.Contains(agents, helloProAgent)
		a.Contains(agents, demoAgent1)
		a.Contains(agents, demoAgent2)

		agents = getAgentsByName(st, "hello", "default")
		a.Len(agents, 1)
		a.Contains(agents, helloAgent)

		agents = getAgentsByName(st, "hello-pro", "default")
		a.Len(agents, 1)
		a.Contains(agents, helloProAgent)

		agents = getAgentsByName(st, "demo", "default")
		a.Len(agents, 2)
		a.Contains(agents, demoAgent1)
		a.Contains(agents, demoAgent2)

		agents = getAgentsByName(st, "does-not-exist", "default")
		a.Len(agents, 0)

		agents = getAgentsByName(st, "hello", "does-not-exist")
		a.Len(agents, 0)
	})

	s.T().Run("presence-redundant", func(t *testing.T) {
		a := assertNew(t)

		clock := &FakeClock{}
		epoch := clock.Now()
		s := NewState(ctx)

		c1 := s.AddClient(testClients["alice"], clock.Now())
		c2 := s.AddClient(testClients["bob"], clock.Now())
		c3 := s.AddClient(testClients["cameron"], clock.Now())

		a.NotNil(s.GetClient(c1))
		a.NotNil(s.GetClient(c2))
		a.NotNil(s.GetClient(c3))
		a.Nil(s.GetClient("asdf"))

		a.Equal(testClients["alice"], s.GetClient(c1))

		clock.When = 10

		a.True(s.MarkSession(&manager.RemainRequest{Session: &manager.SessionInfo{SessionId: c1}}, clock.Now()))
		a.True(s.MarkSession(&manager.RemainRequest{Session: &manager.SessionInfo{SessionId: c2}}, clock.Now()))
		a.False(s.MarkSession(&manager.RemainRequest{Session: &manager.SessionInfo{SessionId: "asdf"}}, clock.Now()))

		moment := epoch.Add(5 * time.Second)
		s.ExpireSessions(ctx, moment, moment)

		a.NotNil(s.GetClient(c1))
		a.NotNil(s.GetClient(c2))
		a.Nil(s.GetClient(c3))

		clock.When = 20

		a.True(s.MarkSession(&manager.RemainRequest{Session: &manager.SessionInfo{SessionId: c1}}, clock.Now()))
		a.True(s.MarkSession(&manager.RemainRequest{Session: &manager.SessionInfo{SessionId: c2}}, clock.Now()))
		a.False(s.MarkSession(&manager.RemainRequest{Session: &manager.SessionInfo{SessionId: c3}}, clock.Now()))

		moment = epoch.Add(5 * time.Second)
		s.ExpireSessions(ctx, moment, moment)

		a.NotNil(s.GetClient(c1))
		a.NotNil(s.GetClient(c2))
		a.Nil(s.GetClient(c3))

		s.RemoveSession(ctx, c2)

		a.NotNil(s.GetClient(c1))
		a.Nil(s.GetClient(c2))
		a.Nil(s.GetClient(c3))

		a.True(s.MarkSession(&manager.RemainRequest{Session: &manager.SessionInfo{SessionId: c1}}, clock.Now()))
		a.False(s.MarkSession(&manager.RemainRequest{Session: &manager.SessionInfo{SessionId: c2}}, clock.Now()))
		a.False(s.MarkSession(&manager.RemainRequest{Session: &manager.SessionInfo{SessionId: c3}}, clock.Now()))
	})
}

func (s *suiteState) TestAddClient() {
	// given
	now := time.Now()

	// when
	s.state.AddClient(&manager.ClientInfo{
		Name:      "my-client",
		InstallId: "1234",
		Product:   "5668",
		Version:   "2.14.2",
		ApiKey:    "xxxx",
	}, now)

	// then
	assert.Equal(s.T(), 1, s.state.sessions.Size())
}

func (s *suiteState) TestRemoveSession() {
	// given
	now := time.Now()
	s.state.sessions.Store("session-1", newClientSessionState(s.ctx, now))
	s.state.sessions.Store("session-2", newAgentSessionState(s.ctx, now))

	// when
	s.state.RemoveSession(s.ctx, "session-1")
	s.state.RemoveSession(s.ctx, "session-2") // won't fail trying to delete consumption.

	// then
	assert.Equal(s.T(), s.state.sessions.Size(), 0)
}

func TestSuiteState(testing *testing.T) {
	suite.Run(testing, new(suiteState))
}
