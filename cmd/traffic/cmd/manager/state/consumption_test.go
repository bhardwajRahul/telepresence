package state

import (
	"time"

	"github.com/stretchr/testify/assert"
)

func (s *suiteState) TestRefreshSessionConsumptionMetrics() {
	// given
	now := time.Now()
	session1 := &ClientSession{}
	session3 := &ClientSession{}
	s.state.clients.Store("session-1", session1)
	s.state.agents.Store("session-2", &AgentSession{})
	s.state.clients.Store("session-3", session3)
	session1.consumptionMetrics = &SessionConsumptionMetrics{}
	session1.consumptionMetrics.connectDuration.Store(int64(42 * time.Second))
	session1.consumptionMetrics.lastUpdate.Store(now.Add(-time.Minute).UnixNano())

	// staled metric
	session3.consumptionMetrics = &SessionConsumptionMetrics{}
	session3.consumptionMetrics.connectDuration.Store(int64(36 * time.Second))
	session3.consumptionMetrics.lastUpdate.Store(now.Add(time.Duration(-ConnectionStaleTimeout) - time.Minute).UnixNano())

	// when
	s.state.RefreshSessionConsumptionMetrics("session-1")
	s.state.RefreshSessionConsumptionMetrics("session-2") // should not fail even if it's an agent session.
	s.state.RefreshSessionConsumptionMetrics("session-3") // should not refresh a stale metric.
	s.state.RefreshSessionConsumptionMetrics("session-4") // doesn't exist but shouldn't fail.

	// then
	ccs1, _ := s.state.clients.Load("session-1")
	ccs3, _ := s.state.clients.Load("session-3")

	assert.Len(s.T(), s.state.GetAllSessionConsumptionMetrics(), 2)
	assert.True(s.T(), ccs1.ConsumptionMetrics().ConnectDuration() > 42*time.Second)
	assert.Equal(s.T(), 41*time.Second, ccs3.ConsumptionMetrics().ConnectDuration())
}
