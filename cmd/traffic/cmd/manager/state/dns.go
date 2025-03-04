package state

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/datawire/dlib/dlog"
	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/managerutil"
	"github.com/telepresenceio/telepresence/v2/pkg/dnsproxy"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
)

// We can use our own Rcodes in the range that is reserved for private use

// RcodeNoAgents means that no agents replied to the DNS request.
const RcodeNoAgents = 3841

// AgentsLookupDNS will send the given request to all agents currently intercepted by the client identified with
// the clientSessionID, it will then wait for results to arrive, collect those results, and return the result.
func (s *state) AgentsLookupDNS(ctx context.Context, clientSessionID tunnel.SessionID, request *rpc.DNSRequest) (dnsproxy.RRs, int, error) {
	rs := s.agentsLookup(ctx, clientSessionID, request)
	if len(rs) == 0 {
		return nil, RcodeNoAgents, nil
	}
	var bestRRs dnsproxy.RRs
	bestRcode := math.MaxInt
	for _, r := range rs {
		rrs, rCode, err := dnsproxy.FromRPC(r)
		if err != nil {
			return nil, rCode, err
		}
		if rCode < bestRcode {
			bestRcode = rCode
			if len(rrs) > len(bestRRs) {
				bestRRs = rrs
			}
		}
	}
	return bestRRs, bestRcode, nil
}

// PostLookupDNSResponse receives lookup responses from an agent and places them in the channel
// that corresponds to the lookup request.
func (s *state) PostLookupDNSResponse(ctx context.Context, response *rpc.DNSAgentResponse) {
	rid := requestId(response.GetRequest())
	as := s.GetAgent(tunnel.SessionID(response.GetSession().SessionId))
	if as != nil {
		if rch, ok := as.dnsResponses.Load(rid); ok {
			select {
			case rch <- response.GetResponse():
			default:
			}
		}
	} else {
		dlog.Debugf(ctx, "attempted to post lookup response failed because there was no recipient. ID=%s", rid)
	}
}

func (s *state) WatchLookupDNS(agentSessionID tunnel.SessionID) <-chan *rpc.DNSRequest {
	as := s.GetAgent(agentSessionID)
	if as != nil {
		return as.dnsRequests
	}
	return nil
}

func (s *state) agentsLookup(ctx context.Context, clientSessionID tunnel.SessionID, request *rpc.DNSRequest) []*rpc.DNSResponse {
	agents := s.getAgentsInterceptedByClient(clientSessionID)
	if len(agents) == 0 {
		if client, ok := s.clients.Load(clientSessionID); ok {
			if client.Namespace == managerutil.GetEnv(ctx).ManagerNamespace {
				// Let traffic-manager do the lookup
				return nil
			}
			agents = s.getAgentsInNamespace(client.Namespace)
		}
	}
	aCount := len(agents)
	if aCount == 0 {
		return nil
	}
	if aCount > 2 {
		// Send the lookup to max two agents
		aCount = 2
	}

	rsBuf := make(chan *rpc.DNSResponse, aCount)

	timout, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(aCount)
	for aID := range agents {
		go func(aID tunnel.SessionID) {
			rid := requestId(request)
			defer func() {
				s.endLookup(aID, rid)
				wg.Done()
			}()

			rsCh := s.startLookup(aID, rid, request)
			if rsCh == nil {
				return
			}
			select {
			case <-timout.Done():
			case rs, ok := <-rsCh:
				if ok {
					rsBuf <- rs
				}
			}
		}(aID)
		aCount--
		if aCount == 0 {
			break
		}
	}
	wg.Wait() // wait for timeout or that all agents have responded
	bz := len(rsBuf)
	rs := make([]*rpc.DNSResponse, bz)
	for i := 0; i < bz; i++ {
		rs[i] = <-rsBuf
	}
	return rs
}

func (s *state) startLookup(agentSessionID tunnel.SessionID, rid string, request *rpc.DNSRequest) <-chan *rpc.DNSResponse {
	as := s.GetAgent(agentSessionID)
	if as == nil {
		return nil
	}
	rch, _ := as.dnsResponses.LoadOrCompute(rid, func() chan *rpc.DNSResponse {
		return make(chan *rpc.DNSResponse)
	})

	// The as.dnsRequests channel may be closed at this point, so guard for panic. And no, we can't read that
	// channel to check if it is closed, because we're not the intended recipient.
	defer func() {
		if r := recover(); r != nil {
			select {
			case <-rch:
				// rch is already closed
			default:
				close(rch)
			}
		}
	}()
	as.dnsRequests <- request
	return rch
}

func (s *state) endLookup(agentSessionID tunnel.SessionID, rid string) {
	if as := s.GetAgent(agentSessionID); as != nil {
		if rch, loaded := as.dnsResponses.LoadAndDelete(rid); loaded {
			close(rch)
		}
	}
}

func requestId(request *rpc.DNSRequest) string {
	return fmt.Sprintf("%s:%s:%d", request.Session.SessionId, request.Name, request.Type)
}
