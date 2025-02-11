package integration_test

import (
	"net"
	"net/url"
	"strconv"
	"sync"
	"time"

	"k8s.io/client-go/tools/clientcmd"

	"github.com/telepresenceio/telepresence/v2/integration_test/itest"
)

type reconnectSuite struct {
	itest.Suite
	itest.TrafficManager
	iptablesChain string
	svc           string
}

func (s *reconnectSuite) SuiteName() string {
	return "Reconnect"
}

func init() {
	itest.AddTrafficManagerSuite("", func(h itest.TrafficManager) itest.TestingSuite {
		return &reconnectSuite{
			Suite:          itest.Suite{Harness: h},
			TrafficManager: h,
			iptablesChain:  "TEL2-CI",
			svc:            "echo-easy",
		}
	})
}

func (s *reconnectSuite) SetupSuite() {
	s.Suite.SetupSuite()

	// Add our iptables table, disable tests if we're unable to.
	err := itest.Run(s.Context(), "sudo", "iptables", "-t", "filter", "-N", s.iptablesChain)
	if err != nil {
		s.T().Skipf("Skipped, because sudo iptables fails: %v", err)
	}

	ctx := s.Context()

	s.ApplyApp(ctx, s.svc, "deploy/"+s.svc)
}

func (s *reconnectSuite) TearDownSuite() {
	ctx := s.Context()
	s.DeleteSvcAndWorkload(ctx, "deploy", s.svc)
	s.NoError(itest.Run(ctx, "sudo", "iptables", "-t", "filter", "-X", s.iptablesChain))
}

func (s *reconnectSuite) Test_ReconnectAfterNetworkFailure() {
	ctx := s.Context()
	rq := s.Require()
	kc := itest.KubeConfig(ctx)
	cfg, err := clientcmd.LoadFromFile(kc)
	rq.NoError(err)

	// Front the Kubernetes API with a socat, so that we can break the connection.
	c, ok := cfg.Contexts[cfg.CurrentContext]
	rq.True(ok)
	cl, ok := cfg.Clusters[c.Cluster]
	rq.True(ok)
	su, err := url.Parse(cl.Server)
	rq.NoError(err)
	ips, err := net.LookupIP(su.Hostname())
	rq.NoError(err)
	rq.Len(ips, 1)
	kubeIP := ips[0]

	// Set up a chain that will drop all tcp packages destined for the kubernetes server.
	rq.NoError(itest.Run(ctx, "sudo", "iptables", "-t", "filter", "-A", s.iptablesChain, "-p", "tcp",
		"-d", kubeIP.String(),
		"--dport", su.Port(),
		"-j", "DROP",
	))
	defer func() {
		// Flush (i.e. clear) the chain
		s.NoError(itest.Run(ctx, "sudo", "iptables", "-t", "filter", "-F", s.iptablesChain))
	}()

	port, cancel := itest.StartLocalHttpEchoServer(ctx, s.svc)
	defer cancel()

	s.TelepresenceConnect(ctx)
	defer itest.TelepresenceQuitOk(ctx)

	itest.TelepresenceOk(ctx, "intercept", "--port", strconv.Itoa(port), "--mount=false", s.svc)

	// Send all tcp-packets in the OUTPUT chain to our iptablesChain. This effectively interrupts all communication
	// going from the telepresence daemons to the kubernetes server.
	rq.NoError(itest.Run(ctx, "sudo", "iptables", "-t", "filter", "-I", "OUTPUT", "-p", "tcp", "-j", s.iptablesChain))

	restore := true
	wg := &sync.WaitGroup{}
	wg.Add(1)
	restoreOutput := func() {
		wg.Done()
		s.NoError(itest.Run(ctx, "sudo", "iptables", "-t", "filter", "-D", "OUTPUT", "-p", "tcp", "-j", s.iptablesChain))
		restore = false
	}

	defer func() {
		// Remove the redirect
		if restore {
			restoreOutput()
		}
	}()

	// Restore connection
	time.AfterFunc(7*time.Second, restoreOutput)

	itest.PingInterceptedEchoServer(ctx, s.svc, "80")

	wg.Wait()
}
