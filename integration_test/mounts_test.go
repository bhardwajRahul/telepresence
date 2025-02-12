package integration_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/datawire/dlib/dtime"
	"github.com/telepresenceio/telepresence/v2/integration_test/itest"
	"github.com/telepresenceio/telepresence/v2/pkg/dos"
)

type mountsSuite struct {
	itest.Suite
	itest.TrafficManager
}

func (s *mountsSuite) SuiteName() string {
	return "Mounts"
}

func init() {
	itest.AddConnectedSuite("", func(h itest.TrafficManager) itest.TestingSuite {
		return &mountsSuite{
			Suite:          itest.Suite{Harness: h},
			TrafficManager: h,
		}
	})
}

func (s *mountsSuite) SetupSuite() {
	if s.IsCI() && runtime.GOOS == "darwin" {
		s.T().Skip("Mount tests don't run on darwin due to macFUSE issues")
		return
	}
	s.Suite.SetupSuite()
}

func (s *mountsSuite) createDeployment() (string, []byte) {
	ctx := s.Context()
	k8s := filepath.Join("testdata", "k8s")
	rq := s.Require()
	pvcPath := filepath.Join(k8s, "local-pvc.yaml")
	rq.NoError(s.Kubectl(ctx, "apply", "-f", pvcPath))
	mf, err := itest.ReadTemplate(ctx, filepath.Join(k8s, "hello-pv-volume.goyaml"), &itest.PersistentVolume{
		Name:           "hello",
		MountDirectory: "/data",
	})
	rq.NoError(err)
	rq.NoError(s.Kubectl(dos.WithStdin(ctx, bytes.NewReader(mf)), "apply", "-f", "-"))
	return pvcPath, mf
}

func (s *mountsSuite) deleteDeployment(pvcPath string, mf []byte) {
	ctx := s.Context()
	rq := s.Require()
	rq.NoError(s.Kubectl(dos.WithStdin(ctx, bytes.NewReader(mf)), "delete", "-f", "-"))
	rq.NoError(s.Kubectl(ctx, "delete", "-f", pvcPath))
}

func (s *mountsSuite) Test_MountWrite() {
	if runtime.GOOS == "windows" {
		s.T().SkipNow()
	}
	pvcPath, mf := s.createDeployment()
	defer s.deleteDeployment(pvcPath, mf)

	ctx := s.Context()
	mountPoint := filepath.Join(s.T().TempDir(), "mnt")
	itest.TelepresenceOk(ctx, "intercept", "hello", "--mount", mountPoint, "--port", "80:80")
	time.Sleep(2 * time.Second)

	content := "hello world\n"
	path := filepath.Join(mountPoint, "data", "hello.txt")
	rq := s.Require()
	rq.NoError(os.WriteFile(path, []byte(content), 0o644))
	itest.TelepresenceOk(ctx, "leave", "hello")
	time.Sleep(2 * time.Second)

	mountPoint = filepath.Join(s.T().TempDir(), "data")
	itest.TelepresenceOk(ctx, "intercept", "hello", "--mount", mountPoint, "--port", "80:80")
	defer itest.TelepresenceOk(ctx, "leave", "hello")
	s.CapturePodLogs(ctx, "hello", "traffic-agent", s.AppNamespace())

	path = filepath.Join(mountPoint, "data", "hello.txt")
	data, err := os.ReadFile(path)
	rq.NoError(err)
	rq.Equal(content, string(data))
}

func (s *mountsSuite) Test_MountReadOnly() {
	if runtime.GOOS == "windows" {
		s.T().SkipNow()
	}
	pvcPath, mf := s.createDeployment()
	defer s.deleteDeployment(pvcPath, mf)
	ctx := s.Context()

	mountPoint := filepath.Join(s.T().TempDir(), "mnt")
	itest.TelepresenceOk(ctx, "intercept", "hello", "--mount", mountPoint+":ro", "--port", "80:80")
	defer itest.TelepresenceOk(ctx, "leave", "hello")
	time.Sleep(2 * time.Second)
	s.Require().Error(os.WriteFile(filepath.Join(mountPoint, "data", "hello.txt"), []byte("hello world\n"), 0o644))
}

// Test_CollidingMounts tests that multiple mounts from several containers are managed correctly
// by the traffic-agent and that an intercept of a container mounts the expected volumes.
func (s *mountsSuite) Test_CollidingMounts() {
	ctx := s.Context()
	s.ApplyTemplate(ctx, filepath.Join("testdata", "k8s", "hello-w-volumes.goyaml"), nil)
	defer s.DeleteSvcAndWorkload(ctx, "deploy", "hello")

	type lm struct {
		name       string
		svcPort    int
		mountPoint string
	}
	var tests []lm
	if runtime.GOOS == "windows" {
		tests = []lm{
			{
				"one",
				80,
				"O:",
			},
			{
				"two",
				81,
				"T:",
			},
		}
	} else {
		tempDir := s.T().TempDir()
		tests = []lm{
			{
				"one",
				80,
				filepath.Join(tempDir, "one"),
			},
			{
				"two",
				81,
				filepath.Join(tempDir, "two"),
			},
		}
	}

	for i, tt := range tests {
		s.Run(tt.name, func() {
			ctx := s.Context()
			require := s.Require()
			stdout := itest.TelepresenceOk(ctx, "intercept", "hello", "--mount", tt.mountPoint, "--port", fmt.Sprintf("%d:%d", tt.svcPort, tt.svcPort))
			defer itest.TelepresenceOk(ctx, "leave", "hello")
			require.Contains(stdout, "Using Deployment hello")
			if i == 0 {
				s.CapturePodLogs(ctx, "hello", "traffic-agent", s.AppNamespace())
			} else {
				// Mounts are sometimes slow
				dtime.SleepWithContext(ctx, 3*time.Second)
			}
			ns, err := os.ReadFile(filepath.Join(tt.mountPoint, "var", "run", "secrets", "kubernetes.io", "serviceaccount", "namespace"))
			require.NoError(err)
			require.Equal(s.AppNamespace(), string(ns))
			token, err := os.ReadFile(filepath.Join(tt.mountPoint, "var", "run", "secrets", "kubernetes.io", "serviceaccount", "token"))
			require.NoError(err)
			require.True(len(token) > 0)
		})
	}
}
