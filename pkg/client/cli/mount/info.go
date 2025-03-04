package mount

import (
	"context"
	"strings"

	"github.com/telepresenceio/telepresence/v2/pkg/client"
)

type Info struct {
	LocalDir  string   `json:"local_dir,omitempty"     yaml:"local_dir,omitempty"`
	RemoteDir string   `json:"remote_dir,omitempty"    yaml:"remote_dir,omitempty"`
	Error     string   `json:"error,omitempty"         yaml:"error,omitempty"`
	PodIP     string   `json:"pod_ip,omitempty"        yaml:"pod_ip,omitempty"`
	Port      int32    `json:"port,omitempty"          yaml:"port,omitempty"`
	Mounts    []string `json:"mounts,omitempty"        yaml:"mounts,omitempty"`
	ReadOnly  bool     `json:"read_only,omitempty"     yaml:"read_only,omitempty"`
}

func NewInfo(ctx context.Context, env map[string]string, ftpPort, sftpPort int32, localDir, remoteDir, podIP string, ro bool) *Info {
	var port int32
	if client.GetConfig(ctx).Intercept().UseFtp {
		port = ftpPort
	} else {
		port = sftpPort
	}
	var mounts []string
	if tpMounts := env["TELEPRESENCE_MOUNTS"]; tpMounts != "" {
		// This is a Unix path, so we cannot use filepath.SplitList
		mounts = strings.Split(tpMounts, ":")
	}
	return &Info{
		LocalDir:  localDir,
		RemoteDir: remoteDir,
		PodIP:     podIP,
		Port:      port,
		Mounts:    mounts,
		ReadOnly:  ro,
	}
}
