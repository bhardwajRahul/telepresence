package connect

import (
	"context"
	"regexp"
	"strconv"

	empty "google.golang.org/protobuf/types/known/emptypb"

	"github.com/telepresenceio/telepresence/v2/pkg/client/cli/daemon"
	"github.com/telepresenceio/telepresence/v2/pkg/dos"
	"github.com/telepresenceio/telepresence/v2/pkg/errcat"
	"github.com/telepresenceio/telepresence/v2/pkg/version"
)

var validPrerelRx = regexp.MustCompile(`^[a-z]+\.\d+$`)

func versionCheck(ctx context.Context, daemonBinary string) error {
	if debug, err := strconv.ParseBool(dos.Getenv(ctx, "TELEPRESENCE_DEBUG")); err == nil && debug {
		return nil
	}

	// Ensure that the already running daemons have the correct version
	userD := daemon.GetUserClient(ctx)
	uv := userD.Semver()
	if userD.Containerized() {
		// The user-daemon is remote (in a docker container, most likely). Compare the major, minor, and patch. Only
		// compare pre-release if it's rc.X or test.X, and don't check if the binaries match.
		cv := version.Structured
		if cv.Major == uv.Major && cv.Minor == uv.Minor && cv.Patch == uv.Patch {
			if len(cv.Pre) != 1 {
				// Prerelease does not consist of exactly one element, so it either doesn't exist or we don't care about it.
				return nil
			}
			if pv := cv.Pre[0].VersionStr; !validPrerelRx.MatchString(pv) || len(uv.Pre) == 1 && pv == uv.Pre[0].VersionStr {
				// Either not a prerelease that we care about comparing, or the prerelease was exactly equal.
				return nil
			}
		}
		return errcat.User.Newf("version mismatch. Client %s != remote user daemon %s", version.Version, uv)
	}
	if !version.Structured.EQ(uv) {
		// OSS Version mismatch. We never allow this
		return errcat.User.Newf("version mismatch. Client %s != user daemon %s, please run 'telepresence quit -s' and reconnect",
			version.Version, uv)
	}
	if daemonBinary != "" && userD.Executable() != daemonBinary {
		return errcat.User.Newf("executable mismatch. Connector using %s, configured to use %s, please run 'telepresence quit -s' and reconnect",
			userD.Executable(), daemonBinary)
	}
	vr, err := userD.RootDaemonVersion(ctx, &empty.Empty{})
	if err == nil && version.Version != vr.Version {
		return errcat.User.Newf("version mismatch. Client %s != Root Daemon %s, please run 'telepresence quit -s' and reconnect",
			version.Version, vr.Version)
	}
	return nil
}
