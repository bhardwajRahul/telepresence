package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/telepresenceio/telepresence/v2/pkg/client/cli/ann"
	"github.com/telepresenceio/telepresence/v2/pkg/client/cli/connect"
	"github.com/telepresenceio/telepresence/v2/pkg/client/cli/daemon"
	cliDocker "github.com/telepresenceio/telepresence/v2/pkg/client/cli/docker"
	"github.com/telepresenceio/telepresence/v2/pkg/client/cli/flags"
	"github.com/telepresenceio/telepresence/v2/pkg/client/cli/global"
	"github.com/telepresenceio/telepresence/v2/pkg/client/docker"
	"github.com/telepresenceio/telepresence/v2/pkg/dos"
	"github.com/telepresenceio/telepresence/v2/pkg/errcat"
	"github.com/telepresenceio/telepresence/v2/pkg/ioutil"
	"github.com/telepresenceio/telepresence/v2/pkg/proc"
)

func dockerRunCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "docker-run",
		Short: "Docker run with daemon network",
		Args:  cobra.ArbitraryArgs,
		Annotations: map[string]string{
			ann.Session: ann.Optional,
		},
		RunE:                  runDockerRunCLI,
		SilenceErrors:         true,
		SilenceUsage:          true,
		DisableFlagParsing:    true,
		DisableFlagsInUseLine: true,
		ValidArgsFunction:     cliDocker.AutocompleteRun,
	}
	return cmd
}

func findAndParseFlag(flags *pflag.FlagSet, flagName string, args []string) ([]string, error) {
	if i := slices.Index(args, "--"+flagName); i >= 0 && i+1 < len(args) {
		if err := flags.Parse(args[i : i+2]); err != nil {
			return nil, err
		}
		args = slices.Delete(args, i, i+2)
	} else if i = slices.IndexFunc(args, func(s string) bool { return strings.HasPrefix(s, "--"+flagName+"=") }); i >= 0 {
		if err := flags.Parse(args[i : i+1]); err != nil {
			return nil, err
		}
		args = slices.Delete(args, i, i+1)
	}
	return args, nil
}

func parseFlags(cmd *cobra.Command, args []string) (*cliDocker.RunFlags, []string, error) {
	// The command has all flag parsing disabled, but we must check for the global flags. Luckily, these flags do not conflict with
	// the docker run flags.
	opts := cmd.Flags()
	var err error
	args, err = findAndParseFlag(opts, global.FlagUse, args)
	if err != nil {
		return nil, nil, err
	}
	args, err = findAndParseFlag(opts, global.FlagOutput, args)
	if err != nil {
		return nil, nil, err
	}
	networkFlags, args, err := cliDocker.ParseRunFlags(args)
	if err != nil {
		return nil, nil, err
	}
	return networkFlags, args, nil
}

func runDockerRunCLI(cmd *cobra.Command, args []string) error {
	return errcat.NoDaemonLogs.New(runDockerRun(cmd, args))
}

func runDockerRun(cmd *cobra.Command, args []string) error {
	opts, args, err := parseFlags(cmd, args)
	if err != nil {
		return err
	}

	if slices.Contains(args, "--help") {
		return proc.StdCommand(cmd.Context(), cliDocker.Exe, slices.Insert(args, 0, "run")...).Run()
	}

	for _, n := range opts.Networks {
		if strings.HasPrefix(n, "container:") {
			return errors.New("this command adds the daemon container network. Adding another container network is not possible")
		}
	}

	err = connect.InitCommand(cmd)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	ud := daemon.GetUserClient(ctx)
	if ud == nil {
		return fmt.Errorf("%s requires a connection", cmd.UseLine())
	}
	if !ud.Containerized() {
		return fmt.Errorf("%s requires that --docker was used when the connection was established", cmd.UseLine())
	}

	cidFileName, err := ioutil.CreateTempName("", "docker-run*.cid")
	if err != nil {
		return err
	}

	daemonName := ud.DaemonID().ContainerName()
	ctx = dos.WithStdio(ctx, cmd)

	cc := proc.StdCommand(ctx, cliDocker.Exe, slices.Insert(args, 0, "run", "--cidfile", cidFileName, "--network", "container:"+daemonName)...)
	cc.Stdin = dos.Stdin(ctx)
	cc.Env = dos.Environ(ctx)
	tty := flags.HasOption("tty", 't', args)
	if !tty {
		proc.CreateNewProcessGroup(cc.Cmd)
	}

	defer func() {
		_ = os.Remove(cidFileName)
	}()

	err = cc.Start()
	if err != nil {
		return err
	}

	containerID, err := cliDocker.ReadContainerID(ctx, cidFileName)
	if err != nil {
		return err
	}

	ctx = docker.EnableClient(ctx)

	var exited, signalled atomic.Bool
	if !tty {
		go cliDocker.EnsureStopContainer(ctx, containerID, nil, &exited, &signalled)
	}

	if len(opts.Networks) > 0 {
		connectCancel, err := cliDocker.ConnectNetworksToDaemon(ctx, opts.Networks, daemonName)
		defer connectCancel()
		if err != nil {
			return err
		}
	}

	err = cc.Wait()
	exited.Store(true)
	if signalled.Load() {
		err = nil
	}
	return err
}
