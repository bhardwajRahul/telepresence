package cmd

import (
	"github.com/spf13/cobra"

	"github.com/telepresenceio/telepresence/v2/pkg/client/cli/ann"
	"github.com/telepresenceio/telepresence/v2/pkg/client/cli/ingest"
	"github.com/telepresenceio/telepresence/v2/pkg/client/cli/intercept"
)

func ingestCmd() *cobra.Command {
	ic := &ingest.Command{}
	cmd := &cobra.Command{
		Use:   "ingest [flags] <workload> [-- <command with arguments...>]",
		Args:  cobra.MinimumNArgs(1),
		Short: "Ingest a container",
		Annotations: map[string]string{
			ann.Session:           ann.Required,
			ann.UpdateCheckFormat: ann.Tel2,
		},
		SilenceUsage:      true,
		SilenceErrors:     true,
		RunE:              ic.Run,
		ValidArgsFunction: intercept.ValidArgs, // a list that this command shares with intercept
	}
	ic.AddFlags(cmd)
	return cmd
}
