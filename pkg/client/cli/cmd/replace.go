package cmd

import (
	"github.com/spf13/cobra"

	"github.com/telepresenceio/telepresence/v2/pkg/client/cli/ann"
	"github.com/telepresenceio/telepresence/v2/pkg/client/cli/intercept"
)

func replaceCmd() *cobra.Command {
	ic := &intercept.Command{}
	cmd := &cobra.Command{
		Use:   "replace [flags] <replace_base_name> [-- <command with arguments...>]",
		Args:  cobra.MinimumNArgs(1),
		Short: "Replace a container",
		Annotations: map[string]string{
			ann.Session:           ann.Required,
			ann.UpdateCheckFormat: ann.Tel2,
		},
		SilenceUsage:      true,
		SilenceErrors:     true,
		RunE:              ic.RunReplace,
		ValidArgsFunction: intercept.ValidArgs,
	}
	ic.AddReplaceFlags(cmd)
	return cmd
}
