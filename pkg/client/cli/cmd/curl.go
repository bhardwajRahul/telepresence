package cmd

import (
	"slices"

	"github.com/spf13/cobra"

	"github.com/telepresenceio/telepresence/v2/pkg/client/cli/ann"
)

func curlCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "curl",
		Short: "curl with daemon network",
		Args:  cobra.ArbitraryArgs,
		Annotations: map[string]string{
			ann.Session: ann.Optional,
		},
		RunE:                  runCurl,
		ValidArgsFunction:     cobra.NoFileCompletions,
		SilenceErrors:         true,
		SilenceUsage:          true,
		DisableFlagParsing:    true,
		DisableFlagsInUseLine: true,
		DisableSuggestions:    true,
	}
	return cmd
}

func runCurl(cmd *cobra.Command, args []string) error {
	return runDockerRun(cmd, slices.Insert(args, 0, "--rm", "curlimages/curl"))
}
