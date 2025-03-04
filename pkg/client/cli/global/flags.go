package global

import (
	"github.com/spf13/pflag"
)

const (
	FlagDocker   = "docker"
	FlagContext  = "context"
	FlagUse      = "use"
	FlagOutput   = "output"
	FlagNoReport = "no-report"
)

func Flags(hasKubeFlags bool) *pflag.FlagSet {
	flags := pflag.NewFlagSet("", 0)
	if !hasKubeFlags {
		// Add deprecated global connect and docker flags.
		flags.String(FlagContext, "", "")
		flags.Lookup(FlagContext).Hidden = true
		flags.Bool(FlagDocker, false, "")
		flags.Lookup(FlagDocker).Hidden = true
	}
	flags.Bool(FlagNoReport, false, "")
	f := flags.Lookup(FlagNoReport)
	f.Hidden = true
	f.Deprecated = "not used"
	flags.String(FlagUse, "", "Match expression that uniquely identifies the daemon container")
	flags.String(FlagOutput, "default", "Set the output format, supported values are 'json', 'yaml', and 'default'")
	return flags
}
