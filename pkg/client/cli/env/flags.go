package env

import (
	"github.com/spf13/pflag"
)

type Flags struct {
	File   string // --env-file
	Syntax Syntax // --env-syntax
	JSON   string // --env-json
}

func (f *Flags) AddFlags(flagSet *pflag.FlagSet) {
	flagSet.StringVarP(&f.File, "env-file", "e", "", ``+
		`Also emit the remote environment to an file. The syntax used in the file can be determined using flag --env-syntax`)

	flagSet.Var(&f.Syntax, "env-syntax", `Syntax used for env-file. One of `+SyntaxUsage())

	flagSet.StringVarP(&f.JSON, "env-json", "j", "", `Also emit the remote environment to a file as a JSON blob.`)
}

func (f *Flags) PerhapsWrite(env map[string]string) error {
	if f.File != "" {
		if err := f.Syntax.writeFile(f.File, env); err != nil {
			return err
		}
	}
	if f.JSON != "" {
		if err := SyntaxJSON.writeFile(f.JSON, env); err != nil {
			return err
		}
	}
	return nil
}
