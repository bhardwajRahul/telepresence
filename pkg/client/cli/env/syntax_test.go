package env

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSyntax_WriteEntry(t *testing.T) {
	tests := []struct {
		name  string
		e     Syntax
		key   string
		value string
		want  string
	}{
		{
			`sh A=B C`,
			SyntaxSh,
			`A`,
			`B C`,
			`A='B C'`,
		},
		{
			`sh A=B "C"`,
			SyntaxSh,
			`A`,
			`B "C"`,
			`A='B "C"'`,
		},
		{
			`sh A="B C"`,
			SyntaxSh,
			`A`,
			`"B C"`,
			`A='"B C"'`,
		},
		{
			`sh A=B 'C X'`,
			SyntaxSh,
			`A`,
			`B 'C X'`,
			`A='B '\''C X'\'`,
		},
		{
			`compose A=B 'C X'`,
			SyntaxCompose,
			`A`,
			`B 'C X'`,
			`A='B \'C X\''`,
		},
		{
			`compose A=B\nC\t"D"`,
			SyntaxCompose,
			`A`,
			"B\nC\t\"D\"",
			`A="B\nC\t\"D\""`,
		},
		{
			`sh A='B C'`,
			SyntaxSh,
			`A`,
			`'B C'`,
			`A=\''B C'\'`,
		},
		{
			`sh A=\"B\" \"C\"`,
			SyntaxSh,
			`A`,
			`\"B\" \"C\"`,
			`A='\"B\" \"C\"'`,
		},
		{
			`ps A=B C`,
			SyntaxPS,
			`A`,
			`B C`,
			`$Env:A='B C'`,
		},
		{
			`ps A='B C'`,
			SyntaxPS,
			`A`,
			`'B C'`,
			`$Env:A='''B C'''`,
		},
		{
			`ps:export A='B C'`,
			SyntaxPSExport,
			`A`,
			`'B C'`,
			`[Environment]::SetEnvironmentVariable('A', '''B C''', 'User')`,
		},
		{
			`ps:export A=B C`,
			SyntaxPSExport,
			`A`,
			`B C`,
			`[Environment]::SetEnvironmentVariable('A', 'B C', 'User')`,
		},
		{
			`ps:export A="B C"`,
			SyntaxPSExport,
			`A`,
			`"B C"`,
			`[Environment]::SetEnvironmentVariable('A', '"B C"', 'User')`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := tt.e.WriteEntry(tt.key, tt.value)
			require.NoError(t, err)
			require.Equal(t, tt.want, r)
		})
	}
}
