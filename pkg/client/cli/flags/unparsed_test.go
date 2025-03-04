package flags

import (
	"slices"
	"strings"
	"testing"
)

func TestConsumeUnparsedFlagValue(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		longForm  string
		shortForm byte
		isBool    bool
		wantFound bool
		wantV     string
		wantArgs  []string
		wantErr   bool
	}{
		{
			"empty value after =",
			[]string{"--name="},
			"name",
			0,
			false,
			true,
			"",
			[]string{},
			false,
		},
		{
			"missing value at end of list",
			[]string{"--name"},
			"name",
			0,
			false,
			false,
			"",
			[]string{"--name"},
			true,
		},
		{
			"missing value before next long-form option",
			[]string{"--name", "--other"},
			"name",
			0,
			false,
			false,
			"",
			[]string{"--name", "--other"},
			true,
		},
		{
			"missing value before next short-form option",
			[]string{"--name", "-o"},
			"name",
			0,
			false,
			false,
			"",
			[]string{"--name", "-o"},
			true,
		},
		{
			"long-form=option",
			[]string{"--name=value"},
			"name",
			0,
			false,
			true,
			"value",
			[]string{},
			false,
		},
		{
			"long-form=-option-",
			[]string{"--name=-value-"},
			"name",
			0,
			false,
			true,
			"-value-",
			[]string{},
			false,
		},
		{
			"long-form option",
			[]string{"--name", "value"},
			"name",
			0,
			false,
			true,
			"value",
			[]string{},
			false,
		},
		{
			"short-form alone",
			[]string{"-i", "value", "next"},
			"interactive",
			'i',
			false,
			true,
			"value",
			[]string{"next"},
			false,
		},
		{
			"short-form boolean alone",
			[]string{"-i", "next"},
			"interactive",
			'i',
			true,
			true,
			"true",
			[]string{"next"},
			false,
		},
		{
			"short-form last",
			[]string{"-abi", "value", "next"},
			"interactive",
			'i',
			false,
			true,
			"value",
			[]string{"-ab", "next"},
			false,
		},
		{
			"short-form boolean last",
			[]string{"-abi", "-x"},
			"interactive",
			'i',
			true,
			true,
			"true",
			[]string{"-ab", "-x"},
			false,
		},
		{
			"short-form first",
			[]string{"-abi", "value", "next"},
			"actual",
			'a',
			false,
			false,
			"",
			[]string{"-abi", "value", "next"},
			true,
		},
		{
			"short-form boolean first",
			[]string{"-abi", "-x"},
			"actual",
			'a',
			true,
			true,
			"true",
			[]string{"-bi", "-x"},
			false,
		},
		{
			"short-form boolean middle",
			[]string{"-abi", "-x"},
			"best",
			'b',
			true,
			true,
			"true",
			[]string{"-ai", "-x"},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotV, gotFound, gotArgs, err := ConsumeUnparsedValue(tt.longForm, tt.shortForm, tt.isBool, tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConsumeUnparsedValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotV != tt.wantV {
				t.Errorf("ConsumeUnparsedValue() gotV = %v, want %v", gotV, tt.wantV)
			}
			if gotFound != tt.wantFound {
				t.Errorf("ConsumeUnparsedValue() found = %t, want %t", gotFound, tt.wantFound)
			}
			if !slices.Equal(gotArgs, tt.wantArgs) {
				t.Errorf("ConsumeUnparsedValue() args = %v, want %v", gotArgs, tt.wantArgs)
			}
		})
	}
}

func TestGetUnparsedFlagBoolean(t *testing.T) {
	tests := []struct {
		args    []string
		flag    string
		wantV   bool
		wantS   bool
		wantErr bool
	}{
		{
			[]string{"--rm="},
			"rm",
			false,
			false,
			true,
		},
		{
			[]string{"--rm"},
			"rm",
			true,
			true,
			false,
		},
		{
			[]string{"--rm", "--other"},
			"rm",
			true,
			true,
			false,
		},
		{
			[]string{"--rm", "-o"},
			"rm",
			true,
			true,
			false,
		},
		{
			[]string{"--rm=value"},
			"rm",
			false,
			false,
			true,
		},
		{
			[]string{"--rm=true"},
			"rm",
			true,
			true,
			false,
		},
		{
			[]string{"--rm=true"},
			"rm",
			true,
			true,
			false,
		},
		{
			[]string{"--rm=True"},
			"rm",
			true,
			true,
			false,
		},
		{
			[]string{"--rm=1"},
			"rm",
			true,
			true,
			false,
		},
		{
			[]string{"--rm=false"},
			"rm",
			false,
			true,
			false,
		},
		{
			[]string{"--rm=False"},
			"rm",
			false,
			true,
			false,
		},
		{
			[]string{"--rm=0"},
			"rm",
			false,
			true,
			false,
		},
		{
			[]string{"--do"},
			"rm",
			false,
			false,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(strings.Join(tt.args, "_"), func(t *testing.T) {
			gotV, gotS, err := GetUnparsedBoolean(tt.args, tt.flag)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetUnparsedBoolean() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotS != tt.wantS {
				t.Errorf("GetUnparsedBoolean() gotS = %v, want %v", gotS, tt.wantS)
			}
			if gotV != tt.wantV {
				t.Errorf("GetUnparsedBoolean() gotV = %v, want %v", gotV, tt.wantV)
			}
		})
	}
}
