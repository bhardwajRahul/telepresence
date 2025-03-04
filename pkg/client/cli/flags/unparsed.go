package flags

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// GetUnparsedValue parses the given args for a matching option. The string value of the option and a boolean
// indicating if the option was found. The function may also return an error for a malformed
// option. Typically a non-bool option that lacks a value.
func GetUnparsedValue(longForm string, shortForm byte, isBool bool, args []string) (string, bool, error) {
	v, found, _, err := ConsumeUnparsedValue(longForm, shortForm, isBool, slices.Clone(args))
	return v, found, err
}

// ConsumeUnparsedValue parses the given args for a matching option. If found, the option and value
// is removed from args. The string value of the option, a boolean indicating if the option was found,
// the possibly modified args array is returned. The function may also return an error for a malformed
// option. Typically a non-bool option that lacks a value.
func ConsumeUnparsedValue(longForm string, shortForm byte, isBool bool, args []string) (string, bool, []string, error) {
	var ixf func(string) bool
	if longForm != "" {
		longFlag := "--" + longForm
		longFlagV := longFlag + "="
		if shortForm != 0 {
			ixf = func(s string) bool {
				return s == longFlag || strings.HasPrefix(s, longFlagV) || len(s) >= 2 && s[0] == '-' && s[1] != '-' && strings.IndexByte(s, shortForm) > 0
			}
		} else {
			ixf = func(s string) bool {
				return s == longFlag || strings.HasPrefix(s, longFlagV)
			}
		}
	} else {
		if shortForm == 0 {
			return "", false, args, nil
		}
		ixf = func(s string) bool {
			return len(s) >= 2 && s[0] == '-' && s[1] != '-' && strings.IndexByte(s, shortForm) > 0
		}
	}
	flagIndex := slices.IndexFunc(args, ixf)
	if flagIndex == -1 {
		return "", false, args, nil
	}

	flag, val, valFound := strings.Cut(args[flagIndex], "=")
	fl := len(flag)
	if flag[1] == '-' || fl == 2 {
		// long form, or short-form as last character in option string. We treat those
		// the same
		switch {
		case valFound:
			// --flag=val
			if isBool && val == "" {
				return "", false, args, fmt.Errorf("flag %q requires a value", flag)
			}
			return val, true, slices.Delete(args, flagIndex, flagIndex+1), nil
		case isBool:
			// --flag
			return "true", true, slices.Delete(args, flagIndex, flagIndex+1), nil
		case flagIndex+1 < len(args) && !strings.HasPrefix(args[flagIndex+1], "-"):
			// --flag val
			val = args[flagIndex+1]
			return val, true, slices.Delete(args, flagIndex, flagIndex+2), nil
		default:
			return "", false, args, fmt.Errorf("flag %q requires a value", flag)
		}
	}

	// Short form with several characters.
	if flag[fl-1] == shortForm {
		// short-form with the found flag last in the list.
		flag = flag[:fl-1]
		switch {
		case valFound:
			// return value of option 'z' and replace "-xyz=val" with "-xy"
			args[flagIndex] = flag
			return val, true, args, nil
		case isBool:
			// return true for option 'z' and replace "-xyz" with "-xy"
			args[flagIndex] = flag
			return "true", true, args, nil
		case flagIndex+1 < len(args) && !strings.HasPrefix(args[flagIndex+1], "-"):
			// return value of option 'z' and replace "-xyz val" with "-xy"
			args[flagIndex] = flag
			val = args[flagIndex+1]
			return val, true, slices.Delete(args, flagIndex+1, flagIndex+2), nil
		default:
			return "", false, args, fmt.Errorf(`flag "-%c" requires a value`, shortForm)
		}
	}

	// short-form, but the found flag not last in the option string
	if !isBool {
		return "", false, args, fmt.Errorf(`flag "-%c" requires a value`, shortForm)
	}
	flag = strings.Replace(flag, string([]byte{shortForm}), "", 1)
	if valFound {
		// "-xzy=val" with "-xy=val"
		args[flagIndex] = flag + "=" + val
	} else {
		// "-xzy=val" with "-xy"
		args[flagIndex] = flag
	}
	return "true", true, args, nil
}

// GetUnparsedBoolean returns the value of a boolean flag that has been provided after a "--" on the command
// line, and hence hasn't been parsed as a normal flag. Typical use case is:
//
//	telepresence intercept --docker-run ... -- --rm
func GetUnparsedBoolean(args []string, flag string) (bool, bool, error) {
	v, found, err := GetUnparsedValue(flag, 0, true, args)
	if !found || err != nil {
		return false, false, err
	}
	bv, err := strconv.ParseBool(v)
	if err != nil {
		return false, false, err
	}
	return bv, true, nil
}

func HasOption(longForm string, shortForm byte, args []string) bool {
	longFlag := "--" + longForm
	return slices.ContainsFunc(args, func(s string) bool {
		return s == longFlag || len(s) >= 2 && s[0] == '-' && s[1] != '-' && strings.IndexByte(s, shortForm) > 0
	})
}
