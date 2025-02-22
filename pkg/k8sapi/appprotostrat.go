package k8sapi

import (
	"fmt"

	"github.com/go-json-experiment/json"
	"github.com/go-json-experiment/json/jsontext"
)

// AppProtocolStrategy specifies how the application protocol for a service port is determined
// in case the service.spec.ports.appProtocol is not set.
type AppProtocolStrategy int

var apsNames = [...]string{"http2Probe", "portName", "http", "http2"} //nolint:gochecknoglobals // constant names

const (
	// Http2Probe means never guess. Choose HTTP/1.1 or HTTP/2 by probing (this is the default behavior).
	Http2Probe AppProtocolStrategy = iota

	// PortName means trust educated guess based on port name when appProtocol is missing and perform a http2 probe
	// if no such guess can be made.
	PortName

	// Http means just assume HTTP/1.1.
	Http

	// Http2 means just assume HTTP/2.
	Http2
)

func (aps AppProtocolStrategy) String() string {
	return apsNames[aps]
}

func NewAppProtocolStrategy(s string) (AppProtocolStrategy, error) {
	for i, n := range apsNames {
		if s == n {
			return AppProtocolStrategy(i), nil
		}
	}
	return 0, fmt.Errorf("invalid AppProtcolStrategy: %q", s)
}

func (aps AppProtocolStrategy) MarshalJSONTo(out *jsontext.Encoder, opts json.Options) error {
	return json.MarshalEncode(out, aps.String(), opts)
}

func (aps *AppProtocolStrategy) EnvDecode(val string) (err error) {
	var as AppProtocolStrategy
	if val == "" {
		as = Http2Probe
	} else if as, err = NewAppProtocolStrategy(val); err != nil {
		return err
	}
	*aps = as
	return nil
}

func (aps *AppProtocolStrategy) UnmarshalJSONFrom(in *jsontext.Decoder, opts json.Options) error {
	var s string
	err := json.UnmarshalDecode(in, &s, opts)
	if err == nil {
		err = aps.EnvDecode(s)
	}
	return err
}
