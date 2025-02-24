package helm

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/go-json-experiment/json"
	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/chartutil"
	"helm.sh/helm/v3/pkg/cli"
	"helm.sh/helm/v3/pkg/registry"

	"github.com/datawire/dlib/dlog"
	"github.com/telepresenceio/telepresence/v2/charts"
)

const chartURL = "oci://ghcr.io/telepresenceio/telepresence-oss"

func loadCoreChart(version string) (*chart.Chart, error) {
	var buf bytes.Buffer
	if err := charts.WriteChart(charts.DirTypeTelepresence, &buf, charts.TelepresenceChartName, version); err != nil {
		return nil, err
	}
	return loader.LoadArchive(&buf)
}

func newDefaultRegistryClient(ctx context.Context) (*registry.Client, error) {
	return registry.NewClient(
		registry.ClientOptDebug(dlog.MaxLogLevel(ctx) >= dlog.LogLevelDebug),
		registry.ClientOptEnableCache(true),
		registry.ClientOptWriter(dlog.StdLogger(ctx, dlog.LogLevelDebug).Writer()),
	)
}

func withDownloadedChart(ctx context.Context, helmConfig *action.Configuration, ref, version string, f func(string) error) error {
	client, err := newDefaultRegistryClient(ctx)
	if err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "helm-")
	if err != nil {
		return err
	}
	defer func() {
		err = os.RemoveAll(dir)
		if err != nil {
			dlog.Error(ctx, err)
		}
	}()
	pull := action.NewPullWithOpts(action.WithConfig(helmConfig))
	pull.Version = version
	pull.DestDir = dir
	pull.Settings = cli.New()
	pull.SetRegistryClient(client)
	out, err := pull.Run(ref)
	dlog.Info(ctx, out)
	if err != nil {
		return err
	}
	return f(filepath.Join(dir, fmt.Sprintf("%s-%s.tgz", charts.TelepresenceChartName, version)))
}

func pullCoreChart(ctx context.Context, helmConfig *action.Configuration, ref, version string) (c *chart.Chart, err error) {
	err = withDownloadedChart(ctx, helmConfig, ref, version, func(path string) error {
		var f *os.File
		f, err = os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		c, err = loader.LoadArchive(f)
		return err
	})
	return c, err
}

func coalesceValues(ctx context.Context, req *Request) (map[string]any, error) {
	// OK, now install things.
	var providedVals map[string]any
	if len(req.ValuesJson) > 0 {
		if err := json.Unmarshal(req.ValuesJson, &providedVals); err != nil {
			return nil, fmt.Errorf("unable to parse values JSON: %w", err)
		}
	}

	var vals map[string]any
	if len(providedVals) > 0 {
		vals = chartutil.CoalesceTables(providedVals, GetValuesFunc(ctx, req))
	} else {
		// No values were provided. This means that an upgrade should retain existing values unless
		// reset-values is true.
		if req.Type == Upgrade && !req.ResetValues {
			req.ReuseValues = true
		}
		vals = GetValuesFunc(ctx, req)
	}
	return vals, nil
}

func loadOrPullChart(ctx context.Context, helmConfig *action.Configuration, req *Request) (chrt *chart.Chart, vals map[string]any, err error) {
	vals, err = coalesceValues(ctx, req)
	if err != nil {
		return nil, nil, err
	}
	if req.Version != "" {
		chrt, err = pullCoreChart(ctx, helmConfig, chartURL, req.Version)
	} else {
		chrt, err = loadCoreChart(getTrafficManagerVersion(vals))
		if err != nil {
			err = fmt.Errorf("unable to load built-in helm chart: %w", err)
		}
	}
	return chrt, vals, err
}
