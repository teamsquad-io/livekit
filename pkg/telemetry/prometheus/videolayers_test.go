// Copyright 2026 TeamSquad
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package prometheus

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/livekit/protocol/livekit"
)

// renderVideoLayers turns what the registry gathers into the same
// `name{label="value",...} value` shape a Prometheus scrape of /metrics produces, so the
// assertions below pin the literal text an operator and Grafana will see.
func renderVideoLayers(t *testing.T, reg *prometheus.Registry) string {
	t.Helper()

	families, err := reg.Gather()
	require.NoError(t, err)

	var lines []string
	for _, mf := range families {
		for _, m := range mf.GetMetric() {
			var labels []string
			for _, l := range m.GetLabel() {
				labels = append(labels, fmt.Sprintf("%s=%q", l.GetName(), l.GetValue()))
			}
			lines = append(lines, fmt.Sprintf("%s{%s} %g", mf.GetName(), strings.Join(labels, ","), m.GetGauge().GetValue()))
			require.Equal(t, dto.MetricType_GAUGE, mf.GetType(), "%s must be a gauge", mf.GetName())
		}
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n") + "\n"
}

func gatherVideoLayers(t *testing.T, samples []VideoLayerSample) string {
	t.Helper()

	sampler := VideoLayerSampler(func() []VideoLayerSample { return samples })
	videoLayerSampler.Store(&sampler)
	t.Cleanup(func() { videoLayerSampler.Store(nil) })

	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, reg.Register(newVideoLayerCollector("ND_test", livekit.NodeType_SERVER)))

	return renderVideoLayers(t, reg)
}

func TestVideoLayerCollector(t *testing.T) {
	t.Run("one healthy 3-rung track and one with its top rung dead", func(t *testing.T) {
		got := gatherVideoLayers(t, []VideoLayerSample{
			{Declared: []int32{0, 1, 2}, Live: []int32{0, 1, 2}, Degraded: false},
			{Declared: []int32{0, 1, 2}, Live: []int32{0, 1}, Degraded: true},
		})

		for _, want := range []string{
			`livekit_video_layer_declared_total{layer="0",node_id="ND_test",node_type="SERVER"} 2`,
			`livekit_video_layer_declared_total{layer="1",node_id="ND_test",node_type="SERVER"} 2`,
			`livekit_video_layer_declared_total{layer="2",node_id="ND_test",node_type="SERVER"} 2`,
			`livekit_video_layer_live_total{layer="0",node_id="ND_test",node_type="SERVER"} 2`,
			`livekit_video_layer_live_total{layer="1",node_id="ND_test",node_type="SERVER"} 2`,
			`livekit_video_layer_live_total{layer="2",node_id="ND_test",node_type="SERVER"} 1`,
			`livekit_video_layer_degraded_total{node_id="ND_test",node_type="SERVER"} 1`,
		} {
			require.Contains(t, got, want)
		}
	})

	t.Run("an idle node reports the layers at zero, it does not stop reporting them", func(t *testing.T) {
		got := gatherVideoLayers(t, nil)

		// The distinction this pins down: a rung at 0 and a node that went silent must not look
		// the same in Grafana.
		require.Equal(t, SpatialLayerCount, strings.Count(got, "livekit_video_layer_live_total{"))
		require.Contains(t, got, `livekit_video_layer_live_total{layer="2",node_id="ND_test",node_type="SERVER"} 0`)
		require.Contains(t, got, `livekit_video_layer_degraded_total{node_id="ND_test",node_type="SERVER"} 0`)
	})

	t.Run("cardinality is fixed: 7 series whatever the load", func(t *testing.T) {
		many := make([]VideoLayerSample, 0, 200)
		for range 200 {
			many = append(many, VideoLayerSample{Declared: []int32{0, 1, 2}, Live: []int32{0}, Degraded: true})
		}

		reg := prometheus.NewPedanticRegistry()
		sampler := VideoLayerSampler(func() []VideoLayerSample { return many })
		videoLayerSampler.Store(&sampler)
		t.Cleanup(func() { videoLayerSampler.Store(nil) })
		require.NoError(t, reg.Register(newVideoLayerCollector("ND_test", livekit.NodeType_SERVER)))

		require.Equal(t, 7, strings.Count(renderVideoLayers(t, reg), "\n"))
	})

	t.Run("a layer index outside the simulcast range cannot widen the series set", func(t *testing.T) {
		got := gatherVideoLayers(t, []VideoLayerSample{
			{Declared: []int32{0, 7}, Live: []int32{-1, 0}, Degraded: true},
		})
		require.NotContains(t, got, `layer="7"`)
		require.NotContains(t, got, `layer="-1"`)
		require.Contains(t, got, `livekit_video_layer_live_total{layer="0",node_id="ND_test",node_type="SERVER"} 1`)
	})
}
