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
	"strconv"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/livekit/protocol/livekit"
)

// SpatialLayerCount is how many simulcast spatial layers a track can have. It mirrors
// buffer.DefaultMaxLayerSpatial+1 and is duplicated rather than imported so this package keeps
// its current property of importing nothing from pkg/ — pkg/rtc carries a gate test that fails
// if the two ever drift.
const SpatialLayerCount = 3

// VideoLayerSample is the reading of ONE published video track, produced by
// rtc.VideoLayerLiveness. Declared is the ladder announced at publish time, Live the subset
// currently delivering, Degraded whether at least one declared rung is not.
type VideoLayerSample struct {
	Declared []int32
	Live     []int32
	Degraded bool
}

// VideoLayerSampler returns one sample per published video track on this node.
type VideoLayerSampler func() []VideoLayerSample

var videoLayerSampler atomic.Pointer[VideoLayerSampler]

// InitVideoLayerStats registers the video layer gauges and the sampler that feeds them.
//
// AST-427. CARDINALITY, THE REASON THE LABELS LOOK LIKE THIS: the obvious shape would be a
// gauge labelled {room, participant, track, layer}. Measured on the production fleet
// (Prometheus, 2026-09-16): peak 36 concurrent rooms per origin over 30 days, but ~1,666 rooms
// CLOSED per origin per 30 days, and Prometheus retains 15 days — so a per-track label set
// would create roughly 833 rooms x 3 origins x 3 layers ~= 7.5k new series per retention
// window against a TSDB whose entire head is 19,885 series today. A track SID is minted anew
// on every publish, so the churn is unbounded by construction, and room names are broadcaster
// identities, on a /metrics that is reachable on the public face of these boxes.
//
// So the only label here is the spatial layer: 3 live + 3 declared + 1 degraded = 7 series per
// node, constant forever, and no identity ever becomes a label. The per-stream truth is served
// by the RoomService API filter instead, which has no cardinality at all.
//
// The gauges are computed AT SCRAPE TIME by a collector rather than kept up to date by a
// ticker: with nobody scraping, this costs exactly nothing, and what is scraped can never be
// stale.
func InitVideoLayerStats(nodeID string, nodeType livekit.NodeType, sampler VideoLayerSampler) {
	if sampler == nil {
		return
	}
	videoLayerSampler.Store(&sampler)
	prometheus.MustRegister(newVideoLayerCollector(nodeID, nodeType))
}

func newVideoLayerCollector(nodeID string, nodeType livekit.NodeType) *videoLayerCollector {
	constLabels := prometheus.Labels{"node_id": nodeID, "node_type": nodeType.String()}
	return &videoLayerCollector{
		live: prometheus.NewDesc(
			livekitNamespace+"_video_layer_live_total",
			"Published video tracks the SFU is currently receiving packets on for this spatial layer.",
			[]string{"layer"}, constLabels,
		),
		declared: prometheus.NewDesc(
			livekitNamespace+"_video_layer_declared_total",
			"Published video tracks that declared this spatial layer when they published.",
			[]string{"layer"}, constLabels,
		),
		degraded: prometheus.NewDesc(
			livekitNamespace+"_video_layer_degraded_total",
			"Published video tracks with at least one declared spatial layer that is not delivering.",
			nil, constLabels,
		),
	}
}

type videoLayerCollector struct {
	live     *prometheus.Desc
	declared *prometheus.Desc
	degraded *prometheus.Desc
}

func (c *videoLayerCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.live
	ch <- c.declared
	ch <- c.degraded
}

func (c *videoLayerCollector) Collect(ch chan<- prometheus.Metric) {
	var live, declared [SpatialLayerCount]float64
	var degraded float64

	if p := videoLayerSampler.Load(); p != nil {
		for _, s := range (*p)() {
			for _, l := range s.Live {
				if l >= 0 && int(l) < SpatialLayerCount {
					live[l]++
				}
			}
			for _, l := range s.Declared {
				if l >= 0 && int(l) < SpatialLayerCount {
					declared[l]++
				}
			}
			if s.Degraded {
				degraded++
			}
		}
	}

	// Every layer is emitted on every scrape, including the ones at zero: a series that
	// disappears when a rung dies is indistinguishable in Grafana from a node that stopped
	// reporting, and telling those two apart is the whole point of this metric.
	for l := range SpatialLayerCount {
		label := strconv.Itoa(l)
		ch <- prometheus.MustNewConstMetric(c.live, prometheus.GaugeValue, live[l], label)
		ch <- prometheus.MustNewConstMetric(c.declared, prometheus.GaugeValue, declared[l], label)
	}
	ch <- prometheus.MustNewConstMetric(c.degraded, prometheus.GaugeValue, degraded)
}
