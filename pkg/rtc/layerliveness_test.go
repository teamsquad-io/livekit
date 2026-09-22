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

package rtc

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/livekit/protocol/livekit"

	"github.com/livekit/livekit-server/pkg/rtc/types/typesfakes"
	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	"github.com/livekit/livekit-server/pkg/telemetry/prometheus"
)

func simulcastTrackInfo() *livekit.TrackInfo {
	return &livekit.TrackInfo{
		Sid:      "TR_video",
		Type:     livekit.TrackType_VIDEO,
		MimeType: "video/h264",
		Layers: []*livekit.VideoLayer{
			{Quality: livekit.VideoQuality_LOW, SpatialLayer: 0, Width: 320, Height: 180},
			{Quality: livekit.VideoQuality_MEDIUM, SpatialLayer: 1, Width: 640, Height: 360},
			{Quality: livekit.VideoQuality_HIGH, SpatialLayer: 2, Width: 1280, Height: 720},
		},
		Codecs: []*livekit.SimulcastCodecInfo{{
			MimeType: "video/h264",
			Layers: []*livekit.VideoLayer{
				{Quality: livekit.VideoQuality_LOW, SpatialLayer: 0, Width: 320, Height: 180},
				{Quality: livekit.VideoQuality_MEDIUM, SpatialLayer: 1, Width: 640, Height: 360},
				{Quality: livekit.VideoQuality_HIGH, SpatialLayer: 2, Width: 1280, Height: 720},
			},
		}},
	}
}

func TestDeclaredSpatialLayers(t *testing.T) {
	t.Run("simulcast ladder", func(t *testing.T) {
		require.Equal(t, []int32{0, 1, 2}, DeclaredSpatialLayers(simulcastTrackInfo()))
	})

	t.Run("a track with no layer list is a single layer, reported as 0", func(t *testing.T) {
		ti := &livekit.TrackInfo{Sid: "TR_one", Type: livekit.TrackType_VIDEO, MimeType: "video/h264"}
		require.Equal(t, []int32{0}, DeclaredSpatialLayers(ti))
	})

	t.Run("OFF rungs are not part of the ladder", func(t *testing.T) {
		ti := simulcastTrackInfo()
		ti.Layers[2].Quality = livekit.VideoQuality_OFF
		ti.Codecs[0].Layers[2].Quality = livekit.VideoQuality_OFF
		require.Equal(t, []int32{0, 1}, DeclaredSpatialLayers(ti))
	})

	t.Run("nil", func(t *testing.T) {
		require.Nil(t, DeclaredSpatialLayers(nil))
	})
}

func TestVideoLayerLivenessDegraded(t *testing.T) {
	live := func(layers ...int32) []LiveLayer {
		out := make([]LiveLayer, 0, len(layers))
		for _, l := range layers {
			out = append(out, LiveLayer{SpatialLayer: l, SinceMs: 1000, Transitions: 1})
		}
		return out
	}

	require.False(t, VideoLayerLiveness{Declared: []int32{0, 1, 2}, Live: live(0, 1, 2), LiveKnown: true}.Degraded())
	require.True(t, VideoLayerLiveness{Declared: []int32{0, 1, 2}, Live: live(0, 1), LiveKnown: true}.Degraded())
	require.True(t, VideoLayerLiveness{Declared: []int32{0}, Live: nil, LiveKnown: true}.Degraded())
	// a rung delivering that was never declared is not a degradation
	require.False(t, VideoLayerLiveness{Declared: []int32{0}, Live: live(0, 1), LiveKnown: true}.Degraded())

	// AST-449. NO READING IS NOT A DEGRADATION. This is the whole point of LiveKnown: a track
	// we cannot read anything about must not be counted as one whose encoder stopped, or the
	// degraded gauge climbs every time a track is between receivers.
	require.False(t, VideoLayerLiveness{Declared: []int32{0, 1, 2}, Live: nil, LiveKnown: false}.Degraded())
	require.Nil(t, VideoLayerLiveness{Declared: []int32{0}, Live: live(0), LiveKnown: false}.LiveSpatialLayers())
	require.Equal(t, []int32{0, 2}, VideoLayerLiveness{Live: live(0, 2), LiveKnown: true}.LiveSpatialLayers())
}

// fakeLiveTrack is a types.MediaTrack that also reports liveness, which is what *MediaTrack is
// in production. The embedded fake supplies the rest of the (large) interface.
type fakeLiveTrack struct {
	*typesfakes.FakeMediaTrack
	ti    *livekit.TrackInfo
	live  []LiveLayer
	known bool
}

func (f *fakeLiveTrack) TrackInfo() *livekit.TrackInfo        { return f.ti }
func (f *fakeLiveTrack) LiveVideoLayers() ([]LiveLayer, bool) { return f.live, f.known }

func newFakeLiveTrack(ti *livekit.TrackInfo, kind livekit.TrackType, live []LiveLayer, known bool) *fakeLiveTrack {
	fake := &typesfakes.FakeMediaTrack{}
	fake.IDReturns(livekit.TrackID(ti.Sid))
	fake.KindReturns(kind)
	return &fakeLiveTrack{FakeMediaTrack: fake, ti: ti, live: live, known: known}
}

func TestVideoLayerLivenessOf(t *testing.T) {
	t.Run("video track reporting liveness", func(t *testing.T) {
		track := newFakeLiveTrack(simulcastTrackInfo(), livekit.TrackType_VIDEO, []LiveLayer{
			{SpatialLayer: 0, SinceMs: 4200, Transitions: 3},
			{SpatialLayer: 1, SinceMs: 900, Transitions: 11},
		}, true)
		reading, ok := VideoLayerLivenessOf(track)
		require.True(t, ok)
		require.Equal(t, livekit.TrackID("TR_video"), reading.TrackID)
		require.Equal(t, []int32{0, 1, 2}, reading.Declared)
		require.True(t, reading.LiveKnown)
		require.Equal(t, []int32{0, 1}, reading.LiveSpatialLayers())
		require.Equal(t, int64(900), reading.Live[1].SinceMs)
		require.Equal(t, uint64(11), reading.Live[1].Transitions)
		require.True(t, reading.Degraded())
	})

	// AST-449. The two zero-length answers are DIFFERENT facts and the reading has to carry
	// which one it is, or a consumer tears down a subscription on a track it cannot see.
	t.Run("nothing delivering is not the same as no reading", func(t *testing.T) {
		nothing, ok := VideoLayerLivenessOf(
			newFakeLiveTrack(simulcastTrackInfo(), livekit.TrackType_VIDEO, []LiveLayer{}, true))
		require.True(t, ok)
		require.True(t, nothing.LiveKnown)
		require.Empty(t, nothing.Live)
		require.True(t, nothing.Degraded())

		unknown, ok := VideoLayerLivenessOf(
			newFakeLiveTrack(simulcastTrackInfo(), livekit.TrackType_VIDEO, nil, false))
		require.True(t, ok)
		require.False(t, unknown.LiveKnown)
		require.Empty(t, unknown.Live)
		require.False(t, unknown.Degraded())
	})

	t.Run("audio is skipped", func(t *testing.T) {
		ti := &livekit.TrackInfo{Sid: "TR_audio", Type: livekit.TrackType_AUDIO, MimeType: "audio/opus"}
		_, ok := VideoLayerLivenessOf(newFakeLiveTrack(ti, livekit.TrackType_AUDIO, nil, false))
		require.False(t, ok)
	})

	t.Run("a track that cannot report liveness is skipped", func(t *testing.T) {
		fake := &typesfakes.FakeMediaTrack{}
		fake.KindReturns(livekit.TrackType_VIDEO)
		_, ok := VideoLayerLivenessOf(fake)
		require.False(t, ok)
	})

	t.Run("nil", func(t *testing.T) {
		_, ok := VideoLayerLivenessOf(nil)
		require.False(t, ok)
	})
}

// TestSpatialLayerCountMatchesBuffer pins the constant duplicated in the prometheus package
// (which imports nothing from pkg/ on purpose) to the SFU's own bound. If a future LiveKit
// release grows a fourth spatial layer, this fails instead of the gauge silently dropping it.
func TestSpatialLayerCountMatchesBuffer(t *testing.T) {
	require.Equal(t, int(buffer.DefaultMaxLayerSpatial)+1, prometheus.SpatialLayerCount)
}
