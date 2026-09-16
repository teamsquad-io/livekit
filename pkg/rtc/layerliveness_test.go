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
	require.False(t, VideoLayerLiveness{Declared: []int32{0, 1, 2}, Live: []int32{0, 1, 2}}.Degraded())
	require.True(t, VideoLayerLiveness{Declared: []int32{0, 1, 2}, Live: []int32{0, 1}}.Degraded())
	require.True(t, VideoLayerLiveness{Declared: []int32{0}, Live: nil}.Degraded())
	// a rung delivering that was never declared is not a degradation
	require.False(t, VideoLayerLiveness{Declared: []int32{0}, Live: []int32{0, 1}}.Degraded())
}

func TestParticipantInfoWithLiveLayers(t *testing.T) {
	newPI := func() *livekit.ParticipantInfo {
		return &livekit.ParticipantInfo{
			Sid:      "PA_pub",
			Identity: "publisher",
			Tracks: []*livekit.TrackInfo{
				simulcastTrackInfo(),
				{Sid: "TR_audio", Type: livekit.TrackType_AUDIO, MimeType: "audio/opus"},
			},
		}
	}

	t.Run("keeps only the rungs currently delivering, in both layer lists", func(t *testing.T) {
		pi := newPI()
		out := ParticipantInfoWithLiveLayers(pi, map[livekit.TrackID][]int32{"TR_video": {0, 1}})

		require.Len(t, out.Tracks[0].Layers, 2)
		require.Equal(t, int32(0), out.Tracks[0].Layers[0].SpatialLayer)
		require.Equal(t, int32(1), out.Tracks[0].Layers[1].SpatialLayer)
		require.Len(t, out.Tracks[0].Codecs[0].Layers, 2)

		// CONTROL POSITIVO: the input is untouched, so the internal model the allocator and the
		// signalling path read cannot be affected by an API response.
		require.Len(t, pi.Tracks[0].Layers, 3)
		require.Len(t, pi.Tracks[0].Codecs[0].Layers, 3)
	})

	t.Run("nothing delivering empties the ladder", func(t *testing.T) {
		out := ParticipantInfoWithLiveLayers(newPI(), map[livekit.TrackID][]int32{"TR_video": {}})
		require.Empty(t, out.Tracks[0].Layers)
		require.Empty(t, out.Tracks[0].Codecs[0].Layers)
	})

	t.Run("a track with no reading is left alone", func(t *testing.T) {
		out := ParticipantInfoWithLiveLayers(newPI(), map[livekit.TrackID][]int32{"TR_other": {0}})
		require.Len(t, out.Tracks[0].Layers, 3)
	})

	t.Run("audio is never touched", func(t *testing.T) {
		out := ParticipantInfoWithLiveLayers(newPI(), map[livekit.TrackID][]int32{"TR_audio": {}})
		require.Equal(t, "TR_audio", out.Tracks[1].Sid)
		require.Empty(t, out.Tracks[1].Layers)
		require.Len(t, out.Tracks[0].Layers, 3)
	})

	t.Run("an empty reading set is a no-op", func(t *testing.T) {
		pi := newPI()
		require.Same(t, pi, ParticipantInfoWithLiveLayers(pi, nil))
	})
}

// fakeLiveTrack is a types.MediaTrack that also reports liveness, which is what *MediaTrack is
// in production. The embedded fake supplies the rest of the (large) interface.
type fakeLiveTrack struct {
	*typesfakes.FakeMediaTrack
	ti   *livekit.TrackInfo
	live []int32
}

func (f *fakeLiveTrack) TrackInfo() *livekit.TrackInfo { return f.ti }
func (f *fakeLiveTrack) LiveSpatialLayers() []int32    { return f.live }

func newFakeLiveTrack(ti *livekit.TrackInfo, kind livekit.TrackType, live []int32) *fakeLiveTrack {
	fake := &typesfakes.FakeMediaTrack{}
	fake.IDReturns(livekit.TrackID(ti.Sid))
	fake.KindReturns(kind)
	return &fakeLiveTrack{FakeMediaTrack: fake, ti: ti, live: live}
}

func TestVideoLayerLivenessOf(t *testing.T) {
	t.Run("video track reporting liveness", func(t *testing.T) {
		track := newFakeLiveTrack(simulcastTrackInfo(), livekit.TrackType_VIDEO, []int32{0, 1})
		reading, ok := VideoLayerLivenessOf(track)
		require.True(t, ok)
		require.Equal(t, livekit.TrackID("TR_video"), reading.TrackID)
		require.Equal(t, []int32{0, 1, 2}, reading.Declared)
		require.Equal(t, []int32{0, 1}, reading.Live)
		require.True(t, reading.Degraded())
	})

	t.Run("audio is skipped", func(t *testing.T) {
		ti := &livekit.TrackInfo{Sid: "TR_audio", Type: livekit.TrackType_AUDIO, MimeType: "audio/opus"}
		_, ok := VideoLayerLivenessOf(newFakeLiveTrack(ti, livekit.TrackType_AUDIO, nil))
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
