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

// AST-429 (the black screen it explains is AST-427). The SFU already knows which spatial layers of a published video track are alive:
// StreamTrackerManager keeps `availableLayers`, fed by one StreamTracker per layer that
// declares a layer started/stopped from observed packets. Today that knowledge never leaves
// the process — it reaches the MOS scorer and the stream allocator and stops there.
//
// A consumer outside the process (our packager, an operator looking at Grafana) has only
// TrackInfo, which is the CONFIGURATION PHOTO taken when the publisher published and never
// moves afterwards. Announcing a rung from that photo after the encoder stopped emitting it
// is how a viewer ends up on a black screen.
//
// This file is the ONE accessor for that knowledge. Both presentations — the RoomService API
// response filter and the Prometheus gauges — read from here, so they cannot drift apart.
//
// It is READ-ONLY by construction: nothing here is called from the packet path, nothing here
// is called from addAvailableLayer/removeAvailableLayer, and nothing here writes any state.
// The only lock it takes is the read lock GetLayeredBitrate() already takes.

package rtc

import (
	"slices"

	"github.com/livekit/protocol/codecs/mime"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/utils"

	"github.com/livekit/livekit-server/pkg/rtc/types"
	"github.com/livekit/livekit-server/pkg/sfu/buffer"
)

// VideoLayerLiveness is a point-in-time reading for ONE published video track.
//
// Declared is the ladder the publisher announced when it published (TrackInfo). Live is the
// subset of it the SFU is receiving packets on right now. Both are spatial layer indices, the
// same space `VideoLayer.SpatialLayer` and the forwarder use.
type VideoLayerLiveness struct {
	TrackID  livekit.TrackID
	Declared []int32
	Live     []int32
}

// Degraded reports whether at least one declared rung is not delivering. This is the signal
// that matters operationally: the ladder a consumer would announce is wider than the ladder
// the encoder is actually producing.
func (l VideoLayerLiveness) Degraded() bool {
	for _, d := range l.Declared {
		if !slices.Contains(l.Live, d) {
			return true
		}
	}
	return false
}

// liveVideoLayersReader is the slice of a published track this file needs. It is declared at
// the consumer, structurally, on purpose: adding a method to types.MediaTrack would force a
// regeneration of every counterfeiter fake in the tree for a read-only accessor.
// *MediaTrackReceiver satisfies it, and therefore so does *MediaTrack.
type liveVideoLayersReader interface {
	TrackInfo() *livekit.TrackInfo
	LiveSpatialLayers() []int32
}

// The assertion is the gate, not documentation. *MediaTrack is the ONLY concrete type that
// ever reaches UpTrackManager.publishedTracks (participant.go, AddPublishedTrack), so if a
// rename ever breaks this the type assertion in VideoLayerLivenessOf would silently start
// returning false and both the metric and the API filter would go quietly blank. This turns
// that into a build failure.
var (
	_ liveVideoLayersReader = (*MediaTrackReceiver)(nil)
	_ liveVideoLayersReader = (*MediaTrack)(nil)
)

// VideoLayerLivenessOf reads one published track. It returns false for anything that is not a
// video track able to report liveness (audio, data, a fake in a test).
func VideoLayerLivenessOf(track types.MediaTrack) (VideoLayerLiveness, bool) {
	if track == nil || track.Kind() != livekit.TrackType_VIDEO {
		return VideoLayerLiveness{}, false
	}

	reader, ok := track.(liveVideoLayersReader)
	if !ok {
		return VideoLayerLiveness{}, false
	}

	return VideoLayerLiveness{
		TrackID:  track.ID(),
		Declared: DeclaredSpatialLayers(reader.TrackInfo()),
		Live:     reader.LiveSpatialLayers(),
	}, true
}

// VideoLayerLivenessOfParticipant reads every published video track of a participant.
func VideoLayerLivenessOfParticipant(p types.LocalParticipant) []VideoLayerLiveness {
	if p == nil {
		return nil
	}

	tracks := p.GetPublishedTracks()
	out := make([]VideoLayerLiveness, 0, len(tracks))
	for _, t := range tracks {
		if reading, ok := VideoLayerLivenessOf(t); ok {
			out = append(out, reading)
		}
	}
	return out
}

// LiveVideoLayersByTrack is the shape the API response filter wants: track SID -> live layers.
// A track present in the map with an EMPTY slice means "nothing is delivering", which is a
// different statement from a track absent from the map ("no reading available").
func LiveVideoLayersByTrack(p types.LocalParticipant) map[livekit.TrackID][]int32 {
	readings := VideoLayerLivenessOfParticipant(p)
	if len(readings) == 0 {
		return nil
	}

	out := make(map[livekit.TrackID][]int32, len(readings))
	for _, r := range readings {
		out[r.TrackID] = r.Live
	}
	return out
}

// DeclaredSpatialLayers returns the spatial layers a TrackInfo declares, deduplicated and
// sorted. A video track with no layer list is a single-layer track, which the SFU treats as
// spatial layer 0 (buffer.GetSpatialLayerForVideoQuality does exactly this), so it is reported
// as [0] rather than as an empty ladder.
func DeclaredSpatialLayers(ti *livekit.TrackInfo) []int32 {
	if ti == nil {
		return nil
	}

	layers := buffer.GetVideoLayersForMimeType(mime.NormalizeMimeType(ti.MimeType), ti)
	out := make([]int32, 0, len(layers))
	for _, l := range layers {
		if l == nil || l.Quality == livekit.VideoQuality_OFF {
			continue
		}
		if !slices.Contains(out, l.SpatialLayer) {
			out = append(out, l.SpatialLayer)
		}
	}
	if len(out) == 0 {
		return []int32{0}
	}

	slices.Sort(out)
	return out
}

// ParticipantInfoWithLiveLayers returns a COPY of pi whose video layer lists carry only the
// rungs currently delivering.
//
// IT IS AN API RESPONSE FILTER AND NOTHING ELSE. It is applied to the proto the RoomService
// psrpc handlers are about to return, NEVER to the TrackInfo the allocator, the forwarder or
// the signalling path read — those keep seeing the full declared ladder, so bandwidth
// allocation and client-side behaviour are byte-for-byte unchanged with or without it.
//
// Layers are matched on VideoLayer.SpatialLayer, the same field the SFU itself keys on
// (buffer.GetSpatialLayerForVideoQuality / GetVideoQualityForSpatialLayer read it): if it were
// unset the SFU would already be mis-forwarding, so this introduces no new failure mode.
func ParticipantInfoWithLiveLayers(pi *livekit.ParticipantInfo, live map[livekit.TrackID][]int32) *livekit.ParticipantInfo {
	if pi == nil || len(live) == 0 {
		return pi
	}

	out := utils.CloneProto(pi)
	for _, ti := range out.GetTracks() {
		if ti.GetType() != livekit.TrackType_VIDEO {
			continue
		}
		liveLayers, ok := live[livekit.TrackID(ti.GetSid())]
		if !ok {
			continue
		}

		ti.Layers = keepLiveLayers(ti.GetLayers(), liveLayers)
		for _, c := range ti.GetCodecs() {
			c.Layers = keepLiveLayers(c.GetLayers(), liveLayers)
		}
	}
	return out
}

func keepLiveLayers(layers []*livekit.VideoLayer, live []int32) []*livekit.VideoLayer {
	if len(layers) == 0 {
		return layers
	}

	out := make([]*livekit.VideoLayer, 0, len(layers))
	for _, l := range layers {
		if l != nil && slices.Contains(live, l.SpatialLayer) {
			out = append(out, l)
		}
	}
	return out
}
