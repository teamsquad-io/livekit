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

// AST-429, amended by AST-449. The SFU already knows which spatial layers of a published video
// track are alive: StreamTrackerManager keeps `availableLayers`, fed by one StreamTracker per
// layer that declares a layer started/stopped from observed packets. Today that knowledge
// reaches the MOS scorer and the stream allocator and stops there.
//
// This file is the ONE accessor for that knowledge. Every presentation of it — the
// livekit_video_layer_* gauges and the /astream/v1 video-layers endpoint — reads from here, so
// they cannot drift apart.
//
// WHAT AST-449 CHANGED, AND WHY. AST-429 also used this accessor to FILTER the TrackInfo that
// GetParticipant/ListParticipants return, dropping the rungs that were not delivering. Measured
// in production on 2026-09-22: a rung the publisher was delivering without interruption at
// 2.4-2.8 Mbps and 21-24 fps disappeared from ListParticipants for 10 s at a time, because the
// underlying `availableLayers` oscillates. A consumer that reads the filtered response cannot
// tell "this rung was never declared" from "this rung is not delivering this second", so it
// spends an expensive decision (how many subscriptions to open) on a volatile signal.
//
// So the filter is gone and the two facts are now reported SIDE BY SIDE, in the same index
// space, and the consumer decides which one to spend on what:
//
//	declared  stable    how many rungs exist  -> expensive decisions
//	live      volatile  which rungs deliver   -> cheap decisions, plus Since/Transitions so a
//	                                            consumer can amortise flapping on data instead
//	                                            of on a typed-in threshold
//
// It is READ-ONLY by construction: nothing here is called from the packet path, nothing here is
// called from addAvailableLayer/removeAvailableLayer, and nothing here writes any state. The
// only lock it takes is the read lock StreamTrackerManager.LayerLiveness() already takes.

package rtc

import (
	"cmp"
	"slices"

	"github.com/livekit/protocol/codecs/mime"
	"github.com/livekit/protocol/livekit"

	"github.com/livekit/livekit-server/pkg/rtc/types"
	"github.com/livekit/livekit-server/pkg/sfu/buffer"
)

// LiveLayer is one rung the SFU is receiving packets on RIGHT NOW, with the ledger the
// StreamTrackerManager keeps for it (AST-449, see pkg/sfu/layerledger.go).
//
// SinceMs is how long it has been in this state and Transitions how many times it has flipped
// since the track was published. A consumer needs both to amortise a flapping rung without
// inventing a threshold: 11 transitions in ten minutes is an encoder bouncing, 1 transition
// forty seconds ago is an encoder that stopped.
type LiveLayer struct {
	SpatialLayer int32
	SinceMs      int64
	Transitions  uint64
}

// VideoLayerLiveness is a point-in-time reading for ONE published video track.
//
// Declared is the ladder the publisher announced when it published (TrackInfo). Live is the
// subset of it the SFU is receiving packets on right now. Both are spatial layer indices, the
// same space `VideoLayer.SpatialLayer` and the forwarder use.
//
// LiveKnown IS NOT DECORATION. "No reading available" (the track has no active receiver able to
// report, so we know nothing) and "nothing is delivering" (we asked and the answer is none) are
// different statements, and a consumer that confuses them tears down subscriptions on a track it
// simply cannot see. With LiveKnown false, Live is nil and says nothing.
type VideoLayerLiveness struct {
	TrackID   livekit.TrackID
	Declared  []int32
	Live      []LiveLayer
	LiveKnown bool
}

// LiveSpatialLayers is Live reduced to bare indices, which is all the Prometheus gauge counts.
func (l VideoLayerLiveness) LiveSpatialLayers() []int32 {
	if !l.LiveKnown {
		return nil
	}

	out := make([]int32, 0, len(l.Live))
	for _, live := range l.Live {
		out = append(out, live.SpatialLayer)
	}
	return out
}

// Degraded reports whether at least one declared rung is not delivering. This is the signal that
// matters operationally: the ladder a consumer would announce is wider than the ladder the
// encoder is actually producing.
//
// With no reading it is FALSE, not true: absence of evidence is not a degradation, and a gauge
// that counted it as one would climb every time a track is between receivers.
func (l VideoLayerLiveness) Degraded() bool {
	if !l.LiveKnown {
		return false
	}

	live := l.LiveSpatialLayers()
	for _, d := range l.Declared {
		if !slices.Contains(live, d) {
			return true
		}
	}
	return false
}

// liveVideoLayersReader is the slice of a published track this file needs. It is declared at the
// consumer, structurally, on purpose: adding a method to types.MediaTrack would force a
// regeneration of every counterfeiter fake in the tree for a read-only accessor.
// *MediaTrackReceiver satisfies it, and therefore so does *MediaTrack.
type liveVideoLayersReader interface {
	TrackInfo() *livekit.TrackInfo
	LiveVideoLayers() ([]LiveLayer, bool)
}

// The assertion is the gate, not documentation. *MediaTrack is the ONLY concrete type that ever
// reaches UpTrackManager.publishedTracks (participant.go, AddPublishedTrack), so if a rename ever
// broke this the type assertion in VideoLayerLivenessOf would silently start returning false and
// every presentation would go quietly blank. This turns that into a build failure.
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

	live, known := reader.LiveVideoLayers()
	return VideoLayerLiveness{
		TrackID:   track.ID(),
		Declared:  DeclaredSpatialLayers(reader.TrackInfo()),
		Live:      live,
		LiveKnown: known,
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

// DeclaredVideoLayers returns the rungs a TrackInfo declares — the WHOLE identity of each one,
// quality and rid and dimensions and target bitrate — deduplicated by spatial layer and sorted by
// it, with the OFF rungs dropped.
//
// It reads the ladder through buffer.GetVideoLayersForMimeType, the same accessor the SFU itself
// uses, so a multi-codec track reports the ladder of the codec in play rather than the deprecated
// top-level list.
//
// READ-ONLY: the returned pointers may alias the TrackInfo they came from. Nothing in this file
// writes to them and no caller may.
func DeclaredVideoLayers(ti *livekit.TrackInfo) []*livekit.VideoLayer {
	if ti == nil {
		return nil
	}

	layers := buffer.GetVideoLayersForMimeType(mime.NormalizeMimeType(ti.MimeType), ti)
	out := make([]*livekit.VideoLayer, 0, len(layers))
	seen := make(map[int32]struct{}, len(layers))
	for _, l := range layers {
		if l == nil || l.Quality == livekit.VideoQuality_OFF {
			continue
		}
		if _, dup := seen[l.SpatialLayer]; dup {
			continue
		}
		seen[l.SpatialLayer] = struct{}{}
		out = append(out, l)
	}

	slices.SortFunc(out, func(a, b *livekit.VideoLayer) int {
		return cmp.Compare(a.SpatialLayer, b.SpatialLayer)
	})
	return out
}

// DeclaredSpatialLayers is DeclaredVideoLayers reduced to bare indices, which is all the
// Prometheus gauge counts. A video track with no layer list is a single-layer track, which the
// SFU treats as spatial layer 0 (buffer.GetSpatialLayerForVideoQuality does exactly this), so it
// is reported as [0] rather than as an empty ladder.
func DeclaredSpatialLayers(ti *livekit.TrackInfo) []int32 {
	if ti == nil {
		return nil
	}

	layers := DeclaredVideoLayers(ti)
	if len(layers) == 0 {
		return []int32{0}
	}

	out := make([]int32, 0, len(layers))
	for _, l := range layers {
		out = append(out, l.SpatialLayer)
	}
	return out
}
