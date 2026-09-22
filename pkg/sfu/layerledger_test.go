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

package sfu

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/livekit/protocol/codecs/mime"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"

	"github.com/livekit/livekit-server/pkg/sfu/buffer"
)

// AST-449. The ledger is driven straight through addAvailableLayer / removeAvailableLayer /
// RemoveAllTrackers — the three and only three places that move `availableLayers`. No SFU, no
// packets, no timers: a StreamTrackerManager is enough.

func newLedgerTestManager(t *testing.T) *StreamTrackerManager {
	t.Helper()

	ti := &livekit.TrackInfo{
		Sid:      "TR_test",
		Type:     livekit.TrackType_VIDEO,
		MimeType: "video/h264",
		Source:   livekit.TrackSource_CAMERA,
		Layers: []*livekit.VideoLayer{
			{Quality: livekit.VideoQuality_LOW, SpatialLayer: 0, Width: 320, Height: 180},
			{Quality: livekit.VideoQuality_MEDIUM, SpatialLayer: 1, Width: 640, Height: 360},
			{Quality: livekit.VideoQuality_HIGH, SpatialLayer: 2, Width: 1280, Height: 720},
		},
	}

	s := NewStreamTrackerManager(
		logger.GetLogger(),
		ti,
		mime.MimeTypeH264,
		90000,
		DefaultStreamTrackerManagerConfig,
	)
	t.Cleanup(s.Close)
	return s
}

func entryFor(entries []LayerLivenessEntry, layer int32) LayerLivenessEntry {
	for _, e := range entries {
		if e.SpatialLayer == layer {
			return e
		}
	}
	return LayerLivenessEntry{SpatialLayer: -1}
}

func TestLayerLedgerShape(t *testing.T) {
	s := newLedgerTestManager(t)

	entries := s.LayerLiveness()

	// One entry per layer the SFU can track, ALWAYS, in index order. A caller must never have to
	// tell "this layer does not exist" from "this layer is dead" by the length of the answer.
	require.Len(t, entries, int(buffer.DefaultMaxLayerSpatial)+1)
	for i, e := range entries {
		require.Equal(t, int32(i), e.SpatialLayer)
		require.False(t, e.Live)
		require.Zero(t, e.Transitions)
		require.False(t, e.Since.IsZero(), "layer %d was never stamped", i)
	}
}

func TestLayerLedgerCountsTransitions(t *testing.T) {
	s := newLedgerTestManager(t)

	before := entryFor(s.LayerLiveness(), 2).Since

	s.addAvailableLayer(2)
	up := entryFor(s.LayerLiveness(), 2)
	require.True(t, up.Live)
	require.Equal(t, uint64(1), up.Transitions)
	require.True(t, up.Since.After(before) || up.Since.Equal(before))

	// A repeated add is not a transition: addAvailableLayer returns early on a layer it already
	// has, and the ledger must not invent a flip out of it.
	s.addAvailableLayer(2)
	require.Equal(t, uint64(1), entryFor(s.LayerLiveness(), 2).Transitions)

	s.removeAvailableLayer(2)
	down := entryFor(s.LayerLiveness(), 2)
	require.False(t, down.Live)
	require.Equal(t, uint64(2), down.Transitions)

	// CONTROL: removeAvailableLayer rebuilds the slice WITHOUT checking whether the layer was in
	// it, so it is routinely called for a layer already gone. That must not count either.
	s.removeAvailableLayer(2)
	require.Equal(t, uint64(2), entryFor(s.LayerLiveness(), 2).Transitions)

	// CONTROL NEGATIVO: the layers nobody touched stayed at zero, so the counts above are this
	// layer's and not a counter shared across the ladder.
	require.Zero(t, entryFor(s.LayerLiveness(), 0).Transitions)
	require.Zero(t, entryFor(s.LayerLiveness(), 1).Transitions)
}

func TestLayerLedgerSinceMovesOnlyOnATransition(t *testing.T) {
	s := newLedgerTestManager(t)

	s.addAvailableLayer(0)
	first := entryFor(s.LayerLiveness(), 0).Since

	time.Sleep(5 * time.Millisecond)
	s.addAvailableLayer(0) // no transition
	require.Equal(t, first, entryFor(s.LayerLiveness(), 0).Since)

	time.Sleep(5 * time.Millisecond)
	s.removeAvailableLayer(0) // transition
	require.True(t, entryFor(s.LayerLiveness(), 0).Since.After(first))
}

// RemoveAllTrackers empties availableLayers in one go without passing through
// removeAvailableLayer. Before AST-449 hooked it, a rung that was live at that moment would have
// kept a `since` from before the teardown and reported an age that never happened.
func TestLayerLedgerRemoveAllTrackers(t *testing.T) {
	s := newLedgerTestManager(t)

	s.addAvailableLayer(0)
	s.addAvailableLayer(2)
	require.Equal(t, uint64(1), entryFor(s.LayerLiveness(), 0).Transitions)

	s.RemoveAllTrackers()

	entries := s.LayerLiveness()
	for _, e := range entries {
		require.False(t, e.Live, "layer %d still live after RemoveAllTrackers", e.SpatialLayer)
	}
	require.Equal(t, uint64(2), entryFor(entries, 0).Transitions)
	require.Equal(t, uint64(2), entryFor(entries, 2).Transitions)
	// layer 1 was never live, so tearing everything down is not a transition for it
	require.Zero(t, entryFor(entries, 1).Transitions)
}

// LIVENESS IS DERIVED FROM availableLayers, NOT FROM THE LEDGER. This is what keeps a missed hook
// from turning into a lie: sabotage the stored half and the reported `Live` must not move.
func TestLayerLedgerLivenessComesFromAvailableLayers(t *testing.T) {
	s := newLedgerTestManager(t)

	s.addAvailableLayer(1)
	require.True(t, entryFor(s.LayerLiveness(), 1).Live)

	s.lock.Lock()
	s.layerLedger[1].live = false // as if a hook had been missed
	s.lock.Unlock()

	require.True(t, entryFor(s.LayerLiveness(), 1).Live,
		"Live must come from availableLayers, not from the ledger's own record")

	// CONTROL POSITIVO of the sabotage: it is the ledger that is now out of step, and it shows up
	// as a spurious transition on the next real move rather than as a wrong liveness.
	s.removeAvailableLayer(1)
	require.False(t, entryFor(s.LayerLiveness(), 1).Live)
}

func TestLayerLedgerIgnoresOutOfRangeLayers(t *testing.T) {
	s := newLedgerTestManager(t)

	s.lock.Lock()
	s.noteLayerStateLocked(-1, true, time.Now())
	s.noteLayerStateLocked(buffer.DefaultMaxLayerSpatial+1, true, time.Now())
	s.lock.Unlock()

	for _, e := range s.LayerLiveness() {
		require.Zero(t, e.Transitions)
	}
}
