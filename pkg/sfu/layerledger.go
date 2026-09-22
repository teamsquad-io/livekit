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

// AST-449. StreamTrackerManager already knows WHICH spatial layers are delivering
// (`availableLayers`). What it does not keep — and what any consumer downstream needs before it
// can amortise a flapping rung without inventing a threshold — is SINCE WHEN and HOW MANY TIMES.
//
// This file is that ledger, and nothing else:
//
//   - It is written only from the three places that move `availableLayers`, under the lock that
//     is already held there, so it costs one comparison and no allocation on a transition.
//   - It is never read on the packet path. The only reader is LayerLiveness(), called once per
//     API request or Prometheus scrape.
//   - LIVENESS ITSELF IS NOT STORED HERE. LayerLiveness() derives `Live` from `availableLayers`,
//     the one source of truth, and the ledger contributes only the timing. So if a future
//     LiveKit release grows a fourth way to move `availableLayers` and we miss the hook, the
//     liveness reported stays correct and only `Since`/`Transitions` go stale — the failure mode
//     degrades instead of lying.
package sfu

import (
	"slices"
	"time"
)

// LayerLivenessEntry is the reading for ONE spatial layer of a published track.
//
// Since is when the layer entered its CURRENT state (delivering or not), and Transitions how
// many times it has flipped since the track was published. Both are needed downstream: a rung
// that has been dead for 400 ms with 11 transitions behind it is a flapping encoder, a rung dead
// for 40 s with 1 transition is an encoder that stopped.
type LayerLivenessEntry struct {
	SpatialLayer int32
	Live         bool
	Since        time.Time
	Transitions  uint64
}

// layerLedgerEntry is the stored half. `live` here is NOT the answer to "is this layer live" —
// that is `availableLayers` — it is only the last state this ledger recorded, kept so a repeated
// note is not counted as a transition.
type layerLedgerEntry struct {
	live        bool
	since       time.Time
	transitions uint64
}

// initLayerLedger stamps every layer as not delivering since `now` with zero transitions. Called
// from the constructor, where nothing else can see the manager yet.
func (s *StreamTrackerManager) initLayerLedger(now time.Time) {
	for i := range s.layerLedger {
		s.layerLedger[i] = layerLedgerEntry{since: now}
	}
}

// noteLayerStateLocked records a transition. The caller holds s.lock for writing.
//
// It is a no-op when the state did not actually change, which is what makes it safe to call from
// removeAvailableLayer: that function rebuilds the slice without checking whether the layer was
// in it, so it is routinely called for a layer that was already gone.
func (s *StreamTrackerManager) noteLayerStateLocked(layer int32, live bool, now time.Time) {
	if layer < 0 || int(layer) >= len(s.layerLedger) {
		return
	}

	e := &s.layerLedger[layer]
	if e.live == live {
		return
	}

	e.live = live
	e.since = now
	e.transitions++
}

// noteAllLayersDeadLocked is the RemoveAllTrackers path, which empties `availableLayers` in one
// go without passing through removeAvailableLayer. The caller holds s.lock for writing.
func (s *StreamTrackerManager) noteAllLayersDeadLocked(now time.Time) {
	for i := range s.layerLedger {
		s.noteLayerStateLocked(int32(i), false, now)
	}
}

// LayerLiveness returns one entry per spatial layer the manager can track, always the same
// number of entries in the same order, so a caller never has to tell "layer absent" from "layer
// dead" by the shape of the answer.
//
// Since is returned as an absolute instant, not an age: the caller subtracts it from the ONE
// instant it stamps the whole response with, so every track in a response is measured against
// the same clock reading.
func (s *StreamTrackerManager) LayerLiveness() []LayerLivenessEntry {
	s.lock.RLock()
	defer s.lock.RUnlock()

	out := make([]LayerLivenessEntry, 0, len(s.layerLedger))
	for i := range s.layerLedger {
		layer := int32(i)
		out = append(out, LayerLivenessEntry{
			SpatialLayer: layer,
			Live:         slices.Contains(s.availableLayers, layer),
			Since:        s.layerLedger[i].since,
			Transitions:  s.layerLedger[i].transitions,
		})
	}
	return out
}
