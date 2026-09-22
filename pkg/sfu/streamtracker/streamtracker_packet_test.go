// Copyright 2023 LiveKit, Inc.
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

package streamtracker

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/livekit/livekit-server/pkg/testutils"
	"github.com/livekit/protocol/logger"
)

func newStreamTrackerPacket(samplesRequired uint32, cyclesRequired uint32, cycleDuration time.Duration) *StreamTracker {
	stp := NewStreamTrackerPacket(StreamTrackerPacketParams{
		Config: StreamTrackerPacketConfig{
			SamplesRequired: samplesRequired,
			CyclesRequired:  cyclesRequired,
			CycleDuration:   cycleDuration,
		},
		Logger: logger.GetLogger(),
	})

	return NewStreamTracker(StreamTrackerParams{
		StreamTrackerImpl:     stp,
		BitrateReportInterval: 1 * time.Second,
		Logger:                logger.GetLogger(),
	})
}

func TestStreamTracker(t *testing.T) {
	t.Run("flips to active on first observe", func(t *testing.T) {
		callbackCalled := atomic.NewBool(false)
		tracker := newStreamTrackerPacket(5, 60, 500*time.Millisecond)
		tracker.Start()
		tracker.OnStatusChanged(func(status StreamStatus) {
			callbackCalled.Store(true)
		})
		require.Equal(t, StreamStatusStopped, tracker.Status())

		// observe first packet
		tracker.Observe(0, 20, 10, false, 0, nil)

		testutils.WithTimeout(t, func() string {
			if callbackCalled.Load() {
				return ""
			}

			return "first packet didn't activate stream"
		})

		require.Equal(t, StreamStatusActive, tracker.Status())
		require.True(t, callbackCalled.Load())

		tracker.Stop()
	})

	t.Run("flips to inactive immediately", func(t *testing.T) {
		tracker := newStreamTrackerPacket(5, 60, 500*time.Millisecond)
		tracker.Start()
		require.Equal(t, StreamStatusStopped, tracker.Status())

		callbackStatusMu := sync.RWMutex{}
		callbackStatusMu.Lock()
		callbackStatus := StreamStatusStopped
		callbackStatusMu.Unlock()
		tracker.OnStatusChanged(func(status StreamStatus) {
			callbackStatusMu.Lock()
			callbackStatus = status
			callbackStatusMu.Unlock()
		})

		tracker.Observe(0, 20, 10, false, 0, nil)
		testutils.WithTimeout(t, func() string {
			callbackStatusMu.RLock()
			defer callbackStatusMu.RUnlock()

			if callbackStatus == StreamStatusActive {
				return ""
			}

			return "first packet did not activate stream"
		})
		require.Equal(t, StreamStatusActive, tracker.Status())

		// run a single iteration
		tracker.updateStatus()

		testutils.WithTimeout(t, func() string {
			callbackStatusMu.RLock()
			defer callbackStatusMu.RUnlock()

			if callbackStatus == StreamStatusStopped {
				return ""
			}

			return "inactive cycle did not declare stream stopped"
		})
		require.Equal(t, StreamStatusStopped, tracker.Status())
		require.Equal(t, StreamStatusStopped, callbackStatus)

		tracker.Stop()
	})

	t.Run("flips back to active after iterations", func(t *testing.T) {
		tracker := newStreamTrackerPacket(1, 2, 500*time.Millisecond)
		tracker.Start()
		require.Equal(t, StreamStatusStopped, tracker.Status())

		tracker.Observe(0, 20, 10, false, 0, nil)
		testutils.WithTimeout(t, func() string {
			if tracker.Status() == StreamStatusActive {
				return ""
			}

			return "first packet did not activate stream"
		})

		tracker.setStatusLocked(StreamStatusStopped)

		tracker.Observe(0, 20, 10, false, 0, nil)
		tracker.updateStatus()
		require.Equal(t, StreamStatusStopped, tracker.Status())

		tracker.Observe(0, 20, 10, false, 0, nil)
		tracker.updateStatus()
		require.Equal(t, StreamStatusActive, tracker.Status())

		tracker.Stop()
	})

	t.Run("changes to inactive when paused", func(t *testing.T) {
		tracker := newStreamTrackerPacket(5, 60, 500*time.Millisecond)
		tracker.Start()
		tracker.Observe(0, 20, 10, false, 0, nil)
		testutils.WithTimeout(t, func() string {
			if tracker.Status() == StreamStatusActive {
				return ""
			}

			return "first packet did not activate stream"
		})

		tracker.SetPaused(true)
		tracker.updateStatus()
		require.Equal(t, StreamStatusStopped, tracker.Status())

		tracker.Stop()
	})

	t.Run("flips back to active on first observe after reset", func(t *testing.T) {
		callbackCalled := atomic.NewUint32(0)
		tracker := newStreamTrackerPacket(5, 60, 500*time.Millisecond)
		tracker.Start()
		tracker.OnStatusChanged(func(status StreamStatus) {
			callbackCalled.Inc()
		})
		require.Equal(t, StreamStatusStopped, tracker.Status())

		// observe first packet
		tracker.Observe(0, 20, 10, false, 0, nil)

		testutils.WithTimeout(t, func() string {
			if callbackCalled.Load() == 1 {
				return ""
			}

			return fmt.Sprintf("expected onStatusChanged to be called once, actual: %d", callbackCalled.Load())
		})

		require.Equal(t, StreamStatusActive, tracker.Status())
		require.Equal(t, uint32(1), callbackCalled.Load())

		// observe a few more
		tracker.Observe(0, 20, 10, false, 0, nil)
		tracker.Observe(0, 20, 10, false, 0, nil)
		tracker.Observe(0, 20, 10, false, 0, nil)
		tracker.Observe(0, 20, 10, false, 0, nil)
		tracker.updateStatus()

		// should still be active
		require.Equal(t, StreamStatusActive, tracker.Status())
		require.Equal(t, uint32(1), callbackCalled.Load())

		// Reset. The first packet after reset should flip state again
		tracker.Reset()
		require.Equal(t, StreamStatusStopped, tracker.Status())
		require.Equal(t, uint32(2), callbackCalled.Load())

		// first packet after reset
		tracker.Observe(0, 20, 10, false, 0, nil)

		testutils.WithTimeout(t, func() string {
			if callbackCalled.Load() == 3 {
				return ""
			}

			return fmt.Sprintf("expected onStatusChanged to be called thrice, actual %d", callbackCalled.Load())
		})

		require.Equal(t, StreamStatusActive, tracker.Status())
		require.Equal(t, uint32(3), callbackCalled.Load())

		tracker.Stop()
	})
}

// AST-449. The readmission window of a video rung, driven through the real impl with the shipped
// defaults. It is the wall clock that is asserted, not the constant, so the test stays honest if
// CycleDuration moves instead of CyclesRequired.
func TestStreamTrackerPacketVideoReadmissionWindow(t *testing.T) {
	// A 720p simulcast rung at ~2.4 Mbps is ~250 pps, so ~125 packets in a 500 ms cycle.
	const packetsPerCycle = 125

	for layer := int32(0); layer <= 2; layer++ {
		cfg := DefaultStreamTrackerPacketConfigVideo[layer]
		impl := NewStreamTrackerPacket(StreamTrackerPacketParams{Config: cfg, Logger: logger.GetLogger()})

		feed := func(n int) {
			for i := 0; i < n; i++ {
				impl.Observe(false, 0)
			}
		}

		feed(1) // first packet: active
		// Control: a rung delivering at rate is never condemned, however long it runs.
		for c := 0; c < 30; c++ {
			feed(packetsPerCycle)
			require.NotEqual(t, StreamStatusChangeStopped, impl.CheckStatus(),
				"layer %d: a rung at rate was condemned on cycle %d", layer, c)
		}

		// One silent cycle condemns. That part is upstream's and stays: the rung really was not
		// delivering for those 500 ms.
		require.Equal(t, StreamStatusChangeStopped, impl.CheckStatus(),
			"layer %d: a silent cycle did not condemn the rung", layer)

		cycles := 0
		for impl.CheckStatus() != StreamStatusChangeActive {
			feed(packetsPerCycle)
			cycles++
			require.Less(t, cycles, 100, "layer %d: rung never readmitted", layer)
		}
		readmission := time.Duration(cycles) * cfg.CycleDuration

		// 2 s is rung 0's window, unchanged since upstream. No rung may cost more than that to
		// take back: rungs 1 and 2 used to cost 10 s, and a publisher that never stopped spent
		// that whole time out of availableLayers.
		require.LessOrEqual(t, readmission, 2*time.Second,
			"layer %d: readmission takes %v (%d cycles of %v)", layer, readmission, cycles, cfg.CycleDuration)
	}
}
