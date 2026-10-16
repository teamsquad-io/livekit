// AST-529 (a-stream). Regression bench for downtracks that go silent for hours: the switch back to a
// spatial layer is rejected with errSwitchPointTooFarBehind because the dummy start offset
// (dummyStartTSOffset) was added in 64 bits, and when it is negative its borrow leaves extRefTS
// 2^32 below the right value. The wrap correction right after it only compares the lower 32 bits
// and cannot see it.
//
// The sequence is the one observed in production on 2026-10-16: the downtrack starts on layer 0
// (with a dummy start, so the offset is set), moves to layer 2 when layer 0 stops, layer 0's 32-bit
// timestamp wraps while layer 2 is being forwarded, and the publisher goes back to layer 0. Every
// step goes through GetTranslationParams, AllocateOptimal and SetRefSenderReport, the same inputs
// the downtrack uses.
package sfu

import (
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"

	"github.com/livekit/mediatransportutil"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"

	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	"github.com/livekit/livekit-server/pkg/sfu/rtpstats"
	"github.com/livekit/livekit-server/pkg/sfu/testutils"
)

const (
	ast529SSRCLow  = uint32(0x0a0a0a0a)
	ast529SSRCHigh = uint32(0x0c0c0c0c)
	ast529Frame    = uint32(3750) // 24 fps at 90 kHz
)

var ast529H264 = webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264, ClockRate: 90000}

func ast529Forwarder(t *testing.T) *Forwarder {
	t.Helper()
	// AST529_LOG=debug makes logTransition say WHICH branch rejects the switch
	lg, err := logger.NewZapLogger(&logger.Config{Level: ast529LogLevel, JSON: true})
	require.NoError(t, err)
	f := NewForwarder(
		webrtc.RTPCodecTypeVideo,
		lg,
		false, // skipReferenceTS, as in downtrack.go
		false, // disableOpportunisticAllocation, as in downtrack.go
		false, // enableStartAtDesiredQuality
		rtpstats.NewRTPStatsSender(rtpstats.RTPStatsParams{}, 1024),
	)
	f.DetermineCodec(ast529H264, nil, livekit.VideoLayer_ONE_SPATIAL_LAYER_PER_STREAM)
	f.SetMaxSpatialLayer(2) // the subscriber asks for HIGH
	f.SetMaxTemporalLayer(0)
	f.SetMaxPublishedLayer(2)
	return f
}

func ast529Brs(layer int32) Bitrates {
	var brs Bitrates
	brs[layer][0] = 1_000_000
	return brs
}

func ast529Pkt(t *testing.T, ssrc uint32, sn uint16, ts uint32, tsCycles int, layer int32) *buffer.ExtPacket {
	t.Helper()
	p, err := testutils.GetTestExtPacket(&testutils.TestExtPacketParams{
		SequenceNumber: sn,
		Timestamp:      ts,
		TSCycles:       tsCycles,
		SSRC:           ssrc,
		PayloadSize:    20,
		Marker:         true,
		IsKeyFrame:     true,
		VideoLayer:     buffer.VideoLayer{Spatial: layer, Temporal: 0},
	})
	require.NoError(t, err)
	return p
}

func ast529SR(ntp time.Time, rtpTS uint32) *livekit.RTCPSenderReportState {
	return &livekit.RTCPSenderReportState{
		RtpTimestamp:    rtpTS,
		RtpTimestampExt: uint64(rtpTS),
		NtpTimestamp:    uint64(mediatransportutil.ToNtpTime(ntp)),
		At:              ntp.UnixNano(),
		AtAdjusted:      ntp.UnixNano(),
	}
}

// ast529RunToHighLayer drives the forwarder to the state of the incident: started on layer 0 with
// `lowStart` as layer 0's first timestamp, forwarding layer 2, with layer 0 stopped. It returns
// layer 0's 32-bit timestamp (and its cycles) for the instant `elapsed` after the start, to be
// used by the keyframe that switches back.
func ast529RunToHighLayer(t *testing.T, f *Forwarder, lowStart uint32, elapsed time.Duration) (uint32, int) {
	t.Helper()
	const highStart = uint32(0x10000000)

	// 1. dummy start (downtrack.go does it on bind) and lock onto layer 0
	f.maybeStart()
	//    (with three layers declared the opportunistic target is 2, but the first keyframe to
	//    arrive is layer 0's and the selector locks onto it: referenceLayerSpatial=0, as logged
	//    for every affected subscription)
	f.AllocateOptimal([]int32{0}, ast529Brs(0), true, false)
	tp, err := f.GetTranslationParams(ast529Pkt(t, ast529SSRCLow, 1000, lowStart, 0, 0), 0)
	require.NoError(t, err)
	require.False(t, tp.shouldDrop, "the first layer 0 packet must go through")
	require.Equal(t, int32(0), f.referenceLayerSpatial)
	require.Equal(t, int32(0), f.CurrentLayer().Spatial)

	// 2. Sender Reports of both layers at the same NTP instant (what a browser sends)
	t0 := time.Now()
	f.SetRefSenderReport(0, ast529SR(t0, lowStart+ast529Frame))
	f.SetRefSenderReport(2, ast529SR(t0, highStart+ast529Frame))

	// 3. layer 0 stops and layer 2 is live: the allocator moves the target and the forwarder switches
	al := f.AllocateOptimal([]int32{2}, ast529Brs(2), true, false)
	require.Equal(t, int32(2), al.TargetLayer.Spatial)
	tp, err = f.GetTranslationParams(ast529Pkt(t, ast529SSRCHigh, 5000, highStart+2*ast529Frame, 0, 2), 2)
	require.NoError(t, err)
	require.False(t, tp.shouldDrop, "switching 0 -> 2 must work (it is the healthy state before)")
	require.Equal(t, int32(2), f.CurrentLayer().Spatial)

	// 4. hours forwarding layer 2: one packet carrying the timestamp of `elapsed` is enough, the
	//    munger does not look at the wall clock
	ticks := uint64(elapsed.Seconds() * 90000)
	highTS := uint64(highStart) + ticks
	tp, err = f.GetTranslationParams(ast529Pkt(t, ast529SSRCHigh, 5001, uint32(highTS), int(highTS>>32), 2), 2)
	require.NoError(t, err)
	require.False(t, tp.shouldDrop)

	// 5. the publisher stops layer 2 and resumes layer 0 (setParameters): the allocator targets 0
	al = f.AllocateOptimal([]int32{0}, ast529Brs(0), true, false)
	require.Equal(t, int32(0), al.TargetLayer.Spatial)
	require.Equal(t, int32(0), al.RequestLayerSpatial)

	lowTS := uint64(lowStart) + ticks + uint64(ast529Frame)
	return uint32(lowTS), int(lowTS >> 32)
}

// Switching back to layer 0 AFTER its 32-bit timestamp has wrapped, with a negative dummy offset:
// the incident case. Without the fix every keyframe is rejected and the downtrack stays on layer 2,
// which no longer carries packets, so the subscriber receives nothing.
func TestAST529SwitchBackAfterReferenceWrap(t *testing.T) {
	f := ast529Forwarder(t)
	// layer 0's first timestamp in the incident (forwarder.go logs it as incomingTS)
	const lowStart = uint32(4044667856)
	lowTS, cycles := ast529RunToHighLayer(t, f, lowStart, 8655*time.Second) // 2 h 24 min 15 s

	// preconditions of the case: negative offset and a 32-bit timestamp below |d|
	d := int64(f.dummyStartTSOffset)
	require.Less(t, d, int64(0), "the case needs a negative dummyStartTSOffset")
	require.Equal(t, 1, cycles, "layer 0's timestamp must have wrapped")
	require.Less(t, int64(lowTS), -d, "layer 0's 32-bit timestamp is below |d|")

	// ten layer 0 keyframes, one per second (the layer-lock PLI): the first one must complete the
	// switch and none may leave the forwarder on layer 2
	for i := 0; i < 10; i++ {
		ts := uint64(lowTS) + uint64(i)*90000
		tp, err := f.GetTranslationParams(ast529Pkt(t, ast529SSRCLow, uint16(1001+i), uint32(ts), cycles+int(ts>>32), 0), 0)
		require.NoError(t, err)
		if !tp.shouldDrop {
			require.Equal(t, int32(0), f.CurrentLayer().Spatial)
			return
		}
	}
	t.Fatalf("10 layer 0 keyframes and the forwarder is still on layer %d: silent downtrack", f.CurrentLayer().Spatial)
}

// CONTROL. Same sequence, but the switch back to layer 0 arrives BEFORE its 32-bit timestamp wraps
// (30 min instead of 2 h 24 min). It must pass with and without the fix: it shows the test above
// fails because of the 32-bit crossing and not because of the setup.
func TestAST529SwitchBackBeforeReferenceWrap(t *testing.T) {
	f := ast529Forwarder(t)
	const lowStart = uint32(4044667856)
	lowTS, cycles := ast529RunToHighLayer(t, f, lowStart, 30*time.Minute)
	require.Equal(t, 0, cycles, "the control must not have wrapped")

	tp, err := f.GetTranslationParams(ast529Pkt(t, ast529SSRCLow, 1001, lowTS, cycles, 0), 0)
	require.NoError(t, err)
	require.False(t, tp.shouldDrop, "without a 32-bit crossing the switch back to layer 0 works")
	require.Equal(t, int32(0), f.CurrentLayer().Spatial)
}

// Sweep: layer 0's first timestamp and the time until the switch back are random (fixed seed).
// With the fix no switch back is rejected. Without it, the rejected fraction measures how much of a
// downtrack's lifetime falls inside a rejection window.
func TestAST529SwitchBackRandomized(t *testing.T) {
	rng := rand.New(rand.NewSource(529))
	const n = 400
	rejected := 0
	for i := 0; i < n; i++ {
		f := ast529Forwarder(t)
		lowStart := rng.Uint32()
		elapsed := time.Duration(60+rng.Int63n(26*3600)) * time.Second
		lowTS, cycles := ast529RunToHighLayer(t, f, lowStart, elapsed)
		tp, err := f.GetTranslationParams(ast529Pkt(t, ast529SSRCLow, 1001, lowTS, cycles, 0), 0)
		require.NoError(t, err)
		if tp.shouldDrop {
			rejected++
		}
	}
	t.Logf("AST529 sweep: %d of %d switches back rejected", rejected, n)
	require.Zero(t, rejected)
}

// AST529_LOG=debug to see the branch (logTransition); silent by default (the sweep runs 400).
var ast529LogLevel = func() string {
	if v := os.Getenv("AST529_LOG"); v != "" {
		return v
	}
	return "warn"
}()
