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

	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"

	"github.com/livekit/protocol/codecs/mime"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"

	"github.com/livekit/livekit-server/pkg/sfu"
	"github.com/livekit/livekit-server/pkg/sfu/sfufakes"
)

// AST-498. After a codec regression the SFU forwards the BACKUP receiver, TrackInfo.MimeType keeps
// the codec the track was published with, and upstream ActiveReceiver returns the receiver that
// STOPPED. These tests pin which receiver the fork reports as forwarded, and that the liveness
// reading follows it.

// ledgerReceiver is a TrackReceiver that also has a StreamTrackerManager, which is what
// *sfu.WebRTCReceiver is in production. With no packets every rung reads "not live", so the
// reading is (empty, true): KNOWN. A receiver without a ledger reads (nil, false). That
// difference is what tells, from the outside, which receiver LiveVideoLayers looked at.
type ledgerReceiver struct {
	*sfufakes.FakeTrackReceiver
	stm *sfu.StreamTrackerManager
}

func (l *ledgerReceiver) StreamTrackerManager() *sfu.StreamTrackerManager { return l.stm }

func fakeReceiver(m string) *sfufakes.FakeTrackReceiver {
	f := &sfufakes.FakeTrackReceiver{}
	f.CodecReturns(webrtc.RTPCodecParameters{RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: m, ClockRate: 90000}})
	f.MimeReturns(mime.NormalizeMimeType(m))
	return f
}

func regressedTrack(t *testing.T) (*MediaTrackReceiver, *simulcastReceiver, sfu.TrackReceiver) {
	t.Helper()
	ti := &livekit.TrackInfo{Sid: "TR_v", Type: livekit.TrackType_VIDEO, MimeType: webrtc.MimeTypeH264}
	primary := &simulcastReceiver{TrackReceiver: fakeReceiver(webrtc.MimeTypeH264), priority: 0}
	vp8 := &ledgerReceiver{
		FakeTrackReceiver: fakeReceiver(webrtc.MimeTypeVP8),
		stm:               sfu.NewStreamTrackerManager(logger.GetLogger(), ti, mime.MimeTypeVP8, 90000, sfu.DefaultStreamTrackerManagerConfig),
	}
	backup := &simulcastReceiver{TrackReceiver: vp8, priority: 1}
	return &MediaTrackReceiver{receivers: []*simulcastReceiver{primary, backup}}, primary, vp8
}

func TestAST498ForwardedCodecFollowsTheRegression(t *testing.T) {
	mtr, primary, vp8 := regressedTrack(t)

	// BEFORE: nothing regressed, the forwarded codec is the published one.
	got, ok := mtr.ForwardedMimeType()
	require.True(t, ok)
	require.Equal(t, mime.MimeTypeH264, got)

	primary.RegressTo(vp8)

	got, ok = mtr.ForwardedMimeType()
	require.True(t, ok)
	require.Equal(t, mime.MimeTypeVP8, got, "after H264 -> VP8 the forwarded codec still reads H264: the packager would reject the live VP8 track")
	require.Same(t, vp8, mtr.ForwardedReceiver())

	// THE CONTROL that makes the arm above mean something: upstream ActiveReceiver returns the
	// receiver that STOPPED. If it ever starts returning the backup, ForwardedReceiver is
	// redundant — and this test says so instead of silently passing.
	require.NotSame(t, vp8, mtr.ActiveReceiver(), "upstream ActiveReceiver now returns the backup; ForwardedReceiver can delegate to it")
}

func TestAST498LiveLayersAreReadFromTheForwardedReceiver(t *testing.T) {
	mtr, primary, vp8 := regressedTrack(t)

	// Not regressed: the primary has no ledger -> no reading. Same as before AST-498.
	_, known := mtr.LiveVideoLayers()
	require.False(t, known)

	primary.RegressTo(vp8)

	// Regressed: the reading comes from the VP8 receiver, which has a ledger -> KNOWN. Reading the
	// stopped H.264 one (what ActiveReceiver returns) gives no reading, and in production a reading
	// of a dead receiver says "no rung delivers" while VP8 flows on all of them.
	live, known := mtr.LiveVideoLayers()
	require.True(t, known, "LiveVideoLayers is not reading the forwarded (backup) receiver")
	require.Empty(t, live)
}

func TestAST498ForwardedReceiverUnwrapsTheDummy(t *testing.T) {
	primary := &simulcastReceiver{TrackReceiver: fakeReceiver(webrtc.MimeTypeH264), priority: 0}
	vp8Params := webrtc.RTPCodecParameters{RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000}}
	dummy := NewDummyReceiver("TR_v", "s", vp8Params, nil)
	mtr := &MediaTrackReceiver{receivers: []*simulcastReceiver{primary, {TrackReceiver: dummy, priority: 1}}}

	primary.RegressTo(dummy)
	// The VP8 media has not arrived yet: the dummy itself is what is forwarded, and its codec is right.
	got, ok := mtr.ForwardedMimeType()
	require.True(t, ok)
	require.Equal(t, mime.MimeTypeVP8, got)
	require.Same(t, sfu.TrackReceiver(dummy), mtr.ForwardedReceiver())

	// The real one arrives: it is the one returned, so its ledger is the one read.
	real := fakeReceiver(webrtc.MimeTypeVP8)
	dummy.Upgrade(real)
	require.Same(t, sfu.TrackReceiver(real), mtr.ForwardedReceiver())
}
