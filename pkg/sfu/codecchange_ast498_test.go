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
	"sync"
	"testing"

	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
)

// AST-498. A PT change inside the same codec (an H.264 profile switch, which Safari, iOS and Opera
// do mid-session) must NOT invalidate the receiver: with a backup codec in TrackInfo that would
// regress a perfectly good H.264 publisher to VP8. A real codec change still invalidates.

func h264Params(pt webrtc.PayloadType, fmtp string) webrtc.RTPCodecParameters {
	return webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264, ClockRate: 90000, SDPFmtpLine: fmtp},
		PayloadType:        pt,
	}
}

func newCodecChangeReceiver(t *testing.T) (*ReceiverBase, func() []ReceiverCodecState) {
	t.Helper()
	r := NewReceiverBase(ReceiverBaseParams{
		TrackID: "TR_video",
		Kind:    webrtc.RTPCodecTypeVideo,
		Codec:   h264Params(102, "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f"),
		Logger:  logger.GetLogger(),
	}, &livekit.TrackInfo{Sid: "TR_video", Type: livekit.TrackType_VIDEO, MimeType: webrtc.MimeTypeH264}, ReceiverCodecStateNormal)
	t.Cleanup(func() { r.Close("test", false) })

	var mu sync.Mutex
	var seen []ReceiverCodecState
	r.AddOnCodecStateChange(func(_ webrtc.RTPCodecParameters, s ReceiverCodecState) {
		mu.Lock()
		seen = append(seen, s)
		mu.Unlock()
	})
	return r, func() []ReceiverCodecState {
		mu.Lock()
		defer mu.Unlock()
		return append([]ReceiverCodecState(nil), seen...)
	}
}

func TestAST498SameCodecPayloadTypeChangeIsNotACodecChange(t *testing.T) {
	r, seen := newCodecChangeReceiver(t)

	// Baseline -> High profile, new PT: the case measured in production.
	r.handleCodecChange(h264Params(127, "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=640c1f"))
	require.Equal(t, ReceiverCodecStateNormal, r.CodecState(), "an H.264 profile switch invalidated the receiver: with a backup codec this regresses the track to VP8")
	require.Empty(t, seen(), "an H.264 profile switch fired a codec state change")

	// Case only differs: still the same codec.
	r.handleCodecChange(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: "video/h264", ClockRate: 90000},
		PayloadType:        125,
	})
	require.Equal(t, ReceiverCodecStateNormal, r.CodecState())

	// THE NEGATIVE CONTROL. Without it a handleCodecChange that ignored everything would pass the
	// two arms above. A real codec change must still invalidate, exactly once.
	r.handleCodecChange(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000},
		PayloadType:        96,
	})
	require.Equal(t, ReceiverCodecStateInvalid, r.CodecState(), "H264 -> VP8 no longer invalidates: the backup codec could never take over")
	require.Equal(t, []ReceiverCodecState{ReceiverCodecStateInvalid}, seen())
}
