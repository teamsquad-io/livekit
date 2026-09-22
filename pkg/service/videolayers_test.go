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

package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"

	"github.com/livekit/livekit-server/pkg/rtc"
	"github.com/livekit/livekit-server/pkg/rtc/types"
	"github.com/livekit/livekit-server/pkg/rtc/types/typesfakes"
)

// AST-449. No SFU: a fake participant with a fake track is all the endpoint reads.

// fakeLiveTrack is a types.MediaTrack that also reports liveness, which is what *rtc.MediaTrack
// is in production. rtc.VideoLayerLivenessOf finds the two extra methods structurally.
type fakeLiveTrack struct {
	*typesfakes.FakeMediaTrack
	ti    *livekit.TrackInfo
	live  []rtc.LiveLayer
	known bool
}

func (f *fakeLiveTrack) TrackInfo() *livekit.TrackInfo            { return f.ti }
func (f *fakeLiveTrack) LiveVideoLayers() ([]rtc.LiveLayer, bool) { return f.live, f.known }

func newFakeLiveTrack(ti *livekit.TrackInfo, live []rtc.LiveLayer, known bool) *fakeLiveTrack {
	fake := &typesfakes.FakeMediaTrack{}
	fake.IDReturns(livekit.TrackID(ti.Sid))
	fake.KindReturns(ti.Type)
	return &fakeLiveTrack{FakeMediaTrack: fake, ti: ti, live: live, known: known}
}

func publisherTrackInfo() *livekit.TrackInfo {
	return &livekit.TrackInfo{
		Sid:      "TR_video",
		Type:     livekit.TrackType_VIDEO,
		MimeType: "video/H264",
		Width:    1280,
		Height:   720,
		Layers: []*livekit.VideoLayer{
			{Quality: livekit.VideoQuality_HIGH, SpatialLayer: 2, Rid: "f", Width: 1280, Height: 720, Bitrate: 2500000},
			{Quality: livekit.VideoQuality_LOW, SpatialLayer: 0, Rid: "q", Width: 426, Height: 240, Bitrate: 400000},
			{Quality: livekit.VideoQuality_MEDIUM, SpatialLayer: 1, Rid: "h", Width: 852, Height: 480, Bitrate: 1000000},
		},
	}
}

func audioTrackInfo() *livekit.TrackInfo {
	return &livekit.TrackInfo{Sid: "TR_audio", Type: livekit.TrackType_AUDIO, MimeType: "audio/opus"}
}

func fakePublisher(tracks ...types.MediaTrack) *typesfakes.FakeLocalParticipant {
	p := &typesfakes.FakeLocalParticipant{}
	p.GetPublishedTracksReturns(tracks)

	infos := make([]*livekit.TrackInfo, 0, len(tracks))
	for _, t := range tracks {
		infos = append(infos, t.(*fakeLiveTrack).ti)
	}
	p.ToProtoReturns(&livekit.ParticipantInfo{
		Sid:      "PA_pub",
		Identity: "b_9yk6bv7i",
		Tracks:   infos,
	})
	return p
}

func TestBuildVideoLayersResponse(t *testing.T) {
	at := time.Date(2026, 9, 22, 9, 40, 0, 0, time.UTC)

	t.Run("declared carries the identity, live only the index", func(t *testing.T) {
		video := newFakeLiveTrack(publisherTrackInfo(), []rtc.LiveLayer{
			{SpatialLayer: 2, SinceMs: 812340, Transitions: 7},
			{SpatialLayer: 0, SinceMs: 812340, Transitions: 0},
		}, true)

		resp := buildVideoLayersResponse("s_5e23ad61", "RM_x", "ND_y", at,
			[]types.LocalParticipant{fakePublisher(video, newFakeLiveTrack(audioTrackInfo(), nil, false))})

		require.Equal(t, "s_5e23ad61", resp.Room)
		require.Equal(t, "RM_x", resp.RoomID)
		require.Equal(t, "ND_y", resp.NodeID)
		require.Equal(t, "2026-09-22T09:40:00Z", resp.At)

		// audio never appears
		require.Len(t, resp.Tracks, 1)
		tr := resp.Tracks[0]
		require.Equal(t, "TR_video", tr.TrackSid)
		require.Equal(t, "b_9yk6bv7i", tr.ParticipantIdentity)
		require.Equal(t, "PA_pub", tr.ParticipantSid)
		require.Equal(t, "video/H264", tr.MimeType)

		// declared is sorted by spatial layer even though the TrackInfo listed them 2, 0, 1
		require.Equal(t, []DeclaredVideoLayer{
			{SpatialLayer: 0, Quality: "LOW", Rid: "q", Width: 426, Height: 240, Bitrate: 400000},
			{SpatialLayer: 1, Quality: "MEDIUM", Rid: "h", Width: 852, Height: 480, Bitrate: 1000000},
			{SpatialLayer: 2, Quality: "HIGH", Rid: "f", Width: 1280, Height: 720, Bitrate: 2500000},
		}, tr.Declared)

		// live references that same index space and carries no identity of its own
		require.True(t, tr.LiveKnown)
		require.Equal(t, []LiveVideoLayer{
			{SpatialLayer: 2, SinceMs: 812340, Transitions: 7},
			{SpatialLayer: 0, SinceMs: 812340, Transitions: 0},
		}, tr.Live)

		// AND THE DECLARED LADDER IS NOT PRUNED BY IT: rung 1 is not delivering and is still
		// there, which is the whole of AST-449.
		require.Len(t, tr.Declared, 3)
	})

	t.Run("nothing delivering and no reading are different answers", func(t *testing.T) {
		nothing := buildVideoLayersResponse("r", "RM", "ND", at,
			[]types.LocalParticipant{fakePublisher(newFakeLiveTrack(publisherTrackInfo(), []rtc.LiveLayer{}, true))})
		require.True(t, nothing.Tracks[0].LiveKnown)
		require.Empty(t, nothing.Tracks[0].Live)
		require.Len(t, nothing.Tracks[0].Declared, 3)

		unknown := buildVideoLayersResponse("r", "RM", "ND", at,
			[]types.LocalParticipant{fakePublisher(newFakeLiveTrack(publisherTrackInfo(), nil, false))})
		require.False(t, unknown.Tracks[0].LiveKnown)
		require.Empty(t, unknown.Tracks[0].Live)
		require.Len(t, unknown.Tracks[0].Declared, 3)
	})

	t.Run("a single layer track is one rung, not an empty ladder", func(t *testing.T) {
		ti := &livekit.TrackInfo{
			Sid: "TR_single", Type: livekit.TrackType_VIDEO, MimeType: "video/H264",
			Width: 640, Height: 360,
		}
		resp := buildVideoLayersResponse("r", "RM", "ND", at,
			[]types.LocalParticipant{fakePublisher(newFakeLiveTrack(ti, []rtc.LiveLayer{{SpatialLayer: 0}}, true))})

		require.Equal(t, []DeclaredVideoLayer{
			{SpatialLayer: 0, Quality: "HIGH", Width: 640, Height: 360},
		}, resp.Tracks[0].Declared)

		// the same rule rtc.DeclaredSpatialLayers applies, pinned here so the two cannot drift
		require.Equal(t, []int32{0}, rtc.DeclaredSpatialLayers(ti))
	})

	t.Run("a room with no publishers is an empty list, never null", func(t *testing.T) {
		resp := buildVideoLayersResponse("r", "RM", "ND", at, nil)
		require.NotNil(t, resp.Tracks)
		require.Empty(t, resp.Tracks)
	})
}

// A Go consumer decodes both `null` and `[]` into a nil slice, so the shape on the wire has to
// carry the discriminator explicitly. This is the test that keeps `live` from ever being null.
func TestVideoLayersJSONShape(t *testing.T) {
	at := time.Date(2026, 9, 22, 9, 40, 0, 0, time.UTC)
	resp := buildVideoLayersResponse("r", "RM", "ND", at,
		[]types.LocalParticipant{fakePublisher(newFakeLiveTrack(publisherTrackInfo(), nil, false))})

	raw, err := json.Marshal(resp)
	require.NoError(t, err)
	require.Contains(t, string(raw), `"liveKnown":false`)
	require.Contains(t, string(raw), `"live":[]`)
	require.NotContains(t, string(raw), `"live":null`)
	require.NotContains(t, string(raw), `"tracks":null`)
}

// ---------------------------------------------------------------------------------------------
// the handler

func adminRequest(t *testing.T, room string, grantRoom string) *http.Request {
	t.Helper()

	r := httptest.NewRequest(http.MethodGet, "/astream/v1/rooms/"+room+"/video-layers", nil)
	if grantRoom != "" {
		r = r.WithContext(WithAPIKey(r.Context(), &auth.ClaimGrants{
			Video: &auth.VideoGrant{RoomAdmin: true, Room: grantRoom},
		}, "APIkey"))
	}
	return r
}

func serve(t *testing.T, svc *VideoLayersService, r *http.Request) *httptest.ResponseRecorder {
	t.Helper()

	mux := http.NewServeMux()
	svc.SetupRoutes(mux)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	return w
}

func TestVideoLayersHandler(t *testing.T) {
	onThisNode := func(_ context.Context, _ livekit.RoomName) (livekit.RoomID, []types.LocalParticipant, bool) {
		return "RM_here", []types.LocalParticipant{
			fakePublisher(newFakeLiveTrack(publisherTrackInfo(), []rtc.LiveLayer{{SpatialLayer: 2, SinceMs: 5000, Transitions: 3}}, true)),
		}, true
	}
	elsewhere := func(_ context.Context, _ livekit.RoomName) (livekit.RoomID, []types.LocalParticipant, bool) {
		return "", nil, false
	}

	t.Run("serves the room it hosts", func(t *testing.T) {
		w := serve(t, NewVideoLayersService("ND_here", onThisNode), adminRequest(t, "s_room", "s_room"))
		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, "application/json", w.Header().Get("Content-Type"))

		var resp VideoLayersResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		require.Equal(t, "s_room", resp.Room)
		require.Equal(t, "ND_here", resp.NodeID)
		require.Len(t, resp.Tracks, 1)
		require.Len(t, resp.Tracks[0].Declared, 3)
		require.Equal(t, int64(5000), resp.Tracks[0].Live[0].SinceMs)
	})

	// The room not being here is NOT the room being empty. A consumer that read an empty track
	// list off a node that simply does not host the room would conclude the publisher stopped.
	t.Run("a room on another node is a 404 that says so", func(t *testing.T) {
		w := serve(t, NewVideoLayersService("ND_here", elsewhere), adminRequest(t, "s_room", "s_room"))
		require.Equal(t, http.StatusNotFound, w.Code)
		require.Contains(t, w.Body.String(), "not hosted on this node")
	})

	t.Run("no token is refused", func(t *testing.T) {
		w := serve(t, NewVideoLayersService("ND_here", onThisNode), adminRequest(t, "s_room", ""))
		require.Equal(t, http.StatusUnauthorized, w.Code)
	})

	// The grant is scoped to a room, exactly as RoomService scopes it. A roomAdmin token for one
	// room must not read another.
	t.Run("a token for another room is refused", func(t *testing.T) {
		w := serve(t, NewVideoLayersService("ND_here", onThisNode), adminRequest(t, "s_room", "s_other"))
		require.Equal(t, http.StatusUnauthorized, w.Code)
	})

	t.Run("a token without roomAdmin is refused", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/astream/v1/rooms/s_room/video-layers", nil)
		r = r.WithContext(WithAPIKey(r.Context(), &auth.ClaimGrants{
			Video: &auth.VideoGrant{RoomJoin: true, Room: "s_room"},
		}, "APIkey"))
		w := serve(t, NewVideoLayersService("ND_here", onThisNode), r)
		require.Equal(t, http.StatusUnauthorized, w.Code)
	})

	// CONTROL NEGATIVO of the four refusals above: with the right grant the same harness returns
	// 200, so they are proving the authorisation and not a broken route.
	t.Run("the route itself is reachable", func(t *testing.T) {
		w := serve(t, NewVideoLayersService("ND_here", onThisNode), adminRequest(t, "s_room", "s_room"))
		require.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("only GET is routed", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/astream/v1/rooms/s_room/video-layers", nil)
		w := serve(t, NewVideoLayersService("ND_here", onThisNode), r)
		require.Equal(t, http.StatusMethodNotAllowed, w.Code)
	})
}
