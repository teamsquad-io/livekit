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

package test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/livekit/livekit-server/pkg/service"
	"github.com/livekit/livekit-server/pkg/testutils"
)

// AST-449. The unit tests in pkg/service prove the SHAPE; this one proves the endpoint is WIRED —
// registered on the real mux, behind the real APIKeyAuthMiddleware, answering from the real
// RoomManager. A unit test cannot fail if server.go never calls SetupRoutes.
//
// It needs no external SFU: the in-process pion client publishes a real track to a real node.

func getVideoLayers(t *testing.T, room, token string) (int, string) {
	t.Helper()

	req, err := http.NewRequest(http.MethodGet,
		fmt.Sprintf("http://localhost:%d/astream/v1/rooms/%s/video-layers", defaultServerPort, room), nil)
	require.NoError(t, err)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	return res.StatusCode, string(body)
}

func TestVideoLayersEndpoint(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
		return
	}

	_, finish := setupSingleNodeTest("TestVideoLayersEndpoint")
	defer finish()

	const room = testRoom

	c1 := createRTCClient("vl_publisher", defaultServerPort, testRTCServicePathv1, nil)
	waitUntilConnected(t, c1)
	defer c1.Stop()

	video, err := c1.AddStaticTrack("video/vp8", "video", "webcam")
	require.NoError(t, err)
	defer video.Stop()
	audio, err := c1.AddStaticTrack("audio/opus", "audio", "webcam")
	require.NoError(t, err)
	defer audio.Stop()

	t.Run("reports the declared ladder of the published video track", func(t *testing.T) {
		var resp service.VideoLayersResponse
		testutils.WithTimeout(t, func() string {
			code, body := getVideoLayers(t, room, adminRoomToken(room))
			if code != http.StatusOK {
				return fmt.Sprintf("expected 200, got %d: %s", code, body)
			}
			resp = service.VideoLayersResponse{}
			if err := json.Unmarshal([]byte(body), &resp); err != nil {
				return fmt.Sprintf("could not decode: %v (%s)", err, body)
			}
			if len(resp.Tracks) != 1 {
				return fmt.Sprintf("expected exactly one video track, got %d: %s", len(resp.Tracks), body)
			}
			return ""
		})

		require.Equal(t, room, resp.Room)
		require.NotEmpty(t, resp.RoomID)
		require.NotEmpty(t, resp.NodeID)
		require.NotEmpty(t, resp.At)

		track := resp.Tracks[0]
		require.Equal(t, "vl_publisher", track.ParticipantIdentity)
		require.NotEmpty(t, track.TrackSid)
		require.NotEmpty(t, track.Declared, "a published video track always declares at least one rung")
		require.Equal(t, int32(0), track.Declared[0].SpatialLayer)

		// The live reading EXISTS — the track has a real receiver behind it, so the answer is
		// "we looked", not "we know nothing". Whether the rung has been declared live yet
		// depends on the StreamTracker's own cycles and is deliberately not asserted here.
		require.True(t, track.LiveKnown, "a track with a real receiver must report a reading")
		require.NotNil(t, track.Live)
		for _, l := range track.Live {
			require.GreaterOrEqual(t, l.SinceMs, int64(0))
		}
	})

	// A room not on this node is NOT an empty room, and the endpoint must not let a consumer
	// read it as "the publisher stopped".
	t.Run("an unknown room is a 404 that says why", func(t *testing.T) {
		code, body := getVideoLayers(t, "no-such-room", adminRoomToken("no-such-room"))
		require.Equal(t, http.StatusNotFound, code)
		require.Contains(t, body, "not hosted on this node")
	})

	t.Run("no token is refused", func(t *testing.T) {
		code, _ := getVideoLayers(t, room, "")
		require.Equal(t, http.StatusUnauthorized, code)
	})

	t.Run("a roomAdmin token for another room is refused", func(t *testing.T) {
		code, _ := getVideoLayers(t, room, adminRoomToken("some-other-room"))
		require.Equal(t, http.StatusUnauthorized, code)
	})

	t.Run("a join token is refused", func(t *testing.T) {
		code, _ := getVideoLayers(t, room, joinToken(room, "someone", nil))
		require.Equal(t, http.StatusUnauthorized, code)
	})
}
