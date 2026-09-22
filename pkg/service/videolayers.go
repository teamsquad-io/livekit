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

// AST-449. GET /astream/v1/rooms/{room}/video-layers — the ladder a published video track
// DECLARED and the rungs it is DELIVERING, side by side, nothing filtered out.
//
// WHY THIS EXISTS AS ITS OWN ENDPOINT AND NOT AS A CHANGE TO ListParticipants. AST-429 made
// GetParticipant/ListParticipants report only the delivering rungs. Two things were wrong with
// that, both measured:
//
//   - It changes the meaning of a standard LiveKit response for every consumer of it, and the
//     control plane has been reading that response since AST-312.
//   - It hands the consumer ONE list, so the consumer cannot tell "this rung does not exist"
//     from "this rung is not delivering this second". Our packager spent an expensive decision
//     (how many subscriptions to open against the SFU) on the volatile signal, and on
//     2026-09-22 it tore down and reopened a live subscription eleven times in ten minutes on a
//     publisher that never stopped delivering.
//
// So ListParticipants is back to upstream behaviour and the two facts are reported here,
// separately, in the same index space.
//
// IT ANSWERS FOR THIS NODE ONLY. The live reading exists only on the node hosting the room —
// every other path answers from the room store snapshot, which does not move per layer
// transition. The packager runs on the origin and calls this over loopback, so node-local is
// exactly the reach it needs; a room that is not here is a 404 that says so.
package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"

	"github.com/livekit/livekit-server/pkg/rtc"
	"github.com/livekit/livekit-server/pkg/rtc/types"
)

const videoLayersPath = "GET /astream/v1/rooms/{room}/video-layers"

var ErrRoomNotOnThisNode = errors.New("room is not hosted on this node")

// RoomParticipantsLookup resolves a room hosted on this node to its participants. False means
// the room is not on this node, which is NOT the same as a room with no participants.
// *RoomManager.RoomParticipants is the production implementation.
type RoomParticipantsLookup func(ctx context.Context, room livekit.RoomName) (livekit.RoomID, []types.LocalParticipant, bool)

// VideoLayersService serves the declared + live ladder of every published video track of a room.
type VideoLayersService struct {
	nodeID livekit.NodeID
	lookup RoomParticipantsLookup
}

func NewVideoLayersService(nodeID livekit.NodeID, lookup RoomParticipantsLookup) *VideoLayersService {
	return &VideoLayersService{nodeID: nodeID, lookup: lookup}
}

func (s *VideoLayersService) SetupRoutes(mux *http.ServeMux) {
	mux.HandleFunc(videoLayersPath, s.videoLayers)
}

// videoLayers is read-only and takes only the read locks the accessors already take.
//
// AUTHORISATION IS THE ONE RoomService ALREADY USES for this room: a token with video.roomAdmin
// whose room matches. No new scheme, no new key, and no way to read a room the caller could not
// already call ListParticipants on.
func (s *VideoLayersService) videoLayers(w http.ResponseWriter, r *http.Request) {
	roomName := livekit.RoomName(r.PathValue("room"))
	if roomName == "" {
		HandleErrorJson(w, r, http.StatusBadRequest, errors.New("room is required"))
		return
	}

	if err := EnsureAdminPermission(r.Context(), roomName); err != nil {
		HandleErrorJson(w, r, http.StatusUnauthorized, err, "room", roomName)
		return
	}

	roomID, participants, onThisNode := s.lookup(r.Context(), roomName)
	if !onThisNode {
		HandleErrorJson(w, r, http.StatusNotFound, ErrRoomNotOnThisNode, "room", roomName)
		return
	}

	resp := buildVideoLayersResponse(roomName, roomID, s.nodeID, time.Now().UTC(), participants)

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		logger.Warnw("failed to write video layers response", err, "room", roomName)
	}
}

// ---------------------------------------------------------------------------------------------
// the wire shape

type VideoLayersResponse struct {
	Room   string             `json:"room"`
	RoomID string             `json:"roomId"`
	NodeID string             `json:"nodeId"`
	At     string             `json:"at"`
	Tracks []VideoLayersTrack `json:"tracks"`
}

type VideoLayersTrack struct {
	TrackSid            string `json:"trackSid"`
	ParticipantIdentity string `json:"participantIdentity"`
	ParticipantSid      string `json:"participantSid"`
	MimeType            string `json:"mimeType"`

	// Declared is the ladder the publisher announced when it published, and it carries the whole
	// IDENTITY of every rung — quality, rid, spatial layer, dimensions, target bitrate. It is
	// stable: it does not move while the track is published.
	Declared []DeclaredVideoLayer `json:"declared"`

	// LiveKnown says whether a live reading was available AT ALL. It is the discriminator a
	// machine reads, because Go decodes both `null` and `[]` into a nil slice and could not
	// otherwise tell "we know nothing" from "nothing is delivering".
	LiveKnown bool `json:"liveKnown"`

	// Live names the rungs delivering right now BY INDEX ONLY — `spatialLayer`, referencing the
	// entries in Declared. The identity travels once, in Declared, so the two can never disagree
	// about which rung is which, and a consumer stops having to guess a rung from the pixel
	// dimensions of a frame.
	//
	// Always an array, never null. With LiveKnown false it is empty and says nothing.
	Live []LiveVideoLayer `json:"live"`
}

type DeclaredVideoLayer struct {
	SpatialLayer int32  `json:"spatialLayer"`
	Quality      string `json:"quality"`
	Rid          string `json:"rid"`
	Width        uint32 `json:"width"`
	Height       uint32 `json:"height"`
	Bitrate      uint32 `json:"bitrate"`
}

type LiveVideoLayer struct {
	SpatialLayer int32 `json:"spatialLayer"`

	// SinceMs is how long this rung has been delivering without interruption, and Transitions how
	// many times it has flipped since the track was published. Together they let a consumer
	// amortise a flapping rung on a measurement instead of on a typed-in threshold.
	SinceMs     int64  `json:"sinceMs"`
	Transitions uint64 `json:"transitions"`
}

// buildVideoLayersResponse is the whole of the logic, pure and with no SFU, no HTTP and no clock
// of its own, so it is testable on its own.
//
// Declared comes from the participant's own ParticipantInfo — the very bytes ListParticipants
// returns — so this endpoint and that one can never disagree about the declared ladder.
func buildVideoLayersResponse(
	room livekit.RoomName,
	roomID livekit.RoomID,
	nodeID livekit.NodeID,
	at time.Time,
	participants []types.LocalParticipant,
) *VideoLayersResponse {
	resp := &VideoLayersResponse{
		Room:   string(room),
		RoomID: string(roomID),
		NodeID: string(nodeID),
		At:     at.UTC().Format(time.RFC3339Nano),
		Tracks: []VideoLayersTrack{},
	}

	for _, p := range participants {
		if p == nil {
			continue
		}

		liveness := make(map[livekit.TrackID]rtc.VideoLayerLiveness)
		for _, reading := range rtc.VideoLayerLivenessOfParticipant(p) {
			liveness[reading.TrackID] = reading
		}

		pi := p.ToProto()
		if pi == nil {
			continue
		}

		for _, ti := range pi.GetTracks() {
			if ti.GetType() != livekit.TrackType_VIDEO {
				continue
			}

			track := VideoLayersTrack{
				TrackSid:            ti.GetSid(),
				ParticipantIdentity: pi.GetIdentity(),
				ParticipantSid:      pi.GetSid(),
				MimeType:            ti.GetMimeType(),
				Declared:            declaredVideoLayers(ti),
				Live:                []LiveVideoLayer{},
			}

			if reading, ok := liveness[livekit.TrackID(ti.GetSid())]; ok {
				track.LiveKnown = reading.LiveKnown
				for _, l := range reading.Live {
					track.Live = append(track.Live, LiveVideoLayer{
						SpatialLayer: l.SpatialLayer,
						SinceMs:      l.SinceMs,
						Transitions:  l.Transitions,
					})
				}
			}

			resp.Tracks = append(resp.Tracks, track)
		}
	}

	return resp
}

// declaredVideoLayers is rtc.DeclaredVideoLayers on the wire. The ladder itself is read through
// the rtc accessor and not re-derived here, so this endpoint and the Prometheus gauge can never
// disagree about what a track declares.
//
// A video track with no layer list is a single-layer track, which the SFU treats as spatial layer
// 0, and it is reported as one rung rather than as an empty ladder — the same rule
// rtc.DeclaredSpatialLayers applies, and a test in this package pins the two together.
func declaredVideoLayers(ti *livekit.TrackInfo) []DeclaredVideoLayer {
	layers := rtc.DeclaredVideoLayers(ti)
	if len(layers) == 0 {
		return []DeclaredVideoLayer{{
			SpatialLayer: 0,
			Quality:      livekit.VideoQuality_HIGH.String(),
			Width:        ti.GetWidth(),
			Height:       ti.GetHeight(),
		}}
	}

	out := make([]DeclaredVideoLayer, 0, len(layers))
	for _, l := range layers {
		out = append(out, DeclaredVideoLayer{
			SpatialLayer: l.GetSpatialLayer(),
			Quality:      l.GetQuality().String(),
			Rid:          l.GetRid(),
			Width:        l.GetWidth(),
			Height:       l.GetHeight(),
			Bitrate:      l.GetBitrate(),
		})
	}
	return out
}
