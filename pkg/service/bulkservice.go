package service

import (
	"context"
	"encoding/json"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/livekit/protocol/livekit"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/telemetry/prometheus"
	"github.com/livekit/livekit-server/pkg/utils"
)

// bulkServiceOperation is the `type` label used with the existing
// service_operation counter, so bulk outcomes land in the same metric (and
// inherit its node_id/node_type labels) instead of a bespoke one.
const bulkServiceOperation = "bulk_participant_item"

// participantUpdater is the slice of livekit.RoomService this service needs.
// It is declared here, at the consumer, so tests need a two-method stub rather
// than a fake of the whole service. *RoomService satisfies it structurally.
type participantUpdater interface {
	UpdateParticipant(context.Context, *livekit.UpdateParticipantRequest) (*livekit.ParticipantInfo, error)
	UpdateSubscriptions(context.Context, *livekit.UpdateSubscriptionsRequest) (*livekit.UpdateSubscriptionsResponse, error)
}

// BulkService applies permission and subscription changes to many participants
// in a single request. It is a pure HTTP fan-in: every op goes through the
// existing RoomService, so auth, limits and routing are not reimplemented here
// and do not drift when upstream changes them.
type BulkService struct {
	conf    config.BulkConfig
	updater participantUpdater
}

func NewBulkService(conf config.BulkConfig, updater participantUpdater) *BulkService {
	return &BulkService{conf: conf, updater: updater}
}

func (s *BulkService) SetupRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /bulk/v1/participants", s.handleParticipants)
}

func (s *BulkService) handleParticipants(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req bulkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		HandleErrorJson(w, r, http.StatusBadRequest, err)
		return
	}
	// Checked before the auth call so a malformed request reports 400 rather
	// than masquerading as an authorization failure.
	if req.Room == "" {
		HandleErrorJson(w, r, http.StatusBadRequest, ErrBulkRoomRequired)
		return
	}

	// AUTHENTICATION. The API key middleware only populates the context when a
	// token is present; it calls next.ServeHTTP either way (auth.go:83-116).
	// THIS CALL is what keeps the route private. Removing it exposes the
	// endpoint to the internet. Pinned by the auth tests in bulkservice_test.go.
	if err := EnsureAdminPermission(ctx, livekit.RoomName(req.Room)); err != nil {
		HandleErrorJson(w, r, http.StatusForbidden, err)
		return
	}

	items, err := parseBulkRequest(&req, s.conf.MaxItems)
	if err != nil {
		HandleErrorJson(w, r, http.StatusBadRequest, err)
		return
	}

	started := time.Now()
	slots := s.apply(ctx, req.Room, items)
	failures, failed, truncated := compactFailures(slots)

	utils.GetLogger(ctx).Infow("bulk participants applied",
		"room", req.Room,
		"items", len(items),
		"applied", len(items)-failed,
		"failed", failed,
		"duration", time.Since(started),
	)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(&bulkResponse{
		Applied:   len(items) - failed,
		Failed:    failed,
		Truncated: truncated,
		Failures:  failures,
	})
}

// apply runs every item through a bounded worker pool.
//
// The unit of work is the ITEM — one participant with its whole chain — not the
// op: ops within an item must stay in order (see applyItem), while items are
// independent of each other. Parallelising at op level would break that.
//
// Each worker writes only to its own index in slots, so there is no shared
// mutable state and no mutex on the hot path.
func (s *BulkService) apply(ctx context.Context, room string, items []parsedItem) []*bulkFailure {
	slots := make([]*bulkFailure, len(items))

	n := s.workers()
	if n > len(items) {
		n = len(items)
	}
	queue := make(chan int, n*2)

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			for idx := range queue {
				slots[idx] = s.applyItem(ctx, room, items[idx])
			}
		}()
	}

dispatch:
	for i := range items {
		select {
		case queue <- i:
		case <-ctx.Done():
			// Client hung up. Stop dispatching; whatever is in flight finishes.
			// No internal retry: the next sweep arrives in ~10s and the endpoint
			// is idempotent (MatchesPermission fast path, participant.go:851).
			break dispatch
		}
	}
	close(queue)
	wg.Wait()

	return slots
}

// applyItem runs one participant's chain, IN ORDER, and ABORTS at the first
// failure. The order is a precondition: with canSubscribe:false a subscribe
// does not attach, so continuing the chain after a failed permission would only
// dirty the log and lie in the counters.
func (s *BulkService) applyItem(ctx context.Context, room string, item parsedItem) *bulkFailure {
	for i, op := range item.ops {
		if err := s.applyOp(ctx, room, item.identity, op); err != nil {
			prometheus.RecordServiceOperationError(bulkServiceOperation, op.kind)
			return &bulkFailure{Identity: item.identity, OpIndex: i, Error: err.Error()}
		}
	}
	prometheus.RecordServiceOperationSuccess(bulkServiceOperation)
	return nil
}

func (s *BulkService) applyOp(ctx context.Context, room, identity string, op parsedOp) error {
	if op.kind == bulkOpPermission {
		// Name/Metadata/Attributes are left zero ON PURPOSE: participant.go
		// :740-748 applies them only when non-empty, so this updates the
		// permission and nothing else. Do not "fix" this by fetching and
		// re-sending metadata — it would be a no-op at best and a clobber at
		// worst.
		_, err := s.updater.UpdateParticipant(ctx, &livekit.UpdateParticipantRequest{
			Room:       room,
			Identity:   identity,
			Permission: op.permission,
		})
		return err
	}

	_, err := s.updater.UpdateSubscriptions(ctx, &livekit.UpdateSubscriptionsRequest{
		Room:      room,
		Identity:  identity,
		TrackSids: op.trackSids,
		Subscribe: op.kind == bulkOpSubscribe,
	})
	return err
}

// workers resolves the configured worker count; 0 means GOMAXPROCS. Used by the
// pool added in Task 6.
func (s *BulkService) workers() int {
	if s.conf.Workers > 0 {
		return s.conf.Workers
	}
	return runtime.GOMAXPROCS(0)
}
