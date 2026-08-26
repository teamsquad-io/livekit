package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"

	"github.com/livekit/livekit-server/pkg/config"
)

// ---- stub of the updater: two methods, not fourteen ----

type recordedCall struct {
	kind       string // bulkOpPermission | bulkOpSubscribe | bulkOpUnsubscribe
	room       string
	identity   string
	trackSids  []string
	permission *livekit.ParticipantPermission
}

type stubUpdater struct {
	mu    sync.Mutex
	calls []recordedCall

	// errOn returns an error when the key "identity:kind" matches.
	errOn map[string]error
	// hook runs (outside the mutex) before recording, for the concurrency and
	// cancellation tests in later tasks.
	hook func(identity string)
}

func newStubUpdater() *stubUpdater {
	return &stubUpdater{errOn: map[string]error{}}
}

func (s *stubUpdater) record(kind, room, identity string, sids []string, permission *livekit.ParticipantPermission) error {
	if s.hook != nil {
		s.hook(identity)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, recordedCall{kind: kind, room: room, identity: identity, trackSids: sids, permission: permission})
	return s.errOn[identity+":"+kind]
}

func (s *stubUpdater) callsFor(identity string) []recordedCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []recordedCall
	for _, c := range s.calls {
		if c.identity == identity {
			out = append(out, c)
		}
	}
	return out
}

func (s *stubUpdater) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

func (s *stubUpdater) UpdateParticipant(_ context.Context, req *livekit.UpdateParticipantRequest) (*livekit.ParticipantInfo, error) {
	if err := s.record(bulkOpPermission, req.Room, req.Identity, nil, req.Permission); err != nil {
		return nil, err
	}
	return &livekit.ParticipantInfo{Identity: req.Identity}, nil
}

func (s *stubUpdater) UpdateSubscriptions(_ context.Context, req *livekit.UpdateSubscriptionsRequest) (*livekit.UpdateSubscriptionsResponse, error) {
	kind := bulkOpUnsubscribe
	if req.Subscribe {
		kind = bulkOpSubscribe
	}
	if err := s.record(kind, req.Room, req.Identity, req.TrackSids, nil); err != nil {
		return nil, err
	}
	return &livekit.UpdateSubscriptionsResponse{}, nil
}

// ---- request helpers ----

func bulkTestService(s *stubUpdater, workers int) *BulkService {
	return NewBulkService(config.BulkConfig{Workers: workers, MaxItems: 5000}, s)
}

// doBulkCtx runs the handler with the given context. A nil grants argument
// means the request carries NO token, which is exactly what the API key
// middleware lets through without grants.
func doBulkCtx(ctx context.Context, svc *BulkService, body string, grants *auth.ClaimGrants) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/bulk/v1/participants", strings.NewReader(body))
	if grants != nil {
		ctx = WithAPIKey(ctx, grants, "test-key")
	}
	r = r.WithContext(ctx)
	w := httptest.NewRecorder()

	mux := http.NewServeMux()
	svc.SetupRoutes(mux)
	mux.ServeHTTP(w, r)
	return w
}

func doBulk(t *testing.T, svc *BulkService, body string, grants *auth.ClaimGrants) *httptest.ResponseRecorder {
	t.Helper()
	return doBulkCtx(context.Background(), svc, body, grants)
}

func adminGrants(room string) *auth.ClaimGrants {
	return &auth.ClaimGrants{Video: &auth.VideoGrant{RoomAdmin: true, Room: room}}
}

const oneItemBody = `{"room":"r1","items":[{"identity":"v_1","ops":[{"op":"subscribe","trackSids":["TR_a"]}]}]}`

// ---- auth tests: the security requirement ----
//
// APIKeyAuthMiddleware does NOT authenticate (auth.go:83-116): without a token
// it calls next.ServeHTTP anyway. What keeps this route private is the
// handler's EnsureAdminPermission call. These tests are the guard rail.

func TestBulkService_NoTokenIsForbidden(t *testing.T) {
	stub := newStubUpdater()
	w := doBulk(t, bulkTestService(stub, 2), oneItemBody, nil)

	require.Equal(t, http.StatusForbidden, w.Code)
	require.Zero(t, stub.callCount(), "nothing may be applied without authorization")
}

func TestBulkService_WithoutRoomAdminIsForbidden(t *testing.T) {
	stub := newStubUpdater()
	grants := &auth.ClaimGrants{Video: &auth.VideoGrant{RoomAdmin: false, Room: "r1"}}
	w := doBulk(t, bulkTestService(stub, 2), oneItemBody, grants)

	require.Equal(t, http.StatusForbidden, w.Code)
	require.Zero(t, stub.callCount())
}

func TestBulkService_WrongRoomIsForbidden(t *testing.T) {
	stub := newStubUpdater()
	w := doBulk(t, bulkTestService(stub, 2), oneItemBody, adminGrants("another-room"))

	require.Equal(t, http.StatusForbidden, w.Code)
	require.Zero(t, stub.callCount())
}

func TestBulkService_BadJsonIsBadRequest(t *testing.T) {
	stub := newStubUpdater()
	w := doBulk(t, bulkTestService(stub, 2), `{"room":`, adminGrants("r1"))

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Zero(t, stub.callCount())
}

// An empty room must be 400, not 403: it is a malformed request, not an
// authorization failure. This pins the check order in the handler.
func TestBulkService_EmptyRoomIsBadRequest(t *testing.T) {
	stub := newStubUpdater()
	w := doBulk(t, bulkTestService(stub, 2), `{"room":"","items":[]}`, adminGrants("r1"))

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestBulkService_UnknownOpIsBadRequestAndAppliesNothing(t *testing.T) {
	stub := newStubUpdater()
	body := `{"room":"r1","items":[
		{"identity":"v_1","ops":[{"op":"subscribe","trackSids":["TR_a"]}]},
		{"identity":"v_2","ops":[{"op":"teleport"}]}
	]}`
	w := doBulk(t, bulkTestService(stub, 2), body, adminGrants("r1"))

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Zero(t, stub.callCount(), "an invalid op must not apply half a sweep")
}

// An item with more ops than maxBulkOpsPerItem must be rejected with 400, and
// nothing may be applied. This is the client-visible guarantee that backs the
// per-item op cap; the parser-level cases live in bulktypes_test.go.
func TestBulkService_TooManyOpsIsBadRequest(t *testing.T) {
	stub := newStubUpdater()
	req := &bulkRequest{Room: "r1", Items: []bulkItemJSON{
		{Identity: "v_1", Ops: opsOf(maxBulkOpsPerItem + 1)},
	}}
	body, err := json.Marshal(req)
	require.NoError(t, err)

	w := doBulk(t, bulkTestService(stub, 2), string(body), adminGrants("r1"))

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Zero(t, stub.callCount(), "too many ops must not apply anything")
}

// ---- harness tests: room and permission must survive the plumbing ----
//
// recordedCall used to drop req.Room and req.Permission, so nothing verified
// that apply() threads the request's room into every downstream call, or that
// a decoded permission arrives at UpdateParticipant unmodified. That gap
// matters most for Task 6, which refactors apply() into a worker pool -
// exactly the kind of change that can cross wires between items.

func TestBulkService_RoomThreadsThroughEveryCall(t *testing.T) {
	stub := newStubUpdater()
	body := `{"room":"r1","items":[
		{"identity":"v_1","ops":[
			{"op":"permission","permission":{"canSubscribe":true}},
			{"op":"subscribe","trackSids":["TR_a"]}
		]},
		{"identity":"v_2","ops":[{"op":"unsubscribe","trackSids":["TR_b"]}]}
	]}`
	w := doBulk(t, bulkTestService(stub, 2), body, adminGrants("r1"))

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, 3, stub.callCount())
	for _, c := range stub.calls {
		require.Equal(t, "r1", c.room, "every recorded call must carry the request's room")
	}
}

func TestBulkService_PermissionFieldsArriveIntact(t *testing.T) {
	stub := newStubUpdater()
	body := `{"room":"r1","items":[
		{"identity":"v_1","ops":[{"op":"permission","permission":{"canSubscribe":false,"canPublishData":true}}]}
	]}`
	w := doBulk(t, bulkTestService(stub, 2), body, adminGrants("r1"))

	require.Equal(t, http.StatusOK, w.Code)
	calls := stub.callsFor("v_1")
	require.Len(t, calls, 1)
	require.NotNil(t, calls[0].permission)
	require.False(t, calls[0].permission.CanSubscribe)
	require.True(t, calls[0].permission.CanPublishData)
}
