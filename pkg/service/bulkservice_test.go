package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

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

func decodeBulkResponse(t *testing.T, w *httptest.ResponseRecorder) *bulkResponse {
	t.Helper()
	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
	var res bulkResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &res))
	return &res
}

// ---- ordering and failure-isolation tests ----
//
// apply() is a plain sequential loop today. Task 6 replaces it with a queue
// and a worker pool - concurrency is exactly what breaks per-item op order if
// the pool is wrong. These tests are written and pinned NOW, against the
// sequential implementation, on purpose: they are the net that catches Task 6
// breaking the invariant, not a test suite shaped after the pool's behavior.

// The op order inside an item is a correctness invariant: with
// canSubscribe:false a subscribe does not attach (see the note in
// applyItem/participant.go), so a re-grant must set the permission BEFORE
// subscribing, and a deny must unsubscribe BEFORE pinning canSubscribe:false.
func TestBulkService_PreservesOpOrderWithinItem(t *testing.T) {
	stub := newStubUpdater()
	body := `{"room":"r1","items":[
		{"identity":"v_regrant","ops":[
			{"op":"permission","permission":{"canSubscribe":true}},
			{"op":"subscribe","trackSids":["TR_a","TR_b"]}
		]},
		{"identity":"v_deny","ops":[
			{"op":"unsubscribe","trackSids":["TR_a","TR_b"]},
			{"op":"permission","permission":{"canSubscribe":false}}
		]}
	]}`
	w := doBulk(t, bulkTestService(stub, 2), body, adminGrants("r1"))
	res := decodeBulkResponse(t, w)

	require.Equal(t, 2, res.Applied)
	require.Equal(t, 0, res.Failed)

	regrant := stub.callsFor("v_regrant")
	require.Len(t, regrant, 2)
	require.Equal(t, bulkOpPermission, regrant[0].kind, "re-grant: permission must be set before subscribe")
	require.Equal(t, bulkOpSubscribe, regrant[1].kind)

	deny := stub.callsFor("v_deny")
	require.Len(t, deny, 2)
	require.Equal(t, bulkOpUnsubscribe, deny[0].kind, "deny: unsubscribe must happen before the permission is pinned")
	require.Equal(t, bulkOpPermission, deny[1].kind)
}

func TestBulkService_FailureAbortsOnlyThatChain(t *testing.T) {
	stub := newStubUpdater()
	stub.errOn["v_gone:"+bulkOpPermission] = ErrParticipantNotFound

	body := `{"room":"r1","items":[
		{"identity":"v_ok","ops":[
			{"op":"permission","permission":{"canSubscribe":true}},
			{"op":"subscribe","trackSids":["TR_a"]}
		]},
		{"identity":"v_gone","ops":[
			{"op":"permission","permission":{"canSubscribe":true}},
			{"op":"subscribe","trackSids":["TR_a"]}
		]}
	]}`
	w := doBulk(t, bulkTestService(stub, 2), body, adminGrants("r1"))
	res := decodeBulkResponse(t, w)

	require.Equal(t, 1, res.Applied)
	require.Equal(t, 1, res.Failed)
	require.False(t, res.Truncated)
	require.Len(t, res.Failures, 1)
	require.Equal(t, "v_gone", res.Failures[0].Identity)
	require.Equal(t, 0, res.Failures[0].OpIndex)

	require.Len(t, stub.callsFor("v_gone"), 1, "the chain must stop at the first failure; subscribe must not be attempted")
	require.Len(t, stub.callsFor("v_ok"), 2, "one viewer's failure must not affect another viewer in the same request")
}

// This variant pins that OpIndex is the REAL index of the failing op, not
// always 0 and not the last index - a failure in the middle of a three-op
// chain must report index 1 and must not run op index 2.
func TestBulkService_FailureMidChainAbortsRest(t *testing.T) {
	stub := newStubUpdater()
	stub.errOn["v_1:"+bulkOpSubscribe] = ErrParticipantNotFound

	body := `{"room":"r1","items":[
		{"identity":"v_1","ops":[
			{"op":"permission","permission":{"canSubscribe":true}},
			{"op":"subscribe","trackSids":["TR_a"]},
			{"op":"unsubscribe","trackSids":["TR_b"]}
		]}
	]}`
	w := doBulk(t, bulkTestService(stub, 2), body, adminGrants("r1"))
	res := decodeBulkResponse(t, w)

	require.Equal(t, 0, res.Applied)
	require.Equal(t, 1, res.Failed)
	require.Len(t, res.Failures, 1)
	require.Equal(t, 1, res.Failures[0].OpIndex, "opIndex must be the real index of the failing op, not 0 or the last")

	calls := stub.callsFor("v_1")
	require.Len(t, calls, 2, "the first two ops ran; the third op must not have been attempted")
	require.Equal(t, bulkOpPermission, calls[0].kind)
	require.Equal(t, bulkOpSubscribe, calls[1].kind)
}

// Real idempotency in production comes from the fast-path in
// participant.SetPermission / MatchesPermission (pkg/rtc/participant.go:851):
// re-applying the same permission there is a no-op. What THIS test pins is
// narrower and just as necessary: the HANDLER itself must not carry state
// between requests, so sending the same request twice produces the same
// result twice rather than, say, a change on the second call because
// something was cached from the first.
func TestBulkService_IsIdempotent(t *testing.T) {
	stub := newStubUpdater()
	svc := bulkTestService(stub, 2)

	w1 := doBulk(t, svc, oneItemBody, adminGrants("r1"))
	res1 := decodeBulkResponse(t, w1)
	require.Equal(t, 1, res1.Applied)
	require.Equal(t, 0, res1.Failed)

	w2 := doBulk(t, svc, oneItemBody, adminGrants("r1"))
	res2 := decodeBulkResponse(t, w2)
	require.Equal(t, 1, res2.Applied)
	require.Equal(t, 0, res2.Failed)

	require.Equal(t, 2, stub.callCount(), "the same request applied twice must produce twice the calls, not be deduped by the handler")
}

// The failures slice is bounded by maxBulkFailuresReported, but the Failed
// counter must keep reporting the TRUE total so a client cannot be lied to
// about how much of a sweep actually broke.
func TestBulkService_FailuresAreTruncated(t *testing.T) {
	stub := newStubUpdater()
	n := maxBulkFailuresReported + 10

	items := make([]bulkItemJSON, n)
	for i := range items {
		identity := fmt.Sprintf("v_%d", i)
		items[i] = bulkItemJSON{Identity: identity, Ops: []bulkOpJSON{{Op: bulkOpSubscribe, TrackSids: []string{"TR_a"}}}}
		stub.errOn[identity+":"+bulkOpSubscribe] = ErrParticipantNotFound
	}
	req := &bulkRequest{Room: "r1", Items: items}
	body, err := json.Marshal(req)
	require.NoError(t, err)

	w := doBulk(t, bulkTestService(stub, 2), string(body), adminGrants("r1"))
	res := decodeBulkResponse(t, w)

	require.Equal(t, 0, res.Applied)
	require.Equal(t, n, res.Failed, "the failed counter must be the TRUE total, not the length of the truncated slice")
	require.Len(t, res.Failures, maxBulkFailuresReported)
	require.True(t, res.Truncated)
}

// ---- worker pool tests (Task 6) ----
//
// The unit of work is the ITEM (one participant, whole chain), not the op:
// ops within an item stay in order (applyItem), items run in parallel with
// each other. These tests are the criterion for correctness of the pool -
// the Task 5 ordering/failure-isolation tests must ALSO stay green once the
// pool lands, since that is what proves parallelism landed at the item level
// and not the op level.

// Pins that the pool ACTUALLY parallelises: the hook blocks until `workers`
// items are inside at once. Against a sequential apply this test hangs and
// fails on the timeout, which is the point.
func TestBulkService_AppliesItemsConcurrently(t *testing.T) {
	const workers = 4

	stub := newStubUpdater()
	var (
		mu      sync.Mutex
		inside  int
		reached = make(chan struct{})
		release = make(chan struct{})
		once    sync.Once
	)
	stub.hook = func(string) {
		mu.Lock()
		inside++
		if inside >= workers {
			once.Do(func() { close(reached) })
		}
		mu.Unlock()
		<-release
	}

	// 8 items, one cheap op each
	req := &bulkRequest{Room: "r1"}
	for i := 0; i < 8; i++ {
		req.Items = append(req.Items, bulkItemJSON{
			Identity: "v_" + strconv.Itoa(i),
			Ops:      []bulkOpJSON{{Op: bulkOpSubscribe, TrackSids: []string{"T"}}},
		})
	}
	body, err := json.Marshal(req)
	require.NoError(t, err)

	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- doBulkCtx(context.Background(), bulkTestService(stub, workers), string(body), adminGrants("r1"))
	}()

	select {
	case <-reached:
		close(release)
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatalf("never had %d items in flight at once: the pool does not parallelise", workers)
	}

	res := decodeBulkResponse(t, <-done)
	require.Equal(t, 8, res.Applied)
	require.Equal(t, 0, res.Failed)
}

// If the client hangs up, dispatch stops. There is no internal retry: the next
// sweep arrives in ~10s and the endpoint is idempotent.
func TestBulkService_StopsDispatchingOnCancel(t *testing.T) {
	stub := newStubUpdater()
	ctx, cancel := context.WithCancel(context.Background())
	stub.hook = func(string) { cancel() } // cancel while handling the first item

	req := &bulkRequest{Room: "r1"}
	for i := 0; i < 500; i++ {
		req.Items = append(req.Items, bulkItemJSON{
			Identity: "v_" + strconv.Itoa(i),
			Ops:      []bulkOpJSON{{Op: bulkOpSubscribe, TrackSids: []string{"T"}}},
		})
	}
	body, err := json.Marshal(req)
	require.NoError(t, err)

	doBulkCtx(ctx, bulkTestService(stub, 2), string(body), adminGrants("r1"))

	require.Less(t, stub.callCount(), 500, "must not keep dispatching all 500 after cancel")
}

// With a pool, each worker writes only its own slot. If indices got crossed,
// failures would be attributed to the wrong participant - silently. Half the
// items fail, and every reported failure must name an identity that really
// was set to fail.
func TestBulkService_FailuresAreAttributedToTheRightItem(t *testing.T) {
	stub := newStubUpdater()

	req := &bulkRequest{Room: "r1"}
	shouldFail := map[string]bool{}
	for i := 0; i < 100; i++ {
		id := "v_" + strconv.Itoa(i)
		if i%2 == 0 {
			stub.errOn[id+":"+bulkOpPermission] = ErrParticipantNotFound
			shouldFail[id] = true
		}
		req.Items = append(req.Items, bulkItemJSON{
			Identity: id,
			Ops:      []bulkOpJSON{{Op: bulkOpPermission, Permission: json.RawMessage(`{"canSubscribe":true}`)}},
		})
	}
	body, err := json.Marshal(req)
	require.NoError(t, err)

	res := decodeBulkResponse(t, doBulk(t, bulkTestService(stub, 8), string(body), adminGrants("r1")))

	require.Equal(t, 50, res.Failed)
	require.Equal(t, 50, res.Applied)
	for _, f := range res.Failures {
		require.True(t, shouldFail[f.Identity], "failure attributed to %q which was not set to fail", f.Identity)
	}
}

