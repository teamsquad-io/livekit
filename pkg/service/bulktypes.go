package service

import (
	"encoding/json"
	"errors"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/livekit/protocol/livekit"
)

const (
	bulkOpPermission  = "permission"
	bulkOpSubscribe   = "subscribe"
	bulkOpUnsubscribe = "unsubscribe"

	// maxBulkFailuresReported bounds the failures array in the response, so a
	// sweep where every one of max_items participants failed cannot produce a
	// multi-megabyte body. The counters still report the true totals.
	maxBulkFailuresReported = 100

	// Real traffic sends 2-3 ops per participant (a re-grant is
	// permission+subscribe; a deny is unsubscribe+permission), so these caps
	// are generous headroom, not a tuning knob. They exist because MaxItems
	// alone does NOT bound the work: without them a single item can carry
	// ~270k ops inside the 10 MiB body limit, and one authenticated request
	// then occupies a handler goroutine for hundreds of thousands of
	// sequential RPCs.
	maxBulkOpsPerItem = 16
	// A room carries a handful of published tracks; 64 is far above any real
	// fan-out and still bounds a single op.
	maxBulkTrackSidsPerOp = 64
	// Bounds the TOTAL work of one request. maxBulkOpsPerItem alone does not:
	// MaxItems(5000) x 16 would still be 80k ops. Real worst case is ~2500
	// participants x 3 ops = 7500.
	maxBulkOpsPerRequest = 20000
)

var (
	ErrBulkRoomRequired       = errors.New("room is required")
	ErrBulkItemsRequired      = errors.New("items must not be empty")
	ErrBulkIdentityRequired   = errors.New("identity is required")
	ErrBulkOpsRequired        = errors.New("ops must not be empty")
	ErrBulkTrackSidsRequired  = errors.New("trackSids must not be empty")
	ErrBulkPermissionRequired = errors.New("permission is required")

	ErrBulkTooManyOps          = errors.New("too many ops for one item")
	ErrBulkTooManyTrackSids    = errors.New("too many trackSids for one op")
	ErrBulkTooManyOpsInRequest = errors.New("too many ops in request")

	// ErrBulkNotAttempted marks an item that never entered the dispatch queue
	// (request deadline exceeded or client disconnected before it was
	// reached). It must NEVER be confused with a real op failure: a nil slot
	// means success to compactFailures, so an item that was never touched
	// MUST be turned into an explicit failure, or it silently counts as
	// applied.
	ErrBulkNotAttempted = errors.New("not attempted: request deadline exceeded or client disconnected")

	// ErrBulkOpDeadlineExceeded marks an op cut by BulkConfig.OpTimeout - OUR
	// per-op deadline - rather than by an error coming back from the
	// RoomService. It is wrapped into the reported failure so the two are
	// distinguishable in `failures[].error`: that is the whole point of the
	// knob, since counting these is how you find out how many ops are waiting
	// on a participant no node answers for.
	//
	// It is a FAILURE like any other. An op that ran out of time was NOT
	// applied, and reporting it as anything softer would silently count it as
	// applied - the same access leak ErrBulkNotAttempted exists to prevent.
	ErrBulkOpDeadlineExceeded = errors.New("op deadline exceeded")
)

// ---- wire types (what the client sends) ----

type bulkOpJSON struct {
	Op        string   `json:"op"`
	TrackSids []string `json:"trackSids,omitempty"`
	// Kept raw so it can be decoded with protojson into the LiveKit type
	// rather than a struct of our own that would drift on every upgrade.
	Permission json.RawMessage `json:"permission,omitempty"`
}

type bulkItemJSON struct {
	Identity string       `json:"identity"`
	Ops      []bulkOpJSON `json:"ops"`
}

type bulkRequest struct {
	Room  string         `json:"room"`
	Items []bulkItemJSON `json:"items"`
}

type bulkFailure struct {
	Identity string `json:"identity"`
	OpIndex  int    `json:"opIndex"`
	Error    string `json:"error"`
}

type bulkResponse struct {
	Applied   int            `json:"applied"`
	Failed    int            `json:"failed"`
	Truncated bool           `json:"truncated"`
	Failures  []*bulkFailure `json:"failures"`
}

// ---- parsed types (what the workers consume) ----

// parsedOp is a validated op with its permission already decoded, so a worker
// never touches JSON. Parsing returns new values and never mutates the request.
type parsedOp struct {
	kind       string
	trackSids  []string
	permission *livekit.ParticipantPermission
}

type parsedItem struct {
	identity string
	ops      []parsedOp
}

// parseBulkRequest validates the whole envelope and decodes every permission
// BEFORE any op is applied. That ordering is the point: a typo in item 1900
// must not leave items 0..1899 applied.
//
// maxItems of 0 disables the item cap.
func parseBulkRequest(req *bulkRequest, maxItems int) ([]parsedItem, error) {
	if req.Room == "" {
		return nil, ErrBulkRoomRequired
	}
	if len(req.Items) == 0 {
		return nil, ErrBulkItemsRequired
	}
	if maxItems > 0 && len(req.Items) > maxItems {
		return nil, fmt.Errorf("items exceeds max_items: %d > %d", len(req.Items), maxItems)
	}

	items := make([]parsedItem, 0, len(req.Items))
	totalOps := 0
	for i, raw := range req.Items {
		item, err := parseBulkItem(raw)
		if err != nil {
			return nil, fmt.Errorf("items[%d]: %w", i, err)
		}
		// Checked inside the loop, against the running total, so an abusive
		// request is cut off early instead of building the full item list
		// first.
		totalOps += len(item.ops)
		if totalOps > maxBulkOpsPerRequest {
			return nil, ErrBulkTooManyOpsInRequest
		}
		items = append(items, item)
	}
	return items, nil
}

func parseBulkItem(raw bulkItemJSON) (parsedItem, error) {
	if raw.Identity == "" {
		return parsedItem{}, ErrBulkIdentityRequired
	}
	if len(raw.Ops) == 0 {
		return parsedItem{}, ErrBulkOpsRequired
	}
	if len(raw.Ops) > maxBulkOpsPerItem {
		return parsedItem{}, ErrBulkTooManyOps
	}

	ops := make([]parsedOp, 0, len(raw.Ops))
	for j, rawOp := range raw.Ops {
		op, err := parseBulkOp(rawOp)
		if err != nil {
			return parsedItem{}, fmt.Errorf("ops[%d]: %w", j, err)
		}
		ops = append(ops, op)
	}
	return parsedItem{identity: raw.Identity, ops: ops}, nil
}

func parseBulkOp(raw bulkOpJSON) (parsedOp, error) {
	switch raw.Op {
	case bulkOpPermission:
		if len(raw.Permission) == 0 {
			return parsedOp{}, ErrBulkPermissionRequired
		}
		var perm livekit.ParticipantPermission
		if err := protojson.Unmarshal(raw.Permission, &perm); err != nil {
			return parsedOp{}, fmt.Errorf("invalid permission: %w", err)
		}
		return parsedOp{kind: bulkOpPermission, permission: &perm}, nil

	case bulkOpSubscribe, bulkOpUnsubscribe:
		if len(raw.TrackSids) == 0 {
			return parsedOp{}, ErrBulkTrackSidsRequired
		}
		if len(raw.TrackSids) > maxBulkTrackSidsPerOp {
			return parsedOp{}, ErrBulkTooManyTrackSids
		}
		return parsedOp{kind: raw.Op, trackSids: raw.TrackSids}, nil

	default:
		return parsedOp{}, fmt.Errorf("unknown op: %q", raw.Op)
	}
}

// compactFailures drops the empty slots and caps the reported failures.
// The returned total is the TRUE count, not the length of the slice.
func compactFailures(slots []*bulkFailure) (failures []*bulkFailure, total int, truncated bool) {
	for _, f := range slots {
		if f == nil {
			continue
		}
		total++
		if len(failures) < maxBulkFailuresReported {
			failures = append(failures, f)
		} else {
			truncated = true
		}
	}
	return failures, total, truncated
}
