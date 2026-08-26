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
)

var (
	ErrBulkRoomRequired       = errors.New("room is required")
	ErrBulkItemsRequired      = errors.New("items must not be empty")
	ErrBulkIdentityRequired   = errors.New("identity is required")
	ErrBulkOpsRequired        = errors.New("ops must not be empty")
	ErrBulkTrackSidsRequired  = errors.New("trackSids must not be empty")
	ErrBulkPermissionRequired = errors.New("permission is required")
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
	for i, raw := range req.Items {
		item, err := parseBulkItem(raw)
		if err != nil {
			return nil, fmt.Errorf("items[%d]: %w", i, err)
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
