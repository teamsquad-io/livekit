package service

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func decodeBulk(t *testing.T, body string) *bulkRequest {
	t.Helper()
	var req bulkRequest
	require.NoError(t, json.Unmarshal([]byte(body), &req))
	return &req
}

func TestParseBulkRequest_Valid(t *testing.T) {
	req := decodeBulk(t, `{"room":"r1","items":[
		{"identity":"v_1","ops":[
			{"op":"permission","permission":{"canSubscribe":true}},
			{"op":"subscribe","trackSids":["TR_a"]}
		]},
		{"identity":"v_2","ops":[{"op":"unsubscribe","trackSids":["TR_a","TR_v"]}]}
	]}`)

	items, err := parseBulkRequest(req, 5000)
	require.NoError(t, err)
	require.Len(t, items, 2)

	require.Equal(t, "v_1", items[0].identity)
	require.Len(t, items[0].ops, 2)
	require.Equal(t, bulkOpPermission, items[0].ops[0].kind)
	require.NotNil(t, items[0].ops[0].permission)
	require.True(t, items[0].ops[0].permission.CanSubscribe)
	require.Equal(t, bulkOpSubscribe, items[0].ops[1].kind)
	require.Equal(t, []string{"TR_a"}, items[0].ops[1].trackSids)

	require.Equal(t, bulkOpUnsubscribe, items[1].ops[0].kind)
}

// protojson accepts both spellings, and that is deliberate: when upstream adds
// a field to the permission there is nothing to update here.
func TestParseBulkRequest_PermissionSnakeCase(t *testing.T) {
	req := decodeBulk(t, `{"room":"r1","items":[{"identity":"v_1","ops":[
		{"op":"permission","permission":{"can_subscribe":true,"can_publish_data":true}}
	]}]}`)

	items, err := parseBulkRequest(req, 5000)
	require.NoError(t, err)
	require.True(t, items[0].ops[0].permission.CanSubscribe)
	require.True(t, items[0].ops[0].permission.CanPublishData)
}

func TestParseBulkRequest_Rejects(t *testing.T) {
	cases := map[string]string{
		"empty room":         `{"room":"","items":[{"identity":"v_1","ops":[{"op":"subscribe","trackSids":["T"]}]}]}`,
		"empty items":        `{"room":"r1","items":[]}`,
		"empty identity":     `{"room":"r1","items":[{"identity":"","ops":[{"op":"subscribe","trackSids":["T"]}]}]}`,
		"empty ops":          `{"room":"r1","items":[{"identity":"v_1","ops":[]}]}`,
		"unknown op":         `{"room":"r1","items":[{"identity":"v_1","ops":[{"op":"teleport","trackSids":["T"]}]}]}`,
		"empty trackSids":    `{"room":"r1","items":[{"identity":"v_1","ops":[{"op":"subscribe","trackSids":[]}]}]}`,
		"missing permission": `{"room":"r1","items":[{"identity":"v_1","ops":[{"op":"permission"}]}]}`,
		"garbage permission": `{"room":"r1","items":[{"identity":"v_1","ops":[{"op":"permission","permission":{"nope":1}}]}]}`,
	}

	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			items, err := parseBulkRequest(decodeBulk(t, body), 5000)
			require.Error(t, err)
			require.Nil(t, items)
		})
	}
}

func TestParseBulkRequest_MaxItems(t *testing.T) {
	req := decodeBulk(t, `{"room":"r1","items":[
		{"identity":"v_1","ops":[{"op":"subscribe","trackSids":["T"]}]},
		{"identity":"v_2","ops":[{"op":"subscribe","trackSids":["T"]}]}
	]}`)

	_, err := parseBulkRequest(req, 1)
	require.ErrorContains(t, err, "max_items")

	// 0 disables the cap
	items, err := parseBulkRequest(req, 0)
	require.NoError(t, err)
	require.Len(t, items, 2)
}

// ---- op/trackSid/request caps ----
//
// MaxItems alone does not bound the work of a request: without these caps a
// single item can carry ~270k ops inside the 10 MiB body limit, occupying a
// handler goroutine for hundreds of thousands of sequential RPCs. These tests
// build the oversized payloads by code, not by hand.

func opsOf(n int) []bulkOpJSON {
	ops := make([]bulkOpJSON, n)
	for i := range ops {
		ops[i] = bulkOpJSON{Op: bulkOpSubscribe, TrackSids: []string{"T"}}
	}
	return ops
}

func TestParseBulkRequest_TooManyOpsInItem(t *testing.T) {
	req := &bulkRequest{Room: "r1", Items: []bulkItemJSON{
		{Identity: "v_1", Ops: opsOf(maxBulkOpsPerItem + 1)},
	}}

	items, err := parseBulkRequest(req, 0)
	require.ErrorIs(t, err, ErrBulkTooManyOps)
	require.Nil(t, items)
}

// The limit is inclusive: exactly maxBulkOpsPerItem ops must be accepted.
func TestParseBulkRequest_MaxOpsPerItemIsInclusive(t *testing.T) {
	req := &bulkRequest{Room: "r1", Items: []bulkItemJSON{
		{Identity: "v_1", Ops: opsOf(maxBulkOpsPerItem)},
	}}

	items, err := parseBulkRequest(req, 0)
	require.NoError(t, err)
	require.Len(t, items[0].ops, maxBulkOpsPerItem)
}

func TestParseBulkRequest_TooManyTrackSids(t *testing.T) {
	sids := make([]string, maxBulkTrackSidsPerOp+1)
	for i := range sids {
		sids[i] = fmt.Sprintf("T%d", i)
	}
	req := &bulkRequest{Room: "r1", Items: []bulkItemJSON{
		{Identity: "v_1", Ops: []bulkOpJSON{{Op: bulkOpSubscribe, TrackSids: sids}}},
	}}

	items, err := parseBulkRequest(req, 0)
	require.ErrorIs(t, err, ErrBulkTooManyTrackSids)
	require.Nil(t, items)
}

// Enough items x ops to cross maxBulkOpsPerRequest, with each item staying
// within maxBulkOpsPerItem so only the request-wide cap is exercised.
func TestParseBulkRequest_TooManyOpsInRequest(t *testing.T) {
	itemCount := maxBulkOpsPerRequest/maxBulkOpsPerItem + 10
	items := make([]bulkItemJSON, itemCount)
	for i := range items {
		items[i] = bulkItemJSON{Identity: fmt.Sprintf("v_%d", i), Ops: opsOf(maxBulkOpsPerItem)}
	}
	req := &bulkRequest{Room: "r1", Items: items}

	// maxItems disabled (0): we're exercising the ops-in-request cap here, not
	// the item cap.
	parsed, err := parseBulkRequest(req, 0)
	require.ErrorIs(t, err, ErrBulkTooManyOpsInRequest)
	require.Nil(t, parsed)
}

func TestCompactFailures(t *testing.T) {
	// nil slots are successes and must be dropped; the total is the TRUE count.
	slots := []*bulkFailure{nil, {Identity: "a"}, nil, {Identity: "b"}}
	failures, total, truncated := compactFailures(slots)
	require.Equal(t, 2, total)
	require.False(t, truncated)
	require.Len(t, failures, 2)
	require.Equal(t, "a", failures[0].Identity)
	require.Equal(t, "b", failures[1].Identity)

	// beyond the cap the reported slice is bounded but the total keeps counting
	many := make([]*bulkFailure, maxBulkFailuresReported+25)
	for i := range many {
		many[i] = &bulkFailure{Identity: "x"}
	}
	failures, total, truncated = compactFailures(many)
	require.Equal(t, maxBulkFailuresReported+25, total)
	require.True(t, truncated)
	require.Len(t, failures, maxBulkFailuresReported)
}
