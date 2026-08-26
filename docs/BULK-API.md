# Bulk Participants API

`POST /bulk/v1/participants` applies permission and subscription changes to
many participants in a single HTTP request. It exists to replace the pattern
of one `UpdateParticipant`/`UpdateSubscriptions` call per participant: a sweep
over 2,500 viewers went from roughly 5,000 HTTP requests to 1.

This document is the contract for that endpoint. It is written for whoever is
integrating a client against it and has not read the server implementation.
If this document and the code ever disagree, the code is right and this
document is out of date — please fix it.

## Endpoint and authentication

```
POST /bulk/v1/participants
Authorization: Bearer <jwt>
Content-Type: application/json
```

The JWT must carry a video grant with `roomAdmin: true` **and** a `room`
claim that matches the `room` field of the request body **exactly** — this is
a plain string equality check, with no case folding or other normalization
(`pkg/service/auth.go:190`). A token scoped to `room-a` cannot be used against
a request for `room-A` or `Room-A`.

- No `Authorization` header, or a header without a valid token → `403`.
- A valid token without `roomAdmin`, or scoped to a different room → `403`.

This is enforced inside the handler, not by a routing rule — there is no
separate "is this a private endpoint" allowlist anywhere in the server. See
"Response codes" below for how this composes with the other failure modes.

## Request schema

```json
{
  "room": "my-room",
  "items": [
    {
      "identity": "viewer-1",
      "ops": [
        { "op": "permission", "permission": { "canSubscribe": true } },
        { "op": "subscribe", "trackSids": ["TR_abc123", "TR_def456"] }
      ]
    },
    {
      "identity": "viewer-2",
      "ops": [
        { "op": "unsubscribe", "trackSids": ["TR_abc123"] },
        { "op": "permission", "permission": { "canSubscribe": false } }
      ]
    }
  ]
}
```

- `room` (string, required): the room every item in this request applies to.
  One request touches exactly one room.
- `items` (array, required, non-empty): one entry per participant.
  - `identity` (string, required): the participant's identity in that room.
  - `ops` (array, required, non-empty): an **ordered** list of operations to
    run against that participant, in order. See "Op order is significant"
    below — this is not incidental.

Each op has an `op` field, one of three values:

| `op`           | Required fields                | Effect                                                          |
| -------------- | ------------------------------- | ---------------------------------------------------------------- |
| `permission`   | `permission` (object)           | Calls `UpdateParticipant` with that permission                   |
| `subscribe`    | `trackSids` (array of strings)  | Calls `UpdateSubscriptions` with `subscribe: true`                |
| `unsubscribe`  | `trackSids` (array of strings)  | Calls `UpdateSubscriptions` with `subscribe: false`               |

Any other value in `op` is rejected as a `400` for the whole request (see
"Response codes").

The `permission` object is decoded with `protojson` directly into LiveKit's
`ParticipantPermission` proto message, so:

- It accepts both `camelCase` and `snake_case` field names
  (`canSubscribe`/`can_subscribe`, `canPublishData`/`can_publish_data`, ...) —
  protojson supports both, and this is deliberate: when a field is added to
  the proto upstream, there is nothing to update here.
- The available fields are exactly whatever `livekit.ParticipantPermission`
  defines in the `livekit-protocol` package (`livekit_models.proto` /
  `livekit_models.pb.go`, message `ParticipantPermission`) at the version this
  server was built against — `canSubscribe`, `canPublish`, `canPublishData`,
  `canPublishSources`, `hidden`, `canUpdateMetadata`, `canSubscribeMetrics`,
  and a couple of deprecated fields. Do not treat this list as fixed; check
  the proto for the version you are targeting.
- Only the permission itself is updated. Name, metadata and attributes are
  never touched by a `permission` op, regardless of what the participant's
  current values are.
- Sending `{"permission": {}}` (all fields at their zero value) is valid and
  means "set every permission field to false/empty" — protojson cannot tell
  the difference between "field omitted" and "field explicitly false".

## Op order is significant

**Ops inside one item run strictly in order, and this is a correctness
requirement, not a convenience.** With `canSubscribe: false`, a `subscribe`
call does not attach the track. So:

- **Re-granting access** to a viewer must set the permission *before*
  subscribing them:
  ```json
  { "op": "permission", "permission": { "canSubscribe": true } },
  { "op": "subscribe", "trackSids": ["TR_abc123"] }
  ```
  Sending `subscribe` first would attempt to subscribe while the viewer still
  cannot subscribe, and the call would not attach anything.

- **Denying access** to a viewer must unsubscribe them *before* the
  permission is pinned to `canSubscribe: false`:
  ```json
  { "op": "unsubscribe", "trackSids": ["TR_abc123"] },
  { "op": "permission", "permission": { "canSubscribe": false } }
  ```
  The reverse order is not necessarily wrong (an explicit `unsubscribe` still
  works once `canSubscribe` is already false), but it is not the pattern this
  API was built around, and it wastes the guarantee below: on the server,
  items run through a worker pool, but the ops *inside* one item never run
  concurrently with each other — they are dispatched to a single worker and
  executed one at a time, in the order you sent them. Different items run in
  parallel with each other; a single item's own chain never does.

## Failure semantics

`applied` and `failed` in the response count **items, not ops**. Every item
in the request ends up in exactly one bucket — `applied` or `failed` — and
`applied + failed` always equals the number of items in the request. An item
is never silently dropped from the count.

For each item, ops run in order and the chain **aborts at the first op that
fails**. `opIndex` in the corresponding failure entry is the real,
zero-based index of the op that failed inside that item's `ops` array — not
always `0`, and not always the last index. Ops after the failing one are
never attempted.

**`opIndex: -1` means something different: the item was never attempted at
all.** This happens when the request ends (client disconnect, or the
server-side `bulk.timeout` below) before that item's turn came up in the
worker pool — it never got to run any op, successful or not. Do not treat
`-1` as "failed on the first op" (`opIndex: 0` means that); it means "this
participant was not touched, and you should assume their state is whatever
it was before this request." Because ops run through a bounded worker pool,
dispatch is sequential by item index, so on a cut-short request the
never-attempted items tend to be a contiguous run at the tail of `items`, but
do not rely on that ordering — read `opIndex` on each failure.

A failure in one item has no effect on any other item in the same request —
each item's outcome is independent. In particular, a participant who has left
the room (or never joined) produces a `failed` item, not a `400` for the
whole request: an unknown-participant error is business data about that one
viewer, not a malformed request.

## Truncation

`failures` in the response is capped at 100 entries; beyond that, `truncated`
is set to `true`. **`failed` is always the true, un-truncated count.** Do not
derive the number of failures from `len(failures)` — read `failed` for the
count and `failures` for (up to) the first 100 details.

## Limits

| Limit                    | Default | Configurable via                | Why                                                                                     |
| ------------------------- | ------- | -------------------------------- | ---------------------------------------------------------------------------------------- |
| Items per request          | 5000    | `bulk.max_items`                 | Bounds how many participants one request can carry; `0` disables the cap.                |
| Ops per item                | 16      | fixed (`maxBulkOpsPerItem`)       | Real traffic sends 2-3 ops per participant. This is headroom, not a tuning knob.          |
| TrackSids per op            | 64      | fixed (`maxBulkTrackSidsPerOp`)   | A room realistically has a handful of published tracks.                                  |
| Total ops per request       | 20000   | fixed (`maxBulkOpsPerRequest`)    | `max_items` alone does not bound total work — see below.                                 |
| Request deadline (server-side) | 30s | `bulk.timeout`                   | Bounds the whole request from the server's side; see "Timeouts and cancellation" below.  |

The three fixed limits live in `pkg/service/bulktypes.go`; `bulk.max_items`,
`bulk.workers` and `bulk.timeout` live in `pkg/config/config.go`.
`bulk.max_items` defaults to `5000`, `bulk.workers` defaults to `0` (meaning
`GOMAXPROCS`), and `bulk.timeout` defaults to `30s`. Setting `bulk.timeout` to
`0` (or a negative value) disables the server-side deadline entirely.

`max_items` bounds the number of *items*, but not the number of *ops*. Without
the per-item and per-request op caps, a single item could carry roughly
270,000 ops and still fit under the default 10 MiB request body limit — which
would tie up a worker running hundreds of thousands of sequential RPCs for
one item. The per-item, per-op and per-request caps exist specifically to
prevent that.

**If you raise `bulk.max_items`, you likely also need to raise
`limit.max_api_request_body_size`** (default 10 MiB, i.e. `10485760` bytes).
At roughly 400 bytes per item, 5000 items is about 2 MB, so there is headroom
at the defaults — but a request that is valid under a raised `max_items` can
still be rejected by the body size limiter first, with a much less specific
`413` instead of a bulk-specific `400`. The body limiter runs as global
middleware ahead of every route, `/bulk/v1/participants` included
(`pkg/service/server.go`, `NewRequestBodyLimiter`).

## Response codes

| Code | Meaning |
| ---- | ------- |
| `200` | The request was well-formed and authorized. The body reports per-item results — `200` does **not** mean every item succeeded; check `applied`/`failed`. |
| `400` | The request itself is malformed. Causes: unparseable JSON body; empty `room`; empty `items`; an item with empty `identity` or empty `ops`; an op with an unrecognized `op` value; a `permission` op missing its `permission` object or with a `permission` object that fails to decode; a `subscribe`/`unsubscribe` op with empty `trackSids`; more items than `bulk.max_items`; more ops in one item than allowed; more `trackSids` in one op than allowed; more total ops in the request than allowed. On a `400`, **nothing in the request is applied** — validation runs for the whole payload before any op is sent to the room. |
| `403` | No token, an invalid/unparseable token, a token without `roomAdmin`, or a token scoped to a different room than the request's `room`. Nothing is applied. |
| `413` | The request body exceeds `limit.max_api_request_body_size` (applies to every route, not specific to this endpoint). |

## Timeouts and cancellation

This section exists because of what we measured while wiring this endpoint
into the server: an op sent against a room with no active RTC node blocks for
the underlying RPC client's default timeout of **3 seconds**
(`psrpc.DefaultClientTimeout`, `github.com/livekit/psrpc` v0.7.3,
`client.go:25`), and this is exactly what we saw in a real run — one item,
one op, against a room nobody had joined:

```
INFO	livekit	service/bulkservice.go:97	bulk participants applied	{"room": "test-room", "items": 1, "applied": 0, "failed": 1, "duration": "3.005771667s", "deadlineExceeded": false}
```

`3.005771667s` for a single item's single op. `deadlineExceeded` is `false`
here because this measurement predates `bulk.timeout` even existing as a
concept — it ran under the default 30s budget, well clear of the 3s it
actually took.

**This ~3s ceiling does not apply uniformly to every failure mode.** It is
the RPC round-trip cost when the request actually has to reach a node over
the message bus and nothing answers — which is always the case for
`subscribe`/`unsubscribe` ops (`RoomService.UpdateSubscriptions`,
`pkg/service/roomservice.go:295-309`, has no local check before calling the
RPC client). A `permission` op, however, first checks participant existence
against the local room store when that store implements `OSSServiceStore`
(`RoomService.UpdateParticipant`, `pkg/service/roomservice.go:281-288`) — both
of the store implementations this server ships, `LocalStore` and
`RedisStore`, implement it. So a `permission` op against a participant that
genuinely never joined the room fails immediately with `ErrParticipantNotFound`
and does **not** pay the ~3s cost; only a `permission` op that reaches the RPC
layer (participant present in the store, but its serving node is unreachable)
does. Do not assume every failed `permission` op is fast — treat ~3s as the
worst case for any op, and note the ordering constraint above already means a
`subscribe`/`unsubscribe` op is common in nearly every chain.

**Consequences for a client:**

- **The server bounds the whole request with `bulk.timeout`, default `30s`.**
  There is no `http.Server` read/write deadline wrapping
  `/bulk/v1/participants` (that would also cut off the WebSocket signaling
  connections served by the same mux, so it is deliberately not there
  either), but the handler itself wraps the request's context with
  `context.WithTimeout(ctx, bulk.timeout)` before dispatching any item.
  Setting `bulk.timeout` to `0` (or negative) disables this and makes the
  server-side worst case unbounded again — the client's own HTTP timeout
  becomes the only ceiling.
- Items are processed by a bounded worker pool (`bulk.workers`, default
  `GOMAXPROCS`), one item's whole op chain per worker slot at a time. So the
  **worst case for one request, before `bulk.timeout` cuts in, is roughly
  `(items / workers) × (ops per item) × 3s`** if every op hits a
  dead/unreachable participant or room. For a simple case of one slow op per
  item: 2,500 items with 10 workers is `2500 / 10 = 250` batches × 3s ≈ **750
  seconds, about 12.5 minutes** — which is exactly why a server-side deadline
  exists: without it, that whole 12.5 minutes is a held handler goroutine. In
  the common case where every op succeeds quickly, the same request returns
  in well under a second.
- **The client should still set its own HTTP timeout** — `bulk.timeout` is
  the server's ceiling, not a substitute for one on the client. A reasonable
  client timeout is `bulk.timeout` plus some margin for network latency and
  response serialization.
- When either the client disconnects or `bulk.timeout` expires, the
  request's `context.Context` is canceled. The server stops **dispatching
  new items** to the worker pool as soon as it observes cancellation, but
  items already handed to a worker are allowed to finish — there is no
  forceful abort of an in-progress RPC (and a real RPC client that itself
  respects context cancellation, like this server's, often returns almost
  immediately once its context is done, rather than running to its own
  timeout). Whatever was applied before cancellation stays applied — **there
  is no rollback**.
- **Every item is still accounted for when this happens — this is not
  best-effort.** An item whose op chain ran and failed reports `failed` with
  the real `opIndex` of the op that failed. An item that was **never
  dispatched** because the request ended first is *also* reported as
  `failed`, with `opIndex: -1` (see "Failure semantics" above) — it is never
  left out of the count and never reported as `applied`. `applied + failed`
  always equals the number of items you sent, with or without a timeout.
- This is safe to retry because **the endpoint is idempotent**. On the
  server, `ParticipantImpl.SetPermission` has a fast path that is a no-op
  when the requested permission already matches the participant's current
  one (`pkg/rtc/participant.go:851`, `MatchesPermission`), and unsubscribing
  from a track that is not currently subscribed is a no-op as well. Sending
  the exact same request again after a timeout does not double-apply
  anything.
- **Recommendation:** size your HTTP client timeout to `bulk.timeout` plus
  margin, and on timeout, do not retry immediately in a loop — let the next
  scheduled sweep pick it up. A control-plane sweep that runs every ~10
  seconds and simply resends the current desired state each time gets this
  retry behavior for free, because the endpoint is idempotent.

**Live example**, from a real run against `bulk.timeout: 1ms` (set that low
specifically to force this path) with 50 items targeting a room with no
active node — every op sent this way would otherwise wait out the ~3s RPC
timeout described above:

```
INFO	livekit	service/bulkservice.go:97	bulk participants applied	{"room": "test-room", "items": 50, "applied": 0, "failed": 50, "duration": "1.769709ms", "deadlineExceeded": true}
```

The response completed in `1.77ms` instead of the ~150s a sequential 3s-per-item
worst case would otherwise imply for 50 items. Of the 50 failures, 30 had
`opIndex: 0` (dispatched, then failed fast because the real RPC client itself
observed the canceled context — `"error": "request canceled"`) and 20 had
`opIndex: -1`, e.g.:

```json
{"identity": "v_30", "opIndex": -1, "error": "not attempted: request deadline exceeded or client disconnected"}
```

`applied` was `0` and `failed` was `50` — every one of the 50 items sent was
accounted for.

## Example

```bash
# devkey/secret are the placeholder keys generated by `--dev` mode only.
# Never use them against a real deployment.
curl -sS -X POST http://127.0.0.1:7880/bulk/v1/participants \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "room": "test-room",
    "items": [
      {
        "identity": "viewer-1",
        "ops": [
          { "op": "permission", "permission": { "canSubscribe": true } },
          { "op": "subscribe", "trackSids": ["TR_abc123"] }
        ]
      }
    ]
  }'
```

`$TOKEN` is a JWT signed with your project's API key/secret, carrying a video
grant with `roomAdmin: true` and `room` set to `"test-room"`. How you mint
that token is up to your server-side tooling — any LiveKit server SDK that
can build an access token with a video grant works.

## Observability

Every request logs one structured line on completion, regardless of outcome:

```
INFO	livekit	service/bulkservice.go:97	bulk participants applied	{"room": "test-room", "items": 1, "applied": 0, "failed": 1, "duration": "3.005771667s", "deadlineExceeded": false}
```

Fields: `room`, `items` (total items in the request), `applied`, `failed`,
`duration` (wall time for the whole `apply` phase, after auth and
validation), `deadlineExceeded` (`true` when `bulk.timeout` — not a client
disconnect — is what stopped dispatch; see "Timeouts and cancellation"
above for a live example with `deadlineExceeded: true`).

There are no new metrics for this endpoint. Per-item outcomes are recorded
into the existing `service_operation` Prometheus counter (registered as
`Namespace: "livekit", Subsystem: "node", Name: "service_operation"` —
`pkg/telemetry/prometheus/node.go`) — the same counter other server
operations use — with label `type="bulk_participant_item"` and `status` of
`success` or `error` (plus `error_type` on failures). This also means bulk
outcomes inherit that metric's existing `node_id`/`node_type` labels for
free. If Prometheus scraping is enabled (`prometheus.port` in config), query
it by that label. Depending on your scrape format negotiation, the exposed
metric name is `livekit_node_service_operation` or
`livekit_node_service_operation_total` — check what your Prometheus shows for
the existing (non-bulk) uses of this counter and match that:

```promql
sum(rate(livekit_node_service_operation[_total]{type="bulk_participant_item"}[5m])) by (status)
```
