# ADR 02: Message Receipts Alternatives

Status: Proposed

## Context

We need to expose message receipt states so API consumers can render single or
double ticks (sent/delivered/read) in chat UIs. The core service already stores
recent messages in Redis Streams (hot path) and long-term history in Minio
segments with a Postgres index. Per ADR 01, receipts are currently considered
transient and aligned with Redis retention.

We need to decide how clients retrieve and keep receipt state in sync.

## Options

### Option 1: Include receipt state in the messages list

Example fields on each message:

```json
{
  "message_id": "...",
  "receipt_status": "sent|delivered|read",
  "delivered_at": 1730000000000,
  "read_at": 1730000000000
}
```

Pros
- Simplest client: one request yields messages and ticks.
- Fits existing pagination/caching.
- Best cold-start experience for a conversation.

Cons / complexity
- Payload bloat: receipt fields repeat for every message.
- Updates are awkward: clients still need a refresh strategy to see changes
  (polling the latest page or refetching recent messages).
- For group chats, a single `receipt_status` is ambiguous; we must define
  aggregation (e.g., "read by all" vs "read by any") or return per-recipient
  state, which can be large.
- Historical messages stored in Minio do not include receipts unless we persist
  them, which conflicts with the current "receipts are transient" model.

Implementation notes
- If we keep receipts transient, only messages in the Redis tail can expose
  receipt fields. Older messages should return null/absent receipt data.
- Aggregate fields for groups can use counts (e.g., `delivered_count`,
  `read_count`, `member_count`) to avoid large per-user maps.

### Option 2: Add a receipts feed endpoint (polling)

Example endpoint:

```
GET /v1/conversations/:id/receipts?since=<cursor>
```

Pros
- Efficient updates: only changes are returned.
- Scales better for busy conversations and group chats.
- Clear separation between initial state (messages list) and incremental
  changes (receipts feed).

Cons / complexity
- Requires new endpoint, cursor semantics, and ordering guarantees.
- Client must merge receipt events into local message state.
- Needs storage/indexing for "receipts since cursor" (likely Redis Streams),
  and well-defined retention behavior.

Implementation notes
- Model receipts as events in a per-conversation stream with a monotonic cursor
  (Redis Stream ID or internal sequence).
- Clients must handle out-of-order events and only apply monotonic upgrades
  (sent -> delivered -> read).

### Option 3: Conversation events stream (SSE/WebSocket)

Generalize option 2 into a unified event stream that carries new messages,
receipts, typing indicators, etc.

Pros
- One channel for real-time updates; minimal polling.
- Clean separation of state vs. changes.

Cons / complexity
- Requires long‑lived connection management (auth, reconnect, heartbeats).
- Needs subscription/fanout logic (which clients receive which events).
- Horizontal scaling adds state (connection registry, pub/sub, backpressure).
- Operational overhead vs polling (LB/TLS/WebSocket tuning, monitoring).

Implementation notes
- Define a WS auth handshake (how access token is presented on connect, how
  failures are surfaced, and how mid‑connection expiry is handled).
- Specify reauth/renewal flow for long‑lived connections (refresh token
  exchange vs forced reconnect) and close codes for auth failures.
- Add a subscription model (e.g., `subscribe: conversation_id`) and enforce
  membership ACLs at subscribe time (reuse existing conversation membership).
- Standardize event envelopes (type, conversation_id, cursor, payload) so one
  stream carries messages, receipts, typing, etc.
- Provide resume semantics (client sends last cursor) to avoid gaps on reconnect;
  if unsupported, document a fallback to refetch recent state with possible
  misses for transient events.

## Decision (Proposed)

Adopt a hybrid approach when implementing receipts:
- Include receipt fields in the messages list for fast initial rendering.
- Add a receipts feed endpoint for incremental updates.

Long-term, converge toward a generalized conversation events stream if/when
real-time delivery is exposed via SSE/WebSocket.

## Consequences

- Clients get immediate ticks on load and efficient follow-up updates.
- We must define aggregation semantics for group chats ("all" vs "any") or
  return counts per message.
- Receipts remain transient unless we explicitly decide to persist them beyond
  Redis retention.
- Additional implementation complexity is localized to the receipts feed and
  event merge logic.

## References

- `docs/adr/01-low-cost-realtime-messaging-paas.md`
