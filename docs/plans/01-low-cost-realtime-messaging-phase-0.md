# Phase 0 Plan: Contracts and API Surface

Status: Accepted
Source ADR: docs/adr/01-low-cost-realtime-messaging-paas.md
Related Plan: docs/plans/01-low-cost-realtime-messaging-execution.md

## Objective
Define the concrete contracts required to implement the write/read paths and
storage segmentation without rework.

## Outputs (This Phase Must Produce)
- Message envelope schema (canonical fields and types).
- Segment object format (header + record framing).
- Pagination token format and query semantics.
- Auth model details (API key + Keycloak token requirements).
- Minimal API surface (endpoints and websocket contracts).

## Contract Drafts (Working Specs)

### 1) Message Envelope (Canonical)

Minimal JSON shape (EDN/raw bytes are allowed as encoded payloads; the canonical
shape below is expressed in JSON for interoperability):

```json
{
  "message_id": "uuid",
  "conversation_id": "uuid",
  "seq": 12345,
  "sender_id": "uuid",
  "sent_at": 1730000000000,
  "type": "text|image|file|system",
  "body": {"text": "hello"},
  "attachments": [
    {
      "attachment_id": "uuid",
      "object_key": "minio/path/to/object",
      "mime_type": "image/png",
      "size_bytes": 12345,
      "checksum": "sha256:..."
    }
  ],
  "client_ref": "optional-idempotency-key",
  "meta": {"any": "extra"}
}
```

Notes
- `seq` is monotonic per conversation.
- `client_ref` is optional and can be used for idempotency.
- `attachments` are stored in Minio; the message only references them.
- System events (e.g., join/leave) are represented via `type: system`.

### 2) Receipt Events (Ephemeral)
Receipts are transient and live only in Redis:

```json
{
  "receipt_type": "delivered|read",
  "message_id": "uuid",
  "user_id": "uuid",
  "at": 1730000000000
}
```

### 3) Segment Object Format

Segment object contains a header and a sequence of records. The header is a
single JSON record at the beginning of the object:

```json
{
  "format_version": 1,
  "codec": "json|edn|raw",
  "compression": "none|gzip",
  "conversation_id": "uuid",
  "seq_start": 12000,
  "seq_end": 12500,
  "message_count": 501,
  "created_at": 1730000000000
}
```

Record framing
- Each record is length-prefixed (`uint32` big-endian) followed by the encoded
  message bytes.
- The header is record 0; messages follow as records 1..N.

### 4) Pagination Tokens

Token is base64url-encoded JSON:

```json
{
  "conversation_id": "uuid",
  "cursor": "stream-id-or-seq",
  "direction": "backward|forward",
  "source": "redis|minio"
}
```

Semantics
- `direction=backward` returns older messages (seq < cursor for Minio, stream id for Redis).
- `direction=forward` returns newer messages (seq > cursor for Minio, stream id for Redis).
- The server returns a new token when more pages are available.

### 5) Auth Model (High Level)

- API key required for all HTTP and websocket connections:
  - Header: `X-Api-Key: <key>`
- User auth required for all message endpoints:
  - Header: `Authorization: Bearer <jwt>` (Keycloak access token)
- Authorization is based on conversation membership stored in Postgres.
- Optional open auth endpoints can be exposed for user creation/login, backed
  by Keycloak. These are the only endpoints that may not require an API key.

### 6) Minimal API Surface (Draft)

- `POST /v1/conversations` (create)
- `GET /v1/conversations/:id` (read metadata)
- `POST /v1/conversations/:id/messages` (send message)
- `GET /v1/conversations/:id/messages?limit=&cursor=&direction=` (history)
- `GET /v1/conversations/:id/stream` (websocket for realtime)
- Optional auth endpoints (proxy to Keycloak):
  - `POST /v1/auth/register`
  - `POST /v1/auth/login`

## Decisions (Confirmed)
- Compression default: `gzip`.
- Max message size: 256 bytes (roughly 256 chars when ASCII-only).
- Default segment size: 256 KB.
- Flush interval: 5 minutes.
- API key header: `X-Api-Key`.

## Definition of Done
- All contracts above are accepted or revised.
- Open questions resolved or tracked explicitly.
- This document is marked as Accepted and referenced from the execution plan.
