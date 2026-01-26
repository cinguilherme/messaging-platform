# ADR 01: Low-Cost Real-Time Messaging Storage for Messaging PaaS

Status: Proposed

## Context

We want to offer a real-time messaging API platform (PaaS-style) similar in
capability to WhatsApp/Telegram/Firebase chat, but optimized for very low
operational cost. The core service should be able to run on a small VPS
(~1 vCPU, 4GB RAM) using a minimal stack: Postgres, Redis, and Minio.

Storing individual messages as rows in Postgres tables is expected to be too
costly at scale (storage bloat, index growth, write amplification). We need a
design that avoids per-message relational storage while still supporting
real-time delivery, history retrieval, and retention policies.

Authentication and authorization must be simple to operate. We will use
Keycloak as the identity provider, and support both application-level API keys
and user-level authN/authZ.

## Decision

Adopt a tiered storage model that separates real-time delivery from long-term
history storage, using segment-based objects rather than per-message rows.

- Postgres stores tenants, users, conversations, memberships, access control,
  and a segment index (not per-message rows). Each segment index record includes
  `conversation_id`, `seq_start`, `seq_end`, `object_key`, `byte_size`, and
  `created_at`.
- Messages receive monotonically increasing sequence numbers per conversation.
  The writer buffers messages into an "open segment" and flushes to Minio when
  size or time thresholds are reached (e.g., 1-5 MB or 1-5 seconds). Each segment
  is immutable and encoded as length-prefixed JSON (or similar) and compressed.
- Redis provides real-time fanout and a hot cache. Use Redis Streams for the
  recent tail (configurable retention window, e.g., last N messages or last 24h)
  and Redis Pub/Sub for immediate websocket delivery. Configure Redis persistence
  (AOF) to reduce loss for messages buffered before segment flush.
- Read path: recent messages and reconnect catch-up come from Redis Streams;
  older history is fetched from Minio by looking up segment ranges in Postgres
  and scanning the segment objects for the requested range.
- Attachments/media are stored in Minio as separate objects and referenced from
  the message payload. Retention/TTL deletes old segment objects and index rows.

### Tunable Parameters (Initial Defaults)

All thresholds are configuration-driven to allow tuning per deployment.

- Segment flush window: size-based (1-5 MB) and time-based (1-5 seconds).
- Redis retention window: last N messages per conversation or a time window
  (e.g., 24 hours) for the real-time tail.
- History retention: hard TTL of 30 days for segment objects and index rows.
- Encoding: use D-Core codecs, default JSON for interoperability; EDN and
  raw bytes are supported as alternatives for specialized clients.

## Implementation Outline (Phases)

Phase 1: Core path
- Writer: assign per-conversation sequence numbers, buffer into an open segment,
  flush to Minio on size/time thresholds, and write segment index rows.
- Reader: serve recent history from Redis Streams; fall back to Minio by
  resolving segment ranges from Postgres.
- Delivery: Redis Pub/Sub for immediate websocket fanout.

Phase 2: Retention and compaction
- Retention worker: delete expired segment objects and index rows (30-day TTL).
- Optional compaction: merge small/adjacent segments to reduce overhead.

Phase 3: Resilience and ops
- Backfill worker to repair missing segments when Redis persists longer than
  Minio flush cycles.
- Metrics/telemetry for flush latency, tail depth, and segment sizes.

Implementation will leverage the D-Core workers system (core.async-based) for
background tasks, using ticker workers for scheduled jobs (no OS-level cron).
For async messaging, prefer low-cost options (Redis queues/streams, RabbitMQ,
or in-memory queues) over higher-cost systems like Kafka. Storage is Minio
self-hosted, with daily backups of Minio objects and Postgres to Cloudflare R2.

## Detailed Plan

Phase 0: Design and contracts (1-2 weeks)
- Define message envelope schema (ids, timestamps, sequence, sender, payload,
  attachments, and optional receipt fields).
- Define auth model: application API keys for tenant apps, user access tokens
  via Keycloak, and authorization rules for conversations/memberships.
- Specify segment format (length-prefixed records + compression) and codec
  selection defaults (JSON; EDN/raw bytes optional).
- Define pagination tokens and range query semantics for history retrieval.
- Decide on max message size, max segment size, and flush windows per SLA.

Phase 1: Storage schema and primitives (1-2 weeks)
- Postgres tables: conversations, memberships, segment_index, and minimal
  metadata for retention/TTL.
- Redis structures: per-conversation stream naming, retention policy, and
  pub/sub channels.
- Minio bucket layout and naming scheme for segment objects and attachments.

Phase 2: Write path (2-4 weeks)
- API: message ingestion endpoint with auth, validation, and rate limits.
- Sequence assignment per conversation (atomic counter in Redis or Postgres).
- Buffering and flush worker: accumulate messages into an open segment, flush
  to Minio on size/time thresholds, and write segment_index rows.
- Real-time fanout via Redis Pub/Sub and append to Redis Streams.
- Receipt handling: ephemeral receipts stored in Redis only.

Phase 3: Read path (2-4 weeks)
- Recent history from Redis Streams with reconnect catch-up.
- Long history: resolve segments via Postgres index, load from Minio, scan for
  range, and return paginated results.
- Pagination tokens and consistent ordering across Redis/Minio boundaries.

Phase 4: Retention, compaction, and cleanup (1-2 weeks)
- Retention worker: delete expired segments and index rows (30-day TTL).
- Stream trimming and receipt expiry aligned to Redis retention.
- Optional compaction job for small/adjacent segments to reduce read overhead.

Phase 5: Reliability and ops (ongoing)
- Metrics: flush latency, stream depth, segment size distribution, Redis memory,
  Minio errors, and end-to-end delivery latency.
- Alerts: sustained flush backlog, Redis memory pressure, Minio error rate.
- Backup/restore plan for Postgres metadata and Minio objects.
- Load tests to validate 1 vCPU / 4GB RAM targets, and adjust thresholds.

Phase 6: Hardening and scale options (optional)
- Idempotency keys on ingest to prevent duplicates on retries.
- Alternative backends (Kafka/Redpanda, larger object storage) if throughput
  requirements exceed single-node limits.

## Consequences

- Postgres storage remains small and index-friendly because it stores only
  conversation and segment metadata, not per-message rows (e.g., 1k users
  sending 10 messages/min still fits a very small Postgres footprint).
- Minio provides cheap, durable storage for large volumes of message history.
- Real-time delivery stays lightweight; Redis is the hot path and Minio is the
  cold path.
- We do not support server-side full-text search; clients can build their own
  indices after retrieving message history if needed.
- Random access into older history is slower because it requires scanning
  segment objects, but a strict 30-day retention policy keeps history bounded.
- Message receipts are transient and only supported while the message is still
  present in Redis (minutes to hours). After Redis retention expires, receipts
  are no longer supported.
- Implementing segment writers/readers and background retention is manageable
  with existing worker system components.

## Comparison: Per-Message Postgres/Timescale

- Per-message rows increase write amplification, index size, and vacuum overhead
  as traffic grows; this pushes compute and storage costs quickly.
- Segment objects shift the bulk of storage to Minio (cheap object storage) and
  keep Postgres small and stable.
- Timescale improves retention and time-window queries but only matters if we
  store per-message rows; for per-conversation or per-segment metadata, row
  counts stay relatively flat for a stable userbase, so the growth cost is
  largely negligible.
- The chosen model trades query flexibility for predictably low cost at scale.

## References

- `docs/adr/00-d-core.md`
