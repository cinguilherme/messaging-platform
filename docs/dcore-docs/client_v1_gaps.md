# Client V1 Gaps and Options

This document tracks the current API gaps that block a minimal v1 of the
React SPA chat app, plus options to resolve each gap. It assumes the existing
plans and contracts are complete and should not be re-litigated.

## Assumptions (Fixed Inputs)

- Phase 0 contracts are accepted (message envelope, pagination, auth model).
- Execution plan phases are considered complete for scope definition.
- Realtime WebSocket stream is not wired yet; polling is the current path.

## Status Legend

- Addressed: Documented and implemented; no blocker for v1.
- Partially addressed: Implemented but still gaps or weak guarantees.
- Open: Missing or unclear; blocks or constrains v1.
- Decision: Agreed v1 behavior; not a backend change (unless revised).

## Gap 1: Conversation list participant display info

Status: Addressed.

Current: `GET /v1/conversations` returns `members` and `counterpart` with local
profile cache enrichment. Missing profiles fall back to Keycloak admin lookup
and are cached. If neither is available, only `user_id` is populated.

Options:
- Implemented: local `user_profiles` store + bulk lookup by ids.

## Gap 2: Conversation detail response schema is incomplete

Status: Addressed.

Current: `GET /v1/conversations/:id` returns the documented `item` shape,
including `members` and timestamps.

Options:
- Implemented: list item shape with `members`.

## Gap 3: Message list response shape alignment

Status: Partially addressed.

Current: The message envelope is documented, but the list response key name is
not standardized (`messages` vs `items`).

Options:
- A) Standardize on `items` to match other list endpoints.
- B) Keep `messages` and update docs to match.
- C) Add an `included` map of user profiles keyed by `user_id`.

## Gap 4: Handle-based user lookup is missing

Status: Addressed.

Current: `GET /v1/users/lookup` supports `email=` (Keycloak) and `username=` (local cache).

Options:
- Implemented: `?username=` (or handle with `@` prefix).

## Gap 5: User profile lookup by user_id is missing

Status: Addressed.

Current: `POST /v1/users/lookup` accepts `{ "ids": [...] }` and returns local
profile fields when available.

Options:
- Implemented: bulk lookup via `POST /v1/users/lookup`.

## Gap 6: Auth response + identity mapping are unclear

Status: Addressed.

Current: The canonical `user_id` is the JWT `sub` claim. `GET /v1/users/me`
returns the resolved user profile (local cache with Keycloak fallback).

Options:
- Implemented: JWT `sub` mapping + `GET /v1/users/me`.

## Gap 7: Realtime ~~not available in v1~~ (Resolved)

Status: Resolved.

The conversation stream WebSocket endpoint is now available at
`ws://localhost:3001/ws/conversations/:id/stream`. Clients can subscribe to
receive new messages in real time instead of polling. See
`docs/dcore-docs/client_integration.md` for the full contract.

## Gap 8: Attachments scope for v1 is ambiguous

Status: Decision (text-only for v1).

Current: message schema includes attachments, but public upload is not exposed.

Options:
- A) Declare v1 as text-only; reject non-text types at the API.
- B) Keep attachments in schema but mark them as "not yet supported" for clients.

## Gap 9: SPA hosting and CORS

Status: Open.

Current: CORS is not configured; browser clients need a proxy or same-origin.

Options:
- A) Require a reverse proxy in dev/prod; document the expected setup.
- B) Add a minimal, config-driven CORS allowlist for dev only.

## Gap 10: Conversation list pagination stability

Status: Open.

Current: `next_cursor` is `updated_at` of the last item; collisions can reorder
items with identical timestamps.

Options:
- A) Keep as-is and treat as best-effort ordering.
- B) Use a composite opaque cursor `(updated_at, conversation_id)` encoded
     as base64url JSON.

## Gap 11: Receipt read/delivered state is not readable

Status: Open.

Current: receipts are write-only; message items do not include read/delivered
state, so the UI can only show optimistic status.

Options:
- A) Add `GET /v1/conversations/:id/receipts` (cursor/pagination) for receipt events.
- B) Include receipt state in message list items (aggregate or per-user fields).
- C) Add conversation-level read markers (e.g., `last_read_seq` per user).
