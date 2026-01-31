# Client V1 Gaps and Options

This document tracks the current API gaps that block a minimal v1 of the
React SPA chat app, plus options to resolve each gap. It assumes the existing
plans and contracts are complete and should not be re-litigated.

## Assumptions (Fixed Inputs)

- Phase 0 contracts are accepted (message envelope, pagination, auth model).
- Execution plan phases are considered complete for scope definition.
- Realtime WebSocket stream is not wired yet; polling is the current path.

## Gap 1: Conversation list lacks participant display info

Current: `GET /v1/conversations` returns metadata, last_message, and unread_count
but no member profiles or counterpart data for direct chats.

Options:
- A) Embed `members` with minimal profile fields per item (id, username, name, avatar).
- B) For `direct`, return a `counterpart` object; for `group`, return `member_ids`
     plus a separate bulk profile lookup endpoint.
- C) Add `GET /v1/conversations/:id/members` and let the client fetch as needed.

## Gap 2: Conversation detail response schema is undefined

Current: `GET /v1/conversations/:id` exists, but the response shape is not
documented.

Options:
- A) Mirror the list item shape and add `members`.
- B) Return `conversation` + `members` as top-level keys for explicitness.
- C) Return `member_ids` only, and rely on a user lookup endpoint for profiles.

## Gap 3: Message list item schema is undefined

Current: pagination for `GET /v1/conversations/:id/messages` is documented, but
the item schema is not.

Options:
- A) Adopt the Phase 0 message envelope as the response item shape.
- B) Add an `included` map of user profiles keyed by `user_id` to avoid extra
     lookups for sender display info.
- C) Keep `sender_id` only and require the client to resolve profiles via a
     separate endpoint.

## Gap 4: Handle-based user lookup is missing

Current: only `GET /v1/users/lookup?email=` is supported.

Options:
- A) Keep email-only lookup for v1; UI uses email for new conversations.
- B) Add `?username=` (or `?handle=`) lookup parameter.
- C) Add a generic `?q=` lookup that matches email or username.

## Gap 5: User profile lookup by user_id is missing

Current: conversations/messages reference `user_id` but there is no endpoint
to resolve ids to profiles.

Options:
- A) Add `GET /v1/users/:id` for single lookups.
- B) Add bulk lookup: `POST /v1/users/lookup` with `{ "ids": [...] }`.
- C) Embed minimal profiles directly in conversation/message responses.

## Gap 6: Auth response + identity mapping are unclear

Current: `/v1/auth/login` and `/v1/auth/register` exist but response shapes and
the canonical `user_id` claim are not documented.

Options:
- A) Document auth responses (tokens, expiry) and declare the JWT `sub` claim
     as the canonical `user_id`.
- B) Add `GET /v1/users/me` to map the access token to the user profile.

## Gap 7: Realtime not available in v1

Current: WebSocket endpoint is echo-only in dev; no production realtime stream.

Options:
- A) Poll `GET /v1/conversations/:id/messages` with `direction=forward` for new
     messages; document recommended poll interval and backoff.
- B) Add a lightweight SSE endpoint as an interim alternative to WS.
- C) Wait for the planned WS stream endpoint; accept polling-only UX in v1.

## Gap 8: Attachments scope for v1 is ambiguous

Current: message schema includes attachments, but public upload is not exposed.

Options:
- A) Declare v1 as text-only; reject non-text types at the API.
- B) Keep attachments in schema but mark them as "not yet supported" for clients.

## Gap 9: SPA hosting and CORS

Current: CORS is not configured; browser clients need a proxy or same-origin.

Options:
- A) Require a reverse proxy in dev/prod; document the expected setup.
- B) Add a minimal, config-driven CORS allowlist for dev only.

## Gap 10: Conversation list pagination stability

Current: `next_cursor` is `updated_at` of the last item; collisions can reorder
items with identical timestamps.

Options:
- A) Keep as-is and treat as best-effort ordering.
- B) Use a composite opaque cursor `(updated_at, conversation_id)` encoded
     as base64url JSON.
