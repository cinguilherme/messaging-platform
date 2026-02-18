# Client Integration Guide (Web + Mobile)

This guide covers how web apps and mobile apps should integrate with the
core-service HTTP API, WebSocket endpoint, and optional GraphQL server.

## Quick Map (Local Defaults)

- HTTP API: `http://localhost:3000`
- WebSocket: `ws://localhost:3001/ws`
- GraphQL: `http://localhost:3002` (path depends on module config; see `docs/dcore-docs/graphql.md`)

These ports come from `duct.edn` defaults and can be changed per environment.

## Authentication Overview

There are two layers:

1) **API key** (required for almost all HTTP/WS routes)
- Header: `X-Api-Key: <key>`
- Config sources: `API_KEYS` env var or `.api_keys.edn` file (see below).
- Bypassed routes: `/v1/auth/register`, `/v1/auth/login`, `/v1/auth/refresh`, `/metrics`.

2) **User access token** (required for conversation/messaging endpoints)
- Header: `Authorization: Bearer <jwt>`
- Expected to be a Keycloak access token.
- `tenant_id` (or `tenant-id`) claim is required for `POST /v1/conversations`.
- The canonical `user_id` for the client is the JWT `sub` (subject) claim.

### API key file example

Create `.api_keys.edn` in the project root:

```clojure
{:keys ["dev-key-1" "dev-key-2"]}
```

Or set environment variable:

```bash
export API_KEYS="dev-key-1,dev-key-2"
```

## Request/Response Format

- Requests: JSON body (`Content-Type: application/json`).
- Responses: JSON by default; EDN supported via `Accept: application/edn`.
- Responses follow the shape `{ :ok true/false, ... }` and often include `:error`
  and `:details` on failure.

## Core Messaging Endpoints (v1)

All endpoints below require `X-Api-Key`. The messaging, user lookup, and
conversation endpoints also require `Authorization: Bearer <jwt>`.

- `POST /v1/auth/register` (optional, Keycloak proxy)
- `POST /v1/auth/login` (optional, Keycloak proxy)
- `POST /v1/auth/refresh` (optional, Keycloak proxy; body: `refresh_token`, optional `scope`)
- `GET /v1/users/lookup?email=`
- `POST /v1/users/lookup`
- `GET /v1/users/me`
- `GET /v1/conversations`
- `POST /v1/conversations`
- `GET /v1/conversations/:id`
- `POST /v1/conversations/:id/messages`
- `GET /v1/conversations/:id/messages?limit=&cursor=&direction=`
- `POST /v1/conversations/:id/receipts`

### Conversation create (request body)

```json
{
  "type": "direct|group",
  "member_ids": ["uuid-1", "uuid-2"],
  "title": "Optional group title"
}
```

### Conversation list (response body)

Query params:
- `limit` (default 50)
- `cursor` (use `next_cursor` from previous response)

```json
{
  "ok": true,
  "items": [
    {
      "conversation_id": "uuid",
      "type": "direct|group",
      "title": "string|null",
      "members": [
        {
          "user_id": "uuid",
          "username": "user",
          "first_name": "User",
          "last_name": "Example",
          "avatar_url": "https://..."
        }
      ],
      "counterpart": {
        "user_id": "uuid",
        "username": "user",
        "first_name": "User",
        "last_name": "Example",
        "avatar_url": "https://..."
      },
      "updated_at": 1730000000000,
      "last_message": {
        "message_id": "uuid",
        "conversation_id": "uuid",
        "seq": 123,
        "sender_id": "uuid",
        "sent_at": 1730000000000,
        "type": "text",
        "body": {"text": "hello"}
      },
      "unread_count": 3
    }
  ],
  "next_cursor": "opaque"
}
```

Notes:
- `members` is populated from the local `user_profiles` store (cached). If a
  profile is missing, the server may fetch it from Keycloak admin and upsert
  into the cache; otherwise only `user_id` is populated and other fields may be
  null/omitted.
- `counterpart` is optional and only set for `type=direct` to simplify sidebar
  labeling. When present it is a subset of `members`.
- `next_cursor` is currently the `updated_at` value (epoch millis) of the last
  item in the page. Treat it as opaque.
- `last_message` is sourced from the Redis stream tail; if none is found,
  Minio history is used as a fallback.
- `unread_count` reflects unread messages in the Redis tail for that
  conversation (based on `read` receipts).

### Conversation detail (response body)

```json
{
  "ok": true,
  "item": {
    "conversation_id": "uuid",
    "type": "direct|group",
    "title": "string|null",
    "members": [
      {
        "user_id": "uuid",
        "username": "user",
        "first_name": "User",
        "last_name": "Example",
        "avatar_url": "https://..."
      }
    ],
    "created_at": 1730000000000,
    "updated_at": 1730000000000
  }
}
```

### Message create (request body)

```json
{
  "type": "text|image|voice|file|system",
  "body": {"text": "hello"},
  "attachments": [
    {
      "attachment_id": "uuid",
      "object_key": "attachments/path",
      "mime_type": "image/png",
      "size_bytes": 123,
      "checksum": "sha256:..."
    }
  ],
  "client_ref": "optional-client-ref",
  "meta": {"any": "extra"}
}
```

**Idempotency is required** for `POST /messages` by default:
- Header: `Idempotency-Key: <unique-per-send>`
- If missing, the request fails unless `client_ref` is allowed and provided.

### Attachment bytes fetch

- `GET /v1/conversations/:id/attachments/:attachment_id?version=original|alt|aac|mp3`
- `HEAD /v1/conversations/:id/attachments/:attachment_id?version=original|alt|aac|mp3`

Version behavior:
- `original`: uploaded object bytes.
- `alt`: image-only low-resolution JPEG placeholder.
- `aac`: voice-only AAC/m4a variant (`audio/mp4`).
- `mp3`: voice-only MP3 variant (`audio/mpeg`).

Recommended client strategy:
- images: probe/fetch `alt`, then fetch `original`.
- voice: probe/fetch `aac`, fallback to `mp3`, then fallback to `original`.

### Messages list (pagination)

- `limit`: 1..200
- `cursor`: opaque token returned as `next_cursor`
- `direction`: `backward` (older) or `forward` (newer)

Use `next_cursor` from the response as the `cursor` query param to continue.

### Messages list (response body)

Items follow the message envelope schema (below).

```json
{
  "ok": true,
  "messages": [
    {
      "message_id": "uuid",
      "conversation_id": "uuid",
      "seq": 12345,
      "sender_id": "uuid",
      "sent_at": 1730000000000,
      "type": "text|image|voice|file|system",
      "body": {"text": "hello"},
      "attachments": [
        {
          "attachment_id": "uuid",
          "object_key": "attachments/path",
          "mime_type": "image/png",
          "size_bytes": 123,
          "checksum": "sha256:..."
        }
      ],
      "client_ref": "optional-client-ref",
      "meta": {"any": "extra"}
    }
  ],
  "next_cursor": "opaque"
}
```

### Message envelope (canonical schema)

```json
{
  "message_id": "uuid",
  "conversation_id": "uuid",
  "seq": 12345,
  "sender_id": "uuid",
  "sent_at": 1730000000000,
  "type": "text|image|voice|file|system",
  "body": {"text": "hello"},
  "attachments": [
    {
      "attachment_id": "uuid",
      "object_key": "attachments/path",
      "mime_type": "image/png",
      "size_bytes": 123,
      "checksum": "sha256:..."
    }
  ],
  "client_ref": "optional-client-ref",
  "meta": {"any": "extra"}
}
```

### User lookup (by email)

- `GET /v1/users/lookup?email=user@example.com`
- Response includes `user_id` plus basic profile fields when available.
  Email-only lookup via Keycloak admin.

```json
{
  "ok": true,
  "items": [
    {
      "user_id": "uuid",
      "email": "user@example.com",
      "username": "user",
      "first_name": "User",
      "last_name": "Example",
      "enabled": true
    }
  ]
}
```

### User lookup (by username/handle)

- `GET /v1/users/lookup?username=@handle` (or without the `@`)

```json
{
  "ok": true,
  "items": [
    {
      "user_id": "uuid",
      "username": "user",
      "first_name": "User",
      "last_name": "Example",
      "avatar_url": "https://...",
      "email": "user@example.com",
      "enabled": true
    }
  ]
}
```

### User lookup (by ids)

Request body:

```json
{
  "ids": ["uuid-1", "uuid-2"]
}
```

Response includes local profile fields when available (populated on login/register
or via Keycloak fallback on conversation list):

```json
{
  "ok": true,
  "items": [
    {
      "user_id": "uuid",
      "username": "user",
      "first_name": "User",
      "last_name": "Example",
      "avatar_url": "https://...",
      "email": "user@example.com",
      "enabled": true
    }
  ]
}
```

### User identity (me)

`GET /v1/users/me` resolves the current user based on the access token and
returns the profile (local cache with Keycloak fallback).

```json
{
  "ok": true,
  "item": {
    "user_id": "uuid",
    "username": "user",
    "first_name": "User",
    "last_name": "Example",
    "avatar_url": "https://...",
    "email": "user@example.com",
    "enabled": true
  }
}
```

### Receipt create (request body)

```json
{
  "receipt_type": "delivered|read",
  "message_id": "uuid",
  "at": 1730000000000
}
```

## WebSocket (Realtime)

### Conversation stream

Connect to receive new messages in real time for an open conversation:

```
ws://localhost:3001/ws/conversations/<conversation-id>/stream
```

**Authentication (same as HTTP endpoints):**

- `X-Api-Key` header (required).
- `Authorization: Bearer <jwt>` header (required).

Browser clients that cannot set WebSocket headers may pass `api_key` and `token`
as query parameters instead.

Auth is enforced on the HTTP upgrade request. If the API key or JWT is invalid
the server responds with `401`. If the user is not a member of the conversation
the server responds with `403`. On success the connection is upgraded to a
WebSocket.

**Server frames:**

Each frame is an EDN-encoded message object matching the message schema returned
by `GET /v1/conversations/:id/messages`. Example:

```edn
{:message_id #uuid "..." :conversation_id #uuid "..." :seq 42
 :sender_id #uuid "..." :sent_at 1706000000000
 :type :text :body {:text "hello"} :attachments [] :client_ref nil :meta nil}
```

**Client frames:**

None required for v1. The stream is server-push only.

**Reconnect / catch-up:**

On reconnect, call `GET /v1/conversations/:id/messages?cursor=<last-seen-cursor>`
once to retrieve any messages missed while disconnected, then resume listening
on the WebSocket.

### Echo endpoint (dev only)

A basic echo handler remains available at `ws://localhost:3001/ws` for
smoke-testing the WS server. It does not require authentication.

## GraphQL (Optional)

GraphQL is exposed on its own port. The module supports `graphql-transport-ws`
subscriptions when enabled. See `docs/dcore-docs/graphql.md` for config details.

## Web App Example (Fetch)

```js
const baseUrl = "http://localhost:3000";
const apiKey = "dev-key-1";
const token = "<jwt>";

async function createConversation() {
  const res = await fetch(`${baseUrl}/v1/conversations`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Api-Key": apiKey,
      "Authorization": `Bearer ${token}`
    },
    body: JSON.stringify({
      type: "direct",
      member_ids: ["11111111-1111-1111-1111-111111111111",
                   "22222222-2222-2222-2222-222222222222"]
    })
  });
  return res.json();
}

async function sendMessage(conversationId, text) {
  const res = await fetch(`${baseUrl}/v1/conversations/${conversationId}/messages`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Api-Key": apiKey,
      "Authorization": `Bearer ${token}`,
      "Idempotency-Key": crypto.randomUUID()
    },
    body: JSON.stringify({ type: "text", body: { text } })
  });
  return res.json();
}
```

## Mobile Example (Kotlin + OkHttp)

```kotlin
val baseUrl = "http://10.0.2.2:3000" // Android emulator -> localhost
val apiKey = "dev-key-1"
val token = "<jwt>"

val body = """
  {"type":"text","body":{"text":"hello"}}
""".trimIndent()

val request = Request.Builder()
  .url("$baseUrl/v1/conversations/$conversationId/messages")
  .post(body.toRequestBody("application/json".toMediaType()))
  .header("X-Api-Key", apiKey)
  .header("Authorization", "Bearer $token")
  .header("Idempotency-Key", UUID.randomUUID().toString())
  .build()
```

## Notes for Mobile & Web

- Store tokens securely (Keychain/Keystore/Secure Storage).
- If you’re running the web app in a browser, CORS is **not configured** by
  default in this service; add CORS at your reverse proxy or middleware.
