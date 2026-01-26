# Authentication and Authorization

D-core provides a small, protocol-driven surface for authentication (Authn) and
authorization (Authz). The default implementation targets JWT access tokens
issued by OIDC/OAuth2 providers like Keycloak.

## Concepts

- Authentication verifies a token and returns a normalized principal map.
- Authorization checks tenant and scopes against that principal.
- Middleware wires these into Ring handlers.

## Protocols

- `d-core.core.authn.protocol/Authenticator`
- `d-core.core.authz.protocol/Authorizer`

## Default implementations

### JWT Authenticator

Namespace: `d-core.core.authn.jwt`

Integrant key: `:d-core.core.authn.jwt/authenticator`

Required config:

- `:jwks-uri` or `:jwks` (static JWKS map)

Common config:

- `:issuer` (OIDC issuer URL)
- `:aud` (audience string)
- `:jwks-cache-ttl-ms` (default 300000)
- `:clock-skew-ms` (default 60000)
- `:tenant-claim` (default "tenant_id")
- `:scope-claim` (default "scope")
- `:http-opts` (passed to `clj-http`)

Principal shape:

- `:subject`
- `:tenant-id`
- `:scopes` (set of strings)
- `:client-id`
- `:aud`
- `:issuer`
- `:expires-at`
- `:actor`
- `:claims` (raw claims map)

### Scope Authorizer

Namespace: `d-core.core.authz.scope`

Integrant key: `:d-core.core.authz.scope/authorizer`

Request context:

- `:tenant` (string/keyword)
- `:scopes` (string, coll, or set)

## HTTP middleware

Namespace: `d-core.core.auth.http`

- `wrap-authentication`: verifies a bearer token and attaches
  `:auth/principal` and `:auth/token` to the request.
- `wrap-authorization`: enforces `:auth/require` from the request or a
  `require-fn` option.

Auth requirement shape:

```clojure
{:tenant "tenant-1"
 :scopes #{"messages:read" "messages:write"}}
```

Integrant keys:

- `:d-core.core.auth.http/authentication-middleware`
- `:d-core.core.auth.http/authorization-middleware`

## Token client (service-to-service)

Namespace: `d-core.core.auth.token-client`

Integrant key: `:d-core.core.auth/token-client`

Supports:

- `client-credentials`
- `token-exchange` (RFC 8693)

Required config:

- `:token-url`

Common config:

- `:client-id`
- `:client-secret`
- `:http-opts` (passed to `clj-http`)

## Minimal config example

```clojure
{:d-core.core.authn.jwt/authenticator
 {:issuer "https://auth.example.com/realms/dev"
  :aud "d-core-api"
  :jwks-uri "https://auth.example.com/realms/dev/protocol/openid-connect/certs"
  :tenant-claim "tenant_id"
  :scope-claim "scope"}

 :d-core.core.authz.scope/authorizer {}

 :d-core.core.auth.http/authentication-middleware
 {:authenticator #ig/ref :d-core.core.authn.jwt/authenticator}

 :d-core.core.auth.http/authorization-middleware
 {:authorizer #ig/ref :d-core.core.authz.scope/authorizer}}
```
