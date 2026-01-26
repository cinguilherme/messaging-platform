# GraphQL (Lacinia)

This module provides a dedicated GraphQL server with optional GraphiQL and
WebSocket subscriptions using the `graphql-transport-ws` protocol.

## Integrant wiring (example)

```edn
{:system
 {:d-core.module/graphql
  {:port 4500
   :schema #ig/ref :my-app.graphql/schema
   :graphql-path "/graphql"
   :graphiql? true
   :graphiql-path "/graphiql"
   :subscriptions? true
   :ws-protocol "graphql-transport-ws"
   :context-fn #ig/ref :my-app.graphql/context}}}
```

## Schema map (example)

```clj
(ns my-app.graphql.schema
  (:require
   [integrant.core :as ig]))

(defmethod ig/init-key :my-app.graphql/schema
  [_ _]
  {:queries
   {:ping
    {:type 'String
     :resolve (fn [_ _ _] "pong")}}})
```

## Context function (example)

```clj
(defmethod ig/init-key :my-app.graphql/context
  [_ _]
  (fn [{:keys [headers]}]
    {:auth-token (get headers "authorization")}))
```

## Notes

- GraphiQL is served from the public `unpkg.com` CDN; disable it in production
  or place it behind auth if needed.
- Subscriptions use `graphql-transport-ws`. The same `/graphql` path is used for
  HTTP and WS.
