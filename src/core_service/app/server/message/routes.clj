(ns core-service.app.server.message.routes
  (:require [core-service.app.schemas.messaging :as msg-schema]
            [core-service.app.server.message.authed :as authed]
            [core-service.app.server.openapi :as api-docs]))

(defn routes
  [{:keys [webdeps]}]
  ["/:id/messages"
   {:openapi {:id api-docs/docs-id}
    :post {:tags ["messages"]
           :summary "Create message"
           :parameters {:path api-docs/PathConversationIdSchema
                        :body msg-schema/MessageCreateSchema}
           :openapi {:security [api-docs/api-key-and-bearer-security]}
           :responses {200 {:body api-docs/MessageCreateResponseSchema}
                       400 {:body api-docs/ErrorEnvelopeSchema}}
           :handler (authed/messages-create {:webdeps webdeps})}
    :get {:tags ["messages"]
          :summary "List messages"
          :description "Messages are paginated by opaque `next_cursor`. Use the returned cursor for the next page."
          :parameters {:path api-docs/PathConversationIdSchema
                       :query api-docs/MessagesListQuerySchema}
          :openapi {:security [api-docs/api-key-and-bearer-security]}
          :responses {200 {:body api-docs/MessagesListResponseSchema}
                      400 {:body api-docs/ErrorEnvelopeSchema}}
          :handler (authed/messages-list {:webdeps webdeps})}}])
