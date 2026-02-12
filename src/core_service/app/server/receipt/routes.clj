(ns core-service.app.server.receipt.routes
  (:require [core-service.app.schemas.messaging :as msg-schema]
            [core-service.app.server.openapi :as api-docs]
            [core-service.app.server.receipt.authed :as authed]))

(defn routes
  [{:keys [webdeps]}]
  ["/:id/receipts"
   {:openapi {:id api-docs/docs-id}
    :post {:tags ["receipts"]
           :summary "Create message receipt"
           :parameters {:path api-docs/PathConversationIdSchema
                        :body msg-schema/ReceiptCreateSchema}
           :openapi {:security [api-docs/api-key-and-bearer-security]}
           :responses {200 {:body api-docs/ReceiptCreateResponseSchema}
                       400 {:body api-docs/ErrorEnvelopeSchema}}
           :handler (authed/receipts-create {:webdeps webdeps})}}])
