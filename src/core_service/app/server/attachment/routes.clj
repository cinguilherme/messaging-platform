(ns core-service.app.server.attachment.routes
  (:require [core-service.app.server.attachment.authed :as attachment-authed]
            [core-service.app.server.openapi :as api-docs]))

(defn routes
  [{:keys [webdeps]}]
  ["/:id/attachments"
   {:openapi {:id api-docs/docs-id}
    :post {:tags ["attachments"]
           :summary "Upload attachment"
           :description "Uploads an attachment for a conversation. Supports multipart upload (`file`/`image`) and raw-body upload."
           :parameters {:path api-docs/PathConversationIdSchema
                        :query api-docs/AttachmentCreateQuerySchema}
           :openapi {:security [api-docs/api-key-and-bearer-security]}
           :responses {200 {:body api-docs/AttachmentCreateResponseSchema}
                       400 {:body api-docs/ErrorEnvelopeSchema}}
           :handler (attachment-authed/attachments-create {:webdeps webdeps})}}])
