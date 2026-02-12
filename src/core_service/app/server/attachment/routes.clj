(ns core-service.app.server.attachment.routes
  (:require [core-service.app.server.attachment.authed :as attachment-authed]
            [core-service.app.server.openapi :as api-docs]))

(defn routes
  [{:keys [webdeps]}]
  ["/:id/attachments"
   {:openapi {:id api-docs/docs-id}
    :post {:tags ["attachments"]
           :summary "Upload attachment"
           :description (str "Uploads an attachment for a conversation.\n\n"
                             "Supported payloads:\n"
                             "- multipart/form-data with `file` or `image` field\n"
                             "- raw binary body with Content-Type `image/*`, `audio/*`, or `application/octet-stream`\n\n"
                             "Attachment URL strategy: responses include `attachment.object_key` metadata only. "
                             "Clients must resolve URLs via a media gateway/CDN or signed URL service.")
           :parameters {:path api-docs/PathConversationIdSchema
                        :query api-docs/AttachmentCreateQuerySchema}
           :openapi {:security [api-docs/api-key-and-bearer-security]
                     :requestBody api-docs/AttachmentCreateRequestBody}
           :responses {200 {:body api-docs/AttachmentCreateResponseSchema}
                       400 {:body api-docs/ErrorEnvelopeSchema}}
           :handler (attachment-authed/attachments-create {:webdeps webdeps})}}])
