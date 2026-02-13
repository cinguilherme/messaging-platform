(ns core-service.app.server.attachment.routes
  (:require [core-service.app.server.attachment.authed :as attachment-authed]
            [core-service.app.server.openapi :as api-docs]))

(defn routes
  [{:keys [webdeps]}]
  ["/:id/attachments"
   {:openapi {:id api-docs/docs-id}}
   ["" {:post {:tags ["attachments"]
               :summary "Upload attachment"
               :description (str "Uploads an attachment for a conversation.\n\n"
                                 "Supported payloads:\n"
                                 "- multipart/form-data with `file` or `image` field\n"
                                 "- raw binary body with Content-Type `image/*`, `audio/*`, or `application/octet-stream`\n\n"
                                 "Image processing runs asynchronously. The API stores the original upload and generates:\n"
                                 "- a low-res variant with a deterministic key suffix `-alt.jpg`\n"
                                 "- an optimized standard variant for large images\n\n"
                                 "Download uploaded content via GET /v1/conversations/{id}/attachments/{attachment_id}.")
               :parameters {:path api-docs/PathConversationIdSchema
                            :query api-docs/AttachmentCreateQuerySchema}
               :openapi {:security [api-docs/api-key-and-bearer-security]
                         :requestBody api-docs/AttachmentCreateRequestBody}
               :responses {200 {:body api-docs/AttachmentCreateResponseSchema}
                           400 {:body api-docs/ErrorEnvelopeSchema}}
               :handler (attachment-authed/attachments-create {:webdeps webdeps})}}]
   ["/:attachment_id"
    {:openapi {:id api-docs/docs-id}
     :get {:tags ["attachments"]
           :summary "Get attachment bytes"
           :description (str "Returns attachment binary payload for an attachment in the conversation.\n\n"
                             "Image variants:\n"
                             "- `version=original` (default): original attachment bytes\n"
                             "- `version=alt`: generated low-resolution JPEG placeholder (`-alt.jpg` convention)\n\n"
                             "Frontend recommendation: request `version=alt` first for fast placeholder rendering, then request `version=original`.")
           :parameters {:path api-docs/PathConversationAttachmentIdSchema
                        :query api-docs/AttachmentGetQuerySchema}
           :openapi {:security [api-docs/api-key-and-bearer-security]}
           :responses {200 {:description "Attachment binary payload"
                            :content api-docs/AttachmentGetResponseContent}
                       400 {:body api-docs/ErrorEnvelopeSchema}
                       404 {:body api-docs/ErrorEnvelopeSchema}}
           :handler (attachment-authed/attachments-get {:webdeps webdeps})}}]])
