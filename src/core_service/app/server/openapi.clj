(ns core-service.app.server.openapi
  (:require [core-service.app.schemas.messaging :as msg-schema]))

(def docs-id ::core-http-routes)

(def attachment-url-strategy-section
  (str "### Attachment URL Strategy\n"
       "- Upload and message APIs return attachment metadata with `attachment_id` and `object_key`.\n"
       "- Fetch media bytes with `GET /v1/conversations/{id}/attachments/{attachment_id}`.\n"
       "- Probe variant availability with `HEAD /v1/conversations/{id}/attachments/{attachment_id}`.\n"
       "- Image uploads generate a low-res variant using a deterministic key suffix: replace the original extension with `-alt.jpg`.\n"
       "  Example: `attachments/image/123.png` -> `attachments/image/123-alt.jpg`.\n"
       "- Voice uploads can generate transcoded variants using deterministic suffixes:\n"
       "  - `version=aac` -> `-aac.m4a`\n"
       "  - `version=mp3` -> `-mp3.mp3`\n"
       "- For image-first UX, clients can `HEAD ?version=alt` for readiness checks, then `GET ?version=alt` for placeholder rendering, and finally fetch the original when needed.\n"
       "- For voice playback, clients can probe `version=aac` first (iOS-friendly) and fallback to `version=mp3`.\n"
       "- `object_key` is internal storage metadata; clients should use API endpoints, not direct object paths."))

(def openapi-info
  {:title "Core Service HTTP API"
   :version "v1"
   :description (str "HTTP surface for auth, users, conversations, messages, and receipts.\n\n"
                     attachment-url-strategy-section)})

(def openapi-servers
  [{:url "/"}])

(def openapi-components
  {:securitySchemes
   {"ApiKeyAuth" {:type "apiKey"
                  :in "header"
                  :name "x-api-key"}
    "BearerAuth" {:type "http"
                  :scheme "bearer"
                  :bearerFormat "JWT"}}})

(def api-key-security
  {"ApiKeyAuth" []})

(def api-key-and-bearer-security
  {"ApiKeyAuth" []
   "BearerAuth" []})

(def ErrorEnvelopeSchema
  [:map
   [:ok [:= false]]
   [:error :string]
   [:status {:optional true} :int]
   [:details {:optional true} :any]])

(def AuthTokenSchema
  [:map
   [:access_token :string]
   [:refresh_token {:optional true} :string]
   [:token_type {:optional true} :string]
   [:expires_in {:optional true} :int]
   [:scope {:optional true} :string]])

(def AuthRegisterResponseSchema
  [:map
   [:ok [:= true]]
   [:action [:enum "register"]]
   [:location {:optional true} :string]])

(def AuthTokenResponseSchema
  [:map
   [:ok [:= true]]
   [:action [:enum "login" "refresh"]]
   [:token AuthTokenSchema]])

(def UserItemSchema
  [:map
   [:user_id :string]
   [:username {:optional true} :string]
   [:first_name {:optional true} :string]
   [:last_name {:optional true} :string]
   [:avatar_url {:optional true} :string]
   [:email {:optional true} :string]
   [:enabled {:optional true} :boolean]])

(def UsersItemsResponseSchema
  [:map
   [:ok [:= true]]
   [:items [:vector UserItemSchema]]])

(def UsersLookupByIdsRequestSchema
  [:map
   [:ids [:vector :uuid]]])

(def UsersLookupQuerySchema
  [:map
   [:email {:optional true} :string]
   [:username {:optional true} :string]])

(def UserMeResponseSchema
  [:map
   [:ok [:= true]]
   [:item UserItemSchema]])

(def ConversationCreateResponseSchema
  [:map
   [:ok [:= true]]
   [:conversation
    [:map
     [:id :uuid]
     [:tenant_id {:optional true} :string]
     [:type :string]
     [:title {:optional true} [:maybe :string]]
     [:created_at :any]]]
   [:memberships :int]])

(def ConversationDetailSchema
  [:map
   [:conversation_id :string]
   [:type :string]
   [:title {:optional true} [:maybe :string]]
   [:created_at {:optional true} :int]
   [:updated_at {:optional true} :int]
   [:members [:vector UserItemSchema]]])

(def ConversationListItemSchema
  [:map
   [:conversation_id :string]
   [:type :string]
   [:title {:optional true} [:maybe :string]]
   [:updated_at {:optional true} :int]
   [:members [:vector UserItemSchema]]
   [:last_message {:optional true} [:maybe msg-schema/MessageEnvelopeSchema]]
   [:unread_count :int]
   [:counterpart {:optional true} UserItemSchema]])

(def ConversationsListResponseSchema
  [:map
   [:ok [:= true]]
   [:items [:vector ConversationListItemSchema]]
   [:next_cursor {:optional true} [:maybe :string]]])

(def ConversationDetailResponseSchema
  [:map
   [:ok [:= true]]
   [:item ConversationDetailSchema]])

(def PathConversationIdSchema
  [:map
   [:id :uuid]])

(def PathConversationAttachmentIdSchema
  [:map
   [:id :uuid]
   [:attachment_id :uuid]])

(def ConversationListQuerySchema
  [:map
   [:limit {:optional true} [:int {:min 1 :max 200}]]
   [:cursor {:optional true} :string]])

(def MessagesListQuerySchema
  [:map
   [:limit {:optional true} [:int {:min 1 :max 200}]]
   [:cursor {:optional true} :string]
   [:direction {:optional true} [:enum :backward :forward]]])

(def MessageCreateResponseSchema
  [:map
   [:ok [:= true]]
   [:conversation_id :string]
   [:message msg-schema/MessageEnvelopeSchema]
   [:stream :string]
   [:entry_id :string]])

(def MessageCreateResultSchema
  [:or MessageCreateResponseSchema ErrorEnvelopeSchema])

(def MessagesListResponseSchema
  [:map
   [:ok [:= true]]
   [:conversation_id :string]
   [:messages [:vector msg-schema/MessageEnvelopeSchema]]
   [:next_cursor {:optional true} [:maybe :string]]])

(def MessagesListResultSchema
  [:or MessagesListResponseSchema ErrorEnvelopeSchema])

(def AttachmentCreateQuerySchema
  [:map
   [:max-bytes {:optional true} [:int {:min 0}]]
   [:filename {:optional true} [:string {:min 1 :max 255}]]
   [:kind {:optional true} [:enum "image" "voice" "file"]]])

(def AttachmentGetQuerySchema
  [:map
   [:version {:optional true} [:enum "original" "alt" "aac" "mp3"]]])

(def AttachmentCreateRequestBody
  {:required true
   :description
   (str "Upload attachment bytes using either multipart form-data or a raw binary body.\n\n"
        "- multipart/form-data: provide `file` or `image` field.\n"
        "- raw body: set Content-Type to `image/*`, `audio/*`, or `application/octet-stream`.\n"
        "- optional query params: `kind`, `max-bytes`, and `filename` (for raw-body uploads).")
   :content
   {"multipart/form-data"
    {:schema {:type "object"
              :properties {"file" {:type "string"
                                   :format "binary"
                                   :description "Generic attachment file field."}
                           "image" {:type "string"
                                    :format "binary"
                                    :description "Image attachment alias field."}}
              :anyOf [{:required ["file"]}
                      {:required ["image"]}]}}
    "image/*" {:schema {:type "string" :format "binary"}}
    "audio/*" {:schema {:type "string" :format "binary"}}
    "application/octet-stream" {:schema {:type "string" :format "binary"}}}})

(def AttachmentGetResponseContent
  {"image/*" {:schema [:string {:format "binary"}]}
   "audio/*" {:schema [:string {:format "binary"}]}
   "application/octet-stream" {:schema [:string {:format "binary"}]}})

(def AttachmentHeadResponseHeaders
  {"Content-Type" {:description "Attachment media type."
                   :schema {:type "string"}}
   "Content-Length" {:description "Attachment byte size."
                     :schema {:type "integer" :format "int64" :minimum 0}}
   "Cache-Control" {:description "Cache policy for authenticated attachment checks."
                    :schema {:type "string"}}
   "ETag" {:description "Opaque entity tag when available from storage."
           :schema {:type "string"}}
   "Last-Modified" {:description "Last modification timestamp in RFC 1123 format when available."
                    :schema {:type "string"}}})

(def AttachmentCreateResponseSchema
  [:map
   [:ok [:= true]]
   [:conversation_id :string]
   [:attachment msg-schema/AttachmentSchema]])

(def ReceiptCreateResponseSchema
  [:map
   [:ok [:= true]]
   [:conversation_id :string]
   [:message_id :string]
   [:receipt_type [:enum "delivered" "read"]]])
