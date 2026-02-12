(ns core-service.app.server.openapi
  (:require [core-service.app.schemas.messaging :as msg-schema]))

(def docs-id ::core-http-routes)

(def openapi-info
  {:title "Core Service HTTP API"
   :version "v1"
   :description "HTTP surface for auth, users, conversations, messages, and receipts."})

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

(def MessagesListResponseSchema
  [:map
   [:ok [:= true]]
   [:conversation_id :string]
   [:messages [:vector msg-schema/MessageEnvelopeSchema]]
   [:next_cursor {:optional true} [:maybe :string]]])

(def AttachmentCreateQuerySchema
  [:map
   [:max-bytes {:optional true} [:int {:min 0}]]
   [:kind {:optional true} [:enum "image" "voice" "file"]]])

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
