(ns core-service.app.schemas.messaging
  (:require [malli.core :as m]))

;; Phase 0 contracts for the messaging PaaS (schema-only; validation wiring later).

(def ImageMetadataSchema
  [:map
   [:width [:int {:min 1}]]
   [:height [:int {:min 1}]]])

(def VoiceMetadataSchema
  [:map
   [:duration_ms [:int {:min 0}]]])

(def AttachmentSchema
  [:map
   [:attachment_id :uuid]
   [:object_key :string]
   [:mime_type :string]
   [:size_bytes [:int {:min 0}]]
   [:checksum :string]
   [:image {:optional true} ImageMetadataSchema]
   [:voice {:optional true} VoiceMetadataSchema]
   [:meta {:optional true} :map]])

(def ReceiptSchema
  [:map
   [:receipt_type [:enum :delivered :read]]
   [:message_id :uuid]
   [:user_id :uuid]
   [:at [:int {:min 0}]]])

(def ReceiptCreateSchema
  [:map
   [:receipt_type [:enum :delivered :read]]
   [:message_id :uuid]
   [:at {:optional true} [:int {:min 0}]]])

(def MessageEnvelopeSchema
  [:map
   [:message_id :uuid]
   [:conversation_id :uuid]
   [:seq [:int {:min 0}]]
   [:sender_id :uuid]
   [:sent_at [:int {:min 0}]]
  [:type [:enum :text :image :voice :file :system]]
   [:body [:map
           [:text {:optional true} [:string {:max 256}]]]]
   [:attachments {:optional true} [:vector AttachmentSchema]]
   [:client_ref {:optional true} :string]
   [:meta {:optional true} :map]])

(def MessageCreateSchema
  [:map
  [:type [:enum :text :image :voice :file :system]]
   [:body [:map
           [:text {:optional true} [:string {:max 256}]]]]
   [:attachments {:optional true} [:vector AttachmentSchema]]
   [:client_ref {:optional true} :string]
   [:meta {:optional true} :map]])

(def ConversationCreateSchema
  [:map
   [:type [:enum :direct :group]]
   [:member_ids [:vector :uuid]]
   [:title {:optional true} [:string {:max 120}]]])

(def PaginationQuerySchema
  [:map
   [:limit {:optional true} [:int {:min 1 :max 200}]]
   [:cursor {:optional true} :string]
   [:direction {:optional true} [:enum :backward :forward]]])

(def SegmentHeaderSchema
  [:map
   [:format_version [:int {:min 1}]]
   [:codec [:enum :json :edn :raw]]
   [:compression [:enum :none :gzip]]
   [:conversation_id :uuid]
   [:seq_start [:int {:min 0}]]
   [:seq_end [:int {:min 0}]]
   [:message_count [:int {:min 0}]]
   [:created_at [:int {:min 0}]]])

(def PaginationTokenSchema
  [:map
   [:conversation_id :uuid]
   [:direction [:enum :backward :forward]]
   [:source [:enum :redis :minio]]
   [:cursor [:or :string [:int {:min 0}]]]])

(defn valid?
  "Schema helper for quick REPL validation."
  [schema value]
  (m/validate schema value))
