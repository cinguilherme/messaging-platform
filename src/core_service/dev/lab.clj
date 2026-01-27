(ns core-service.dev.lab
  (:require [core-service.app.schemas.auth :as auth-schema]
            [core-service.app.schemas.messaging :as msg-schema]
            [malli.core :as m]))

;; REPL helpers for validating Phase 0 schemas.
(comment
  (def sample-message
    {:message_id (java.util.UUID/randomUUID)
     :conversation_id (java.util.UUID/randomUUID)
     :seq 1
     :sender_id (java.util.UUID/randomUUID)
     :sent_at (System/currentTimeMillis)
     :type :text
     :body {:text "hello"}
     :attachments []
     :meta {:source "repl"}})

  (m/validate msg-schema/MessageEnvelopeSchema sample-message)

  (def create-message
    {:type :text
     :body {:text "hi"}
     :client_ref "client-1"})

  (m/validate msg-schema/MessageCreateSchema create-message)

  (def create-conversation
    {:type :direct
     :member_ids [(java.util.UUID/randomUUID)
                  (java.util.UUID/randomUUID)]
     :title "Support"})

  (m/validate msg-schema/ConversationCreateSchema create-conversation)

  (def pagination
    {:limit 50
     :direction :backward})

  (m/validate msg-schema/PaginationQuerySchema pagination)

  (def register-payload
    {:username "alice"
     :password "secret123"
     :email "alice@example.com"})

  (m/validate auth-schema/RegisterSchema register-payload)

  (def login-payload
    {:username "alice"
     :password "secret123"})

  (m/validate auth-schema/LoginSchema login-payload))
