(ns core-service.app.server.receipt.authed
  (:require [core-service.app.db.conversations :as conversations-db]
            [core-service.app.libs.redis :as redis-lib]
            [core-service.app.observability.logging :as obs-log]
            [core-service.app.redis.receipts :as receipts]
            [core-service.app.redis.unread-index :as unread-index]
            [core-service.app.server.receipt.logic :as logic]
            [taoensso.carmine :as car]))

(defn- publish-to-conv-stream!
  "Publishes a receipt event to the conversation's Redis Pub/Sub channel.
  The event is encoded as an EDN string."
  [redis naming conv-id data sender-id]
  (let [pubsub-ch (str (get-in naming [:redis :pubsub-prefix] "chat:conv:") conv-id)
        event {:kind :receipt
               :receipt_type (name (:receipt_type data))
               :message_id (str (:message_id data))
               :user_id (str sender-id)
               :at (or (:at data) (System/currentTimeMillis))}]
    (car/wcar (redis-lib/conn redis)
              (car/publish pubsub-ch (pr-str event)))))

(defn receipts-create
  "Endpoint handler for creating a message receipt (delivered/read).
  Validates conversation membership, records the receipt in Redis with 
  optional TTL, and publishes a receipt event to the conversation's 
  Pub/Sub channel for real-time updates."
  [{:keys [webdeps]}]
  (let [{:keys [db redis state-store naming receipt metrics logger logging]} webdeps]
    (fn [req]
      (let [conv-id (get-in req [:parameters :path :id])
            sender-id (:user-id req)
            data (some-> (get-in req [:parameters :body])
                         logic/coerce-receipt-create)]
        (cond
          (nil? sender-id)
          {:status 401 :body {:ok false :error "invalid sender id"}}

          (not (conversations-db/member? db {:conversation-id conv-id :user-id sender-id}))
          {:status 403 :body {:ok false :error "not a member"}}

          :else
          (do
            (receipts/record! {:redis redis
                               :state-store state-store
                               :naming naming
                               :receipt receipt
                               :metrics metrics}
                              {:conversation-id conv-id
                               :message-id (:message_id data)
                               :user-id sender-id
                               :receipt-type (:receipt_type data)
                               :at (:at data)})
            (when (= :read (:receipt_type data))
              (try
                (unread-index/update-last-read-by-message! {:redis redis
                                                            :state-store state-store
                                                            :naming naming
                                                            :metrics metrics}
                                                           conv-id sender-id (:message_id data))
                (catch Exception e
                  (obs-log/log! logger logging :warn ::last-read-update-failed
                                {:conversation-id conv-id
                                 :sender-id sender-id
                                 :message-id (:message_id data)
                                 :error (.getMessage e)}))))
            (publish-to-conv-stream! redis naming conv-id data sender-id)
            {:ok true
             :conversation_id (str conv-id)
             :message_id (str (:message_id data))
             :receipt_type (name (:receipt_type data))}))))))
