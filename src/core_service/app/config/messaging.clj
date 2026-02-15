(ns core-service.app.config.messaging
  (:require [integrant.core :as ig]))

;; Config components: return config maps (can include Integrant refs via ig/ref).

(def default-routing
  {:defaults {:source :redis
              ;; Dead letter defaults (topic-level `:deadletter` can override)
              :deadletter {:sink :hybrid
                           :policy :default
                           :max-attempts 3
                           :delay-ms 100
                           ;; Derived DLQ destination naming (default: \".dl\")
                           :suffix ".dl"}}
   :topics {:default {:stream "core:default"
                      :group "core"}
            :sample-fail {:stream "sample:fail-sample"
                          :group "sample"}
            :test-queue {}}
   :publish {:default {:targets [{:producer :redis
                                  :stream "core:default"}]}
             :sample-fail {:targets [{:producer :redis
                                      :stream "sample:fail-sample"}]}
             :test-queue {:targets [{:producer :in-memory}]}}
   :subscriptions {:default {:source :redis
                             :topic :default
                             ;; Resolved at init time from :handlers (d-core routing init).
                             :handler :log-consumed
                             :options {:block-ms 5000}}
                   :sample-fail {:source :redis
                                 :topic :sample-fail
                                 :handler :log-consumed
                                 :options {:block-ms 5000}
                                 :deadletter {:sink :hybrid
                                              :policy :default
                                              :max-attempts 3
                                              :delay-ms 100
                                              :suffix ".dl"}}}})

(defmethod ig/init-key :core-service.app.config.messaging/default-routing
  [_ _]
  default-routing)

(def messaging-storage-names
  {:redis {:stream-prefix "chat:conv:"
           :pubsub-prefix "chat:conv:"
           :receipts-prefix "chat:receipts:"
           :message-index-prefix "chat:msg_index:"
           :message-seq-prefix "chat:msg_seq:"
           :last-read-prefix "chat:last_read:"
           :idempotency-prefix "chat:idemp:"
           :sequence-prefix "chat:seq:"
           :flush-prefix "chat:flush:"
           :stream-meta-prefix "__dcore:stream"}
   :minio {:bucket "messages"
           :segments-prefix "segments/"
           :attachments-prefix "attachments/"}})

(defmethod ig/init-key :core-service.app.config.messaging/storage-names
  [_ opts]
  (merge messaging-storage-names opts))

(defmethod ig/init-key :core-service.app.config.messaging/stream-meta-prefix
  [_ {:keys [naming prefix]}]
  (or prefix
      (get-in naming [:redis :stream-meta-prefix])
      "__dcore:stream"))

(def default-segment-config
  {:max-bytes 262144
   :flush-interval-ms 300000
   :compression :gzip
   :codec :edn
   :batch-size 200
   :trim-stream? true
   :trim-min-entries 100})

(defmethod ig/init-key :core-service.app.config.messaging/segment-config
  [_ opts]
  (merge default-segment-config opts))

(def default-retention-config
  {:max-age-ms 2592000000
   :batch-size 200
   :interval-ms 3600000})

(defmethod ig/init-key :core-service.app.config.messaging/retention-config
  [_ opts]
  (merge default-retention-config opts))

(def default-attachment-retention-config
  {:max-age-ms 2592000000
   :batch-size 200
   :interval-ms 3600000})

(defmethod ig/init-key :core-service.app.config.messaging/attachment-retention-config
  [_ opts]
  (merge default-attachment-retention-config opts))

(def default-receipt-config
  {:ttl-ms 3600000})

(defmethod ig/init-key :core-service.app.config.messaging/receipt-config
  [_ opts]
  (merge default-receipt-config opts))

(def default-idempotency-config
  {:ttl-ms 21600000
   :header "idempotency-key"
   :require? true
   :allow-client-ref? true
   :max-length 128})

(defmethod ig/init-key :core-service.app.config.messaging/idempotency-config
  [_ opts]
  (merge default-idempotency-config opts))
