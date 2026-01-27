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
           :sequence-prefix "chat:seq:"}
   :minio {:bucket "messages"
           :segments-prefix "segments/"
           :attachments-prefix "attachments/"}})

(defmethod ig/init-key :core-service.app.config.messaging/storage-names
  [_ opts]
  (merge messaging-storage-names opts))
