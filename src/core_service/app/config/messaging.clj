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
            :test-queue {}
            :kafka-test {:kafka-topic "core.kafka_test"
                         :group "core"}
            :jetstream-test {:subject "core.jetstream_test"
                             :stream "core_jetstream_test"
                             :durable "core_jetstream_test"}}
   :publish {:default {:targets [{:producer :redis
                                  :stream "core:default"}]}
             :sample-fail {:targets [{:producer :redis
                                      :stream "sample:fail-sample"}]}
             :test-queue {:targets [{:producer :in-memory}]}
             :kafka-test {:targets [{:producer :kafka
                                     :kafka-topic "core.kafka_test"}]}
             :jetstream-test {:targets [{:producer :jetstream
                                         :subject "core.jetstream_test"
                                         :stream "core_jetstream_test"}]}}
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
                                              :suffix ".dl"}}
                   :kafka-test {:source :kafka
                                :topic :kafka-test
                                :handler :log-consumed
                                :options {:poll-ms 250}}
                   :jetstream-test {:source :jetstream
                                    :topic :jetstream-test
                                    :handler :log-consumed
                                    :options {:pull-batch 1
                                              :expires-ms 1000}}}})

(defmethod ig/init-key :core-service.app.config.messaging/routing
  [_ overrides]
  ;; Allow duct.edn to pass overrides (env-specific tweaks) while keeping the base
  ;; routing map in code. D-Core handles deep-merge and handler resolution.
  (let [overrides (or overrides {})]
    (if (or (contains? overrides :default-routing)
            (contains? overrides :overrides))
      (cond-> overrides
        (not (contains? overrides :default-routing))
        (assoc :default-routing default-routing))
      {:default-routing default-routing
       :overrides overrides})))
