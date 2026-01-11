(ns core-service.consumers.kafka
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [core-service.clients.kafka.client :as kc]
            [core-service.messaging.codec :as codec]
            [core-service.messaging.routing :as routing]
            [core-service.messaging.dead-letter :as dl]))

(defn- topic->kafka-topic
  [routing topic]
  (let [cfg (routing/topic-config routing topic)]
    (or (:kafka-topic cfg)
        (str "core." (name topic)))))

(defn- topic->group-id
  [routing topic]
  (let [cfg (routing/topic-config routing topic)]
    (or (:group cfg) "core")))

(defn- start-kafka-subscription!
  [{:keys [subscription-id kafka routing codec handler dead-letter stop? logger topic options]}]
  (future
    (let [options (or options {})
          topic (or topic :default)
          kafka-topic (or (:kafka-topic options)
                          (topic->kafka-topic routing topic))
          group-id (or (:group-id options)
                       (:group options)
                       (topic->group-id routing topic))
          poll-ms (or (:poll-ms options) 250)
          consumer (kc/make-consumer kafka {:group-id (str group-id)})]
      (logger/log logger :report ::kafka-subscription-started
                  {:id subscription-id :topic topic :kafka-topic kafka-topic :group-id group-id})
      (try
        (kc/subscribe! consumer [kafka-topic])
        ;; Join group / initial poll
        (kc/poll! consumer {:timeout-ms 100})
        (while (not @stop?)
          (let [records (kc/poll! consumer {:timeout-ms poll-ms})]
            (doseq [r records]
              (let [envelope (codec/decode codec (:value r))]
                (try
                  (handler envelope)
                  (kc/commit! consumer)
                  (catch Exception e
                    (logger/log logger :error ::kafka-handler-failed
                                {:subscription-id subscription-id
                                 :topic topic
                                 :kafka-topic kafka-topic
                                 :error (.getMessage e)})
                    (if dead-letter
                      (let [dl-res (dl/send-dead-letter! dead-letter envelope
                                                         {:error (.getMessage e)
                                                          :stacktrace (with-out-str (.printStackTrace e))}
                                                         {})]
                        (if (:ok dl-res)
                          (do
                            (logger/log logger :info ::kafka-dead-letter-success {:subscription-id subscription-id})
                            (kc/commit! consumer))
                          (logger/log logger :error ::kafka-dead-letter-failed
                                      {:subscription-id subscription-id :error (:error dl-res)})))
                      (logger/log logger :warn ::no-dlq-configured {:subscription-id subscription-id}))))))))
        (finally
          (kc/close-consumer! consumer)
          (logger/log logger :report ::kafka-subscription-stopped {:id subscription-id}))))))

(defmethod ig/init-key :core-service.consumers.kafka/runtime
  [_ {:keys [kafka routing codec dead-letter logger]}]
  (let [stop? (atom false)
        subscriptions (-> routing :subscriptions (or {}))
        kafka-subs (into {}
                         (filter (fn [[_id sub]] (= :kafka (:source sub))))
                         subscriptions)
        threads
        (into {}
              (map (fn [[subscription-id {:keys [topic handler options]
                                         :or {options {}}}]]
                     (let [topic (or topic :default)]
                       [subscription-id
                        (start-kafka-subscription!
                          {:subscription-id subscription-id
                           :kafka kafka
                           :routing routing
                           :codec codec
                           :handler handler
                           :dead-letter dead-letter
                           :stop? stop?
                           :logger logger
                           :topic topic
                           :options options})])))
              kafka-subs)]
    {:stop? stop?
     :threads threads
     :logger logger}))

(defmethod ig/halt-key! :core-service.consumers.kafka/runtime
  [_ {:keys [stop? threads logger]}]
  (logger/log logger :report ::halting-kafka-runtime)
  (when stop?
    (reset! stop? true))
  (doseq [[_id thread] threads]
    (deref thread 1000 nil))
  nil)

