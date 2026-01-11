(ns core-service.core.consumers.consumer
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [core-service.queue :as q]
            [core-service.core.messaging.dead-letter :as dl]))

(defn- start-subscription!
  [{:keys [subscription-id queue poll-ms stop? handler dead-letter logger]}]
  (future
    (logger/log logger :report ::subscription-started {:id subscription-id})
    (while (not @stop?)
      (if-let [item (q/dequeue! queue)]
        (try
          (handler item)
          (catch Exception e
            (logger/log logger :error ::handler-failed {:id subscription-id :error (.getMessage e)})
            (when dead-letter
              (dl/send-dead-letter! dead-letter item 
                                    {:error (.getMessage e) 
                                     :stacktrace (with-out-str (.printStackTrace e))} 
                                    {}))))
        (Thread/sleep poll-ms)))
    (logger/log logger :report ::subscription-stopped {:id subscription-id})))

(defmethod ig/init-key :core-service.core.consumers.consumer/consumer
  [_ {:keys [queues routing redis-runtime jetstream-runtime kafka-runtime dead-letter default-poll-ms logger]
      :or {default-poll-ms 100}}]
  (let [stop? (atom false)
        ;; Subscriptions come from the shared routing component.
        ;; Non in-memory subscriptions are handled by their respective runtimes
        ;; (e.g. :core-service.core.consumers.redis/runtime, :core-service.core.consumers.kafka/runtime, etc).
        subscriptions (-> routing :subscriptions (or {}))
        in-mem-subs (into {}
                          (filter (fn [[_id sub]] (= :in-memory (:source sub))))
                          subscriptions)
        threads
        (into {}
              (map (fn [[subscription-id {:keys [topic handler options]
                                         :or {options {}}}]]
                     (let [topic (or topic :default)
                           queue (q/get-queue! queues topic)
                           poll-ms (or (:poll-ms options) default-poll-ms)]
                       [subscription-id
                        (start-subscription! {:subscription-id subscription-id
                                              :queue queue
                                              :poll-ms poll-ms
                                              :stop? stop?
                                              :handler handler
                                              :dead-letter dead-letter
                                              :logger logger})])))
              in-mem-subs)]
    {:queues queues
     :routing routing
     ;; Keep the ref so Integrant orders startup correctly.
     :redis-runtime redis-runtime
     :jetstream-runtime jetstream-runtime
     :kafka-runtime kafka-runtime
     :dead-letter dead-letter
     :default-poll-ms default-poll-ms
     :stop? stop?
     :threads threads
     :logger logger}))

(defmethod ig/halt-key! :core-service.core.consumers.consumer/consumer
  [_ {:keys [stop? threads logger]}]
  (logger/log logger :report ::halting-consumer)
  (when stop?
    (reset! stop? true))
  (doseq [[_id thread] threads]
    (deref thread 1000 nil))
  nil)
