(ns core-service.consumers.consumer
  (:require [integrant.core :as ig]
            [core-service.queue :as q]))

(defn- start-subscription!
  [{:keys [subscription-id queue poll-ms stop? handler]}]
  (future
    (println "Subscription started:" subscription-id)
    (while (not @stop?)
      (if-let [item (q/dequeue! queue)]
        (handler item)
        (Thread/sleep poll-ms)))
    (println "Subscription stopped:" subscription-id)))

(defmethod ig/init-key :core-service.consumers.consumer/consumer
  [_ {:keys [queues routing redis-runtime default-poll-ms]
      :or {default-poll-ms 100}}]
  (let [stop? (atom false)
        ;; Subscriptions come from the shared routing component.
        ;; (redis subscriptions are handled by :core-service.consumers.redis/runtime).
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
                                              :handler handler})])))
              in-mem-subs)]
    {:queues queues
     :routing routing
     ;; Keep the ref so Integrant orders startup correctly.
     :redis-runtime redis-runtime
     :default-poll-ms default-poll-ms
     :stop? stop?
     :threads threads}))

(defmethod ig/halt-key! :core-service.consumers.consumer/consumer
  [_ {:keys [stop? threads]}]
  (when stop?
    (reset! stop? true))
  (doseq [[_id thread] threads]
    (deref thread 1000 nil))
  nil)
