(ns core-service.consumers.consumer
  (:require [integrant.core :as ig]
            [core-service.queue :as q]))

(defn- start-consumer!
  [{:keys [queue poll-ms stop?]}]
  (future
    (println "Consumer started")
    (while (not @stop?)
      (if-let [item (q/dequeue! queue)]
        (println "Consumed item:" item)
        (Thread/sleep poll-ms)))
    (println "Consumer stopped")))

(defmethod ig/init-key :core-service.consumers.consumer/consumer
  [_ {:keys [queue poll-ms] :or {poll-ms 100}}]
  (let [stop? (atom false)
        thread (start-consumer! {:queue queue :poll-ms poll-ms :stop? stop?})]
    {:queue queue
     :poll-ms poll-ms
     :stop? stop?
     :thread thread}))

(defmethod ig/halt-key! :core-service.consumers.consumer/consumer
  [_ {:keys [stop? thread]}]
  (when stop?
    (reset! stop? true))
  (when thread
    (deref thread 1000 nil))
  nil)