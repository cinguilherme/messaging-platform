(ns core-service.producers.in-memory
  (:require [integrant.core :as ig]
            [core-service.queue :as q]
            [core-service.producers.protocol :as p]))

(defrecord InMemoryProducer [queue]
  p/Producer
  (produce! [_ msg-map options]
    (let [envelope {:msg msg-map
                    :options options
                    :produced-at (System/currentTimeMillis)}]
      (q/enqueue! queue envelope)
      {:ok true
       :backend :in-memory
       :queue-size (q/size queue)})))

(defmethod ig/init-key :core-service.producers.in-memory/producer
  [_ {:keys [queue]}]
  (->InMemoryProducer queue))

