(ns core-service.producers.in-memory
  (:require [integrant.core :as ig]
            [core-service.queue :as q]
            [core-service.producers.protocol :as p]))

(defrecord InMemoryProducer [queues]
  p/Producer
  (produce! [_ msg-map options]
    (let [options (or options {})
          topic (or (:topic options) :default)
          queue (q/get-queue! queues topic)
          envelope {:msg msg-map
                    :options options
                    :produced-at (System/currentTimeMillis)}]
      (q/enqueue! queue envelope)
      {:ok true
       :backend :in-memory
       :topic topic
       :queue-size (q/size queue)})))

(defmethod ig/init-key :core-service.producers.in-memory/producer
  [_ {:keys [queues]}]
  (->InMemoryProducer queues))

