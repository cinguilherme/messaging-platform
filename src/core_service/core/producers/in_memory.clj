(ns core-service.core.producers.in-memory
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [core-service.queue :as q]
            [core-service.core.producers.protocol :as p]))

(defrecord InMemoryProducer [queues logger]
  p/Producer
  (produce! [_ msg-map options]
    (let [options (or options {})
          topic (or (:topic options) :default)
          trace (:trace options)
          queue (q/get-queue! queues topic)
          envelope {:msg msg-map
                    :options options
                    :metadata (cond-> {}
                                trace (assoc :trace trace))
                    :produced-at (System/currentTimeMillis)}]
      (logger/log logger :info ::producing-message {:topic topic :msg msg-map :trace trace})
      (q/enqueue! queue envelope)
      {:ok true
       :backend :in-memory
       :topic topic
       :queue-size (q/size queue)})))

(defmethod ig/init-key :core-service.core.producers.in-memory/producer
  [_ {:keys [queues logger]}]
  (logger/log logger :info ::initializing-producer {:flavor :in-memory
                                                    :queues queues})
  (->InMemoryProducer queues logger))

