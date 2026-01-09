(ns core-service.producers.in-memory
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [core-service.queue :as q]
            [core-service.producers.protocol :as p]))

(defrecord InMemoryProducer [queues logger]
  p/Producer
  (produce! [_ msg-map options]
    (let [options (or options {})
          topic (or (:topic options) :default)
          queue (q/get-queue! queues topic)
          envelope {:msg msg-map
                    :options options
                    :produced-at (System/currentTimeMillis)}]
      (logger/log logger :info ::producing-message {:topic topic :msg msg-map})
      (q/enqueue! queue envelope)
      {:ok true
       :backend :in-memory
       :topic topic
       :queue-size (q/size queue)})))

(defmethod ig/init-key :core-service.producers.in-memory/producer
  [_ {:keys [queues logger]}]
  (logger/log logger :info ::initializing-producer {:flavor :in-memory
                                                    :queues queues})
  (->InMemoryProducer queues logger))

