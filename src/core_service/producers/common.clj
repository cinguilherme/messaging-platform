(ns core-service.producers.common
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [core-service.messaging.routing :as routing]
            [core-service.producers.protocol :as p]))

(defrecord CommonProducer [default-producer-key producers routing logger]
  p/Producer
  (produce! [_ msg-map options]
    (let [options (or options {})
          topic (or (:topic options) :default)
          ;; Backwards-compat: allow explicit :producer override.
          producer-key (or (:producer options)
                           (:source options)
                           (routing/source-for-topic routing topic)
                           default-producer-key)
          _ (logger/log logger :info "Producing message with producer:" producer-key)
          delegate (get producers producer-key)]
      (when-not delegate
        (throw (ex-info "Unknown producer key"
                        {:producer producer-key
                         :known (keys producers)})))
      (logger/log logger :info ::delegating-production {:topic topic :to producer-key})
      (p/produce! delegate msg-map options))))

(defmethod ig/init-key :core-service.producers.common/producer
  [_ {:keys [default-producer producers routing logger]
      :or {default-producer :in-memory}}]
  (->CommonProducer default-producer producers routing logger))

