(ns core-service.core.producers.producer
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [core-service.core.producers.protocol :as p]))

;; producers-map should be a map of producer-name -> producer-component available in the system map
;; like {:kafka {:producer :core-service.core.producers.kafka/producer}
;;       :rabbitmq {:producer :core-service.core.producers.rabbitmq/producer}}

;;Or it could be a map of producer-name -> producer instance, but this needs to account that this producers have their life cycles handled by Duct system map and lifecycles.


(defrecord CommonProducer [default-producer-key producers logger]
  ;; Holds all producers that it can delegate to.
  p/Producer
  (produce! [_ msg-map options]
    (let [producer-key (or (:producer options) default-producer-key)
          _ (logger/log logger :info "Producing message with producer:" producer-key)
          delegate (get producers producer-key)]
      (when-not delegate
        (throw (ex-info "Unknown producer key"
                        {:producer producer-key
                         :known (keys producers)})))
      (p/produce! delegate msg-map options))))

(defmethod ig/init-key :core-service.core.producers.common/producer
  [_ {:keys [default-producer producers logger]
      :or {default-producer :in-memory}}]
  (->CommonProducer default-producer producers logger))

