(ns core-service.producers.common
  (:require [integrant.core :as ig]
            [core-service.producers.protocol :as p]))

(defrecord CommonProducer [default-producer-key producers]
  p/Producer
  (produce! [_ msg-map options]
    (let [producer-key (or (:producer options) default-producer-key)
          delegate (get producers producer-key)]
      (when-not delegate
        (throw (ex-info "Unknown producer key"
                        {:producer producer-key
                         :known (keys producers)})))
      (p/produce! delegate msg-map options))))

(defmethod ig/init-key :core-service.producers.common/producer
  [_ {:keys [default-producer producers]
      :or {default-producer :in-memory}}]
  (->CommonProducer default-producer producers))

