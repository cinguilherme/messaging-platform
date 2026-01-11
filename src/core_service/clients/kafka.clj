(ns core-service.clients.kafka
  (:require [integrant.core :as ig]
            [core-service.clients.kafka.client :as impl]))

(defmethod ig/init-key :core-service.clients.kafka/client
  [_ opts]
  (impl/make-client opts))

(defmethod ig/halt-key! :core-service.clients.kafka/client
  [_ client]
  (impl/close! client))

