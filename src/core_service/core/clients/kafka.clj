(ns core-service.core.clients.kafka
  (:require [integrant.core :as ig]
            [core-service.core.clients.kafka.client :as impl]))

(defmethod ig/init-key :core-service.core.clients.kafka/client
  [_ opts]
  (impl/make-client opts))

(defmethod ig/halt-key! :core-service.core.clients.kafka/client
  [_ client]
  (impl/close! client))

