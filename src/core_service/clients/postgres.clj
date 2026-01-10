(ns core-service.clients.postgres
  (:require [integrant.core :as ig]
            [core-service.clients.postgres.client :as impl]))

(defmethod ig/init-key :core-service.clients.postgres/client
  [_ opts]
  (impl/make-client opts))

(defmethod ig/halt-key! :core-service.clients.postgres/client
  [_ client]
  (impl/close! client))

