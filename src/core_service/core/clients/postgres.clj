(ns core-service.core.clients.postgres
  (:require [integrant.core :as ig]
            [core-service.core.clients.postgres.client :as impl]))

(defmethod ig/init-key :core-service.core.clients.postgres/client
  [_ opts]
  (impl/make-client opts))

(defmethod ig/halt-key! :core-service.core.clients.postgres/client
  [_ client]
  (impl/close! client))

