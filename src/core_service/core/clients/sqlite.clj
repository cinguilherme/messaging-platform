(ns core-service.core.clients.sqlite
  (:require [integrant.core :as ig]
            [core-service.core.clients.sqlite.client :as impl]))

(defmethod ig/init-key :core-service.core.clients.sqlite/client
  [_ opts]
  (impl/make-client opts))

(defmethod ig/halt-key! :core-service.core.clients.sqlite/client
  [_ client]
  (impl/close! client))

