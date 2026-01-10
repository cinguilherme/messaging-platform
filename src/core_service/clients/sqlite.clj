(ns core-service.clients.sqlite
  (:require [integrant.core :as ig]
            [core-service.clients.sqlite.client :as impl]))

(defmethod ig/init-key :core-service.clients.sqlite/client
  [_ opts]
  (impl/make-client opts))

(defmethod ig/halt-key! :core-service.clients.sqlite/client
  [_ client]
  (impl/close! client))

