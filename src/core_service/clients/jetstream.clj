(ns core-service.clients.jetstream
  (:require [integrant.core :as ig]
            [core-service.clients.jetstream.client :as impl]))

(defmethod ig/init-key :core-service.clients.jetstream/client
  [_ opts]
  (impl/make-client opts))

(defmethod ig/halt-key! :core-service.clients.jetstream/client
  [_ client]
  (impl/close! client))

