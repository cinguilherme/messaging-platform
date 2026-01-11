(ns core-service.clients.typesense
  (:require [integrant.core :as ig]
            [core-service.clients.typesense.client :as impl]))

(defmethod ig/init-key :core-service.clients.typesense/client
  [_ opts]
  (impl/make-client opts))

