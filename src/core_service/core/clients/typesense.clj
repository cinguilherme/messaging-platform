(ns core-service.core.clients.typesense
  (:require [integrant.core :as ig]
            [core-service.core.clients.typesense.client :as impl]))

(defmethod ig/init-key :core-service.core.clients.typesense/client
  [_ opts]
  (impl/make-client opts))

