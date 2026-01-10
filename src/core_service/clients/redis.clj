(ns core-service.clients.redis
  (:require [integrant.core :as ig]
            [core-service.clients.redis.client :as impl]))

(defmethod ig/init-key :core-service.clients.redis/client
  [_ opts]
  (impl/make-client opts))

