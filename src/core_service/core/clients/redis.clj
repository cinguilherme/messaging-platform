(ns core-service.core.clients.redis
  (:require [integrant.core :as ig]
            [core-service.core.clients.redis.client :as impl]))

(defmethod ig/init-key :core-service.core.clients.redis/client
  [_ opts]
  (impl/make-client opts))

