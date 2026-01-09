(ns core-service.redis.client
  (:require [integrant.core :as ig]))

(defrecord RedisClient [conn]
  Object
  (toString [_] (str "#RedisClient" (dissoc conn :spec))))

(defmethod ig/init-key :core-service.redis/client
  [_ {:keys [uri] :or {uri "redis://localhost:6379"}}]
  (->RedisClient {:pool {}
                 :spec {:uri uri}}))

