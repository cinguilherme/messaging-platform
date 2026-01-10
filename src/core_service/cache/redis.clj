(ns core-service.cache.redis
  (:require [integrant.core :as ig]
            [taoensso.carmine :as car]
            [core-service.cache.protocol :as p]
            [core-service.clients.redis.client]))

(defrecord RedisCache [redis-client]
  p/CacheProtocol
  (cache-lookup [_ key _opts]
    (car/wcar (:conn redis-client)
      (car/get key)))
  (cache-put [_ key value opts]
    (car/wcar (:conn redis-client)
      (if-let [ttl (:ttl opts)]
        (car/setex key ttl value)
        (car/set key value))))
  (cache-delete [_ key _opts]
    (car/wcar (:conn redis-client)
      (car/del key)))
  (cache-clear [_ _opts]
    (car/wcar (:conn redis-client)
      (car/flushdb))))

(defmethod ig/init-key :core-service.cache.redis/redis
  [_ {:keys [redis-client]}]
  (->RedisCache redis-client))
