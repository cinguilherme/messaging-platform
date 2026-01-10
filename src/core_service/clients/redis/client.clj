(ns core-service.clients.redis.client)

(defrecord RedisClient [conn]
  Object
  (toString [_] (str "#RedisClient" (dissoc conn :spec))))

(defn make-client
  [{:keys [uri] :or {uri "redis://localhost:6379"}}]
  (->RedisClient {:pool {}
                 :spec {:uri uri}}))

