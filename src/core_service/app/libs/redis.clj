(ns core-service.app.libs.redis
  (:require [d-core.core.clients.redis.utils :as redis-utils]))

(defn conn
  [redis-client]
  (redis-utils/conn redis-client))

(defn normalize-key
  [k]
  (redis-utils/normalize-key k))

(defn fields->map
  [fields]
  (redis-utils/fields->map fields))
