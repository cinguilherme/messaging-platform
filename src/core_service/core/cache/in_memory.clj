(ns core-service.core.cache.in-memory
  (:require [integrant.core :as ig]
            [core-service.core.cache.protocol :as p]
            [duct.logger :as logger]))

(defrecord InMemoryCache [data]
  p/CacheProtocol
  (cache-lookup [_ key _opts]
    (get @data key))
  (cache-put [_ key value _opts]
    (swap! data assoc key value)
    value)
  (cache-delete [_ key _opts]
    (swap! data dissoc key)
    nil)
  (cache-clear [_ _opts]
    (reset! data {})
    nil))

(defmethod ig/init-key :core-service.core.cache.in-memory/in-memory
  [_ {:keys [logger]}]
  (logger/log logger :info ::initializing-in-memory-cache)
  (->InMemoryCache (atom {})))

(defmethod ig/halt-key! :core-service.core.cache.in-memory/in-memory
  [_ this {:keys [logger]}]
  (logger/log logger :info ::halting-in-memory-cache)
  (reset! (:data this) {}))
