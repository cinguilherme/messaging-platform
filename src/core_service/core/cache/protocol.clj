(ns core-service.core.cache.protocol)

(defprotocol CacheProtocol
  (cache-lookup [this key opts] "Get a value from the cache")
  (cache-put [this key value opts] "Set a value in the cache with options")
  (cache-delete [this key opts] "Delete a value from the cache")
  (cache-clear [this opts] "Flush the cache"))
