(ns core-service.cache.common
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [core-service.cache.protocol :as p]))

(defrecord CommonCache [default-cache-key caches logger]
  p/CacheProtocol
  (cache-lookup [_ key opts]
    (let [cache-key (or (:cache opts) default-cache-key)
          delegate (get caches cache-key)]
      (logger/log logger :debug ::cache-lookup {:key key :cache cache-key})
      (p/cache-lookup delegate key opts)))
  (cache-put [_ key value opts]
    (let [cache-key (or (:cache opts) default-cache-key)
          delegate (get caches cache-key)]
      (logger/log logger :debug ::cache-put {:key key :cache cache-key})
      (p/cache-put delegate key value opts)))
  (cache-delete [_ key opts]
    (let [cache-key (or (:cache opts) default-cache-key)
          delegate (get caches cache-key)]
      (logger/log logger :debug ::cache-delete {:key key :cache cache-key})
      (p/cache-delete delegate key opts)))
  (cache-clear [_ opts]
    (let [cache-key (or (:cache opts) default-cache-key)
          delegate (get caches cache-key)]
      (logger/log logger :debug ::cache-clear {:cache cache-key})
      (p/cache-clear delegate opts))))

(defmethod ig/init-key :core-service.cache.common/common
  [_ {:keys [default-cache caches logger]
      :or {default-cache :in-memory}}]
  (logger/log logger :info ::initializing-common-cache {:default-cache default-cache})
  (->CommonCache default-cache caches logger))
