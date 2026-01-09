(ns core-service.storage.common
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [core-service.storage.protocol :as p]))

(defrecord CommonStorage [default-storage-key backends logger]
  p/StorageProtocol
  (storage-get [_ key opts]
    (let [storage-key (or (:storage opts) default-storage-key)
          delegate (get backends storage-key)]
      (logger/log logger :debug ::storage-get {:key key :storage storage-key})
      (p/storage-get delegate key opts)))
  (storage-put [_ key value opts]
    (let [storage-key (or (:storage opts) default-storage-key)
          delegate (get backends storage-key)]
      (logger/log logger :debug ::storage-put {:key key :storage storage-key})
      (p/storage-put delegate key value opts)))
  (storage-delete [_ key opts]
    (let [storage-key (or (:storage opts) default-storage-key)
          delegate (get backends storage-key)]
      (logger/log logger :debug ::storage-delete {:key key :storage storage-key})
      (p/storage-delete delegate key opts))))

(defmethod ig/init-key :core-service.storage/common
  [_ {:keys [default-storage backends logger]
      :or {default-storage :local-disk}}]
  (logger/log logger :info ::initializing-common-storage {:default-storage default-storage})
  (->CommonStorage default-storage backends logger))
