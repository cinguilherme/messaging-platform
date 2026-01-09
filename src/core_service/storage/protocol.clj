(ns core-service.storage.protocol)

(defprotocol StorageProtocol
  (storage-get [this key opts] "Get a value from the storage")
  (storage-put [this key value opts] "Set a value in the storage")
  (storage-delete [this key opts] "Delete a value from the storage"))
