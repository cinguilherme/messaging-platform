(ns core-service.storage.protocol)

(defprotocol StorageProtocol
  (get [this key] "Get a value from the storage")
  (set [this key value] "Set a value in the storage")
  (del [this key] "Delete a value from the storage"))

