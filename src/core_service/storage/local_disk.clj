(ns core-service.storage.local-disk
  (:require [integrant.core :as ig]
            [clojure.java.io :as io]
            [duct.logger :as logger]
            [core-service.storage.protocol :as p]))

(defrecord LocalDiskStorage [root-path logger]
  p/StorageProtocol
  (storage-get [_ key _opts]
    (let [file (io/file root-path key)]
      (if (.exists file)
        (slurp file)
        (do
          (logger/log logger :warn ::file-not-found {:path (.getPath file)})
          nil))))
  (storage-put [_ key value _opts]
    (let [file (io/file root-path key)]
      (io/make-parents file)
      (spit file value)
      {:ok true :path (.getPath file)}))
  (storage-delete [_ key _opts]
    (let [file (io/file root-path key)]
      (if (.exists file)
        (do
          (io/delete-file file)
          {:ok true})
        {:ok false :error :not-found}))))

(defmethod ig/init-key :core-service.storage/local-disk
  [_ {:keys [root-path logger] :or {root-path "storage"}}]
  (logger/log logger :info ::initializing-local-disk-storage {:root-path root-path})
  (let [dir (io/file root-path)]
    (when-not (.exists dir)
      (.mkdirs dir)))
  (->LocalDiskStorage root-path logger))
