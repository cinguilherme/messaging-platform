(ns core-service.storage.minio
  (:require [integrant.core :as ig]
            [amazonica.aws.s3 :as s3]
            [duct.logger :as logger]
            [core-service.storage.protocol :as p]))

(defrecord MinioStorage [config bucket logger]
  p/StorageProtocol
  (storage-get [_ key _opts]
    (try
      (let [result (s3/get-object config :bucket-name bucket :key key)]
        (slurp (:input-stream result)))
      (catch Exception e
        (logger/log logger :error ::storage-get-failed {:key key :error (.getMessage e)})
        nil)))
  (storage-put [_ key value _opts]
    (try
      (s3/put-object config :bucket-name bucket :key key :content value)
      {:ok true}
      (catch Exception e
        (logger/log logger :error ::storage-put-failed {:key key :error (.getMessage e)})
        {:ok false :error (.getMessage e)})))
  (storage-delete [_ key _opts]
    (try
      (s3/delete-object config :bucket-name bucket :key key)
      {:ok true}
      (catch Exception e
        (logger/log logger :error ::storage-delete-failed {:key key :error (.getMessage e)})
        {:ok false :error (.getMessage e)}))))

(defmethod ig/init-key :core-service.storage/minio
  [_ {:keys [endpoint access-key secret-key bucket logger]}]
  (let [config {:endpoint endpoint
                :access-key access-key
                :secret-key secret-key
                :client-config {:path-style-access-enabled true}}]
    (logger/log logger :info ::initializing-minio-storage {:endpoint endpoint :bucket bucket})
    (->MinioStorage config bucket logger)))
