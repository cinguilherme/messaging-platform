(ns core-service.storage.minio
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [core-service.storage.protocol :as p]))

(import (com.amazonaws.auth AWSStaticCredentialsProvider BasicAWSCredentials)
        (com.amazonaws.client.builder AwsClientBuilder$EndpointConfiguration)
        (com.amazonaws.services.s3 AmazonS3 AmazonS3ClientBuilder)
        (com.amazonaws.services.s3.model ObjectMetadata PutObjectRequest))

(defn- ensure-bucket!
  [^AmazonS3 client bucket logger]
  (try
    (when-not (.doesBucketExistV2 client bucket)
      (logger/log logger :info ::creating-bucket {:bucket bucket})
      (.createBucket client bucket))
    (catch Exception e
      (logger/log logger :error ::ensure-bucket-failed {:bucket bucket :error (.getMessage e)})
      (throw e))))

(defn- build-s3-client
  [{:keys [endpoint access-key secret-key]}]
  (let [creds (AWSStaticCredentialsProvider.
               (BasicAWSCredentials. access-key secret-key))
        ;; MinIO generally expects path-style access for localhost endpoints.
        builder (doto (AmazonS3ClientBuilder/standard)
                  (.withCredentials creds)
                  (.withPathStyleAccessEnabled true)
                  (.withEndpointConfiguration (AwsClientBuilder$EndpointConfiguration.
                                              endpoint
                                              "us-east-1")))]
    (.build builder)))

(defrecord MinioStorage [^AmazonS3 client bucket logger]
  p/StorageProtocol
  (storage-get [_ key _opts]
    (try
      (let [obj (.getObject client bucket key)]
        (with-open [in (.getObjectContent obj)]
          (slurp in)))
      (catch Exception e
        (logger/log logger :error ::storage-get-failed {:key key :error (.getMessage e)})
        nil)))
  (storage-put [_ key value _opts]
    (try
      (let [^bytes bytes (.getBytes (str value) "UTF-8")
            meta (doto (ObjectMetadata.)
                   (.setContentLength (alength bytes))
                   (.setContentType "application/json"))
            req (PutObjectRequest.
                 bucket
                 key
                 (java.io.ByteArrayInputStream. bytes)
                 meta)]
        (.putObject client req)
        {:ok true})
      (catch Exception e
        (logger/log logger :error ::storage-put-failed {:key key :error (.getMessage e)})
        {:ok false :error (.getMessage e)})))
  (storage-delete [_ key _opts]
    (try
      (.deleteObject client bucket key)
      {:ok true}
      (catch Exception e
        (logger/log logger :error ::storage-delete-failed {:key key :error (.getMessage e)})
        {:ok false :error (.getMessage e)}))))

(defmethod ig/init-key :core-service.storage/minio
  [_ {:keys [endpoint access-key secret-key bucket logger]}]
  (logger/log logger :info ::initializing-minio-storage {:endpoint endpoint :bucket bucket})
  (let [client (build-s3-client {:endpoint endpoint
                                :access-key access-key
                                :secret-key secret-key})]
    (ensure-bucket! client bucket logger)
    (->MinioStorage client bucket logger)))
