(ns core-service.app.storage.minio
  (:require [core-service.app.metrics :as app-metrics]
            [integrant.core :as ig]
            [duct.logger :as logger])
  (:import (com.amazonaws.auth AWSStaticCredentialsProvider BasicAWSCredentials)
           (com.amazonaws.client.builder AwsClientBuilder$EndpointConfiguration)
           (com.amazonaws.services.s3 AmazonS3 AmazonS3ClientBuilder)
           (com.amazonaws.services.s3.model ListObjectsV2Request ObjectMetadata PutObjectRequest)))

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
        builder (doto (AmazonS3ClientBuilder/standard)
                  (.withCredentials creds)
                  (.withPathStyleAccessEnabled true)
                  (.withEndpointConfiguration
                   (AwsClientBuilder$EndpointConfiguration. endpoint "us-east-1")))]
    (.build builder)))

(defn put-bytes!
  [{:keys [^AmazonS3 client bucket logger metrics]} key bytes content-type]
  (let [start (System/nanoTime)]
    (try
      (let [content-type (or content-type "application/octet-stream")
            meta (doto (ObjectMetadata.)
                   (.setContentLength (alength ^bytes bytes))
                   (.setContentType content-type))
            req (PutObjectRequest.
                 bucket
                 key
                 (java.io.ByteArrayInputStream. bytes)
                 meta)]
        (.putObject client req)
        (app-metrics/record-minio! metrics :put
                                   (app-metrics/duration-seconds start)
                                   :ok (alength ^bytes bytes))
        {:ok true :key key :bucket bucket :content-type content-type})
      (catch Exception e
        (app-metrics/record-minio! metrics :put
                                   (app-metrics/duration-seconds start)
                                   :error nil)
        (logger/log logger :error ::storage-put-failed {:key key :error (.getMessage e)})
        {:ok false :error (.getMessage e) :key key :bucket bucket}))))

(defn list-objects
  [{:keys [^AmazonS3 client bucket logger metrics]} {:keys [prefix limit token]}]
  (let [start (System/nanoTime)]
    (try
      (let [req (doto (ListObjectsV2Request.)
                  (.setBucketName bucket)
                  (.setPrefix (or prefix "images/"))
                  (.setMaxKeys (int (or limit 50))))
            _ (when (seq token)
                (.setContinuationToken req token))
            resp (.listObjectsV2 client req)
            summaries (.getObjectSummaries resp)
            items (mapv (fn [summary]
                          {:key (.getKey summary)
                           :size (.getSize summary)
                           :last-modified (.getLastModified summary)})
                        summaries)]
        (app-metrics/record-minio! metrics :list
                                   (app-metrics/duration-seconds start)
                                   :ok nil)
        {:ok true
         :items items
         :prefix (.getPrefix resp)
         :truncated? (.isTruncated resp)
         :next-token (.getNextContinuationToken resp)})
      (catch Exception e
        (app-metrics/record-minio! metrics :list
                                   (app-metrics/duration-seconds start)
                                   :error nil)
        (logger/log logger :error ::list-objects-failed {:error (.getMessage e)})
        {:ok false :error (.getMessage e)}))))

(defn delete-object!
  [{:keys [^AmazonS3 client bucket logger metrics]} key]
  (let [start (System/nanoTime)]
    (try
      (.deleteObject client bucket key)
      (app-metrics/record-minio! metrics :delete
                                 (app-metrics/duration-seconds start)
                                 :ok nil)
      {:ok true :key key :bucket bucket}
      (catch Exception e
        (app-metrics/record-minio! metrics :delete
                                   (app-metrics/duration-seconds start)
                                   :error nil)
        (logger/log logger :error ::delete-object-failed {:key key :error (.getMessage e)})
        {:ok false :key key :bucket bucket :error (.getMessage e)}))))

(defn get-bytes!
  [{:keys [^AmazonS3 client bucket logger metrics]} key]
  (let [start (System/nanoTime)]
    (try
      (with-open [obj (.getObject client bucket key)
                  stream (.getObjectContent obj)
                  out (java.io.ByteArrayOutputStream.)]
        (let [buf (byte-array 4096)]
          (loop [n (.read stream buf)]
            (when (pos? n)
              (.write out buf 0 n)
              (recur (.read stream buf)))))
        (let [bytes (.toByteArray out)]
          (app-metrics/record-minio! metrics :get
                                     (app-metrics/duration-seconds start)
                                     :ok (alength ^bytes bytes))
          {:ok true
           :key key
           :bucket bucket
           :bytes bytes}))
      (catch Exception e
        (app-metrics/record-minio! metrics :get
                                   (app-metrics/duration-seconds start)
                                   :error nil)
        (logger/log logger :error ::get-object-failed {:key key :error (.getMessage e)})
        {:ok false :key key :bucket bucket :error (.getMessage e)}))))

(defmethod ig/init-key :core-service.app.storage.minio/client
  [_ {:keys [config metrics] :as opts}]
  (let [{:keys [endpoint access-key secret-key bucket logger]}
        (if (contains? opts :config) config opts)]
    (logger/log logger :info ::initializing-minio-client {:endpoint endpoint :bucket bucket})
    (let [client (build-s3-client {:endpoint endpoint
                                   :access-key access-key
                                   :secret-key secret-key})]
      (ensure-bucket! client bucket logger)
      {:client client :bucket bucket :logger logger :metrics metrics})))
