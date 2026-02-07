(ns core-service.app.storage.minio
  (:require [core-service.app.metrics :as app-metrics]
            [d-core.core.storage.protocol :as storage]))

(defn- normalize-ctx
  [storage-or-ctx]
  (cond
    (and (map? storage-or-ctx) (contains? storage-or-ctx :storage)) storage-or-ctx
    :else {:storage storage-or-ctx}))

(defn- result-status
  [result]
  (cond
    (:ok result) :ok
    (= :not-found (:error-type result)) :not-found
    :else :error))

(defn put-bytes!
  [storage-or-ctx key bytes content-type]
  (let [{:keys [storage metrics]} (normalize-ctx storage-or-ctx)
        start (System/nanoTime)
        result (try
                 (storage/storage-put-bytes storage key bytes {:content-type content-type})
                 (catch Exception e
                   {:ok false :key key :error (.getMessage e)}))]
    (app-metrics/record-minio! metrics :put
                               (app-metrics/duration-seconds start)
                               (result-status result)
                               (when (:ok result) (alength ^bytes bytes)))
    result))

(defn list-objects
  [storage-or-ctx {:keys [prefix limit token] :as opts}]
  (let [{:keys [storage metrics]} (normalize-ctx storage-or-ctx)
        start (System/nanoTime)
        result (try
                 (storage/storage-list storage (merge opts {:prefix prefix
                                                           :limit limit
                                                           :token token}))
                 (catch Exception e
                   {:ok false :error (.getMessage e)}))]
    (app-metrics/record-minio! metrics :list
                               (app-metrics/duration-seconds start)
                               (result-status result)
                               nil)
    result))

(defn delete-object!
  [storage-or-ctx key]
  (let [{:keys [storage metrics]} (normalize-ctx storage-or-ctx)
        start (System/nanoTime)
        result (try
                 (storage/storage-delete storage key {})
                 (catch Exception e
                   {:ok false :key key :error (.getMessage e)}))]
    (app-metrics/record-minio! metrics :delete
                               (app-metrics/duration-seconds start)
                               (result-status result)
                               nil)
    result))

(defn get-bytes!
  [storage-or-ctx key]
  (let [{:keys [storage metrics]} (normalize-ctx storage-or-ctx)
        start (System/nanoTime)
        result (try
                 (storage/storage-get-bytes storage key {})
                 (catch Exception e
                   {:ok false :key key :error (.getMessage e)}))]
    (app-metrics/record-minio! metrics :get
                               (app-metrics/duration-seconds start)
                               (result-status result)
                               (when (:ok result)
                                 (alength ^bytes (:bytes result))))
    result))
