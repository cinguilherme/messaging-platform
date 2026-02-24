(ns core-service.app.config.storage
  (:require [core-service.app.libs.env :as env]
            [integrant.core :as ig]))


(def ^:private default-timeout-ms 10000)

(defmethod ig/init-key :core-service.app.config.storage/minio
  [_ {:keys [endpoint access-key secret-key bucket logger connection-timeout-ms socket-timeout-ms]}]
  {:endpoint (or endpoint
                 (env/getenv "MINIO_ENDPOINT")
                 (str "http://" (env/default-host "minio" "localhost") ":9000"))
   :access-key (or access-key
                   (env/getenv "MINIO_ACCESS_KEY")
                   "minio")
   :secret-key (or secret-key
                   (env/getenv "MINIO_SECRET_KEY")
                   "minio123")
   :bucket (or bucket
               (env/getenv "MINIO_BUCKET")
               "messaging-platform")
   :logger logger
   :connection-timeout-ms (or connection-timeout-ms
                               (env/env-long "MINIO_CONNECTION_TIMEOUT_MS")
                               default-timeout-ms)
   :socket-timeout-ms (or socket-timeout-ms
                          (env/env-long "MINIO_SOCKET_TIMEOUT_MS")
                          default-timeout-ms)})
