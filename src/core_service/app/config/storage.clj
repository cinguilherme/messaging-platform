(ns core-service.app.config.storage
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [integrant.core :as ig]))

(defn- getenv
  "Returns env var value if present and not blank."
  [k]
  (let [v (some-> (System/getenv k) str/trim)]
    (when (seq v) v)))

(defn- in-docker?
  "Best-effort detection for running inside a Docker container."
  []
  (or (boolean (getenv "DOCKER"))
      (boolean (getenv "IN_DOCKER"))
      (.exists (io/file "/.dockerenv"))))

(defn- default-host
  "Picks a default host based on whether we're running in Docker."
  [docker-host local-host]
  (if (in-docker?) docker-host local-host))

(def ^:private default-timeout-ms 10000)

(defmethod ig/init-key :core-service.app.config.storage/minio
  [_ {:keys [endpoint access-key secret-key bucket logger connection-timeout-ms socket-timeout-ms]}]
  {:endpoint (or endpoint
                 (getenv "MINIO_ENDPOINT")
                 (str "http://" (default-host "minio" "localhost") ":9000"))
   :access-key (or access-key
                   (getenv "MINIO_ACCESS_KEY")
                   "minio")
   :secret-key (or secret-key
                   (getenv "MINIO_SECRET_KEY")
                   "minio123")
   :bucket (or bucket
               (getenv "MINIO_BUCKET")
               "messaging-platform")
   :logger logger
   :connection-timeout-ms (or connection-timeout-ms
                               (some-> (getenv "MINIO_CONNECTION_TIMEOUT_MS") parse-long)
                               default-timeout-ms)
   :socket-timeout-ms (or socket-timeout-ms
                          (some-> (getenv "MINIO_SOCKET_TIMEOUT_MS") parse-long)
                          default-timeout-ms)})
