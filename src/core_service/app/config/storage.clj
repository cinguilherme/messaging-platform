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

(defmethod ig/init-key :core-service.app.config.storage/minio
  [_ {:keys [endpoint access-key secret-key bucket logger]}]
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
   :logger logger})
