(ns core-service.app.config.clients
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

(defmethod ig/init-key :core-service.app.config.clients/redis
  [_ {:keys [uri]}]
  {:uri (or uri
            (getenv "REDIS_URI")
            (str "redis://" (default-host "redis" "localhost") ":6379"))})

(defmethod ig/init-key :core-service.app.config.clients/jetstream
  [_ {:keys [uri]}]
  {:uri (or uri
            (getenv "NATS_URI")
            (getenv "JETSTREAM_URI")
            (str "nats://" (default-host "nats" "localhost") ":4222"))})

(defmethod ig/init-key :core-service.app.config.clients/rabbitmq
  [_ {:keys [uri]}]
  {:uri (or uri
            (getenv "RABBITMQ_URI")
            (str "amqp://" (default-host "rabbitmq" "localhost") ":5672"))})

(defmethod ig/init-key :core-service.app.config.clients/valkey
  [_ {:keys [uri]}]
  {:uri (or uri
            (getenv "VALKEY_URI")
            (str "valkey://" (default-host "valkey" "localhost") ":6379"))})

(defmethod ig/init-key :core-service.app.config.clients/kafka
  [_ {:keys [bootstrap-servers producer-config]}]
  {:bootstrap-servers (or bootstrap-servers
                          (getenv "KAFKA_BOOTSTRAP_SERVERS")
                          (str (default-host "kafka" "localhost") ":" (if (in-docker?) "9092" "29092")))
   :producer-config (or producer-config {})})

