(ns core-service.app.config.clients
  (:require [core-service.app.libs.env :as env]
            [integrant.core :as ig]))

(defmethod ig/init-key :core-service.app.config.clients/redis
  [_ {:keys [uri]}]
  {:uri (or uri
            (env/getenv "REDIS_URI")
            (str "redis://" (env/default-host "redis" "localhost") ":6379"))})

(defmethod ig/init-key :core-service.app.config.clients/jetstream
  [_ {:keys [uri]}]
  {:uri (or uri
            (env/getenv "NATS_URI")
            (env/getenv "JETSTREAM_URI")
            (str "nats://" (env/default-host "nats" "localhost") ":4222"))})

(defmethod ig/init-key :core-service.app.config.clients/rabbitmq
  [_ {:keys [uri]}]
  {:uri (or uri
            (env/getenv "RABBITMQ_URI")
            (str "amqp://" (env/default-host "rabbitmq" "localhost") ":5672"))})

(defmethod ig/init-key :core-service.app.config.clients/valkey
  [_ {:keys [uri]}]
  {:uri (or uri
            (env/getenv "VALKEY_URI")
            (str "valkey://" (env/default-host "valkey" "localhost") ":6379"))})

(defmethod ig/init-key :core-service.app.config.clients/kafka
  [_ {:keys [bootstrap-servers producer-config]}]
  {:bootstrap-servers (or bootstrap-servers
                          (env/getenv "KAFKA_BOOTSTRAP_SERVERS")
                          (str (env/default-host "kafka" "localhost") ":" (if (env/in-docker?) "9092" "29092")))
   :producer-config (or producer-config {})})

