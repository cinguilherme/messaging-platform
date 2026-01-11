(ns core-service.clients.jetstream.client
  (:import (io.nats.client Connection Nats Options)
           (io.nats.client JetStream JetStreamManagement)))

(defrecord JetStreamClient [^Connection conn ^JetStream js ^JetStreamManagement jsm uri]
  Object
  (toString [_] (str "#JetStreamClient{:uri " (pr-str uri) "}")))

(defn make-client
  [{:keys [uri]
    :or {uri "nats://localhost:4222"}}]
  (let [^Options opts (-> (Options/builder)
                          (.server uri)
                          (.maxReconnects 60)
                          (.reconnectWait (java.time.Duration/ofMillis 250))
                          (.connectionTimeout (java.time.Duration/ofSeconds 2))
                          (.build))
        ^Connection conn (Nats/connect opts)]
    (->JetStreamClient conn (.jetStream conn) (.jetStreamManagement conn) uri)))

(defn close!
  [^JetStreamClient client]
  (when-let [^Connection conn (:conn client)]
    (.close conn)))

