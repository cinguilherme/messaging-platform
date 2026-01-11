(ns core-service.core.producers.jetstream
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [core-service.core.messaging.codec :as codec]
            [core-service.core.messaging.routing :as routing]
            [core-service.core.producers.protocol :as p])
  (:import (io.nats.client JetStreamManagement JetStream)
           (io.nats.client.api StreamConfiguration StorageType)))

(defn- topic->subject
  [routing topic]
  (let [cfg (routing/topic-config routing topic)]
    (or (:subject cfg)
        (str "core." (name topic)))))

(defn- topic->stream
  [routing topic]
  (let [cfg (routing/topic-config routing topic)]
    (or (:stream cfg)
        (str "core_" (name topic)))))

(defn- ensure-stream!
  [^JetStreamManagement jsm stream subject]
  (try
    (.getStreamInfo jsm stream)
    (catch Exception _e
      ;; Create a simple stream storing messages on disk.
      (let [cfg (-> (StreamConfiguration/builder)
                    (.name stream)
                    (.storageType StorageType/File)
                    (.subjects (into-array String [subject]))
                    (.build))]
        (try
          (.addStream jsm cfg)
          (catch Exception _e2
            ;; If it already exists due to a race, ignore.
            nil))))))

(defrecord JetStreamProducer [jetstream-client routing codec logger]
  p/Producer
  (produce! [_ msg-map options]
    (let [options (or options {})
          topic (or (:topic options) :default)
          trace (:trace options)
          subject (topic->subject routing topic)
          stream (topic->stream routing topic)
          envelope {:msg msg-map
                    :options options
                    :metadata (cond-> {}
                                trace (assoc :trace trace))
                    :produced-at (System/currentTimeMillis)}
          payload (codec/encode codec envelope)
          bytes (.getBytes (str payload) "UTF-8")
          ^JetStream js (-> jetstream-client :js)
          ^JetStreamManagement jsm (-> jetstream-client :jsm)]
      (ensure-stream! jsm stream subject)
      (logger/log logger :info ::producing-message {:topic topic :subject subject :stream stream :trace trace})
      (let [ack (.publish js subject bytes)]
        {:ok true
         :backend :jetstream
         :topic topic
         :subject subject
         :stream (.getStream ack)
         :seq (.getSeqno ack)}))))

(defmethod ig/init-key :core-service.core.producers.jetstream/producer
  [_ {:keys [jetstream routing codec logger]}]
  (->JetStreamProducer jetstream routing codec logger))

