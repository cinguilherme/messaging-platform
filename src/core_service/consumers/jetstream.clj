(ns core-service.consumers.jetstream
  (:require [integrant.core :as ig]
            [duct.logger :as logger]
            [core-service.messaging.codec :as codec]
            [core-service.messaging.routing :as routing]
            [core-service.messaging.dead-letter :as dl])
  (:import (io.nats.client JetStream JetStreamManagement JetStreamSubscription Message)
           (io.nats.client.api StreamConfiguration StorageType ConsumerConfiguration AckPolicy DeliverPolicy)
           (io.nats.client PullSubscribeOptions)
           (java.time Duration)))

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

(defn- topic->durable
  [routing topic]
  (let [cfg (routing/topic-config routing topic)]
    (or (:durable cfg)
        (str "core_" (name topic)))))

(defn- ensure-stream!
  [^JetStreamManagement jsm stream subject]
  (try
    (.getStreamInfo jsm stream)
    (catch Exception _e
      (let [cfg (-> (StreamConfiguration/builder)
                    (.name stream)
                    (.storageType StorageType/File)
                    (.subjects (into-array String [subject]))
                    (.build))]
        (try
          (.addStream jsm cfg)
          (catch Exception _e2
            nil))))))

(defn- ensure-consumer!
  [^JetStreamManagement jsm stream durable subject]
  (let [cfg (-> (ConsumerConfiguration/builder)
                (.durable durable)
                (.filterSubject subject)
                (.ackPolicy AckPolicy/Explicit)
                (.deliverPolicy DeliverPolicy/All)
                (.build))]
    ;; idempotent-ish: updates if exists, creates if missing
    (.addOrUpdateConsumer jsm stream cfg)))

(defn- start-jetstream-subscription!
  [{:keys [subscription-id js jsm routing codec handler dead-letter stop? logger topic options]}]
  (future
    (let [options (or options {})
          subject (topic->subject routing topic)
          stream (topic->stream routing topic)
          durable (or (:durable options) (topic->durable routing topic))
          batch (or (:pull-batch options) 1)
          expires-ms (or (:expires-ms options) 1000)]
      (ensure-stream! ^JetStreamManagement jsm stream subject)
      (ensure-consumer! ^JetStreamManagement jsm stream durable subject)
      (let [pull-opts (PullSubscribeOptions/bind stream durable)
            ^JetStreamSubscription sub (.subscribe ^JetStream js subject pull-opts)]
        (logger/log logger :report ::jetstream-subscription-started
                    {:id subscription-id :topic topic :subject subject :stream stream :durable durable})
        (try
          (while (not @stop?)
            (let [msgs (.fetch sub (int batch) (Duration/ofMillis (long expires-ms)))]
              (doseq [^Message m msgs]
                (let [payload (.getData m)
                      envelope (codec/decode codec payload)]
                  (try
                    (handler envelope)
                    (.ack m)
                    (catch Exception e
                      (logger/log logger :error ::jetstream-handler-failed
                                  {:subscription-id subscription-id
                                   :topic topic
                                   :subject subject
                                   :error (.getMessage e)})
                      (if dead-letter
                        (let [dl-res (dl/send-dead-letter! dead-letter envelope
                                                           {:error (.getMessage e)
                                                            :stacktrace (with-out-str (.printStackTrace e))}
                                                           {})]
                          (if (:ok dl-res)
                            (do
                              (logger/log logger :info ::jetstream-dead-letter-success {:subscription-id subscription-id})
                              (.ack m))
                            (logger/log logger :error ::jetstream-dead-letter-failed
                                        {:subscription-id subscription-id :error (:error dl-res)})))
                        (logger/log logger :warn ::no-dlq-configured {:subscription-id subscription-id}))))))))
          (finally
            (try (.unsubscribe sub) (catch Exception _e nil))
            (logger/log logger :report ::jetstream-subscription-stopped {:id subscription-id})))))))

(defmethod ig/init-key :core-service.consumers.jetstream/runtime
  [_ {:keys [jetstream routing codec dead-letter logger]}]
  (let [stop? (atom false)
        subscriptions (-> routing :subscriptions (or {}))
        js-subs (into {}
                      (filter (fn [[_id sub]] (= :jetstream (:source sub))))
                      subscriptions)
        threads
        (into {}
              (map (fn [[subscription-id {:keys [topic handler options]
                                         :or {options {}}}]]
                     (let [topic (or topic :default)]
                       [subscription-id
                        (start-jetstream-subscription!
                          {:subscription-id subscription-id
                           :js (:js jetstream)
                           :jsm (:jsm jetstream)
                           :routing routing
                           :codec codec
                           :handler handler
                           :dead-letter dead-letter
                           :stop? stop?
                           :logger logger
                           :topic topic
                           :options options})])))
              js-subs)]
    {:stop? stop?
     :threads threads
     :logger logger}))

(defmethod ig/halt-key! :core-service.consumers.jetstream/runtime
  [_ {:keys [stop? threads logger]}]
  (logger/log logger :report ::halting-jetstream-runtime)
  (when stop?
    (reset! stop? true))
  (doseq [[_id thread] threads]
    (deref thread 1000 nil))
  nil)

