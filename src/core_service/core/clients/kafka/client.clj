(ns core-service.core.clients.kafka.client
  (:import (org.apache.kafka.clients.producer KafkaProducer ProducerRecord RecordMetadata)
           (org.apache.kafka.clients.consumer KafkaConsumer ConsumerRecord)
           (org.apache.kafka.common.serialization ByteArraySerializer ByteArrayDeserializer)
           (org.apache.kafka.common.header.internals RecordHeaders)
           (java.time Duration)
           (java.util Properties UUID)
           (java.util.concurrent TimeUnit)))

(defrecord KafkaClient [bootstrap-servers ^KafkaProducer producer producer-config]
  Object
  (toString [_]
    (str "#KafkaClient{:bootstrap-servers " (pr-str bootstrap-servers) "}")))

(defrecord KafkaConsumerClient [bootstrap-servers group-id client-id ^KafkaConsumer consumer]
  Object
  (toString [_]
    (str "#KafkaConsumerClient{:bootstrap-servers " (pr-str bootstrap-servers)
         ", :group-id " (pr-str group-id)
         ", :client-id " (pr-str client-id) "}")))

(defn- ->string [x]
  (cond
    (nil? x) nil
    (keyword? x) (name x)
    :else (str x)))

(defn- normalize-prop-val [v]
  (cond
    (instance? Class v) (.getName ^Class v)
    (keyword? v) (name v)
    :else v))

(defn- ->props
  ^Properties
  [m]
  (let [^Properties p (Properties.)]
    (doseq [[k v] (or m {})]
      (.put p (->string k) (normalize-prop-val v)))
    p))

(defn make-client
  "Creates a Kafka client with a shared producer.

  Options:
  - :bootstrap-servers (string) default: \"localhost:29092\"
  - :producer-config (map) additional Kafka producer configs"
  [{:keys [bootstrap-servers producer-config]
    :or {bootstrap-servers "localhost:29092"
         producer-config {}}}]
  (let [cfg (merge {"bootstrap.servers" bootstrap-servers
                    "key.serializer" (.getName ByteArraySerializer)
                    "value.serializer" (.getName ByteArraySerializer)
                    ;; Default to safe semantics; callers can override in producer-config.
                    "acks" "all"
                    "enable.idempotence" "true"}
                   producer-config)
        producer (KafkaProducer. (->props cfg))]
    (->KafkaClient bootstrap-servers producer cfg)))

(defn close!
  [^KafkaClient client]
  (when-let [^KafkaProducer p (:producer client)]
    (try
      (.close p)
      (catch Exception _e
        nil))))

(defn- ->headers
  ^RecordHeaders
  [headers]
  (let [h (RecordHeaders.)]
    (doseq [[k v] (or headers {})]
      (let [k (->string k)
            vb (cond
                 (nil? v) nil
                 (bytes? v) v
                 :else (.getBytes (str v) "UTF-8"))]
        (when (and k vb)
          (.add h k ^bytes vb))))
    h))

(defn send!
  "Send a message using the shared producer.

  opts:
  - :topic (string, required)
  - :key (bytes, optional)
  - :value (bytes, required)
  - :headers (map, optional; values are coerced to UTF-8 bytes)
  - :timeout-ms (int, default 5000)

  Returns {:ok true ...metadata...}."
  [^KafkaClient client {:keys [topic key value headers timeout-ms]
                        :or {timeout-ms 5000}}]
  (when-not (string? topic)
    (throw (ex-info "Kafka send! requires :topic string" {:topic topic})))
  (when-not (bytes? value)
    (throw (ex-info "Kafka send! requires :value bytes" {:value-type (type value)})))
  (let [^KafkaProducer p (:producer client)
        headers-obj (->headers headers)
        rec (ProducerRecord. ^String topic nil nil ^bytes key ^bytes value headers-obj)
        fut (.send p rec)
        ^RecordMetadata md (.get fut (long timeout-ms) TimeUnit/MILLISECONDS)]
    {:ok true
     :backend :kafka
     :topic (.topic md)
     :partition (.partition md)
     :offset (.offset md)
     :timestamp (.timestamp md)}))

(defn make-consumer
  "Creates a consumer (caller owns it; call close-consumer!).

  opts:
  - :group-id (string, required)
  - :client-id (string, default random)
  - :auto-offset-reset (\"earliest\"|\"latest\", default \"earliest\")
  - :enable-auto-commit? (boolean, default false)
  - :consumer-config (map) additional Kafka consumer configs"
  [^KafkaClient client {:keys [group-id client-id auto-offset-reset enable-auto-commit? consumer-config]
                        :or {auto-offset-reset "earliest"
                             enable-auto-commit? false
                             consumer-config {}}}]
  (when-not (string? group-id)
    (throw (ex-info "Kafka make-consumer requires :group-id string" {:group-id group-id})))
  (let [client-id (or client-id (str "core-service-" (UUID/randomUUID)))
        cfg (merge {"bootstrap.servers" (:bootstrap-servers client)
                    "group.id" group-id
                    "client.id" client-id
                    "enable.auto.commit" (str (boolean enable-auto-commit?))
                    "auto.offset.reset" auto-offset-reset
                    "key.deserializer" (.getName ByteArrayDeserializer)
                    "value.deserializer" (.getName ByteArrayDeserializer)
                    ;; Keep it simple and low-latency for runtime polling loops.
                    "max.poll.records" "10"}
                   consumer-config)
        consumer (KafkaConsumer. (->props cfg))]
    (->KafkaConsumerClient (:bootstrap-servers client) group-id client-id consumer)))

(defn close-consumer!
  [^KafkaConsumerClient cc]
  (when-let [^KafkaConsumer c (:consumer cc)]
    (try
      (.close c)
      (catch Exception _e
        nil))))

(defn subscribe!
  [^KafkaConsumerClient cc topics]
  (let [^KafkaConsumer c (:consumer cc)
        topics (mapv ->string topics)]
    (.subscribe c topics)
    cc))

(defn poll!
  "Poll for records, returning a vector of {:topic :partition :offset :key :value :timestamp :headers}."
  [^KafkaConsumerClient cc {:keys [timeout-ms] :or {timeout-ms 250}}]
  (let [^KafkaConsumer c (:consumer cc)
        records (.poll c (Duration/ofMillis (long timeout-ms)))]
    (reduce
      (fn [acc ^ConsumerRecord r]
        (conj acc {:topic (.topic r)
                   :partition (.partition r)
                   :offset (.offset r)
                   :timestamp (.timestamp r)
                   :key (.key r)
                   :value (.value r)
                   :headers (into {}
                                  (map (fn [h] [(.key h) (.value h)]))
                                  (.headers r))}))
      []
      records)))

(defn commit!
  "Commit offsets synchronously."
  [^KafkaConsumerClient cc]
  (let [^KafkaConsumer c (:consumer cc)]
    (.commitSync c)
    {:ok true}))

