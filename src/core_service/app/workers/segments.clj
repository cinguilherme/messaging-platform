(ns core-service.app.workers.segments
  (:require [cheshire.core :as json]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [core-service.app.db.segments :as segments-db]
            [core-service.app.metrics :as app-metrics]
            [core-service.app.libs.redis :as redis-lib]
            [core-service.app.segments.format :as segment-format]
            [core-service.app.storage.minio :as minio]
            [core-service.app.streams.redis :as streams]
            [d-core.core.metrics.protocol :as metrics]
            [d-core.libs.workers :as workers]
            [duct.logger :as logger]
            [integrant.core :as ig]
            [taoensso.carmine :as car]))

(defn- scan-keys
  [redis-client metrics pattern]
  (loop [cursor "0"
         acc []]
    (let [[next-cursor keys] (app-metrics/with-redis metrics :scan
                              #(car/wcar (redis-lib/conn redis-client)
                                 (car/scan cursor "MATCH" pattern "COUNT" 200)))
          keys (mapv redis-lib/normalize-key keys)
          acc (into acc keys)]
      (if (= "0" next-cursor)
        acc
        (recur next-cursor acc)))))

(defn- parse-conversation-id
  [stream-key prefix]
  (let [suffix (subs stream-key (count prefix))]
    (try
      (java.util.UUID/fromString suffix)
      (catch Exception _
        nil))))

(defn- cursor-key
  [naming conv-id]
  (str (get-in naming [:redis :flush-prefix] "chat:flush:") conv-id))

(defn- get-cursor
  [redis-client metrics key]
  (app-metrics/with-redis metrics :get
    #(car/wcar (redis-lib/conn redis-client)
       (car/get key))))

(defn- set-cursor!
  [redis-client metrics key cursor]
  (app-metrics/with-redis metrics :set
    #(car/wcar (redis-lib/conn redis-client)
       (car/set key cursor))))

(defn- decode-message
  [payload]
  (cond
    (bytes? payload) (edn/read-string (String. ^bytes payload "UTF-8"))
    (string? payload) (edn/read-string payload)
    :else nil))

(defn- record-size
  [^bytes payload]
  (+ 4 (alength payload)))

(defn- header-bytes
  [header]
  (.getBytes (json/generate-string header) "UTF-8"))

(defn- trim-to-fit
  [entries header max-bytes]
  (let [header-size (record-size (header-bytes header))]
    (loop [selected (vec entries)
           size (+ header-size (reduce + (map :record-size entries)))]
      (cond
        (empty? selected) []
        (<= size max-bytes) selected
        :else (let [last-entry (peek selected)
                    selected' (pop selected)
                    size' (- size (:record-size last-entry))]
                (recur selected' size'))))))

(defn- build-object-key
  [naming {:keys [conversation-id seq-start seq-end created-at compression]}]
  (let [prefix (get-in naming [:minio :segments-prefix] "segments/")
        prefix (if (str/ends-with? prefix "/") prefix (str prefix "/"))
        ext (if (= compression :gzip) ".seg.gz" ".seg")]
    (str prefix conversation-id "/" seq-start "-" seq-end "-" created-at ext)))

(defn- parse-stream-id
  [id]
  (when (and id (string? id) (str/includes? id "-"))
    (let [[ms seq] (str/split id #"-" 2)]
      [(Long/parseLong ms) (Long/parseLong (or seq "0"))])))

(defn- stream-id<=?
  [a b]
  (let [[ams aseq] (parse-stream-id a)
        [bms bseq] (parse-stream-id b)]
    (or (< ams bms)
        (and (= ams bms) (<= aseq bseq)))))

(defn- retention-trim-id
  [redis metrics stream last-id trim-min-entries]
  (let [trim-min-entries (long (or trim-min-entries 0))]
    (if (<= trim-min-entries 0)
      last-id
      (let [entries (app-metrics/with-redis metrics :xrevrange
                       #(car/wcar (redis-lib/conn redis)
                          (car/xrevrange stream "+" "-" "COUNT" trim-min-entries)))
            retain-id (some-> entries last first)]
        (if (and retain-id (stream-id<=? retain-id last-id))
          retain-id
          last-id)))))

(defn- entry->prepared [entries codec last-seq]
  (->> entries
       (map (fn [{:keys [id payload] :as entry}]
              (let [msg (decode-message payload)
                    payload-bytes (case codec
                                    :json (when msg
                                            (.getBytes (json/generate-string msg) "UTF-8"))
                                    :edn (cond
                                           (bytes? payload) payload
                                           (string? payload) (.getBytes ^String payload "UTF-8")
                                           msg (.getBytes (pr-str msg) "UTF-8")
                                           :else nil)
                                    :raw (cond
                                           (bytes? payload) payload
                                           (string? payload) (.getBytes ^String payload "UTF-8")
                                           :else nil)
                                    nil)]
                (when (and payload-bytes msg (:seq msg))
                  (assoc entry
                         :id id
                         :payload payload-bytes
                         :message msg
                         :seq (long (:seq msg))
                         :record-size (record-size payload-bytes))))))
       (remove nil?)
       (filter (fn [{:keys [seq]}] (> seq last-seq)))
       vec))

(defn- read-prepared
  [redis metrics stream batch-size cursor codec last-seq]
  (let [{:keys [entries]} (streams/read! redis metrics stream {:direction :forward
                                                               :limit batch-size
                                                               :cursor cursor})
        prepared (entry->prepared entries codec last-seq)]
    {:entries entries
     :prepared prepared}))

(defn- build-header
  [conversation-id codec compression prepared created-at]
  {:format_version 1
   :codec (name codec)
   :compression (name compression)
   :conversation_id (str conversation-id)
   :seq_start (:seq (first prepared))
   :seq_end (:seq (last prepared))
   :message_count (count prepared)
   :created_at created-at})

(defn- store-segment!
  [{:keys [db minio naming logger]} {:keys [conversation-id header payloads compression created-at]}]
  (let [segment-bytes (segment-format/encode-segment {:header header
                                                      :messages payloads
                                                      :compression compression})
        object-key (build-object-key naming {:conversation-id conversation-id
                                             :seq-start (:seq_start header)
                                             :seq-end (:seq_end header)
                                             :created-at created-at
                                             :compression compression})
        store (minio/put-bytes! minio object-key segment-bytes "application/octet-stream")
        byte-size (alength ^bytes segment-bytes)]
    (if-not (:ok store)
      (do
        (when logger
          (logger/log logger :error ::segment-store-failed
                      {:conversation-id conversation-id
                       :error (:error store)
                       :object-key object-key}))
        {:status :error :reason :minio-failed :conversation-id conversation-id})
      (do
        (segments-db/insert-segment! db {:conversation-id conversation-id
                                         :seq-start (:seq_start header)
                                         :seq-end (:seq_end header)
                                         :object-key object-key
                                         :byte-size byte-size})
        {:status :ok
         :conversation-id conversation-id
         :seq-start (:seq_start header)
         :seq-end (:seq_end header)
         :object-key object-key
         :byte_size byte-size}))))

(defn- trim-stream!
  [redis metrics stream last-id trim-min-entries]
  (when-let [trim-id (retention-trim-id redis metrics stream last-id trim-min-entries)]
    (when (and trim-id (not (str/blank? trim-id)))
      (app-metrics/with-redis metrics :xtrim
        #(car/wcar (redis-lib/conn redis)
           (car/xtrim stream "MINID" trim-id))))))

(defn- commit-flush-conversation!
  [{:keys [db redis minio naming logger metrics]}
   {:keys [conversation-id prepared created-at
           codec compression max-bytes
           cursor-k stream trim-stream?
           trim-min-entries]}]
  (let [header (build-header conversation-id codec compression prepared created-at)
        selected (trim-to-fit prepared header max-bytes)]
    (if (empty? selected)
      {:status :skipped :reason :segment-too-small :conversation-id conversation-id}
      (let [header' (assoc header
                           :seq_start (:seq (first selected))
                           :seq_end (:seq (last selected))
                           :message_count (count selected))
            payloads (mapv :payload selected)
            result (store-segment! {:db db
                                    :minio minio
                                    :naming naming
                                    :logger logger}
                                   {:conversation-id conversation-id
                                    :header header'
                                    :payloads payloads
                                    :compression compression
                                    :created-at created-at})]
        (if-not (= :ok (:status result))
          result
          (do
            (when-let [last-id (:id (last selected))]
              (set-cursor! redis metrics cursor-k last-id))
            (when trim-stream?
              (when-let [last-id (:id (last selected))]
                (trim-stream! redis metrics stream last-id trim-min-entries)))
            (assoc result :message-count (:message_count header'))))))))

(defn- record-flush-metrics!
  [metrics-component result duration-seconds]
  (when (and metrics-component (:metrics metrics-component))
    (let [metrics-api (:metrics metrics-component)
          segment-metrics (:segments metrics-component)
          status (name (:status result))
          byte-size (or (:byte-size result) (:byte_size result))
          message-count (:message-count result)]
      (when-let [counter (:flush-total segment-metrics)]
        (metrics/inc! metrics-api
                      (.labels counter (app-metrics/labels->array status))))
      (when-let [hist (:flush-duration segment-metrics)]
        (metrics/observe! metrics-api hist duration-seconds))
      (when (and (= :ok (:status result)) byte-size)
        (when-let [hist (:segment-bytes segment-metrics)]
          (metrics/observe! metrics-api hist byte-size)))
      (when (and (= :ok (:status result)) message-count)
        (when-let [hist (:segment-messages segment-metrics)]
          (metrics/observe! metrics-api hist message-count))))))

(defn flush-conversation!
  [{:keys [db redis naming segments metrics] :as components} conversation-id]
  (let [stream-prefix (get-in naming [:redis :stream-prefix] "chat:conv:")
        stream (str stream-prefix conversation-id)
        cursor-k (cursor-key naming conversation-id)
        last-cursor (get-cursor redis metrics cursor-k)
        last-seq (segments-db/last-seq-end db conversation-id)
        last-seq (long (or last-seq -1))
        {:keys [max-bytes batch-size compression codec trim-stream? trim-min-entries]} segments
        batch-size (long (or batch-size 200))
        max-bytes (long (or max-bytes 262144))
        compression (or compression :gzip)
        codec (or codec :edn)
        start (System/nanoTime)
        {:keys [entries prepared]} (read-prepared redis metrics stream batch-size last-cursor codec last-seq)
        result (cond
                 (empty? entries)
                 {:status :empty :conversation-id conversation-id}

                 (empty? prepared)
                 (do
                   (when-let [last-id (:id (last entries))]
                     (set-cursor! redis metrics cursor-k last-id))
                   {:status :skipped :reason :no-new-seq :conversation-id conversation-id})

                 :else
                 (commit-flush-conversation!
                  components
                  {:conversation-id conversation-id
                   :prepared prepared
                   :created-at (System/currentTimeMillis)
                   :codec codec
                   :compression compression
                   :max-bytes max-bytes
                   :cursor-k cursor-k
                   :stream stream
                   :trim-stream? trim-stream?
                   :trim-min-entries trim-min-entries}))
        duration (/ (double (- (System/nanoTime) start)) 1000000000.0)]
    (record-flush-metrics! metrics result duration)
    result))

(defn flush-all!
  [{:keys [redis naming metrics] :as components}]
  (let [prefix (get-in naming [:redis :stream-prefix] "chat:conv:")
        keys (scan-keys redis metrics (str prefix "*"))]
    (mapv (fn [stream-key]
            (if-let [conv-id (parse-conversation-id stream-key prefix)]
              (flush-conversation! components conv-id)
              {:status :skipped :reason :invalid-stream-key :stream stream-key}))
          keys)))

(defn segment-flush-worker
  [{:keys [components]} _msg]
  (let [result (flush-all! components)]
    (when-let [log (:logger components)]
      (logger/log log :info ::flush-result {:count (count result)}))
    result))

(def default-definition
  {:channels {:segments/flush-ticks {:buffer 1}
              :workers/errors {:buffer 4}}
   :workers {:segments-ticker {:kind :ticker
                               :interval-ms 300000
                               :out :segments/flush-ticks
                               :dispatch :go}
             :segments-flush {:kind :command
                              :in :segments/flush-ticks
                              :worker-fn segment-flush-worker
                              :dispatch :thread
                              :fail-chan :workers/errors}}})

(defn- apply-flush-interval
  [definition interval-ms]
  (if (some? interval-ms)
    (assoc-in definition [:workers :segments-ticker :interval-ms] interval-ms)
    definition))

(defmethod ig/init-key :core-service.app.workers.segments/system
  [_ {:keys [logger db redis minio naming segments definition metrics]}]
  (let [segments (merge {:max-bytes 262144
                         :flush-interval-ms 300000
                         :compression :gzip
                         :codec :edn
                         :batch-size 200
                         :trim-stream? false}
                        segments)
        definition (-> (or definition default-definition)
                       (apply-flush-interval (:flush-interval-ms segments)))]
    (logger/log logger :info ::initializing-segment-workers
                {:channels (keys (:channels definition))
                 :workers (keys (:workers definition))})
    (workers/start-workers definition {:logger logger
                                       :metrics metrics
                                       :observability metrics
                                       :db db
                                       :redis redis
                                       :minio minio
                                       :naming naming
                                       :segments segments})))

(defmethod ig/halt-key! :core-service.app.workers.segments/system
  [_ system]
  (when-let [stop! (:stop! system)]
    (stop!)))
