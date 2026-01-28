(ns core-service.app.workers.segments
  (:require [cheshire.core :as json]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [core-service.app.db.segments :as segments-db]
            [core-service.app.metrics :as app-metrics]
            [core-service.app.libs.redis :as redis-lib]
            [core-service.app.observability.logging :as obs-log]
            [core-service.app.segments.format :as segment-format]
            [core-service.app.storage.minio :as minio]
            [core-service.app.streams.redis :as streams]
            [d-core.core.metrics.protocol :as metrics]
            [d-core.libs.workers :as workers]
            [duct.logger :as logger]
            [integrant.core :as ig]
            [taoensso.carmine :as car]))

(def segment-component :segment-flush)
(def segment-worker :segments-flush)

(defonce tick-log-state (atom {}))

(defn- now-ms []
  (System/currentTimeMillis))

(defn- duration-ms
  [start-nanos]
  (long (/ (double (- (System/nanoTime) start-nanos)) 1000000.0)))

(defn- should-log-tick?
  [logging component now-ms]
  (let [interval-ms (obs-log/tick-interval-ms logging)]
    (if (<= interval-ms 0)
      true
      (let [last-ms (get @tick-log-state component 0)]
        (when (>= (- now-ms last-ms) interval-ms)
          (swap! tick-log-state assoc component now-ms)
          true)))))

(defn- log-stage!
  [logger logging log-ctx level stage fields]
  (when (obs-log/stage-logs-enabled? logging segment-component)
    (obs-log/log! logger logging level ::segment-flush-stage
                  (merge log-ctx {:stage stage} fields))))

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
  [{:keys [db minio naming logger logging log-ctx]}
   {:keys [conversation-id header payloads compression created-at]}]
  (let [encode-start (System/nanoTime)
        segment-bytes (segment-format/encode-segment {:header header
                                                      :messages payloads
                                                      :compression compression})
        encode-duration (duration-ms encode-start)
        object-key (build-object-key naming {:conversation-id conversation-id
                                             :seq-start (:seq_start header)
                                             :seq-end (:seq_end header)
                                             :created-at created-at
                                             :compression compression})
        byte-size (alength ^bytes segment-bytes)
        segment-id (str (:seq_start header) "-" (:seq_end header))
        log-ctx (merge log-ctx
                       {:segment-id segment-id
                        :seq-start (:seq_start header)
                        :seq-end (:seq_end header)
                        :message-count (:message_count header)})]
    (log-stage! logger logging log-ctx :debug :encode
                {:duration-ms encode-duration
                 :payload-bytes byte-size})
    (let [put-start (System/nanoTime)
          store (minio/put-bytes! minio object-key segment-bytes "application/octet-stream")
          put-duration (duration-ms put-start)]
      (if-not (:ok store)
        (do
          (log-stage! logger logging log-ctx :error :minio-put
                      {:duration-ms put-duration
                       :storage.bucket (:bucket minio)
                       :storage.key object-key
                       :payload-bytes byte-size
                       :error (:error store)})
          (when logger
            (logger/log logger :error ::segment-store-failed
                        {:conversation-id conversation-id
                         :error (:error store)
                         :object-key object-key}))
          {:status :error :reason :minio-failed :conversation-id conversation-id})
        (do
          (log-stage! logger logging log-ctx :info :minio-put
                      {:duration-ms put-duration
                       :storage.bucket (:bucket minio)
                       :storage.key object-key
                       :payload-bytes byte-size})
          (let [index-start (System/nanoTime)]
            (try
              (segments-db/insert-segment! db {:conversation-id conversation-id
                                               :seq-start (:seq_start header)
                                               :seq-end (:seq_end header)
                                               :object-key object-key
                                               :byte-size byte-size})
              (log-stage! logger logging log-ctx :info :index-write
                          {:duration-ms (duration-ms index-start)
                           :pg.table :segment_index
                           :pg.operation :insert
                           :pg.rows 1})
              {:status :ok
               :conversation-id conversation-id
               :segment-id segment-id
               :seq-start (:seq_start header)
               :seq-end (:seq_end header)
               :object-key object-key
               :byte_size byte-size}
              (catch Exception e
                (log-stage! logger logging log-ctx :error :index-write
                            {:duration-ms (duration-ms index-start)
                             :pg.table :segment_index
                             :pg.operation :insert
                             :pg.rows 1
                             :error (.getMessage e)})
                (throw e)))))))))

(defn- trim-stream!
  [redis metrics stream last-id trim-min-entries]
  (when-let [trim-id (retention-trim-id redis metrics stream last-id trim-min-entries)]
    (when (and trim-id (not (str/blank? trim-id)))
      (app-metrics/with-redis metrics :xtrim
        #(car/wcar (redis-lib/conn redis)
           (car/xtrim stream "MINID" trim-id))))))

(defn- commit-flush-conversation!
  [{:keys [db redis minio naming logger metrics logging]}
   {:keys [conversation-id prepared created-at
           codec compression max-bytes
           cursor-k stream trim-stream?
           trim-min-entries log-ctx]}]
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
                                    :logger logger
                                    :logging logging
                                    :log-ctx log-ctx}
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

(defn- result->log-fields
  [result]
  (let [byte-size (or (:byte-size result) (:byte_size result))]
    (cond-> (select-keys result [:status :reason :conversation-id :segment-id
                                 :seq-start :seq-end :object-key :message-count])
      byte-size (assoc :payload-bytes byte-size))))

(defn flush-conversation!
  ([components conversation-id]
   (flush-conversation! components conversation-id nil))
  ([{:keys [db redis naming segments metrics logger logging] :as components} conversation-id log-ctx]
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
         log-ctx (merge {:component segment-component
                         :worker segment-worker
                         :conversation-id conversation-id}
                        log-ctx)
         start (System/nanoTime)
         {:keys [entries prepared]} (read-prepared redis metrics stream batch-size last-cursor codec last-seq)
         prepared-bytes (reduce + 0 (map :record-size prepared))
         _ (log-stage! logger logging log-ctx :debug :buffer-snapshot
                       {:entries-count (count entries)
                        :message-count (count prepared)
                        :payload-bytes prepared-bytes})
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
                    :trim-min-entries trim-min-entries
                    :log-ctx log-ctx}))
         duration-seconds (/ (double (- (System/nanoTime) start)) 1000000000.0)
         duration-ms (duration-ms start)]
     (record-flush-metrics! metrics result duration-seconds)
     (obs-log/log! logger logging :debug ::segment-flush-result
                   (merge log-ctx (result->log-fields result) {:duration-ms duration-ms}))
     result)))

(defn flush-all!
  ([components]
   (flush-all! components nil))
  ([{:keys [redis naming metrics logger logging] :as components} log-ctx]
   (let [prefix (get-in naming [:redis :stream-prefix] "chat:conv:")
         scan-start (System/nanoTime)
         keys (scan-keys redis metrics (str prefix "*"))
         scan-duration (duration-ms scan-start)
         log-ctx (merge {:component segment-component
                         :worker segment-worker}
                        log-ctx)]
     (obs-log/log! logger logging :debug ::segment-flush-scan
                   (merge log-ctx {:streams-count (count keys)
                                   :duration-ms scan-duration}))
     (mapv (fn [stream-key]
             (if-let [conv-id (parse-conversation-id stream-key prefix)]
               (flush-conversation! components conv-id log-ctx)
               (do
                 (obs-log/log! logger logging :debug ::segment-flush-skip
                               (merge log-ctx {:reason :invalid-stream-key
                                               :stream stream-key}))
                 {:status :skipped :reason :invalid-stream-key :stream stream-key})))
           keys))))

(defn- summarize-results
  [results]
  (let [counts (frequencies (map :status results))]
    {:result-count (count results)
     :ok-count (get counts :ok 0)
     :skipped-count (get counts :skipped 0)
     :empty-count (get counts :empty 0)
     :error-count (get counts :error 0)}))

(defn segment-flush-worker
  [{:keys [components]} _msg]
  (let [{:keys [logger logging]} components
        tick-id (str (java.util.UUID/randomUUID))
        tick-started-at (now-ms)
        log-ctx {:component segment-component
                 :worker segment-worker
                 :tick-id tick-id
                 :tick-started-at tick-started-at}]
    (when (and (obs-log/tick-enabled? logging)
               (should-log-tick? logging segment-component tick-started-at))
      (obs-log/log! logger logging :debug ::segment-flush-tick-start log-ctx))
    (try
      (let [result (flush-all! components log-ctx)
            tick-ended-at (now-ms)
            duration-ms (- tick-ended-at tick-started-at)
            summary (summarize-results result)
            success? (zero? (:error-count summary))]
        (obs-log/log! logger logging :info ::segment-flush-tick-complete
                      (merge log-ctx summary
                             {:tick-ended-at tick-ended-at
                              :duration-ms duration-ms
                              :success success?}))
        result)
      (catch Exception e
        (obs-log/log! logger logging :error ::segment-flush-tick-failed
                      (merge log-ctx
                             {:duration-ms (- (now-ms) tick-started-at)
                              :error (.getMessage e)}))
        (throw e)))))

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
  [_ {:keys [logger db redis minio naming segments definition metrics logging]}]
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
                                       :logging logging
                                       :db db
                                       :redis redis
                                       :minio minio
                                       :naming naming
                                       :segments segments})))

(defmethod ig/halt-key! :core-service.app.workers.segments/system
  [_ system]
  (when-let [stop! (:stop! system)]
    (stop!)))
