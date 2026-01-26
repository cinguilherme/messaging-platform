(ns core-service.app.server.routes
  (:refer-clojure :exclude [test])
  (:require [cheshire.core :as json]
            [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [d-core.libs.workers :as workers]
            [core-service.app.storage.minio :as minio]
            [d-core.core.producers.protocol :as producer]
            [d-core.core.cache.protocol :as cache]
            [d-core.core.storage.protocol :as storage]
            [d-core.core.messaging.dead-letter.admin.protocol :as dl-admin]
            [duct.logger :as logger]))

(defn- get-accept-format
  "Determines response format from Accept header or defaults to JSON"
  [req]
  (let [accept (get-in req [:headers "accept"] "application/json")]
    (cond
      (re-find #"application/edn" accept) :edn
      (re-find #"application/json" accept) :json
      :else :json)))

(defn- format-response
  "Formats response data based on requested format"
  [data format]
  (case format
    :edn {:status 200
          :headers {"content-type" "application/edn"}
          :body (pr-str data)}
    :json {:status 200
           :headers {"content-type" "application/json"}
           :body (json/generate-string data)}))

(defn index [_options]
  (fn [_req]
    (format-response {:ok true} (get-accept-format _req))))

(defn test [_options]
  (fn [_req]
    (try 
      (let [p (:producer _options)
            c (:cache _options)
            s (:storage _options)
            msg {:message "Hello, World!" :timestamp (System/currentTimeMillis)}
            cache-key "last-test-msg"
            storage-key (str "msg-" (:timestamp msg) ".json")

            ;; Cache operations
            cached-msg (cache/cache-lookup c cache-key {})
            newly-cached? (not cached-msg)
            _ (when newly-cached?
                (cache/cache-put c cache-key msg {}))

            
            valkey-msg-get (cache/cache-lookup c "valkey-test" {:cache :valkey})

            valkey-msg-put (when (nil? valkey-msg-get) 
                             (cache/cache-put c "valkey-test" msg {:cache :valkey}))

            ;; Storage operations
            storage-result (storage/storage-put s storage-key (json/generate-string msg) {})

            ack (producer/produce! p msg {:topic :default})
            ;; Produce to a topic that fails once
            
            ;; sample kafka and jetstream messages
            kafka-msg-ack (producer/produce! p {:type :kafka-test :msg "this is a kafka message"} {:topic :kafka-test})
            jetstream-msg-ack (producer/produce! p {:type :jetstream-test :msg "this is a jetstream message"} {:topic :jetstream-test})
            rabbitmq-msg-ack (producer/produce! p {:type :rabbitmq-test :msg "this is a rabbitmq message"} {:topic :rabbitmq-test})
            fail-ack (producer/produce! p {:type :fail-test :msg "this should fail once"} {:topic :to-fail})]
        (format-response
         {:ok true
          :msg msg
          :cached-msg cached-msg
          :valkey-msg-get valkey-msg-get
          :valkey-msg-put valkey-msg-put
          :ack ack
          :kafka-msg-ack kafka-msg-ack
          :jetstream-msg-ack jetstream-msg-ack
          :rabbitmq-msg-ack rabbitmq-msg-ack
          :fail-ack fail-ack
          :cached? (boolean cached-msg)
          :newly-cached? newly-cached?
          :storage-result storage-result}
         (get-accept-format _req)))
      (catch Exception e
        (format-response {:ok false :error (.getMessage e)} (get-accept-format _req))))))

(defn- param
  [req k]
  (or (get-in req [:query-params k])
      (get-in req [:params k])
      (get-in req [:params (keyword k)])))

(defn- parse-long
  [value default]
  (try
    (if (some? value)
      (Long/parseLong (str value))
      default)
    (catch Exception _
      default)))

(defn- slurp-bytes
  [input]
  (with-open [in (io/input-stream input)
              out (java.io.ByteArrayOutputStream.)]
    (io/copy in out)
    (.toByteArray out)))

(defn- normalize-content-type
  [content-type]
  (some-> content-type (str/split #";" 2) first str/trim))

(defn- read-upload
  [req]
  (let [params (or (:multipart-params req) (:params req) {})
        file-param (or (get params "image")
                       (get params :image)
                       (get params "file")
                       (get params :file))
        header-content-type (normalize-content-type (get-in req [:headers "content-type"]))
        multipart? (and header-content-type
                        (str/starts-with? header-content-type "multipart/"))]
    (cond
      (and (map? file-param) (:tempfile file-param))
      (let [bytes (slurp-bytes (:tempfile file-param))]
        {:bytes bytes
         :filename (:filename file-param)
         :content-type (normalize-content-type (:content-type file-param))
         :source :multipart})

      (and (:body req) (not multipart?))
      (let [bytes (slurp-bytes (:body req))]
        {:bytes bytes
         :filename (or (param req "filename") "upload")
         :content-type header-content-type
         :source :raw-body})

      :else nil)))

;; Instead of suppling infra specific information about deadletters, we can supply very generic identifiable information such as a deadletter ID (if we are using any storage that can support it) and we can supply a deadletter payload hash, something that is agnotistic of any storage requirement or not, 
;; a payload hash can be recovered from a log or even the raw payload if we are using a storage that can support it.
(defn dl-list
  "List DLQ items (transport/config agnostic).

  Query params:
  - topic: topic keyword (required; e.g. \"default\")
  - status: optional status filter (:eligible/:stuck/:poison/:manual)
  - limit: optional limit (default 50)"
  [{:keys [deadletter-admin]}]
  (fn [req]
    (let [format (get-accept-format req)
          topic-s (param req "topic")
          status-s (param req "status")
          limit (Long/parseLong (str (or (param req "limit") 50)))
          topic (when topic-s (keyword topic-s))
          status (when status-s (keyword status-s))]
      (if-not topic
        (format-response {:ok false :error "missing topic"} format)
        (format-response
          (dl-admin/list-deadletters deadletter-admin {:topic topic :status status :limit limit} {})
          format)))))

(defn dl-get
  "Get a DLQ item by dlq-id."
  [{:keys [deadletter-admin]}]
  (fn [req]
    (let [format (get-accept-format req)
          dlq-id (or (param req "dlq-id") (param req "id"))]
      (if-not dlq-id
        (format-response {:ok false :error "missing dlq-id"} format)
        (format-response (dl-admin/get-deadletter deadletter-admin dlq-id {}) format)))))

(defn dl-mark
  "Mark a DLQ item status by dlq-id.

  Params:
  - dlq-id (path/query)
  - status (required)"
  [{:keys [deadletter-admin]}]
  (fn [req]
    (let [format (get-accept-format req)
          dlq-id (or (param req "dlq-id") (param req "id"))
          status (some-> (param req "status") keyword)]
      (if (and dlq-id status)
        (format-response (dl-admin/mark-deadletter! deadletter-admin dlq-id status {}) format)
        (format-response {:ok false :error "missing dlq-id or status"} format)))))

(defn dl-replay
  "Replay a DLQ item by dlq-id (transport/config agnostic).

  Params:
  - dlq-id (path/query)
  - topic (optional override)"
  [{:keys [deadletter-admin]}]
  (fn [req]
    (let [format (get-accept-format req)
          dlq-id (or (param req "dlq-id") (param req "id"))
          topic (some-> (param req "topic") keyword)]
      (if-not dlq-id
        (format-response {:ok false :error "missing dlq-id"} format)
        (format-response (dl-admin/replay-deadletter! deadletter-admin dlq-id (cond-> {} topic (assoc :topic topic))) format)))))

(defn image-upload
  "Accepts an image upload. If it exceeds the max-bytes threshold, it is resized
  via the worker system before being stored in MinIO. The request blocks until
  processing completes."
  [{:keys [workers logger max-bytes max-dim timeout-ms]}]
  (fn [req]
    (let [format (get-accept-format req)
          upload (read-upload req)
          max-bytes (parse-long (param req "max-bytes") (or max-bytes 200000))
          max-dim (parse-long (param req "max-dim") (or max-dim 1024))
          timeout-ms (parse-long (param req "timeout-ms") timeout-ms)]
      (if-not upload
        (format-response {:ok false :error "missing image payload"} format)
        (let [{:keys [bytes filename content-type source]} upload
              byte-count (alength ^bytes bytes)]
          (if (zero? byte-count)
            (format-response {:ok false :error "empty image payload"} format)
            (let [worker-id (if (> byte-count max-bytes) :image-resize :image-store)
                  msg {:bytes bytes
                       :filename filename
                       :content-type content-type
                       :original-bytes byte-count
                       :request-id (str (java.util.UUID/randomUUID))
                       :meta {:upload-source source}}
                  msg (if (= worker-id :image-resize)
                        (assoc msg :max-dim max-dim)
                        msg)
                  reply-chan (workers/request! workers worker-id msg)
                  timeout? (and (some? timeout-ms) (pos? timeout-ms))]
              (if timeout?
                (let [timeout-chan (async/timeout timeout-ms)
                      [result ch] (async/alts!! [reply-chan timeout-chan])]
                  (if (= ch timeout-chan)
                    (do
                      (when logger
                        (logger/log logger :warn ::image-upload-timeout
                                    {:bytes byte-count
                                     :max-bytes max-bytes
                                     :max-dim max-dim
                                     :worker worker-id}))
                      (format-response {:ok false
                                        :error "processing timeout"
                                        :worker worker-id
                                        :timeout-ms timeout-ms}
                                       format))
                    (format-response
                     {:ok (= :stored (:status result))
                      :worker worker-id
                      :threshold-bytes max-bytes
                      :bytes byte-count
                      :content-type content-type
                      :result result}
                     format)))
                (let [result (async/<!! reply-chan)]
                  (format-response
                   {:ok (= :stored (:status result))
                    :worker worker-id
                    :threshold-bytes max-bytes
                    :bytes byte-count
                    :content-type content-type
                    :result result}
                   format))))))))))

(defn image-list
  "List images stored by the upload endpoint."
  [{:keys [minio logger]}]
  (fn [req]
    (let [format (get-accept-format req)
          prefix (or (param req "prefix") "images/")
          limit (parse-long (param req "limit") 50)
          token (param req "token")
          result (minio/list-objects minio {:prefix prefix
                                            :limit limit
                                            :token token})]
      (when (and logger (not (:ok result)))
        (logger/log logger :error ::image-list-failed {:error (:error result)}))
      (format-response result format))))
