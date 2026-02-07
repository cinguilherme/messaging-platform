(ns core-service.app.server.routes
  (:refer-clojure :exclude [test])
  (:require [cheshire.core :as json]
            [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [d-core.libs.workers :as workers]
            [core-service.app.storage.minio :as minio]
            [core-service.app.server.http :as http]
            [d-core.core.producers.protocol :as producer]
            [d-core.core.cache.protocol :as cache]
            [d-core.core.storage.protocol :as storage]
            [d-core.core.messaging.dead-letter.admin.protocol :as dl-admin]
            [integrant.core :as ig]
            [duct.logger :as logger]))

(defn index [_options]
  (fn [_req]
    (http/format-response {:ok true} (http/get-accept-format _req))))

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
            
            fail-ack (producer/produce! p {:type :fail-test :msg "this should fail once"} {:topic :to-fail})]
        (http/format-response
         {:ok true
          :msg msg
          :cached-msg cached-msg
          :valkey-msg-get valkey-msg-get
          :valkey-msg-put valkey-msg-put
          :ack ack
          :fail-ack fail-ack
          :cached? (boolean cached-msg)
          :newly-cached? newly-cached?
          :storage-result storage-result}
         (http/get-accept-format _req)))
      (catch Exception e
        (http/format-response {:ok false :error (.getMessage e)} (http/get-accept-format _req))))))

(defn- slurp-bytes
  [input]
  (with-open [in (io/input-stream input)
              out (java.io.ByteArrayOutputStream.)]
    (io/copy in out)
    (.toByteArray out)))

(defn- read-upload
  [req]
  (let [params (or (:multipart-params req) (:params req) {})
        file-param (or (get params "image")
                       (get params :image)
                       (get params "file")
                       (get params :file))
        header-content-type (http/normalize-content-type (get-in req [:headers "content-type"]))
        multipart? (and header-content-type
                        (str/starts-with? header-content-type "multipart/"))]
    (cond
      (and (map? file-param) (:tempfile file-param))
      (let [bytes (slurp-bytes (:tempfile file-param))]
        {:bytes bytes
         :filename (:filename file-param)
         :content-type (http/normalize-content-type (:content-type file-param))
         :source :multipart})

      (and (:body req) (not multipart?))
      (let [bytes (slurp-bytes (:body req))]
        {:bytes bytes
         :filename (or (http/param req "filename") "upload")
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
    (let [format (http/get-accept-format req)
          topic-s (http/param req "topic")
          status-s (http/param req "status")
          limit (Long/parseLong (str (or (http/param req "limit") 50)))
          topic (when topic-s (keyword topic-s))
          status (when status-s (keyword status-s))]
      (if-not topic
        (http/format-response {:ok false :error "missing topic"} format)
        (http/format-response
          (dl-admin/list-deadletters deadletter-admin {:topic topic :status status :limit limit} {})
          format)))))

(defn dl-get
  "Get a DLQ item by dlq-id."
  [{:keys [deadletter-admin]}]
  (fn [req]
    (let [format (http/get-accept-format req)
          dlq-id (or (http/param req "dlq-id") (http/param req "id"))]
      (if-not dlq-id
        (http/format-response {:ok false :error "missing dlq-id"} format)
        (http/format-response (dl-admin/get-deadletter deadletter-admin dlq-id {}) format)))))

(defn dl-mark
  "Mark a DLQ item status by dlq-id.

  Params:
  - dlq-id (path/query)
  - status (required)"
  [{:keys [deadletter-admin]}]
  (fn [req]
    (let [format (http/get-accept-format req)
          dlq-id (or (http/param req "dlq-id") (http/param req "id"))
          status (some-> (http/param req "status") keyword)]
      (if (and dlq-id status)
        (http/format-response (dl-admin/mark-deadletter! deadletter-admin dlq-id status {}) format)
        (http/format-response {:ok false :error "missing dlq-id or status"} format)))))

(defn dl-replay
  "Replay a DLQ item by dlq-id (transport/config agnostic).

  Params:
  - dlq-id (path/query)
  - topic (optional override)"
  [{:keys [deadletter-admin]}]
  (fn [req]
    (let [format (http/get-accept-format req)
          dlq-id (or (http/param req "dlq-id") (http/param req "id"))
          topic (some-> (http/param req "topic") keyword)]
      (if-not dlq-id
        (http/format-response {:ok false :error "missing dlq-id"} format)
        (http/format-response (dl-admin/replay-deadletter! deadletter-admin dlq-id (cond-> {} topic (assoc :topic topic))) format)))))

(defn image-upload
  "Accepts an image upload. If it exceeds the max-bytes threshold, it is resized
  via the worker system before being stored in MinIO. The request blocks until
  processing completes."
  [{:keys [workers logger max-bytes max-dim timeout-ms]}]
  (fn [req]
    (let [format (http/get-accept-format req)
          upload (read-upload req)
          max-bytes (http/parse-long (http/param req "max-bytes") (or max-bytes 200000))
          max-dim (http/parse-long (http/param req "max-dim") (or max-dim 1024))
          timeout-ms (http/parse-long (http/param req "timeout-ms") timeout-ms)]
      (if-not upload
        (http/format-response {:ok false :error "missing image payload"} format)
        (let [{:keys [bytes filename content-type source]} upload
              byte-count (alength ^bytes bytes)]
          (if (zero? byte-count)
            (http/format-response {:ok false :error "empty image payload"} format)
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
                      (http/format-response {:ok false
                                        :error "processing timeout"
                                        :worker worker-id
                                        :timeout-ms timeout-ms}
                                       format))
                    (http/format-response
                     {:ok (= :stored (:status result))
                      :worker worker-id
                      :threshold-bytes max-bytes
                      :bytes byte-count
                      :content-type content-type
                      :result result}
                     format)))
                (let [result (async/<!! reply-chan)]
                  (http/format-response
                   {:ok (= :stored (:status result))
                    :worker worker-id
                    :threshold-bytes max-bytes
                    :bytes byte-count
                    :content-type content-type
                    :result result}
                   format))))))))))

(defn image-list
  "List images stored by the upload endpoint."
  [{:keys [minio logger metrics]}]
  (fn [req]
    (let [format (http/get-accept-format req)
          prefix (or (http/param req "prefix") "images/")
          limit (http/parse-long (http/param req "limit") 50)
          token (http/param req "token")
          result (minio/list-objects {:storage minio :metrics metrics}
                                     {:prefix prefix
                                      :limit limit
                                      :token token})]
      (when (and logger (not (:ok result)))
        (logger/log logger :error ::image-list-failed {:error (:error result)}))
      (http/format-response result format))))

(defmethod ig/init-key :core-service.app.server.routes/main
  [_ {:keys [index test image-upload image-list metrics]}]
  [["/" {:get index}]
   ["/test" {:get test}]
   ["/test/image-upload" {:post image-upload}]
   ["/test/image-upload/list" {:get image-list}]
   ["/metrics" {:get metrics}]])

(defmethod ig/init-key :core-service.app.server.routes/admin
  [_ {:keys [dl-list dl-get dl-replay dl-mark]}]
  [["/admin/dl" {:get dl-list}]
   ["/admin/dl/:dlq-id" {:get dl-get}]
   ["/admin/dl/:dlq-id/replay" {:post dl-replay}]
   ["/admin/dl/:dlq-id/mark" {:post dl-mark}]])
