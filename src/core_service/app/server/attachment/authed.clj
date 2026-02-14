(ns core-service.app.server.attachment.authed
  (:require [clojure.string :as str]
            [core-service.app.db.attachments :as attachments-db]
            [core-service.app.db.conversations :as conversations-db]
            [core-service.app.schemas.messaging :as msg-schema]
            [core-service.app.server.attachment.logic :as logic]
            [core-service.app.server.http :as http]
            [core-service.app.server.message.logic :as message-logic]
            [d-core.core.storage.protocol :as p-storage]
            [d-core.libs.workers :as workers]
            [malli.core :as m]
            [malli.error :as me])
  (:import (java.time ZoneOffset)
           (java.time.format DateTimeFormatter)))

(def ^:private default-attachment-max-age-ms 2592000000)

(defn- attachment-max-age-ms
  [attachment-retention]
  (let [configured (some-> (:max-age-ms attachment-retention) long)]
    (if (and configured (pos? configured))
      configured
      default-attachment-max-age-ms)))

(defn- attachment-row-expired?
  [row now-ms]
  (let [expires-at (:expires_at row)]
    (and expires-at
         (<= (.getTime ^java.util.Date expires-at) now-ms))))

(defn- missing-object?
  [result]
  (let [error-type (:error-type result)
        error-code (:error-code result)
        status (:status result)]
    (or (= error-type :not-found)
        (= error-code "NoSuchKey")
        (= error-code "NotFound")
        (= 404 status))))

(defn- format-http-date
  [value]
  (when (instance? java.util.Date value)
    (.format DateTimeFormatter/RFC_1123_DATE_TIME
             (.atZone (.toInstant ^java.util.Date value) ZoneOffset/UTC))))

(defn- render-response
  [payload format]
  (cond
    (and (map? payload)
         (contains? payload :status)
         (map? (:body payload)))
    (let [formatted (http/format-response (:body payload) format)]
      (-> payload
          (assoc :body (:body formatted))
          (update :headers merge (:headers formatted))))

    (and (map? payload)
         (contains? payload :status))
    payload

    :else
    (http/format-response payload format)))

(defn- enqueue-new-attachment!
  [attachment-workers attachment-id conv-id object-key bytes attachment]
  (try
    {:accepted?
     (workers/command! attachment-workers
                       :attachment-orchestrator
                       {:attachment-id attachment-id
                        :conversation-id conv-id
                        :object-key object-key
                        :bytes bytes
                        :size-bytes (:size_bytes attachment)
                        :mime-type (:mime_type attachment)
                        :kind (get-in attachment [:meta :kind])})}
    (catch Exception _
      {:enqueue-error? true})))

(defn- delete-attachment-best-effort!
  [db attachment-id]
  ;; Rollback should not hide the original enqueue error.
  (try
    (attachments-db/delete-attachment! db {:attachment-id attachment-id})
    (catch Exception _
      nil)))

(defn- new-attachment-transact!
  [max-age-ms attachment db conv-id sender-id object-key attachment-workers bytes]
  (let [expires-at (java.sql.Timestamp. (+ (System/currentTimeMillis) max-age-ms))
        attachment-id (:attachment_id attachment)]
    (try
      (attachments-db/insert-attachment! db {:attachment-id attachment-id
                                             :conversation-id conv-id
                                             :uploader-id sender-id
                                             :object-key object-key
                                             :mime-type (:mime_type attachment)
                                             :size-bytes (:size_bytes attachment)
                                             :checksum (:checksum attachment)
                                             :expires-at expires-at})
      (let [{:keys [accepted? enqueue-error?]}
            (enqueue-new-attachment! attachment-workers attachment-id conv-id object-key bytes attachment)]
        (if (or enqueue-error? (false? accepted?))
          (do
            (delete-attachment-best-effort! db attachment-id)
            {:ok false
             :error "attachment processing enqueue failed"})
          {:ok true
           :conversation_id (str conv-id)
           :attachment attachment}))
      (catch Exception _
        {:ok false
         :error "attachment registry write failed"}))))

(defn- create! [upload max-bytes naming kind max-age-ms db conv-id sender-id attachment-workers]
  (let [{:keys [bytes filename content-type source]} upload
                byte-count (alength ^bytes bytes)]
            (cond
              (zero? byte-count)
              {:ok false :error "empty attachment payload"}

              (and (pos? max-bytes) (> byte-count max-bytes))
              {:ok false
               :error "attachment too large"
               :size_bytes byte-count
               :max_bytes max-bytes}

              :else
              (let [attachments-prefix (get-in naming [:minio :attachments-prefix] "attachments/")
                    {:keys [ok attachment object-key error]}
                   (logic/prepare-attachment {:bytes bytes
                                               :content-type content-type
                                               :filename filename
                                               :kind kind
                                               :attachments-prefix attachments-prefix
                                               :expected-size-bytes byte-count
                                               :expected-mime-type content-type
                                               :source source})]
                (cond
                  (not ok)
                  {:ok false :error error}

                  (not (m/validate msg-schema/AttachmentSchema attachment))
                  {:ok false
                   :error "invalid attachment metadata"
                   :details (me/humanize (m/explain msg-schema/AttachmentSchema attachment))}

                  :else
                  (new-attachment-transact! max-age-ms attachment db conv-id sender-id object-key attachment-workers bytes))))))

(defn attachments-create
  "Endpoint handler for uploading attachments to a conversation.
  Validates membership, processes the multipart upload, registers the attachment
  in the database with an expiration date, and enqueues it for background 
  processing (orchestration/resizing)."
  [{:keys [webdeps]}]
  (let [{:keys [db naming attachment-retention attachment-workers]} webdeps
        max-age-ms (attachment-max-age-ms attachment-retention)]
    (fn [req]
      (let [format (http/get-accept-format req)
            conv-id (http/parse-uuid (http/param req "id"))
            sender-id (message-logic/sender-id-from-request req)
            max-bytes (http/parse-long (http/param req "max-bytes") 10485760)
            kind (http/param req "kind")
            upload (http/read-upload req)
            payload (cond
                      (not conv-id)
                      {:ok false :error "invalid conversation id"}

                      (nil? sender-id)
                      {:ok false :error "invalid sender id"}

                      (not (conversations-db/member? db {:conversation-id conv-id :user-id sender-id}))
                      {:ok false :error "not a member"}

                      (nil? attachment-workers)
                      {:ok false :error "attachment workers not configured"}

                      (not upload)
                      {:ok false :error "missing attachment payload"}

                      :else
                      (create! upload max-bytes naming kind max-age-ms db conv-id sender-id attachment-workers))]
        (render-response payload format)))))

(defn- fetch-ref-db-then-storage [db attachment-id version conv-id minio]
  (let [row (attachments-db/fetch-attachment-by-id db {:attachment-id attachment-id})
                            now-ms (System/currentTimeMillis)
                            alt-version? (= version "alt")]
                        (cond
                          (or (nil? row)
                              (not= conv-id (:conversation_id row)))
                          {:status 404
                           :body {:ok false :error "attachment not found"}}

                          (attachment-row-expired? row now-ms)
                          {:status 404
                           :body {:ok false :error "attachment expired"}}

                          (and alt-version?
                               (not (str/starts-with? (or (:mime_type row) "") "image/")))
                          {:status 404
                           :body {:ok false :error "attachment variant not found"}}

                          :else
                          (let [object-key (if alt-version?
                                             (logic/derive-alt-key (:object_key row))
                                             (:object_key row))
                                obj (p-storage/storage-get-bytes minio object-key {})
                                bytes (:bytes obj)
                                mime-type (if alt-version?
                                            "image/jpeg"
                                            (or (:mime_type row) "application/octet-stream"))]
                            (cond
                              (and (:ok obj) (bytes? bytes))
                              {:status 200
                               :headers {"content-type" mime-type
                                         "content-length" (str (alength ^bytes bytes))
                                         "cache-control" "private, max-age=60"}
                               :body bytes}

                              (missing-object? obj)
                              {:status 404
                               :body {:ok false :error "attachment not found"}}

                              :else
                              {:status 500
                               :body {:ok false :error "attachment fetch failed"}})))))

(defn attachments-get
    "Endpoint handler for retrieving attachment content. Validates membership, checks
    attachment existence and expiration, and fetches the content from Minio storage.
    Supports fetching the original or an alternate (resized) version of the attachment."
  [{:keys [webdeps]}]
  (let [{:keys [db minio]} webdeps]
    (fn [req]
      (let [format (http/get-accept-format req)
            conv-id (http/parse-uuid (http/param req "id"))
            attachment-id (http/parse-uuid (http/param req "attachment_id"))
            version (some-> (http/param req "version") str/lower-case)
            sender-id (message-logic/sender-id-from-request req)
            payload (cond
                      (not conv-id)
                      {:ok false :error "invalid conversation id"}

                      (nil? attachment-id)
                      {:ok false :error "invalid attachment id"}

                      (nil? sender-id)
                      {:ok false :error "invalid sender id"}

                      (not (conversations-db/member? db {:conversation-id conv-id :user-id sender-id}))
                      {:ok false :error "not a member"}

                      (nil? minio)
                      {:ok false :error "minio not configured"}

                      (not (or (nil? version) (= version "original") (= version "alt")))
                      {:ok false :error "invalid version"}

                      :else
                      (fetch-ref-db-then-storage db attachment-id version conv-id minio))]
        (render-response payload format)))))

(defn- fetch-ref-from-db-then-storage [db attachment-id version conv-id minio]
  (let [row (attachments-db/fetch-attachment-by-id db {:attachment-id attachment-id})
                            now-ms (System/currentTimeMillis)
                            alt-version? (= version "alt")]
                        (cond
                          (or (nil? row)
                              (not= conv-id (:conversation_id row)))
                          {:status 404 :body nil}

                          (attachment-row-expired? row now-ms)
                          {:status 404 :body nil}

                          (and alt-version?
                               (not (str/starts-with? (or (:mime_type row) "") "image/")))
                          {:status 404 :body nil}

                          :else
                          (let [object-key (if alt-version?
                                             (logic/derive-alt-key (:object_key row))
                                             (:object_key row))
                                obj (p-storage/storage-head minio object-key {})
                                mime-type-fallback (if alt-version?
                                                     "image/jpeg"
                                                     (or (:mime_type row) "application/octet-stream"))
                                content-type (or (:content-type obj) mime-type-fallback)
                                content-length (or (:size obj) 0)
                                etag (:etag obj)
                                last-modified (format-http-date (:last-modified obj))]
                            (cond
                              (:ok obj)
                              {:status 200
                               :headers (cond-> {"content-type" content-type
                                                 "content-length" (str content-length)
                                                 "cache-control" "private, max-age=60"}
                                          (seq etag) (assoc "etag" etag)
                                          (seq last-modified) (assoc "last-modified" last-modified))
                               :body nil}

                              (missing-object? obj)
                              {:status 404 :body nil}

                              :else
                              {:status 500 :body nil})))))

(defn attachments-head
  "Endpoint handler for checking attachment existence and metadata without fetching content."
  [{:keys [webdeps]}]
  (let [{:keys [db minio]} webdeps]
    (fn [req]
      (let [format (http/get-accept-format req)
            conv-id (http/parse-uuid (http/param req "id"))
            attachment-id (http/parse-uuid (http/param req "attachment_id"))
            version (some-> (http/param req "version") str/lower-case)
            sender-id (message-logic/sender-id-from-request req)
            payload (cond
                      (not conv-id)
                      {:status 400 :body nil}

                      (nil? attachment-id)
                      {:status 400 :body nil}

                      (nil? sender-id)
                      {:status 401 :body nil}

                      (not (conversations-db/member? db {:conversation-id conv-id :user-id sender-id}))
                      {:status 403 :body nil}

                      (nil? minio)
                      {:status 500 :body nil}

                      (not (or (nil? version) (= version "original") (= version "alt")))
                      {:status 400 :body nil}

                      :else
                      (fetch-ref-from-db-then-storage db attachment-id version conv-id minio))]
        (render-response payload format)))))
