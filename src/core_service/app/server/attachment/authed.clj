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

(defn attachments-create
  [{:keys [webdeps]}]
  (let [{:keys [db naming attachment-retention attachment-workers]} webdeps
        max-age-ms (attachment-max-age-ms attachment-retention)]
    (fn [req]
      (let [format (http/get-accept-format req)
            conv-id (http/parse-uuid (http/param req "id"))
            sender-id (message-logic/sender-id-from-request req)
            max-bytes (http/parse-long (http/param req "max-bytes") 10485760)
            kind (http/param req "kind")
            upload (http/read-upload req)]
        (cond
          (not conv-id)
          (http/format-response {:ok false :error "invalid conversation id"} format)

          (nil? sender-id)
          (http/format-response {:ok false :error "invalid sender id"} format)

          (not (conversations-db/member? db {:conversation-id conv-id :user-id sender-id}))
          (http/format-response {:ok false :error "not a member"} format)

          (nil? attachment-workers)
          (http/format-response {:ok false :error "attachment workers not configured"} format)

          (not upload)
          (http/format-response {:ok false :error "missing attachment payload"} format)

          :else
          (let [{:keys [bytes filename content-type source]} upload
                byte-count (alength ^bytes bytes)]
            (cond
              (zero? byte-count)
              (http/format-response {:ok false :error "empty attachment payload"} format)

              (and (pos? max-bytes) (> byte-count max-bytes))
              (http/format-response {:ok false
                                     :error "attachment too large"
                                     :size_bytes byte-count
                                     :max_bytes max-bytes}
                                    format)

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
                  (http/format-response {:ok false :error error} format)

                  (not (m/validate msg-schema/AttachmentSchema attachment))
                  (http/format-response {:ok false
                                         :error "invalid attachment metadata"
                                         :details (me/humanize (m/explain msg-schema/AttachmentSchema attachment))}
                                        format)

                  :else
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
                      (try
                        (let [accepted? (workers/command! attachment-workers
                                                         :attachment-orchestrator
                                                         {:attachment-id attachment-id
                                                          :conversation-id conv-id
                                                          :object-key object-key
                                                          :bytes bytes
                                                          :size-bytes (:size_bytes attachment)
                                                          :mime-type (:mime_type attachment)
                                                          :kind (get-in attachment [:meta :kind])})]
                          (if (false? accepted?)
                            (do
                              (attachments-db/delete-attachment! db {:attachment-id attachment-id})
                              (http/format-response {:ok false
                                                     :error "attachment processing enqueue failed"}
                                                    format))
                            (http/format-response {:ok true
                                                   :conversation_id (str conv-id)
                                                   :attachment attachment}
                                                  format)))
                        (catch Exception _
                          ;; Best effort rollback, upload has not been persisted yet.
                          (attachments-db/delete-attachment! db {:attachment-id attachment-id})
                          (http/format-response {:ok false
                                                 :error "attachment processing enqueue failed"}
                                                format)))
                      (catch Exception _
                        (http/format-response {:ok false
                                               :error "attachment registry write failed"}
                                              format)))))))))))))

(defn attachments-get
  [{:keys [webdeps]}]
  (let [{:keys [db minio]} webdeps]
    (fn [req]
      (let [format (http/get-accept-format req)
            conv-id (http/parse-uuid (http/param req "id"))
            attachment-id (http/parse-uuid (http/param req "attachment_id"))
            version (some-> (http/param req "version") str/lower-case)
            sender-id (message-logic/sender-id-from-request req)]
        (cond
          (not conv-id)
          (http/format-response {:ok false :error "invalid conversation id"} format)

          (nil? attachment-id)
          (http/format-response {:ok false :error "invalid attachment id"} format)

          (nil? sender-id)
          (http/format-response {:ok false :error "invalid sender id"} format)

          (not (conversations-db/member? db {:conversation-id conv-id :user-id sender-id}))
          (http/format-response {:ok false :error "not a member"} format)

          (nil? minio)
          (http/format-response {:ok false :error "minio not configured"} format)

          (not (or (nil? version) (= version "original") (= version "alt")))
          (http/format-response {:ok false :error "invalid version"} format)

          :else
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
                   :body {:ok false :error "attachment fetch failed"}})))))))))

(defn attachments-head
  [{:keys [webdeps]}]
  (let [{:keys [db minio]} webdeps]
    (fn [req]
      (let [conv-id (http/parse-uuid (http/param req "id"))
            attachment-id (http/parse-uuid (http/param req "attachment_id"))
            version (some-> (http/param req "version") str/lower-case)
            sender-id (message-logic/sender-id-from-request req)]
        (cond
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
                  {:status 500 :body nil})))))))))
