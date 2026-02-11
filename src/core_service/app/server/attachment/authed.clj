(ns core-service.app.server.attachment.authed
  (:require [core-service.app.db.attachments :as attachments-db]
            [core-service.app.db.conversations :as conversations-db]
            [core-service.app.schemas.messaging :as msg-schema]
            [core-service.app.server.attachment.logic :as logic]
            [core-service.app.server.http :as http]
            [core-service.app.server.message.logic :as message-logic]
            [d-core.core.storage.protocol :as p-storage]
            [malli.core :as m]
            [malli.error :as me]))

(def ^:private default-attachment-max-age-ms 2592000000)

(defn- attachment-max-age-ms
  [attachment-retention]
  (let [configured (some-> (:max-age-ms attachment-retention) long)]
    (if (and configured (pos? configured))
      configured
      default-attachment-max-age-ms)))

(defn attachments-create
  [{:keys [webdeps]}]
  (let [{:keys [db minio naming attachment-retention]} webdeps
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

          (nil? minio)
          (http/format-response {:ok false :error "minio not configured"} format)

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
                  (let [store-result (p-storage/storage-put-bytes minio object-key bytes
                                                                 {:content-type (:mime_type attachment)})
                        expires-at (java.sql.Timestamp. (+ (System/currentTimeMillis) max-age-ms))]
                    (if-not (:ok store-result)
                      (http/format-response {:ok false :error (:error store-result)} format)
                      (try
                        (attachments-db/insert-attachment! db {:attachment-id (:attachment_id attachment)
                                                               :conversation-id conv-id
                                                               :uploader-id sender-id
                                                               :object-key object-key
                                                               :mime-type (:mime_type attachment)
                                                               :size-bytes (:size_bytes attachment)
                                                               :checksum (:checksum attachment)
                                                               :expires-at expires-at})
                        (http/format-response {:ok true
                                               :conversation_id (str conv-id)
                                               :attachment attachment}
                                              format)
                        (catch Exception _
                          ;; Best effort: avoid leaking uploaded object if DB registry write fails.
                          (p-storage/storage-delete minio object-key {})
                          (http/format-response {:ok false
                                                 :error "attachment registry write failed"}
                                                format))))))))))))))
