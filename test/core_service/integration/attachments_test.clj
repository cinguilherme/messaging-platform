(ns core-service.integration.attachments-test
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [core-service.app.config.clients]
            [core-service.app.config.messaging]
            [core-service.app.config.storage]
            [core-service.app.server.attachment.authed :as attachment-authed]
            [core-service.app.server.attachment.logic :as attachment-logic]
            [core-service.app.server.conversation.v1.authed.authed :as message-authed]
            [core-service.app.workers.attachments]
            [core-service.app.workers.attachment-retention :as attachment-retention]
            [core-service.integration.helpers :as helpers]
            [d-core.core.clients.redis]
            [d-core.core.databases.protocols.simple-sql :as sql]
            [d-core.core.storage.minio]
            [d-core.core.storage.protocol :as p-storage]
            [integrant.core :as ig])
  (:import (java.io ByteArrayOutputStream)
           (java.awt.image BufferedImage)
           (java.util Random)
           (javax.imageio ImageIO)))

(defn- write-temp-file!
  [^bytes bytes suffix]
  (let [file (java.io.File/createTempFile "attachment" (or suffix ".bin"))]
    (with-open [out (java.io.FileOutputStream. file)]
      (.write out bytes))
    file))

(defn- tiny-png-bytes
  []
  (let [image (BufferedImage. 1 1 BufferedImage/TYPE_INT_RGB)
        out (ByteArrayOutputStream.)]
    (ImageIO/write image "png" out)
    (.toByteArray out)))

(defn- noisy-png-bytes
  [width height]
  (let [image (BufferedImage. width height BufferedImage/TYPE_INT_RGB)
        rnd (Random.)
        out (ByteArrayOutputStream.)]
    (dotimes [x width]
      (dotimes [y height]
        (.setRGB image x y (.nextInt rnd 0x1000000))))
    (ImageIO/write image "png" out)
    (.toByteArray out)))

(defn- wait-for-object
  [storage object-key]
  (loop [attempt 0]
    (let [result (p-storage/storage-get-bytes storage object-key {})]
      (if (or (:ok result) (>= attempt 200))
        result
        (do
          (Thread/sleep 25)
          (recur (inc attempt)))))))

(defn- wait-for-object-where
  [storage object-key pred]
  (loop [attempt 0]
    (let [result (p-storage/storage-get-bytes storage object-key {})]
      (if (or (and (:ok result) (pred result))
              (>= attempt 200))
        result
        (do
          (Thread/sleep 25)
          (recur (inc attempt)))))))

(defn- empty-head-body?
  [body]
  (or (nil? body)
      (and (string? body) (str/blank? body))
      (and (bytes? body) (zero? (alength ^bytes body)))))

(defn- init-attachment-workers
  [minio-client]
  (ig/init-key :core-service.app.workers.attachments/system
               {:webdeps {:minio minio-client
                          :storage minio-client}
                :logger nil
                :metrics nil
                :processing {:optimize-threshold-bytes 1048576
                             :alt-max-dim 320
                             ;; Keep below test fixture dimensions (1400x1400)
                             ;; so large-image optimization is guaranteed in integration tests.
                             :standard-max-dim 1280}}))

(deftest attachments-upload-and-send-message
  (let [redis-cfg (ig/init-key :core-service.app.config.clients/redis {})
        redis-client (ig/init-key :d-core.core.clients.redis/client redis-cfg)
        minio-cfg (ig/init-key :core-service.app.config.storage/minio {})
        minio-client (ig/init-key :d-core.core.storage/minio minio-cfg)]
    (if-not (and (helpers/redis-up? redis-client) (helpers/minio-up? minio-client))
      (is false "Redis or Minio not reachable. Start docker-compose and retry.")
      (let [{:keys [db client]} (helpers/init-db)
            naming (ig/init-key :core-service.app.config.messaging/storage-names {})
            streams (helpers/init-streams-backend redis-client naming)
            attachment-workers-system (init-attachment-workers minio-client)
            attach-handler (attachment-authed/attachments-create {:webdeps {:db db
                                                                            :minio minio-client
                                                                            :naming naming
                                                                            :attachment-workers attachment-workers-system}})
            message-handler (message-authed/messages-create {:webdeps {:db db
                                                                       :redis redis-client
                                                                       :streams streams
                                                                       :naming naming}})
            conv-id (java.util.UUID/randomUUID)
            sender-id (java.util.UUID/randomUUID)
            object-key* (atom nil)
            alt-key* (atom nil)
            png-bytes (tiny-png-bytes)
            temp-file (write-temp-file! png-bytes ".png")]
        (try
          (helpers/setup-conversation! db {:conversation-id conv-id
                                           :user-id sender-id})
          (helpers/clear-redis-conversation! redis-client naming conv-id)
          (let [upload-resp (helpers/invoke-handler attach-handler {:request-method :post
                                             :headers {"accept" "application/json"
                                                       "content-type" "multipart/form-data"}
                                             :params {:id (str conv-id)
                                                      :kind "image"}
                                             :multipart-params {"file" {:tempfile temp-file
                                                                         :filename "tiny.png"
                                                                         :content-type "image/png"}}
                                             :auth/principal {:subject (str sender-id)
                                                              :tenant-id "tenant-1"}})
                upload-body (json/parse-string (:body upload-resp) true)]
            (testing "attachment upload response"
              (is (= 200 (:status upload-resp)))
              (is (:ok upload-body) (str "upload failed: " upload-body)))
            (when (:ok upload-body)
              (let [attachment (:attachment upload-body)
                    object-key (:object_key attachment)
                    alt-key (attachment-logic/derive-alt-key object-key)
                    _ (reset! object-key* object-key)
                    _ (reset! alt-key* alt-key)
                    stored (wait-for-object minio-client object-key)
                    stored-alt (wait-for-object minio-client alt-key)]
                (testing "attachment upload response"
                  (is (= "image/png" (:mime_type attachment)))
                  (is (string? object-key)))
                (testing "attachment stored in minio"
                  (is (:ok stored))
                  (is (pos? (alength ^bytes (:bytes stored)))))
                (testing "alt placeholder stored in minio"
                  (is (:ok stored-alt))
                  (is (pos? (alength ^bytes (:bytes stored-alt)))))
                (let [payload (json/generate-string {:type "image"
                                                     :body {}
                                                     :attachments [attachment]})
                      msg-resp (helpers/invoke-handler message-handler {:request-method :post
                                                 :headers {"accept" "application/json"}
                                                 :params {:id (str conv-id)}
                                                 :body payload
                                                 :auth/principal {:subject (str sender-id)
                                                                  :tenant-id "tenant-1"}})
                      msg-body (json/parse-string (:body msg-resp) true)]
                  (testing "message includes attachment reference"
                    (is (= 200 (:status msg-resp)))
                    (is (:ok msg-body) (str "message failed: " msg-body))
                    (when (:ok msg-body)
                      (let [msg-attachment (first (get-in msg-body [:message :attachments]))]
                        (is (= object-key (:object_key msg-attachment))))))))))
          (finally
            (when-let [object-key @object-key*]
              (p-storage/storage-delete minio-client object-key {}))
            (when-let [alt-key @alt-key*]
              (p-storage/storage-delete minio-client alt-key {}))
            (when (.exists temp-file)
              (.delete temp-file))
            (helpers/clear-redis-conversation! redis-client naming conv-id)
            (helpers/cleanup-conversation! db conv-id)
            (ig/halt-key! :core-service.app.workers.attachments/system attachment-workers-system)
            (ig/halt-key! :d-core.core.clients.postgres/client client)))))))

(deftest attachments-message-rejects-tampered-reference
  (let [redis-cfg (ig/init-key :core-service.app.config.clients/redis {})
        redis-client (ig/init-key :d-core.core.clients.redis/client redis-cfg)
        minio-cfg (ig/init-key :core-service.app.config.storage/minio {})
        minio-client (ig/init-key :d-core.core.storage/minio minio-cfg)]
    (if-not (and (helpers/redis-up? redis-client) (helpers/minio-up? minio-client))
      (is false "Redis or Minio not reachable. Start docker-compose and retry.")
      (let [{:keys [db client]} (helpers/init-db)
            naming (ig/init-key :core-service.app.config.messaging/storage-names {})
            streams (helpers/init-streams-backend redis-client naming)
            attachment-workers-system (init-attachment-workers minio-client)
            attach-handler (attachment-authed/attachments-create {:webdeps {:db db
                                                                            :minio minio-client
                                                                            :naming naming
                                                                            :attachment-workers attachment-workers-system}})
            message-handler (message-authed/messages-create {:webdeps {:db db
                                                                       :redis redis-client
                                                                       :streams streams
                                                                       :naming naming}})
            conv-id (java.util.UUID/randomUUID)
            sender-id (java.util.UUID/randomUUID)
            object-key* (atom nil)
            png-bytes (tiny-png-bytes)
            temp-file (write-temp-file! png-bytes ".png")]
        (try
          (helpers/setup-conversation! db {:conversation-id conv-id
                                           :user-id sender-id})
          (helpers/clear-redis-conversation! redis-client naming conv-id)
          (let [upload-resp (helpers/invoke-handler attach-handler {:request-method :post
                                             :headers {"accept" "application/json"
                                                       "content-type" "multipart/form-data"}
                                             :params {:id (str conv-id)
                                                      :kind "image"}
                                             :multipart-params {"file" {:tempfile temp-file
                                                                         :filename "tiny.png"
                                                                         :content-type "image/png"}}
                                             :auth/principal {:subject (str sender-id)
                                                              :tenant-id "tenant-1"}})
                upload-body (json/parse-string (:body upload-resp) true)]
            (testing "upload succeeds"
              (is (= 200 (:status upload-resp)))
              (is (:ok upload-body)))
            (when (:ok upload-body)
              (let [attachment (:attachment upload-body)
                    object-key (:object_key attachment)
                    _ (reset! object-key* object-key)
                    tampered (assoc attachment :object_key (str object-key ".tampered"))
                    payload (json/generate-string {:type "image"
                                                   :body {}
                                                   :attachments [tampered]})
                    msg-resp (helpers/invoke-handler message-handler {:request-method :post
                                               :headers {"accept" "application/json"}
                                               :params {:id (str conv-id)}
                                               :body payload
                                               :auth/principal {:subject (str sender-id)
                                                                :tenant-id "tenant-1"}})
                    msg-body (json/parse-string (:body msg-resp) true)]
                (testing "message rejects forged attachment metadata"
                  (is (= 400 (:status msg-resp)))
                  (is (false? (:ok msg-body)))
                  (is (= "invalid attachment reference" (:error msg-body)))))))
          (finally
            (when-let [object-key @object-key*]
              (p-storage/storage-delete minio-client object-key {}))
            (when-let [object-key @object-key*]
              (p-storage/storage-delete minio-client (attachment-logic/derive-alt-key object-key) {}))
            (when (.exists temp-file)
              (.delete temp-file))
            (helpers/clear-redis-conversation! redis-client naming conv-id)
            (helpers/cleanup-conversation! db conv-id)
            (ig/halt-key! :core-service.app.workers.attachments/system attachment-workers-system)
            (ig/halt-key! :d-core.core.clients.postgres/client client)))))))

(deftest attachments-retention-removes-expired-object-and-row
  (let [minio-cfg (ig/init-key :core-service.app.config.storage/minio {})
        minio-client (ig/init-key :d-core.core.storage/minio minio-cfg)]
    (if-not (helpers/minio-up? minio-client)
      (is false "Minio not reachable. Start docker-compose and retry.")
      (let [{:keys [db client]} (helpers/init-db)
            naming (ig/init-key :core-service.app.config.messaging/storage-names {})
            attachment-workers-system (init-attachment-workers minio-client)
            attach-handler (attachment-authed/attachments-create {:webdeps {:db db
                                                                            :minio minio-client
                                                                            :naming naming
                                                                            :attachment-workers attachment-workers-system
                                                                            :attachment-retention {:max-age-ms 60000}}})
            conv-id (java.util.UUID/randomUUID)
            sender-id (java.util.UUID/randomUUID)
            object-key* (atom nil)
            attachment-id* (atom nil)
            png-bytes (tiny-png-bytes)
            temp-file (write-temp-file! png-bytes ".png")]
        (try
          (helpers/setup-conversation! db {:conversation-id conv-id
                                           :user-id sender-id})
          (let [upload-resp (helpers/invoke-handler attach-handler {:request-method :post
                                             :headers {"accept" "application/json"
                                                       "content-type" "multipart/form-data"}
                                             :params {:id (str conv-id)
                                                      :kind "image"}
                                             :multipart-params {"file" {:tempfile temp-file
                                                                         :filename "tiny.png"
                                                                         :content-type "image/png"}}
                                             :auth/principal {:subject (str sender-id)
                                                              :tenant-id "tenant-1"}})
                upload-body (json/parse-string (:body upload-resp) true)]
            (testing "upload succeeds"
              (is (= 200 (:status upload-resp)))
              (is (:ok upload-body)))
            (when (:ok upload-body)
              (let [attachment (:attachment upload-body)
                    attachment-id (java.util.UUID/fromString (:attachment_id attachment))
                    object-key (:object_key attachment)
                    alt-key (attachment-logic/derive-alt-key object-key)
                    _ (reset! attachment-id* attachment-id)
                    _ (reset! object-key* object-key)
                    _ (wait-for-object minio-client object-key)
                    _ (wait-for-object minio-client alt-key)
                    expired-at (java.sql.Timestamp. (- (System/currentTimeMillis) 2000))]
                (sql/execute! db ["UPDATE attachments SET expires_at = ? WHERE attachment_id = ?"
                                  expired-at attachment-id]
                              {})
                (let [result (attachment-retention/cleanup! {:db db
                                                             :minio minio-client
                                                             :retention {:max-age-ms 1
                                                                         :batch-size 10}})
                      obj (p-storage/storage-get-bytes minio-client object-key {})
                      alt-obj (p-storage/storage-get-bytes minio-client alt-key {})
                      row (first (sql/select db {:table :attachments
                                                 :where {:attachment_id attachment-id}}))]
                  (testing "worker deletes expired attachment original/alt objects and row"
                    (is (= :ok (:status result)))
                    (is (= 1 (:deleted result)))
                    (is (false? (:ok obj)))
                    (is (false? (:ok alt-obj)))
                    (is (nil? row)))))))
          (finally
            (when-let [object-key @object-key*]
              (p-storage/storage-delete minio-client object-key {}))
            (when-let [object-key @object-key*]
              (p-storage/storage-delete minio-client (attachment-logic/derive-alt-key object-key) {}))
            (when (.exists temp-file)
              (.delete temp-file))
            (helpers/cleanup-conversation! db conv-id)
            (ig/halt-key! :core-service.app.workers.attachments/system attachment-workers-system)
            (ig/halt-key! :d-core.core.clients.postgres/client client)))))))

(deftest attachments-retention-uses-expires-at-directly
  (let [minio-cfg (ig/init-key :core-service.app.config.storage/minio {})
        minio-client (ig/init-key :d-core.core.storage/minio minio-cfg)]
    (if-not (helpers/minio-up? minio-client)
      (is false "Minio not reachable. Start docker-compose and retry.")
      (let [{:keys [db client]} (helpers/init-db)
            naming (ig/init-key :core-service.app.config.messaging/storage-names {})
            attachment-workers-system (init-attachment-workers minio-client)
            attach-handler (attachment-authed/attachments-create {:webdeps {:db db
                                                                            :minio minio-client
                                                                            :naming naming
                                                                            :attachment-workers attachment-workers-system
                                                                            :attachment-retention {:max-age-ms 20}}})
            conv-id (java.util.UUID/randomUUID)
            sender-id (java.util.UUID/randomUUID)
            object-key* (atom nil)
            attachment-id* (atom nil)
            png-bytes (tiny-png-bytes)
            temp-file (write-temp-file! png-bytes ".png")]
        (try
          (helpers/setup-conversation! db {:conversation-id conv-id
                                           :user-id sender-id})
          (let [upload-resp (helpers/invoke-handler attach-handler {:request-method :post
                                             :headers {"accept" "application/json"
                                                       "content-type" "multipart/form-data"}
                                             :params {:id (str conv-id)
                                                      :kind "image"}
                                             :multipart-params {"file" {:tempfile temp-file
                                                                         :filename "tiny.png"
                                                                         :content-type "image/png"}}
                                             :auth/principal {:subject (str sender-id)
                                                              :tenant-id "tenant-1"}})
                upload-body (json/parse-string (:body upload-resp) true)]
            (testing "upload succeeds"
              (is (= 200 (:status upload-resp)))
              (is (:ok upload-body)))
            (when (:ok upload-body)
              (let [attachment (:attachment upload-body)
                    attachment-id (java.util.UUID/fromString (:attachment_id attachment))
                    object-key (:object_key attachment)
                    alt-key (attachment-logic/derive-alt-key object-key)
                    _ (reset! attachment-id* attachment-id)
                    _ (reset! object-key* object-key)
                    _ (wait-for-object minio-client object-key)
                    _ (wait-for-object minio-client alt-key)]
                ;; Wait for the attachment row TTL to pass.
                (Thread/sleep 80)
                (let [result (attachment-retention/cleanup! {:db db
                                                             :minio minio-client
                                                             ;; Large max-age to prove selection uses expires_at directly.
                                                             :retention {:max-age-ms 60000
                                                                         :batch-size 10}})
                      obj (p-storage/storage-get-bytes minio-client object-key {})
                      alt-obj (p-storage/storage-get-bytes minio-client alt-key {})
                      row (first (sql/select db {:table :attachments
                                                 :where {:attachment_id attachment-id}}))]
                  (testing "cleanup removes row/object once expires_at passes"
                    (is (= :ok (:status result)))
                    (is (= 1 (:deleted result)))
                    (is (false? (:ok obj)))
                    (is (false? (:ok alt-obj)))
                    (is (nil? row)))))))
          (finally
            (when-let [object-key @object-key*]
              (p-storage/storage-delete minio-client object-key {}))
            (when-let [object-key @object-key*]
              (p-storage/storage-delete minio-client (attachment-logic/derive-alt-key object-key) {}))
            (when (.exists temp-file)
              (.delete temp-file))
            (helpers/cleanup-conversation! db conv-id)
            (ig/halt-key! :core-service.app.workers.attachments/system attachment-workers-system)
            (ig/halt-key! :d-core.core.clients.postgres/client client)))))))

(deftest attachments-async-image-variants
  (let [minio-cfg (ig/init-key :core-service.app.config.storage/minio {})
        minio-client (ig/init-key :d-core.core.storage/minio minio-cfg)]
    (if-not (helpers/minio-up? minio-client)
      (is false "Minio not reachable. Start docker-compose and retry.")
      (let [{:keys [db client]} (helpers/init-db)
            naming (ig/init-key :core-service.app.config.messaging/storage-names {})
            attachment-workers-system (init-attachment-workers minio-client)
            attach-handler (attachment-authed/attachments-create {:webdeps {:db db
                                                                            :minio minio-client
                                                                            :naming naming
                                                                            :attachment-workers attachment-workers-system}})
            get-handler (attachment-authed/attachments-get {:webdeps {:db db
                                                                      :minio minio-client}})
            head-handler (attachment-authed/attachments-head {:webdeps {:db db
                                                                        :minio minio-client}})
            conv-id (java.util.UUID/randomUUID)
            sender-id (java.util.UUID/randomUUID)
            object-keys* (atom [])
            small-bytes (tiny-png-bytes)
            large-bytes (noisy-png-bytes 1400 1400)
            small-file (write-temp-file! small-bytes ".png")
            large-file (write-temp-file! large-bytes ".png")]
        (try
          (helpers/setup-conversation! db {:conversation-id conv-id
                                           :user-id sender-id})

          (let [upload-small (helpers/invoke-handler attach-handler {:request-method :post
                                              :headers {"accept" "application/json"
                                                        "content-type" "multipart/form-data"}
                                              :params {:id (str conv-id)
                                                       :kind "image"}
                                              :multipart-params {"file" {:tempfile small-file
                                                                          :filename "small.png"
                                                                          :content-type "image/png"}}
                                              :auth/principal {:subject (str sender-id)
                                                               :tenant-id "tenant-1"}})
                body-small (json/parse-string (:body upload-small) true)]
            (testing "upload <1MB queues and stores original + alt"
              (is (= 200 (:status upload-small)))
              (is (:ok body-small)))
            (when (:ok body-small)
              (let [attachment (:attachment body-small)
                    object-key (:object_key attachment)
                    alt-key (attachment-logic/derive-alt-key object-key)
                    _ (swap! object-keys* conj object-key alt-key)
                    original-stored (wait-for-object minio-client object-key)
                    alt-stored (wait-for-object minio-client alt-key)
                    head-original-resp (helpers/invoke-handler head-handler {:request-method :head
                                                      :params {:id (str conv-id)
                                                               :attachment_id (str (:attachment_id attachment))}
                                                      :auth/principal {:subject (str sender-id)
                                                                       :tenant-id "tenant-1"}})
                    head-alt-resp (helpers/invoke-handler head-handler {:request-method :head
                                                 :params {:id (str conv-id)
                                                          :attachment_id (str (:attachment_id attachment))
                                                          :version "alt"}
                                                 :auth/principal {:subject (str sender-id)
                                                                  :tenant-id "tenant-1"}})
                    alt-resp (helpers/invoke-handler get-handler {:request-method :get
                                           :params {:id (str conv-id)
                                                    :attachment_id (str (:attachment_id attachment))
                                                    :version "alt"}
                                           :auth/principal {:subject (str sender-id)
                                                            :tenant-id "tenant-1"}})]
                (is (:ok original-stored))
                (is (:ok alt-stored))
                (is (= 200 (:status head-original-resp)))
                (is (= (:mime_type attachment) (get-in head-original-resp [:headers "content-type"])))
                (is (= (str (alength ^bytes (:bytes original-stored)))
                       (get-in head-original-resp [:headers "content-length"])))
                (is (empty-head-body? (:body head-original-resp)))
                (is (= 200 (:status head-alt-resp)))
                (is (= "image/jpeg" (get-in head-alt-resp [:headers "content-type"])))
                (is (= (str (alength ^bytes (:bytes alt-stored)))
                       (get-in head-alt-resp [:headers "content-length"])))
                (is (empty-head-body? (:body head-alt-resp)))
                (is (= 200 (:status alt-resp)))
                (is (= "image/jpeg" (get-in alt-resp [:headers "content-type"])))
                (is (bytes? (:body alt-resp)))
                (p-storage/storage-delete minio-client alt-key {})
                (let [missing-alt-head (helpers/invoke-handler head-handler {:request-method :head
                                                      :params {:id (str conv-id)
                                                               :attachment_id (str (:attachment_id attachment))
                                                               :version "alt"}
                                                      :auth/principal {:subject (str sender-id)
                                                                       :tenant-id "tenant-1"}})]
                  (is (= 404 (:status missing-alt-head)))
                  (is (empty-head-body? (:body missing-alt-head)))))))

          (let [upload-large (helpers/invoke-handler attach-handler {:request-method :post
                                              :headers {"accept" "application/json"
                                                        "content-type" "multipart/form-data"}
                                              :params {:id (str conv-id)
                                                       :kind "image"
                                                       :max-bytes 60000000}
                                              :multipart-params {"file" {:tempfile large-file
                                                                          :filename "large.png"
                                                                          :content-type "image/png"}}
                                              :auth/principal {:subject (str sender-id)
                                                               :tenant-id "tenant-1"}})
                body-large (json/parse-string (:body upload-large) true)]
            (testing "upload >1MB stores optimized original + alt"
              (is (= 200 (:status upload-large)))
              (is (:ok body-large)))
            (when (:ok body-large)
              (let [attachment (:attachment body-large)
                    object-key (:object_key attachment)
                    alt-key (attachment-logic/derive-alt-key object-key)
                    _ (swap! object-keys* conj object-key alt-key)
                    original-stored (wait-for-object-where
                                     minio-client
                                     object-key
                                     (fn [result]
                                       (< (alength ^bytes (:bytes result))
                                          (alength ^bytes large-bytes))))
                    alt-stored (wait-for-object minio-client alt-key)
                    stored-size (alength ^bytes (:bytes original-stored))]
                (is (:ok original-stored))
                (is (:ok alt-stored))
                (is (< stored-size (alength ^bytes large-bytes))
                    (str "expected optimized bytes < input bytes, got "
                         stored-size " vs " (alength ^bytes large-bytes))))))
          (finally
            (doseq [object-key @object-keys*]
              (p-storage/storage-delete minio-client object-key {}))
            (when (.exists small-file)
              (.delete small-file))
            (when (.exists large-file)
              (.delete large-file))
            (helpers/cleanup-conversation! db conv-id)
            (ig/halt-key! :core-service.app.workers.attachments/system attachment-workers-system)
            (ig/halt-key! :d-core.core.clients.postgres/client client)))))))

(deftest attachments-head-returns-404-for-non-image-alt-and-missing-original
  (let [minio-cfg (ig/init-key :core-service.app.config.storage/minio {})
        minio-client (ig/init-key :d-core.core.storage/minio minio-cfg)]
    (if-not (helpers/minio-up? minio-client)
      (is false "Minio not reachable. Start docker-compose and retry.")
      (let [{:keys [db client]} (helpers/init-db)
            naming (ig/init-key :core-service.app.config.messaging/storage-names {})
            attachment-workers-system (init-attachment-workers minio-client)
            attach-handler (attachment-authed/attachments-create {:webdeps {:db db
                                                                            :minio minio-client
                                                                            :naming naming
                                                                            :attachment-workers attachment-workers-system}})
            head-handler (attachment-authed/attachments-head {:webdeps {:db db
                                                                        :minio minio-client}})
            conv-id (java.util.UUID/randomUUID)
            sender-id (java.util.UUID/randomUUID)
            object-key* (atom nil)
            payload-bytes (.getBytes "hello attachment" "UTF-8")
            payload-file (write-temp-file! payload-bytes ".txt")]
        (try
          (helpers/setup-conversation! db {:conversation-id conv-id
                                           :user-id sender-id})
          (let [upload-resp (helpers/invoke-handler attach-handler {:request-method :post
                                             :headers {"accept" "application/json"
                                                       "content-type" "multipart/form-data"}
                                             :params {:id (str conv-id)
                                                      :kind "file"}
                                             :multipart-params {"file" {:tempfile payload-file
                                                                         :filename "payload.txt"
                                                                         :content-type "text/plain"}}
                                             :auth/principal {:subject (str sender-id)
                                                              :tenant-id "tenant-1"}})
                upload-body (json/parse-string (:body upload-resp) true)]
            (is (= 200 (:status upload-resp)))
            (is (:ok upload-body))
            (when (:ok upload-body)
              (let [attachment (:attachment upload-body)
                    object-key (:object_key attachment)
                    _ (reset! object-key* object-key)
                    _ (wait-for-object minio-client object-key)
                    alt-head-resp (helpers/invoke-handler head-handler {:request-method :head
                                                 :params {:id (str conv-id)
                                                          :attachment_id (str (:attachment_id attachment))
                                                          :version "alt"}
                                                 :auth/principal {:subject (str sender-id)
                                                                  :tenant-id "tenant-1"}})]
                (is (= 404 (:status alt-head-resp)))
                (is (empty-head-body? (:body alt-head-resp)))
                (p-storage/storage-delete minio-client object-key {})
                (let [missing-original-head (helpers/invoke-handler head-handler {:request-method :head
                                                           :params {:id (str conv-id)
                                                                    :attachment_id (str (:attachment_id attachment))
                                                                    :version "original"}
                                                           :auth/principal {:subject (str sender-id)
                                                                            :tenant-id "tenant-1"}})]
                  (is (= 404 (:status missing-original-head)))
                  (is (empty-head-body? (:body missing-original-head)))))))
          (finally
            (when-let [object-key @object-key*]
              (p-storage/storage-delete minio-client object-key {}))
            (when (.exists payload-file)
              (.delete payload-file))
            (helpers/cleanup-conversation! db conv-id)
            (ig/halt-key! :core-service.app.workers.attachments/system attachment-workers-system)
            (ig/halt-key! :d-core.core.clients.postgres/client client)))))))
