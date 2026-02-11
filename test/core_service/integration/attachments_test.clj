(ns core-service.integration.attachments-test
  (:require [cheshire.core :as json]
            [clojure.test :refer [deftest is testing]]
            [core-service.app.config.clients]
            [core-service.app.config.messaging]
            [core-service.app.config.storage]
            [core-service.app.server.attachment.authed :as attachment-authed]
            [core-service.app.server.conversation.v1.authed.authed :as message-authed]
            [core-service.integration.helpers :as helpers]
            [d-core.core.clients.redis]
            [d-core.core.storage.minio]
            [d-core.core.storage.protocol :as p-storage]
            [integrant.core :as ig])
  (:import (java.io ByteArrayOutputStream)
           (java.awt.image BufferedImage)
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
            attach-handler (attachment-authed/attachments-create {:webdeps {:db db
                                                                            :minio minio-client
                                                                            :naming naming}})
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
          (let [upload-resp (attach-handler {:request-method :post
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
                    _ (reset! object-key* object-key)
                    stored (p-storage/storage-get-bytes minio-client object-key {})]
                (testing "attachment upload response"
                  (is (= "image/png" (:mime_type attachment)))
                  (is (string? object-key)))
                (testing "attachment stored in minio"
                  (is (:ok stored))
                  (is (pos? (alength ^bytes (:bytes stored)))))
                (let [payload (json/generate-string {:type "image"
                                                     :body {}
                                                     :attachments [attachment]})
                      msg-resp (message-handler {:request-method :post
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
            (when (.exists temp-file)
              (.delete temp-file))
            (helpers/clear-redis-conversation! redis-client naming conv-id)
            (helpers/cleanup-conversation! db conv-id)
            (ig/halt-key! :d-core.core.clients.postgres/client client)))))))
