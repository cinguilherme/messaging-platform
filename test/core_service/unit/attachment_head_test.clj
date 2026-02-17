(ns core-service.unit.attachment-head-test
  (:require [clojure.test :refer [deftest is testing]]
            [core-service.app.db.attachments :as attachments-db]
            [core-service.app.db.conversations :as conversations-db]
            [core-service.app.server.attachment.authed :as attachment-authed]
            [core-service.integration.helpers :as helpers]
            [core-service.app.server.message.logic :as message-logic]
            [d-core.core.storage.protocol :as p-storage]))

(deftest attachments-head-uses-storage-head-only
  (let [conv-id #uuid "11111111-1111-1111-1111-111111111111"
        attachment-id #uuid "22222222-2222-2222-2222-222222222222"
        sender-id #uuid "33333333-3333-3333-3333-333333333333"
        storage-key* (atom nil)
        handler (attachment-authed/attachments-head {:webdeps {:db :db
                                                               :minio :storage}})
        last-modified (java.util.Date.)
        response (with-redefs [message-logic/sender-id-from-request (fn [_] sender-id)
                               conversations-db/member? (fn [_ _] true)
                               attachments-db/fetch-attachment-by-id (fn [_ _]
                                                                       {:attachment_id attachment-id
                                                                        :conversation_id conv-id
                                                                        :mime_type "image/png"
                                                                        :object_key "attachments/image/test.png"
                                                                        :expires_at nil})
                               p-storage/storage-head (fn [_ key _]
                                                       (reset! storage-key* key)
                                                       {:ok true
                                                        :size 123
                                                        :content-type "image/jpeg"
                                                        :etag "etag-123"
                                                        :last-modified last-modified})
                               p-storage/storage-get-bytes (fn [& _]
                                                            (throw (ex-info "storage-get-bytes must not be called by HEAD" {})))]
                   (helpers/invoke-handler handler {:request-method :head
                                                    :params {:id (str conv-id)
                                                             :attachment_id (str attachment-id)
                                                             :version "alt"}
                                                    :auth/principal {:subject (str sender-id)
                                                                     :tenant-id "tenant-1"}}))]
    (testing "HEAD probes alt key through storage-head and returns metadata headers"
      (is (= "attachments/image/test-alt.jpg" @storage-key*))
      (is (= 200 (:status response)))
      (is (= "image/jpeg" (get-in response [:headers "content-type"])))
      (is (= "123" (get-in response [:headers "content-length"])))
      (is (= "etag-123" (get-in response [:headers "etag"])))
      (is (string? (get-in response [:headers "last-modified"])))
      (is (nil? (:body response))))))
