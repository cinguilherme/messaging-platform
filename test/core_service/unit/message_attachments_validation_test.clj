(ns core-service.unit.message-attachments-validation-test
  (:require [clojure.test :refer [deftest is testing]]
            [core-service.app.db.attachments :as attachments-db]
            [core-service.app.server.message.authed :as message-authed]))

(deftest validate-message-attachments-allows-mutable-storage-metadata
  (let [conv-id #uuid "00000000-0000-0000-0000-000000000010"
        attachment-id #uuid "00000000-0000-0000-0000-000000000011"
        row {:attachment_id attachment-id
             :conversation_id conv-id
             :object_key "attachments/image/a.png"
             :mime_type "image/png"
             :size_bytes 1200
             :checksum "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
             :expires_at nil}
        payload {:attachment_id attachment-id
                 :object_key "attachments/image/a.png"
                 :mime_type "image/png"
                 ;; Stale client metadata from pre-optimization upload response.
                 :size_bytes 9999
                 :checksum "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
        validate-fn #'message-authed/validate-message-attachments]
    (with-redefs [attachments-db/fetch-attachments-by-ids (fn [_ _] [row])]
      (testing "validation succeeds and canonical attachment comes from DB row"
        (let [result (validate-fn :db conv-id [payload])]
          (is (:ok result))
          (is (= [attachment-id] (:attachment-ids result)))
          (is (= [(:size_bytes row)]
                 (mapv :size_bytes (:canonical-attachments result))))
          (is (= [(:checksum row)]
                 (mapv :checksum (:canonical-attachments result)))))))))

(deftest validate-message-attachments-still-rejects-object-key-tampering
  (let [conv-id #uuid "00000000-0000-0000-0000-000000000020"
        attachment-id #uuid "00000000-0000-0000-0000-000000000021"
        row {:attachment_id attachment-id
             :conversation_id conv-id
             :object_key "attachments/image/a.png"
             :mime_type "image/png"
             :size_bytes 1200
             :checksum "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
             :expires_at nil}
        payload {:attachment_id attachment-id
                 :object_key "attachments/image/tampered.png"
                 :mime_type "image/png"
                 :size_bytes 1200
                 :checksum "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
        validate-fn #'message-authed/validate-message-attachments]
    (with-redefs [attachments-db/fetch-attachments-by-ids (fn [_ _] [row])]
      (testing "object_key mismatch is still rejected"
        (let [result (validate-fn :db conv-id [payload])]
          (is (false? (:ok result)))
          (is (= "invalid attachment reference" (:error result))))))))
