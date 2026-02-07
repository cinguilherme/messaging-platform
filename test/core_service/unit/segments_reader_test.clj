(ns core-service.unit.segments-reader-test
  (:require [clojure.test :refer [deftest is testing]]
            [core-service.app.db.segments :as segments-db]
            [core-service.app.segments.reader :as reader]
            [core-service.app.storage.minio :as minio]))

(deftest fetch-messages-prunes-missing-segment
  (let [conv-id (java.util.UUID/randomUUID)
        deleted (atom nil)
        calls (atom 0)
        row {:conversation_id conv-id
             :seq_start 1
             :seq_end 2
             :object_key "segments/missing"}]
    (with-redefs [segments-db/list-segments (fn [_ _]
                                              (let [n (swap! calls inc)]
                                                (if (= n 1) [row] [])))
                  segments-db/delete-segment! (fn [_ args] (reset! deleted args))
                  minio/get-bytes! (fn [_ _] {:ok false :error-type :not-found})]
      (let [result (reader/fetch-messages {:db :db
                                           :minio :minio
                                           :segments {:segment-batch 1
                                                      :compression :none
                                                      :codec :raw}}
                                          conv-id
                                          {:limit 1 :direction :backward})]
        (testing "missing segment is pruned and does not break pagination"
          (is (= [] (:messages result)))
          (is (false? (:has-more? result)))
          (is (= {:conversation-id conv-id :seq-start 1} @deleted)))))))

(deftest fetch-messages-does-not-prune-on-non-missing-error
  (let [conv-id (java.util.UUID/randomUUID)
        delete-called (atom nil)
        calls (atom 0)
        row {:conversation_id conv-id
             :seq_start 1
             :seq_end 2
             :object_key "segments/some-key"}]
    (with-redefs [segments-db/list-segments (fn [_ _]
                                             (let [n (swap! calls inc)]
                                               (if (= n 1) [row] [])))
                  segments-db/delete-segment! (fn [_ args] (reset! delete-called args))
                  minio/get-bytes! (fn [_ _] {:ok false :error "InternalError"})]
      (let [result (reader/fetch-messages {:db :db
                                           :minio :minio
                                           :segments {:segment-batch 1
                                                      :compression :none
                                                      :codec :raw}}
                                          conv-id
                                          {:limit 1 :direction :backward})]
        (testing "non-missing error returns empty messages and does not prune"
          (is (= [] (:messages result)))
          (is (false? (:has-more? result)))
          (is (nil? @delete-called)))))))

(deftest fetch-messages-prunes-on-structured-missing-signals
  (letfn [(run-case [label minio-result]
            (let [conv-id (java.util.UUID/randomUUID)
                  deleted (atom nil)
                  calls (atom 0)
                  row {:conversation_id conv-id
                       :seq_start 5
                       :seq_end 6
                       :object_key "segments/missing"}]
              (with-redefs [segments-db/list-segments (fn [_ _]
                                                        (let [n (swap! calls inc)]
                                                          (if (= n 1) [row] [])))
                            segments-db/delete-segment! (fn [_ args] (reset! deleted args))
                            minio/get-bytes! (fn [_ _] minio-result)]
                (let [result (reader/fetch-messages {:db :db
                                                     :minio :minio
                                                     :segments {:segment-batch 1
                                                                :compression :none
                                                                :codec :raw}}
                                                    conv-id
                                                    {:limit 1 :direction :backward})]
                  (testing label
                    (is (= [] (:messages result)))
                    (is (false? (:has-more? result)))
                    (is (= {:conversation-id conv-id :seq-start 5} @deleted)))))))]
    (run-case "prunes when :error-type is :not-found"
              {:ok false :error-type :not-found})
    (run-case "prunes when :error-code is NoSuchKey"
              {:ok false :error-code "NoSuchKey"})
    (run-case "prunes when :error-code is NotFound"
              {:ok false :error-code "NotFound"})
    (run-case "prunes when :status is 404"
              {:ok false :status 404})))

(deftest fetch-messages-does-not-prune-on-non-404-status
  (let [conv-id (java.util.UUID/randomUUID)
        delete-called (atom nil)
        calls (atom 0)
        row {:conversation_id conv-id
             :seq_start 1
             :seq_end 2
             :object_key "segments/error"}]
    (with-redefs [segments-db/list-segments (fn [_ _]
                                              (let [n (swap! calls inc)]
                                                (if (= n 1) [row] [])))
                  segments-db/delete-segment! (fn [_ args] (reset! delete-called args))
                  minio/get-bytes! (fn [_ _] {:ok false :status 500 :error "InternalError"})]
      (let [result (reader/fetch-messages {:db :db
                                           :minio :minio
                                           :segments {:segment-batch 1
                                                      :compression :none
                                                      :codec :raw}}
                                          conv-id
                                          {:limit 1 :direction :backward})]
        (testing "non-404 status returns empty messages and does not prune"
          (is (= [] (:messages result)))
          (is (false? (:has-more? result)))
          (is (nil? @delete-called)))))))
