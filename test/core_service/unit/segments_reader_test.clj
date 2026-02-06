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
