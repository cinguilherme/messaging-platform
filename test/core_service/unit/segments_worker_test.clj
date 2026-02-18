(ns core-service.unit.segments-worker-test
  (:require [clojure.test :refer [deftest is testing]]
            [core-service.app.db.segments :as segments-db]
            [core-service.app.observability.logging :as obs-log]
            [core-service.app.segments.format :as segment-format]
            [core-service.app.workers.segments :as segments]
            [d-core.core.storage.protocol :as p-storage]))

(def ^:private store-segment!
  @#'core-service.app.workers.segments/store-segment!)

(deftest store-segment-skips-row-count-query
  (testing "segment flush does not execute segment-row-count query after insert"
    (let [called? (atom false)
          conversation-id (java.util.UUID/randomUUID)
          created-at (System/currentTimeMillis)
          header {:seq_start 1 :seq_end 1 :message_count 1}
          payload (.getBytes "payload" "UTF-8")]
      (with-redefs [segment-format/encode-segment (fn [& _] (.getBytes "segment" "UTF-8"))
                    p-storage/storage-put-bytes (fn [& _] {:ok true :bucket "bucket"})
                    segments-db/insert-segment! (fn [& _] nil)
                    obs-log/stage-logs-enabled? (fn [& _] true)
                    obs-log/log-enabled? (fn [& _] true)
                    segments-db/segment-row-count (fn [& _]
                                                    (reset! called? true)
                                                    (throw (ex-info "segment-row-count should not be called" {})))]
        (is (= :ok (:status (store-segment! {:db :db
                                             :minio :minio
                                             :naming {:minio {:segments-prefix "segments/"}}
                                             :logger nil
                                             :logging {}
                                             :log-ctx {}}
                                            {:conversation-id conversation-id
                                             :header header
                                             :payloads [payload]
                                             :compression :none
                                             :created-at created-at}))))
        (is (false? @called?))))))
