(ns core-service.unit.unread-index-trim-test
  (:require [clojure.test :refer [deftest is testing]]
            [core-service.app.metrics :as app-metrics]
            [core-service.app.redis.unread-index :as unread-index]))

(def ^:private naming
  {:redis {:message-index-prefix "chat:msg_index:"
           :message-seq-prefix "chat:msg_seq:"
           :last-read-prefix "chat:last_read:"}})

(def ^:private conv-id (java.util.UUID/randomUUID))

(deftest trim-index-guards-missing-required-inputs
  (testing "does not invoke redis metrics wrapper when required args are missing"
    (let [calls (atom [])]
      (with-redefs [app-metrics/with-redis (fn [_ op _f]
                                             (swap! calls conj op)
                                             nil)]
        (is (nil? (unread-index/trim-index! {:redis nil :naming naming}
                                            {:conversation-id conv-id
                                             :seq-end 10
                                             :trim-min-entries 0})))
        (is (nil? (unread-index/trim-index! {:redis :redis :naming nil}
                                            {:conversation-id conv-id
                                             :seq-end 10
                                             :trim-min-entries 0})))
        (is (nil? (unread-index/trim-index! {:redis :redis :naming naming}
                                            {:conversation-id nil
                                             :seq-end 10
                                             :trim-min-entries 0})))
        (is (= [] @calls))))))

(deftest trim-index-by-score-path
  (testing "trim-min-entries <= 0 uses score-based trim path when seq-end is provided"
    (let [calls (atom [])]
      (with-redefs [app-metrics/with-redis (fn [_ op _f]
                                             (swap! calls conj op)
                                             [])]
        (unread-index/trim-index! {:redis :redis :naming naming :metrics :metrics}
                                  {:conversation-id conv-id
                                   :seq-end 42
                                   :trim-min-entries 0})
        (is (= [:message_index_trim] @calls)))))

  (testing "trim-min-entries <= 0 with missing seq-end is a no-op"
    (let [calls (atom [])]
      (with-redefs [app-metrics/with-redis (fn [_ op _f]
                                             (swap! calls conj op)
                                             nil)]
        (is (nil? (unread-index/trim-index! {:redis :redis :naming naming :metrics :metrics}
                                            {:conversation-id conv-id
                                             :seq-end nil
                                             :trim-min-entries 0})))
        (is (= [] @calls))))))

(deftest trim-index-by-keep-n-path
  (testing "trim-min-entries > 0 uses rank-based keep-N trim path"
    (let [calls (atom [])]
      (with-redefs [app-metrics/with-redis (fn [_ op _f]
                                             (swap! calls conj op)
                                             nil)]
        (unread-index/trim-index! {:redis :redis :naming naming :metrics :metrics}
                                  {:conversation-id conv-id
                                   :seq-end nil
                                   :trim-min-entries 100})
        (is (= [:message_index_trim] @calls))))))
