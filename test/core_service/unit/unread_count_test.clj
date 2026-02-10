(ns core-service.unit.unread-count-test
  (:require [clojure.test :refer [deftest is testing]]
            [core-service.app.server.conversation.v1.authed.logic :as logic]
            [core-service.app.streams.redis :as streams]))

;; Access the private function via its var
(def ^:private unread-count-from-redis
  @#'core-service.app.server.conversation.v1.authed.logic/unread-count-from-redis)

(defn- make-entry
  "Build a mock stream entry with an EDN-encoded message payload."
  [message-id sender-id]
  {:id (str (System/nanoTime) "-0")
   :payload (pr-str {:message_id message-id
                     :sender_id sender-id
                     :body {:text "hello"}})})

(def ^:private naming {:redis {:stream-prefix "chat:conv:"}})
(def ^:private conv-id (java.util.UUID/randomUUID))
(def ^:private user-a (java.util.UUID/randomUUID))
(def ^:private user-b (java.util.UUID/randomUUID))

;; Helper: rebind both streams/read! and the private batch-receipt-read?
(defmacro with-unread-mocks
  "Rebind streams/read! and batch-receipt-read? for unit testing."
  [read-fn receipt-fn & body]
  `(with-redefs [streams/read! ~read-fn
                 core-service.app.server.conversation.v1.authed.logic/batch-receipt-read? ~receipt-fn]
     ~@body))

(def ^:private no-receipts
  "A batch-receipt-read? mock that returns no receipts."
  (fn [_ _ _ _ _ _] {}))

(deftest empty-stream-returns-zero
  (testing "when stream has no entries, unread count is 0"
    (with-unread-mocks
      (fn [_ _ _ _] {:entries [] :next-cursor nil})
      no-receipts
      (is (= 0 (unread-count-from-redis :redis nil naming conv-id user-a))))))

(deftest all-messages-from-self-returns-zero
  (testing "messages sent by the user themselves are not counted as unread"
    (let [entries [(make-entry (java.util.UUID/randomUUID) user-a)
                   (make-entry (java.util.UUID/randomUUID) user-a)]]
      (with-unread-mocks
        (fn [_ _ _ _] {:entries entries :next-cursor nil})
        no-receipts
        (is (= 0 (unread-count-from-redis :redis nil naming conv-id user-a)))))))

(deftest single-unread-message
  (testing "one message from another user with no read receipt is counted"
    (let [mid (java.util.UUID/randomUUID)
          entries [(make-entry mid user-b)]]
      (with-unread-mocks
        (fn [_ _ _ _] {:entries entries :next-cursor nil})
        no-receipts
        (is (= 1 (unread-count-from-redis :redis nil naming conv-id user-a)))))))

(deftest multiple-unread-messages
  (testing "multiple messages from another user with no receipts are all counted"
    (let [entries [(make-entry (java.util.UUID/randomUUID) user-b)
                   (make-entry (java.util.UUID/randomUUID) user-b)
                   (make-entry (java.util.UUID/randomUUID) user-b)]]
      (with-unread-mocks
        (fn [_ _ _ _] {:entries entries :next-cursor nil})
        no-receipts
        (is (= 3 (unread-count-from-redis :redis nil naming conv-id user-a)))))))

(deftest read-receipt-stops-counting
  (testing "when a read receipt is found, counting stops and earlier messages are excluded"
    (let [mid-1 (java.util.UUID/randomUUID)
          mid-2 (java.util.UUID/randomUUID)
          mid-3 (java.util.UUID/randomUUID)
          ;; Stream is read backward: mid-3 is newest, mid-1 is oldest
          entries [(make-entry mid-3 user-b)
                   (make-entry mid-2 user-b)
                   (make-entry mid-1 user-b)]]
      (with-unread-mocks
        (fn [_ _ _ _] {:entries entries :next-cursor nil})
        ;; mid-2 has a read receipt: mid-3 is unread (1), mid-2 stops the count
        (fn [_ _ _ _ message-ids _]
          (zipmap message-ids
                  (map #(when (= % mid-2) "1234567890") message-ids)))
        (is (= 1 (unread-count-from-redis :redis nil naming conv-id user-a)))))))

(deftest all-messages-read-returns-zero
  (testing "when the newest message has a read receipt, unread count is 0"
    (let [mid (java.util.UUID/randomUUID)
          entries [(make-entry mid user-b)]]
      (with-unread-mocks
        (fn [_ _ _ _] {:entries entries :next-cursor nil})
        (fn [_ _ _ _ message-ids _]
          (zipmap message-ids (repeat "1234567890")))
        (is (= 0 (unread-count-from-redis :redis nil naming conv-id user-a)))))))

(deftest mixed-self-and-other-messages
  (testing "only messages from other users are considered for unread counting"
    (let [entries [(make-entry (java.util.UUID/randomUUID) user-b)       ;; from other, unread
                   (make-entry (java.util.UUID/randomUUID) user-a)       ;; from self, skip
                   (make-entry (java.util.UUID/randomUUID) user-b)]]     ;; from other, unread
      (with-unread-mocks
        (fn [_ _ _ _] {:entries entries :next-cursor nil})
        no-receipts
        (is (= 2 (unread-count-from-redis :redis nil naming conv-id user-a)))))))

(deftest pagination-across-multiple-batches
  (testing "when entries span multiple pages, all batches are processed correctly"
    (let [page-1 [(make-entry (java.util.UUID/randomUUID) user-b)]
          page-2 [(make-entry (java.util.UUID/randomUUID) user-b)]
          page-3 [(make-entry (java.util.UUID/randomUUID) user-b)]
          call-count (atom 0)]
      (with-unread-mocks
        (fn [_ _ _ _opts]
          (let [n (swap! call-count inc)]
            (case (int n)
              1 {:entries page-1 :next-cursor "cursor-1"}
              2 {:entries page-2 :next-cursor "cursor-2"}
              3 {:entries page-3 :next-cursor nil}
              {:entries [] :next-cursor nil})))
        no-receipts
        (is (= 3 (unread-count-from-redis :redis nil naming conv-id user-a)))
        (is (= 3 @call-count) "should have made exactly 3 stream read calls")))))

(deftest pagination-stops-at-read-receipt-in-later-batch
  (testing "pagination stops when a read receipt is found in a subsequent batch"
    (let [mid-1 (java.util.UUID/randomUUID)
          mid-2 (java.util.UUID/randomUUID)
          page-1 [(make-entry mid-1 user-b)]
          page-2 [(make-entry mid-2 user-b)]
          call-count (atom 0)]
      (with-unread-mocks
        (fn [_ _ _ _opts]
          (let [n (swap! call-count inc)]
            (case (int n)
              1 {:entries page-1 :next-cursor "cursor-1"}
              2 {:entries page-2 :next-cursor "cursor-2"}
              {:entries [] :next-cursor nil})))
        ;; mid-2 (in second batch) has a read receipt
        (fn [_ _ _ _ message-ids _]
          (zipmap message-ids
                  (map #(when (= % mid-2) "1234567890") message-ids)))
        ;; mid-1 is unread, mid-2 is read -> stop. Count = 1 (from page 1)
        (is (= 1 (unread-count-from-redis :redis nil naming conv-id user-a)))
        (is (= 2 @call-count) "should stop after finding read receipt in page 2")))))

(deftest nil-inputs-return-nil
  (testing "returns nil when required inputs are missing"
    (is (nil? (unread-count-from-redis nil nil naming conv-id user-a)))
    (is (nil? (unread-count-from-redis :redis nil nil conv-id user-a)))
    (is (nil? (unread-count-from-redis :redis nil naming nil user-a)))
    (is (nil? (unread-count-from-redis :redis nil naming conv-id nil)))))
