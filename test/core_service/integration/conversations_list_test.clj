(ns core-service.integration.conversations-list-test
  (:require [cheshire.core :as json]
            [clojure.test :refer [deftest is testing]]
            [core-service.app.config.clients]
            [core-service.app.config.messaging]
            [core-service.app.config.storage]
            [core-service.app.server.conversation.v1.authed :as authed]
            [core-service.app.segments.reader :as segment-reader]
            [core-service.app.workers.segments :as segments]
            [d-core.core.storage.protocol :as p-storage]
            [core-service.integration.helpers :as helpers]
            [d-core.core.clients.redis]
            [d-core.core.storage.minio]
            [d-core.core.databases.protocols.simple-sql :as sql]
            [integrant.core :as ig]))

(defn- make-components []
  (let [redis-cfg (ig/init-key :core-service.app.config.clients/redis {})
        redis (ig/init-key :d-core.core.clients.redis/client redis-cfg)
        minio-cfg (ig/init-key :core-service.app.config.storage/minio {})
        minio (ig/init-key :d-core.core.storage/minio minio-cfg)
        naming (ig/init-key :core-service.app.config.messaging/storage-names {})
        idempotency (ig/init-key :core-service.app.config.messaging/idempotency-config {})
        receipt (ig/init-key :core-service.app.config.messaging/receipt-config {})
        segment-config (ig/init-key :core-service.app.config.messaging/segment-config {})
        timeout-ms 100]
    {:redis redis
     :minio minio
     :naming naming
     :idempotency idempotency
     :receipt receipt
     :segments segment-config
     :timeout-ms timeout-ms}))

(defn- make-webdeps
  [{:keys [db redis naming idempotency receipt minio segments timeout-ms]}]
  (cond-> {:db db}
    redis (assoc :redis redis)
    naming (assoc :naming naming)
    idempotency (assoc :idempotency idempotency)
    receipt (assoc :receipt receipt)
    minio (assoc :minio minio)
    segments (assoc :segments segments)
    timeout-ms (assoc :conversations-list-item-timeout-ms timeout-ms)))

(defn- make-handlers
  [webdeps]
  {:create (authed/messages-create {:webdeps webdeps})
   :receipt (authed/receipts-create {:webdeps webdeps})
   :list (authed/conversations-list {:webdeps webdeps})})

(defmacro with-db
  [[db client] & body]
  `(let [{:keys [~'db ~'client]} (helpers/init-db)]
     (try
       (let [~db ~'db ~client ~'client]
         ~@body)
       (finally
         (ig/halt-key! :d-core.core.clients.postgres/client ~'client)))))

(defmacro with-db-cleanup
  [[db client] cleanup & body]
  `(let [{:keys [~'db ~'client]} (helpers/init-db)]
     (try
       (let [~db ~'db ~client ~'client]
         ~@body)
       (finally
         ~cleanup
         (ig/halt-key! :d-core.core.clients.postgres/client ~'client)))))

(def ^:private accept-json {"accept" "application/json"})

(defn- parse-body
  [resp]
  (json/parse-string (:body resp) true))

(defn- auth-principal
  ([user-id] {:subject (str user-id)})
  ([user-id tenant-id] {:subject (str user-id) :tenant-id tenant-id}))

(defn- authed-get
  [handler user-id & {:keys [query-params headers]}]
  (handler {:request-method :get
            :headers (merge accept-json headers)
            :query-params query-params
            :auth/principal (auth-principal user-id)}))

(defn- authed-post
  [handler user-id & {:keys [params body headers tenant-id]}]
  (handler {:request-method :post
            :headers (merge accept-json headers)
            :params params
            :body body
            :auth/principal (auth-principal user-id tenant-id)}))

(def test-ids {:sender-id (java.util.UUID/randomUUID)
               :receiver-id (java.util.UUID/randomUUID)
               :conv-id (java.util.UUID/randomUUID)
               :conv-new (java.util.UUID/randomUUID)
               :conv-old (java.util.UUID/randomUUID)
               :ts-old 1700000000000
               :ts-new 1700000005000
               :payload (json/generate-string {:type "text"
                                               :body {:text "hello"}})})

(deftest conversations-list-requires-sender
  (with-db [db client]
    (let [{:keys [list]} (make-handlers (make-webdeps {:db db :client client}))
          resp (list {:request-method :get
                      :headers accept-json})
          body (parse-body resp)]
      (testing "missing sender id"
        (is (= 200 (:status resp)))
        (is (= false (:ok body)))
        (is (= "invalid sender id" (:error body)))))))

(deftest conversations-list-returns-items
  (let [{:keys [sender-id conv-id]} test-ids]
    (with-db-cleanup [db client]
      (helpers/cleanup-conversation! db conv-id)
      (let [{:keys [list]} (make-handlers (make-webdeps {:db db}))]
      (helpers/setup-conversation! db {:conversation-id conv-id
                                       :user-id sender-id
                                       :title "Test"})
      (let [resp (authed-get list sender-id)
            body (parse-body resp)
            item (first (:items body))]
        (testing "response shape"
          (is (= 200 (:status resp)))
          (is (:ok body))
          (is (= (str conv-id) (:conversation_id item)))
          (is (contains? item :updated_at))
          (is (contains? item :last_message))
          (is (contains? item :unread_count))
          (is (some #(= (str sender-id) (:user_id %)) (:members item)))))))))

(deftest conversations-list-paginates-with-cursor
  (let [{:keys [sender-id conv-old conv-new ts-old ts-new]} test-ids]
    (with-db-cleanup [db client]
      (do
        (helpers/cleanup-conversation! db conv-old)
        (helpers/cleanup-conversation! db conv-new))
      (let [{:keys [list]} (make-handlers (make-webdeps {:db db}))]
      (helpers/setup-conversation! db {:conversation-id conv-old
                                       :user-id sender-id
                                       :title "Old"})
      (helpers/setup-conversation! db {:conversation-id conv-new
                                       :user-id sender-id
                                       :title "New"})
      (sql/execute! db ["UPDATE conversations SET created_at = ? WHERE id = ?"
                        (java.sql.Timestamp. ts-old)
                        conv-old]
                    {})
      (sql/execute! db ["UPDATE conversations SET created_at = ? WHERE id = ?"
                        (java.sql.Timestamp. ts-new)
                        conv-new]
                    {})
      (let [resp (authed-get list sender-id :query-params {"limit" "1"})
            body (parse-body resp)
            item (first (:items body))
            cursor (:next_cursor body)]
        (testing "first page returns newest"
          (is (= 200 (:status resp)))
          (is (:ok body))
          (is (= (str conv-new) (:conversation_id item)))
          (is (= (str ts-new) cursor)))
        (let [resp2 (authed-get list sender-id :query-params {"limit" "1"
                                                             "cursor" cursor})
              body2 (parse-body resp2)
              item2 (first (:items body2))]
          (testing "second page returns older"
            (is (= 200 (:status resp2)))
            (is (:ok body2))
            (is (= (str conv-old) (:conversation_id item2))))))))))

(deftest conversations-list-invalid-cursor
  (with-db [db client]
    (let [{:keys [list]} (make-handlers (make-webdeps {:db db}))
          {:keys [sender-id]} test-ids
          resp (authed-get list sender-id :query-params {"cursor" "nope"})
          body (parse-body resp)]
      (testing "invalid cursor"
        (is (= 200 (:status resp)))
        (is (= false (:ok body)))
        (is (= "invalid cursor" (:error body)))))))

(deftest conversations-list-last-message-and-unread
  (let [{:keys [naming idempotency receipt redis]} (make-components)]
    (if-not (helpers/redis-up? redis)
      (is false "Redis not reachable. Start docker-compose and retry.")
      (let [{:keys [sender-id conv-id receiver-id payload]} test-ids]
        (with-db-cleanup [db client]
          (do
            (helpers/clear-redis-conversation! redis naming conv-id)
            (helpers/cleanup-conversation! db conv-id))
          (let [webdeps (make-webdeps {:db db
                                       :redis redis
                                       :naming naming
                                       :idempotency idempotency
                                       :receipt receipt})
                {:keys [create receipt list]} (make-handlers webdeps)]
            (helpers/setup-conversation! db {:conversation-id conv-id :user-id sender-id})
            (helpers/ensure-membership! db {:conversation-id conv-id :user-id receiver-id})
            (helpers/clear-redis-conversation! redis naming conv-id)
            (let [resp (authed-post create sender-id
                                    :headers {"idempotency-key" (str (java.util.UUID/randomUUID))}
                                    :params {:id (str conv-id)}
                                    :body payload
                                    :tenant-id "tenant-1")
                  body (parse-body resp)
                  message-id (get-in body [:message :message_id])]
              (testing "list shows last_message and unread_count before receipt"
                (let [resp1 (authed-get list receiver-id)
                      body1 (parse-body resp1)
                      item (first (:items body1))]
                  (is (= 200 (:status resp1)))
                  (is (:ok body1))
                  (is (= "hello" (get-in item [:last_message :body :text])))
                  (is (= 1 (:unread_count item)))))
              (authed-post receipt receiver-id
                           :params {:id (str conv-id)}
                           :body (json/generate-string {:receipt_type "read"
                                                        :message_id message-id})
                           :tenant-id "tenant-1")
              (testing "list shows unread_count 0 after read receipt"
                (let [resp2 (authed-get list receiver-id)
                      body2 (parse-body resp2)
                      item2 (first (:items body2))]
                  (is (= 200 (:status resp2)))
                  (is (:ok body2))
                  (is (= "hello" (get-in item2 [:last_message :body :text])))
                  (is (= 0 (:unread_count item2))))))))))))

(deftest conversations-list-missing-minio-segment
  (let [{:keys [redis minio naming idempotency segments]} (make-components)
        {:keys [sender-id conv-id payload]} test-ids]
    (if-not (and (helpers/redis-up? redis) (helpers/minio-up? minio))
      (is false "Redis or Minio not reachable. Start docker-compose and retry.")
      (with-db-cleanup [db client]
        (do
          (helpers/clear-redis-conversation! redis naming conv-id)
          (helpers/cleanup-segment-object-and-index! db minio conv-id)
          (helpers/cleanup-conversation! db conv-id))
        (let [webdeps (make-webdeps {:db db
                                     :redis redis
                                     :naming naming
                                     :idempotency idempotency
                                     :minio minio
                                     :segments segments})
              {:keys [create list]} (make-handlers webdeps)
              row (do
                    (helpers/setup-conversation! db {:conversation-id conv-id
                                                     :user-id sender-id})
                    (helpers/clear-redis-conversation! redis naming conv-id)
                    (authed-post create sender-id
                                 :headers {"idempotency-key" (str (java.util.UUID/randomUUID))}
                                 :params {:id (str conv-id)}
                                 :body payload
                                 :tenant-id "tenant-1")
                    (segments/flush-conversation! webdeps conv-id)
                    (first (sql/select db {:table :segment_index
                                           :where {:conversation_id conv-id}})))
              object-key (:object_key row)
              resp (do
                     (testing "segment created"
                       (is (string? object-key)))
                     (p-storage/storage-delete minio object-key {})
                     (testing "segment index remains after object removal"
                       (is (seq (sql/select db {:table :segment_index
                                                :where {:conversation_id conv-id}}))))
                     (helpers/clear-redis-conversation! redis naming conv-id)
                     (authed-get list sender-id))
              body (parse-body resp)
              item (first (:items body))]
          (testing "list returns even when minio segment is missing"
            (is (= 200 (:status resp)))
            (is (:ok body))
            (is (= (str conv-id) (:conversation_id item)))
            (is (nil? (:last_message item)))
            (is (= 0 (:unread_count item))))
          (testing "stale segment index is removed"
            (is (empty? (sql/select db {:table :segment_index
                                        :where {:conversation_id conv-id}})))))))))

(deftest conversations-list-timeout-returns-fallback-item
  (let [{:keys [sender-id conv-id]} test-ids
        blocking-promise (promise)]
    (with-db-cleanup [db client]
      (do
        (helpers/cleanup-conversation! db conv-id))
      (let [webdeps (make-webdeps {:db db
                                   :minio :fake-minio
                                   :segments {:segment-batch 1
                                              :compression :none
                                              :codec :raw}
                                   :timeout-ms 50})
            {:keys [list]} (make-handlers webdeps)]
        (helpers/setup-conversation! db {:conversation-id conv-id
                                         :user-id sender-id})
        (with-redefs [segment-reader/fetch-messages (fn [_ _ _] @blocking-promise)]
          (let [resp (authed-get list sender-id)
                body (parse-body resp)
                item (first (:items body))]
            (testing "list returns 200 when one conversation item would hang"
              (is (= 200 (:status resp)))
              (is (:ok body))
              (is (= (str conv-id) (:conversation_id item)))
              (is (nil? (:last_message item)))
              (is (= 0 (:unread_count item)))))
          (deliver blocking-promise {:messages [] :has-more? false}))))))
