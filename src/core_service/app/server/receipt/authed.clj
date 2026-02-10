(ns core-service.app.server.receipt.authed
  (:require [core-service.app.db.conversations :as conversations-db]
            [core-service.app.redis.receipts :as receipts]
            [core-service.app.schemas.messaging :as msg-schema]
            [core-service.app.server.receipt.logic :as logic]
            [core-service.app.server.http :as http]
            [malli.core :as m]))

(defn receipts-create
  [{:keys [webdeps]}]
  (let [{:keys [db redis naming receipt metrics]} webdeps]
    (fn [req]
      (let [format (http/get-accept-format req)
            conv-id (http/parse-uuid (http/param req "id"))
            sender-id (logic/sender-id-from-request req)
            {:keys [ok data error]} (http/read-json-body req)
            data (when ok (logic/coerce-receipt-create data))]
        (cond
          (not conv-id) (http/format-response {:ok false :error "invalid conversation id"} format)
          (nil? sender-id) (http/format-response {:ok false :error "invalid sender id"} format)
          (not (conversations-db/member? db {:conversation-id conv-id :user-id sender-id}))
          (http/format-response {:ok false :error "not a member"} format)
          (not ok) (http/format-response {:ok false :error error} format)
          (not (m/validate msg-schema/ReceiptCreateSchema data))
          (http/invalid-response format msg-schema/ReceiptCreateSchema data)
          :else
          (do
            (receipts/record! {:redis redis
                               :naming naming
                               :receipt receipt
                               :metrics metrics}
                              {:conversation-id conv-id
                               :message-id (:message_id data)
                               :user-id sender-id
                               :receipt-type (:receipt_type data)
                               :at (:at data)})
            (http/format-response {:ok true
                                   :conversation_id (str conv-id)
                                   :message_id (str (:message_id data))
                                   :receipt_type (name (:receipt_type data))}
                                  format)))))))
