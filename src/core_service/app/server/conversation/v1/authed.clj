(ns core-service.app.server.conversation.v1.authed
  (:require [core-service.app.schemas.messaging :as msg-schema]
            [core-service.app.server.http :as http]
            [malli.core :as m]
            [malli.error :as me]))

(defn conversations-create
  [_options]
  (fn [req]
    (let [format (http/get-accept-format req)
          {:keys [ok data error]} (http/read-json-body req)]
      (cond
        (not ok) (http/format-response {:ok false :error error} format)
        (not (m/validate msg-schema/ConversationCreateSchema data))
        (http/invalid-response format msg-schema/ConversationCreateSchema data)
        :else
        (http/format-response {:ok true :conversation data} format)))))

(defn conversations-get
  [_options]
  (fn [req]
    (let [format (http/get-accept-format req)
          conv-id (http/parse-uuid (http/param req "id"))]
      (if-not conv-id
        (http/format-response {:ok false :error "invalid conversation id"} format)
        (http/format-response {:ok true :conversation_id (str conv-id)} format)))))

(defn messages-create
  [_options]
  (fn [req]
    (let [format (http/get-accept-format req)
          conv-id (http/parse-uuid (http/param req "id"))
          {:keys [ok data error]} (http/read-json-body req)]
      (cond
        (not conv-id) (http/format-response {:ok false :error "invalid conversation id"} format)
        (not ok) (http/format-response {:ok false :error error} format)
        (not (m/validate msg-schema/MessageCreateSchema data))
        (http/invalid-response format msg-schema/MessageCreateSchema data)
        :else
        (http/format-response {:ok true
                               :conversation_id (str conv-id)
                               :message data}
                              format)))))

(defn messages-list
  [_options]
  (fn [req]
    (let [format (http/get-accept-format req)
          conv-id (http/parse-uuid (http/param req "id"))
          limit (http/parse-long (http/param req "limit") 50)
          cursor (http/param req "cursor")
          direction (some-> (http/param req "direction") keyword)
          query {:limit limit
                 :cursor cursor
                 :direction direction}]
      (cond
        (not conv-id) (http/format-response {:ok false :error "invalid conversation id"} format)
        (not (m/validate msg-schema/PaginationQuerySchema query))
        (http/format-response {:ok false
                               :error "invalid query"
                               :details (me/humanize (m/explain msg-schema/PaginationQuerySchema query))}
                              format)
        :else
        (http/format-response {:ok true
                               :conversation_id (str conv-id)
                               :messages []
                               :next_cursor nil}
                              format)))))
