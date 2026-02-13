(ns core-service.app.server.conversation.v1.authed.authed
  (:require [core-service.app.db.conversations :as conversations-db]
            [core-service.app.libs.util :as util]
            [core-service.app.schemas.messaging :as msg-schema]
            [core-service.app.server.conversation.v1.authed.logic :as logic]
            [core-service.app.server.http :as http]
            [core-service.app.server.openapi :as api-docs]
            [core-service.app.server.message.authed :as message-authed]
            [core-service.app.server.message.routes :as message-routes]
            [core-service.app.server.receipt.authed :as receipt-authed]
            [core-service.app.server.receipt.routes :as receipt-routes]
            [core-service.app.server.attachment.routes :as attachment-routes]
            [duct.logger :as logger]
            [integrant.core :as ig]
            [malli.core :as m]))

(defn conversations-create
  [{:keys [webdeps]}]
  (let [{:keys [db logger]} webdeps]
    (fn [req]
      (let [format (http/get-accept-format req)
            {:keys [ok data error]} (http/read-json-body req)
            data (when ok (logic/coerce-conversation-create data))
            tenant-id (logic/tenant-id-from-request req)]
        (logger/log logger ::conversations-create-req format)
        (cond
          (not ok) (http/format-response {:ok false :error error} format)
          (nil? tenant-id) (http/format-response {:ok false :error "missing tenant"} format)
          (not (m/validate msg-schema/ConversationCreateSchema data))
          (http/invalid-response format msg-schema/ConversationCreateSchema data)
          (empty? (:member_ids data))
          (http/format-response {:ok false :error "member_ids cannot be empty"} format)
          :else
          (let [result (conversations-db/create-conversation!
                        db
                        {:tenant-id tenant-id
                         :type (:type data)
                         :title (:title data)
                         :member-ids (:member_ids data)})]
            (http/format-response (assoc result :ok true) format)))))))

(defn conversations-get
  [{:keys [webdeps]}]
  (let [{:keys [db token-client keycloak]} webdeps]
    (fn [req]
      (let [format (http/get-accept-format req)
            conv-id (http/parse-uuid (http/param req "id"))
            sender-id (logic/sender-id-from-request req)]
        (cond
          (not conv-id)
          (http/format-response {:ok false :error "invalid conversation id"} format)

          (nil? sender-id)
          (http/format-response {:ok false :error "invalid sender id"} format)

          (not (conversations-db/member? db {:conversation-id conv-id :user-id sender-id}))
          (http/format-response {:ok false :error "not a member"} format)

          :else
          (let [row (conversations-db/get-conversation db {:conversation-id conv-id})]
            (if-not row
              (http/format-response {:ok false :error "conversation not found"} format)
              (let [members-by-conv (conversations-db/list-memberships db {:conversation-ids [conv-id]})
                    member-ids (get members-by-conv conv-id)
                    profiles (logic/resolve-member-profiles db token-client keycloak member-ids)
                    members (logic/build-member-items member-ids profiles)
                    item (assoc (logic/conversation-row->detail row)
                                :members members)]
                (http/format-response {:ok true :item item} format)))))))))

(def ^:private default-conversation-item-timeout-ms 10000)

(defn conversations-list
  [{:keys [webdeps]}]
  (let [{:keys [db token-client keycloak logger] :as components} webdeps
        item-timeout-ms (or (get webdeps :conversations-list-item-timeout-ms) default-conversation-item-timeout-ms)]
    (fn [req]
      (let [ttap (partial util/ltap logger ::conversations-list-req)
            format (ttap (http/get-accept-format req))
            sender-id (logic/sender-id-from-request req)
            limit (http/parse-long (http/param req "limit") 50)
            cursor-param (http/param req "cursor")
            before-ms (http/parse-long cursor-param ::invalid)
            invalid-cursor? (and (some? cursor-param) (= before-ms ::invalid))
            before-ms (when-not invalid-cursor? before-ms)
            before-ts (logic/timestamp-from-ms before-ms)]
        (cond
          (nil? sender-id) (http/format-response {:ok false :error "invalid sender id"} format)
          invalid-cursor? (http/format-response {:ok false :error "invalid cursor"} format)
          :else
          (let [rows (ttap (conversations-db/list-conversations db {:user-id sender-id
                                                                    :limit limit
                                                                    :before-ts before-ts}))
                conv-ids (ttap (mapv :id rows))
                members-by-conv (util/ltap logger ::conversations-list-members-by-conv (conversations-db/list-memberships db {:conversation-ids conv-ids}))
                member-ids (util/ltap logger ::conversations-list-member-ids (->> members-by-conv vals (mapcat identity) distinct ttap))
                profiles (util/ltap logger ::conversations-list-profiles (logic/resolve-member-profiles db token-client keycloak member-ids))
                futures (mapv (fn [row]
                                (let [mids (get members-by-conv (:id row))]
                                  {:row row
                                   :member-ids mids
                                   :future (future (logic/conversation-item components row sender-id mids profiles))}))
                              rows)
                items (util/ltap logger ::conversations-list-items
                                 (mapv (fn [{:keys [row member-ids] f :future}]
                                         (let [result (deref f item-timeout-ms ::timeout)]
                                           (if (= result ::timeout)
                                             (do (when logger (logger/log logger ::conversation-item-timeout {:conv-id (:id row)}))
                                                 (future-cancel f)
                                                 (logic/conversation-item-fallback row sender-id member-ids profiles))
                                             result)))
                                       futures))
                next-cursor (util/ltap logger ::conversations-list-next-cursor (when (= (count rows) limit)
                                                                                 (some-> (last rows) :created_at (.getTime) str)))
                _ (logger/log logger ::conversations-list-end-let items next-cursor)]
            (logger/log logger ::conversations-list-resp items next-cursor)
            (http/format-response {:ok true
                                   :items items
                                   :next_cursor next-cursor}
                                  format)))))))

;; Keep compatibility with existing tests/callers while handlers live in resource namespaces.
(defn messages-create
  [{:keys [webdeps]}]
  (message-authed/messages-create {:webdeps webdeps}))

(defn messages-list
  [{:keys [webdeps]}]
  (message-authed/messages-list {:webdeps webdeps}))

(defn receipts-create
  [{:keys [webdeps]}]
  (receipt-authed/receipts-create {:webdeps webdeps}))

(defmethod ig/init-key :core-service.app.server.conversation.v1.authed/routes
  [_ {:keys [webdeps attachment-workers]}]
  (let [attachment-webdeps (assoc webdeps :attachment-workers attachment-workers)]
    ["/v1/conversations"
     ["" {:openapi {:id api-docs/docs-id}
          :post {:tags ["conversations"]
                 :summary "Create conversation"
                 :parameters {:body msg-schema/ConversationCreateSchema}
                 :openapi {:security [api-docs/api-key-and-bearer-security]}
                 :responses {200 {:body api-docs/ConversationCreateResponseSchema}
                             400 {:body api-docs/ErrorEnvelopeSchema}}
                 :handler (conversations-create {:webdeps webdeps})}
          :get {:tags ["conversations"]
                :summary "List conversations"
                :description "Returns newest-first conversations. `cursor` must be the previous page `next_cursor` timestamp in milliseconds."
                :parameters {:query api-docs/ConversationListQuerySchema}
                :openapi {:security [api-docs/api-key-and-bearer-security]}
                :responses {200 {:body api-docs/ConversationsListResponseSchema}
                            400 {:body api-docs/ErrorEnvelopeSchema}}
                :handler (conversations-list {:webdeps webdeps})}}]
     ["/:id" {:openapi {:id api-docs/docs-id}
              :get {:tags ["conversations"]
                    :summary "Get conversation details"
                    :parameters {:path api-docs/PathConversationIdSchema}
                    :openapi {:security [api-docs/api-key-and-bearer-security]}
                    :responses {200 {:body api-docs/ConversationDetailResponseSchema}
                                400 {:body api-docs/ErrorEnvelopeSchema}}
                    :handler (conversations-get {:webdeps webdeps})}}]
     (message-routes/routes {:webdeps webdeps})
     (attachment-routes/routes {:webdeps attachment-webdeps})
     (receipt-routes/routes {:webdeps webdeps})]))
