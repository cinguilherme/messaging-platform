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
      (let [data (get-in req [:parameters :body])
            tenant-id (logic/tenant-id-from-request req)]
        (logger/log logger ::conversations-create-req {:tenant-id tenant-id})
        (cond
          (nil? tenant-id)
          {:status 400 :body {:ok false :error "missing tenant"}}

          (empty? (:member_ids data))
          {:status 400 :body {:ok false :error "member_ids cannot be empty"}}

          :else
          (-> (conversations-db/create-conversation!
               db
               {:tenant-id tenant-id
                :type (:type data)
                :title (:title data)
                :member-ids (:member_ids data)})
              (assoc :ok true)))))))

(defn conversations-get
  [{:keys [webdeps]}]
  (let [{:keys [db token-client keycloak]} webdeps]
    (fn [req]
      (let [conv-id (get-in req [:parameters :path :id])
            sender-id (:user-id req)]
        (cond
          (nil? sender-id)
          {:status 401 :body {:ok false :error "invalid sender id"}}

          (not (conversations-db/member? db {:conversation-id conv-id :user-id sender-id}))
          {:status 403 :body {:ok false :error "not a member"}}

          :else
          (let [row (conversations-db/get-conversation db {:conversation-id conv-id})]
            (if-not row
              {:status 404 :body {:ok false :error "conversation not found"}}
              (let [members-by-conv (conversations-db/list-memberships db {:conversation-ids [conv-id]})
                    member-ids (get members-by-conv conv-id)
                    profiles (logic/resolve-member-profiles db token-client keycloak member-ids)
                    members (logic/build-member-items member-ids profiles)
                    item (assoc (logic/conversation-row->detail row)
                                :members members)]
                {:ok true :item item}))))))))

(def ^:private default-conversation-item-timeout-ms 10000)

(defn- futures->items [logger item-timeout-ms sender-id profiles futures]
  (util/ltap logger ::conversations-list-items
             (mapv (fn [{:keys [row member-ids] f :future}]
                     (let [result (deref f item-timeout-ms ::timeout)]
                       (if (= result ::timeout)
                         (do (when logger (logger/log logger ::conversation-item-timeout {:conv-id (:id row)}))
                             (future-cancel f)
                             (logic/conversation-item-fallback row sender-id member-ids profiles))
                         result)))
                   futures)))

(defn- rows->futures [members-by-conv components sender-id profiles rows]
  (mapv (fn [row]
          (let [mids (get members-by-conv (:id row))]
            {:row row
             :member-ids mids
             :future (future (logic/conversation-item components row sender-id mids profiles))}))
        rows))

(defn conversations-list
  [{:keys [webdeps]}]
  (let [{:keys [db token-client keycloak logger] :as components} webdeps
        item-timeout-ms (or (get webdeps :conversations-list-item-timeout-ms) default-conversation-item-timeout-ms)]
    (fn [req]
      (let [{:keys [limit cursor]} (get-in req [:parameters :query])
            sender-id (:user-id req)
            before-ms (http/parse-long cursor nil)
            before-ts (logic/timestamp-from-ms before-ms)]
        (if (nil? sender-id)
          {:status 401 :body {:ok false :error "invalid sender id"}}
          (let [limit (or limit 50)
                rows (util/ltap logger ::conversations-list-rows
                                (conversations-db/list-conversations db {:user-id sender-id
                                                                         :limit limit
                                                                         :before-ts before-ts}))
                conv-ids (mapv :id rows)
                members-by-conv (conversations-db/list-memberships db {:conversation-ids conv-ids})
                member-ids (->> members-by-conv vals (mapcat identity) distinct vec)
                profiles (logic/resolve-member-profiles db token-client keycloak member-ids)
                futures (rows->futures members-by-conv components sender-id profiles rows)
                items (futures->items logger item-timeout-ms sender-id profiles futures)
                next-cursor (when (= (count rows) limit)
                              (some-> (last rows) :created_at (.getTime) str))]
            (logger/log logger ::conversations-list-resp {:count (count items)})
            {:ok true
             :items items
             :next_cursor next-cursor}))))))

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
