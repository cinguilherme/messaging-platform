(ns core-service.app.db.conversations
  (:require [d-core.core.databases.protocols.simple-sql :as sql]
            [next.jdbc.result-set :as rs]))

(defn create-conversation!
  [db {:keys [tenant-id type title member-ids]}]
  (let [conversation-id (java.util.UUID/randomUUID)
        row (first (sql/insert! db {:id conversation-id
                                    :tenant_id tenant-id
                                    :type (name type)
                                    :title title}
                                {:table :conversations
                                 :returning [:id :tenant_id :type :title :created_at]
                                 :builder-fn rs/as-unqualified-lower-maps}))
        members (->> member-ids distinct vec)]
    (doseq [user-id members]
      (sql/insert! db {:conversation_id conversation-id
                       :user_id user-id
                       :role "member"}
                   {:table :memberships}))
    {:conversation row
     :memberships (count members)}))

(defn member?
  [db {:keys [conversation-id user-id]}]
  (boolean
    (seq
      (sql/select db {:table :memberships
                      :columns [:conversation_id]
                      :where {:conversation_id conversation-id
                              :user_id user-id}
                      :limit 1}))))

(defn list-conversations
  [db {:keys [user-id limit before-ts]}]
  (let [limit (long (or limit 50))
        [query params] (if before-ts
                         [(str "SELECT c.id, c.tenant_id, c.type, c.title, c.created_at "
                               "FROM memberships m "
                               "JOIN conversations c ON c.id = m.conversation_id "
                               "WHERE m.user_id = ? AND c.created_at < ? "
                               "ORDER BY c.created_at DESC "
                               "LIMIT ?")
                          [user-id before-ts limit]]
                         [(str "SELECT c.id, c.tenant_id, c.type, c.title, c.created_at "
                               "FROM memberships m "
                               "JOIN conversations c ON c.id = m.conversation_id "
                               "WHERE m.user_id = ? "
                               "ORDER BY c.created_at DESC "
                               "LIMIT ?")
                          [user-id limit]])]
    (sql/execute! db (into [query] params)
                  {:builder-fn rs/as-unqualified-lower-maps})))
