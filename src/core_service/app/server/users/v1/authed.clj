(ns core-service.app.server.users.v1.authed
  (:require [cheshire.core :as json]
            [clj-http.client :as http-client]
            [clojure.string :as str]
            [duct.logger :as logger]
            [integrant.core :as ig]
            [core-service.app.db.users :as users-db]
            [core-service.app.server.http :as http]
            [core-service.app.server.openapi :as api-docs]
            [core-service.app.protocols :as protocols]
            [core-service.app.server.users.v1.adapers :as a.users]
            [core-service.app.config.webdeps]
            [d-core.core.auth.token-client :as token-client])
  (:import [core_service.app.config.webdeps WebDeps]))

(defn- fetch-user-by-id
  [{:keys [token-client keycloak]} user-id]
  (when (and token-client (:admin-url keycloak) user-id)
    (let [admin-token (token-client/client-credentials token-client {})
          access-token (:access-token admin-token)
          admin-url (:admin-url keycloak)
          resp (http-client/get (str admin-url "/users/" user-id)
                                (merge {:headers {"authorization" (str "Bearer " access-token)}
                                        :as :text
                                        :throw-exceptions false}
                                       (:http-opts keycloak)))
          status (:status resp)
          parsed (some-> (:body resp) (json/parse-string true))]
      (when (<= 200 status 299)
        (a.users/keycloak-user->profile parsed)))))

(defn- coerce-user-ids
  [ids]
  (when (sequential? ids)
    (->> ids
         (map http/parse-uuid)
         (remove nil?)
         vec)))

(defn users-lookup
  "Lookup users by email via Keycloak admin API or by username via local cache."
  [{:keys [webdeps]}]
  (let [{:keys [db token-client keycloak logger]} webdeps]
    (fn [req]
      (let [query (get-in req [:parameters :query])
            email (a.users/normalize-email (:email query))
            username (a.users/normalize-username (:username query))]
        (when logger
          (logger/log logger ::users-lookup-req email username))
        (cond
          (and (str/blank? email) (str/blank? username))
          {:status 400 :body {:ok false :error "missing email or username"}}

          (and (seq username) (nil? db))
          {:status 500 :body {:ok false :error "lookup backend not configured"}}

          (seq username)
          (let [profile (users-db/fetch-user-profile-by-username db {:username username})
                item (when profile (a.users/profile->item profile))]
            {:ok true
             :items (vec (remove nil? [item]))})

          (or (nil? token-client) (nil? (:admin-url keycloak)))
          {:status 500 :body {:ok false :error "lookup backend not configured"}}

          :else
          (try
            (let [admin-token (token-client/client-credentials token-client {})
                  access-token (:access-token admin-token)
                  admin-url (:admin-url keycloak)
                  resp (http-client/get (str admin-url "/users")
                                        (merge {:headers {"authorization" (str "Bearer " access-token)}
                                                :query-params {:email email
                                                               :exact "true"}
                                                :as :text
                                                :throw-exceptions false}
                                               (:http-opts keycloak)))
                  status (:status resp)
                  parsed (some-> (:body resp) (json/parse-string true))]
              (if (<= 200 status 299)
                {:ok true
                 :items (vec (map a.users/user->item parsed))}
                {:status status
                 :body {:ok false
                        :error "lookup failed"
                        :details parsed}}))
            (catch Exception ex
              {:status 500
               :body {:ok false
                      :error "lookup failed"
                      :details (.getMessage ex)}})))))))

(defn users-lookup-by-ids
  "Lookup users by id via local user_profiles store."
  [{:keys [webdeps]}]
  (let [{:keys [db]} webdeps]
    (fn [req]
      (let [ids (get-in req [:parameters :body :ids])]
        (if (empty? ids)
          {:status 400 :body {:ok false :error "missing ids"}}
          (let [rows (users-db/fetch-user-profiles db {:user-ids ids})
                items (mapv (fn [row]
                              (assoc row :user_id (str (:user_id row))))
                            rows)]
            {:ok true :items items}))))))

(extend-type WebDeps
  protocols/ProfileService
  (resolve-user-profile [this user-id]
    (let [{:keys [db token-client keycloak]} this
          local-profile (users-db/fetch-user-profile db {:user-id user-id})
          fetched-profile (when-not local-profile
                            (fetch-user-by-id {:token-client token-client
                                               :keycloak keycloak}
                                              (str user-id)))]
      (when (and fetched-profile (map? fetched-profile))
        (users-db/upsert-user-profile! db (a.users/profile->db fetched-profile)))
      (or local-profile fetched-profile))))

(defn users-me
  "Resolve the current user from the access token and return a profile.
  Uses local user_profiles cache with a Keycloak admin fallback."
  [{:keys [webdeps]}]
  (let [{:keys [logger]} webdeps]
    (fn [{:keys [user-id]}]
      (when logger
        (logger/log logger ::users-me user-id))
      (if (nil? user-id)
        {:status 401 :body {:ok false :error "invalid user id"}}
        (if-let [profile (protocols/resolve-user-profile webdeps user-id)]
          {:ok true :item (a.users/profile->item profile)}
          {:ok true :item {:user_id (str user-id)}})))))

(defmethod ig/init-key :core-service.app.server.users.v1.authed/routes
  [_ {:keys [webdeps]}]
  ["/v1/users" {:openapi {:id api-docs/docs-id}}
   ["/lookup"
    {:get {:tags ["users"]
           :summary "Lookup users"
           :description "Lookup by `email` (Keycloak) or `username` (local profile cache). At least one query parameter is required."
           :parameters {:query api-docs/UsersLookupQuerySchema}
           :openapi {:security [api-docs/api-key-security]}
           :responses {200 {:body api-docs/UsersItemsResponseSchema}
                       400 {:body api-docs/ErrorEnvelopeSchema}}
           :handler (users-lookup {:webdeps webdeps})}
     :post {:tags ["users"]
            :summary "Lookup users by ids"
            :parameters {:body api-docs/UsersLookupByIdsRequestSchema}
            :openapi {:security [api-docs/api-key-security]}
            :responses {200 {:body api-docs/UsersItemsResponseSchema}
                        400 {:body api-docs/ErrorEnvelopeSchema}}
            :handler (users-lookup-by-ids {:webdeps webdeps})}}]
   ["/me"
    {:get {:tags ["users"]
           :summary "Get current user"
           :description "Resolves the current authenticated user profile."
           :openapi {:security [api-docs/api-key-and-bearer-security]}
           :responses {200 {:body api-docs/UserMeResponseSchema}
                       401 {:body api-docs/ErrorEnvelopeSchema}}
           :handler (users-me {:webdeps webdeps})}}]])
