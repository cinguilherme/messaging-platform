(ns core-service.app.server.users.v1.authed
  (:require [cheshire.core :as json]
            [clj-http.client :as http-client]
            [clojure.string :as str]
            [duct.logger :as logger]
            [core-service.app.db.users :as users-db]
            [core-service.app.server.http :as http]
            [d-core.core.auth.token-client :as token-client]))

(defn- normalize-email
  [value]
  (some-> value str/trim str/lower-case))

(defn- normalize-username
  [value]
  (let [value (some-> value str/trim)]
    (when (and value (not (str/blank? value)))
      (if (str/starts-with? value "@")
        (subs value 1)
        value))))

(defn- user->item
  [user]
  {:user_id (:id user)
   :email (:email user)
   :username (:username user)
   :first_name (:firstName user)
   :last_name (:lastName user)
   :enabled (:enabled user)})

(defn- attribute-value
  [attrs k]
  (let [v (or (get attrs k) (get attrs (keyword (name k))))]
    (cond
      (vector? v) (first v)
      (sequential? v) (first v)
      :else v)))

(defn- keycloak-user->profile
  [user]
  (let [attrs (:attributes user)]
    {:user_id (:id user)
     :username (:username user)
     :first_name (:firstName user)
     :last_name (:lastName user)
     :avatar_url (or (attribute-value attrs :avatar_url)
                     (attribute-value attrs :avatarUrl))
     :email (:email user)
     :enabled (:enabled user)}))

(defn- profile->item
  [profile]
  (-> profile
      (update :user_id (fn [v] (when v (str v))))
      (select-keys [:user_id :username :first_name :last_name :avatar_url :email :enabled])))

(defn- profile->db
  [profile]
  {:user-id (:user_id profile)
   :username (:username profile)
   :first-name (:first_name profile)
   :last-name (:last_name profile)
   :avatar-url (:avatar_url profile)
   :email (:email profile)
   :enabled (:enabled profile)})

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
        (keycloak-user->profile parsed)))))

(defn- coerce-user-ids
  [ids]
  (when (sequential? ids)
    (->> ids
         (map http/parse-uuid)
         (remove nil?)
         vec)))

(defn users-lookup
  "Lookup users by email via Keycloak admin API or by username via local cache."
  [{:keys [db token-client keycloak]}]
  (fn [req]
    (let [format (http/get-accept-format req)
          email (normalize-email (http/param req "email"))
          username (normalize-username (http/param req "username"))]
      (cond
        (and (str/blank? email) (str/blank? username))
        (http/format-response {:ok false :error "missing email or username"} format)

        (and (seq username) (nil? db))
        (http/format-response {:ok false :error "lookup backend not configured"} format)

        (seq username)
        (let [profile (users-db/fetch-user-profile-by-username db {:username username})
              item (when profile (profile->item profile))]
          (http/format-response {:ok true
                                 :items (vec (remove nil? [item]))}
                                format))

        (or (nil? token-client) (nil? (:admin-url keycloak)))
        (http/format-response {:ok false :error "lookup backend not configured"} format)

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
              (http/format-response {:ok true
                                     :items (vec (map user->item parsed))}
                                    format)
              (http/format-response {:ok false
                                     :error "lookup failed"
                                     :status status
                                     :details parsed}
                                    format)))
          (catch Exception ex
            (http/format-response {:ok false
                                   :error "lookup failed"
                                   :details (.getMessage ex)}
                                  format)))))))

(defn users-lookup-by-ids
  "Lookup users by id via local user_profiles store."
  [{:keys [db]}]
  (fn [req]
    (let [format (http/get-accept-format req)
          {:keys [ok data error]} (http/read-json-body req)
          ids (when ok (coerce-user-ids (:ids data)))]
      (cond
        (not ok)
        (http/format-response {:ok false :error error} format)

        (not (seq ids))
        (http/format-response {:ok false :error "missing ids"} format)

        :else
        (let [rows (users-db/fetch-user-profiles db {:user-ids ids})
              items (mapv (fn [row]
                            (assoc row :user_id (str (:user_id row))))
                          rows)]
          (http/format-response {:ok true :items items} format))))))

(defn users-me
  "Resolve the current user from the access token and return a profile.
  Uses local user_profiles cache with a Keycloak admin fallback."
  [{:keys [db token-client keycloak logger]}]
  (fn [req]
    (let [format (http/get-accept-format req)
          _ (logger/log logger ::users-me-req format)
          user-id (or (http/parse-uuid (get-in req [:auth/principal :subject]))
                      (http/parse-uuid (get-in req [:auth/principal :user_id])))]
      (logger/log logger ::users-me user-id)
      (cond
        (nil? user-id)
        (http/format-response {:ok false :error "invalid user id"} format)

        :else
        (let [local-profile (users-db/fetch-user-profile db {:user-id user-id})
              fetched-profile (when-not local-profile
                                (fetch-user-by-id {:token-client token-client
                                                   :keycloak keycloak}
                                                  (str user-id)))
              profile (or local-profile fetched-profile)]
          (when (and fetched-profile (map? fetched-profile))
            (users-db/upsert-user-profile! db (profile->db fetched-profile)))
          (if profile
            (http/format-response {:ok true :item (profile->item profile)} format)
            (http/format-response {:ok true :item {:user_id (str user-id)}} format)))))))
