(ns core-service.app.server.conversation.v1.public
  (:require [cheshire.core :as json]
            [clj-http.client :as http-client]
            [clojure.string :as str]
            [duct.logger :as logger]
            [core-service.app.schemas.auth :as auth-schema]
            [core-service.app.server.http :as http]
            [core-service.app.db.users :as users-db]
            [d-core.core.auth.token-client :as token-client]
            [malli.core :as m]))

(defn- token-request
  [{:keys [token-url client-id client-secret http-opts]} params]
  (let [params (cond-> params
                 client-id (assoc :client_id client-id)
                 client-secret (assoc :client_secret client-secret))
        resp (http-client/post token-url (merge {:form-params params
                                                 :as :text
                                                 :throw-exceptions false}
                                                http-opts))
        status (:status resp)
        parsed (some-> (:body resp) (json/parse-string true))]
    (if (<= 200 status 299)
      {:ok true :data parsed}
      {:ok false :status status :error parsed})))

(defn- decode-base64-url
  [value]
  (try
    (let [pad (mod (count value) 4)
          padded (cond
                   (= pad 2) (str value "==")
                   (= pad 3) (str value "=")
                   (= pad 1) (str value "===")
                   :else value)]
      (.decode (java.util.Base64/getUrlDecoder) padded))
    (catch Exception _
      nil)))

(defn- decode-jwt-claims
  [token]
  (try
    (let [parts (str/split (or token "") #"\.")
          payload (nth parts 1 nil)
          bytes (when payload (decode-base64-url payload))]
      (when bytes
        (json/parse-string (String. ^bytes bytes "UTF-8") true)))
    (catch Exception _
      nil)))

(defn- admin-token-details
  [token]
  (let [claims (decode-jwt-claims token)]
    {:claims claims
     :realm_roles (get-in claims [:realm_access :roles])
     :realm_management_roles (get-in claims [:resource_access "realm-management" :roles])}))

(defn- build-user-repr
  [{:keys [username password email first_name last_name enabled]}]
  (cond-> {:username username
           :enabled (if (some? enabled) enabled true)}
    email (assoc :email email)
    first_name (assoc :firstName first_name)
    last_name (assoc :lastName last_name)
    password (assoc :credentials [{:type "password"
                                   :value password
                                   :temporary false}])))

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
    {:user-id (:id user)
     :username (:username user)
     :first-name (:firstName user)
     :last-name (:lastName user)
     :avatar-url (or (attribute-value attrs :avatar_url)
                     (attribute-value attrs :avatarUrl))
     :email (:email user)
     :enabled (:enabled user)}))

(defn- parse-user-id-from-location
  [location]
  (when (string? location)
    (try
      (java.util.UUID/fromString (last (str/split location #"/")))
      (catch Exception _
        nil))))

(defn- fetch-user-by-username
  [{:keys [token-client keycloak]} username]
  (when (and token-client (:admin-url keycloak) (seq username))
    (let [admin-token (token-client/client-credentials token-client {})
          access-token (:access-token admin-token)
          admin-url (:admin-url keycloak)
          resp (http-client/get (str admin-url "/users")
                                (merge {:headers {"authorization" (str "Bearer " access-token)}
                                        :query-params {:username username
                                                       :exact "true"}
                                        :as :text
                                        :throw-exceptions false}
                                       (:http-opts keycloak)))
          status (:status resp)
          parsed (some-> (:body resp) (json/parse-string true))
          user (first parsed)]
      (when (and (<= 200 status 299) user)
        (keycloak-user->profile user)))))

(defn auth-register
  "Proxy endpoint for user registration (backed by Keycloak)."
  [{:keys [db token-client keycloak logger]}]
  (fn [req]
    (let [format (http/get-accept-format req)
          {:keys [ok data error]} (http/read-json-body req)]
      (logger/log logger ::auth-register data)
      (cond
        (not ok) (http/format-response {:ok false :error error} format)
        (not (m/validate auth-schema/RegisterSchema data))
        (http/invalid-response format auth-schema/RegisterSchema data)
        (or (nil? token-client) (nil? (:admin-url keycloak)))
        (http/format-response {:ok false :error "auth backend not configured"} format)
        (nil? (:token-url keycloak))
        (http/format-response {:ok false :error "token endpoint not configured"} format)
        (nil? (:client-id keycloak))
        (http/format-response {:ok false :error "client id not configured"} format)
        :else
        (try
          (logger/log logger ::auth-register-token-client token-client)
          (let [admin-token (token-client/client-credentials token-client {})
                access-token (:access-token admin-token)
                token-details (admin-token-details access-token)
                admin-client-id (or (:admin-client-id keycloak) (:client-id keycloak))
                log-details {:realm (:realm keycloak)
                             :admin-client-id admin-client-id
                             :admin-url (:admin-url keycloak)
                             :realm-roles (:realm_roles token-details)
                             :realm-management-roles (:realm_management_roles token-details)}
                admin-url (:admin-url keycloak)
                user-repr (build-user-repr data)
                resp (http-client/post (str admin-url "/users")
                                       (merge {:headers {"authorization" (str "Bearer " access-token)}
                                               :content-type :json
                                               :body (json/generate-string user-repr)
                                               :as :text
                                               :throw-exceptions false}
                                              (:http-opts keycloak)))
                status (:status resp)
                location (get-in resp [:headers "location"])]
            (logger/log logger ::auth-register-admin-token log-details)
            (logger/log logger ::auth-register-resp resp)
            (if (<= 200 status 299)
              (do
                (when-let [user-id (parse-user-id-from-location location)]
                  (when db
                    (users-db/upsert-user-profile! db {:user-id user-id
                                                       :username (:username data)
                                                       :first-name (:first_name data)
                                                       :last-name (:last_name data)
                                                       :email (:email data)
                                                       :enabled (if (some? (:enabled data)) (:enabled data) true)})))
                (http/format-response {:ok true
                                       :action :register
                                       :location location}
                                      format))
              (http/format-response {:ok false
                                     :error "registration failed"
                                     :status status
                                     :details (some-> (:body resp) (json/parse-string true))
                                     :debug log-details}
                                    format)))
          (catch Exception ex
            (http/format-response {:ok false
                                   :error "registration failed"
                                   :details (.getMessage ex)}
                                  format)))))))

(defn auth-login
  "Proxy endpoint for user login (backed by Keycloak)."
  [{:keys [db keycloak token-client logger]}]
  (fn [req]
    (let [format (http/get-accept-format req)
          {:keys [ok data error]} (http/read-json-body req)]
      (logger/log logger ::auth-login data)
      (cond
        (not ok) (http/format-response {:ok false :error error} format)
        (not (m/validate auth-schema/LoginSchema data))
        (http/invalid-response format auth-schema/LoginSchema data)
        :else
        (let [{:keys [token-url client-id client-secret http-opts]} keycloak
              params (cond-> {:grant_type "password"
                              :username (:username data)
                              :password (:password data)}
                       (:scope data) (assoc :scope (:scope data)))
              resp (token-request {:token-url token-url
                                   :client-id client-id
                                   :client-secret client-secret
                                   :http-opts http-opts}
                                  params)]
          (logger/log logger ::auth-login-resp resp)
          (if (:ok resp)
            (do
              (when-let [profile (fetch-user-by-username {:token-client token-client
                                                          :keycloak keycloak}
                                                         (:username data))]
                (when db
                  (users-db/upsert-user-profile! db profile)))
              (http/format-response {:ok true
                                     :action :login
                                     :token (:data resp)}
                                    format))
            (http/format-response {:ok false
                                   :error "login failed"
                                   :status (:status resp)
                                   :details (:error resp)}
                                  format)))))))
