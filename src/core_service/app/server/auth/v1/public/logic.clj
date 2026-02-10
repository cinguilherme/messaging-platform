(ns core-service.app.server.auth.v1.public.logic
  (:require [cheshire.core :as json]
            [clj-http.client :as http-client]
            [clojure.string :as str]
            [d-core.core.auth.token-client :as token-client]))


(defn token-request
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

(defn decode-base64-url
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

(defn decode-jwt-claims
  [token]
  (try
    (let [parts (str/split (or token "") #"\.")
          payload (nth parts 1 nil)
          bytes (when payload (decode-base64-url payload))]
      (when bytes
        (json/parse-string (String. ^bytes bytes "UTF-8") true)))
    (catch Exception _
      nil)))

(defn admin-token-details
  [token]
  (let [claims (decode-jwt-claims token)]
    {:claims claims
     :realm_roles (get-in claims [:realm_access :roles])
     :realm_management_roles (get-in claims [:resource_access "realm-management" :roles])}))

(defn build-user-repr
  [{:keys [username password email first_name last_name enabled]}]
  (cond-> {:username username
           :enabled (if (some? enabled) enabled true)}
    email (assoc :email email)
    first_name (assoc :firstName first_name)
    last_name (assoc :lastName last_name)
    password (assoc :credentials [{:type "password"
                                   :value password
                                   :temporary false}])))

(defn attribute-value
  [attrs k]
  (let [v (or (get attrs k) (get attrs (keyword (name k))))]
    (cond
      (vector? v) (first v)
      (sequential? v) (first v)
      :else v)))

(defn keycloak-user->profile
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

(defn parse-user-id-from-location
  [location]
  (when (string? location)
    (try
      (java.util.UUID/fromString (last (str/split location #"/")))
      (catch Exception _
        nil))))

(defn fetch-user-by-username
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
