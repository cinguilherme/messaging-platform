(ns core-service.app.config.auth
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [core-service.app.libs.env :as env]
            [integrant.core :as ig]))

(defn- parse-long-safe
  [value]
  (when (seq value)
    (try
      (Long/parseLong value)
      (catch Exception _
        nil))))

(defn- positive-long
  [value default]
  (let [v (cond
            (number? value) (long value)
            (string? value) (parse-long-safe value)
            :else nil)]
    (if (and (some? v) (pos? v))
      v
      default)))

(defn- parse-api-keys
  [value]
  (cond
    (set? value) value
    (sequential? value) (->> value (map str) (map str/trim) (remove str/blank?) set)
    (string? value) (->> (str/split value #",") (map str/trim) (remove str/blank?) set)
    :else #{}))

(defn- read-api-keys-file
  [path]
  (let [f (io/file path)]
    (when (.exists f)
      (let [data (edn/read-string (slurp f))]
        (cond
          (set? data) data
          (sequential? data) (set data)
          (map? data) (parse-api-keys (or (:keys data) (get data "keys")))
          :else #{})))))

(defmethod ig/init-key :core-service.app.config.auth/api-keys
  [_ {:keys [keys file]}]
  (let [env-keys (env/getenv "API_KEYS")
        file-keys (read-api-keys-file (or file ".api_keys.edn"))
        explicit (when (seq keys) (parse-api-keys keys))
        env (when (seq env-keys) (parse-api-keys env-keys))
        file (when (seq file-keys) file-keys)
        resolved (or explicit env file #{})]
    resolved))

(defmethod ig/init-key :core-service.app.config.auth/keycloak
  [_ {:keys [base-url realm issuer aud jwks-uri token-url admin-url
             client-id client-secret admin-client-id admin-client-secret
             tenant-claim scope-claim profile-hydration
             http-opts]}]
  (let [base-url (or base-url (env/getenv "KEYCLOAK_BASE_URL") "http://localhost:8080")
        realm (or realm (env/getenv "KEYCLOAK_REALM") "d-core")
        issuer (or issuer (env/getenv "KEYCLOAK_ISSUER") (str base-url "/realms/" realm))
        aud (or aud (env/getenv "KEYCLOAK_AUDIENCE") (env/getenv "KEYCLOAK_CLIENT_ID") "d-core-api")
        jwks-uri (or jwks-uri (env/getenv "KEYCLOAK_JWKS_URI")
                     (str base-url "/realms/" realm "/protocol/openid-connect/certs"))
        token-url (or token-url (env/getenv "KEYCLOAK_TOKEN_URL")
                      (str base-url "/realms/" realm "/protocol/openid-connect/token"))
        admin-url (or admin-url (env/getenv "KEYCLOAK_ADMIN_URL")
                      (str base-url "/admin/realms/" realm))
        client-id (or client-id (env/getenv "KEYCLOAK_CLIENT_ID") "d-core-api")
        client-secret (or client-secret (env/getenv "KEYCLOAK_CLIENT_SECRET"))
        admin-client-id (or admin-client-id (env/getenv "KEYCLOAK_ADMIN_CLIENT_ID") "d-core-service")
        admin-client-secret (or admin-client-secret (env/getenv "KEYCLOAK_ADMIN_CLIENT_SECRET") "d-core-secret")
        tenant-claim (or tenant-claim (env/getenv "KEYCLOAK_TENANT_CLAIM") "tenant_id")
        scope-claim (or scope-claim (env/getenv "KEYCLOAK_SCOPE_CLAIM") "dcore_scope")
        profile-hydration (or profile-hydration {})
        max-concurrency (positive-long (or (:max-concurrency profile-hydration)
                                           (env/getenv "KEYCLOAK_PROFILE_FETCH_MAX_CONCURRENCY"))
                                       8)
        total-timeout-ms (positive-long (or (:total-timeout-ms profile-hydration)
                                            (env/getenv "KEYCLOAK_PROFILE_FETCH_TOTAL_TIMEOUT_MS"))
                                        600)
        request-timeout-ms (positive-long (or (:request-timeout-ms profile-hydration)
                                              (env/getenv "KEYCLOAK_PROFILE_FETCH_REQUEST_TIMEOUT_MS"))
                                          250)]
    {:base-url base-url
     :realm realm
     :issuer issuer
     :aud aud
     :jwks-uri jwks-uri
     :token-url token-url
     :admin-url admin-url
     :client-id client-id
     :client-secret client-secret
     :admin-client-id admin-client-id
     :admin-client-secret admin-client-secret
     :tenant-claim tenant-claim
     :scope-claim scope-claim
     :profile-hydration {:max-concurrency max-concurrency
                         :total-timeout-ms total-timeout-ms
                         :request-timeout-ms request-timeout-ms}
     :http-opts http-opts}))

(defmethod ig/init-key :core-service.app.config.auth/keycloak-admin
  [_ {:keys [keycloak]}]
  (let [{:keys [token-url http-opts admin-client-id admin-client-secret
                client-id client-secret]} keycloak]
    {:token-url token-url
     :client-id (or admin-client-id client-id)
     :client-secret (or admin-client-secret client-secret)
     :http-opts http-opts}))
