(ns core-service.app.config.auth
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [integrant.core :as ig]))

(defn- getenv
  "Returns env var value if present and not blank."
  [k]
  (let [v (some-> (System/getenv k) str/trim)]
    (when (seq v) v)))

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
  (let [env-keys (getenv "API_KEYS")
        file-keys (read-api-keys-file (or file ".api_keys.edn"))
        explicit (when (seq keys) (parse-api-keys keys))
        env (when (seq env-keys) (parse-api-keys env-keys))
        file (when (seq file-keys) file-keys)
        resolved (or explicit env file #{})]
    resolved))

(defmethod ig/init-key :core-service.app.config.auth/keycloak
  [_ {:keys [base-url realm issuer aud jwks-uri token-url admin-url
             client-id client-secret admin-client-id admin-client-secret
             tenant-claim scope-claim
             http-opts]}]
  (let [base-url (or base-url (getenv "KEYCLOAK_BASE_URL") "http://localhost:8080")
        realm (or realm (getenv "KEYCLOAK_REALM") "core-service")
        issuer (or issuer (getenv "KEYCLOAK_ISSUER") (str base-url "/realms/" realm))
        aud (or aud (getenv "KEYCLOAK_AUDIENCE") (getenv "KEYCLOAK_CLIENT_ID"))
        jwks-uri (or jwks-uri (getenv "KEYCLOAK_JWKS_URI")
                     (str base-url "/realms/" realm "/protocol/openid-connect/certs"))
        token-url (or token-url (getenv "KEYCLOAK_TOKEN_URL")
                      (str base-url "/realms/" realm "/protocol/openid-connect/token"))
        admin-url (or admin-url (getenv "KEYCLOAK_ADMIN_URL")
                      (str base-url "/admin/realms/" realm))
        client-id (or client-id (getenv "KEYCLOAK_CLIENT_ID"))
        client-secret (or client-secret (getenv "KEYCLOAK_CLIENT_SECRET"))
        admin-client-id (or admin-client-id (getenv "KEYCLOAK_ADMIN_CLIENT_ID"))
        admin-client-secret (or admin-client-secret (getenv "KEYCLOAK_ADMIN_CLIENT_SECRET"))
        tenant-claim (or tenant-claim (getenv "KEYCLOAK_TENANT_CLAIM") "tenant_id")
        scope-claim (or scope-claim (getenv "KEYCLOAK_SCOPE_CLAIM") "dcore_scope")]
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
     :http-opts http-opts}))

(defmethod ig/init-key :core-service.app.config.auth/keycloak-admin
  [_ {:keys [keycloak]}]
  (let [{:keys [token-url http-opts admin-client-id admin-client-secret
                client-id client-secret]} keycloak]
    {:token-url token-url
     :client-id (or admin-client-id client-id)
     :client-secret (or admin-client-secret client-secret)
     :http-opts http-opts}))
