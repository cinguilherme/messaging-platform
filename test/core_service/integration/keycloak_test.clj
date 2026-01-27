(ns core-service.integration.keycloak-test
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [clojure.test :refer [deftest is testing]]
            [d-core.core.authn.protocol :as authn]
            [d-core.core.auth.token-client :as token-client]
            [integrant.core :as ig]))

(defn- env-or
  [k default]
  (or (System/getenv k) default))

(defn- keycloak-config
  []
  (let [base-url (env-or "KEYCLOAK_BASE_URL" "http://localhost:8080")
        realm (env-or "KEYCLOAK_REALM" "d-core")]
    {:base-url base-url
     :realm realm
     :issuer (str base-url "/realms/" realm)
     :jwks-uri (str base-url "/realms/" realm "/protocol/openid-connect/certs")
     :token-url (str base-url "/realms/" realm "/protocol/openid-connect/token")
     :aud (System/getenv "KEYCLOAK_AUDIENCE")
     :scope-claim (env-or "KEYCLOAK_SCOPE_CLAIM" "dcore_scope")
     :tenant-claim (env-or "KEYCLOAK_TENANT_CLAIM" "tenant_id")
     :client-id (env-or "KEYCLOAK_CLIENT_ID" "d-core-api")
     :client-secret (System/getenv "KEYCLOAK_CLIENT_SECRET")
     :admin-client-id (env-or "KEYCLOAK_ADMIN_CLIENT_ID" "d-core-service")
     :admin-client-secret (env-or "KEYCLOAK_ADMIN_CLIENT_SECRET" "d-core-secret")}))

(defn- keycloak-up?
  [{:keys [base-url realm]}]
  (let [url (str base-url "/realms/" realm "/.well-known/openid-configuration")
        resp (http/get url {:as :text
                            :throw-exceptions false
                            :socket-timeout 1000
                            :conn-timeout 1000})]
    (= 200 (:status resp))))

(defn- password-token
  [{:keys [token-url client-id client-secret]}]
  (let [params (cond-> {:grant_type "password"
                        :username "alice"
                        :password "alice"}
                 client-id (assoc :client_id client-id)
                 client-secret (assoc :client_secret client-secret))
        resp (http/post token-url {:form-params params
                                   :as :text
                                   :throw-exceptions false})]
    (when-not (= 200 (:status resp))
      (throw (ex-info "Token request failed"
                      {:status (:status resp)
                       :body (:body resp)})))
    (-> (:body resp) (json/parse-string true) :access_token)))

(deftest keycloak-auth-components
  (let [cfg (keycloak-config)]
    (if-not (keycloak-up? cfg)
      (is false "Keycloak not reachable. Start docker-compose and retry.")
      (testing "JWT authenticator verifies a token from Keycloak"
        (let [token (password-token cfg)
              authenticator (ig/init-key :d-core.core.authn.jwt/authenticator
                                         {:issuer (:issuer cfg)
                                          :aud (:aud cfg)
                                          :jwks-uri (:jwks-uri cfg)
                                          :tenant-claim (:tenant-claim cfg)
                                          :scope-claim (:scope-claim cfg)})
              {:keys [principal]} (authn/authenticate authenticator
                                                     {:headers {"authorization"
                                                                (str "Bearer " token)}}
                                                     {})]
          (is (= "tenant-1" (:tenant-id principal)))
          (is (contains? (:scopes principal) "messages:read"))
          (is (contains? (:scopes principal) "messages:write"))))
      (testing "Token client can obtain a client-credentials token"
        (let [client (ig/init-key :d-core.core.auth/token-client
                                  {:token-url (:token-url cfg)
                                   :client-id (:admin-client-id cfg)
                                   :client-secret (:admin-client-secret cfg)})
              resp (token-client/client-credentials client {})]
          (is (string? (:access-token resp)))
          (is (pos? (count (:access-token resp)))))))))
