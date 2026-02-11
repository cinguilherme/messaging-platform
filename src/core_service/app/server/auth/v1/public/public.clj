(ns core-service.app.server.auth.v1.public.public
  (:require [cheshire.core :as json]
            [clj-http.client :as http-client]
            [duct.logger :as logger]
            [integrant.core :as ig]
            [core-service.app.schemas.auth :as auth-schema]
            [core-service.app.server.http :as http]
            [core-service.app.server.openapi :as api-docs]
            [core-service.app.db.users :as users-db]
            [core-service.app.server.auth.v1.public.logic :as logic]
            [d-core.core.auth.token-client :as token-client]
            [malli.core :as m]))

(defn auth-register
  "Proxy endpoint for user registration (backed by Keycloak)."
  [{:keys [webdeps]}]
  (let [{:keys [db token-client keycloak logger]} webdeps]
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
                  token-details (logic/admin-token-details access-token)
                  admin-client-id (or (:admin-client-id keycloak) (:client-id keycloak))
                  log-details {:realm (:realm keycloak)
                               :admin-client-id admin-client-id
                               :admin-url (:admin-url keycloak)
                               :realm-roles (:realm_roles token-details)
                               :realm-management-roles (:realm_management_roles token-details)}
                  admin-url (:admin-url keycloak)
                  user-repr (logic/build-user-repr data)
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
                  (when-let [user-id (logic/parse-user-id-from-location location)]
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
                                    format))))))))

(defn auth-login
  "Proxy endpoint for user login (backed by Keycloak)."
  [{:keys [webdeps]}]
  (let [{:keys [db keycloak token-client logger]} webdeps]
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
                resp (logic/token-request {:token-url token-url
                                     :client-id client-id
                                     :client-secret client-secret
                                     :http-opts http-opts}
                                    params)]
            (logger/log logger ::auth-login-resp resp)
            (if (:ok resp)
              (do
                (when-let [profile (logic/fetch-user-by-username {:token-client token-client
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
                                    format))))))))

(defn auth-refresh
  "Proxy endpoint for token refresh (backed by Keycloak)."
  [{:keys [webdeps]}]
  (let [{:keys [keycloak logger]} webdeps]
    (fn [req]
      (let [format (http/get-accept-format req)
            {:keys [ok data error]} (http/read-json-body req)
            safe-data (cond-> data
                        (contains? data :refresh_token)
                        (assoc :refresh_token "<redacted>"))]
        (logger/log logger ::auth-refresh safe-data)
        (cond
          (not ok) (http/format-response {:ok false :error error} format)
          (not (m/validate auth-schema/RefreshSchema data))
          (http/invalid-response format auth-schema/RefreshSchema data)
          :else
          (let [{:keys [token-url client-id client-secret http-opts]} keycloak
                params (cond-> {:grant_type "refresh_token"
                                :refresh_token (:refresh_token data)}
                         (:scope data) (assoc :scope (:scope data)))
                resp (logic/token-request {:token-url token-url
                                     :client-id client-id
                                     :client-secret client-secret
                                     :http-opts http-opts}
                                    params)]
            (logger/log logger ::auth-refresh-resp resp)
            (if (:ok resp)
              (http/format-response {:ok true
                                     :action :refresh
                                     :token (:data resp)}
                                    format)
              (http/format-response {:ok false
                                     :error "refresh failed"
                                     :status (:status resp)
                                     :details (:error resp)}
                                    format))))))))

(defmethod ig/init-key :core-service.app.server.auth.v1.public/routes
  [_ {:keys [webdeps]}]
  ["/v1/auth" {:openapi {:id api-docs/docs-id}}
   ["/register"
    {:post {:tags ["auth"]
            :summary "Register user"
            :description "Creates a user in Keycloak and persists the profile locally."
            :parameters {:body auth-schema/RegisterSchema}
            :responses {200 {:body api-docs/AuthRegisterResponseSchema}
                        400 {:body api-docs/ErrorEnvelopeSchema}}
            :handler (auth-register {:webdeps webdeps})}}]
   ["/login"
    {:post {:tags ["auth"]
            :summary "Login user"
            :description "Password grant login and returns token payload."
            :parameters {:body auth-schema/LoginSchema}
            :responses {200 {:body api-docs/AuthTokenResponseSchema}
                        400 {:body api-docs/ErrorEnvelopeSchema}}
            :handler (auth-login {:webdeps webdeps})}}]
   ["/refresh"
    {:post {:tags ["auth"]
            :summary "Refresh token"
            :description "Refreshes an access token using a refresh token."
            :parameters {:body auth-schema/RefreshSchema}
            :responses {200 {:body api-docs/AuthTokenResponseSchema}
                        400 {:body api-docs/ErrorEnvelopeSchema}}
            :handler (auth-refresh {:webdeps webdeps})}}]])
