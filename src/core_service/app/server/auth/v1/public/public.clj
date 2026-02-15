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
      (let [data (get-in req [:parameters :body])]
        (logger/log logger ::auth-register data)
        (cond
          (or (nil? token-client) (nil? (:admin-url keycloak)))
          {:status 500 :body {:ok false :error "auth backend not configured"}}

          (nil? (:token-url keycloak))
          {:status 500 :body {:ok false :error "token endpoint not configured"}}

          (nil? (:client-id keycloak))
          {:status 500 :body {:ok false :error "client id not configured"}}

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
                  {:ok true
                   :action :register
                   :location location})
                {:status status
                 :body {:ok false
                        :error "registration failed"
                        :details (some-> (:body resp) (json/parse-string true))
                        :debug log-details}}))
            (catch Exception ex
              {:status 500
               :body {:ok false
                      :error "registration failed"
                      :details (.getMessage ex)}})))))))

(defn auth-login
  "Proxy endpoint for user login (backed by Keycloak)."
  [{:keys [webdeps]}]
  (let [{:keys [db keycloak token-client logger]} webdeps]
    (fn [req]
      (let [data (get-in req [:parameters :body])]
        (logger/log logger ::auth-login data)
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
              {:ok true
               :action :login
               :token (:data resp)})
            {:status (:status resp)
             :body {:ok false
                    :error "login failed"
                    :details (:error resp)}}))))))

(defn auth-refresh
  "Proxy endpoint for token refresh (backed by Keycloak)."
  [{:keys [webdeps]}]
  (let [{:keys [keycloak logger]} webdeps]
    (fn [req]
      (let [data (get-in req [:parameters :body])
            safe-data (cond-> data
                        (contains? data :refresh_token)
                        (assoc :refresh_token "<redacted>"))]
        (logger/log logger ::auth-refresh safe-data)
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
            {:ok true
             :action :refresh
             :token (:data resp)}
            {:status (:status resp)
             :body {:ok false
                    :error "refresh failed"
                    :details (:error resp)}}))))))

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
