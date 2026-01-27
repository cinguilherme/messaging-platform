(ns core-service.app.server.conversation.v1.public
  (:require [cheshire.core :as json]
            [clj-http.client :as http-client]
            [core-service.app.schemas.auth :as auth-schema]
            [core-service.app.server.http :as http]
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

(defn auth-register
  "Proxy endpoint for user registration (backed by Keycloak)."
  [{:keys [token-client keycloak]}]
  (fn [req]
    (let [format (http/get-accept-format req)
          {:keys [ok data error]} (http/read-json-body req)]
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
          (let [admin-token (token-client/client-credentials token-client {})
                access-token (:access-token admin-token)
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
            (if (<= 200 status 299)
              (http/format-response {:ok true
                                     :action :register
                                     :location location}
                                    format)
              (http/format-response {:ok false
                                     :error "registration failed"
                                     :status status
                                     :details (some-> (:body resp) (json/parse-string true))}
                                    format)))
          (catch Exception ex
            (http/format-response {:ok false
                                   :error "registration failed"
                                   :details (.getMessage ex)}
                                  format)))))))

(defn auth-login
  "Proxy endpoint for user login (backed by Keycloak)."
  [{:keys [keycloak]}]
  (fn [req]
    (let [format (http/get-accept-format req)
          {:keys [ok data error]} (http/read-json-body req)]
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
          (if (:ok resp)
            (http/format-response {:ok true
                                   :action :login
                                   :token (:data resp)}
                                  format)
            (http/format-response {:ok false
                                   :error "login failed"
                                   :status (:status resp)
                                   :details (:error resp)}
                                  format)))))))
