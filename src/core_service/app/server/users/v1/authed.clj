(ns core-service.app.server.users.v1.authed
  (:require [cheshire.core :as json]
            [clj-http.client :as http-client]
            [clojure.string :as str]
            [core-service.app.server.http :as http]
            [d-core.core.auth.token-client :as token-client]))

(defn- normalize-email
  [value]
  (some-> value str/trim str/lower-case))

(defn- user->item
  [user]
  {:user_id (:id user)
   :email (:email user)
   :username (:username user)
   :first_name (:firstName user)
   :last_name (:lastName user)
   :enabled (:enabled user)})

(defn users-lookup
  "Lookup users by email via Keycloak admin API."
  [{:keys [token-client keycloak]}]
  (fn [req]
    (let [format (http/get-accept-format req)
          email (normalize-email (http/param req "email"))]
      (cond
        (str/blank? email)
        (http/format-response {:ok false :error "missing email"} format)

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
