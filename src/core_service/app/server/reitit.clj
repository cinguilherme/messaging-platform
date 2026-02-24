(ns core-service.app.server.reitit
  (:require [cheshire.core :as json]
            [core-service.app.server.openapi :as api-docs]
            [integrant.core :as ig]
            [reitit.coercion.malli]
            [reitit.http :as http]
            [reitit.interceptor.sieppari :as sieppari]
            [reitit.openapi :as openapi]
            [reitit.ring :as ring]
            [reitit.swagger-ui :as swagger-ui]))

(defn- openapi-json-handler
  []
  (let [handler (openapi/create-openapi-handler)]
    (fn [req]
      (let [resp (handler req)
            body (:body resp)]
        (if (map? body)
          (-> resp
              (assoc :body (json/generate-string body))
              (assoc :headers {"content-type" "application/json"}))
          resp)))))

(defn- openapi-route
  []
  ["/openapi.json"
   {:get {:no-doc true
          :openapi {:id api-docs/docs-id
                    :info api-docs/openapi-info
                    :servers api-docs/openapi-servers
                    :components api-docs/openapi-components}
          :handler (openapi-json-handler)}}])

(defmethod ig/init-key :core-service.app.server.reitit/router
  [_ {:keys [routes http-base-interceptors]}]
  (let [interceptors (into [openapi/openapi-feature] (or http-base-interceptors []))]
    (http/router
     (into [(openapi-route)] (vec routes))
     {:data {:interceptors interceptors
             :coercion reitit.coercion.malli/coercion}
      :executor sieppari/executor})))

(defmethod ig/init-key :core-service.app.server.reitit/handler
  [_ {:keys [router]}]
  (http/ring-handler
   router
   (ring/routes
    (swagger-ui/create-swagger-ui-handler {:path "/docs"
                                           :url "/openapi.json"})
    (ring/create-resource-handler {:path "/"})
    (ring/create-default-handler))
   {:executor sieppari/executor}))
