(ns core-service.app.server.reitit
  (:require [integrant.core :as ig]
            [reitit.http :as http]
            [reitit.ring :as ring]
            [reitit.interceptor.sieppari :as sieppari]))

(defmethod ig/init-key :core-service.app.server.reitit/router
  [_ {:keys [routes http-base-interceptors]}]
  (http/router
   routes
   {:data {:interceptors (or http-base-interceptors [])}
    :executor sieppari/executor}))

(defmethod ig/init-key :core-service.app.server.reitit/handler
  [_ {:keys [router]}]
  (http/ring-handler
    router
    (ring/routes
      (ring/create-resource-handler {:path "/"})
      (ring/create-default-handler))
    {:executor sieppari/executor}))
