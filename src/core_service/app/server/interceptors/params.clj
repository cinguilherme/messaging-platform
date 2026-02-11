(ns core-service.app.server.interceptors.params
  (:require [integrant.core :as ig]
            [ring.middleware.params :as params]
            [core-service.app.server.interceptors.common :as common]))

(defmethod ig/init-key :core-service.app.server.interceptors.params/params
  [_ _]
  (common/middleware->interceptor params/wrap-params))
