(ns core-service.app.server.interceptors.multipart
  (:require [integrant.core :as ig]
            [ring.middleware.multipart-params :as multipart]
            [core-service.app.server.interceptors.common :as common]))

(defmethod ig/init-key :core-service.app.server.interceptors.multipart/multipart
  [_ _]
  (common/middleware->interceptor multipart/wrap-multipart-params))
