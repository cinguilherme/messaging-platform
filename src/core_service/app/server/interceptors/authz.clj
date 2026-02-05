(ns core-service.app.server.interceptors.authz
  (:require [integrant.core :as ig]
            [core-service.app.server.interceptors.common :as common]))

(defmethod ig/init-key :core-service.app.server.interceptors.authz/authz
  [_ {:keys [middleware]}]
  (common/middleware->interceptor middleware))
