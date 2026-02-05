(ns core-service.app.server.interceptors.auth
  (:require [integrant.core :as ig]
            [core-service.app.server.interceptors.common :as common]))

(defmethod ig/init-key :core-service.app.server.interceptors.auth/auth
  [_ {:keys [middleware]}]
  (common/middleware->interceptor middleware))
