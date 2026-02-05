(ns core-service.app.server.interceptors
  (:require [integrant.core :as ig]))

(defmethod ig/init-key :core-service.app.server.interceptors/http-base-interceptors
  [_ {:keys [metrics format api-key auth user-context authz response-logger]}]
  [metrics format api-key auth user-context authz response-logger])
