(ns core-service.app.server.interceptors.coercion
  (:require [integrant.core :as ig]
            [reitit.http.coercion :as coercion]))

(defmethod ig/init-key :core-service.app.server.interceptors.coercion/coerce-exceptions
  [_ _]
  (coercion/coerce-exceptions-interceptor))

(defmethod ig/init-key :core-service.app.server.interceptors.coercion/coerce-request
  [_ _]
  (coercion/coerce-request-interceptor))
