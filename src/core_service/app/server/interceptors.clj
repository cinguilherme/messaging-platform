(ns core-service.app.server.interceptors
  (:require [integrant.core :as ig]))

(defmethod ig/init-key :core-service.app.server.interceptors/http-base-interceptors
  [_ {:keys [metrics cid params response-logger format api-key auth user-context authz
             multipart json-body coerce-exceptions coerce-request]}]
  [metrics
   cid
   params
   multipart
   response-logger
   format
   api-key
   auth
   user-context
   authz
   json-body
   coerce-exceptions
   coerce-request])
