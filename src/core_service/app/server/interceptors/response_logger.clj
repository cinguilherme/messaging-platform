(ns core-service.app.server.interceptors.response-logger
  (:require [integrant.core :as ig]
            [duct.logger :as logger]))

(defn response-logger
  "A Reitit-style interceptor for logging the request data on 'enter' and response data on 'leave'."
  [logger]
  {:name  ::response-logger
   :enter (fn [ctx]
            (let [req (:request ctx)]
              (when logger
                (logger/log logger :info ::api-entry
                            {:uri (:uri req)
                             :method (:request-method req)
                             :params (:params req)
                             :headers (:headers req)}))
              ctx))
   :leave (fn [ctx]
            (let [req  (:request ctx)
                  resp (:response ctx)]
              (when logger
                (logger/log logger :info ::api-exit
                            {:uri (:uri req)
                             :method (:request-method req)
                             :status (:status resp)
                             :data (:body resp)}))
              ctx))})

(defmethod ig/init-key :core-service.app.server.interceptors.response-logger/response-logger
  [_ {:keys [logger]}]
  (response-logger logger))
