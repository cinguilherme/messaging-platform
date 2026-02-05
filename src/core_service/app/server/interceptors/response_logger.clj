(ns core-service.app.server.interceptors.response-logger
  (:require [integrant.core :as ig]
            [duct.logger :as logger]))

(defn response-logger
  "A Reitit-style interceptor for logging request and response data in a single 'leave' event."
  [logger]
  {:name  ::response-logger
   :enter (fn [ctx]
            (let [req (:request ctx)]
              (assoc ctx ::request-info {:uri (:uri req)
                                         :method (:request-method req)
                                         :params (:params req)
                                         :headers (:headers req)
                                         :cid (:cid ctx)
                                         :start-time (System/nanoTime)})))
   :leave (fn [ctx]
            (let [req-info (::request-info ctx)
                  resp (:response ctx)
                  duration-ms (quot (- (System/nanoTime) (:start-time req-info)) 1000000)]
              (when logger
                (logger/log logger :info ::api-request
                            (assoc req-info
                                   :status (:status resp)
                                   :data (:body resp)
                                   :duration-ms duration-ms)))
              ctx))})

(defmethod ig/init-key :core-service.app.server.interceptors.response-logger/response-logger
  [_ {:keys [logger]}]
  (response-logger logger))
