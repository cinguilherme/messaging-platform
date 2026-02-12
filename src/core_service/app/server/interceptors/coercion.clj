(ns core-service.app.server.interceptors.coercion
  (:require [integrant.core :as ig]
            [reitit.coercion :as rcoercion]
            [reitit.http.coercion :as coercion]))

(defn- envelope-error
  [status message data]
  {:status status
   :body {:ok false
          :error message
          :details (rcoercion/encode-error data)}})

(defn coerce-exceptions-interceptor
  []
  {:name ::coerce-exceptions
   :error (fn [ctx]
            (let [data (some-> ctx :error ex-data)]
              (case (:type data)
                ::rcoercion/request-coercion
                (-> ctx
                    (assoc :response (envelope-error 400 "invalid request" data))
                    (assoc :error nil))

                ::rcoercion/response-coercion
                (-> ctx
                    (assoc :response (envelope-error 500 "invalid response" data))
                    (assoc :error nil))

                ctx)))})

(defmethod ig/init-key :core-service.app.server.interceptors.coercion/coerce-exceptions
  [_ _]
  (coerce-exceptions-interceptor))

(defmethod ig/init-key :core-service.app.server.interceptors.coercion/coerce-request
  [_ _]
  (coercion/coerce-request-interceptor))
