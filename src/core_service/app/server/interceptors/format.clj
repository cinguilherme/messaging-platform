(ns core-service.app.server.interceptors.format
  (:require [integrant.core :as ig]
            [core-service.app.server.http :as http]))

(defn format-interceptor
  "A Reitit-style interceptor for content negotiation.
   - Enter: Detects format from Accept header and assocs to context and request.
   - Leave: Formats the response body if it's a map without a status code."
  []
  {:name ::format
   :enter (fn [ctx]
            (let [format (http/get-accept-format (:request ctx))]
              (-> ctx
                  (assoc :request-format format)
                  (update :request assoc :response-format format))))
   :leave (fn [ctx]
            (let [format (:request-format ctx)
                  resp   (:response ctx)]
              (if (and (map? resp) 
                       (not (contains? resp :status)) 
                       (not (contains? resp :request))
                       (not (:reitit.core/match resp)))
                (assoc ctx :response (http/format-response resp format))
                ctx)))})

(defmethod ig/init-key :core-service.app.server.interceptors.format/format
  [_ _]
  (format-interceptor))
