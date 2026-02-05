(ns core-service.app.server.interceptors
  (:require [duct.logger :as logger]
            [integrant.core :as ig]
            [clojure.string :as str]
            [reitit.ring :as ring]
            [ring.middleware.multipart-params :as multipart]
            [d-core.core.metrics.protocol :as d-metrics]
            [core-service.app.server.http :as http]))

(defn middleware->interceptor
  [middleware]
  {:name ::middleware
   :enter (fn [ctx]
            (let [done (atom false)
                  ctx-ref (atom nil)
                  handler (middleware (fn [req] 
                                        (reset! done true)
                                        (reset! ctx-ref (assoc ctx :request req))))
                  result (handler (:request ctx))]
              (if @done
                @ctx-ref
                (assoc ctx :response result))))})

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

(defmethod ig/init-key :core-service.app.server.interceptors/response-logger
  [_ {:keys [logger]}]
  (response-logger logger))

(defmethod ig/init-key :core-service.app.server.interceptors/format
  [_ _]
  (format-interceptor))

(defn user-context-interceptor
  []
  {:name ::user-context
   :enter (fn [ctx]
            (let [req (:request ctx)
                  user-id (or (http/parse-uuid (get-in req [:auth/principal :subject]))
                              (http/parse-uuid (get-in req [:auth/principal :user_id])))]
              (update ctx :request assoc :user-id user-id)))})

(defmethod ig/init-key :core-service.app.server.interceptors/user-context
  [_ _]
  (user-context-interceptor))

(defn api-key-interceptor
  [{:keys [keys header bypass-paths bypass-prefixes]}]
  (let [header (some-> (or header "x-api-key") str/lower-case)
        key-set (set keys)
        bypass-paths (set bypass-paths)
        bypass-prefixes (or bypass-prefixes [])]
    {:name ::api-key
     :enter (fn [ctx]
              (let [req (:request ctx)
                    uri (:uri req)
                    bypass? (or (contains? bypass-paths uri)
                                (some #(str/starts-with? uri %) bypass-prefixes))]
                (if bypass?
                  ctx
                  (if (empty? key-set)
                    (assoc ctx :response {:status 401 :body "API keys not configured"})
                    (let [provided (get-in req [:headers header])]
                      (if (and provided (contains? key-set provided))
                        (update ctx :request assoc :auth/api-key provided)
                        (assoc ctx :response {:status 401 :body "Unauthorized"})))))))}))

(defmethod ig/init-key :core-service.app.server.interceptors/api-key
  [_ opts]
  (api-key-interceptor opts))

(defn metrics-interceptor
  [{:keys [metrics]}]
  (let [{:keys [requests-total request-duration]} (:http metrics)
        metrics-api (:metrics metrics)]
    {:name ::metrics
     :enter (fn [ctx]
              (assoc ctx ::start-time (System/nanoTime)))
     :leave (fn [ctx]
              (let [start (::start-time ctx)
                    duration (/ (double (- (System/nanoTime) start)) 1000000000.0)
                    req (:request ctx)
                    resp (:response ctx)
                    method (some-> (:request-method req) name str/lower-case)
                    route (or (get-in ctx [:reitit.core/match :template]) (:uri req))
                    status (str (or (:status resp) 200))]
                (try
                  (when (and metrics-api requests-total)
                    (d-metrics/inc! metrics-api
                                    (.labels requests-total (into-array String [(str method) (str route) (str status)]))))
                  (when (and metrics-api request-duration)
                    (d-metrics/observe! metrics-api
                                        (.labels request-duration (into-array String [(str method) (str route)]))
                                        duration))
                  (catch Exception _))
                ctx))}))

(defmethod ig/init-key :core-service.app.server.interceptors/auth
  [_ {:keys [middleware]}]
  (middleware->interceptor middleware))

(defmethod ig/init-key :core-service.app.server.interceptors/authz
  [_ {:keys [middleware]}]
  (middleware->interceptor middleware))

(defmethod ig/init-key :core-service.app.server.interceptors/metrics
  [_ opts]
  (metrics-interceptor opts))

(defmethod ig/init-key :core-service.app.server.interceptors/multipart
  [_ _]
  (middleware->interceptor multipart/wrap-multipart-params))

(defmethod ig/init-key :core-service.app.server.interceptors/middleware
  [_ {:keys [middleware opts]}]
  (if opts
    (middleware->interceptor (fn [handler] (middleware handler opts)))
    (middleware->interceptor middleware)))
