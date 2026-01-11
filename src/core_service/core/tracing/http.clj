(ns core-service.core.tracing.http
  (:require [integrant.core :as ig]
            [core-service.tracing :as tracing]))

(def header-name "x-correlation-id")

(defn wrap-tracing
  "Ring middleware that:
   - reads x-correlation-id (trace/span encoded)
   - creates a child span for this HTTP hop
   - binds tracing/*ctx* during handler execution
   - sets x-correlation-id on the response"
  [handler]
  (fn [req]
    (let [incoming (get-in req [:headers header-name])
          parent (tracing/decode-ctx incoming)
          ctx (tracing/child-ctx parent)
          trace (tracing/encode-ctx ctx)]
      (tracing/with-ctx ctx
        (let [resp (handler (assoc req :trace/ctx ctx))]
          (assoc-in resp [:headers header-name] trace))))))

(defmethod ig/init-key :core-service.core.tracing.http/middleware
  [_ _]
  wrap-tracing)

