(ns core-service.app.server.interceptors.common)

(defn middleware->interceptor
  "Adapts a standard Ring middleware function to a Reitit-style interceptor.
   Correctly handles short-circuiting middleware (those that return a response map directly
   instead of calling the next handler)."
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
