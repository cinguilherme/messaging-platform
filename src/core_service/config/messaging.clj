(ns core-service.config.messaging
  (:require [integrant.core :as ig]
            [core-service.tracing :as tracing]))

;; Config components: return config maps (can include Integrant refs via ig/ref).

(def default-routing
  {:defaults {:source :redis}
   :topics {:default {:source :redis
                      :stream "core:default"
                      :group "core"}
           :test-queue {:source :in-memory}
           :jetstream-test {:source :jetstream
                            ;; JetStream specifics
                            :subject "core.jetstream_test"
                            :stream "core_jetstream_test"
                            :durable "core_jetstream_test"}}
   :subscriptions {:default {:source :redis
                             :topic :default
                             ;; Resolved at init time from :handlers override map (see init-key).
                             :handler :log-consumed
                             :options {:block-ms 5000}}
                   :jetstream-test {:source :jetstream
                                    :topic :jetstream-test
                                    :handler :log-consumed
                                    :options {:pull-batch 1
                                              :expires-ms 1000}}}})

(defn- deep-merge
  "Recursively merges maps. Non-map values on the right overwrite."
  [& xs]
  (letfn [(dm [a b]
            (if (and (map? a) (map? b))
              (merge-with dm a b)
              b))]
    (reduce dm {} xs)))

(defmethod ig/init-key :core-service.config.messaging/routing
  [_ overrides]
  ;; Allow duct.edn to pass overrides (e.g. env-specific tweaks) while keeping the heavy
  ;; default routing map in code.
  (letfn [(wrap-handler [handler-fn]
            (fn [envelope]
              (let [parent (some-> (get-in envelope [:metadata :trace]) tracing/decode-ctx)
                    ctx (tracing/child-ctx parent)]
                (tracing/with-ctx ctx
                  (handler-fn envelope)))))]
    (let [handlers (:handlers overrides)
          merged (deep-merge default-routing (or (dissoc overrides :handlers) {}))]
      (update merged :subscriptions
              (fn [subs]
                (into {}
                      (map (fn [[id sub]]
                             (let [h (:handler sub)]
                               (cond
                                 (fn? h)
                                 [id (update sub :handler wrap-handler)]

                                 (and (keyword? h) (contains? handlers h))
                                 [id (assoc sub :handler (wrap-handler (get handlers h)))]

                                 :else
                                 (throw (ex-info "Subscription handler must be a function or a known handler key"
                                                 {:subscription id
                                                  :handler h
                                                  :known-handlers (keys handlers)}))))))
                      (or subs {})))))))
