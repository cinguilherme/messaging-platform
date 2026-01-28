(ns core-service.app.config.observability
  (:require [integrant.core :as ig]))

(def default-logging
  {:level :info
   :components #{}
   :workers #{}
   :segment-flush {:enabled true
                   :stage-logs true}
   :segment-retention {:enabled true}
   :ticker {:enabled true
            :tick-log-interval-ms 0}
   :sampling {:enabled false
              :default-rate 1.0
              :per-component {}}
   :redact-fields #{}
   :include-stacktraces false})

(defmethod ig/init-key :core-service.app.config.observability/logging
  [_ opts]
  (merge default-logging opts))
