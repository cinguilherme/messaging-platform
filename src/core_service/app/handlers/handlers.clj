(ns core-service.app.handlers.handlers
  (:require [core-service.app.metrics :as app-metrics]
            [integrant.core :as ig]
            [duct.logger :as logger]))

(defmethod ig/init-key :core-service.app.handlers.handlers/log-consumed
  [_ {:keys [logger metrics]}]
  (app-metrics/consumer-handler metrics :log-consumed [envelope]
    (let [trace (get-in envelope [:metadata :trace])]
      (logger/log logger :info ::envelope-received {:envelope envelope :trace trace}))))

(defmethod ig/init-key :core-service.app.handlers.handlers/fail-once
  [_ {:keys [logger metrics]}]
  (app-metrics/consumer-handler metrics :fail-once [envelope]
    (let [trace (get-in envelope [:metadata :trace])]
      (logger/log logger :info ::checking-message {:envelope envelope :trace trace})
      (if (get-in envelope [:msg :original-envelope])
        (logger/log logger :info ::success-on-retry {:envelope envelope :trace trace})
        (do
          (logger/log logger :warn ::failing-first-attempt {:envelope envelope :trace trace})
          (throw (ex-info "Simulated failure on first attempt" {:envelope envelope :trace trace})))))))
