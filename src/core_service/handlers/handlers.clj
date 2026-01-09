(ns core-service.handlers.handlers
  (:require [integrant.core :as ig]
            [duct.logger :as logger]))

(defmethod ig/init-key :core-service.handlers.handlers/log-consumed
  [_ {:keys [logger]}]
  (fn [envelope]
    (logger/log logger :info ::envelope-received {:envelope envelope})))

(defmethod ig/init-key :core-service.handlers.handlers/fail-once
  [_ {:keys [logger]}]
  (fn [envelope]
    (logger/log logger :info ::checking-message {:envelope envelope})
    (if (get-in envelope [:msg :original-envelope])
      (logger/log logger :info ::success-on-retry {:envelope envelope})
      (do
        (logger/log logger :warn ::failing-first-attempt {:envelope envelope})
        (throw (ex-info "Simulated failure on first attempt" {:envelope envelope}))))))
