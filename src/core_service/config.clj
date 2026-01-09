(ns core-service.config
  (:require [integrant.core :as ig]
            [duct.logger :as logger]))

;; Some important bits will be in envs like DUCT_ENV, DUCT_PROFILE, REDIS_QUEUES_URI, POSTGRES_URI, etc
(defn get-env-config [env])

(def bare-config {})

;; Design of condifiguration and features


(defmethod ig/init-key :core-service.config/get-config
  [_ {:keys [logger]}]
  (logger/log logger :report ::getting-configs)
  {:resources {:databases {:default {:engine :memory}}
               :queues {:default {:engine :memory}}
               :storage {:default {:engine :memory}}}
   
   :features {:messaging {:enabled true}
              :durable-messages {:enabled false}
              :durable-queues {:enabled false}
              :durable-storage {:enabled false}}})
