(ns core-service.config)

;; Some important bits will be in envs like DUCT_ENV, DUCT_PROFILE, REDIS_QUEUES_URI, POSTGRES_URI, etc
(defn get-env-config [env])

(def bare-config {})

;; Design of condifiguration and features


(defn get-config [_options]
  (println "Getting configs")
  {:resources {:databases {:default {:engine :memory}}
               :queues {:default {:engine :memory}}
               :storage {:default {:engine :memory}}}
   
   :features {:messaging {:enabled true}
              :durable-messages {:enabled false}
              :durable-queues {:enabled false}
              :durable-storage {:enabled false}}})