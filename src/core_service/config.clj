(ns core-service.config)

(defn get-env-config [env])

(def bare-config {})

;; Design of condifiguration and features


(defn get-config [_options]
  (println "Getting configs")
  {:resources {:databases {:default {:engine :memory}}
               :queues {:default {:engine :memory}}
               :storage {:default {:engine :memory}}}
   :features {}})