(ns core-service.app.schemas.auth
  (:require [malli.core :as m]))

(def RegisterSchema
  [:map
   [:username :string]
   [:password [:string {:min 6}]]
   [:email {:optional true} :string]
   [:first_name {:optional true} :string]
   [:last_name {:optional true} :string]
   [:enabled {:optional true} :boolean]])

(def LoginSchema
  [:map
   [:username :string]
   [:password :string]
   [:scope {:optional true} :string]])

(defn valid?
  "Schema helper for quick REPL validation."
  [schema value]
  (m/validate schema value))
