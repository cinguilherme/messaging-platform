(ns core-service.app.libs.identity
  (:refer-clojure :exclude [parse-uuid]))

(defn parse-uuid
  [value]
  (try
    (cond
      (nil? value) nil
      (instance? java.util.UUID value) value
      :else (java.util.UUID/fromString (str value)))
    (catch Exception _
      nil)))

(defn user-id-from-request
  [req]
  (or (parse-uuid (get-in req [:auth/principal :subject]))
      (parse-uuid (get-in req [:auth/principal :user_id]))))