(ns core-service.app.libs.redis)

(defn conn
  [redis-client]
  (:conn redis-client))

(defn normalize-key
  [k]
  (cond
    (string? k) k
    (keyword? k) (name k)
    (bytes? k) (String. ^bytes k "UTF-8")
    :else (str k)))

(defn fields->map
  [fields]
  (let [pairs (partition 2 fields)]
    (into {}
          (map (fn [[k v]] [(normalize-key k) v]))
          pairs)))
