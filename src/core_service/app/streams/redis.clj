(ns core-service.app.streams.redis
  (:require [taoensso.carmine :as car]))

(defn- conn
  [redis-client]
  (:conn redis-client))

(defn- normalize-field-key
  [k]
  (cond
    (string? k) k
    (keyword? k) (name k)
    (bytes? k) (String. ^bytes k "UTF-8")
    :else (str k)))

(defn- fields->map
  [fields]
  (let [pairs (partition 2 fields)]
    (into {}
          (map (fn [[k v]] [(normalize-field-key k) v]))
          pairs)))

(defn append!
  "Append raw bytes payload to a Redis stream. Returns the stream entry id."
  [redis-client stream payload-bytes]
  (when-not (bytes? payload-bytes)
    (throw (ex-info "payload must be raw bytes" {:type ::invalid-payload})))
  (car/wcar (conn redis-client)
    (car/xadd stream "*" "payload" payload-bytes)))

(defn read!
  "Read entries from a Redis stream.
  Options: {:direction :backward|:forward :limit n :cursor id}
  Returns {:entries [{:id .. :payload ..}] :next-cursor <id or nil>}."
  [redis-client stream {:keys [direction limit cursor]}]
  (let [limit (long (or limit 50))
        direction (or direction :backward)
        end (if cursor (str "(" cursor) "+")
        start (if cursor (str "(" cursor) "-")
        entries (car/wcar (conn redis-client)
                  (if (= direction :forward)
                    (car/xrange stream start "+" "COUNT" limit)
                    (car/xrevrange stream end "-" "COUNT" limit)))
        decoded (mapv (fn [[id fields]]
                        {:id id
                         :payload (get (fields->map fields) "payload")})
                      entries)
        next-cursor (when (= (count decoded) limit)
                      (:id (last decoded)))]
    {:entries decoded
     :next-cursor next-cursor}))
