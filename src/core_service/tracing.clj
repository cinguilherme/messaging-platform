(ns core-service.tracing
  (:require [clojure.string :as str]))

(def ^:private trace-chars "abcdefghijklmnopqrstuvwxyz0123456789")

(def ^:dynamic *ctx* nil)

(defmacro with-ctx
  "Bind tracing context for the duration of body."
  [ctx & body]
  `(binding [*ctx* ~ctx]
     ~@body))

(defn new-trace-id
  "Short trace id for an entire request history."
  []
  (apply str (repeatedly 12 #(rand-nth trace-chars))))

(defn new-span-id
  "Short span id for a single hop."
  []
  (apply str (repeatedly 8 #(rand-nth trace-chars))))

(defn ensure-ctx
  "Ensure we have a base context. Creates a root ctx if nil."
  [ctx]
  (if (map? ctx)
    ctx
    {:trace-id (new-trace-id)
     :span-id (new-span-id)
     :parent-span-id nil}))

(defn child-ctx
  "Create a child ctx under the given ctx (or a new root ctx if nil)."
  [ctx]
  (let [ctx (ensure-ctx ctx)]
    {:trace-id (:trace-id ctx)
     :span-id (new-span-id)
     :parent-span-id (:span-id ctx)}))

(defn encode-ctx
  "Encode ctx into a compact string suitable for headers and metadata."
  [{:keys [trace-id span-id parent-span-id]}]
  (str "t=" trace-id ";s=" span-id ";p=" (or parent-span-id "-")))

(defn decode-ctx
  "Decode ctx from a string like: t=...;s=...;p=...
   Returns nil when parsing fails."
  [s]
  (when (and (string? s) (not (str/blank? s)))
    (try
      (let [m (->> (str/split s #";")
                   (map #(str/split % #"=" 2))
                   (filter #(= 2 (count %)))
                   (into {}))
            t (get m "t")
            sp (get m "s")
            p (get m "p")]
        (when (and (seq t) (seq sp))
          {:trace-id t
           :span-id sp
           :parent-span-id (when (and p (not= p "-") (seq p)) p)}))
      (catch Exception _e
        nil))))

