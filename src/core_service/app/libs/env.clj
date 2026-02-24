(ns core-service.app.libs.env
  (:refer-clojure :exclude [parse-double parse-long])
  (:require [clojure.java.io :as io]
            [clojure.string :as str]))

(defn getenv
  [k]
  (let [v (some-> (System/getenv k) str/trim)]
    (when (seq v) v)))

(defn parse-bool
  [value]
  (let [v (some-> value str str/trim str/lower-case)]
    (cond
      (#{"1" "true" "yes" "y" "on"} v) true
      (#{"0" "false" "no" "n" "off"} v) false
      :else nil)))

(defn env-true?
  [k]
  (true? (parse-bool (getenv k))))

(defn parse-long
  [value]
  (try
    (when (some? value)
      (Long/parseLong (str value)))
    (catch Exception _
      nil)))

(defn parse-double
  [value]
  (try
    (when (some? value)
      (Double/parseDouble (str value)))
    (catch Exception _
      nil)))

(defn env-long
  [k]
  (some-> (getenv k) parse-long))

(defn env-double
  [k]
  (some-> (getenv k) parse-double))

(defn in-docker?
  []
  (or (boolean (getenv "DOCKER"))
      (boolean (getenv "IN_DOCKER"))
      (.exists (io/file "/.dockerenv"))))

(defn default-host
  [docker-host local-host]
  (if (in-docker?) docker-host local-host))