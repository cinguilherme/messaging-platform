(ns core-service.app.segments.format
  (:require [cheshire.core :as json]
            [clojure.edn :as edn])
  (:import (java.io ByteArrayInputStream ByteArrayOutputStream DataInputStream DataOutputStream EOFException)
           (java.util.zip GZIPInputStream GZIPOutputStream)))

(defn- write-record!
  [^DataOutputStream out ^bytes data]
  (.writeInt out (alength data))
  (.write out data))

(defn- gzip-bytes
  [^bytes data]
  (let [out (ByteArrayOutputStream.)
        gzip (GZIPOutputStream. out)]
    (.write gzip data)
    (.close gzip)
    (.toByteArray out)))

(defn- gunzip-bytes
  [^bytes data]
  (let [in (GZIPInputStream. (ByteArrayInputStream. data))
        out (ByteArrayOutputStream.)
        buf (byte-array 4096)]
    (loop [n (.read in buf)]
      (when (pos? n)
        (.write out buf 0 n)
        (recur (.read in buf))))
    (.toByteArray out)))

(defn- read-records
  [^DataInputStream in]
  (loop [acc []]
    (let [len (try
                (.readInt in)
                (catch EOFException _
                  ::eof))]
      (if (= ::eof len)
        acc
        (let [data (byte-array len)]
          (.readFully in data)
          (recur (conj acc data)))))))

(defn encode-segment
  "Encode a segment with a JSON header record and message records.
  messages must be a sequence of raw payload bytes. compression: :none|:gzip."
  [{:keys [header messages compression]}]
  (let [raw (ByteArrayOutputStream.)
        out (DataOutputStream. raw)
        header-bytes (.getBytes (json/generate-string header) "UTF-8")]
    (write-record! out header-bytes)
    (doseq [^bytes payload messages]
      (write-record! out payload))
    (.flush out)
    (let [bytes (.toByteArray raw)]
      (if (= compression :gzip)
        (gzip-bytes bytes)
        bytes))))

(defn decode-segment
  "Decode a segment into {:header ... :messages ... :raw-messages ...}.
  Options: {:compression :none|:gzip :codec :edn|:json|:raw}."
  [^bytes segment-bytes {:keys [compression codec] :or {compression :none codec :raw}}]
  (let [raw (cond
              (= compression :gzip) (gunzip-bytes segment-bytes)
              (= compression :none) segment-bytes
              :else (throw (ex-info "Invalid compression" {:compression compression})))
        in (DataInputStream. (ByteArrayInputStream. raw))
        records (read-records in)
        header (when-let [h (first records)]
                 (json/parse-string (String. ^bytes h "UTF-8") true))
        messages-bytes (vec (rest records))
        parse-message (case codec
                        :edn (fn [b] (edn/read-string (String. ^bytes b "UTF-8")))
                        :json (fn [b] (json/parse-string (String. ^bytes b "UTF-8") true))
                        :raw identity
                        identity)
        messages (mapv parse-message messages-bytes)]
    {:header header
     :messages messages
     :raw-messages messages-bytes}))
