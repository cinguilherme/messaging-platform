(ns core-service.messaging.codec
  (:require [integrant.core :as ig]))

(defprotocol Codec
  (encode [codec value] "Encode a Clojure value (envelope/map) into a transport representation (string/bytes).")
  (decode [codec payload] "Decode a transport representation (string/bytes) back into a Clojure value."))

(defrecord PipelineCodec [codecs]
  Codec
  (encode [_ value]
    (reduce (fn [v c] (encode c v)) value codecs))
  (decode [_ payload]
    (reduce (fn [v c] (decode c v)) payload (reverse codecs))))

(defmethod ig/init-key :core-service.messaging/codec
  [_ {:keys [codecs codec]
      :or {codecs nil}}]
  ;; Accept either:
  ;; - {:codec <single-codec>} or
  ;; - {:codecs [<codec1> <codec2> ...]} for a pipeline.
  (cond
    (sequential? codecs) (->PipelineCodec codecs)
    codec codec
    :else (throw (ex-info "Codec component requires :codec or :codecs" {}))))

