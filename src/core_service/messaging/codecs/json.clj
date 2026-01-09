(ns core-service.messaging.codecs.json
  (:require [integrant.core :as ig] 
            [core-service.messaging.codec :as codec]
            [cheshire.core :as json]))

(defrecord JsonCodec []
  codec/Codec
  (encode [_ value] (json/generate-string value))
  (decode [_ value] (json/parse-string value keyword)))

(defmethod ig/init-key :core-service.messaging.codecs/json
  [_ _opts]
  (->JsonCodec))