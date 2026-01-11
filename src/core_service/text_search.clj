(ns core-service.text-search
  (:require [integrant.core :as ig]
            [core-service.text-search.common :as impl]))

(defmethod ig/init-key :core-service.text-search/common
  [_ opts]
  (impl/init-common opts))

