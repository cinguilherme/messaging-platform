(ns core-service.core.text-search
  (:require [integrant.core :as ig]
            [core-service.core.text-search.common :as impl]))

(defmethod ig/init-key :core-service.core.text-search/common
  [_ opts]
  (impl/init-common opts))

