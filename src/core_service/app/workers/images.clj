(ns core-service.app.workers.images
  (:require [clojure.core.async :as async]
            [d-core.libs.workers :as workers]
            [duct.logger :as logger]
            [integrant.core :as ig]
            [core-service.app.storage.minio :as minio])
  (:import (java.io ByteArrayInputStream ByteArrayOutputStream)
           (java.util UUID)
           (javax.imageio ImageIO)))

(defn- content-type->ext
  [content-type]
  (case content-type
    "image/jpeg" "jpg"
    "image/jpg" "jpg"
    "image/png" "png"
    "image/gif" "gif"
    "image/webp" "webp"
    "bin"))

(defn- build-object-key
  [{:keys [prefix ext]}]
  (let [ext (or ext "bin")
        prefix (or prefix "images")]
    (str prefix "/" (UUID/randomUUID) "." ext)))

(defn- resize-image
  [{:keys [bytes max-dim]}]
  (let [image (ImageIO/read (ByteArrayInputStream. bytes))]
    (when-not image
      (throw (ex-info "Unsupported image format" {:reason :image-io-read-failed})))
    (let [width (.getWidth image)
          height (.getHeight image)
          max-dim (long (or max-dim 1024))
          scale (min 1.0 (/ (double max-dim) (double (max width height))))
          new-width (max 1 (long (Math/round (* width scale))))
          new-height (max 1 (long (Math/round (* height scale))))
          resized (java.awt.image.BufferedImage.
                   new-width new-height java.awt.image.BufferedImage/TYPE_INT_RGB)
          g (.createGraphics resized)
          _ (.drawImage g image 0 0 new-width new-height nil)
          _ (.dispose g)
          out (ByteArrayOutputStream.)]
      (ImageIO/write resized "jpg" out)
      {:bytes (.toByteArray out)
       :size [new-width new-height]
       :format "jpg"
       :original-size [width height]})))

(defn- reply!
  [reply-chan payload]
  (when reply-chan
    (async/put! reply-chan payload)))

(defn image-resize-worker
  [{:keys [channels] :as ctx}
   {:keys [bytes max-dim reply-chan content-type filename original-bytes] :as msg}]
  (try
    (let [{:keys [bytes size format original-size]} (resize-image {:bytes bytes :max-dim max-dim})
          store-chan (get channels :images/store)]
      (async/put! store-chan (merge
                              {:bytes bytes
                               :reply-chan reply-chan
                               :content-type "image/jpeg"
                               :filename (or filename "upload.jpg")
                               :source :resized
                               :size size
                               :original-size original-size
                               :original-bytes original-bytes
                               :resized-bytes (alength ^bytes bytes)
                               :ext (or format "jpg")}
                              (select-keys msg [:request-id :meta])))
      {:status :queued :stage :store})
    (catch Exception e
      (when-let [log (:logger (:components ctx))]
        (logger/log log :error ::resize-failed
                    {:error (.getMessage e) :content-type content-type :filename filename}))
      (reply! reply-chan {:status :error
                          :stage :resize
                          :error (.getMessage e)})
      {:status :error :stage :resize :error (.getMessage e)})))

(defn image-store-worker
  [{:keys [components] :as ctx}
   {:keys [bytes reply-chan content-type filename source size original-size original-bytes resized-bytes ext request-id meta]}]
  (try
    (let [minio-client (:minio components)
          content-type* (or content-type "application/octet-stream")
          ext (or ext (content-type->ext content-type*))
          key (build-object-key {:prefix (if (= source :resized) "images/resized" "images/original")
                                 :ext ext})
          store-result (minio/put-bytes! minio-client key bytes content-type*)
          result (merge
                  {:status (if (:ok store-result) :stored :error)
                   :key key
                   :bucket (:bucket store-result)
                   :content-type (:content-type store-result)
                   :filename filename
                   :bytes (alength ^bytes bytes)
                   :source (or source :original)
                   :size size
                   :original-size original-size
                   :original-bytes original-bytes
                   :resized-bytes resized-bytes
                   :request-id request-id
                   :meta meta}
                  (when-not (:ok store-result)
                    {:error (:error store-result)}))]
      (reply! reply-chan result)
      result)
    (catch Exception e
      (when-let [log (:logger components)]
        (logger/log log :error ::store-failed
                    {:error (.getMessage e) :filename filename}))
      (reply! reply-chan {:status :error
                          :stage :store
                          :error (.getMessage e)})
      {:status :error :stage :store :error (.getMessage e)})))

(def default-definition
  {:channels {:images/resize {:buffer 8}
              :images/store {:buffer 8}
              :workers/errors {:buffer 8}}
   :workers {:image-resize {:kind :command
                            :in :images/resize
                            :worker-fn image-resize-worker
                            :dispatch :thread
                            :fail-chan :workers/errors
                            :expose? true}
             :image-store {:kind :command
                           :in :images/store
                           :worker-fn image-store-worker
                           :dispatch :thread
                           :fail-chan :workers/errors
                           :expose? true}}})

(defmethod ig/init-key :core-service.app.workers.images/system
  [_ {:keys [logger minio definition]}]
  (let [definition (or definition default-definition)]
    (logger/log logger :info ::initializing-image-workers
                {:channels (keys (:channels definition))
                 :workers (keys (:workers definition))})
    (workers/start-workers definition {:logger logger :minio minio})))

(defmethod ig/halt-key! :core-service.app.workers.images/system
  [_ system]
  (when-let [stop! (:stop! system)]
    (stop!)))
