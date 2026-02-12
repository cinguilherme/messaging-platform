(ns core-service.app.workers.attachments
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [core-service.app.server.attachment.logic :as attachment-logic]
            [d-core.core.storage.protocol :as p-storage]
            [d-core.libs.workers :as workers]
            [duct.logger :as logger]
            [integrant.core :as ig])
  (:import (java.awt RenderingHints)
           (java.awt.image BufferedImage)
           (java.io ByteArrayInputStream ByteArrayOutputStream)
           (javax.imageio ImageIO)))

(def default-processing
  {:optimize-threshold-bytes 1048576
   :alt-max-dim 320
   :standard-max-dim 1920})

(defn- image-content-type?
  [value]
  (and (string? value) (str/starts-with? value "image/")))

(defn- image-kind?
  [kind mime-type]
  (or (= kind :image)
      (= kind "image")
      (image-content-type? mime-type)))

(defn- content-type->format
  [content-type]
  (case content-type
    "image/jpeg" "jpg"
    "image/jpg" "jpg"
    "image/png" "png"
    "image/gif" "gif"
    "image/webp" "webp"
    nil))

(defn- scaled-size
  [width height max-dim]
  (let [max-dim (long (or max-dim 0))]
    (if (or (<= max-dim 0) (<= (max width height) max-dim))
      [width height]
      (let [scale (/ (double max-dim) (double (max width height)))
            new-width (max 1 (long (Math/round (* width scale))))
            new-height (max 1 (long (Math/round (* height scale))))]
        [new-width new-height]))))

(defn- resize-image-bytes
  [{:keys [bytes max-dim format]}]
  (let [image (ImageIO/read (ByteArrayInputStream. bytes))]
    (when-not image
      (throw (ex-info "invalid image payload" {:reason :image-read-failed})))
    (let [width (.getWidth image)
          height (.getHeight image)
          [new-width new-height] (scaled-size width height max-dim)
          image-type (if (= "png" format)
                       BufferedImage/TYPE_INT_ARGB
                       BufferedImage/TYPE_INT_RGB)
          resized (BufferedImage. new-width new-height image-type)
          g (.createGraphics resized)
          _ (.setRenderingHint g RenderingHints/KEY_INTERPOLATION RenderingHints/VALUE_INTERPOLATION_BILINEAR)
          _ (.drawImage g image 0 0 new-width new-height nil)
          _ (.dispose g)
          out (ByteArrayOutputStream.)
          ok? (ImageIO/write resized format out)]
      (when-not ok?
        (throw (ex-info "unsupported image output format" {:format format})))
      {:bytes (.toByteArray out)
       :width new-width
       :height new-height
       :original-width width
       :original-height height})))

(defn image-storer
  [{:keys [components]}
   {:keys [object-key bytes content-type variant attachment-id]}]
  (let [storage (or (:storage components) (:minio components))
        content-type (or content-type "application/octet-stream")]
    (if-not storage
      {:status :error
       :error "storage not configured"
       :variant variant
       :object-key object-key
       :attachment-id attachment-id}
      (let [result (p-storage/storage-put-bytes storage object-key bytes {:content-type content-type})
            status (if (:ok result) :stored :error)]
        (when-let [log (:logger components)]
          (logger/log log (if (= status :stored) :debug :warn) ::attachment-store
                      {:attachment-id attachment-id
                       :variant variant
                       :object-key object-key
                       :bytes (alength ^bytes bytes)
                       :status status
                       :error (:error result)}))
        {:status status
         :variant variant
         :object-key object-key
         :bytes (alength ^bytes bytes)
         :attachment-id attachment-id
         :error (:error result)}))))

(defn image-resizer
  [{:keys [channels components]}
   {:keys [bytes max-dim target-key target-content-type target-format
           variant attachment-id conversation-id]}]
  (try
    (let [{:keys [bytes width height original-width original-height]}
          (resize-image-bytes {:bytes bytes
                               :max-dim max-dim
                               :format target-format})
          store-chan (get channels :attachments/store)]
      (async/put! store-chan {:attachment-id attachment-id
                              :conversation-id conversation-id
                              :object-key target-key
                              :bytes bytes
                              :content-type target-content-type
                              :variant variant
                              :image {:width width
                                      :height height
                                      :original-width original-width
                                      :original-height original-height}})
      {:status :queued
       :stage :store
       :variant variant
       :object-key target-key
       :attachment-id attachment-id})
    (catch Exception e
      (when-let [log (:logger components)]
        (logger/log log :warn ::attachment-resize-failed
                    {:attachment-id attachment-id
                     :variant variant
                     :object-key target-key
                     :error (.getMessage e)}))
      {:status :error
       :stage :resize
       :variant variant
       :object-key target-key
       :attachment-id attachment-id
       :error (.getMessage e)})))

(defn attachment-orchestrator
  [{:keys [channels components]}
   {:keys [attachment-id conversation-id object-key bytes mime-type size-bytes kind]}]
  (let [processing (merge default-processing (:processing components))
        image? (image-kind? kind mime-type)
        size-bytes (long (or size-bytes (alength ^bytes bytes)))
        resize-chan (get channels :attachments/resize)
        store-chan (get channels :attachments/store)
        alt-key (attachment-logic/derive-alt-key object-key)
        standard-format (content-type->format mime-type)]
    ;; Store uploaded bytes first, then optionally overwrite with optimized bytes.
    (async/put! store-chan {:attachment-id attachment-id
                            :conversation-id conversation-id
                            :object-key object-key
                            :bytes bytes
                            :content-type mime-type
                            :variant :original})
    (when image?
      (async/put! resize-chan {:attachment-id attachment-id
                               :conversation-id conversation-id
                               :bytes bytes
                               :variant :alt
                               :max-dim (:alt-max-dim processing)
                               :target-key alt-key
                               :target-content-type "image/jpeg"
                               :target-format "jpg"})
      (when (and (> size-bytes (:optimize-threshold-bytes processing))
                 standard-format)
        (async/put! resize-chan {:attachment-id attachment-id
                                 :conversation-id conversation-id
                                 :bytes bytes
                                 :variant :standard
                                 :max-dim (:standard-max-dim processing)
                                 :target-key object-key
                                 :target-content-type mime-type
                                 :target-format standard-format})))
    {:status :queued
     :attachment-id attachment-id
     :conversation-id conversation-id
     :image? image?
     :alt-key (when image? alt-key)
     :standard-optimization? (and image?
                                  (> size-bytes (:optimize-threshold-bytes processing))
                                  (some? standard-format))}))

(def default-definition
  {:channels {:attachments/orchestrate {:buffer 64}
              :attachments/resize {:buffer 64}
              :attachments/store {:buffer 64}
              :workers/errors {:buffer 16}}
   :workers {:attachment-orchestrator {:kind :command
                                       :in :attachments/orchestrate
                                       :worker-fn attachment-orchestrator
                                       :dispatch :thread
                                       :fail-chan :workers/errors
                                       :expose? true}
             :image-resizer {:kind :command
                             :in :attachments/resize
                             :worker-fn image-resizer
                             :dispatch :thread
                             :fail-chan :workers/errors}
             :image-storer {:kind :command
                            :in :attachments/store
                            :worker-fn image-storer
                            :dispatch :thread
                            :fail-chan :workers/errors}}})

(defmethod ig/init-key :core-service.app.workers.attachments/system
  [_ {:keys [webdeps logger metrics definition processing]}]
  (let [webdeps (or webdeps {})
        logger (or logger (:logger webdeps))
        storage (or (:storage webdeps) (:minio webdeps))
        components {:logger logger
                    :storage storage
                    :metrics (or metrics (:metrics webdeps))
                    :observability (or metrics (:metrics webdeps))
                    :processing (merge default-processing processing)}]
    (when logger
      (logger/log logger :info ::initializing-attachment-workers
                  {:channels (keys (:channels (or definition default-definition)))
                   :workers (keys (:workers (or definition default-definition)))
                   :processing (:processing components)}))
    (workers/start-workers (or definition default-definition) components)))

(defmethod ig/halt-key! :core-service.app.workers.attachments/system
  [_ system]
  (when-let [stop! (:stop! system)]
    (stop!)))
