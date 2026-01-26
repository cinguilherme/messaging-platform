(ns core-service.dev.dev-workers
  (:require [d-core.libs.workers :as workers :refer [command! request! stats-snapshot]]
            [clojure.java.io :as io]
            [clojure.core.async :as async])
  (:import (java.io ByteArrayInputStream ByteArrayOutputStream)
           (java.net URL)
           (javax.imageio ImageIO)))





;; Experimental wiring

(defn example-command
  [ctx msg]
  (println "command:" msg "components:" (keys (:components ctx))))

(defn download-bytes
  [image-url error-chan]
  (try
    (with-open [in (io/input-stream (URL. image-url))
              out (ByteArrayOutputStream.)]
    (io/copy in out)
    (.toByteArray out))
  (catch Exception e
    (println "error downloading image" {:image-url image-url :error e})
    (async/put! error-chan {:image-url image-url :error e})
    nil)))

(defn maybe-queue-resize
  [{:keys [channels]} {:keys [image-url bytes max-bytes resize reply-chan]}]
  (let [byte-count (alength ^bytes bytes)
        resize-chan (get channels :images/resize)]
    (if (and max-bytes (> byte-count max-bytes))
      (do
        (async/put! resize-chan {:image-url image-url
                                 :bytes bytes
                                 :resize resize
                                 :reply-chan reply-chan
                                 :original-bytes byte-count})
        (println "queued resize" {:image-url image-url
                                  :bytes byte-count}))
      (do
        (println "image under threshold" {:image-url image-url
                                          :bytes byte-count})
        (when reply-chan
          (async/put! reply-chan {:status :skipped
                                  :reason :under-threshold
                                  :image-url image-url
                                  :bytes byte-count}))))))

(defn image-resize-worker
  [{:keys [channels]} {:keys [image-url bytes resize original-bytes reply-chan]}]
  (let [{:keys [max-dim]} resize
        image (ImageIO/read (ByteArrayInputStream. bytes))
        width (.getWidth image)
        height (.getHeight image)
        max-dim (long (or max-dim 1024))
        scale (min 1.0 (/ (double max-dim) (double (max width height))))
        new-width (max 1 (long (Math/round (* width scale))))
        new-height (max 1 (long (Math/round (* height scale))))
        resized (java.awt.image.BufferedImage. new-width new-height java.awt.image.BufferedImage/TYPE_INT_RGB)
        g (.createGraphics resized)
        _ (.drawImage g image 0 0 new-width new-height nil)
        _ (.dispose g)
        out (ByteArrayOutputStream.)]
    (ImageIO/write resized "jpg" out)
    (let [resized-bytes (.toByteArray out)
          resized-size (.size out)
          store-chan (get channels :images/store)]
      (println "resized image" {:image-url image-url
                                :from [width height]
                                :to [new-width new-height]
                                :original-bytes original-bytes
                                :resized-bytes resized-size})
      (async/put! store-chan {:image-url image-url
                              :bytes resized-bytes
                              :reply-chan reply-chan
                              :original-bytes original-bytes
                              :resized-bytes resized-size
                              :size [new-width new-height]}))))

(defn image-download-worker
  "This worker can error, it will not be retried. It needs to emmit a :fail event to the :fail-chan if it fails."
  [ctx {:keys [image-url max-bytes resize reply-chan error-chan]}]
  (let [bytes (download-bytes image-url error-chan)]
    (maybe-queue-resize ctx {:image-url image-url
                             :bytes bytes  
                             :max-bytes max-bytes
                             :resize resize
                             :reply-chan reply-chan
                             :error-chan error-chan})))

(defn store-image!
  [dir-path {:keys [bytes]}]
  (let [dir (io/file dir-path)
        _ (.mkdirs dir)
        filename (str (java.util.UUID/randomUUID) ".jpg")
        out-file (io/file dir filename)]
    (with-open [out (io/output-stream out-file)]
      (.write out ^bytes bytes))
    (.getPath out-file)))

(defn image-store-worker
  [_ctx {:keys [image-url bytes reply-chan original-bytes resized-bytes size]}]
  (let [path (store-image! "./resized_images" {:bytes bytes})
        result {:status :stored
                :image-url image-url
                :path path
                :original-bytes original-bytes
                :resized-bytes resized-bytes
                :size size}]
    (println "stored image" result)
    (when reply-chan
      (async/put! reply-chan result))
    result))

(def example-definition
  {:channels {:clock/ticks {:buffer 1}
              :commands/in {:buffer 8}
              ;; buffer-type: :fixed (default), :dropping, :sliding
              ;; put-mode: :async (default), :block, :drop
              :images/in {:buffer 4 :buffer-type :dropping :put-mode :drop}
              :images/resize {:buffer 2}
              :images/store {:buffer 2}
              :images/out {:buffer 4}
              :workers/errors {:buffer 8}}
   :workers {:clock {:kind :ticker
                     :interval-ms 1000
                     :out :clock/ticks
                     :dispatch :go}
             :commands {:kind :command
                        :in :commands/in
                        :worker-fn example-command
                        :dispatch :thread
                        :fail-chan :workers/errors
                        :expose? true}
             :image-download {:kind :command
                              :in :images/in
                              :worker-fn image-download-worker
                              :dispatch :thread
                              :fail-chan :workers/errors
                              :expose? true}
             :image-resize {:kind :command
                            :in :images/resize
                            :worker-fn image-resize-worker
                            :dispatch :thread
                            :fail-chan :workers/errors}
             :image-store {:kind :command
                           :in :images/store
                           :worker-fn image-store-worker
                           :output-chan :images/out
                           :dispatch :thread
                           :fail-chan :workers/errors}}})

(defonce example-system (atom nil))



;; after this call my system is running.

(def image-url-not-found "https://upload.wikimedia.org/wikipedia/commons/3/3f/Fronalpstock_big.jpg")
(def image-url "https://images.unsplash.com/photo-1761839256547-0a1cd11b6dfb?q=80&w=2338&auto=format&fit=crop&ixlib=rb-4.1.0&ixid=M3wxMjA3fDF8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D")

(defn run-image-examples!
  "Run example image-related worker commands against `@example-system`.
  Intended for interactive use from the REPL, after the example system
  has been started."
  []
  (command! @example-system :commands {:cmd :ping-3})
  (command! @example-system :image-download {:image-url image-url
                                             :max-bytes 200000
                                             :resize {:max-dim 1024}})
  (async/<!! (request! @example-system :image-download {:image-url image-url
                                                        :max-bytes 200000
                                                        :resize {:max-dim 1024}})))

(comment
  ;; start comment
  (def system
    (workers/start-workers example-definition {}
                           {:dev-guard? true
                            :guard-ms 50}))

  (workers/command! system :image-download {:image-url image-url
                                             :max-bytes 200000
                                             :resize {:max-dim 1024}})


  ;; will fail, use an async block to handle the error. 
  (async/go
    (let [reply (request! system :image-download {:image-url image-url-not-found
                                                   :max-bytes 200000
                                                   :resize {:max-dim 1024}})]
      (println "reply:" reply)))

  (stats-snapshot system)

  ;; end comment
  )


;; REPL feel:
;; (start-example!)
;; (command! @example-system :commands {:cmd :ping-3})
;; (command! @example-system :image-download {:image-url example-image-url
;;                                            :max-bytes 200000
;;                                            :resize {:max-dim 1024}})

;; this will lock my repl until the image is downloaded, not ideal if there is an error in the pipeline.
;; (let [reply (request! @example-system :image-download {:image-url example-image-url
;;                                                        :max-bytes 200000
;;                                                        :resize {:max-dim 1024}})]
;;   (async/<!! reply))
;; (async/go-loop []
;;   (when-some [event (async/<! (get-in @example-system [:channels :images/out]))]
;;     (println "image event:" event)
;;     (recur)))
;; (async/go-loop []
;;   (when-some [event (async/<! (get-in @example-system [:channels :workers/errors]))]
;;     (println "worker error event:" (dissoc event :error))
;;     (recur)))
;; (stats-snapshot @example-system)