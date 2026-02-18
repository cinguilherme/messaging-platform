(ns core-service.app.media.audio-transcode
  (:require [clojure.java.io :as io]
            [clojure.string :as str])
  (:import (java.io File)
           (java.nio.file Files)
           (java.util.concurrent TimeUnit)))

(def ^:private default-timeout-ms 30000)
(def ^:private ffmpeg-check-timeout-ms 2000)

(def ^:private target-specs
  {:aac {:suffix ".m4a"
         :content-type "audio/mp4"
         :codec "aac"
         :bitrate-key :aac-bitrate}
   :mp3 {:suffix ".mp3"
         :content-type "audio/mpeg"
         :codec "libmp3lame"
         :bitrate-key :mp3-bitrate}})

(defn supported-target?
  [target]
  (contains? target-specs target))

(defn content-type-for-target
  [target]
  (get-in target-specs [target :content-type]))

(defn output-suffix-for-target
  [target]
  (get-in target-specs [target :suffix]))

(defn- input-suffix-for-mime
  [mime-type]
  (let [mime (some-> mime-type str/lower-case)]
    (case mime
      "audio/webm" ".webm"
      "audio/ogg" ".ogg"
      "audio/mpeg" ".mp3"
      "audio/mp3" ".mp3"
      "audio/wav" ".wav"
      "audio/x-wav" ".wav"
      "audio/aac" ".aac"
      "audio/mp4" ".m4a"
      ".bin")))

(defn build-command
  [{:keys [ffmpeg-bin sample-rate channels] :as cfg}
   {:keys [target input-path output-path]}]
  (let [{:keys [codec bitrate-key]} (get target-specs target)
        bitrate (or (get cfg bitrate-key) "64k")]
    [(or ffmpeg-bin "ffmpeg")
     "-y"
     "-hide_banner"
     "-loglevel" "error"
     "-i" input-path
     "-vn"
     "-ac" (str (or channels 1))
     "-ar" (str (or sample-rate 24000))
     "-c:a" codec
     "-b:a" bitrate
     output-path]))

(defn- run-command
  [cmd timeout-ms]
  (let [builder (ProcessBuilder. ^java.util.List (mapv str cmd))
        _ (.redirectErrorStream builder true)
        process (.start builder)
        output-fut (future
                     (try
                       (slurp (io/reader (.getInputStream process)))
                       (catch Exception _
                         "")))
        finished? (.waitFor process (long timeout-ms) TimeUnit/MILLISECONDS)]
    (if-not finished?
      (do
        (.destroyForcibly process)
        {:ok false
         :error :timeout
         :output (deref output-fut 250 "")})
      (let [exit-code (.exitValue process)
            output (deref output-fut 500 "")]
        (if (zero? exit-code)
          {:ok true :output output}
          {:ok false
           :error :non-zero-exit
           :exit-code exit-code
           :output output})))))

(defn ffmpeg-available?
  ([]
   (ffmpeg-available? "ffmpeg"))
  ([ffmpeg-bin]
   (try
     (:ok (run-command [(or ffmpeg-bin "ffmpeg") "-version"] ffmpeg-check-timeout-ms))
     (catch Exception _
       false))))

(defn transcode-bytes
  [cfg {:keys [target bytes input-mime-type]}]
  (let [target-spec (get target-specs target)]
    (cond
      (not target-spec)
      {:ok false :error :unsupported-target}

      (not (bytes? bytes))
      {:ok false :error :invalid-input}

      :else
      (let [input-file (File/createTempFile "attachment-audio-in" (input-suffix-for-mime input-mime-type))
            output-file (File/createTempFile (str "attachment-audio-" (name target))
                                             (output-suffix-for-target target))
            timeout-ms (long (or (:timeout-ms cfg) default-timeout-ms))
            cmd (build-command cfg {:target target
                                    :input-path (.getAbsolutePath input-file)
                                    :output-path (.getAbsolutePath output-file)})]
        (try
          (with-open [out (io/output-stream input-file)]
            (.write out ^bytes bytes))
          (let [run-result (run-command cmd timeout-ms)]
            (if-not (:ok run-result)
              (assoc run-result :command cmd)
              (if-not (and (.exists output-file) (pos? (.length output-file)))
                {:ok false
                 :error :empty-output
                 :command cmd}
                {:ok true
                 :bytes (Files/readAllBytes (.toPath output-file))
                 :content-type (:content-type target-spec)
                 :target target})))
          (finally
            (.delete input-file)
            (.delete output-file)))))))
