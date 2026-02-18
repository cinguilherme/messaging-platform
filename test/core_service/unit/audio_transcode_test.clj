(ns core-service.unit.audio-transcode-test
  (:require [clojure.test :refer [deftest is testing]]
            [core-service.app.media.audio-transcode :as audio-transcode]))

(deftest build-command-emits-aac-args
  (let [cmd (audio-transcode/build-command
             {:ffmpeg-bin "ffmpeg"
              :sample-rate 24000
              :channels 1
              :aac-bitrate "64k"}
             {:target :aac
              :input-path "/tmp/in.webm"
              :output-path "/tmp/out.m4a"})]
    (is (= "ffmpeg" (first cmd)))
    (is (some #{"-c:a"} cmd))
    (is (some #{"aac"} cmd))
    (is (some #{"64k"} cmd))
    (is (= "/tmp/out.m4a" (last cmd)))))

(deftest build-command-emits-mp3-args
  (let [cmd (audio-transcode/build-command
             {:ffmpeg-bin "ffmpeg"
              :sample-rate 24000
              :channels 1
              :mp3-bitrate "64k"}
             {:target :mp3
              :input-path "/tmp/in.webm"
              :output-path "/tmp/out.mp3"})]
    (is (= "ffmpeg" (first cmd)))
    (is (some #{"-c:a"} cmd))
    (is (some #{"libmp3lame"} cmd))
    (is (some #{"64k"} cmd))
    (is (= "/tmp/out.mp3" (last cmd)))))

(deftest content-type-and-target-support
  (testing "supported targets"
    (is (true? (audio-transcode/supported-target? :aac)))
    (is (true? (audio-transcode/supported-target? :mp3)))
    (is (false? (audio-transcode/supported-target? :ogg))))
  (testing "content-types for supported targets"
    (is (= "audio/mp4" (audio-transcode/content-type-for-target :aac)))
    (is (= "audio/mpeg" (audio-transcode/content-type-for-target :mp3)))))
