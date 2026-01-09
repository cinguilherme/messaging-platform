(ns core-service.queue
  (:refer-clojure :exclude [peek])
  (:require [integrant.core :as ig]))

;; Integrant-managed, in-memory FIFO queue.
;;
;; We store a PersistentQueue inside an atom so we can update it atomically.
(defmethod ig/init-key :core-service.queue/in-memory-queue
  [_ _opts]
  (atom clojure.lang.PersistentQueue/EMPTY))

(defmethod ig/init-key :core-service.queue/in-memory-queues
  [_ {:keys [topics] :or {topics []}}]
  {:topics->queue (into {}
                        (map (fn [topic]
                               [topic (atom clojure.lang.PersistentQueue/EMPTY)]))
                        topics)})

(defn known-topics
  [queues]
  (set (keys (:topics->queue queues))))

(defn get-queue
  [queues topic]
  (get-in queues [:topics->queue topic]))

(defn get-queue!
  [queues topic]
  (or (get-queue queues topic)
      (throw (ex-info "Unknown topic queue"
                      {:topic topic
                       :known-topics (sort (known-topics queues))}))))

(defn enqueue!
  [queue item]
  (swap! queue conj item))

(defn dequeue!
  "Removes and returns the next item from the queue, or nil when empty."
  [queue]
  (let [[old _new] (swap-vals! queue (fn [q] (if (empty? q) q (pop q))))]
    (when-not (empty? old)
      (clojure.core/peek old))))

(defn peek
  "Returns the next item without removing it, or nil when empty."
  [queue]
  (clojure.core/peek @queue))

(defn size
  [queue]
  (count @queue))

;; Backwards-compatible names (for early iteration).
(def enqueue enqueue!)
(def dequeue dequeue!)