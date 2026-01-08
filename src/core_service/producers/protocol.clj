(ns core-service.producers.protocol)

(defprotocol Producer
  (produce! [producer msg-map options]
    "Produce a message.

     - msg-map: portable representation of the message
     - options: backend/routing extras (topic/queue, headers, partitions, etc.)

     Returns an ack map."))

(defn produce
  "Convenience wrapper for (produce! producer msg-map options)."
  ([producer msg-map]
   (produce! producer msg-map nil))
  ([producer msg-map options]
   (produce! producer msg-map options)))

