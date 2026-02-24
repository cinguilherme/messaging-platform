(ns core-service.app.libs.time)

(defn now-ms
	[]
	(System/currentTimeMillis))

(defn now-nanos
	[]
	(System/nanoTime))

(defn nano-span->ms
	[start-nanos]
	(quot (- (System/nanoTime) start-nanos) 1000000))

(defn nano-span->seconds
	[start-nanos]
	(/ (double (- (System/nanoTime) start-nanos)) 1000000000.0))