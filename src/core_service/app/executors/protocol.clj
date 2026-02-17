(ns core-service.app.executors.protocol)

(defprotocol Task
  "A handle to an asynchronous computation."
  (wait-for [this timeout-ms timeout-val] "Wait for result with timeout.")
  (cancel [this] "Attempt to cancel the task."))

(defprotocol Executor
  "A system that can execute tasks."
  (execute [this task-fn] "Submit a 0-arity function for execution. Returns a Task."))