(ns dwd.ops
  {:author "Eric Sayle"
   :date "Mon Jul  7 11:24:55 PDT 2014"
   :description   "For OOB operators"}
  (:require [clojure.java.shell :refer [sh]]
            [clojure.tools.logging :as log]
            [clojure.string :refer [join trim]])
  (:require [dwd.check-result :refer [make-check-result]]))

(def op-sym-tab (atom {}))

(defn opconfig [k v]
  (when (contains? @op-sym-tab k)
    (log/warnf "Attempting to overwrite already defined operator %s" k))
  (swap! op-sym-tab #(assoc % k v)))

(defn op-registry []
  @op-sym-tab)

(defn- run-shell-command [& args]
  (try
    (apply sh args)
    (catch Exception e
      {:exit 1
       :err (join "\n" (cons (.getMessage e) (.getStackTrace e)))})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Pre-defined operators
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; shell-result-code
;; ex. `(exec-op shell-result-code "ls" "/tmp/test.txt")
;; Runs a command, result is the exit code of the command
(defn- shell-exit-code [ & args]
  (let [shell-output (apply run-shell-command args)
        result (:exit shell-output)
        messages (trim (:out shell-output))
        exceptions (:err shell-output)]
    (make-check-result
     {:result result
      :messages messages
      :exceptions exceptions})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; shell-output
;; ex `(exec-op shell-output "ls" "/tmp/test.txt"
;; Runs a command, result is the stdout of that command
(defn- shell-output [& args]
  (let [shell-output (apply run-shell-command args)
        messages (:exit shell-output)
        result (trim (:out shell-output))
        exceptions (:err shell-output)]
    (make-check-result
     {:result result
      :messages messages
      :exceptions exceptions})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Set up registry
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(opconfig 'shell-exit-code shell-exit-code)
(opconfig 'shell-output shell-output)
