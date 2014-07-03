(ns dwd.endpoint.file
  "file-based operations"
  {:author "Eric Sayle"
   :date "Wed Jul  2 2014"}
  (:require [clojure.java.io :refer [as-file]]
            [clojure.java.shell :refer [sh]])
  (:require [dwd.check-result :refer [make-check-result]]))

(defn local-file-present? [expr env]
  (make-check-result
   { :result (.exists (as-file expr))
     :data expr
     :desc (get env :desc)}))

(defn s3-file-present? [expr env]
  (let [s3-config (:s3-config env)
        secret-key (:secret-key s3-config)
        access-key (:access-key s3-config)]
    (if (or (empty? secret-key)
            (empty? access-key))
      ; bad configuration
      (make-check-result
       { :result :error
         :data expr
        :desc (:desc env)
        :messages "Need secret-key and access-key for s3 config"})
      ; run the s3cmd to see if it's there
      (let [sh-result (sh "/Users/esayle/s3cmd-1.5.0-rc1/s3cmd"
                      (format "--access_key=%s" access-key)
                      (format "--secret_key=%s" secret-key)
                      "ls"
                      expr)
            result (if (and (= 0 (:exit sh-result))
                            (not (empty? (:out sh-result)))
                            (> (.indexOf (:out sh-result) expr) -1))
                     true
                     false)
            messages (:out sh-result)
            exceptions (:err sh-result)
            desc (get env :desc)]
        (make-check-result
         {:result result
          :data expr
          :desc desc
          :messages messages
          :exceptions exceptions})))))

(defn ftp-file-present? [expr env]
  (str "Trying to validate " env "using FTP"))
