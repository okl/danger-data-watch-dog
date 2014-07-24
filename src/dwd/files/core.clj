(ns dwd.files.core
  "Registry of files lives here"
  {:author "Eric Sayle"
   :date "Wed Jul 23 13:00:33 PDT 2014"}
  (:require [clojure.tools.logging :as log])
  (:require [roxxi.utils.common :refer [def-]])
  (:require [dwd.files.local-file :refer [make-local-file]]
            [dwd.files.ftp-file :refer [make-ftp-file]]
            [dwd.files.s3-file :refer [make-s3-file]]
            [dwd.files.sftp-file :refer [make-sftp-file]]
            [dwd.files.file :refer [File]]
            [dwd.check-result :refer [make-check-result]]))

(defn- make-error-result [type]
  (make-check-result
   {:result :error
    :exceptions (str "Unable to find type for " type)}))

(deftype ErrorFile [type]
  File
  (file-present? [_]
    (make-error-result type))
  (file-mtime [_]
    (make-error-result type))
  (file-hash [_]
    (make-error-result type))
  (file-size [_]
    (make-error-result type)))

(def- file-type-registry
  {:local make-local-file
   :s3    make-s3-file
   :ftp   make-ftp-file
   :sftp  make-sftp-file})

(defn file-for-type [type]
  (let [keyword-type (if (keyword? type) type (keyword type))
        result (get file-type-registry keyword-type)]
    (if (nil? result)
      (fn [expr env] (ErrorFile. type))
      result)))
