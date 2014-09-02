(ns dwd.files.core
  "Registry of files lives here"
  {:author "Eric Sayle"
   :date "Wed Jul 23 13:00:33 PDT 2014"}
  (:require [clojure.tools.logging :as log])
  (:require [roxxi.utils.common :refer [def-]])
  (:require [dwd.files.local-file :refer [make-local-file
                                          make-local-file-system]]
            [dwd.files.ftp-file :refer [make-ftp-file
                                        make-ftp-file-system]]
            [dwd.files.s3-file :refer [make-s3-file
                                       make-s3-file-system]]
            [dwd.files.sftp-file :refer [make-sftp-file
                                         make-sftp-file-system]]
            [dwd.files.file :refer [File]]
            [dwd.files.file-system :refer [FileSystem]]
            [dwd.check-result :refer [make-check-result]]))

;; # Helpers

(defn- make-error-result [type]
  (make-check-result
   {:result :error
    :exceptions (str "Unable to find type for " type)}))

(defn- lookup-type-in-registry [type registry error-case]
  (let [keyword-type (if (keyword? type) type (keyword type))
        result (get registry keyword-type)]
    (if (nil? result)
      (fn [expr env] error-case)
      result)))

;; # Files

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
   :sftp  make-sftp-file
   nil    make-local-file})

(defn file-for-type [type]
  (lookup-type-in-registry type
                           file-type-registry
                           (ErrorFile. type)))

;; # Filesystems

(deftype ErrorFileSystem [type]
  FileSystem
  (list-files-matching-prefix [_ prefix options]
    (make-error-result type)))

(def- file-system-type-registry
  {:local make-local-file-system
   :s3    make-s3-file-system
   :ftp   make-ftp-file-system
   :sftp  make-sftp-file-system
   nil    make-local-file-system})

(defn filesystem-for-type [type]
  (lookup-type-in-registry type
                           file-system-type-registry
                           (ErrorFileSystem. type)))
