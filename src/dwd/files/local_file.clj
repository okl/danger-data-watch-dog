(ns dwd.files.local-file
  "For local/nfs files"
  {:author "Eric Sayle"
   :date "Wed Jul 23 13:09:58 PDT 2014"}
  (:require [clojure.java.io :refer [as-file]])
  (:require [digest]
            [clj-time.coerce :as c])
  (:require [dwd.files.file :refer [File]]
            [dwd.files.file-system :refer [FileSystem]]
            [dwd.check-result :refer [make-check-result]]))

;; # File

(deftype LocalFile [file desc]
  File
  (file-present? [_]
    (make-check-result
     {:result (.exists (as-file file))
      :data file
      :desc desc}))
  (file-mtime [_]
    (make-check-result
     {:result (c/from-long (.lastModified (as-file file)))
      :data file
      :desc desc}))
  (file-hash [_]
    (make-check-result
     {:result (digest/md5 (as-file file))
      :data file
      :desc desc}))
  (file-size [_]
    (make-check-result
     {:result (.length (as-file file))
      :data file
      :desc desc})))

(defn make-local-file [expr env]
  (LocalFile. expr (:desc env)))

;; # Filesystem

(deftype LocalFileSystem [desc]
  FileSystem
  (list-files-matching-prefix [_ prefix options]
    (make-check-result
     {:result :error
      :data prefix
      :desc desc
      :exceptions "List-files-matching-prefix not supported for local filesystem"})))

(defn make-local-file-system [env]
  (LocalFileSystem. (:desc env)))
