(ns dwd.files.local-file
  "For local/nfs files"
  {:author "Eric Sayle"
   :date "Wed Jul 23 13:09:58 PDT 2014"}
  (:require [clojure.java.io :refer [as-file]])
  (:require [digest]
            [clj-time.coerce :as c])
  (:require [dwd.files.file :refer [File]]
            [dwd.check-result :refer [make-check-result]]))

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
