(ns dwd.files.s3-file
  "For s3 files"
  {:author "Eric Sayle"
   :date "Wed Jul 23 13:09:58 PDT 2014"}
  (:require [clojure.tools.logging :as log]
            [clojure.string :refer [split join]])
  (:require [aws.sdk.s3 :as s3])
  (:require [dwd.files.file :refer [File]]
            [dwd.check-result :refer [make-check-result]]))

(defn- file-metadata-result [prop cred bucket path desc]
  (let [metadata (s3/get-object-metadata cred bucket path)]
    (make-check-result
     {:result (get metadata prop)
        :data path
        :desc desc})))

(deftype S3File [cred bucket path desc]
  File
  (file-present? [_]
    (make-check-result
     {:result (not (nil? (s3/get-object-metadata cred bucket path)))
      :data path
      :desc desc}))
  (file-mtime [_]
    (file-metadata-result :last-modiifed cred bucket path desc))
  (file-hash [_]
    (file-metadata-result :content-md5 cred bucket path desc))
  (file-size [_]
    (file-metadata-result :content-length cred bucket path desc)))

(defn make-s3-file [expr env]
  (let [s3-config (:s3-config env)
        secret-key (:secret-key s3-config)
        access-key (:access-key s3-config)
        path-broken-up (split expr #"/")]
    (cond (or (empty? secret-key)
              (empty? access-key))
          (log/error "Missing passwords in s3 configuration")
          (not (.startsWith expr "s3://"))
          (log/error (format "Invalid file path %s, should start with s3://"
                             expr))
          :else (S3File. {:access-key access-key :secret-key secret-key}
                         (nth path-broken-up 2)
                         (join "/" (nthrest path-broken-up 3))
                         (:desc env)))))
