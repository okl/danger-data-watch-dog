(ns dwd.files.s3-file
  "For s3 files"
  {:author "Eric Sayle"
   :date "Wed Jul 23 13:09:58 PDT 2014"}
  (:require [clojure.tools.logging :as log]
            [clojure.string :refer [split join]])
  (:require [aws.sdk.s3 :as s3])
  (:require [dwd.files.file :refer [File]]
            [dwd.files.file-system :refer [FileSystem]]
            [dwd.check-result :refer [make-check-result]]))

;; # Helpers

(defmacro cfg-property [env property] `(get (:s3-config ~env) ~property))
(defn- secret-key [env] (cfg-property env :secret-key))
(defn- access-key [env] (cfg-property env :access-key))

(defn bucket [s3-url]
  (let [path-broken-up (split s3-url #"/")]
    (nth path-broken-up 2)))
(defn s3-key [s3-url]
  (let [path-broken-up (split s3-url #"/")]
    (join "/" (nthrest path-broken-up 3))))

;; # File

(defn- get-metadata-if-exists [cred bucket path]
  (if (s3/object-exists? cred bucket path)
      (s3/get-object-metadata cred bucket path)
      nil))

(defn- file-metadata-result [prop cred bucket path desc]
  (let [metadata (get-metadata-if-exists cred bucket path)]
    (make-check-result
     {:result (get metadata prop)
      :data path
      :desc desc})))

(deftype S3File [cred bucket path desc]
  File
  (file-present? [_]
    (make-check-result
     {:result (not (nil? (get-metadata-if-exists cred bucket path)))
      :data path
      :desc desc}))
  (file-mtime [_]
    (file-metadata-result :last-modified cred bucket path desc))
  (file-hash [_]
    (file-metadata-result :content-md5 cred bucket path desc))
  (file-size [_]
    (file-metadata-result :content-length cred bucket path desc)))

(defn make-s3-file [expr env]
  (let [secret-key (secret-key env)
        access-key (access-key env)]
    (cond (or (empty? secret-key)
              (empty? access-key))
          (log/error "Missing passwords in s3 configuration")
          (not (.startsWith expr "s3://"))
          (log/error (format "Invalid file path %s, should start with s3://"
                             expr))
          :else (S3File. {:access-key access-key :secret-key secret-key}
                         (bucket expr)
                         (s3-key expr)
                         (:desc env)))))


;; # Filesystem

(defn- ls-chunk [s3-url cred marker]
  (log/debug (format "getting chunk for s3-url=[%s] and marker=[%s]" s3-url marker))
  (let [bucket (bucket s3-url)
        matches (s3/list-objects cred bucket {:prefix (s3-key s3-url)
                                              :marker marker})
        objects (:objects matches)
        names (map #(str "s3://" bucket "/" (:key %)) objects)]
    {:more? (:truncated? matches)
     :names names
     :marker marker
     :next-marker (:next-marker matches)}))

(defn- lazily-ls-chunks
  ([s3-url cred]
     (lazily-ls-chunks s3-url cred nil))
  ([s3-url cred marker]
     (let [this-chunk (ls-chunk s3-url cred marker)
           names (:names this-chunk)
           next-marker (:next-marker this-chunk)]
       (if (not (:more? this-chunk))
         names
         ;; This is efficient with regards to network I/O,
         ;; but I'm not sure if it's efficient with regards to memory.
         ;; i.e. I'm not sure if it holds onto the entire-sequence-thus-far
         ;; in memory or not... I *think* there aren't any closures in here
         ;; which would mean that's determined by the caller. So hooray,
         ;; probably.
         (lazy-cat names (lazily-ls-chunks s3-url cred next-marker))))))

(deftype S3FileSystem [cred desc]
  FileSystem
  (list-files-matching-prefix [_ prefix]
    (make-check-result
     {:result (lazily-ls-chunks prefix cred)
      :data prefix
      :desc desc})))

(defn make-s3-file-system [env]
  (let [secret-key (secret-key env)
        access-key (access-key env)]
    (cond (or (empty? secret-key)
              (empty? access-key))
          (log/error "Missing passwords in s3 configuration")
          :else
          (S3FileSystem. {:access-key access-key :secret-key secret-key}
                         (:desc env)))))
