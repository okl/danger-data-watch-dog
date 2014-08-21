(ns dwd.files.ftp-file
  "Files connected via ftp (not sftp)"
  {:author "Eric Sayle"
   :date "Wed Jul 23 14:01:29 PDT 2014"}
  (:require [clojure.tools.logging :as log]
            [clojure.string :refer [split]])
  (:require [miner.ftp :as ftp])
  (:require [dwd.files.file :refer [File]]
            [dwd.files.file-system :refer [FileSystem]]
            [dwd.check-result :refer [make-check-result]]))

;; # Helpers

(defmacro cfg-property [env property] `(get (:ftp-config ~env) ~property))
(defn- username [env] (cfg-property env :username))
(defn- password [env] (cfg-property env :password))
(defn- hostname [env] (cfg-property env :hostname))
(defn- port [env] (if (nil? (cfg-property env :port))
                    21
                    (cfg-property env :port)))
(defn- uri [env]
  (let [username (username env)
        password (password env)
        hostname (hostname env)
        port (port env)]
    (if (or (nil? hostname)
            (nil? username)
            (nil? password))
      (log/error "missing configuration info for ftp")
      (format "ftp://%s:%s@%s:%d/"
              username
              password
              hostname
              port))))

;; # File

(deftype FtpFile [username password uri file desc]
  File
  (file-present? [_]
    (let [filename (last (split file #"/"))
          path (subs file 0 (- (count file) (count filename)))
          path-files (ftp/list-files (str uri path))]
      (make-check-result
       {:result (if (some #{filename} path-files) true false)
        :data file
        :desc desc})))
  (file-mtime [_]
    (make-check-result
     {:result :error
      :data file
      :desc desc
      :exceptions "Modified time not supported via FTP"}))
  (file-size [_]
    (make-check-result
     {:result :error
      :data file
      :desc desc
      :exceptions "Size not supported via FTP"}))
  (file-hash [_]
    (make-check-result
     {:result :error
      :data file
      :desc desc
      :exceptions "Hash not supported via FTP"})))

(defn make-ftp-file [expr env]
  (let [username (username env)
        password (password env)
        uri (uri env)]
    (if (or (nil? username)
            (nil? password)
            (nil? uri)
            (nil? expr))
      (log/error "missing configuration info for ftp")
      (FtpFile. username password uri expr (:desc env)))))

;; # Filesystem

(deftype FtpFileSystem [uri desc]
  FileSystem
  (list-files-matching-prefix [_ prefix options]
    (make-check-result
     {:result :error
      :data prefix
      :desc desc
      :exceptions "List-files-matching-prefix not supported for FTP filesystem"})))

(defn make-ftp-file-system [env]
  (let [uri (uri env)]
    (if (nil? uri)
      (log/error "missing configuration info for ftp")
      (FtpFileSystem. uri (:desc env)))))
