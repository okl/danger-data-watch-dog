(ns dwd.files.ftp-file
  "Files connected via ftp (not sftp)"
  {:author "Eric Sayle"
   :date "Wed Jul 23 14:01:29 PDT 2014"}
  (:require [clojure.tools.logging :as log]
            [clojure.string :refer [split]])
  (:require [miner.ftp :as ftp])
  (:require [dwd.files.file :refer [File]]
            [dwd.check-result :refer [make-check-result]]))

(deftype FtpFile [username password uri file desc]
  File
  (file-present? [_]
    (let [filename (last (split file #"/"))
          path (subs file 0 (- (count file) (count filename)))
          path-files (ftp/list-files (str uri path))]

      (make-check-result
       {:result (if (some #{filename} path-files) true false)
        :data file
        :desc desc}))))

(defn make-ftp-file [expr env]
  (let [config (:ftp-config env)
        hostname (:hostname config)
        username (:username config)
        password (:password config)
        port (if (nil? (:port config))
               21
               (:port config))
        uri (format "ftp://%s:%s@%s:%d/"
                    username
                    password
                    hostname
                    port)]
    (if (or (nil? hostname)
            (nil? username)
            (nil? password)
            (nil? expr))
      (log/error "missing configuration info for ftp")
      (FtpFile. username password uri expr (:desc env)))))
