(ns dwd.files.sftp-file
  "Files connected via sftp (not ftp)"
  {:author "Eric Sayle"
   :date "Wed Jul 23 14:01:29 PDT 2014"}
  (:require [clojure.tools.logging :as log])
  (:require [clj-ssh.cli :refer [sftp]]
            [clj-time.coerce :as c])
  (:require [dwd.files.file :refer [File]]
            [dwd.check-result :refer [make-check-result]]))

(defprotocol SftpConnection
  (list-file [_ file])
  (stat-file [_ file]))

(defn- exec-sftp-command [hostname port username password command file]
  (try
    (sftp hostname command file
          :username username
          :password password
          :port port)
    (catch Exception e
      e)))

(defn- get-sftp-connection [hostname port username password]
  (reify SftpConnection
    (list-file [_ file]
      (exec-sftp-command hostname port username password :ls file))
    (stat-file [_ file]
      (exec-sftp-command hostname port username password :stat file))))

(deftype SftpFile [username password hostname port file desc]
  File
  (file-present? [_]
    (let [file-names (list-file
                      (get-sftp-connection hostname port username password)
                      file)
          result (and (not (instance? Exception file-names))
                      (= (count file-names) 1))
          exceptions (when (instance? Exception file-names) file-names)
          messages file-names]
      (make-check-result
       {:result result
        :data file
        :desc desc
        :exceptions exceptions
        :messages messages})))
  (file-mtime [_]
    (let [file-stat (stat-file
                     (get-sftp-connection hostname port username password)
                     file)
          result (if (instance? Exception file-stat)
                   :error
                   (c/from-long (* 1000 (.getMTime file-stat))))
          exceptions (when (instance? Exception file-stat) file-stat)
          messages file-stat]
      (make-check-result
       {:result result
        :data file
        :desc desc
        :exceptions exceptions
        :messages messages})))
  (file-size [_]
    (let [file-stat (stat-file
                     (get-sftp-connection hostname port username password)
                     file)
          result (if (instance? Exception file-stat)
                   :error
                   (.getSize file-stat))
          exceptions (when (instance? Exception file-stat) file-stat)
          messages file-stat]
      (make-check-result
       {:result result
        :data file
        :desc desc
        :exceptions exceptions
        :messages messages})))
  (file-hash [_]
    (make-check-result
     {:result :error
      :data file
      :desc desc
      :exceptions "Hash not supported via SFTP"})))

(defn make-sftp-file [expr env]
  (let [config (:sftp-config env)
        username (:username config)
        password (:password config)
        hostname (:hostname config)
        port (if (empty? (:port config))
               22
               (:port config))]
     (if (or (nil? username)
             (nil? password)
             (nil? hostname))
       (log/error "Missing configuration for sftp")
       (SftpFile. username password hostname port expr (:desc env)))))
