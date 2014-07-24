(ns dwd.files.sftp-file
  "Files connected via sftp (not ftp)"
  {:author "Eric Sayle"
   :date "Wed Jul 23 14:01:29 PDT 2014"}
  (:require [clojure.tools.logging :as log]
            [clj-ssh.cli :refer [sftp]])
  (:require [dwd.files.file :refer [File]]
            [dwd.check-result :refer [make-check-result]]))

(deftype SftpFile [username password hostname port file desc]
  File
  (file-present? [_]
    (let [file-names (try
                       (sftp hostname :ls file
                             :username username
                             :password password
                             :port port)
                       (catch Exception e
                         e))
          result (and (not (instance? Exception file-names))
                      (= (count file-names) 1))
          exceptions (when (instance? Exception file-names) file-names)
          messages file-names]
      (make-check-result
       {:result result
        :data file
        :desc desc
        :exceptions exceptions
        :messages messages}))))

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
