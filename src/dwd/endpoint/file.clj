(ns dwd.endpoint.file
  "file-based operations"
  {:author "Eric Sayle"
   :date "Wed Jul  2 2014"}
  (:require [clojure.java.io :refer [as-file]]
            [clojure.java.shell :refer [sh]]
            [clojure.string :refer [split join]]
            [clj-ssh.cli :refer [sftp]])
  (:require [miner.ftp :refer [with-ftp client-file-names client-cd]])
  (:require [dwd.check-result :refer [make-check-result]]))

(defn local-file-present? [expr env]
  (make-check-result
   { :result (.exists (as-file expr))
     :data expr
     :desc (get env :desc)}))

(defn s3-file-present? [expr env]
  (let [s3-config (:s3-config env)
        secret-key (:secret-key s3-config)
        access-key (:access-key s3-config)]
    (if (or (empty? secret-key)
            (empty? access-key))
      ; bad configuration
      (make-check-result
       {:result :error
        :data expr
        :desc (:desc env)
        :exceptions "Need secret-key and access-key for s3 config"})
      ; run the s3cmd to see if it's there
      (let [sh-result (sh "/Users/esayle/s3cmd-1.5.0-rc1/s3cmd"
                      (format "--access_key=%s" access-key)
                      (format "--secret_key=%s" secret-key)
                      "ls"
                      expr)
            result (if (and (= 0 (:exit sh-result))
                            (not (empty? (:out sh-result)))
                            (> (.indexOf (:out sh-result) expr) -1))
                     true
                     false)
            messages (:out sh-result)
            exceptions (:err sh-result)
            desc (get env :desc)]
        (make-check-result
         {:result result
          :data expr
          :desc desc
          :messages messages
          :exceptions exceptions})))))

(defn- real-ftp-file-present? [expr env]
  (let [config (:ftp-config env)
        hostname (:hostname config)
        username (:username config)
        password (:password config)
        port (if (nil? (:port config))
               21
               (:port config))
        ;; EEE: /file.txt is the same as file.txt, but
        ;; /dir/file.txt is different than dir/file.txt
        filename (last (split expr #"/"))
        path (join "/" (butlast (split expr #"/")))]
    (if (or (nil? hostname)
            (nil? username)
            (nil? password))
      (make-check-result
       {:result :error
        :data expr
        :desc (:desc env)
        :exceptions "Need hostname, username, and password for ftp config"})
      (let [uri (format "ftp://%s:%s@%s:%d"
                        username
                        password
                        hostname
                        port)]
        (with-ftp [client uri]
          (let [cd-result (client-cd client path)
                file-names (client-file-names client)
                file-names-result (some #{filename} file-names)
                result (if (and cd-result file-names-result) true false)]
            (make-check-result
             {:result result
              :data expr
              :desc (:desc env)
              :messages file-names})))))))


(defn- sftp-file-present? [expr env]
  (let [config (:ftp-config env)
        username (:username config)
        password (:password config)
        hostname (:hostname config)
        port (if (empty? (:port config))
               22
               (:port config))]
    (println
     (str "I'm in sftp. Hostname is " hostname ", password is " password ", username is " username))
     (if (or (nil? username)
             (nil? password)
             (nil? hostname))
       (make-check-result
        {:result :error
         :data expr
         :desc (:desc env)
         :exceptions "Need hostname, username, and password for sftp config"})
       (let [file-names (try
                          (sftp hostname :ls expr
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
           :data expr
           :desc (:desc env)
           :exceptions exceptions
           :messages messages})))))

(defn ftp-file-present? [expr env]
  (let [config (:ftp-config env)
        ssl (:ssl config)] ; false if not set at all
    (if ssl
      (sftp-file-present? expr env)
      (real-ftp-file-present? expr env))))
