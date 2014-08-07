(ns dwd.files.file
  "file-based operations"
  {:author "Eric Sayle"
   :date "Wed Jul  2 2014"}
  (:require [clojure.java.shell :refer [sh]]
            [clojure.string :refer [split join]]
            [clj-ssh.cli :refer [sftp]]
            [clojure.tools.logging :as log])
  (:require [miner.ftp :refer [with-ftp client-file-names client-cd]])
  (:require [dwd.check-result :refer [make-check-result]]))

(defprotocol File
  (file-present? [_])
  (file-mtime [_])
  (file-size [_])
  (file-hash [_]))
