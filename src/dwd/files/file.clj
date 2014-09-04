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
  (file-hash [_])
  (file-stream [_]
    "Returns a BufferedInputStream of the contents of the file.
    You may find the following idiom useful:
      (line-seq (clojure.java.io/reader (file-stream f)))
    It wraps the stream in a BufferedReader, then returns a lazy-seq
    of the lines of the file.

    IMPORTANT: make sure you close the stream when you're done with it!
    (Wrapping it in a with-open block is ok too.)

    IMPORTANT: it is the user's responsibility to make sure the file exists
    before trying to stream its contents."))
