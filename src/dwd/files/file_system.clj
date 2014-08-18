(ns dwd.files.file-system
  "filesystem-based operations"
  {:author "Matt Halverson"
   :date "Fri Aug 15 2014"})

(defprotocol FileSystem
  (list-files-matching-prefix [_ prefix]
    "Lists all files matching the specified prefix-string, s3-key style."))
