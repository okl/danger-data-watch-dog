(defproject com.onekingslane.danger/data-watch-dog "0.1.4-SNAPSHOT"
  :description "Monitoring library for specifying checks and executing them"
  :url "https://github.com/okl/danger-data-watch-dog"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/java.jdbc "0.3.2"]
                 [org.clojure/tools.logging "0.2.6"]
                 [com.onekingslane.danger/diesel "1.1.0"]
                 [vertica-jdbc5 "6.1.3-0"]
                 [com.onekingslane.danger/clojure-common-utils "0.0.24"]
                 [com.onekingslane.danger/date-expr "0.3.0"]
                 [com.velisco/clj-ftp "0.3.1"]
                 [clj-ssh "0.5.10"]
                 [clj-yaml "0.4.0"]
                 [clj-time "0.7.0"]
                 [de.ubercode.clostache/clostache "1.4.0"]
                 [clj-aws-s3 "0.3.9"]
                 [digest "1.4.4"]]
  :plugins [[lein-localrepo "0.5.3"]]
  :main dwd.cli
  :aot [dwd.cli]
  :profiles {:dev {:dependencies [[org.xerial/sqlite-jdbc "3.7.2"]
                                  [org.slf4j/slf4j-simple "1.7.5"]]
                   :resource-paths ["test/logging"]}})
