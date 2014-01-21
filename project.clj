(defproject dwd "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/java.jdbc "0.3.2"]
                 [org.clojure/tools.logging "0.2.6"]
                 [com.onekingslane.danger/diesel "1.0.1"]
                 [com.onekingslane.danger/clojure-common-utils "0.0.19"]]
  :profiles {:dev {:dependencies [[org.xerial/sqlite-jdbc "3.7.2"]
                                  [org.slf4j/slf4j-simple "1.7.5"]]
                   :resource-paths ["test/logging"]}})
