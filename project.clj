(defproject dwd "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/java.jdbc "0.3.2"]
                 [com.onekingslane.danger/diesel "1.0.1"]
                 [com.onekingslane.danger/clojure-common-utils "0.0.19"]]
  :profiles {:dev {:dependencies [[org.xerial/sqlite-jdbc "3.7.2"]]}})
