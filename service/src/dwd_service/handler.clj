(ns dwd-service.handler
  "Handler class for service webserver"
  {:author "Eric Sayle"
   :date "Fri Jul 18 14:23:25 PDT 2014"}
  (:require [clojure.pprint :refer [pprint]])
  (:require [compojure.core :refer :all]
            [compojure.route :as route]
            [compojure.handler :as handler]
            [clojure.java.io :as io]
            [ring.middleware.json :as middleware])
  (:require [dwd.core :refer :all]
            [dwd.id-interp :refer :all]
            [dwd.check-result :refer :all]
            [dwd.icinga-interp :refer [process-file]])
  (:import [dwd.check_result ConcreteCheckResult]))

(defprotocol Mappable
  (->map [self]))

(extend-type ConcreteCheckResult
  Mappable
  (->map [self]
    {:result (result self)
     :exceptions (exceptions self)
     :time-executed (time-executed self)
     :execution-duration (execution-duration self)
     :messages (messages self)
     :data (data self)
     :description (description self)}))


(def base-check-file "tmp/check.out")
(defn- load-check-configs
  ([_] (load-check-configs))
  ([] (let [file-name base-check-file
            file (io/as-file file-name)]
        (when (.exists file)
          (with-open [reader (java.io.PushbackReader. (io/reader file))]
            (apply merge (map #(id-interp % {}) (read reader))))))))

(def all-checks (atom {}))
(defn- get-all-checks []
  (when (empty? @all-checks)
    (swap! all-checks load-check-configs))
  @all-checks)
(defn- add-check! [new-check]
  (let [old-checks @all-checks
        new-checks (id-interp new-check {})
        merged-checks (merge old-checks new-checks)]
    (swap! all-checks (fn [arg1 arg2] arg2) merged-checks)))

(defn- run-check! [check-id]
  (let [check-def (get (get-all-checks) check-id)
        results (exec-interp check-def {})]
    (if (seq? results)
      (first results)
      results)))

(defn- save-checks! []
  (io/make-parents base-check-file)
  (spit base-check-file (with-out-str (pprint (vals (get-all-checks))))))

(defn- create-icinga-config! []
  (process-file base-check-file))

(defroutes app-routes
  (GET "/" [] "<h1>Hello World</h1>")
  (GET "/test" [] "Testing")
  (GET "/run/:check-id" [check-id]
    (let [result (run-check! (symbol check-id))]
      (cond
       (nil? result)
       {:status 404
        :body (str "Check " check-id " not found")}
       (instance? Exception result)
       {:status 500
        :body (str result)}
       :else
       {:status 200
        :body (->map result)})))
  (POST "/create-check" [data]
    (if (not data)
      {:status 400
       :body "Missing JSON field \"data\""}
      (let [parsed-data (read-string data)]
        {:status 200
         :body (add-check! parsed-data)})))
  (GET "/all-checks" []
    {:status 200
     :body (get-all-checks)})
  (POST "/save-checks" []
    {:status 200
     :body (save-checks!)})
  (POST "/create-icinga-config" []
    {:status 200
     :body (create-icinga-config!)})
  (route/not-found "<h1>Page not found</h1>"))


(def app
  (-> (handler/site app-routes)
      (middleware/wrap-json-response)
      (middleware/wrap-json-params)))
