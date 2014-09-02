(ns dwd.check-result
  {:author "Eric Sayle"
   :date "Thu Jul  3 11:39:56 PDT 2014"})


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Check Results
;;
;; In order to have consistency in our return values, let's define what every check
;; should return so we can layer on higher order logic

(defprotocol CheckResult
  (result [this])
  (exceptions [this])
  (time-executed [this])
  (execution-duration [this])
  (messages [this])
  (data [this])
  (description [this]))

(deftype ConcreteCheckResult [config]
  CheckResult
  (result [_]
    (:result config))
  (exceptions [_]
    (:exceptions config))
  (time-executed [_]
    (:time config))
  (execution-duration [_]
    (:duration config))
  (messages [_]
    (:messages config))
  (data [_]
    (:data config))
  (description [_]
    (:desc config))
  (toString [_]
    (str config)))


(defn make-check-result [config]
  (ConcreteCheckResult. config))

(defn xform-the-result-field [check-result xform]
  (let [cfg (.config check-result)
        new-cfg (update-in cfg [:result] xform)]
    (make-check-result new-cfg)))

(defn merge-check-results [results]
  (make-check-result
   {:result (map result results)
    :exceptions (map exceptions results)
    :time (map time-executed results)
    :duration (reduce + (filter number? (map execution-duration results)))
    :messages (map messages results)
    :data (map data results)
    :desc (map description results)}))
