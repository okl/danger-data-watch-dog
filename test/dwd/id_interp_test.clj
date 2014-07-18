(ns dwd.id-interp-test
  (:require [clojure.test :refer :all])
  (:require [dwd.id-interp :refer :all]))

(defn check-explicit-id [id]
  (list 'check id "testing" '(= 1 1)))

(def check-anon-id
  '(check "testing" (= 1 1)))


(deftest test-check
  (testing "explicit ids"
    (is (= (id-interp (check-explicit-id 'id) {})
           {'id (check-explicit-id 'id)})))
  (testing "anonymous ids"
    (let [interp-output (id-interp check-anon-id {})
          key (first (keys interp-output))]
      (is (= interp-output
             {key (check-explicit-id key)})))))

(deftest test-testing
  (testing "single ids"
    (testing "explicit id"
      (is (= (id-interp
              (list 'testing "something" (check-explicit-id 'my-id))
              {})
             {'my-id (list
                      'testing
                      "something"
                      (check-explicit-id 'my-id))})))
    (testing "anonymous id"
      (let [expr (list 'testing "something" check-anon-id)
            interp-output (id-interp expr {})
            key (first (keys interp-output))]
        (is (= interp-output
               {key (list 'testing "something" (check-explicit-id key))})))))
  (testing "multiple ids"
    (let [expr (list 'testing
                     "something"
                     check-anon-id
                     (check-explicit-id 'id))
          interp-output (id-interp expr {})
          result-keys (keys interp-output)
          named-result (list 'testing "something" (check-explicit-id 'id))
          anon-result (list 'testing "something" (check-explicit-id (second result-keys)))]
      (is (= interp-output
             {'id named-result
              (second result-keys) anon-result}))))
  (testing "multiple levels"
    (let [expr (list 'testing
                     "something"
                     check-anon-id
                     (list 'testing "something" (check-explicit-id 'id)))
          interp-output (id-interp expr {})
          result-keys (keys interp-output)
          named-result (list 'testing
                             "something"
                             (list 'testing "something" (check-explicit-id 'id)))
          anon-result (list 'testing
                            "something"
                            (check-explicit-id (second result-keys)))]
      (is (= interp-output
             {'id named-result
              (second result-keys) anon-result})))))

(deftest test-groups
  (testing "id-group-only-ids"
    (is (= (id-interp '(group id "mygroup" [someid]) {})
            {'id '(group id "mygroup" [someid])})))
  (testing "id-group-check-def"
    (is (= (id-interp '(group groupid "mygroup" [(check checkid "mycheck" (= 1 1))]) {})
           {'groupid '(group groupid "mygroup" [checkid])
            'checkid '(check checkid "mycheck" (= 1 1))})))
  (testing "id-group-check-def-and-id"
    (let [check-expr '(check checkdef "mycheck" (= 1 1))
          group-expr (list 'group 'groupid "mygroup" [check-expr 'othercheck])]
      (is (= (id-interp group-expr {})
             {'groupid '(group groupid "mygroup" [othercheck checkdef])
              'checkdef check-expr})))))

(deftest test-mixed
  (let [check-expr '(check checkdef "mycheck" (= 1 1))
        group-expr (list 'group 'groupdef "mygruop" ['something check-expr 'else])
        testing-expr (list 'testing "somestring" group-expr)]
    (is (= (id-interp testing-expr {})
        {'groupdef '(testing "somestring" (group groupdef "mygruop" [something else checkdef]))
         'checkdef '(testing "somestring" (check checkdef "mycheck" (= 1 1)))}))))
