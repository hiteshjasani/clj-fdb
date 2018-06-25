(ns clj-fdb.tuple-test
  (:require [clojure.test :refer [run-tests deftest testing is are
                                  with-test use-fixtures]])
  (:use [clj-fdb.tuple :rename {range tuple-range}])
  (:import (com.apple.foundationdb.tuple Tuple)))

(deftest making-tuples
  (testing "single tuples"
    (is (equals (Tuple.) (tuple)))
    (is (= "foo" (to-str (tuple "foo")))))

  (testing "tuples as strings"
    (is (= "foo" (to-str (tuple "foo" "bar"))))
    (is (= ["foo" "bar"] (to-strs (tuple "foo" "bar"))))
    (is (= [] (to-strs (tuple))))
    (is (= "(\"foo\", \"bar\")" (.toString (tuple "foo" "bar"))))
    )

  (testing "homogeneous tuples"
    (is (= ["1" "2" "3"] (to-strs (from 1 2 3))))
    (is (= ["foo" ""]
           (to-strs (from "foo" nil))))
    (is (= ["" "foo" ""]
           (to-strs (from nil "foo" nil))))
    (is (= ["foo" "bar" "baz" "quux"]
           (to-strs (from "foo" "bar" "baz" "quux"))))
    )

  (testing "heterogeneous tuples"
    (is (= ["foo" "(\"bar\")" "baz"]
           (to-strs (tuple "foo" (tuple "bar") "baz"))))

    (is (= ["foo" "true" "(\"bar\", \"baz\")" "quux"]
           (to-strs (-> (tuple "foo" true (tuple "bar" "baz") "quux")))))
    (is (= ["9" "3.14159" "foo" "(\"bar\")" "true" ""]
           (to-strs (tuple 9 3.14159 "foo" (tuple "bar") true nil))))
    )

  (testing "embedded byte strings and tuples"
    (let [act (tuple 9 3.14159 "foo" (.getBytes "bar") true (tuple "baz" 3))]
      (is (= 9 (to-long act 0)))
      (is (= 3.14159 (to-double act 1)))
      (is (= "foo" (to-str act 2)))
      (is (= "bar" (String. (to-bytes act 3))))
      (is (= true (to-boolean act 4)))
      (let [nt (to-nested-tuple act 5)]
        (is (= "baz" (to-str nt 0)))
        (is (= 3 (to-long nt 1))))
      )
    )
  )

(run-tests)
