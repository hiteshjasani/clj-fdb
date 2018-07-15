(ns clj-fdb.subspace-test
  (:require [clojure.test :refer [run-tests deftest testing is are
                                  with-test use-fixtures]]
            [clj-fdb.interfaces :refer [pack]]
            [clj-fdb.tuple :as t])
  (:use [clj-fdb.subspace :rename {range tuple-range}])
  (:import (java.util Arrays)
           (com.apple.foundationdb.subspace Subspace)))

(deftest making-subspaces
  (is (= (Subspace.) (subspace)))
  (is (= "Subspace(rawPrefix=foo)" (as-str (subspace "foo"))))
  )

(deftest subspaces-with-tuples
  (testing "raw prefixes and prefix tuples"
    (is (= "Subspace(rawPrefix=)" (as-str (subspace))))
    (is (= "Subspace(rawPrefix=foo)"
           (as-str (subspace "foo"))))
    (is (= "Subspace(rawPrefix=\\x02foo\\x00)"
           (as-str (subspace (t/tuple "foo")))))
    (is (= "Subspace(rawPrefix=\\x02foo\\x00\\x02bar\\x00)"
           (as-str (subspace (t/tuple "foo" "bar")))))
    )
  (testing "packing and unpacking removes subspace prefixes"
    (let [my-tuple (t/tuple "foo" "bar")
          ss-raw (subspace "test subspace")
          ss-tuple (subspace (t/tuple "test subspace"))]
      (is (= "[116, 101, 115, 116, 32, 115, 117, 98, 115, 112, 97, 99, 101, 2, 102, 111, 111, 0, 2, 98, 97, 114, 0]"
             (Arrays/toString (pack ss-raw my-tuple))))
      (is (= "[2, 116, 101, 115, 116, 32, 115, 117, 98, 115, 112, 97, 99, 101, 0, 2, 102, 111, 111, 0, 2, 98, 97, 114, 0]"
             (Arrays/toString (pack ss-tuple my-tuple))))
      (is (= my-tuple (unpack ss-raw (pack ss-raw my-tuple))))
      (is (= my-tuple (unpack ss-tuple (pack ss-tuple my-tuple))))
      ))
  )

(run-tests)
