(ns clj-fdb.simple-test
  (:require [clojure.test :refer [run-tests deftest testing is are with-test
                                  use-fixtures]]
            [clj-fdb.db :as fdb]
            [clj-fdb.directory :as dir]
            [clj-fdb.subspace :as ss]
            [clj-fdb.tuple :as tup]
            [clj-fdb.simple :refer :all]
            ))

(def test-subspace ["test" "simple-test"])
(def ^:dynamic *db* nil)
(def ^:dynamic *ss* nil)

(defn overall-fixture [f]
  ;; (println "Creating test directory subspace")
  (with-open [db (fdb/open)]
    (let [ss (.join (dir/create-or-open (dir/directory-layer) db
                                        test-subspace))]
      (binding [*db* db
                *ss* ss]
        (f))
      ;; (println "Removing test directory subspace")
      (.join (dir/rm-if-exists!? (dir/directory-layer) db
                                 (conj [] (first test-subspace)))))))

(use-fixtures :once overall-fixture)

(deftest db-ss
  (is *db*)
  (is *ss*)
  (is (= ["test"] (conj [] (first test-subspace)))))

(deftest put-get
  (testing "strings"
    (put-val *db* (tup/pack *ss* "foo") (tup/byte-arr "bar"))
    (let [v (tup/to-str (get-val *db* (tup/pack *ss* "foo")))]
      (is (= 3 (count v)))
      (is (= java.lang.String (type v)))
      (is (= "bar" v))))

  (testing "booleans"
    (put-val *db* (tup/pack *ss* "is-true") (tup/byte-arr true))
    (is (= true (tup/to-bool (get-val *db* (tup/pack *ss* "is-true")))))
    (put-val *db* (tup/pack *ss* "is-false") (tup/byte-arr false))
    (is (= false (tup/to-bool (get-val *db* (tup/pack *ss* "is-false")))))
    (is (= nil
           (tup/to-bool (get-val *db* (tup/pack *ss* "bool-does-not-exist")))))
    )

  (testing "integral numbers"
    (put-val *db* (tup/pack *ss* "int-answer") (tup/byte-arr (int 42)))
    (is (= (int 42) (tup/to-int (get-val *db* (tup/pack *ss* "int-answer")))))
    (put-val *db* (tup/pack *ss* "long-answer") (tup/byte-arr (long 42)))
    (is (= 42 (long 42)
           (tup/to-long (get-val *db* (tup/pack *ss* "long-answer")))))
    (put-val *db* (tup/pack *ss* "short-answer") (tup/byte-arr (short 42)))
    (is (= (short 42)
           (tup/to-short (get-val *db* (tup/pack *ss* "short-answer")))))
    (put-val *db* (tup/pack *ss* "byte-answer") (tup/byte-arr (byte 42)))
    (is (= (byte 42)
           (tup/to-byte (get-val *db* (tup/pack *ss* "byte-answer")))))
    (put-val *db* (tup/pack *ss* "big-int-answer")
             (tup/byte-arr (BigInteger. "42")))
    (is (= (BigInteger. "42")
           (tup/to-big-int (get-val *db* (tup/pack *ss* "big-int-answer")))))
    )

  (testing "floating point numbers"
    (put-val *db* (tup/pack *ss* "f-pi") (tup/byte-arr (float 3.14159)))
    (is (= (float 3.14159) (tup/to-float (get-val *db* (tup/pack *ss* "f-pi")))))
    (put-val *db* (tup/pack *ss* "d-pi") (tup/byte-arr (double 3.14159)))
    (is (= 3.14159 (double 3.14159)
           (tup/to-double (get-val *db* (tup/pack *ss* "d-pi")))))
    )
  )

(run-tests)
