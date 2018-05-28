(ns clj-fdb.simple-test
  (:require [clojure.test :refer [run-tests deftest testing is are with-test
                                  use-fixtures]]
            [clj-fdb.db :as fdb]
            [clj-fdb.directory :as dir]
            [clj-fdb.subspace :as ss]
            [clj-fdb.tuple :as tup]
            [clj-fdb.value :as val]
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
    (put-val *db* (tup/pack *ss* "foo") (val/byte-arr "bar"))
    (let [v (val/to-str (get-val *db* (tup/pack *ss* "foo")))]
      (is (= 3 (count v)))
      (is (= java.lang.String (type v)))
      (is (= "bar" v))))

  (testing "booleans"
    (put-val *db* (tup/pack *ss* "is-true") (val/byte-arr true))
    (is (= true (val/to-bool (get-val *db* (tup/pack *ss* "is-true")))))
    (put-val *db* (tup/pack *ss* "is-false") (val/byte-arr false))
    (is (= false (val/to-bool (get-val *db* (tup/pack *ss* "is-false")))))
    (is (= nil
           (val/to-bool (get-val *db* (tup/pack *ss* "bool-does-not-exist")))))
    )

  (testing "integral numbers"
    (put-val *db* (tup/pack *ss* "int-answer") (val/byte-arr (int 42)))
    (is (= (int 42) (val/to-int (get-val *db* (tup/pack *ss* "int-answer")))))
    (put-val *db* (tup/pack *ss* "long-answer") (val/byte-arr (long 42)))
    (is (= 42 (long 42)
           (val/to-long (get-val *db* (tup/pack *ss* "long-answer")))))
    (put-val *db* (tup/pack *ss* "short-answer") (val/byte-arr (short 42)))
    (is (= (short 42)
           (val/to-short (get-val *db* (tup/pack *ss* "short-answer")))))
    (put-val *db* (tup/pack *ss* "byte-answer") (val/byte-arr (byte 42)))
    (is (= (byte 42)
           (val/to-byte (get-val *db* (tup/pack *ss* "byte-answer")))))
    (put-val *db* (tup/pack *ss* "big-int-answer")
             (val/byte-arr (BigInteger. "42")))
    (is (= (BigInteger. "42")
           (val/to-big-int (get-val *db* (tup/pack *ss* "big-int-answer")))))
    )

  (testing "floating point numbers"
    (put-val *db* (tup/pack *ss* "f-pi") (val/byte-arr (float 3.14159)))
    (is (= (float 3.14159) (val/to-float (get-val *db* (tup/pack *ss* "f-pi")))))
    (put-val *db* (tup/pack *ss* "d-pi") (val/byte-arr (double 3.14159)))
    (is (= 3.14159 (double 3.14159)
           (val/to-double (get-val *db* (tup/pack *ss* "d-pi")))))
    )
  )

(deftest put-vals-test
  (testing "strings"
    (let [ss (-> (dir/create-or-open (dir/directory-layer) *db*
                                     (conj test-subspace "put-vals-strings"))
                 (.join))]
      (put-vals *db* {(tup/pack ss "name") (val/byte-arr "elle")
                      (tup/pack ss "state") (val/byte-arr "wa")
                      (tup/pack ss "city") (val/byte-arr "seattle")})
      (is (= "elle" (val/to-str (get-val *db* (tup/pack ss "name")))))
      (is (= "wa" (val/to-str (get-val *db* (tup/pack ss "state")))))
      (is (= "seattle" (val/to-str (get-val *db* (tup/pack ss "city")))))
      ))

  (testing "combo-types"
    (let [ss (-> (dir/create-or-open (dir/directory-layer) *db*
                                     (conj test-subspace "put-vals-combos"))
                 (.join))]
      (put-vals *db* {(tup/pack ss "name") (val/byte-arr "elle")
                      (tup/pack ss "age") (val/byte-arr 22)
                      (tup/pack ss "score") (val/byte-arr 3.14159)
                      (tup/pack ss "female?") (val/byte-arr true)})
      (is (= "elle" (val/to-str (get-val *db* (tup/pack ss "name")))))
      (is (= 22 (val/to-long (get-val *db* (tup/pack ss "age")))))
      (is (= 3.14159 (val/to-double (get-val *db* (tup/pack ss "score")))))
      (is (= true (val/to-bool (get-val *db* (tup/pack ss "female?")))))
      ))
  )

(deftest atomic-tests
  (testing "counters"
    (let [ss (-> (dir/create-or-open (dir/directory-layer) *db*
                                     (conj test-subspace "atomic-counters"))
                 (.join))]
      (testing "incrementing"
        (put-val *db* (tup/pack ss "id-gen") (val/byte-arr 0))
        (is (= 0 (val/to-long (get-val *db* (tup/pack ss "id-gen")))))
        (atomic *db* (tup/pack ss "id-gen") :add (val/byte-arr 1))
        (is (= 1 (val/to-long (get-val *db* (tup/pack ss "id-gen")))))
        (atomic *db* (tup/pack ss "id-gen") :add (val/byte-arr 1))
        (is (= 2 (val/to-long (get-val *db* (tup/pack ss "id-gen"))))))
      (testing "incrementing by more than 1"
        (atomic *db* (tup/pack ss "id-gen") :add (val/byte-arr 3))
        (is (= 5 (val/to-long (get-val *db* (tup/pack ss "id-gen"))))))
      (testing "decrementing"
        (atomic *db* (tup/pack ss "id-gen") :add (val/byte-arr 0))
        (is (= 5 (val/to-long (get-val *db* (tup/pack ss "id-gen")))))
        (atomic *db* (tup/pack ss "id-gen") :add (val/byte-arr -1))
        (is (= 4 (val/to-long (get-val *db* (tup/pack ss "id-gen")))))
        (atomic *db* (tup/pack ss "id-gen") :add (val/byte-arr -3))
        (is (= 1 (val/to-long (get-val *db* (tup/pack ss "id-gen")))))
        )
      ))

  (testing "bit ops"
    (let [ss (-> (dir/create-or-open (dir/directory-layer) *db*
                                     (conj test-subspace "atomic-bit-ops"))
                 (.join))]
      (testing "AND"
        (put-val *db* (tup/pack ss "foo") (val/byte-arr 0xabcd))
        (is (= 0xabcd (val/to-long (get-val *db* (tup/pack ss "foo")))))

        (atomic *db* (tup/pack ss "foo") :bit-and (val/byte-arr 0x0f0f))
        (is (= 0x0b0d (val/to-long (get-val *db* (tup/pack ss "foo")))))
        )

      (testing "OR"
        (put-val *db* (tup/pack ss "foo") (val/byte-arr 0x0b0d))
        (is (= 0x0b0d (val/to-long (get-val *db* (tup/pack ss "foo")))))

        (atomic *db* (tup/pack ss "foo") :bit-or (val/byte-arr 0xa0c0))
        (is (= 0xabcd (val/to-long (get-val *db* (tup/pack ss "foo")))))
        )

      (testing "XOR"
        (put-val *db* (tup/pack ss "foo") (val/byte-arr 0xabcd))
        (is (= 0xabcd (val/to-long (get-val *db* (tup/pack ss "foo")))))

        (atomic *db* (tup/pack ss "foo") :bit-xor (val/byte-arr 0x6622))
        (is (= 0xcdef (val/to-long (get-val *db* (tup/pack ss "foo")))))
        )

      (testing "MIN"
        (put-val *db* (tup/pack ss "foo") (val/byte-arr 0xabcd))
        (is (= 0xabcd (val/to-long (get-val *db* (tup/pack ss "foo")))))

        (atomic *db* (tup/pack ss "foo") :min (val/byte-arr 0x6622))
        (is (= 0x6622 (val/to-long (get-val *db* (tup/pack ss "foo")))))

        (atomic *db* (tup/pack ss "foo") :min (val/byte-arr 0xcdef))
        (is (= 0x6622 (val/to-long (get-val *db* (tup/pack ss "foo")))))
        )

      (testing "MAX"
        (put-val *db* (tup/pack ss "foo") (val/byte-arr 0xabcd))
        (is (= 0xabcd (val/to-long (get-val *db* (tup/pack ss "foo")))))

        (atomic *db* (tup/pack ss "foo") :max (val/byte-arr 0x6622))
        (is (= 0xabcd (val/to-long (get-val *db* (tup/pack ss "foo")))))

        (atomic *db* (tup/pack ss "foo") :max (val/byte-arr 0xcdef))
        (is (= 0xcdef (val/to-long (get-val *db* (tup/pack ss "foo")))))
        )
      ))
  )

(deftest range-tests
  (testing "subspace range"
    (let [ss (-> (dir/create-or-open (dir/directory-layer) *db*
                                     (conj test-subspace "subspace range"))
                 (.join))]
      (testing "incrementing"
        (put-val *db* (tup/pack ss (tup/tuple "blob" 1))
                 (val/byte-arr "part 1"))
        (put-val *db* (tup/pack ss (tup/tuple "blob" 2))
                 (val/byte-arr "part 2"))
        (put-val *db* (tup/pack ss (tup/tuple "blob" 3))
                 (val/byte-arr "part 3"))

        (let [rows (map (fn [exp-k exp-v act-kv] [exp-k exp-v act-kv])
                        [[["blob"] [1]] [["blob"] [2]] [["blob"] [3]]]
                        ["part 1" "part 2" "part 3"]
                        (get-range *db* (.range ss)))]
          (doseq [[exp-k exp-v act-kv] rows]
            (is (= exp-k (tup/to-items
                          (tup/tuple (.unpack ss (.getKey act-kv))))))
            (is (= exp-v (val/to-str (.getValue act-kv))))
            ))
        )
      ))
  )

(run-tests)
