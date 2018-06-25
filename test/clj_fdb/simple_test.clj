(ns clj-fdb.simple-test
  (:refer-clojure :rename {range core-range})
  (:require [clojure.test :refer [run-tests deftest testing is are with-test
                                  use-fixtures]]
            [clj-fdb.db :as fdb]
            [clj-fdb.directory :as dir]
            [clj-fdb.subspace :as ss]
            [clj-fdb.tuple :as tup]
            [clj-fdb.value :as val]
            [clj-fdb.key-selector :as ks]
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
    (put-val *db* (pack *ss* "foo") (val/byte-arr "bar"))
    (let [v (val/to-str (get-val *db* (pack *ss* "foo")))]
      (is (= 3 (count v)))
      (is (= java.lang.String (type v)))
      (is (= "bar" v))))

  (testing "booleans"
    (put-val *db* (pack *ss* "is-true") (val/byte-arr true))
    (is (= true (val/to-bool (get-val *db* (pack *ss* "is-true")))))
    (put-val *db* (pack *ss* "is-false") (val/byte-arr false))
    (is (= false (val/to-bool (get-val *db* (pack *ss* "is-false")))))
    (is (= nil
           (val/to-bool (get-val *db* (pack *ss* "bool-does-not-exist")))))
    )

  (testing "integral numbers"
    (put-val *db* (pack *ss* "int-answer") (val/byte-arr (int 42)))
    (is (= (int 42) (val/to-int (get-val *db* (pack *ss* "int-answer")))))
    (put-val *db* (pack *ss* "long-answer") (val/byte-arr (long 42)))
    (is (= 42 (long 42)
           (val/to-long (get-val *db* (pack *ss* "long-answer")))))
    (put-val *db* (pack *ss* "short-answer") (val/byte-arr (short 42)))
    (is (= (short 42)
           (val/to-short (get-val *db* (pack *ss* "short-answer")))))
    (put-val *db* (pack *ss* "byte-answer") (val/byte-arr (byte 42)))
    (is (= (byte 42)
           (val/to-byte (get-val *db* (pack *ss* "byte-answer")))))
    (put-val *db* (pack *ss* "big-int-answer")
             (val/byte-arr (BigInteger. "42")))
    (is (= (BigInteger. "42")
           (val/to-big-int (get-val *db* (pack *ss* "big-int-answer")))))
    )

  (testing "floating point numbers"
    (put-val *db* (pack *ss* "f-pi") (val/byte-arr (float 3.14159)))
    (is (= (float 3.14159) (val/to-float (get-val *db* (pack *ss* "f-pi")))))
    (put-val *db* (pack *ss* "d-pi") (val/byte-arr (double 3.14159)))
    (is (= 3.14159 (double 3.14159)
           (val/to-double (get-val *db* (pack *ss* "d-pi")))))
    )
  )

(deftest put-vals-test
  (testing "strings"
    (let [ss (-> (dir/create-or-open (dir/directory-layer) *db*
                                     (conj test-subspace "put-vals-strings"))
                 (.join))]
      (put-vals *db* {(pack ss "name") (val/byte-arr "elle")
                      (pack ss "state") (val/byte-arr "wa")
                      (pack ss "city") (val/byte-arr "seattle")})
      (is (= "elle" (val/to-str (get-val *db* (pack ss "name")))))
      (is (= "wa" (val/to-str (get-val *db* (pack ss "state")))))
      (is (= "seattle" (val/to-str (get-val *db* (pack ss "city")))))
      ))

  (testing "combo-types"
    (let [ss (-> (dir/create-or-open (dir/directory-layer) *db*
                                     (conj test-subspace "put-vals-combos"))
                 (.join))]
      (put-vals *db* {(pack ss "name") (val/byte-arr "elle")
                      (pack ss "age") (val/byte-arr 22)
                      (pack ss "score") (val/byte-arr 3.14159)
                      (pack ss "female?") (val/byte-arr true)})
      (is (= "elle" (val/to-str (get-val *db* (pack ss "name")))))
      (is (= 22 (val/to-long (get-val *db* (pack ss "age")))))
      (is (= 3.14159 (val/to-double (get-val *db* (pack ss "score")))))
      (is (= true (val/to-bool (get-val *db* (pack ss "female?")))))
      ))
  )

(deftest atomic-tests
  (testing "counters"
    (let [ss (-> (dir/create-or-open (dir/directory-layer) *db*
                                     (conj test-subspace "atomic-counters"))
                 (.join))]
      (testing "incrementing"
        (put-val *db* (pack ss "id-gen") (val/byte-arr 0))
        (is (= 0 (val/to-long (get-val *db* (pack ss "id-gen")))))
        (atomic *db* (pack ss "id-gen") :add (val/byte-arr 1))
        (is (= 1 (val/to-long (get-val *db* (pack ss "id-gen")))))
        (atomic *db* (pack ss "id-gen") :add (val/byte-arr 1))
        (is (= 2 (val/to-long (get-val *db* (pack ss "id-gen"))))))
      (testing "incrementing by more than 1"
        (atomic *db* (pack ss "id-gen") :add (val/byte-arr 3))
        (is (= 5 (val/to-long (get-val *db* (pack ss "id-gen"))))))
      (testing "decrementing"
        (atomic *db* (pack ss "id-gen") :add (val/byte-arr 0))
        (is (= 5 (val/to-long (get-val *db* (pack ss "id-gen")))))
        (atomic *db* (pack ss "id-gen") :add (val/byte-arr -1))
        (is (= 4 (val/to-long (get-val *db* (pack ss "id-gen")))))
        (atomic *db* (pack ss "id-gen") :add (val/byte-arr -3))
        (is (= 1 (val/to-long (get-val *db* (pack ss "id-gen")))))
        )
      ))

  (testing "bit ops"
    (let [ss (-> (dir/create-or-open (dir/directory-layer) *db*
                                     (conj test-subspace "atomic-bit-ops"))
                 (.join))]
      (testing "AND"
        (put-val *db* (pack ss "foo") (val/byte-arr 0xabcd))
        (is (= 0xabcd (val/to-long (get-val *db* (pack ss "foo")))))

        (atomic *db* (pack ss "foo") :bit-and (val/byte-arr 0x0f0f))
        (is (= 0x0b0d (val/to-long (get-val *db* (pack ss "foo")))))
        )

      (testing "OR"
        (put-val *db* (pack ss "foo") (val/byte-arr 0x0b0d))
        (is (= 0x0b0d (val/to-long (get-val *db* (pack ss "foo")))))

        (atomic *db* (pack ss "foo") :bit-or (val/byte-arr 0xa0c0))
        (is (= 0xabcd (val/to-long (get-val *db* (pack ss "foo")))))
        )

      (testing "XOR"
        (put-val *db* (pack ss "foo") (val/byte-arr 0xabcd))
        (is (= 0xabcd (val/to-long (get-val *db* (pack ss "foo")))))

        (atomic *db* (pack ss "foo") :bit-xor (val/byte-arr 0x6622))
        (is (= 0xcdef (val/to-long (get-val *db* (pack ss "foo")))))
        )

      (testing "MIN"
        (put-val *db* (pack ss "foo") (val/byte-arr 0xabcd))
        (is (= 0xabcd (val/to-long (get-val *db* (pack ss "foo")))))

        (atomic *db* (pack ss "foo") :min (val/byte-arr 0x6622))
        (is (= 0x6622 (val/to-long (get-val *db* (pack ss "foo")))))

        (atomic *db* (pack ss "foo") :min (val/byte-arr 0xcdef))
        (is (= 0x6622 (val/to-long (get-val *db* (pack ss "foo")))))
        )

      (testing "MAX"
        (put-val *db* (pack ss "foo") (val/byte-arr 0xabcd))
        (is (= 0xabcd (val/to-long (get-val *db* (pack ss "foo")))))

        (atomic *db* (pack ss "foo") :max (val/byte-arr 0x6622))
        (is (= 0xabcd (val/to-long (get-val *db* (pack ss "foo")))))

        (atomic *db* (pack ss "foo") :max (val/byte-arr 0xcdef))
        (is (= 0xcdef (val/to-long (get-val *db* (pack ss "foo")))))
        )
      ))
  )

(deftest range-tests
  (testing "subspace range"
    (let [ss         (-> (dir/create-or-open (dir/directory-layer) *db*
                                             (conj test-subspace "subspace range"))
                         (.join))
          num-recs   10
          key-tuples (mapv #(tup/tuple "id" %) (core-range num-recs))
          values     (mapv #(str "person " %) (core-range num-recs))
          kvs        (zipmap key-tuples values)
          ]
      (doseq [[k v] kvs]
        (put-val *db* (pack ss k) (val/byte-arr v)))

      (testing "increasing keys, full subspace"
        (let [rows (map (fn [exp-k exp-v act-kv]
                          [exp-k exp-v act-kv])
                        key-tuples
                        values
                        (get-range *db* (range ss)))]
          (is (= num-recs (count rows)))
          (doseq [[exp-k exp-v act-kv] rows]
            (is (tup/equals exp-k (.unpack ss (.getKey act-kv))))
            (is (= exp-v (val/to-str (.getValue act-kv))))
            )))

      (testing "increasing keys, partial tuple key"
        (let [rows (map (fn [exp-k exp-v act-kv]
                          [exp-k exp-v act-kv])
                        key-tuples
                        values
                        (get-range *db* (range ss (tup/tuple "id"))))]
          (is (= num-recs (count rows)))
          (doseq [[exp-k exp-v act-kv] rows]
            (is (tup/equals exp-k (.unpack ss (.getKey act-kv))))
            (is (= exp-v (val/to-str (.getValue act-kv))))
            )))

      (testing "increasing keys, range starts with partial tuple key"
        (let [rows (map (fn [exp-k exp-v act-kv]
                          [exp-k exp-v act-kv])
                        key-tuples
                        values
                        (get-range *db* (range-starts-with
                                         (pack ss (tup/tuple "id" 0)))))]
          (is (= 1 (count rows)))
          (doseq [[exp-k exp-v act-kv] rows]
            (is (tup/equals exp-k (.unpack ss (.getKey act-kv))))
            (is (= exp-v (val/to-str (.getValue act-kv))))
            )))

      (testing "non-matching partial tuple key returns zero matches"
        (let [rows (map (fn [exp-k exp-v act-kv]
                          [exp-k exp-v act-kv])
                        key-tuples
                        values
                        (get-range *db* (range ss (tup/tuple "invalid-key"))))]
          (is (zero? (count rows)))
          ))

      (testing "increasing keys, tuple key from [0,9)"
        (let [rows (map (fn [exp-k exp-v act-kv]
                          [exp-k exp-v act-kv])
                        key-tuples
                        values
                        (get-range *db* (ks/>= (pack ss (first key-tuples)))
                                   (ks/<= (pack ss (last key-tuples)))))]
          (is (= (dec num-recs) (count rows)))
          (doseq [[exp-k exp-v act-kv] rows]
            (is (tup/equals exp-k (.unpack ss (.getKey act-kv))))
            (is (= exp-v (val/to-str (.getValue act-kv))))
            )))

      (testing "increasing keys, tuple key from [0,9), limit 3"
        (let [rows (map (fn [exp-k exp-v act-kv]
                          [exp-k exp-v act-kv])
                        key-tuples
                        values
                        (get-range *db* (ks/>= (pack ss (first key-tuples)))
                                   (ks/<= (pack ss (last key-tuples)))
                                   3))]
          (is (= 3 (count rows)))
          (doseq [[exp-k exp-v act-kv] rows]
            (is (tup/equals exp-k (.unpack ss (.getKey act-kv))))
            (is (= exp-v (val/to-str (.getValue act-kv))))
            )))

      (testing "increasing keys, partial list [2,5)"
        (let [rows (map (fn [exp-k exp-v act-kv]
                          [exp-k exp-v act-kv])
                        (drop 2 key-tuples)
                        (drop 2 values)
                        (get-range *db* (ks/>= (pack ss (nth key-tuples 2)))
                                   (ks/<= (pack ss (nth key-tuples 5)))))]
          (is (= 3 (count rows)))
          (doseq [[exp-k exp-v act-kv] rows]
            (is (tup/equals exp-k (.unpack ss (.getKey act-kv))))
            (is (= exp-v (val/to-str (.getValue act-kv))))
            )))
      ))
  )

(run-tests)
