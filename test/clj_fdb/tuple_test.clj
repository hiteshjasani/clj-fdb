(ns clj-fdb.tuple-test
  (:require [clojure.test :refer [run-tests deftest testing is are
                                  with-test use-fixtures]])
  (:use [clj-fdb.tuple :rename {range tuple-range}])
  (:import (com.apple.foundationdb.tuple Tuple)))

(deftest making-tuples
  (is (= (Tuple.) (tuple nil)))
  (is (= "foo" (to-str (tuple "foo"))))
  )

(deftest strings-can-roundtrip
  (is (= "Foo$bar98" (to-str (byte-arr "Foo$bar98"))))
  (is (= "" (to-str (byte-arr ""))))
  (is (= "012345678901234567890123456789"
         (to-str (byte-arr "012345678901234567890123456789")))))

(deftest bools-can-roundtrip
  (is (true? (to-bool (byte-arr true))))
  (is (false? (to-bool (byte-arr false)))))

(deftest ints-can-roundtrip
  (testing "byte values"
    (is (= (byte 127) (to-byte (byte-arr (byte 127)))))
    (is (= (byte 0) (to-byte (byte-arr (byte 0)))))
    (is (= (byte -42) (to-byte (byte-arr (byte -42)))))
    (is (= Byte/MAX_VALUE (to-byte (byte-arr Byte/MAX_VALUE))))
    (is (= Byte/MIN_VALUE (to-byte (byte-arr Byte/MIN_VALUE)))))
  (testing "short values"
    (is (= (short 1234) (to-short (byte-arr (short 1234)))))
    (is (= (short 0) (to-short (byte-arr (short 0)))))
    (is (= (short -42) (to-short (byte-arr (short -42)))))
    (is (= Short/MAX_VALUE (to-short (byte-arr Short/MAX_VALUE))))
    (is (= Short/MIN_VALUE (to-short (byte-arr Short/MIN_VALUE)))))
  (testing "integer values"
    (is (= (int 1234) (to-int (byte-arr (int 1234)))))
    (is (= (int 0) (to-int (byte-arr (int 0)))))
    (is (= (int -42) (to-int (byte-arr (int -42)))))
    (is (= Integer/MAX_VALUE (to-int (byte-arr Integer/MAX_VALUE))))
    (is (= Integer/MIN_VALUE (to-int (byte-arr Integer/MIN_VALUE)))))
  (testing "long values"
    (is (= 1234 (to-long (byte-arr 1234))))
    (is (= 0 (to-long (byte-arr 0))))
    (is (= -42 (to-long (byte-arr -42))))
    (is (= Long/MAX_VALUE (to-long (byte-arr Long/MAX_VALUE))))
    (is (= Long/MIN_VALUE (to-long (byte-arr Long/MIN_VALUE))))))

(deftest floats-can-roundtrip
  (testing "float values"
    (is (= (float 0.0) (to-float (byte-arr (float 0.0)))))
    (is (= (float -42.0) (to-float (byte-arr (float -42.0)))))
    (is (= (float 3.14159) (to-float (byte-arr (float 3.14159)))))
    (is (= Float/MAX_VALUE (to-float (byte-arr Float/MAX_VALUE))))
    (is (= Float/MIN_VALUE (to-float (byte-arr Float/MIN_VALUE)))))
  (testing "double values"
    (is (= 0.0 (to-double (byte-arr 0.0))))
    (is (= -42.0 (to-double (byte-arr -42.0))))
    (is (= 3.14159 (to-double (byte-arr 3.14159))))
    (is (= Double/MAX_VALUE (to-double (byte-arr Double/MAX_VALUE))))
    (is (= Double/MIN_VALUE (to-double (byte-arr Double/MIN_VALUE))))))

(run-tests)
