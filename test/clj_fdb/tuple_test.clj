(ns clj-fdb.tuple-test
  (:require [clojure.test :refer [run-tests deftest testing is are
                                  with-test use-fixtures]])
  (:use [clj-fdb.tuple :rename {range tuple-range}])
  (:import (com.apple.foundationdb.tuple Tuple)))

(deftest making-tuples
  (testing "single tuples"
    (is (= (Tuple.) (tuple nil)))
    (is (= "foo" (to-str (tuple "foo")))))

  (testing "2 tuples"
    (is (= ["(\"foo\")" "(\"bar\")"] (to-strs (tuple "foo" "bar"))))
    (is (= "((\"foo\"), (\"bar\"))" (.toString (tuple "foo" "bar"))))
    )
  )

;; packed ints do not currently have conversion functions back to ints
#_(deftest ints-can-roundtrip-as-packed
  (testing "byte values"
    (is (= (byte 127) (to-byte (pack (byte 127)))))
    (is (= (byte 0) (to-byte (pack (byte 0)))))
    (is (= (byte -42) (to-byte (pack (byte -42)))))
    (is (= Byte/MAX_VALUE (to-byte (pack Byte/MAX_VALUE))))
    (is (= Byte/MIN_VALUE (to-byte (pack Byte/MIN_VALUE)))))
  (testing "short values"
    (is (= (short 1234) (to-short (pack (short 1234)))))
    (is (= (short 0) (to-short (pack (short 0)))))
    (is (= (short -42) (to-short (pack (short -42)))))
    (is (= Short/MAX_VALUE (to-short (pack Short/MAX_VALUE))))
    (is (= Short/MIN_VALUE (to-short (pack Short/MIN_VALUE)))))
  (testing "integer values"
    (is (= (int 1234) (to-int (pack (int 1234)))))
    (is (= (int 0) (to-int (pack (int 0)))))
    (is (= (int -42) (to-int (pack (int -42)))))
    (is (= Integer/MAX_VALUE (to-int (pack Integer/MAX_VALUE))))
    (is (= Integer/MIN_VALUE (to-int (pack Integer/MIN_VALUE)))))
  (testing "long values"
    (is (= 1234 (to-long (pack 1234))))
    (is (= 0 (to-long (pack 0))))
    (is (= -42 (to-long (pack -42))))
    (is (= Long/MAX_VALUE (to-long (pack Long/MAX_VALUE))))
    (is (= Long/MIN_VALUE (to-long (pack Long/MIN_VALUE))))))



(run-tests)
