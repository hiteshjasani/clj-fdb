(ns clj-fdb.byte-array
  (:require [octet.core :as buf])
  (:import (java.nio ByteBuffer)))

(defn ^String to-s
  [byte-arr]
  (if (nil? byte-arr)
    nil
    (String. byte-arr)))

(defn ^Long to-long
  [byte-arr]
  (if (nil? byte-arr)
    nil
    (buf/read (ByteBuffer/wrap byte-arr) (buf/int64))))

(defn ^Integer to-int
  [byte-arr]
  (if (nil? byte-arr)
    nil
    (buf/read (ByteBuffer/wrap byte-arr) (buf/int32))))

(defn ^Short to-short
  [byte-arr]
  (if (nil? byte-arr)
    nil
    (buf/read (ByteBuffer/wrap byte-arr) (buf/int16))))

(defn ^Boolean to-bool
  [byte-arr]
  (if (nil? byte-arr)
    nil
    (buf/read (ByteBuffer/wrap byte-arr) (buf/bool))))

(defn ^Double to-double
  [byte-arr]
  (if (nil? byte-arr)
    nil
    (buf/read (ByteBuffer/wrap byte-arr) (buf/double))))

(defn ^Float to-float
  [byte-arr]
  (if (nil? byte-arr)
    nil
    (buf/read (ByteBuffer/wrap byte-arr) (buf/float))))

(defn ^Byte to-byte
  [byte-arr]
  (if (nil? byte-arr)
    nil
    (buf/read (ByteBuffer/wrap byte-arr) (buf/byte))))

(defn sz
  [typ]
  (buf/size typ))

(defn make-heap-buffer
  [size]
  (buf/allocate size {:type :heap :impl :nio}))

(defmulti byte-arr
  "Make a byte array : byte[]"
  class)
(defmethod byte-arr String [x] (.getBytes x))
(defmethod byte-arr Long [x] (let [b (make-heap-buffer (sz buf/int64))]
                               (buf/write! b x buf/int64)
                               (.array b)))
(defmethod byte-arr Integer [x] (let [b (make-heap-buffer (sz buf/int32))]
                                  (buf/write! b x buf/int32)
                                  (.array b)))
(defmethod byte-arr Short [x] (let [b (make-heap-buffer (sz buf/int16))]
                                (buf/write! b x buf/int16)
                                (.array b)))
(defmethod byte-arr Boolean [x] (let [b (make-heap-buffer (sz buf/bool))]
                                  (buf/write! b x buf/bool)
                                  (.array b)))
(defmethod byte-arr Float [x] (let [b (make-heap-buffer (sz buf/float))]
                                (buf/write! b x buf/float)
                                (.array b)))
(defmethod byte-arr Double [x] (let [b (make-heap-buffer (sz buf/double))]
                                 (buf/write! b x buf/double)
                                 (.array b)))
(defmethod byte-arr Byte [x] (let [b (make-heap-buffer (sz buf/byte))]
                               (buf/write! b x buf/byte)
                               (.array b)))
(defmethod byte-arr (Class/forName "[B") [x] x)
