(ns clj-fdb.tuple
  (:refer-clojure :rename {range core-range})
  (:require [octet.core :as buf])
  (:import (java.math BigInteger)
           (java.nio ByteBuffer)
           (com.apple.foundationdb.tuple Tuple)))

(defprotocol ConvertibleToTuple
  (tuple [x]))

(extend-protocol ConvertibleToTuple
  nil
  (tuple [x] (Tuple.))

  String
  (tuple [x] (Tuple/from (into-array String [x])))

  Long
  (tuple [x] (Tuple/from (into-array Long [x])))

  BigInteger
  (tuple [x] (Tuple/from (into-array BigInteger [x])))

  Float
  (tuple [x] (Tuple/from (into-array Float [x])))

  Double
  (tuple [x] (Tuple/from (into-array Double [x])))

  Tuple
  (tuple [x] x))

(extend-protocol ConvertibleToTuple
  ;; byte[]
  (Class/forName "[B")
  (tuple [x] (Tuple/fromBytes x)))

(defmulti pack
  "Pack into a byte[]"
  (fn [x & ys] (vec (concat [(class x)] (map class ys)))))
(defmethod pack [Long] [x] (.pack (tuple x)))
(defmethod pack [Long Long] [x y] (.pack (.add (tuple x) (tuple y))))
(defmethod pack [Long String] [x y] (.pack (.add (tuple x) (tuple y))))
(defmethod pack [String] [x] (.pack (tuple x)))
(defmethod pack [String Long] [x y] (.pack (.add (tuple x) (tuple y))))
(defmethod pack [String String] [x y] (.pack (.add (tuple x) (tuple y))))
(defmethod pack [Tuple] [x] (.pack x))
(defmethod pack [Tuple Tuple] [x y] (.pack (.add x y)))

(defmulti range
  "Make a Range for use in queries and updates"
  (fn [& ys] (mapv class ys)))
(defmethod range [Tuple] [x] (.range x))

(defprotocol ConvertibleToUsableTypes
  (^String to-str [x])
  (to-strs [x])
  (^long to-long [x])
  (^int to-int [x])
  (^BigInteger to-big-int [x])
  (^short to-short [x])
  (^boolean to-bool [x])
  (^double to-double [x])
  (^float to-float [x])
  (^byte to-byte [x]))

(extend-protocol ConvertibleToUsableTypes
  Tuple
  (to-str [x] (.getString x 0))
  (to-strs [x] (.getItems x))
  (to-long [x] (.getLong x 0))
  (to-int [x] (.getLong x 0))           ; ack! possible truncated values
  (to-big-int [x] (.getBigInteger x 0))
  (to-short [x] (.getLong x 0))         ; ack! possible truncated values
  (to-bool [x] (.getBoolean x 0))
  (to-double [x] (.getDouble x 0))
  (to-float [x] (.getFloat x 0))
  (to-byte [x] (.getLong x 0))          ; ack! possible truncated values
  )

(extend-protocol ConvertibleToUsableTypes
  ;; byte[]
  (Class/forName "[B")
  (to-str [x] (String. x))
  (to-strs [x] [(String. x)])
  (to-long [x] (buf/read (ByteBuffer/wrap x) (buf/int64)))
  (to-int [x] (buf/read (ByteBuffer/wrap x) (buf/int32)))
  (to-big-int [x] (BigInteger. x))
  (to-short [x] (buf/read (ByteBuffer/wrap x) (buf/int16)))
  (to-bool [x] (buf/read (ByteBuffer/wrap x) (buf/bool)))
  (to-double [x] (buf/read (ByteBuffer/wrap x) (buf/double)))
  (to-float [x] (buf/read (ByteBuffer/wrap x) (buf/float)))
  (to-byte [x] (buf/read (ByteBuffer/wrap x) (buf/byte)))
  )

(defn sz
  [typ]
  (buf/size typ))

(defn make-heap-buffer
  [size]
  (buf/allocate size {:type :heap :impl :nio}))

(defprotocol ConvertibleToByteArray
  (byte-arr [x]))

(extend-protocol ConvertibleToByteArray
  String
  (byte-arr [x] (.getBytes x))

  Long
  (byte-arr [x] (let [b (make-heap-buffer (sz buf/int64))]
                  (buf/write! b x buf/int64)
                  (.array b)))

  Integer
  (byte-arr [x] (let [b (make-heap-buffer (sz buf/int32))]
                  (buf/write! b x buf/int32)
                  (.array b)))

  Short
  (byte-arr [x] (let [b (make-heap-buffer (sz buf/int16))]
                  (buf/write! b x buf/int16)
                  (.array b)))

  Boolean
  (byte-arr [x] (let [b (make-heap-buffer (sz buf/bool))]
                  (buf/write! b x buf/bool)
                  (.array b)))

  Boolean
  (byte-arr [x] (let [b (make-heap-buffer (sz buf/bool))]
                  (buf/write! b x buf/bool)
                  (.array b)))

  Byte
  (byte-arr [x] (let [b (make-heap-buffer (sz buf/byte))]
                  (buf/write! b x buf/byte)
                  (.array b)))

  Float
  (byte-arr [x] (let [b (make-heap-buffer (sz buf/float))]
                  (buf/write! b x buf/float)
                  (.array b)))

  Double
  (byte-arr [x] (let [b (make-heap-buffer (sz buf/double))]
                  (buf/write! b x buf/double)
                  (.array b)))
  )

(extend-protocol ConvertibleToByteArray
  (Class/forName "[B")
  (byte-arr [x] x))
