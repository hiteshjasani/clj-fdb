(ns clj-fdb.tuple
  (:refer-clojure :rename {range core-range})
  (:require [octet.core :as buf])
  (:import (java.math BigInteger)
           (java.nio ByteBuffer ByteOrder)
           (java.nio.charset StandardCharsets)
           (com.apple.foundationdb.tuple Tuple)))

(def _byte-order_ ByteOrder/BIG_ENDIAN)

(defprotocol ConvertibleToTuple
  (tuple [x]))

(extend-protocol ConvertibleToTuple
  nil
  (tuple [x] (Tuple.))

  String
  (tuple [x] (Tuple/from (into-array String [x])))

  Long
  (tuple [x] (Tuple/from (into-array Long [x])))

  Integer
  (tuple [x] (Tuple/from (into-array Integer [x])))

  BigInteger
  (tuple [x] (Tuple/from (into-array BigInteger [x])))

  Double
  (tuple [x] (Tuple/from (into-array Double [x])))

  Float
  (tuple [x] (Tuple/from (into-array Float [x])))

  Tuple
  (tuple [x] x))

(extend-protocol ConvertibleToTuple
  ;; byte[]
  (Class/forName "[B")
  (tuple [x] (Tuple/fromBytes x)))

(defmulti pack
  "Pack into a byte[].  This function is usable for keys but should
  not be used for turning values into byte arrays as the `to-*`
  functions will not be able to convert them back."
  (fn [x & ys] (vec (concat [(class x)] (map class ys)))))
(defmethod pack [Long] [x] (.pack (tuple x)))
(defmethod pack [Double] [x] (.pack (tuple x)))
(defmethod pack [Integer] [x] (.pack (tuple x)))
(defmethod pack [Float] [x] (.pack (tuple x)))
(defmethod pack [String] [x] (.getBytes x StandardCharsets/UTF_8))
(defmethod pack [Tuple] [x] (.pack x))

(defmethod pack [Long Long] [x y] (.pack (.add (tuple x) (tuple y))))
(defmethod pack [Long String] [x y] (.pack (.add (tuple x) (tuple y))))
(defmethod pack [String Long] [x y] (.pack (.add (tuple x) (tuple y))))
(defmethod pack [String String] [x y] (.pack (.add (tuple x) (tuple y))))
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
  (to-str [x] (String. x StandardCharsets/UTF_8))
  (to-strs [x] [(String. x StandardCharsets/UTF_8)])

  (to-long [x] (-> (ByteBuffer/wrap x)
                   (.order _byte-order_)
                   (.getLong)))
  (to-int [x] (-> (ByteBuffer/wrap x)
                  (.order _byte-order_)
                  (.getInt)))
  (to-big-int [x] (BigInteger. x))
  (to-short [x] (-> (ByteBuffer/wrap x)
                    (.order _byte-order_)
                    (.getShort)))
  (to-bool [x] (if (= 0x01 (.get (ByteBuffer/wrap x)))
                 true
                 false))
  (to-double [x] (-> (ByteBuffer/wrap x)
                     (.order _byte-order_)
                     (.getDouble)))
  (to-float [x] (-> (ByteBuffer/wrap x)
                    (.order _byte-order_)
                    (.getFloat)))
  (to-byte [x] (-> (ByteBuffer/wrap x)
                   (.order _byte-order_)
                   (.get)))
  )

(defn sz
  [type-kw]
  (case type-kw
    :boolean 1
    :char    1
    :byte    1
    :short   2
    :int     4
    :long    8
    :float   4
    :double  8))

(defn into-byte-arr
  [x type-kw]
  (let [bb (-> (ByteBuffer/allocate (sz type-kw))
               (.order _byte-order_))]
    (-> (case type-kw
          :boolean (.put bb (if x (byte 0x01) (byte 0x00)))
          :char    (.putChar bb x)
          :byte    (.put bb x)
          :short   (.putShort bb x)
          :int     (.putInt bb x)
          :long    (.putLong bb x)
          :float   (.putFloat bb x)
          :double  (.putDouble bb x)
          )
     (.array))))

(defprotocol ConvertibleToByteArray
  (byte-arr [x]))

(extend-protocol ConvertibleToByteArray
  ;; char
  ;; (byte-arr [x] (into-byte-arr x :char))

  String
  (byte-arr [x] (.getBytes x))

  Long
  (byte-arr [x] (into-byte-arr x :long))

  Integer
  (byte-arr [x] (into-byte-arr x :int))

  Short
  (byte-arr [x] (into-byte-arr x :short))

  Boolean
  (byte-arr [x] (into-byte-arr x :boolean))

  Byte
  (byte-arr [x] (into-byte-arr x :byte))

  Float
  (byte-arr [x] (into-byte-arr x :float))

  Double
  (byte-arr [x] (into-byte-arr x :double))
  )

(extend-protocol ConvertibleToByteArray
  (Class/forName "[B")
  (byte-arr [x] x))
