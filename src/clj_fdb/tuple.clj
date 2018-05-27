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

(defn to-str
  ([^Tuple x]
   (to-str x 0))
  ([^Tuple x ^Integer index]
   (.getString x index)))

(defn to-strs
  [^Tuple x]
  (.getItems x))
