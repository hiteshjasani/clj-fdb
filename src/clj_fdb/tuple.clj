(ns clj-fdb.tuple
  (:refer-clojure :rename {range core-range})
  (:require [octet.core :as buf]
            [clj-fdb.impl.core :refer [pack range]])
  (:import (java.math BigInteger)
           (java.nio ByteBuffer ByteOrder)
           (java.nio.charset StandardCharsets)
           (com.apple.foundationdb.tuple Tuple)))

(defn from
  "Creates a new Tuple from a variable number of elements. The elements
  must follow the type guidelines from add, and so can only be
  Strings, byte[]s, Numbers, UUIDs, Booleans, Lists, Tuples, or
  nulls.

  Performance: Similar to `tuple` except assumes that all the elements
  are of the same type.  If not, then an IllegalArgumentException is
  thrown."
  [& args]
  (Tuple/from (into-array args)))

(defn tuple
  "Creates a new Tuple from a variable number of elements. The elements
  must follow the type guidelines from add, and so can only be
  Strings, byte[]s, Numbers, UUIDs, Booleans, Lists, Tuples, or
  nulls.

  Performance: If you know that all the elements will be of the same type,
  then `from` will be a bit faster.
  "
  [& args]
  (if (empty? args)
    (Tuple.)
    (let [typ (type (first args))]
      (if (every? #(= typ (type %)) args)
        (Tuple/from (into-array args))
        (reduce (fn [acc x]
                  (.add acc x))
                (Tuple.)
                args)))))

(defmethod pack [Tuple] [x] (.pack x))

(defmethod range [Tuple] [x] (.range x))

(defn to-items
  [^Tuple x]
  (.getItems x))

(defn to-strs
  [^Tuple x]
  (map str (.getItems x)))

(defn ^String to-str
  ([^Tuple x]
   (to-str x 0))
  ([^Tuple x ^Integer index]
   (.getString x index)))

(defn ^boolean to-boolean
  [^Tuple x index]
  (.getBoolean x index))

(defn ^BigInteger to-big-int
  [^Tuple x index]
  (.getBigInteger x index))

(defn ^"[B" to-bytes
  [^Tuple x index]
  (.getBytes x index))

(defn ^long to-long
  [^Tuple x index]
  (.getLong x index))

(defn ^double to-double
  [^Tuple x index]
  (.getDouble x index))

(defn ^Tuple to-nested-tuple
  [^Tuple x index]
  (.getNestedTuple x index))

(defn equals
  [^Tuple a ^Tuple b]
  (.equals a b))

(defn byte-str
  [^String s]
  (.getBytes s))
