(ns clj-fdb.tuple
  (:refer-clojure :rename {range core-range})
  (:import (com.apple.foundationdb.tuple Tuple)))

(defmulti tuple
  "Make a tuple"
  class)
(defmethod tuple nil [x] (Tuple.))
(defmethod tuple Long [x] (Tuple/from (into-array Long [x])))
(defmethod tuple String [x] (Tuple/from (into-array String [x])))
;; byte[]
(defmethod tuple (Class/forName "[B") [x] (Tuple/fromBytes x))
(defmethod tuple Tuple [x] x)

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

(defprotocol TupleConvertible
  (as-tuple [x] [prefix x])
  (as-bytes [x])
  (as-str [x])
  (as-strs [x])
  )

(defn as-bytes
  [^Tuple t]
  (.pack t))

(defn as-str
  [^Tuple t]
  (.getString t 0))

(defn as-strs
  [^Tuple t]
  (.getItems t))
