(ns clj-fdb.subspace
  (:refer-clojure :rename {range core-range})
  (:require [clj-fdb.tuple :as tup :refer [tuple pack range]])
  (:import (com.apple.foundationdb.subspace Subspace)
           (com.apple.foundationdb.tuple Tuple)))

(defmethod pack [Subspace] [x] (.pack x))
(defmethod pack [Subspace Tuple] [x y] (.pack x y))
(defmethod pack [Subspace String] [x y] (.pack x y))
(defmethod pack [Subspace Long] [x y] (.pack x y))

(defmulti subspace
  "Make a Subspace"
  (fn [& ys] (vec (concat [] (map class ys)))))
(defmethod subspace [] [] (Subspace.))
(defmethod subspace [String] [x] (Subspace. (tuple x)))
(defmethod subspace [Tuple] [x] (Subspace. x))
;; byte[]
(defmethod subspace [(Class/forName "[B")] [x] (Subspace. x))
(defmethod subspace [Tuple (Class/forName "[B")] [x y] (Subspace. x y))

(defmethod range [Subspace] [x] (.range x))
(defmethod range [Subspace Tuple] [x y] (.range x y))
(defmethod range [Subspace String] [x y] (.range x (tuple y)))
(defmethod range [Subspace Long] [x y] (.range x (tuple y)))

(extend-protocol tup/ConvertibleToTuple
  Subspace
  (tuple [x] (Tuple/fromBytes (.pack x))))

(defmethod pack [Subspace] [x] (.pack x))
(defmethod pack [Subspace Tuple] [x y] (.pack x y))
(defmethod pack [Subspace String] [x y] (.pack x y))
(defmethod pack [Subspace Long] [x y] (.pack x y))
(defmethod pack [Subspace String] [x y] (.pack x y))

(defn as-str
  [^Subspace x]
  (.toString x))

(defn as-strs
  [^Subspace x]
  (tup/to-strs (Tuple/fromBytes (.pack x))))
