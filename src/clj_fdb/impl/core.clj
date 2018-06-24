(ns clj-fdb.impl.core
  (:refer-clojure :rename {range core-range}))

(defmulti pack
  "Make a byte array for keys from Tuples and/or Subspaces"
  (fn [& ys] (mapv class ys)))

(defmulti range
  "Make a Range for use in queries and updates"
  (fn [& ys] (mapv class ys)))
