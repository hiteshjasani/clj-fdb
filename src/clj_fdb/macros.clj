(ns clj-fdb.macros
  (:import (java.util.function Function)
           (com.apple.foundationdb TransactionContext)))

(defn ^Function as-java-fn [f]
  (reify Function
    (apply [this arg]
      (f arg))))

(defmacro jfn [& args]
  `(as-java-fn (fn ~@args)))

(defmacro run-tx
  [^TransactionContext tx-ctx java-fn]
  `(.run ~tx-ctx ~java-fn))

(defmacro read-tx
  [^TransactionContext tx-ctx java-fn]
  `(.read ~tx-ctx ~java-fn))
