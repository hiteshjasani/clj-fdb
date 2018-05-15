(ns clj-fdb.macros
  (:import (java.util.function Function)))

(defn ^Function as-java-fn [f]
  (reify Function
    (apply [this arg]
      (f arg))))

(defmacro jfn [& args]
  `(as-java-fn (fn ~@args)))
