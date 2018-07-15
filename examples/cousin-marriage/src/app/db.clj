(ns app.db
  (:require [clojure.pprint :as pp]
            [clj-fdb.db]
            [clj-fdb.macros :refer [jfn]]
            [clj-fdb.directory :as dir]
            [clj-fdb.subspace :as ss]
            [clj-fdb.tuple :as tup]
            [clj-fdb.value :as val]
            [clj-fdb.simple :as simp]
            [mount.lite :as mount])
  (:import (com.apple.foundationdb Database TransactionContext)
           (com.apple.foundationdb.subspace Subspace)))

(def subspace-name ["examples" "cousin-marriage"])

;; DB connection
(mount/defstate db
  :start (clj-fdb.db/open)
  :stop (clj-fdb.db/close @db))

;; Subspace for our example.
;;
;; The original example does not use a Subspace. But we're using one
;; since it's a good practice to use a namespace with your keys to
;; avoid collisions with other apps or examples using the same db.
(mount/defstate ss
  :start (.join (dir/create-or-open (dir/directory-layer) @db subspace-name))
  :stop (.join (dir/rm-if-exists!? (dir/directory-layer) @db subspace-name)))
