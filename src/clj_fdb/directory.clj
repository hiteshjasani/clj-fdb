(ns clj-fdb.directory
  (:require [clj-fdb.byte-array :as ba]
            [clj-fdb.tuple :as tup])
  (:import (com.apple.foundationdb TransactionContext)
           (com.apple.foundationdb.directory DirectoryLayer)))

(defn directory-layer
  "Get the default DirectoryLayer"
  []
  (DirectoryLayer/getDefault))

(defn create-or-open
  "Create or open a subdirectory at path.

  returns: java.util.concurrent.CompletableFuture<DirectorySubspace>

  [async] Async call, invoke `.join` to block until the value is returned."
  ([^DirectoryLayer dl ^TransactionContext tx-ctx path-seq]
   (.createOrOpen dl tx-ctx path-seq (ba/byte-arr "")))
  ([^DirectoryLayer dl ^TransactionContext tx-ctx path-seq ^String layer-name]
   (.createOrOpen dl tx-ctx path-seq (ba/byte-arr layer-name))))

(defn exists?
  "Check if path exists.

  [async] Async call, invoke `.join` to block until the value is returned."
  [^DirectoryLayer dl ^TransactionContext tx-ctx path-seq]
  (.exists dl tx-ctx path-seq))

(defn ls
  "List subdirectories at path.

  [async] Async call, invoke `.join` to block until the value is returned."
  [^DirectoryLayer dl ^TransactionContext tx-ctx path-seq]
  (.list dl tx-ctx path-seq))

(defn mv!
  [^DirectoryLayer dl ^TransactionContext tx-ctx old-path-seq new-path-seq]
  (.move dl tx-ctx old-path-seq new-path-seq))

(defn rm!
  "Removes directory at path-seq and all of its subdirectories and contents.

  [async] Async call, invoke `.join` to block until the value is returned."
  [^DirectoryLayer dl ^TransactionContext tx-ctx path-seq]
  (.remove dl tx-ctx path-seq))

(defn rm-if-exists!?
  "Removes directory at path-seq and all of its subdirectories and contents.
  Returns true if the directory was removed.

  [async] Async call, invoke `.join` to block until the value is returned."
  [^DirectoryLayer dl ^TransactionContext tx-ctx path-seq]
  (.removeIfExists dl tx-ctx path-seq))
