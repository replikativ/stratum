(ns stratum.enum-test
  "End-to-end coverage for PostgreSQL/DuckDB-compatible `CREATE TYPE …
   AS ENUM (…)` in Stratum: parser, registry, column binding, INSERT
   validation, catalog introspection (pg_type + pg_enum), durability
   across restart, and ORDER BY by declaration position."
  (:require [clojure.test :refer [deftest is testing]]
            [konserve.core :as kstore]
            [stratum.server :as srv]
            [stratum.sql.enum-ddl :as enum-ddl])
  (:import [java.io File]
           [java.util UUID]
           [stratum.internal PgWireServer$QueryResult]))

;; ---------------------------------------------------------------------------
;; Helpers — mirror those in `server-restart-test`.

(defn- temp-dir ^String []
  (let [d (File/createTempFile "stratum-enum-" "")]
    (.delete d) (.mkdirs d) (.getAbsolutePath d)))

(defn- delete-dir [^String path]
  (let [f (File. path)]
    (when (.exists f)
      (doseq [^File child (reverse (file-seq f))] (.delete child)))))

(defn- file-store-at [^String path]
  (let [cfg {:backend :file
             :path    path
             :id      (UUID/nameUUIDFromBytes (.getBytes path "UTF-8"))}]
    (if (kstore/store-exists? cfg {:sync? true})
      (kstore/connect-store cfg {:sync? true})
      (kstore/create-store  cfg {:sync? true}))))

(def ^:private port-counter (atom 5800))
(defn- next-port [] (swap! port-counter inc))

(defn- run-sql [server sql]
  (let [^PgWireServer$QueryResult qr
        (@(requiring-resolve 'stratum.server/execute-sql)
         sql (:registry server) (:data-dir server) (:store server))]
    {:tag   (.commandTag qr)
     :error (.error qr)
     :rows  (when (.rows qr) (mapv vec (vec (.rows qr))))}))

(defmacro ^:private with-server [[binding-name opts] & body]
  `(let [~binding-name (srv/start ~opts)]
     (try ~@body (finally (srv/stop ~binding-name)))))

;; ===========================================================================
;; Parser (phase 1a) — preserves declaration order, validates uniqueness.

(deftest parser-happy-path
  (is (= {:op          :create-type-enum
          :type-name   "mood"
          :values      ["sad" "ok" "happy"]
          :or-replace? false}
         (enum-ddl/parse-create-type-enum
          "CREATE TYPE mood AS ENUM ('sad','ok','happy')"))))

(deftest parser-or-replace
  (is (:or-replace?
        (enum-ddl/parse-create-type-enum
         "CREATE OR REPLACE TYPE mood AS ENUM ('a','b')"))))

(deftest parser-strips-schema-qualifier
  (is (= "mood"
         (:type-name
          (enum-ddl/parse-create-type-enum
           "CREATE TYPE public.mood AS ENUM ('x')")))))

(deftest parser-rejects-empty-value-list
  (is (thrown-with-msg? clojure.lang.ExceptionInfo
                        #"at least one value"
                        (enum-ddl/parse-create-type-enum
                         "CREATE TYPE m AS ENUM ()"))))

(deftest parser-rejects-duplicate-values
  (is (thrown-with-msg? clojure.lang.ExceptionInfo
                        #"unique"
                        (enum-ddl/parse-create-type-enum
                         "CREATE TYPE m AS ENUM ('a','a','b')"))))

;; ===========================================================================
;; CREATE TYPE registers + CREATE TABLE binds the enum.

(deftest create-type-then-table-uses-enum
  (with-server [s {:port (next-port)}]
    (is (nil? (:error (run-sql s "CREATE TYPE mood AS ENUM ('sad','ok','happy')"))))
    ;; registry holds the enum
    (let [enums (get @(:registry s) "__enums__")]
      (is (contains? enums "mood"))
      (is (= ["sad" "ok" "happy"] (-> enums (get "mood") :values-ordered)))
      (is (pos? (-> enums (get "mood") :oid))))
    ;; A column typed as the enum binds without error
    (is (nil? (:error (run-sql s "CREATE TABLE pet (name TEXT, m mood)"))))))

(deftest unknown-type-errors-with-clear-message
  (with-server [s {:port (next-port)}]
    (let [r (run-sql s "CREATE TABLE t (c not_a_real_type)")]
      (is (some? (:error r)))
      (is (re-find #"Unknown column type" (:error r))))))

;; ===========================================================================
;; INSERT validation rejects unknown labels.

(deftest insert-rejects-unknown-enum-label
  (with-server [s {:port (next-port)}]
    (run-sql s "CREATE TYPE mood AS ENUM ('sad','ok','happy')")
    (run-sql s "CREATE TABLE pet (name TEXT, m mood)")
    (is (nil? (:error (run-sql s "INSERT INTO pet VALUES ('rex','happy')"))))
    (let [r (run-sql s "INSERT INTO pet VALUES ('mio','grumpy')")]
      (is (some? (:error r)))
      (is (re-find #"invalid input value for enum mood" (:error r)))
      (is (re-find #"grumpy" (:error r))))))

(deftest insert-allows-null-into-enum-column
  ;; SQL allows NULL in enum columns unless NOT NULL is declared
  (with-server [s {:port (next-port)}]
    (run-sql s "CREATE TYPE mood AS ENUM ('sad','ok','happy')")
    (run-sql s "CREATE TABLE pet (name TEXT, m mood)")
    (is (nil? (:error (run-sql s "INSERT INTO pet (name, m) VALUES ('rex', NULL)"))))))

;; ===========================================================================
;; pg_type + pg_enum catalog introspection.

(deftest pg-type-includes-declared-enums
  (with-server [s {:port (next-port)}]
    (run-sql s "CREATE TYPE mood AS ENUM ('sad','ok','happy')")
    (let [r (run-sql s "SELECT oid, typname, typlen FROM pg_type")]
      (is (nil? (:error r)))
      (is (some (fn [[_ name _]] (= "mood" name)) (:rows r))
          "pg_type must surface declared enum"))))

(deftest pg-enum-rows-declaration-order
  (with-server [s {:port (next-port)}]
    (run-sql s "CREATE TYPE color AS ENUM ('red','green','blue')")
    (let [r (run-sql s "SELECT enumtypid, enumsortorder, enumlabel FROM pg_enum")
          labels (mapv #(nth % 2) (:rows r))]
      (is (nil? (:error r)))
      ;; Declaration position should be preserved
      (is (= ["red" "green" "blue"] labels)))))

;; ===========================================================================
;; Durability across restart.

(deftest enum-declarations-persist
  (let [path (temp-dir)]
    (try
      (let [store (file-store-at path)]
        (with-server [s {:port (next-port) :store store}]
          (run-sql s "CREATE TYPE tee_size AS ENUM ('S','M','L','XL')")
          (is (= ["S" "M" "L" "XL"]
                 (-> @(:registry s) (get "__enums__") (get "tee_size")
                     :values-ordered)))))
      ;; Reopen — enum and OID must hydrate
      (let [store (file-store-at path)]
        (with-server [s {:port (next-port) :store store}]
          (is (= ["S" "M" "L" "XL"]
                 (-> @(:registry s) (get "__enums__") (get "tee_size")
                     :values-ordered))
              "ENUM declaration must survive restart")
          ;; A new table can still bind to the enum
          (is (nil? (:error (run-sql s "CREATE TABLE shirt (sz tee_size)"))))))
      (finally (delete-dir path)))))

(deftest oid-is-stable-across-restart
  (let [path (temp-dir)
        first-oid (atom nil)]
    (try
      (let [store (file-store-at path)]
        (with-server [s {:port (next-port) :store store}]
          (run-sql s "CREATE TYPE x AS ENUM ('a','b')")
          (reset! first-oid
                  (-> @(:registry s) (get "__enums__") (get "x") :oid))))
      (let [store (file-store-at path)]
        (with-server [s {:port (next-port) :store store}]
          (is (= @first-oid
                 (-> @(:registry s) (get "__enums__") (get "x") :oid))
              "OID must round-trip unchanged across restart")))
      (finally (delete-dir path)))))
