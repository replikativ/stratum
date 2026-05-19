(ns stratum.pgjdbc-test
  "Step W4: end-to-end validation that Stratum's pgwire protocol (text +
   binary) works against the canonical PostgreSQL JDBC client.

   These tests connect via pgjdbc to a real Stratum server bound to an
   ephemeral port. pgjdbc's `binaryTransfer*` connection properties let
   us force binary mode for specific OIDs, exercising both the encoder
   and decoder sides of PgBinaryCodec through realistic client code."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [stratum.server :as srv]
            [stratum.sql :as sql])
  (:import [java.sql Connection DriverManager PreparedStatement ResultSet Types]
           [java.math BigDecimal]
           [java.util Properties]
           [stratum.internal PgWireServer]))

;; ---------------------------------------------------------------------------
;; Fixture — Stratum server with an analytic table on an ephemeral port.

(def ^:private ^:dynamic *srv* nil)
(def ^:private ^:dynamic *port* nil)

(defn- server-fixture [f]
  (let [srv (srv/start {:port 0})]
    (srv/register-table!
     srv "orders"
     {:price    (double-array [10.0 50.0 100.0 200.0 500.0])
      :quantity (long-array   [1    2    5     10    20])
      :region   (into-array String ["N" "N" "S" "S" "E"])})
    (binding [*srv* srv
              *port* (.getPort ^PgWireServer (:server srv))]
      (try (f) (finally (srv/stop srv))))))

(use-fixtures :each server-fixture)

;; ---------------------------------------------------------------------------
;; Connection helpers

(defn- ^Connection connect
  "Open a pgjdbc connection to the running Stratum server. The optional
   `props` map overrides connection properties — most usefully
   `binaryTransferEnable` (comma-separated OIDs to force binary)."
  ([] (connect {}))
  ([props]
   (let [p (Properties.)]
     (.setProperty p "user" "stratum")
     (.setProperty p "preferQueryMode" "extended")
     ;; Force server-side prepared statements after the first execution so
     ;; binary transfers can engage on the second call. Setting it to 0
     ;; means "always use prepared statements".
     (.setProperty p "prepareThreshold" "0")
     (doseq [[k v] props] (.setProperty p (name k) (str v)))
     (DriverManager/getConnection
      (str "jdbc:postgresql://127.0.0.1:" *port* "/stratum") p))))

(defmacro with-conn [[sym & [props]] & body]
  `(with-open [~sym (connect ~(or props {}))]
     ~@body))

;; ---------------------------------------------------------------------------
;; Sanity — text mode (the default for most types) still works.

(deftest pgjdbc-text-aggregate-test
  (testing "Simple aggregate over the orders table via pgjdbc, text mode"
    (with-conn [c]
      (with-open [st (.createStatement c)
                  rs (.executeQuery st "SELECT SUM(price) AS total FROM orders")]
        (is (.next rs))
        (is (= 860.0 (.getDouble rs "total")))))))

(deftest pgjdbc-text-prepared-test
  (testing "Prepared statement with text params returns matching count"
    (with-conn [c]
      (with-open [ps (.prepareStatement c "SELECT COUNT(*) FROM orders WHERE price > ?")]
        (.setDouble ps 1 100.0)
        (with-open [rs (.executeQuery ps)]
          (is (.next rs))
          (is (= 2 (.getInt rs 1))))))))

;; ---------------------------------------------------------------------------
;; Binary mode — explicitly enable for FLOAT8 (701) and INT8 (20).
;;
;; pgjdbc's default binary-transfer OID list varies across driver versions.
;; Setting `binaryTransferEnable=20,701` makes the test self-contained.

(def ^:private BINARY-CORE
  "20,21,23,700,701,1043,25,1082,1114,1184,1700,2950,3802")

(deftest pgjdbc-binary-int8-test
  (testing "INT8 column returned as 8 binary bytes is decoded correctly by pgjdbc"
    (with-conn [c {:binaryTransferEnable BINARY-CORE}]
      (with-open [ps (.prepareStatement c "SELECT quantity FROM orders WHERE price > ? ORDER BY quantity")]
        (.setDouble ps 1 50.0)
        (with-open [rs (.executeQuery ps)]
          (let [vals (loop [acc []]
                       (if (.next rs) (recur (conj acc (.getLong rs 1))) acc))]
            (is (= [5 10 20] vals))))))))

(deftest pgjdbc-binary-float8-test
  (testing "FLOAT8 column returned in binary IEEE 754 BE form"
    (with-conn [c {:binaryTransferEnable BINARY-CORE}]
      (with-open [ps (.prepareStatement c "SELECT price FROM orders WHERE quantity > ? ORDER BY price")]
        (.setLong ps 1 1)
        (with-open [rs (.executeQuery ps)]
          (let [vals (loop [acc []]
                       (if (.next rs) (recur (conj acc (.getDouble rs 1))) acc))]
            (is (= [50.0 100.0 200.0 500.0] vals))))))))

(deftest pgjdbc-binary-text-test
  (testing "TEXT column returned via binary path roundtrips through UTF-8"
    (with-conn [c {:binaryTransferEnable BINARY-CORE}]
      (with-open [ps (.prepareStatement c "SELECT region FROM orders WHERE quantity > ? ORDER BY price")]
        (.setLong ps 1 2)
        (with-open [rs (.executeQuery ps)]
          (let [vals (loop [acc []]
                       (if (.next rs) (recur (conj acc (.getString rs 1))) acc))]
            (is (= ["S" "S" "E"] vals))))))))

(deftest pgjdbc-binary-int8-param-test
  (testing "Binary INT8 param sent through pgjdbc setLong roundtrips"
    (with-conn [c {:binaryTransferEnable BINARY-CORE}]
      ;; Heat the cache so pgjdbc switches to binary param transfer.
      (with-open [ps (.prepareStatement c "SELECT COUNT(*) FROM orders WHERE quantity > ?")]
        (.setLong ps 1 1)
        (with-open [rs (.executeQuery ps)] (.next rs))
        (.setLong ps 1 5)
        (with-open [rs (.executeQuery ps)]
          (is (.next rs))
          (is (= 2 (.getInt rs 1))
              "binary INT8 param 5 should match quantities {10, 20}"))))))

(deftest pgjdbc-binary-float8-param-test
  (testing "Binary FLOAT8 param via setDouble"
    (with-conn [c {:binaryTransferEnable BINARY-CORE}]
      (with-open [ps (.prepareStatement c "SELECT COUNT(*) FROM orders WHERE price > ?")]
        (.setDouble ps 1 50.0)
        (with-open [rs (.executeQuery ps)] (.next rs))
        (.setDouble ps 1 100.0)
        (with-open [rs (.executeQuery ps)]
          (is (.next rs))
          (is (= 2 (.getInt rs 1))))))))

(deftest pgjdbc-binary-string-param-test
  (testing "Binary TEXT param via setString roundtrips"
    (with-conn [c {:binaryTransferEnable BINARY-CORE}]
      (with-open [ps (.prepareStatement c "SELECT SUM(price) FROM orders WHERE region = ?")]
        (.setString ps 1 "N")
        (with-open [rs (.executeQuery ps)] (.next rs))
        (.setString ps 1 "S")
        (with-open [rs (.executeQuery ps)]
          (is (.next rs))
          (is (= 300.0 (.getDouble rs 1))
              "region = S → prices 100 + 200"))))))
