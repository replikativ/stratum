(ns stratum.interval-column-test
  "Step 7a: INTERVAL as a fixed-size 16-byte primitive — Java value class,
   binary codec (OID 1186), and metadata-driven OID resolution.

   Stratum models INTERVAL after PG (interval_send at timestamp.c:997-1032)
   and DuckDB (interval_t at interval.hpp:25-29) — three components
   (months/days/micros) precisely because calendar arithmetic is ambiguous
   otherwise (variable-length months and days).

   This test covers the foundational layer:
     - Interval value class roundtrip
     - PgBinaryCodec encode/decode at OID 1186
     - Pinned wire bytes matching PG's interval_send byte-for-byte
     - PG-style toString rendering

   Out of scope for step 7a — deferred to step 7b:
     - INTERVAL columns registered via register-table! (needs encode-column
       branch + query-engine path for non-primitive arrays)
     - CREATE TABLE … INTERVAL DDL + INSERT VALUES parsing
     - INTERVAL arithmetic across columns (TIMESTAMP + INTERVAL_column)
     - Durable (Konserve-backed) INTERVAL storage"
  (:require [clojure.test :refer [deftest is testing]])
  (:import [stratum.internal Interval PgBinaryCodec PgWireServer PgWireServer$QueryResult]))

;; ---------------------------------------------------------------------------
;; Codec roundtrips

(deftest interval-binary-codec-roundtrip
  (doseq [iv [(Interval. 0 0 0)
              (Interval. 1000000 0 0)         ;; 1 second
              (Interval. 0 1 0)               ;; 1 day
              (Interval. 0 0 1)               ;; 1 month
              (Interval. 30000000 5 14)       ;; mixed
              (Interval. 3600000000 0 12)     ;; 1 year + 1 hour
              (Interval. -1000000 -1 -1)]]    ;; all-negative
    (let [b  (PgBinaryCodec/encodeInterval iv)
          rt (PgBinaryCodec/decodeInterval b)]
      (is (= 16 (alength ^bytes b)))
      (is (= iv rt)
          (str "INTERVAL roundtrip failed for " iv)))))

(deftest interval-binary-pinned-bytes
  ;; (months=1, days=2, micros=3000000)
  ;; Layout: int64 micros (8 bytes) + int32 days (4 bytes) + int32 months (4 bytes)
  ;; micros=3000000 = 0x002DC6C0
  ;; days=2 = 0x00000002, months=1 = 0x00000001
  (let [iv (Interval. 3000000 2 1)
        b  (PgBinaryCodec/encodeInterval iv)
        sb (StringBuilder.)]
    (doseq [x b] (.append sb (format "%02x" (bit-and (long x) 0xff))))
    (is (= "00000000002dc6c00000000200000001" (str sb))
        "INTERVAL binary layout must match PG interval_send byte-for-byte")))

(deftest interval-dispatcher-recognises-oid
  (is (PgBinaryCodec/supportsBinaryOutput PgWireServer/OID_INTERVAL))
  (is (PgBinaryCodec/supportsBinaryInput  PgWireServer/OID_INTERVAL))
  (let [iv (Interval. 1000000 1 1)
        b  (PgBinaryCodec/encode PgWireServer/OID_INTERVAL iv)
        rt (PgBinaryCodec/decode PgWireServer/OID_INTERVAL b)]
    (is (= iv rt))))

;; ---------------------------------------------------------------------------
;; toString — PG output format

(deftest interval-tostring
  (testing "zero interval renders as 00:00:00"
    (is (= "00:00:00" (.toString (Interval. 0 0 0)))))
  (testing "single-unit intervals"
    (is (= "1 mon"   (.toString (Interval. 0 0 1))))
    (is (= "2 mons"  (.toString (Interval. 0 0 2))))
    (is (= "1 day"   (.toString (Interval. 0 1 0))))
    (is (= "5 days"  (.toString (Interval. 0 5 0)))))
  (testing "year/month split"
    (is (= "1 year 2 mons" (.toString (Interval. 0 0 14))))
    (is (= "2 years"       (.toString (Interval. 0 0 24)))))
  (testing "time component"
    (is (= "01:00:00"        (.toString (Interval. 3600000000 0 0))))
    (is (= "01:00:00.500000" (.toString (Interval. 3600500000 0 0)))
        "fractional seconds render with 6-digit microseconds")
    (is (= "00:00:00.000001" (.toString (Interval. 1 0 0)))))
  (testing "compound: months + days + time"
    (is (= "1 mon 5 days 00:00:30"
           (.toString (Interval. 30000000 5 1))))))

;; ---------------------------------------------------------------------------
;; Value class semantics

(deftest interval-equals-and-hashcode
  (let [a (Interval. 3600000000 5 14)
        b (Interval. 3600000000 5 14)
        c (Interval. 3600000000 5 13)]
    (is (= a b))
    (is (not= a c))
    (is (= (.hashCode a) (.hashCode b)))))

(deftest interval-static-constructors
  (is (= (Interval. 0 7 0)        (Interval/ofDays 7)))
  (is (= (Interval. 0 0 12)       (Interval/ofMonths 12)))
  (is (= (Interval. 1000000 0 0)  (Interval/ofMicros 1000000)))
  (is (= (Interval. 0 0 0)        Interval/ZERO)))

;; ---------------------------------------------------------------------------
;; Column intake — encode-column accepts Interval arrays
;;
;; The query engine doesn't yet route through INTERVAL columns for SELECT
;; (its materialization pass casts arrays to double[] / long[]). What's
;; covered here is the *registration* path: register-table! must validate
;; the column shape against the Malli schema and encode-column must
;; preserve the array.

(require '[stratum.column :as column])

(deftest interval-encode-column-from-typed-array
  (let [arr (into-array Interval [(Interval. 0 1 0) (Interval. 3600000000 0 0)])
        enc (column/encode-column arr)]
    (is (= :interval (:type enc))
        "encode-column tags Interval[] as :type :interval")
    (is (identical? arr (:data enc))
        "underlying array passes through unchanged")))

(deftest interval-encode-column-from-object-array
  (let [arr (object-array [(Interval. 0 1 0)])
        enc (column/encode-column arr)]
    (is (= :interval (:type enc))
        "Object[] whose first element is an Interval is also accepted")))

;; ---------------------------------------------------------------------------
;; Step 7b — engine integration for SELECT col passthrough

(require '[stratum.server :as srv])

(deftest interval-select-passthrough-no-filter
  (testing "SELECT col FROM t (no WHERE) — engine returns INTERVAL column unchanged"
    (let [server (srv/start {:port 0})]
      (try
        (srv/register-table! server "durations"
                             {:idx (long-array [1 2 3 4])
                              :dur (into-array Interval
                                               [(Interval. 0 1 0)
                                                (Interval. 0 5 0)
                                                (Interval. 0 0 1)
                                                (Interval. 3600000000 0 0)])})
        (let [^PgWireServer$QueryResult qr
              (@(requiring-resolve 'stratum.server/execute-sql)
               "SELECT dur FROM durations"
               (:registry server) (:data-dir server) (:store server))]
          (is (nil? (.error qr))
              (str "INTERVAL SELECT errored: " (.error qr)))
          (is (= [PgWireServer/OID_INTERVAL] (vec (.columnOids qr)))
              "OID 1186 advertised for INTERVAL column")
          (let [rows (mapv vec (vec (.rows qr)))]
            (is (= 4 (count rows)))
            (is (some #{["1 day"]}    rows))
            (is (some #{["5 days"]}   rows))
            (is (some #{["1 mon"]}    rows))
            (is (some #{["01:00:00"]} rows))))
        (finally (srv/stop server))))))

(deftest interval-select-with-where-filter
  (testing "SELECT col FROM t WHERE other-col > N — engine scatter-gather over INTERVAL column"
    (let [server (srv/start {:port 0})]
      (try
        (srv/register-table! server "durations"
                             {:idx (long-array [1 2 3 4])
                              :dur (into-array Interval
                                               [(Interval. 0 1 0)
                                                (Interval. 0 5 0)
                                                (Interval. 0 0 1)
                                                (Interval. 3600000000 0 0)])})
        (let [^PgWireServer$QueryResult qr
              (@(requiring-resolve 'stratum.server/execute-sql)
               "SELECT dur FROM durations WHERE idx > 2"
               (:registry server) (:data-dir server) (:store server))]
          (is (nil? (.error qr))
              (str "WHERE over INTERVAL columns errored: " (.error qr)))
          (let [rows (mapv vec (vec (.rows qr)))]
            (is (= 2 (count rows))
                "idx > 2 selects rows 3 and 4")
            ;; idx=3 → 1 mon, idx=4 → 1h
            (is (some #{["1 mon"]}    rows))
            (is (some #{["01:00:00"]} rows))))
        (finally (srv/stop server))))))
