(ns stratum.no-sentinel-null-test
  "Sentinel-free NULL handling infrastructure for long-backed columns.

   Stratum's default convention: `Long.MIN_VALUE` in a long[] column
   marks NULL. This works for the common case but collides when a column
   wants to STORE `Long.MIN_VALUE` as a genuine value (e.g., a UBIGINT
   column holding 2^63 — `Long.MIN_VALUE`'s bit pattern).

   The opt-out: callers pass `{:no-sentinel-null? true}` to
   `encode-column` (2-arity). Effects:
     - The Long.MIN_VALUE scan is skipped.
     - The flag is recorded in the column map.
     - `format-results` consults the flag when rendering and does NOT
       map `Long.MIN_VALUE` to NULL — every bit pattern is a valid value.

   What this test pins:
     - register-table! + the flag → 2^63 round-trips correctly via
       Long.toUnsignedString.
     - The flag is independent of `:unsigned-width` — any caller can opt
       in, including future int128 storage layers.

   What this test does NOT cover (future work, deliberately deferred):
     - SQL CREATE TABLE / INSERT path for UBIGINT does NOT yet pass the
       flag, so the documented 2^63 ↔ NULL collision in UBIGINT INSERTs
       persists. The fix requires the INSERT path (server.clj lines
       993/1065/1127/1198) to build an explicit validity bitmap when
       writing nil → long[], rather than mapping nil → Long.MIN_VALUE."
  (:require [clojure.test :refer [deftest is testing]]
            [stratum.column :as column]
            [stratum.server :as srv])
  (:import [stratum.internal PgWireServer$QueryResult]))

(defmacro ^:private with-server [[binding-name opts] & body]
  `(let [~binding-name (srv/start ~opts)]
     (try ~@body (finally (srv/stop ~binding-name)))))

(deftest encode-column-no-sentinel-null-skips-scan
  (testing "Pass :no-sentinel-null? to encode-column 2-arity → no validity bitmap is generated even with Long.MIN_VALUE in the data"
    (let [arr (long-array [1 Long/MIN_VALUE 2])
          enc (column/encode-column arr {:no-sentinel-null? true})]
      (is (= :int64 (:type enc)))
      (is (identical? arr (:data enc)))
      (is (true? (:no-sentinel-null? enc)))
      (is (nil? (:validity enc))
          "no validity bitmap is built — caller would supply it if needed"))))

(deftest encode-column-default-still-scans
  (testing "Without the flag, Long.MIN_VALUE is treated as NULL (existing behavior)"
    (let [arr (long-array [1 Long/MIN_VALUE 2])
          enc (column/encode-column arr)]
      (is (= :int64 (:type enc)))
      (is (some? (:validity enc))
          "validity bitmap built because the scan found a sentinel"))))

(deftest format-results-honors-no-sentinel-null-flag
  (testing "When a column's meta has :no-sentinel-null?, format-results renders Long.MIN_VALUE as a value, not NULL"
    (with-server [s {:port 0}]
      ;; Pre-normalize the column so the flag rides directly into the
      ;; registry. The :column-schema metadata tells format-results
      ;; about the flag; :unsigned-width=64 + value->string render
      ;; the value via Long.toUnsignedString.
      (let [arr (long-array [1 Long/MIN_VALUE 2])
            normalized (column/encode-column arr {:no-sentinel-null? true})]
        (srv/register-table! s "t"
                              (with-meta {:id normalized}
                                {:column-schema {:id {:no-sentinel-null? true
                                                      :unsigned-width 64}}})))
      (let [^PgWireServer$QueryResult qr
            (@(requiring-resolve 'stratum.server/execute-sql)
             "SELECT id FROM t" (:registry s) (:data-dir s) (:store s))
            rows (mapv vec (vec (.rows qr)))]
        (is (nil? (.error qr)) (str "errored: " (.error qr)))
        (is (= [["1"]
                ["9223372036854775808"]   ;; Long.MIN_VALUE bit pattern as unsigned
                ["2"]]
               rows)
            "Long.MIN_VALUE in a :no-sentinel-null? column renders as 2^63, not NULL")))))

(deftest format-results-default-still-maps-sentinel-to-null
  (testing "Without the flag, Long.MIN_VALUE still renders as NULL (no regression)"
    (with-server [s {:port 0}]
      (srv/register-table! s "t" {:id (long-array [1 Long/MIN_VALUE 2])})
      (let [^PgWireServer$QueryResult qr
            (@(requiring-resolve 'stratum.server/execute-sql)
             "SELECT id FROM t" (:registry s) (:data-dir s) (:store s))
            rows (mapv vec (vec (.rows qr)))]
        (is (= [["1"] [nil] ["2"]] rows)
            "Long.MIN_VALUE still surfaces as NULL on default long[] columns")))))
