(ns stratum.pgwire-test
  "PostgreSQL wire protocol integration tests.

   Tests the extended query protocol (Parse/Bind/Describe/Execute/Sync) and
   transaction state signals via raw TCP sockets against a live server.
   This exercises the full stack: wire layer → execute-sql → query engine."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [stratum.server])
  (:import [stratum.internal PgWireServer]
           [java.io DataInputStream DataOutputStream
            BufferedInputStream BufferedOutputStream
            ByteArrayOutputStream]
           [java.net Socket]
           [java.nio ByteBuffer]
           [java.nio.charset StandardCharsets]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Fixture — server with a small analytic table
;; ============================================================================

(def ^:private ^:dynamic *srv* nil)

(defn- server-fixture [f]
  (let [srv (stratum.server/start {:port 0})]
    (stratum.server/register-table!
     srv "orders"
     {:price    (double-array [10.0 50.0 100.0 200.0 500.0])
      :quantity (long-array   [1    2    5     10    20])
      :region   (into-array String ["N" "N" "S" "S" "E"])})
    (binding [*srv* srv]
      (try (f) (finally (stratum.server/stop srv))))))

(use-fixtures :each server-fixture)

(defn- srv-port []
  (.getPort ^PgWireServer (:server *srv*)))

;; ============================================================================
;; Minimal PG wire client helpers
;; ============================================================================

(defn- pg-connect!
  "Open a TCP connection and perform the PG v3 startup sequence.
   Returns {:sock :in :out} ready to send/receive messages."
  [port]
  (let [sock (Socket. "127.0.0.1" (int port))
        in   (DataInputStream.  (BufferedInputStream.  (.getInputStream  sock)))
        out  (DataOutputStream. (BufferedOutputStream. (.getOutputStream sock)))
        ;; StartupMessage: length(4) + protocol-version(4) + params
        params   (str "user\u0000stratum\u0000database\u0000stratum\u0000\u0000")
        pb       (.getBytes params StandardCharsets/UTF_8)]
    (.writeInt out (+ 8 (count pb)))
    (.writeInt out 196608)  ;; v3.0
    (.write    out pb)
    (.flush    out)
    ;; Drain startup response messages until ReadyForQuery ('Z')
    (loop []
      (let [t  (int (.read in))
            ml (.readInt in)
            b  (byte-array (max 0 (- ml 4)))]
        (.readFully in b)
        (when (not= t (int \Z)) (recur))))
    {:sock sock :in in :out out}))

(defn- read-msg
  "Read one PG backend message. Returns {:type char :body byte[]}."
  [{:keys [^DataInputStream in]}]
  (let [t  (char (.read in))
        ml (.readInt in)
        b  (byte-array (max 0 (- ml 4)))]
    (.readFully in b)
    {:type t :body b}))

(defn- drain-rfq
  "Read messages until ReadyForQuery. Returns {:messages [...] :tx-status \\I/T/E}."
  [conn]
  (loop [msgs []]
    (let [{:keys [type] :as m} (read-msg conn)]
      (if (= type \Z)
        {:messages msgs
         :tx-status (char (aget ^bytes (:body m) 0))}
        (recur (conj msgs m))))))

(defn- decode-row-description
  "Parse a RowDescription ('T') body into [{:name :oid} ...]."
  [^bytes body]
  (let [buf (ByteBuffer/wrap body)
        n   (.getShort buf)]
    (mapv (fn [_]
            (let [sb (StringBuilder.)]
              (loop []
                (let [b (.get buf)]
                  (when-not (zero? b)
                    (.append sb (char (bit-and (int b) 0xFF)))
                    (recur))))
              (let [_   (.getInt   buf)   ;; table OID (4)
                    _   (.getShort buf)   ;; attr number (2)
                    oid (.getInt   buf)   ;; type OID (4)
                    _   (.getShort buf)   ;; type size (2)
                    _   (.getInt   buf)   ;; type modifier (4)
                    _   (.getShort buf)]  ;; format code (2)
                {:name (str sb) :oid oid})))
          (range n))))

(defn- decode-data-rows
  "Extract DataRow ('D') messages from a drain-rfq result into seq of string-value vectors."
  [result]
  (for [{:keys [type body]} (:messages result)
        :when (= type \D)]
    (let [buf (ByteBuffer/wrap body)
          n   (.getShort buf)]
      (mapv (fn [_]
              (let [len (.getInt buf)]
                (when-not (neg? len)
                  (let [b (byte-array len)]
                    (.get buf b)
                    (String. b StandardCharsets/UTF_8)))))
            (range n)))))

;; ============================================================================
;; Message senders
;; ============================================================================

(defn- send-query!
  "Simple Query ('Q') protocol."
  [{:keys [^DataOutputStream out]} ^String sql]
  (let [b (.getBytes sql StandardCharsets/UTF_8)]
    (.writeByte out (int \Q))
    (.writeInt  out (+ 4 (count b) 1))
    (.write     out b)
    (.writeByte out 0)
    (.flush     out)))

(defn- send-parse!
  "Parse ('P') message using the unnamed statement."
  [{:keys [^DataOutputStream out]} ^String sql]
  (let [sql-b (.getBytes sql StandardCharsets/UTF_8)
        baos  (ByteArrayOutputStream.)
        dos   (DataOutputStream. baos)]
    (.writeByte dos 0)             ;; unnamed statement
    (.write     dos sql-b)
    (.writeByte dos 0)             ;; null terminator
    (.writeShort dos 0)            ;; 0 param type hints
    (.flush dos)
    (let [body (.toByteArray baos)]
      (.writeByte out (int \P))
      (.writeInt  out (+ 4 (count body)))
      (.write     out body)
      (.flush     out))))

(defn- send-bind!
  "Bind ('B') message. Sends params as text strings to the unnamed portal."
  [{:keys [^DataOutputStream out]} params]
  (let [baos (ByteArrayOutputStream.)
        dos  (DataOutputStream. baos)]
    (.writeByte dos 0)                    ;; unnamed portal
    (.writeByte dos 0)                    ;; unnamed statement
    (.writeShort dos 0)                   ;; no format codes (all text)
    (.writeShort dos (count params))
    (doseq [p params]
      (if (nil? p)
        (.writeInt dos -1)
        (let [b (.getBytes (str p) StandardCharsets/UTF_8)]
          (.writeInt dos (count b))
          (.write dos b))))
    (.writeShort dos 0)                   ;; no result format codes
    (.flush dos)
    (let [body (.toByteArray baos)]
      (.writeByte out (int \B))
      (.writeInt  out (+ 4 (count body)))
      (.write     out body)
      (.flush     out))))

(defn- send-describe-portal!
  "Describe ('D') the unnamed portal — returns RowDescription."
  [{:keys [^DataOutputStream out]}]
  (.writeByte out (int \D))
  (.writeInt  out 6)
  (.writeByte out (int \P))  ;; 'P' = portal
  (.writeByte out 0)         ;; unnamed portal
  (.flush     out))

(defn- send-describe-stmt!
  "Describe ('D') the unnamed prepared statement — returns ParameterDescription."
  [{:keys [^DataOutputStream out]}]
  (.writeByte out (int \D))
  (.writeInt  out 6)
  (.writeByte out (int \S))  ;; 'S' = statement
  (.writeByte out 0)         ;; unnamed statement
  (.flush     out))

(defn- send-execute!
  "Execute ('E') the unnamed portal (unlimited rows)."
  [{:keys [^DataOutputStream out]}]
  (.writeByte out (int \E))
  (.writeInt  out 9)
  (.writeByte out 0)   ;; unnamed portal
  (.writeInt  out 0)   ;; max rows = 0 (unlimited)
  (.flush     out))

(defn- send-sync!
  "Sync ('S') — ends the extended query pipeline, triggers ReadyForQuery."
  [{:keys [^DataOutputStream out]}]
  (.writeByte out (int \S))
  (.writeInt  out 4)
  (.flush     out))

(defmacro with-conn [[sym port] & body]
  `(let [~sym (pg-connect! ~port)]
     (try ~@body (finally (.close ^Socket (:sock ~sym))))))

;; ============================================================================
;; Tests
;; ============================================================================

(deftest simple-query-test
  (testing "Simple Query protocol returns correct aggregation result"
    (with-conn [c (srv-port)]
      (send-query! c "SELECT SUM(price) AS total FROM orders")
      (let [result (drain-rfq c)
            rows   (decode-data-rows result)
            rd-msgs (filter #(= \T (:type %)) (:messages result))
            col-names (when (seq rd-msgs)
                        (mapv :name (decode-row-description (:body (first rd-msgs)))))]
        (is (= ["total"] col-names) "column name should be 'total'")
        (is (= 1 (count rows)) (str "expected 1 row, got " (count rows) " cols=" col-names " rows=" (vec rows)))
        (is (= "860.0" (ffirst rows)))))))

(deftest simple-query-count-test
  (testing "Simple Query COUNT(*)"
    (with-conn [c (srv-port)]
      (send-query! c "SELECT COUNT(*) FROM orders")
      (let [rows (decode-data-rows (drain-rfq c))]
        (is (= "5" (ffirst rows)))))))

(deftest extended-basic-test
  (testing "Parse→Bind→Execute→Sync without params"
    (with-conn [c (srv-port)]
      (send-parse!   c "SELECT COUNT(*) FROM orders")
      (send-bind!    c [])
      (send-execute! c)
      (send-sync!    c)
      (let [result (drain-rfq c)
            rows   (decode-data-rows result)]
        (is (= 1 (count rows)))
        (is (= "5" (ffirst rows)))))))

(deftest extended-single-param-test
  (testing "Parse→Bind→Execute with one numeric parameter"
    (with-conn [c (srv-port)]
      ;; price > 100.0: rows [200.0, 500.0] → SUM = 700.0
      (send-parse!   c "SELECT SUM(price) AS total FROM orders WHERE price > $1")
      (send-bind!    c ["100.0"])
      (send-execute! c)
      (send-sync!    c)
      (let [rows (decode-data-rows (drain-rfq c))]
        (is (= 1 (count rows)))
        (is (= "700.0" (ffirst rows)))))))

(deftest extended-multi-param-test
  (testing "Parse→Bind→Execute with two numeric parameters"
    (with-conn [c (srv-port)]
      ;; price >= 50.0 AND price <= 200.0: rows [50.0, 100.0, 200.0] → COUNT = 3
      (send-parse!   c "SELECT COUNT(*) FROM orders WHERE price >= $1 AND price <= $2")
      (send-bind!    c ["50.0" "200.0"])
      (send-execute! c)
      (send-sync!    c)
      (let [rows (decode-data-rows (drain-rfq c))]
        (is (= 1 (count rows)))
        (is (= "3" (ffirst rows)))))))

(deftest extended-string-param-test
  (testing "Parse→Bind→Execute with a string parameter"
    (with-conn [c (srv-port)]
      ;; region = 'N': rows [10.0, 50.0] → SUM = 60.0
      (send-parse!   c "SELECT SUM(price) AS total FROM orders WHERE region = $1")
      (send-bind!    c ["N"])
      (send-execute! c)
      (send-sync!    c)
      (let [rows (decode-data-rows (drain-rfq c))]
        (is (= 1 (count rows)))
        (is (= "60.0" (ffirst rows)))))))

(deftest extended-null-param-test
  (testing "NULL parameter substitution keeps the connection alive"
    (with-conn [c (srv-port)]
      ;; Send a query with a NULL param — the SQL semantics may produce an error
      ;; (e.g. 'price > NULL' is not valid in all paths), but the connection must
      ;; remain usable afterward (Sync always produces ReadyForQuery).
      (send-parse!   c "SELECT COUNT(*) AS cnt FROM orders WHERE price > $1")
      (send-bind!    c [nil])
      (send-execute! c)
      (send-sync!    c)
      ;; drain-rfq must complete without hanging (Sync always triggers ReadyForQuery)
      (let [result (drain-rfq c)]
        (is (contains? #{\I \T \E} (:tx-status result))
            "connection must respond with a valid ReadyForQuery status"))
      ;; Connection must still be usable after the null-param query
      (send-query! c "SELECT COUNT(*) FROM orders")
      (let [rows (decode-data-rows (drain-rfq c))]
        (is (= "5" (ffirst rows)) "connection remains functional after null param")))))

(deftest describe-portal-returns-row-description-test
  (testing "Describe portal sends RowDescription with correct column names and types"
    (with-conn [c (srv-port)]
      ;; Send the full pipeline: Parse→Bind→Describe→Execute→Sync
      (send-parse!            c "SELECT price, quantity FROM orders WHERE price > $1")
      (send-bind!             c ["50.0"])
      (send-describe-portal!  c)
      (send-execute!          c)
      (send-sync!             c)
      (let [result (drain-rfq c)
            types  (mapv :type (:messages result))]
        ;; ParseComplete(1), BindComplete(2), RowDescription(T), DataRows(D), CommandComplete(C)
        (is (some #{\T} types) "RowDescription should be present")
        ;; RowDescription sent by Describe; Execute must NOT resend it
        (is (= 1 (count (filter #{\T} types))) "RowDescription must appear exactly once")
        ;; Verify column names from the RowDescription
        (let [rd-body (:body (first (filter #(= \T (:type %)) (:messages result))))
              cols    (decode-row-description rd-body)]
          (is (= 2 (count cols)))
          (is (= "price"    (:name (first cols))))
          (is (= "quantity" (:name (second cols)))))
        ;; Rows with price > 50: [100.0/5, 200.0/10, 500.0/20]
        (is (= 3 (count (decode-data-rows result))))))))

(deftest describe-stmt-parameter-description-test
  (testing "Describe statement sends ParameterDescription ('t') for $N params"
    (with-conn [c (srv-port)]
      ;; Describe stmt without Bind: server sends ParameterDescription
      ;; and then NoData (can't determine output types without bound params)
      (send-parse!         c "SELECT SUM(price) FROM orders WHERE price > $1")
      (send-describe-stmt! c)
      (send-sync!          c)
      (let [result (drain-rfq c)
            types  (mapv :type (:messages result))]
        ;; Must include ParameterDescription
        (is (some #{\t} types) "ParameterDescription must be sent")
        ;; NoData is acceptable when params are unbound at Describe time
        ;; NoData ('n') or RowDescription ('T') expected after ParameterDescription
        (is (or (some #{(char 110)} types)    ;; 'n' = NoData
                (some #{\T} types))
            "NoData or RowDescription expected")))))

(deftest transaction-status-begin-commit-test
  (testing "BEGIN sets txStatus to T, COMMIT resets to I"
    (with-conn [c (srv-port)]
      ;; Initial state: idle
      (send-query! c "SELECT 1")
      (is (= \I (:tx-status (drain-rfq c))) "initial state should be idle")

      ;; BEGIN → in-transaction
      (send-query! c "BEGIN")
      (is (= \T (:tx-status (drain-rfq c))) "after BEGIN should be in-transaction")

      ;; Query during transaction → still T
      (send-query! c "SELECT COUNT(*) FROM orders")
      (is (= \T (:tx-status (drain-rfq c))) "queries in tx should propagate T status")

      ;; COMMIT → back to idle
      (send-query! c "COMMIT")
      (is (= \I (:tx-status (drain-rfq c))) "after COMMIT should return to idle"))))

(deftest transaction-abort-state-test
  (testing "Error in transaction sets txStatus to E, commands rejected until ROLLBACK"
    (with-conn [c (srv-port)]
      (send-query! c "BEGIN")
      (drain-rfq c)

      ;; Trigger an error (bad SQL)
      (send-query! c "SELECT * FROM nonexistent_table_xyz")
      (let [result (drain-rfq c)]
        (is (= \E (:tx-status result)) "error in tx should set status E"))

      ;; Normal command → rejected with error message
      (send-query! c "SELECT COUNT(*) FROM orders")
      (let [result (drain-rfq c)]
        (is (some #(= \E (:type %)) (:messages result)) "command after error should be rejected")
        (is (= \E (:tx-status result)) "txStatus should remain E"))

      ;; ROLLBACK → clears the error state
      (send-query! c "ROLLBACK")
      (is (= \I (:tx-status (drain-rfq c))) "ROLLBACK should reset to idle")

      ;; Now queries work again
      (send-query! c "SELECT COUNT(*) FROM orders")
      (let [result (drain-rfq c)]
        (is (= \I (:tx-status result)))
        (is (= "5" (ffirst (decode-data-rows result))))))))

(deftest statement-splitter-comment-test
  (testing "-- comments are stripped (psql sends these before statements)"
    (with-conn [c (srv-port)]
      (send-query! c "-- get total count\nSELECT COUNT(*) FROM orders")
      (let [rows (decode-data-rows (drain-rfq c))]
        (is (= "5" (ffirst rows)))))))

(deftest extended-group-by-test
  (testing "Parse→Bind→Execute with GROUP BY analytic query"
    (with-conn [c (srv-port)]
      ;; price > 50: [100/S, 200/S, 500/E] → E: SUM(qty)=20, S: SUM(qty)=15
      (send-parse!   c "SELECT region, SUM(quantity) FROM orders WHERE price > $1 GROUP BY region ORDER BY region")
      (send-bind!    c ["50.0"])
      (send-execute! c)
      (send-sync!    c)
      (let [rows (vec (decode-data-rows (drain-rfq c)))]
        (is (= 2 (count rows)))
        (is (= "E" (first (first rows))))
        (is (= "S" (first (second rows))))
        ;; Verify SUM values (last column in each row)
        (is (= "20" (last (first rows))))
        (is (= "15" (last (second rows))))))))

;; ============================================================================
;; Step W1 — format-code plumbing
;; ============================================================================

(defn- send-bind-fmt!
  "Bind with explicit per-param and per-result format codes (each a vector
   of int16 codes, 0=text, 1=binary)."
  [{:keys [^DataOutputStream out]} param-fmts params result-fmts]
  (let [baos (ByteArrayOutputStream.)
        dos  (DataOutputStream. baos)]
    (.writeByte dos 0)                     ;; unnamed portal
    (.writeByte dos 0)                     ;; unnamed statement
    (.writeShort dos (count param-fmts))
    (doseq [f param-fmts] (.writeShort dos (int f)))
    (.writeShort dos (count params))
    (doseq [p params]
      (if (nil? p)
        (.writeInt dos -1)
        (let [b (.getBytes (str p) StandardCharsets/UTF_8)]
          (.writeInt dos (count b))
          (.write dos b))))
    (.writeShort dos (count result-fmts))
    (doseq [f result-fmts] (.writeShort dos (int f)))
    (.flush dos)
    (let [body (.toByteArray baos)]
      (.writeByte out (int \B))
      (.writeInt  out (+ 4 (count body)))
      (.write     out body)
      (.flush     out))))

(defn- send-parse-typed!
  "Parse with explicit param-type OIDs (a vector of int32 OIDs)."
  [{:keys [^DataOutputStream out]} ^String sql param-oids]
  (let [sql-b (.getBytes sql StandardCharsets/UTF_8)
        baos  (ByteArrayOutputStream.)
        dos   (DataOutputStream. baos)]
    (.writeByte dos 0)                  ;; unnamed statement
    (.write     dos sql-b)
    (.writeByte dos 0)
    (.writeShort dos (count param-oids))
    (doseq [oid param-oids] (.writeInt dos (int oid)))
    (.flush dos)
    (let [body (.toByteArray baos)]
      (.writeByte out (int \P))
      (.writeInt  out (+ 4 (count body)))
      (.write     out body)
      (.flush     out))))

(defn- send-bind-binary!
  "Bind that sends each param as raw binary bytes (param-fmts = [1 1 ...]).
   Each param in `bin-params` is a byte[] (or nil for SQL NULL)."
  [{:keys [^DataOutputStream out]} bin-params result-fmts]
  (let [baos (ByteArrayOutputStream.)
        dos  (DataOutputStream. baos)]
    (.writeByte dos 0)                     ;; unnamed portal
    (.writeByte dos 0)                     ;; unnamed statement
    (.writeShort dos (count bin-params))
    (doseq [_ bin-params] (.writeShort dos 1))  ;; all binary
    (.writeShort dos (count bin-params))
    (doseq [p bin-params]
      (if (nil? p)
        (.writeInt dos -1)
        (let [^bytes b p]
          (.writeInt dos (alength b))
          (.write dos b))))
    (.writeShort dos (count result-fmts))
    (doseq [f result-fmts] (.writeShort dos (int f)))
    (.flush dos)
    (let [body (.toByteArray baos)]
      (.writeByte out (int \B))
      (.writeInt  out (+ 4 (count body)))
      (.write     out body)
      (.flush     out))))

(defn- decode-row-description-fmts
  "Parse a RowDescription body into a vector of [name oid format] tuples."
  [^bytes body]
  (let [buf (ByteBuffer/wrap body)
        n   (.getShort buf)]
    (mapv (fn [_]
            (let [sb (StringBuilder.)]
              (loop []
                (let [b (.get buf)]
                  (when-not (zero? b)
                    (.append sb (char (bit-and (int b) 0xFF)))
                    (recur))))
              (let [_   (.getInt   buf)   ;; table OID
                    _   (.getShort buf)   ;; attr number
                    oid (.getInt   buf)   ;; type OID
                    _   (.getShort buf)   ;; type size
                    _   (.getInt   buf)   ;; type modifier
                    fmt (.getShort buf)]
                [(str sb) oid fmt])))
          (range n))))

(defn- find-error
  "Return {:code :message} from the first ErrorResponse ('E') in a drain
   result, or nil if none."
  [drained]
  (when-let [e (some #(when (= \E (:type %)) %) (:messages drained))]
    (let [buf (ByteBuffer/wrap ^bytes (:body e))
          fields (loop [acc {}]
                   (let [b (.get buf)]
                     (if (zero? b)
                       acc
                       (let [sb (StringBuilder.)]
                         (loop []
                           (let [c (.get buf)]
                             (when-not (zero? c)
                               (.append sb (char (bit-and (int c) 0xFF)))
                               (recur))))
                         (recur (assoc acc (char b) (str sb)))))))]
      {:code (get fields \C) :message (get fields \M)})))

(deftest w1-result-format-text-broadcast-test
  (testing "Bind with single result-format code = 0 broadcasts text to all columns"
    (with-conn [c (srv-port)]
      (send-parse!   c "SELECT price, quantity FROM orders WHERE price > $1")
      ;; 1 param format code (text) + 1 result format code (text, broadcast)
      (send-bind-fmt! c [0] ["50.0"] [0])
      (send-describe-portal! c)
      (send-execute! c)
      (send-sync!    c)
      (let [result (drain-rfq c)
            rd-body (:body (first (filter #(= \T (:type %)) (:messages result))))
            cols    (decode-row-description-fmts rd-body)
            rows    (decode-data-rows result)]
        (is (= 2 (count cols)))
        (is (every? #(zero? (nth % 2)) cols)
            "every column's format code must be 0 (text)")
        (is (= 3 (count rows))
            "rows still arrive correctly when format codes are explicit text")))))

(deftest w3-param-format-binary-without-oid-rejected-test
  (testing "Bind with binary input format but no declared type OID at Parse → ErrorResponse 0A000"
    (with-conn [c (srv-port)]
      ;; send-parse! declares 0 param type OIDs — binary input requires a type
      (send-parse!   c "SELECT COUNT(*) FROM orders WHERE price > $1")
      (send-bind-fmt! c [1] ["100.0"] [])
      (send-sync!    c)
      (let [result (drain-rfq c)
            err    (find-error result)]
        (is (some? err) "ErrorResponse expected for binary input without OID")
        (is (= "0A000" (:code err)))
        (is (re-find #"requires a type OID" (:message err))
            "error must mention the missing type OID"))
      ;; Connection must remain usable
      (send-query! c "SELECT COUNT(*) FROM orders")
      (let [rows (decode-data-rows (drain-rfq c))]
        (is (= "5" (ffirst rows)) "connection remains functional after binary-param error")))))

(deftest w1-unknown-result-format-code-rejected-test
  (testing "Unknown wire format code (e.g., 2) is refused with 0A000"
    (with-conn [c (srv-port)]
      (send-parse!   c "SELECT price FROM orders WHERE price > $1")
      ;; Format code 2 is not defined in the PG protocol (only 0=text, 1=binary)
      (send-bind-fmt! c [0] ["50.0"] [2])
      (send-execute! c)
      (send-sync!    c)
      (let [result (drain-rfq c)
            err    (find-error result)]
        (is (some? err))
        (is (= "0A000" (:code err)))
        (is (re-find #"[Uu]nknown wire format" (:message err))
            "error message must mention the unknown format code"))
      ;; Connection still usable
      (send-query! c "SELECT COUNT(*) FROM orders")
      (let [rows (decode-data-rows (drain-rfq c))]
        (is (= "5" (ffirst rows)))))))

;; ============================================================================
;; Step W2 — outbound binary encoders
;; ============================================================================

(defn- decode-data-rows-raw
  "Like decode-data-rows but returns raw payload byte[] per column instead of UTF-8 strings."
  [result]
  (for [{:keys [type body]} (:messages result)
        :when (= type \D)]
    (let [buf (ByteBuffer/wrap body)
          n   (.getShort buf)]
      (mapv (fn [_]
              (let [len (.getInt buf)]
                (when-not (neg? len)
                  (let [b (byte-array len)]
                    (.get buf b)
                    b))))
            (range n)))))

(defn- be->long
  "Decode a big-endian byte[] (1..8 bytes) into a signed long."
  ^long [^bytes b]
  (let [n (alength b)]
    (loop [i 0 acc 0]
      (if (= i n)
        (let [shift (* 8 (- 8 n))]
          (if (zero? shift) acc (bit-shift-right (bit-shift-left acc shift) shift)))
        (recur (inc i) (bit-or (bit-shift-left acc 8) (long (bit-and (aget b i) 0xff))))))))

(deftest w2-binary-int8-test
  (testing "Bind requesting binary result format for INT8 returns 8-byte big-endian payload"
    (with-conn [c (srv-port)]
      (send-parse!   c "SELECT quantity FROM orders WHERE price > $1 ORDER BY quantity")
      (send-bind-fmt! c [0] ["50.0"] [1])  ;; binary result
      (send-describe-portal! c)
      (send-execute! c)
      (send-sync!    c)
      (let [result (drain-rfq c)
            rd     (decode-row-description-fmts
                    (:body (first (filter #(= \T (:type %)) (:messages result)))))
            rows   (vec (decode-data-rows-raw result))]
        (is (= 1 (count rd)))
        (is (= 1 (nth (first rd) 2)) "RowDescription must advertise format=1 (binary)")
        ;; price > 50: rows are 5, 10, 20 (sorted asc)
        (is (= 3 (count rows)))
        (is (every? #(= 8 (alength ^bytes (first %))) rows)
            "every INT8 payload must be 8 bytes")
        (is (= [5 10 20] (mapv (fn [r] (be->long (first r))) rows))
            "binary-decoded INT8 values must match the rows")))))

(deftest w2-binary-float8-test
  (testing "Bind requesting binary result format for FLOAT8 returns 8-byte IEEE 754 BE payload"
    (with-conn [c (srv-port)]
      (send-parse!   c "SELECT price FROM orders WHERE price > $1 ORDER BY price")
      (send-bind-fmt! c [0] ["50.0"] [1])
      (send-execute! c)
      (send-sync!    c)
      (let [rows (vec (decode-data-rows-raw (drain-rfq c)))]
        ;; price > 50: 100.0, 200.0, 500.0
        (is (= 3 (count rows)))
        (let [decoded (mapv (fn [r]
                              (Double/longBitsToDouble (be->long (first r))))
                            rows)]
          (is (= [100.0 200.0 500.0] decoded)
              "binary-decoded FLOAT8 values match the rows"))))))

;; ============================================================================
;; Step W3 — inbound binary decoders
;; ============================================================================

(deftest w3-binary-int8-param-roundtrip-test
  (testing "Binary INT8 param decodes correctly when its type OID is declared at Parse"
    (with-conn [c (srv-port)]
      ;; quantity > 5 → row 5 (q=5 strict-gt fails) wait: orders quantities are 1,2,5,10,20
      ;; quantity > 5: 10 + 20 = 30 rows-count = 2. Test SUM(price) WHERE quantity > $1.
      (send-parse-typed! c "SELECT SUM(price) AS t FROM orders WHERE quantity > $1"
                         [PgWireServer/OID_INT8])
      (let [bin (byte-array 8)]
        ;; Encode 5 as 8-byte big-endian
        (aset-byte bin 7 (byte 5))
        (send-bind-binary! c [bin] [0])
        (send-execute! c)
        (send-sync!    c)
        (let [rows (decode-data-rows (drain-rfq c))]
          ;; quantity > 5 → quantities 10, 20 → prices 200.0 + 500.0 = 700.0
          (is (= "700.0" (ffirst rows))
              "binary INT8 param 5 should select prices for quantity > 5"))))))

(deftest w3-binary-float8-param-roundtrip-test
  (testing "Binary FLOAT8 param decodes correctly"
    (with-conn [c (srv-port)]
      (send-parse-typed! c "SELECT COUNT(*) FROM orders WHERE price > $1"
                         [PgWireServer/OID_FLOAT8])
      ;; Encode 100.0 as 8-byte big-endian IEEE 754
      (let [bits (Double/doubleToRawLongBits 100.0)
            bin  (byte-array 8)]
        (dotimes [i 8]
          (aset-byte bin i (byte (bit-and (unsigned-bit-shift-right bits (* 8 (- 7 i))) 0xff))))
        (send-bind-binary! c [bin] [0])
        (send-execute! c)
        (send-sync!    c)
        (let [rows (decode-data-rows (drain-rfq c))]
          ;; price > 100.0 → prices 200, 500 → count = 2
          (is (= "2" (ffirst rows))))))))

(deftest w3-binary-text-param-roundtrip-test
  (testing "Binary TEXT param decodes correctly"
    (with-conn [c (srv-port)]
      (send-parse-typed! c "SELECT SUM(price) FROM orders WHERE region = $1"
                         [PgWireServer/OID_TEXT])
      (let [bin (.getBytes "N" StandardCharsets/UTF_8)]
        (send-bind-binary! c [bin] [0])
        (send-execute! c)
        (send-sync!    c)
        (let [rows (decode-data-rows (drain-rfq c))]
          ;; region = N: prices 10 + 50 = 60.0
          (is (= "60.0" (ffirst rows))))))))

(deftest w3-binary-mixed-formats-param-test
  (testing "Mixed binary + text param formats both decode correctly"
    (with-conn [c (srv-port)]
      ;; param 1 binary INT8, param 2 text
      (send-parse-typed! c "SELECT COUNT(*) FROM orders WHERE quantity > $1 AND region = $2"
                         [PgWireServer/OID_INT8 PgWireServer/OID_TEXT])
      (let [bin1 (byte-array 8)]
        (aset-byte bin1 7 (byte 1))  ;; quantity > 1
        (let [baos (ByteArrayOutputStream.)
              dos  (DataOutputStream. baos)]
          (.writeByte dos 0) (.writeByte dos 0)
          (.writeShort dos 2)
          (.writeShort dos 1) (.writeShort dos 0)  ;; param 1 binary, param 2 text
          (.writeShort dos 2)
          (.writeInt dos 8) (.write dos bin1)
          (let [region-bytes (.getBytes "S" StandardCharsets/UTF_8)]
            (.writeInt dos (alength region-bytes))
            (.write dos region-bytes))
          (.writeShort dos 0)  ;; no result formats
          (.flush dos)
          (let [body (.toByteArray baos)]
            (.writeByte (:out c) (int \B))
            (.writeInt  (:out c) (+ 4 (count body)))
            (.write     (:out c) body)
            (.flush     (:out c))))
        (send-execute! c)
        (send-sync!    c)
        (let [rows (decode-data-rows (drain-rfq c))]
          ;; quantity > 1 AND region = S: (5,S) (10,S) → 2
          (is (= "2" (ffirst rows))))))))

(deftest w3-binary-param-unsupported-oid-rejected-test
  (testing "Binary input for an OID we don't decode → 0A000 with a clear message"
    (with-conn [c (srv-port)]
      ;; INT2VECTOR (22) — declared but not supported
      (send-parse-typed! c "SELECT 1 WHERE $1 IS NOT NULL" [22])
      (send-bind-binary! c [(byte-array 2)] [])
      (send-sync! c)
      (let [err (find-error (drain-rfq c))]
        (is (some? err))
        (is (= "0A000" (:code err)))
        (is (re-find #"OID=22" (:message err)))))))

(deftest w2-binary-mixed-row-test
  (testing "Per-column mix: column 1 text, column 2 binary — both work in the same row"
    (with-conn [c (srv-port)]
      (send-parse!   c "SELECT region, quantity FROM orders WHERE price > $1 ORDER BY price")
      (send-bind-fmt! c [0] ["50.0"] [0 1])  ;; text + binary
      (send-describe-portal! c)
      (send-execute! c)
      (send-sync!    c)
      (let [result (drain-rfq c)
            rd     (decode-row-description-fmts
                    (:body (first (filter #(= \T (:type %)) (:messages result)))))
            rows   (vec (decode-data-rows-raw result))]
        (is (= [0 1] (mapv #(nth % 2) rd))
            "RowDescription advertises per-column formats matching the request")
        (is (= 3 (count rows)))
        ;; price > 50 sorted ascending by price: [S 5] [S 10] [E 20]
        (let [decoded (mapv (fn [r]
                              [(String. ^bytes (first r) StandardCharsets/UTF_8)
                               (be->long (second r))])
                            rows)]
          (is (= [["S" 5] ["S" 10] ["E" 20]] decoded)
              "text + binary mix decodes correctly"))))))
