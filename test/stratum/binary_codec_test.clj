(ns stratum.binary-codec-test
  "Step W2/W3: roundtrip unit tests for stratum.internal.PgBinaryCodec.

   The codec implements PostgreSQL binary wire format (format code 1) for
   every OID the wire layer tags. These tests pin each encoder/decoder
   pair against fixed expected byte sequences (catching endianness or
   epoch-shift regressions) and verify lossless roundtrips across a
   range of edge values."
  (:require [clojure.test :refer [deftest is testing]])
  (:import [java.math BigDecimal]
           [java.util UUID]
           [stratum.internal PgBinaryCodec PgWireServer]))

;; ---------------------------------------------------------------------------
;; Helpers

(defn- ->hex [^bytes b]
  (when b
    (let [sb (StringBuilder.)]
      (doseq [x b]
        (.append sb (format "%02x" (bit-and (long x) 0xff))))
      (str sb))))

(defn- bytes-of [& vs]
  (byte-array (mapv #(unchecked-byte (long %)) vs)))

;; ---------------------------------------------------------------------------
;; Integer types

(deftest int2-roundtrip
  (doseq [v [0 1 -1 32767 -32768 12345 -12345]]
    (let [b (PgBinaryCodec/encodeInt2 (short v))]
      (is (= 2 (alength ^bytes b)))
      (is (= v (PgBinaryCodec/decodeInt2 b))
          (str "INT2 roundtrip for " v)))))

(deftest int4-roundtrip
  (doseq [v [0 1 -1 Integer/MAX_VALUE Integer/MIN_VALUE 1000000 -1000000]]
    (let [b (PgBinaryCodec/encodeInt4 (int v))]
      (is (= 4 (alength ^bytes b)))
      (is (= v (PgBinaryCodec/decodeInt4 b))))))

(deftest int8-roundtrip
  (doseq [v [0 1 -1 Long/MAX_VALUE Long/MIN_VALUE 1234567890123 -987654321098]]
    (let [b (PgBinaryCodec/encodeInt8 (long v))]
      (is (= 8 (alength ^bytes b)))
      (is (= v (PgBinaryCodec/decodeInt8 b))))))

(deftest int4-pins-big-endian
  ;; 0x01020304 must serialize as 01 02 03 04
  (is (= "01020304" (->hex (PgBinaryCodec/encodeInt4 0x01020304)))))

;; ---------------------------------------------------------------------------
;; Floats

(deftest float4-roundtrip
  (doseq [v [0.0 1.5 -1.5 (Float/MIN_VALUE) (Float/MAX_VALUE) 3.14159]]
    (let [b (PgBinaryCodec/encodeFloat4 (float v))]
      (is (= 4 (alength ^bytes b)))
      (is (= (float v) (PgBinaryCodec/decodeFloat4 b))))))

(deftest float8-roundtrip
  (doseq [v [0.0 1.5 -1.5 Double/MIN_VALUE Double/MAX_VALUE Math/PI -0.0]]
    (let [b (PgBinaryCodec/encodeFloat8 (double v))]
      (is (= 8 (alength ^bytes b)))
      (is (= (double v) (PgBinaryCodec/decodeFloat8 b))))))

;; ---------------------------------------------------------------------------
;; BOOL

(deftest bool-roundtrip
  (is (= "01" (->hex (PgBinaryCodec/encodeBool true))))
  (is (= "00" (->hex (PgBinaryCodec/encodeBool false))))
  (is (true?  (PgBinaryCodec/decodeBool (bytes-of 1))))
  (is (false? (PgBinaryCodec/decodeBool (bytes-of 0)))))

;; ---------------------------------------------------------------------------
;; TEXT

(deftest text-roundtrip
  (doseq [s ["" "hello" "ünïcödë ✓" "tabs\tnewlines\nquotes\""]]
    (is (= s (PgBinaryCodec/decodeText (PgBinaryCodec/encodeText s))))))

;; ---------------------------------------------------------------------------
;; DATE — PG epoch is 2000-01-01 = day 10957 since 1970-01-01

(deftest date-pg-epoch-pinned
  ;; Day 10957 (2000-01-01 Unix) must encode as 0 on the wire
  (is (= "00000000" (->hex (PgBinaryCodec/encodeDate 10957))))
  ;; Day 0 (1970-01-01 Unix) → -10957 on the wire = 0xFFFFD533
  (is (= "ffffd533" (->hex (PgBinaryCodec/encodeDate 0)))))

(deftest date-roundtrip
  (doseq [unix-days [0 10957 20000 -1000 19479]]  ;; 19479 = 2023-04-01
    (is (= unix-days (PgBinaryCodec/decodeDate
                      (PgBinaryCodec/encodeDate unix-days))))))

;; ---------------------------------------------------------------------------
;; TIMESTAMP — PG epoch in micros = 946684800000000

(deftest timestamp-pg-epoch-pinned
  (is (= "0000000000000000"
         (->hex (PgBinaryCodec/encodeTimestamp 946684800000000)))))

(deftest timestamp-roundtrip
  (doseq [unix-micros [0 946684800000000 1700000000000000 -1234567]]
    (is (= unix-micros (PgBinaryCodec/decodeTimestamp
                        (PgBinaryCodec/encodeTimestamp unix-micros))))))

;; ---------------------------------------------------------------------------
;; UUID

(deftest uuid-roundtrip
  (let [u (UUID/fromString "deadbeef-cafe-1234-5678-90abcdef0001")
        b (PgBinaryCodec/encodeUuid u)]
    (is (= 16 (alength ^bytes b)))
    (is (= u (PgBinaryCodec/decodeUuid b)))))

(deftest uuid-from-bytes
  (let [u (UUID/randomUUID)
        b1 (PgBinaryCodec/encodeUuid u)
        b2 (PgBinaryCodec/encodeUuid b1)]  ;; accept byte[16] too
    (is (= u (PgBinaryCodec/decodeUuid b2)))))

;; ---------------------------------------------------------------------------
;; JSONB — version byte 0x01 + UTF-8 body

(deftest jsonb-pinned-version
  (let [b (PgBinaryCodec/encodeJsonb "{}")]
    (is (= 0x01 (bit-and (aget ^bytes b 0) 0xff))
        "JSONB version byte must be 0x01")
    (is (= "{}" (PgBinaryCodec/decodeJsonb b)))))

(deftest jsonb-roundtrip
  (doseq [j ["{}" "[1,2,3]" "{\"a\":\"hello\"}" "null" "42"]]
    (is (= j (PgBinaryCodec/decodeJsonb (PgBinaryCodec/encodeJsonb j))))))

;; ---------------------------------------------------------------------------
;; NUMERIC — the highest-stakes encoder. We pin a few values against the
;; published wire format, then roundtrip a wide spread.

(deftest numeric-zero
  ;; Zero: ndigits=0 weight=0 sign=POS dscale=0 — 8 bytes total
  (let [b (PgBinaryCodec/encodeNumeric BigDecimal/ZERO)]
    (is (= "0000000000000000" (->hex b)))
    (is (= 0 (.compareTo BigDecimal/ZERO (PgBinaryCodec/decodeNumeric b))))))

(deftest numeric-one
  ;; 1 → ndigits=1, weight=0, sign=POS(0), dscale=0, digit[0]=0001
  (let [b (PgBinaryCodec/encodeNumeric (BigDecimal/ONE))]
    (is (= "00010000000000000001" (->hex b)))))

(deftest numeric-negative
  ;; -1 → sign=NEG(0x4000)
  (let [b (PgBinaryCodec/encodeNumeric (BigDecimal/valueOf -1))]
    (is (= "0001000040000000" (subs (->hex b) 0 16))
        "header for -1 must have sign=0x4000")))

(deftest numeric-fractional
  ;; 0.5: scale=1, dscale=1. Unscaled 5 padded to 5000 (1 base-10000 digit
  ;; in the fractional part). weight=-1.
  (let [b (PgBinaryCodec/encodeNumeric (BigDecimal. "0.5"))
        rt (PgBinaryCodec/decodeNumeric b)]
    (is (= 0 (.compareTo (BigDecimal. "0.5") rt)))))

(deftest numeric-roundtrip
  (doseq [s ["0" "1" "-1" "100" "100.00" "12345.6789"
             "0.0001" "-99999.99" "1234567890.12345"
             "0.5" "0.05" "0.005" "1.10" "-0.5"]]
    (let [bd (BigDecimal. ^String s)
          rt (PgBinaryCodec/decodeNumeric (PgBinaryCodec/encodeNumeric bd))]
      (is (= 0 (.compareTo bd rt))
          (str "NUMERIC roundtrip for " s " → " rt)))))

(deftest numeric-preserves-dscale-on-trailing-zero
  ;; Trailing zeros matter for display: 1.10 (scale=2) vs 1.1 (scale=1)
  ;; The dscale field on the wire preserves the requested precision.
  (let [bd (BigDecimal. "1.10")
        b  (PgBinaryCodec/encodeNumeric bd)
        rt (PgBinaryCodec/decodeNumeric b)]
    (is (= 2 (.scale rt))
        "dscale must roundtrip — 1.10 keeps scale=2 not 1")))

;; ---------------------------------------------------------------------------
;; Dispatcher

(deftest dispatcher-supports-set
  (doseq [oid [PgWireServer/OID_BOOL PgWireServer/OID_INT2 PgWireServer/OID_INT4
               PgWireServer/OID_INT8 PgWireServer/OID_FLOAT4 PgWireServer/OID_FLOAT8
               PgWireServer/OID_TEXT PgWireServer/OID_VARCHAR PgWireServer/OID_DATE
               PgWireServer/OID_TIMESTAMP PgWireServer/OID_TIMESTAMPTZ
               PgWireServer/OID_NUMERIC PgWireServer/OID_UUID PgWireServer/OID_JSONB]]
    (is (PgBinaryCodec/supportsBinaryOutput oid)
        (str "OID " oid " must report binary-output support"))
    (is (PgBinaryCodec/supportsBinaryInput oid)
        (str "OID " oid " must report binary-input support"))))

(deftest dispatcher-rejects-unknown
  ;; Some OID we never tag — say, INT2VECTOR (22)
  (is (not (PgBinaryCodec/supportsBinaryOutput 22)))
  (is (thrown? IllegalArgumentException
               (PgBinaryCodec/encode 22 (long 0)))))

(deftest dispatcher-encode-decode-roundtrip
  (let [pairs
        [[PgWireServer/OID_INT8  (long 9876)]
         [PgWireServer/OID_INT4  (int 12345)]
         [PgWireServer/OID_FLOAT8 (double Math/E)]
         [PgWireServer/OID_BOOL  true]
         [PgWireServer/OID_TEXT  "hello"]
         [PgWireServer/OID_DATE  (long 19479)]
         [PgWireServer/OID_TIMESTAMP (long 1700000000000000)]
         [PgWireServer/OID_NUMERIC (BigDecimal. "12345.6789")]
         [PgWireServer/OID_UUID  (UUID/randomUUID)]
         [PgWireServer/OID_JSONB "{\"x\":1}"]]]
    (doseq [[oid v] pairs]
      (let [b (PgBinaryCodec/encode oid v)
            rt (PgBinaryCodec/decode oid b)]
        (cond
          (instance? BigDecimal v) (is (= 0 (.compareTo ^BigDecimal v ^BigDecimal rt)))
          :else (is (= v rt)
                    (str "encode/decode for OID " oid " value " v)))))))
