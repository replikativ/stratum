(ns asof-bench
  "ASOF JOIN benchmark: Stratum vs DuckDB.

   Three shapes lifted from the established benchmark canon (see /home/christian-weilbach/Development/stratum/MEMORY.md
   project notes for sources):

     ASOF-Q1 dense    — DuckDB micro/asof_join.benchmark + ClickHouse asof.xml.
                        5M build × 5M probe, 50 keys, minute-spaced ts, perfectly
                        sorted, every probe matches between two build rows
                        (one-step search per probe — pure two-pointer territory).

     ASOF-Q2 unpart   — DuckDB micro/asof_join_unpartitioned.benchmark (downscaled).
                        10M build × 100K probe, no equality key, random unsorted ts
                        (galloping-friendly: each probe skips ~100 build rows).

     ASOF-Q3 mismatch — ClickHouse 03146_asof_join_ddb_merge_long.
                        6M build × 1000 probe, 2 keys, second-resolution build vs
                        hour-resolution probe (3600× sparser probe → galloping wins).

   Usage:
     clj -M:asof-bench           ; run all three at default scale
     clj -M:asof-bench q1        ; just Q1
     clj -M:asof-bench q2 q3     ; Q2 and Q3"
  (:require [stratum.query :as q]
            [clojure.string :as str])
  (:import [stratum.internal ColumnOps]
           [java.sql DriverManager Connection Statement ResultSet]
           [org.duckdb DuckDBConnection DuckDBAppender]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Bench harness
;; ============================================================================

(def ^:private ^:dynamic *warmup* 5)
(def ^:private ^:dynamic *iters*  10)

(defn- bench [f]
  (dotimes [_ *warmup*] (f))
  (let [times (mapv (fn [_]
                      (let [t0 (System/nanoTime)]
                        (f)
                        (/ (- (System/nanoTime) t0) 1e6)))
                    (range *iters*))
        sorted (sort times)]
    {:median (nth sorted (quot *iters* 2))
     :min    (first sorted)
     :p90    (nth sorted (int (* *iters* 0.9)))}))

(defn- bench-1t [f]
  (ColumnOps/setParallelThreshold Integer/MAX_VALUE)
  (try (bench f)
       (finally (ColumnOps/setParallelThreshold 200000))))

(defn- gc! [] (System/gc) (Thread/sleep 200))

(defn- fmt-ms [^double ms] (format "%7.1fms" ms))

(defn- fmt-ratio [^double s ^double d]
  (cond
    (and (< s 0.1) (< d 0.1)) "  both <0.1ms"
    (zero? s)                 "  Stratum 0ms"
    :else (let [r (/ d s)]
            (cond (>= r 1.0) (format "  %.1fx faster" r)
                  :else      (format "  %.1fx SLOWER" (/ 1.0 r))))))

;; ============================================================================
;; DuckDB JDBC harness
;; ============================================================================

(defn- duckdb-connect ^Connection []
  (Class/forName "org.duckdb.DuckDBDriver")
  (DriverManager/getConnection "jdbc:duckdb:"))

(defn- duck-exec! [^Connection conn ^String sql]
  (with-open [^Statement st (.createStatement conn)]
    (.execute st sql)))

(defn- duck-scalar [^Connection conn ^String sql]
  (with-open [^Statement st (.createStatement conn)
              ^ResultSet rs (.executeQuery st sql)]
    (when (.next rs) (.getObject rs 1))))

(defn- duck-bench [^Connection conn ^String sql & {:keys [threads]}]
  (when threads (duck-exec! conn (str "SET threads=" threads)))
  (try
    (dotimes [_ *warmup*]
      (with-open [^Statement st (.createStatement conn)
                  ^ResultSet rs (.executeQuery st sql)]
        (while (.next rs))))
    (let [times (mapv (fn [_]
                        (let [t0 (System/nanoTime)]
                          (with-open [^Statement st (.createStatement conn)
                                      ^ResultSet rs (.executeQuery st sql)]
                            (while (.next rs)))
                          (/ (- (System/nanoTime) t0) 1e6)))
                      (range *iters*))
          sorted (sort times)]
      {:median (nth sorted (quot *iters* 2))
       :min    (first sorted)
       :p90    (nth sorted (int (* *iters* 0.9)))})
    (finally
      (when threads
        (duck-exec! conn (str "SET threads=" (.availableProcessors (Runtime/getRuntime))))))))

;; ============================================================================
;; Data generation — deterministic so DuckDB and Stratum see identical data
;; ============================================================================
;;
;; For each benchmark, we generate the build/probe arrays in Clojure and stream
;; them to DuckDB via a single read-from-VALUES SQL statement, OR use DuckDB's
;; range() to generate the same shape natively. Q1 mirrors DuckDB's range-based
;; generation exactly; Q2/Q3 use a deterministic LCG so both sides match.

(defn- lcg-next ^long [^long s]
  ;; Numerical Recipes parameters; deterministic, reasonable distribution.
  (unchecked-add (unchecked-multiply s 1664525) 1013904223))

;; ----------------------------------------------------------------------------
;; Q1: dense, 50 keys × 100K minutes, probe shifted -30s
;; Build: k ∈ 0..49, v ∈ 0..99999, t = base + v*60 (seconds)
;; Probe: k*2, t = build.t - 30 (seconds)

(defn- gen-q1 [^long n-mins ^long n-keys]
  (let [n      (* n-keys n-mins)
        b-k    (long-array n)
        b-t    (long-array n)
        b-v    (long-array n)]
    (dotimes [k n-keys]
      (dotimes [v n-mins]
        (let [i (+ (* (long k) n-mins) v)]
          (aset b-k i (long k))
          (aset b-t i (* (long v) 60))   ; seconds
          (aset b-v i (long v)))))
    (let [p-k (long-array n)
          p-t (long-array n)]
      (dotimes [i n]
        (aset p-k i (* 2 (aget b-k i)))
        (aset p-t i (- (aget b-t i) 30)))
      {:build-k b-k :build-t b-t :build-v b-v
       :probe-k p-k :probe-t p-t :n n})))

;; ----------------------------------------------------------------------------
;; Q2: unpartitioned, 10M build × 100K probe, ts shuffled but unique per side
;; (unique avoids non-deterministic tie-breaking diverging from DuckDB)

(defn- shuffled-unique-ts
  "Return long[] of length n with a deterministic permutation of 0..n-1.
   Random-looking via Fisher-Yates with seeded LCG."
  ^longs [^long n ^long seed]
  (let [out (long-array n)]
    (dotimes [i n] (aset out i (long i)))
    (loop [i (dec n) s seed]
      (when (pos? i)
        (let [s' (lcg-next s)
              j  (long (mod (Math/abs s') (inc i)))
              ti (aget out i)
              tj (aget out j)]
          (aset out i tj)
          (aset out j ti)
          (recur (dec i) s'))))
    out))

(defn- gen-q2 [^long n-build ^long n-probe]
  ;; Build ts ∈ [0, n-build), unique. Probe ts ∈ [0, n-build), unique subset.
  ;; Both shuffled into random order so the sort is non-trivial. Probe values are
  ;; drawn from a wider range (n-build) so most probes land between build rows.
  (let [b-t (shuffled-unique-ts n-build 1234567)
        b-v (long-array n-build)
        p-t (long-array n-probe)]
    (dotimes [i n-build] (aset b-v i (long i)))
    (let [stride (max 1 (long (/ n-build n-probe)))]
      (dotimes [i n-probe]
        ;; Probe ts = i*stride + offset (so probe values are interleaved between build values)
        (aset p-t i (long (* i stride)))))
    ;; Shuffle probe ts
    (let [seed 7654321
          n n-probe]
      (loop [i (dec n) s seed]
        (when (pos? i)
          (let [s' (lcg-next s)
                j  (long (mod (Math/abs s') (inc i)))
                ti (aget p-t i)
                tj (aget p-t j)]
            (aset p-t i tj)
            (aset p-t j ti)
            (recur (dec i) s')))))
    {:build-t b-t :build-v b-v :probe-t p-t :n-build n-build :n-probe n-probe}))

;; ----------------------------------------------------------------------------
;; Q3: granularity mismatch — 6M build / 1K probe / 2 keys
;; Build: k ∈ {0,1}, t = i seconds (3M seconds per key ≈ 35 days)
;; Probe: 1000 rows × 2 keys = 2000 rows total. ts at hourly intervals (3600× sparser)

(defn- gen-q3 [^long build-per-key ^long probe-per-key]
  (let [n-keys 2
        n-build (* n-keys build-per-key)
        n-probe (* n-keys probe-per-key)
        b-k (long-array n-build)
        b-t (long-array n-build)
        b-v (long-array n-build)
        p-k (long-array n-probe)
        p-t (long-array n-probe)]
    (dotimes [k n-keys]
      (dotimes [i build-per-key]
        (let [idx (+ (* (long k) build-per-key) i)]
          (aset b-k idx (long k))
          (aset b-t idx (long i))     ; second
          (aset b-v idx (long i)))))
    (dotimes [k n-keys]
      (dotimes [i probe-per-key]
        (let [idx (+ (* (long k) probe-per-key) i)]
          (aset p-k idx (long k))
          ;; hourly-spaced: i*3600. Range fits inside build (i*3600 < build-per-key)
          ;; provided probe-per-key * 3600 ≤ build-per-key.
          (aset p-t idx (long (* i 3600))))))
    {:build-k b-k :build-t b-t :build-v b-v
     :probe-k p-k :probe-t p-t
     :n-build n-build :n-probe n-probe}))

;; ============================================================================
;; DuckDB data load — prepared statements (one per benchmark, deterministic)
;; ============================================================================

(defn- ^DuckDBConnection ->duck-conn [^Connection conn]
  (.unwrap conn DuckDBConnection))

(defn- load-q1-into-duckdb! [^Connection conn data]
  (println "  Loading Q1 data into DuckDB...")
  (duck-exec! conn "DROP TABLE IF EXISTS asof_q1_b")
  (duck-exec! conn "DROP TABLE IF EXISTS asof_q1_p")
  (let [n (long (:n data))
        ^longs bk (:build-k data)
        ^longs bt (:build-t data)
        ^longs bv (:build-v data)
        ^longs pk (:probe-k data)
        ^longs pt (:probe-t data)
        dconn (->duck-conn conn)]
    (duck-exec! conn "CREATE TABLE asof_q1_b (k BIGINT, t BIGINT, v BIGINT)")
    (duck-exec! conn "CREATE TABLE asof_q1_p (k BIGINT, t BIGINT)")
    (with-open [^DuckDBAppender app (.createAppender dconn "main" "asof_q1_b")]
      (dotimes [i n]
        (.beginRow app)
        (.append app (aget bk i))
        (.append app (aget bt i))
        (.append app (aget bv i))
        (.endRow app)))
    (with-open [^DuckDBAppender app (.createAppender dconn "main" "asof_q1_p")]
      (dotimes [i n]
        (.beginRow app)
        (.append app (aget pk i))
        (.append app (aget pt i))
        (.endRow app)))
    (println (format "  build=%s probe=%s" n n))))

(defn- load-q2-into-duckdb! [^Connection conn data]
  (println "  Loading Q2 data into DuckDB...")
  (duck-exec! conn "DROP TABLE IF EXISTS asof_q2_b")
  (duck-exec! conn "DROP TABLE IF EXISTS asof_q2_p")
  (let [^longs bt (:build-t data)
        ^longs bv (:build-v data)
        ^longs pt (:probe-t data)
        nb (long (:n-build data))
        np (long (:n-probe data))
        dconn (->duck-conn conn)]
    (duck-exec! conn "CREATE TABLE asof_q2_b (t BIGINT, v BIGINT)")
    (duck-exec! conn "CREATE TABLE asof_q2_p (t BIGINT)")
    (with-open [^DuckDBAppender app (.createAppender dconn "main" "asof_q2_b")]
      (dotimes [i nb]
        (.beginRow app)
        (.append app (aget bt i))
        (.append app (aget bv i))
        (.endRow app)))
    (with-open [^DuckDBAppender app (.createAppender dconn "main" "asof_q2_p")]
      (dotimes [i np]
        (.beginRow app)
        (.append app (aget pt i))
        (.endRow app)))
    (println (format "  build=%s probe=%s" nb np))))

(defn- load-q3-into-duckdb! [^Connection conn data]
  (println "  Loading Q3 data into DuckDB...")
  (duck-exec! conn "DROP TABLE IF EXISTS asof_q3_b")
  (duck-exec! conn "DROP TABLE IF EXISTS asof_q3_p")
  (let [^longs bk (:build-k data)
        ^longs bt (:build-t data)
        ^longs bv (:build-v data)
        ^longs pk (:probe-k data)
        ^longs pt (:probe-t data)
        nb (long (:n-build data))
        np (long (:n-probe data))
        dconn (->duck-conn conn)]
    (duck-exec! conn "CREATE TABLE asof_q3_b (k BIGINT, t BIGINT, v BIGINT)")
    (duck-exec! conn "CREATE TABLE asof_q3_p (k BIGINT, t BIGINT)")
    (with-open [^DuckDBAppender app (.createAppender dconn "main" "asof_q3_b")]
      (dotimes [i nb]
        (.beginRow app)
        (.append app (aget bk i))
        (.append app (aget bt i))
        (.append app (aget bv i))
        (.endRow app)))
    (with-open [^DuckDBAppender app (.createAppender dconn "main" "asof_q3_p")]
      (dotimes [i np]
        (.beginRow app)
        (.append app (aget pk i))
        (.append app (aget pt i))
        (.endRow app)))
    (println (format "  build=%s probe=%s" nb np))))

;; ============================================================================
;; Queries
;; ============================================================================

(defn- q1-stratum [data]
  (q/q {:from {:k (:probe-k data) :pt (:probe-t data)}
        :join [{:with {:k (:build-k data) :bt (:build-t data) :v (:build-v data)}
                :type :asof
                :on [[:= :k :k]
                     [:>= :pt :bt]]}]
        :agg [[:sum :v]]}))

(def ^:private q1-sql
  "SELECT SUM(v) FROM asof_q1_p p ASOF JOIN asof_q1_b b ON p.k = b.k AND p.t >= b.t")

(defn- q2-stratum [data]
  (q/q {:from {:pt (:probe-t data)}
        :join [{:with {:bt (:build-t data) :v (:build-v data)}
                :type :asof
                :on [:>= :pt :bt]}]
        :agg [[:sum :v]]}))

(def ^:private q2-sql
  "SELECT SUM(v) FROM asof_q2_p p ASOF JOIN asof_q2_b b ON p.t >= b.t")

(defn- q3-stratum [data]
  (q/q {:from {:k (:probe-k data) :pt (:probe-t data)}
        :join [{:with {:k (:build-k data) :bt (:build-t data) :v (:build-v data)}
                :type :asof
                :on [[:= :k :k]
                     [:>= :pt :bt]]}]
        :agg [[:sum :v]]}))

(def ^:private q3-sql
  "SELECT SUM(v) FROM asof_q3_p p ASOF JOIN asof_q3_b b ON p.k = b.k AND p.t >= b.t")

;; ============================================================================
;; Bench runner
;; ============================================================================

(defn- run-bench [name stratum-fn duckdb-conn duckdb-sql]
  (println (str "\n=== " name " ==="))
  (gc!)
  (let [;; Verify correctness: read :sum from the auto-aliased agg, not :_count
        row (first (stratum-fn))
        s-val (long (or (:sum row) 0))
        d-val (long (or (duck-scalar duckdb-conn duckdb-sql) 0))]
    (println (format "  Stratum SUM=%d  DuckDB SUM=%d  %s"
                     s-val d-val
                     (if (= s-val d-val) "MATCH" "*** MISMATCH ***"))))
  (gc!)
  (let [s-1t (bench-1t stratum-fn)
        _ (gc!)
        s-nt (bench  stratum-fn)
        _ (gc!)
        d-1t (duck-bench duckdb-conn duckdb-sql :threads 1)
        _ (gc!)
        d-nt (duck-bench duckdb-conn duckdb-sql)]
    (println (format "  Stratum 1T %s | NT %s"
                     (fmt-ms (:median s-1t)) (fmt-ms (:median s-nt))))
    (println (format "  DuckDB  1T %s | NT %s"
                     (fmt-ms (:median d-1t)) (fmt-ms (:median d-nt))))
    (println (format "          1T %s | NT %s"
                     (fmt-ratio (:median s-1t) (:median d-1t))
                     (fmt-ratio (:median s-nt) (:median d-nt))))
    {:name name
     :s-1t (:median s-1t) :s-nt (:median s-nt)
     :d-1t (:median d-1t) :d-nt (:median d-nt)}))

;; ============================================================================
;; Main
;; ============================================================================

(defn- run-q1 [^Connection conn]
  (println "\n--- ASOF-Q1 dense (5M build × 5M probe, 50 keys, sorted) ---")
  (let [t0 (System/nanoTime)
        data (gen-q1 100000 50)
        _ (println (format "  Gen: %.1fs" (/ (- (System/nanoTime) t0) 1e9)))]
    (load-q1-into-duckdb! conn data)
    (run-bench "ASOF-Q1 dense" #(q1-stratum data) conn q1-sql)))

(defn- run-q2 [^Connection conn]
  (println "\n--- ASOF-Q2 unpartitioned (10M build × 100K probe, random ts) ---")
  (let [t0 (System/nanoTime)
        data (gen-q2 10000000 100000)
        _ (println (format "  Gen: %.1fs" (/ (- (System/nanoTime) t0) 1e9)))]
    (load-q2-into-duckdb! conn data)
    (run-bench "ASOF-Q2 unpart" #(q2-stratum data) conn q2-sql)))

(defn- run-q3 [^Connection conn]
  (println "\n--- ASOF-Q3 granularity-mismatch (6M build / 1K probe per key, 2 keys, sec vs hour) ---")
  (let [t0 (System/nanoTime)
        data (gen-q3 3000000 800)   ; 800*3600 ≈ 2.88M < 3M build per key
        _ (println (format "  Gen: %.1fs" (/ (- (System/nanoTime) t0) 1e9)))]
    (load-q3-into-duckdb! conn data)
    (run-bench "ASOF-Q3 mismatch" #(q3-stratum data) conn q3-sql)))

(defn- print-summary [results]
  (println "\n========== ASOF Benchmark Summary ==========")
  (println (format "%-22s %10s %10s %10s %10s   %-10s %-10s"
                   "Bench" "Strat 1T" "Duck 1T" "Strat NT" "Duck NT" "1T ratio" "NT ratio"))
  (doseq [{:keys [name s-1t s-nt d-1t d-nt]} results]
    (println (format "%-22s %10s %10s %10s %10s   %-10s %-10s"
                     name
                     (fmt-ms s-1t) (fmt-ms d-1t) (fmt-ms s-nt) (fmt-ms d-nt)
                     (str/trim (fmt-ratio s-1t d-1t))
                     (str/trim (fmt-ratio s-nt d-nt))))))

(defn -main [& args]
  (let [picks (set (map keyword (or (seq args) ["q1" "q2" "q3"])))
        conn (duckdb-connect)]
    (try
      (let [results (cond-> []
                      (contains? picks :q1) (conj (run-q1 conn))
                      (contains? picks :q2) (conj (run-q2 conn))
                      (contains? picks :q3) (conj (run-q3 conn)))]
        (print-summary results))
      (finally
        (.close conn)))))
