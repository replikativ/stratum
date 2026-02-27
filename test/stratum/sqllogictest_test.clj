(ns stratum.sqllogictest-test
  "sqllogictest runner for Stratum.
   Parses .test files in the sqllogictest format and executes them
   through stratum.sql/parse-sql + stratum.query/q."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [stratum.sql :as sql]
            [stratum.query :as q]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; DDL execution (mutable registry per test file)
;; ============================================================================

(defn- arr-length
  "Return the length of a typed array."
  [arr]
  (cond
    (instance? (Class/forName "[J") arr) (alength ^longs arr)
    (instance? (Class/forName "[D") arr) (alength ^doubles arr)
    (instance? (Class/forName "[Ljava.lang.String;") arr) (alength ^"[Ljava.lang.String;" arr)
    :else 0))

(defn- eval-row-val
  "Evaluate an expression for row i against table columns."
  [existing i e]
  (cond
    (keyword? e)
    (let [arr (get existing e)]
      (cond
        (instance? (Class/forName "[J") arr)
        (let [v (aget ^longs arr (int i))]
          (when-not (= v Long/MIN_VALUE) v))
        (instance? (Class/forName "[D") arr)
        (let [v (aget ^doubles arr (int i))]
          (when-not (Double/isNaN v) v))
        (instance? (Class/forName "[Ljava.lang.String;") arr)
        (aget ^"[Ljava.lang.String;" arr (int i))))
    (number? e) e
    (string? e) e
    (nil? e) nil
    (vector? e)
    (case (first e)
      :+ (+ (double (eval-row-val existing i (nth e 1)))
            (double (eval-row-val existing i (nth e 2))))
      :- (- (double (eval-row-val existing i (nth e 1)))
            (double (eval-row-val existing i (nth e 2))))
      :* (* (double (eval-row-val existing i (nth e 1)))
            (double (eval-row-val existing i (nth e 2))))
      :/ (/ (double (eval-row-val existing i (nth e 1)))
            (double (eval-row-val existing i (nth e 2))))
      nil)))

(defn- eval-pred
  "Evaluate a single predicate for row i."
  [existing i pred]
  (let [ev (fn [e] (eval-row-val existing i e))]
    (case (first pred)
      := (let [l (ev (nth pred 1)) r (ev (nth pred 2))]
           (and (some? l) (some? r) (= l r)))
      (:!= :<>) (let [l (ev (nth pred 1)) r (ev (nth pred 2))]
                  (and (some? l) (some? r) (not= l r)))
      :> (let [l (ev (nth pred 1)) r (ev (nth pred 2))]
           (and (some? l) (some? r) (> (double l) (double r))))
      :< (let [l (ev (nth pred 1)) r (ev (nth pred 2))]
           (and (some? l) (some? r) (< (double l) (double r))))
      :>= (let [l (ev (nth pred 1)) r (ev (nth pred 2))]
            (and (some? l) (some? r) (>= (double l) (double r))))
      :<= (let [l (ev (nth pred 1)) r (ev (nth pred 2))]
            (and (some? l) (some? r) (<= (double l) (double r))))
      :is-null (nil? (ev (nth pred 1)))
      :is-not-null (some? (ev (nth pred 1)))
      false)))

(defn- execute-ddl!
  "Execute a DDL descriptor against a mutable table registry atom."
  [registry-atom {:keys [op table columns rows assignments where
                         conflict-cols action from table-alias]}]
  (case op
    :create-table
    (let [cols (into {}
                     (map (fn [{:keys [name type]}]
                            [(keyword name)
                             (case type
                               :int64   (long-array 0)
                               :float64 (double-array 0)
                               :string  (make-array String 0))]))
                     columns)]
      (swap! registry-atom assoc table cols))

    :drop-table
    (swap! registry-atom dissoc table)

    :insert
    (let [existing (get @registry-atom table)]
      (when-not existing
        (throw (ex-info (str "Table not found: " table) {:table table})))
      (let [col-keys (vec (keys existing))
            n-existing (if-let [first-col (get existing (first col-keys))]
                         (cond
                           (instance? (Class/forName "[J") first-col)
                           (alength ^longs first-col)
                           (instance? (Class/forName "[D") first-col)
                           (alength ^doubles first-col)
                           (instance? (Class/forName "[Ljava.lang.String;") first-col)
                           (alength ^"[Ljava.lang.String;" first-col)
                           :else 0)
                         0)
            n-new (count rows)
            n-total (+ n-existing n-new)
            new-cols
            (into {}
                  (map-indexed
                   (fn [ci col-key]
                     (let [old-arr (get existing col-key)]
                       [col-key
                        (cond
                          (instance? (Class/forName "[J") old-arr)
                          (let [arr (long-array n-total)]
                            (System/arraycopy ^longs old-arr 0 arr 0 n-existing)
                            (dotimes [r n-new]
                              (let [v (nth (nth rows r) ci)]
                                (aset arr (+ n-existing r)
                                      (long (if (nil? v) Long/MIN_VALUE v)))))
                            arr)

                          (instance? (Class/forName "[D") old-arr)
                          (let [arr (double-array n-total)]
                            (System/arraycopy ^doubles old-arr 0 arr 0 n-existing)
                            (dotimes [r n-new]
                              (let [v (nth (nth rows r) ci)]
                                (aset arr (+ n-existing r)
                                      (double (if (nil? v) Double/NaN v)))))
                            arr)

                          (instance? (Class/forName "[Ljava.lang.String;") old-arr)
                          (let [arr (make-array String n-total)]
                            (System/arraycopy ^"[Ljava.lang.String;" old-arr 0
                                              arr 0 n-existing)
                            (dotimes [r n-new]
                              (aset ^"[Ljava.lang.String;" arr (+ n-existing r)
                                    (let [v (nth (nth rows r) ci)]
                                      (when (some? v) (str v)))))
                            arr))]))
                   col-keys))]
        (swap! registry-atom assoc table new-cols)))

    :update
    (let [existing (get @registry-atom table)]
      (when-not existing
        (throw (ex-info (str "Table not found: " table) {:table table})))
      (let [from-existing (when from
                            (let [ft (get @registry-atom (:table from))]
                              (when-not ft
                                (throw (ex-info (str "FROM table not found: " (:table from))
                                                {:table (:table from)})))
                              ft))
            col-keys (vec (keys existing))
            n-rows (arr-length (get existing (first col-keys)))
            n-from (when from-existing
                     (arr-length (val (first from-existing))))
            from-table-name (when from (:table from))
            from-alias-name (when from (:alias from))
            ;; Evaluate expr resolving from both tables
            eval-merged (fn eval-merged [e target from-t ti fi]
                          (cond
                            (keyword? e)
                            (let [ns (namespace e)
                                  col-kw (keyword (name e))]
                              (if ns
                                ;; Qualified: resolve to specific table
                                (if (or (= ns table) (= ns table-alias))
                                  (eval-row-val target ti col-kw)
                                  (when (and from-t (>= fi 0))
                                    (eval-row-val from-t fi col-kw)))
                                ;; Unqualified: target first, then FROM
                                (or (eval-row-val target ti e)
                                    (when (and from-t (>= fi 0))
                                      (eval-row-val from-t fi e)))))
                            (number? e) e
                            (string? e) e
                            (nil? e) nil
                            (vector? e)
                            (case (first e)
                              :+ (+ (double (eval-merged (nth e 1) target from-t ti fi))
                                    (double (eval-merged (nth e 2) target from-t ti fi)))
                              :- (- (double (eval-merged (nth e 1) target from-t ti fi))
                                    (double (eval-merged (nth e 2) target from-t ti fi)))
                              :* (* (double (eval-merged (nth e 1) target from-t ti fi))
                                    (double (eval-merged (nth e 2) target from-t ti fi)))
                              :/ (/ (double (eval-merged (nth e 1) target from-t ti fi))
                                    (double (eval-merged (nth e 2) target from-t ti fi)))
                              nil)))
            ;; Evaluate pred resolving from both tables
            eval-pred-merged (fn [pred target from-t ti fi]
                               (let [ev (fn [e] (eval-merged e target from-t ti fi))]
                                 (case (first pred)
                                   := (let [l (ev (nth pred 1)) r (ev (nth pred 2))]
                                        (and (some? l) (some? r) (= l r)))
                                   (:!= :<>) (let [l (ev (nth pred 1)) r (ev (nth pred 2))]
                                               (and (some? l) (some? r) (not= l r)))
                                   :> (let [l (ev (nth pred 1)) r (ev (nth pred 2))]
                                        (and (some? l) (some? r) (> (double l) (double r))))
                                   :< (let [l (ev (nth pred 1)) r (ev (nth pred 2))]
                                        (and (some? l) (some? r) (< (double l) (double r))))
                                   :>= (let [l (ev (nth pred 1)) r (ev (nth pred 2))]
                                         (and (some? l) (some? r) (>= (double l) (double r))))
                                   :<= (let [l (ev (nth pred 1)) r (ev (nth pred 2))]
                                         (and (some? l) (some? r) (<= (double l) (double r))))
                                   :is-null (nil? (ev (nth pred 1)))
                                   :is-not-null (some? (ev (nth pred 1)))
                                   false)))]
        (if from-existing
          ;; UPDATE FROM: for each target row, find matching FROM row
          (dotimes [i n-rows]
            (loop [j (int 0)]
              (when (< j (int n-from))
                (if (every? #(eval-pred-merged % existing from-existing (long i) (long j)) where)
                  (doseq [{:keys [col expr]} assignments]
                    (let [arr (get existing col)
                          v (eval-merged expr existing from-existing (long i) (long j))]
                      (cond
                        (instance? (Class/forName "[J") arr)
                        (aset ^longs arr (int i) (long (if (nil? v) Long/MIN_VALUE v)))
                        (instance? (Class/forName "[D") arr)
                        (aset ^doubles arr (int i) (double (if (nil? v) Double/NaN v)))
                        (instance? (Class/forName "[Ljava.lang.String;") arr)
                        (aset ^"[Ljava.lang.String;" arr (int i)
                              (when (some? v) (str v))))))
                  (recur (inc j))))))
          ;; Simple UPDATE
          (dotimes [i n-rows]
            (when (or (nil? where)
                      (every? #(eval-pred existing i %) where))
              (doseq [{:keys [col expr]} assignments]
                (let [arr (get existing col)
                      v (eval-row-val existing i expr)]
                  (cond
                    (instance? (Class/forName "[J") arr)
                    (aset ^longs arr (int i) (long (if (nil? v) Long/MIN_VALUE v)))
                    (instance? (Class/forName "[D") arr)
                    (aset ^doubles arr (int i) (double (if (nil? v) Double/NaN v)))
                    (instance? (Class/forName "[Ljava.lang.String;") arr)
                    (aset ^"[Ljava.lang.String;" arr (int i)
                          (when (some? v) (str v)))))))))))

    :delete
    (let [existing (get @registry-atom table)]
      (when-not existing
        (throw (ex-info (str "Table not found: " table) {:table table})))
      (let [col-keys (vec (keys existing))
            n-rows (arr-length (get existing (first col-keys)))
            ;; Build survival mask
            keep (boolean-array n-rows)]
        (dotimes [i n-rows]
          (aset keep i
                (boolean
                 (if (nil? where)
                   false  ;; DELETE all
                   (not (every? #(eval-pred existing i %) where))))))
        (let [n-surviving (loop [i (int 0) c (int 0)]
                            (if (>= i n-rows) c
                                (recur (inc i) (if (aget keep i) (inc c) c))))
              new-cols
              (into {}
                    (map (fn [col-key]
                           (let [old-arr (get existing col-key)]
                             [col-key
                              (cond
                                (instance? (Class/forName "[J") old-arr)
                                (let [arr (long-array n-surviving)]
                                  (loop [i (int 0) j (int 0)]
                                    (when (< i n-rows)
                                      (if (aget keep i)
                                        (do (aset arr j (aget ^longs old-arr i))
                                            (recur (inc i) (inc j)))
                                        (recur (inc i) j))))
                                  arr)

                                (instance? (Class/forName "[D") old-arr)
                                (let [arr (double-array n-surviving)]
                                  (loop [i (int 0) j (int 0)]
                                    (when (< i n-rows)
                                      (if (aget keep i)
                                        (do (aset arr j (aget ^doubles old-arr i))
                                            (recur (inc i) (inc j)))
                                        (recur (inc i) j))))
                                  arr)

                                (instance? (Class/forName "[Ljava.lang.String;") old-arr)
                                (let [arr (make-array String n-surviving)]
                                  (loop [i (int 0) j (int 0)]
                                    (when (< i n-rows)
                                      (if (aget keep i)
                                        (do (aset ^"[Ljava.lang.String;" arr j
                                                  (aget ^"[Ljava.lang.String;" old-arr i))
                                            (recur (inc i) (inc j)))
                                        (recur (inc i) j))))
                                  arr))]))
                         col-keys))]
          (swap! registry-atom assoc table new-cols))))

    :upsert
    (let [existing (get @registry-atom table)]
      (when-not existing
        (throw (ex-info (str "Table not found: " table) {:table table})))
      (let [col-keys (vec (keys existing))
            n-existing (arr-length (get existing (first col-keys)))
            conflict-col-indices (mapv #(.indexOf ^java.util.List col-keys %) conflict-cols)
            get-val (fn [arr ^long i]
                      (cond
                        (instance? (Class/forName "[J") arr)
                        (let [v (aget ^longs arr (int i))]
                          (when-not (= v Long/MIN_VALUE) v))
                        (instance? (Class/forName "[D") arr)
                        (let [v (aget ^doubles arr (int i))]
                          (when-not (Double/isNaN v) v))
                        (instance? (Class/forName "[Ljava.lang.String;") arr)
                        (aget ^"[Ljava.lang.String;" arr (int i))))
            set-val (fn [arr ^long i v]
                      (cond
                        (instance? (Class/forName "[J") arr)
                        (aset ^longs arr (int i) (long (if (nil? v) Long/MIN_VALUE v)))
                        (instance? (Class/forName "[D") arr)
                        (aset ^doubles arr (int i) (double (if (nil? v) Double/NaN v)))
                        (instance? (Class/forName "[Ljava.lang.String;") arr)
                        (aset ^"[Ljava.lang.String;" arr (int i)
                              (when (some? v) (str v)))))
            result (reduce
                    (fn [{:keys [cols n-rows]} row]
                      (let [row-conflict-vals (mapv #(nth row (int %)) conflict-col-indices)
                            match-idx (loop [i (int 0)]
                                        (when (< i n-rows)
                                          (let [existing-vals
                                                (mapv (fn [cc]
                                                        (get-val (get cols cc) i))
                                                      conflict-cols)]
                                            (if (= row-conflict-vals existing-vals)
                                              i
                                              (recur (inc i))))))]
                        (if match-idx
                          (if (= action :do-update)
                            (let [excluded (zipmap col-keys row)]
                              (doseq [{:keys [col expr]} assignments]
                                (let [arr (get cols col)
                                      v (cond
                                          (keyword? expr)
                                          (if (= "excluded" (namespace expr))
                                            (get excluded (keyword (name expr)))
                                            (get-val (get cols expr) match-idx))
                                          (number? expr) expr
                                          (string? expr) expr
                                          (nil? expr) nil
                                          (vector? expr)
                                          (let [ev (fn ev [e]
                                                     (cond
                                                       (keyword? e)
                                                       (if (= "excluded" (namespace e))
                                                         (get excluded (keyword (name e)))
                                                         (get-val (get cols e) match-idx))
                                                       (number? e) e
                                                       (vector? e)
                                                       (case (first e)
                                                         :+ (+ (double (ev (nth e 1)))
                                                               (double (ev (nth e 2))))
                                                         :- (- (double (ev (nth e 1)))
                                                               (double (ev (nth e 2))))
                                                         :* (* (double (ev (nth e 1)))
                                                               (double (ev (nth e 2))))
                                                         :/ (/ (double (ev (nth e 1)))
                                                               (double (ev (nth e 2))))
                                                         nil)))]
                                            (ev expr)))]
                                  (set-val arr match-idx v)))
                              {:cols cols :n-rows n-rows})
                            ;; DO NOTHING
                            {:cols cols :n-rows n-rows})
                          ;; No conflict — append
                          (let [new-n (inc n-rows)
                                new-cols
                                (into {}
                                      (map-indexed
                                       (fn [ci col-key]
                                         (let [old-arr (get cols col-key)]
                                           [col-key
                                            (cond
                                              (instance? (Class/forName "[J") old-arr)
                                              (let [arr (long-array new-n)]
                                                (System/arraycopy ^longs old-arr 0 arr 0 n-rows)
                                                (aset arr n-rows
                                                      (long (let [v (nth row ci)]
                                                              (if (nil? v) Long/MIN_VALUE v))))
                                                arr)
                                              (instance? (Class/forName "[D") old-arr)
                                              (let [arr (double-array new-n)]
                                                (System/arraycopy ^doubles old-arr 0 arr 0 n-rows)
                                                (aset arr n-rows
                                                      (double (let [v (nth row ci)]
                                                                (if (nil? v) Double/NaN v))))
                                                arr)
                                              (instance? (Class/forName "[Ljava.lang.String;") old-arr)
                                              (let [arr (make-array String new-n)]
                                                (System/arraycopy ^"[Ljava.lang.String;" old-arr 0
                                                                  arr 0 n-rows)
                                                (aset ^"[Ljava.lang.String;" arr n-rows
                                                      (let [v (nth row ci)]
                                                        (when (some? v) (str v))))
                                                arr))]))
                                       col-keys))]
                            {:cols new-cols :n-rows new-n}))))
                    {:cols existing :n-rows n-existing}
                    rows)]
        (swap! registry-atom assoc table (:cols result))))))

;; ============================================================================
;; Result formatting
;; ============================================================================

(defn- format-value
  "Format a single result value according to sqllogictest type codes."
  [v type-char]
  (cond
    (nil? v)
    "NULL"

    (= type-char \I)
    (cond
      (and (instance? Long v) (= (long v) Long/MIN_VALUE)) "NULL"
      (instance? Double v) (if (Double/isNaN (double v))
                             "NULL"
                             (str (long (double v))))
      :else (str (long v)))

    (= type-char \R)
    (cond
      (and (instance? Double v) (Double/isNaN (double v))) "NULL"
      (and (instance? Long v) (= (long v) Long/MIN_VALUE)) "NULL"
      :else (format "%.6f" (double v)))

    ;; T (text) or default
    :else
    (let [s (str v)]
      (if (or (= s "") (= s "nil"))
        "NULL"
        s))))

(defn- strip-system-keys
  "Remove system keys like :_count from result maps."
  [results]
  (mapv #(dissoc % :_count) results))

(defn- result-to-lines
  "Convert query results to sqllogictest lines (tab-separated values per row).
   type-str is the type code string (e.g. \"IIR\")."
  [results ^String type-str col-keys]
  (vec
   (map
    (fn [row]
      (str/join "\t"
                (map-indexed
                 (fn [i col-key]
                   (let [v (get row col-key)
                         tc (if (< i (count type-str))
                              (.charAt type-str i)
                              \T)]
                     (format-value v tc)))
                 col-keys)))
    results)))

;; ============================================================================
;; Test file parser
;; ============================================================================

(defn- parse-test-file
  "Parse a .test file into a sequence of records."
  [^String content]
  (let [lines (vec (str/split-lines content))
        n (count lines)]
    (loop [i (int 0) records []]
      (if (>= i n)
        records
        (let [line (str/trim (nth lines i))]
          (cond
            ;; Empty or comment
            (or (str/blank? line) (.startsWith line "#"))
            (recur (inc i) records)

            ;; PRAGMA / SET — skip
            (or (.startsWith (.toUpperCase line) "PRAGMA")
                (re-matches #"(?i)^SET\s+.*" line))
            (recur (inc i) records)

            ;; statement ok
            (.startsWith line "statement ok")
            (let [[sql-lines next-i]
                  (loop [j (int (inc i)) sql []]
                    (if (or (>= j n) (str/blank? (nth lines j)))
                      [sql j]
                      (recur (int (inc j)) (conj sql (nth lines j)))))]
              (recur (int next-i)
                     (conj records {:type :statement
                                    :expect :ok
                                    :sql (str/join " " sql-lines)})))

            ;; statement error
            (.startsWith line "statement error")
            (let [[sql-lines next-i]
                  (loop [j (int (inc i)) sql []]
                    (if (or (>= j n) (str/blank? (nth lines j)))
                      [sql j]
                      (recur (int (inc j)) (conj sql (nth lines j)))))]
              (recur (int next-i)
                     (conj records {:type :statement
                                    :expect :error
                                    :sql (str/join " " sql-lines)})))

            ;; query <types> [nosort|rowsort|valuesort] [label]
            (re-matches #"query\s+\S+.*" line)
            (let [parts (str/split line #"\s+")
                  types (second parts)
                  sort-mode (or (nth parts 2 nil) "nosort")
                  ;; Collect SQL until ----
                  [sql-lines sep-i]
                  (loop [j (int (inc i)) sql []]
                    (cond
                      (>= j n)
                      [sql j]
                      (= "----" (str/trim (nth lines j)))
                      [sql (inc j)]
                      :else
                      (recur (int (inc j)) (conj sql (nth lines j)))))
                  ;; Collect expected results until blank line or EOF
                  [expected-lines next-i]
                  (loop [j (int sep-i) exp []]
                    (if (or (>= j n)
                            (str/blank? (nth lines j)))
                      [exp j]
                      (recur (int (inc j)) (conj exp (str/trim (nth lines j))))))]
              (recur (int next-i)
                     (conj records {:type :query
                                    :types types
                                    :sort sort-mode
                                    :sql (str/join " " sql-lines)
                                    :expected expected-lines})))

            ;; Skip unknown directives (halt, loop, etc.)
            :else
            (recur (inc i) records)))))))

;; ============================================================================
;; Record execution
;; ============================================================================

(defn- execute-record!
  "Execute a single test record against the registry. Returns nil on success,
   or a failure message string on error."
  [registry-atom record]
  (let [{:keys [type expect sql]} record]
    (case type
      :statement
      (let [parsed (sql/parse-sql sql @registry-atom)]
        (case expect
          :ok
          (cond
            (:ddl parsed)
            (do (execute-ddl! registry-atom (:ddl parsed)) nil)

            (:query parsed)
            (try
              (q/q (:query parsed))
              nil
              (catch Exception e
                (str "Statement expected ok but got execution error: " (.getMessage e)
                     "\n  SQL: " sql)))

            (:system parsed)
            nil

            (:error parsed)
            (str "Expected statement ok but got error: " (:error parsed)
                 "\n  SQL: " sql))

          :error
          (cond
            (:error parsed) nil
            (:ddl parsed)
            (try
              (execute-ddl! registry-atom (:ddl parsed))
              (str "Expected statement error but succeeded\n  SQL: " sql)
              (catch Exception _e nil))
            (:query parsed)
            (try
              (q/q (:query parsed))
              (str "Expected statement error but succeeded\n  SQL: " sql)
              (catch Exception _e nil))
            :else nil)))

      :query
      (let [parsed (sql/parse-sql sql @registry-atom)]
        (cond
          (:error parsed)
          (str "Query parse error: " (:error parsed) "\n  SQL: " sql)

          (:query parsed)
          (try
            (let [results (strip-system-keys (q/q (:query parsed)))
                  col-keys (when (seq results)
                             (vec (keys (first results))))
                  actual-lines (result-to-lines results (:types record)
                                                (or col-keys []))
                  expected (:expected record)
                  [actual-sorted expected-sorted]
                  (case (:sort record)
                    ("rowsort" "valuesort")
                    [(sort actual-lines) (sort expected)]
                    [actual-lines expected])]
              (when (not= (vec actual-sorted) (vec expected-sorted))
                (str "Result mismatch\n  SQL: " sql
                     "\n  Expected: " (pr-str (vec expected-sorted))
                     "\n  Actual:   " (pr-str (vec actual-sorted)))))
            (catch Exception e
              (str "Query execution error: " (.getMessage e) "\n  SQL: " sql)))

          (:system parsed)
          nil

          :else
          (str "Unexpected parse result for query\n  SQL: " sql))))))

;; ============================================================================
;; Test file runner
;; ============================================================================

(defn- run-test-file
  "Run all records in a .test file. Reports each failure via clojure.test/is."
  [^String path]
  (let [content (slurp path)
        records (parse-test-file content)
        registry (atom {})]
    (doseq [record records]
      (let [msg (execute-record! registry record)]
        (is (nil? msg) (or msg ""))))))

;; ============================================================================
;; Test entry point
;; ============================================================================

(deftest sqllogictest-suite
  (let [test-dir (io/file "test/sqllogictest")]
    (when (.isDirectory test-dir)
      (doseq [f (sort-by #(.getName ^java.io.File %) (file-seq test-dir))]
        (when (.endsWith (.getName ^java.io.File f) ".test")
          (testing (.getName ^java.io.File f)
            (run-test-file (.getPath ^java.io.File f))))))))
