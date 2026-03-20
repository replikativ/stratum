(ns stratum.query.expression
  "Vectorized and scalar expression evaluation for the query engine.

   Exports:
     *columns-meta*       — dynamic var bound during query execution for dict access
     long-array?          — type predicate
     double-array?        — type predicate
     string-array?        — type predicate
     string-producing-expr? — true if expression returns a dict-encoded string column
     ensure-longs         — get column data as long[], converting double[] if needed
     col-as-doubles-cached — get column as double[] with conversion cache
     eval-case-pred-mask  — evaluate a CASE/WHEN predicate to a long[] mask
     eval-string-expr     — evaluate a string expression to a dict-encoded column entry
     eval-expr-vectorized — evaluate an expression tree on whole arrays (vectorized)
     eval-expr-to-long    — eval returning long[] directly (avoids round-trip)
     materialize-string-exprs — pre-materialize string expressions to temp columns"
  (:require [stratum.query.normalization :as norm])
  (:import [stratum.internal ColumnOps ColumnOpsExt ColumnOpsLong]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Array type helpers
;; ============================================================================

(def ^Class long-array-class   (Class/forName "[J"))
(def ^Class double-array-class (Class/forName "[D"))
(def ^Class string-array-class (Class/forName "[Ljava.lang.String;"))
(def ^Class object-array-class (Class/forName "[Ljava.lang.Object;"))

(defn long-array?   [x] (instance? long-array-class x))
(defn double-array? [x] (instance? double-array-class x))
(defn string-array? [x] (instance? string-array-class x))

;; ============================================================================
;; Dynamic column metadata (for string function dict access during eval)
;; ============================================================================

(def ^:dynamic *columns-meta*
  "Map of column-keyword → column-info, bound during query execution.
   Provides dict metadata to eval-expr-vectorized and eval-agg-expr for
   string operations (LENGTH, CAST to string, etc.)."
  nil)

;; ============================================================================
;; String Expression Materialization
;; ============================================================================

(def string-transform-ops
  "Expression ops that produce string (dict-encoded) columns."
  #{:upper :lower :substr :replace :trim :concat})

(def cast-to-string-types
  "CAST target types that produce string columns."
  #{:string})

(defn string-producing-expr?
  "Returns true if the normalized expression produces a string (dict-encoded) result."
  [expr]
  (and (map? expr)
       (or (string-transform-ops (:op expr))
           (and (= :cast (:op expr)) (cast-to-string-types (second (:args expr))))
           (and (= :case (:op expr))
                (some #(string? (:val %)) (:branches expr))))))

;; ============================================================================
;; Column conversion helpers
;; ============================================================================

(defn ensure-longs
  "Get a column's data as long[], converting double[] if needed."
  ^longs [col-data ^long length]
  (if (long-array? col-data)
    col-data
    (let [la (long-array length)]
      (dotimes [i length] (aset la i (long (aget ^doubles col-data i))))
      la)))

(defn col-as-doubles-cached
  "Get column as double[], using local cache to avoid duplicate longToDouble conversions
   within a single query execution."
  ^doubles [col-data ^long length ^java.util.HashMap cache]
  (if (double-array? col-data)
    col-data
    (if (and cache (.containsKey cache col-data))
      (.get cache col-data)
      (let [result (ColumnOps/longToDoubleNullSafe ^longs col-data (int length))]
        (when cache (.put cache col-data result))
        result))))

;; ============================================================================
;; Type coercion helpers
;; ============================================================================

(defn ensure-double-arr
  "Ensure result is double[], converting long[] if needed. Used as adapter
   between polymorphic (long[]/double[]) and double[]-only callers."
  ^doubles [arr ^long length]
  (if (double-array? arr)
    arr
    (ColumnOps/longToDoubleNullSafe ^longs arr (int length))))

;; ============================================================================
;; Mutual recursion forward declaration
;; ============================================================================

(declare eval-expr-vectorized)
(declare eval-expr-polymorphic)

;; ============================================================================
;; CASE/WHEN predicate mask evaluation
;; ============================================================================

(defn eval-case-pred-mask
  "Evaluate a CASE/WHEN predicate expression to a long[] mask (1=true, 0=false).
   Predicate can be a comparison like [:= :col val] or an expression."
  ^longs [pred-expr col-arrays ^long length ^java.util.HashMap cache]
  (let [pred (norm/normalize-pred pred-expr)
        col-ref (first pred)
        op (second pred)
        args (subvec pred 2)
        col-data (if (keyword? col-ref)
                   (col-as-doubles-cached (get col-arrays col-ref) length cache)
                   (eval-expr-vectorized col-ref col-arrays length cache))
        mask (long-array length)]
    (dotimes [i length]
      (let [v (aget ^doubles col-data i)]
        (aset mask i
              (long (if (case op
                          :lt  (< v (double (first args)))
                          :gt  (> v (double (first args)))
                          :lte (<= v (double (first args)))
                          :gte (>= v (double (first args)))
                          :eq  (== v (double (first args)))
                          :neq (not (== v (double (first args))))
                          :range (and (>= v (double (first args))) (< v (double (second args))))
                          false)
                      1 0)))))
    mask))

;; ============================================================================
;; String expression evaluation (produces dict-encoded columns)
;; ============================================================================

(defn eval-string-expr
  "Evaluate a string expression into a dict-encoded column entry.
   Returns {:type :int64 :data long[] :dict String[] :dict-type :string}."
  [expr columns ^long length]
  (let [args (:args expr)]
    (case (:op expr)
      :upper
      (let [col-key (first args)
            col-info (get columns col-key)
            ^longs codes (:data col-info)
            ^"[Ljava.lang.String;" dict (:dict col-info)
            ^objects result (ColumnOps/arrayStringUpper codes dict (int length))]
        {:type :int64 :data (aget result 0) :dict (aget result 1) :dict-type :string})

      :lower
      (let [col-key (first args)
            col-info (get columns col-key)
            ^longs codes (:data col-info)
            ^"[Ljava.lang.String;" dict (:dict col-info)
            ^objects result (ColumnOps/arrayStringLower codes dict (int length))]
        {:type :int64 :data (aget result 0) :dict (aget result 1) :dict-type :string})

      :substr
      (let [col-key (first args)
            start (int (second args))
            len (int (if (>= (count args) 3) (nth args 2) -1))
            col-info (get columns col-key)
            ^longs codes (:data col-info)
            ^"[Ljava.lang.String;" dict (:dict col-info)
            ^objects result (ColumnOps/arrayStringSubstr codes dict start len (int length))]
        {:type :int64 :data (aget result 0) :dict (aget result 1) :dict-type :string})

      :replace
      (let [col-key (first args)
            target (str (second args))
            replacement (str (nth args 2))
            col-info (get columns col-key)
            ^longs codes (:data col-info)
            ^"[Ljava.lang.String;" dict (:dict col-info)
            ^objects result (ColumnOps/arrayStringReplace codes dict target replacement (int length))]
        {:type :int64 :data (aget result 0) :dict (aget result 1) :dict-type :string})

      :trim
      (let [col-key (first args)
            col-info (get columns col-key)
            ^longs codes (:data col-info)
            ^"[Ljava.lang.String;" dict (:dict col-info)
            ^objects result (ColumnOps/arrayStringTrim codes dict (int length))]
        {:type :int64 :data (aget result 0) :dict (aget result 1) :dict-type :string})

      :concat
      (let [col-key1 (first args)
            arg2 (second args)
            col-info1 (get columns col-key1)]
        (if (string? arg2)
          ;; CONCAT column with scalar string
          (let [^longs codes1 (:data col-info1)
                ^"[Ljava.lang.String;" dict1 (:dict col-info1)
                ^objects result (ColumnOps/arrayStringConcatScalar codes1 dict1 (str arg2) false (int length))]
            {:type :int64 :data (aget result 0) :dict (aget result 1) :dict-type :string})
          ;; CONCAT two columns
          (let [col-key2 arg2
                col-info2 (get columns col-key2)
                ^longs codes1 (:data col-info1)
                ^"[Ljava.lang.String;" dict1 (:dict col-info1)
                ^longs codes2 (:data col-info2)
                ^"[Ljava.lang.String;" dict2 (:dict col-info2)
                ^objects result (ColumnOps/arrayStringConcat codes1 dict1 codes2 dict2 (int length))]
            {:type :int64 :data (aget result 0) :dict (aget result 1) :dict-type :string})))

      :cast
      (let [source-key (first args)
            target-type (second args)
            col-info (get columns source-key)]
        (case target-type
          :string
          (cond
            ;; Already dict-encoded string
            (and (:dict col-info) (= :string (:dict-type col-info)))
            col-info
            ;; long[] → string
            (= :int64 (:type col-info))
            (let [^objects result (ColumnOps/arrayLongToString ^longs (:data col-info) (int length))]
              {:type :int64 :data (aget result 0) :dict (aget result 1) :dict-type :string})
            ;; double[] → string
            :else
            (let [^objects result (ColumnOps/arrayDoubleToString ^doubles (:data col-info) (int length))]
              {:type :int64 :data (aget result 0) :dict (aget result 1) :dict-type :string}))))

      :case
      (let [branches (:branches expr)
            ;; Collect string literals and nested expression results
            ;; First pass: evaluate nested expressions to get their dicts
            col-arrays (into {} (map (fn [[k v]] [k (:data v)])) columns)
            cache (java.util.HashMap.)
            ;; Evaluate nested expression branches and collect all strings
            branch-results (mapv (fn [branch]
                                   (let [v (:val branch)]
                                     (if (string? v)
                                       {:string v}
                                       ;; Nested expression (e.g. nested CASE) — evaluate recursively
                                       {:nested (eval-string-expr v columns length)})))
                                 branches)
            ;; Build unified dictionary from all string literals + nested dicts
            all-dict-entries (java.util.ArrayList.)
            _ (doseq [br branch-results]
                (if (:string br)
                  (.add all-dict-entries (:string br))
                  (let [^"[Ljava.lang.String;" d (:dict (:nested br))]
                    (dotimes [j (alength d)]
                      (.add all-dict-entries (aget d j))))))
            distinct-strings (vec (distinct all-dict-entries))
            dict (into-array String distinct-strings)
            dict-map (into {} (map-indexed (fn [i s] [s (long i)])) distinct-strings)
            codes (long-array length Long/MIN_VALUE)
            assigned (long-array length)]
        ;; Process each branch
        (dotimes [bi (count branches)]
          (let [branch (nth branches bi)
                br-result (nth branch-results bi)]
            (if (= :else (:op branch))
              ;; ELSE: assign to all unassigned rows
              (if (:string br-result)
                (let [code (long (get dict-map (:string br-result) Long/MIN_VALUE))]
                  (dotimes [i length]
                    (when (zero? (aget assigned i))
                      (aset codes i code)
                      (aset assigned i 1))))
                ;; Nested expression result for ELSE
                (let [nested (:nested br-result)
                      ^longs nested-codes (:data nested)
                      ^"[Ljava.lang.String;" nested-dict (:dict nested)]
                  (dotimes [i length]
                    (when (zero? (aget assigned i))
                      (let [nc (aget nested-codes i)]
                        (if (= nc Long/MIN_VALUE)
                          (aset codes i Long/MIN_VALUE)
                          (aset codes i (long (get dict-map (aget nested-dict (int nc)) Long/MIN_VALUE)))))
                      (aset assigned i 1)))))
              ;; Conditional: evaluate predicate mask, assign matching unassigned rows
              (let [mask (eval-case-pred-mask (:pred branch) col-arrays length cache)]
                (if (:string br-result)
                  (let [code (long (get dict-map (:string br-result) Long/MIN_VALUE))]
                    (dotimes [i length]
                      (when (and (zero? (aget assigned i)) (== 1 (aget ^longs mask i)))
                        (aset codes i code)
                        (aset assigned i 1))))
                  ;; Nested expression result for WHEN
                  (let [nested (:nested br-result)
                        ^longs nested-codes (:data nested)
                        ^"[Ljava.lang.String;" nested-dict (:dict nested)]
                    (dotimes [i length]
                      (when (and (zero? (aget assigned i)) (== 1 (aget ^longs mask i)))
                        (let [nc (aget nested-codes i)]
                          (if (= nc Long/MIN_VALUE)
                            (aset codes i Long/MIN_VALUE)
                            (aset codes i (long (get dict-map (aget nested-dict (int nc)) Long/MIN_VALUE)))))
                        (aset assigned i 1)))))))))
        {:type :int64 :data codes :dict dict :dict-type :string}))))

;; ============================================================================
;; Date helper functions (shared by eval-expr-vectorized and eval-expr-to-long)
;; ============================================================================

(defn- eval-date-trunc-to-long
  "Evaluate date-trunc returning long[] directly."
  ^longs [unit col-data ^long length]
  (let [^longs long-data (ensure-longs col-data length)]
    (case unit
      :year   (ColumnOps/arrayDateTruncYear long-data (int length))
      :month  (ColumnOps/arrayDateTruncMonth long-data (int length))
      :day    (ColumnOps/arrayDateTruncDay long-data (int length))
      :hour   (ColumnOps/arrayDateTruncHour long-data (int length))
      :minute (ColumnOps/arrayDateTruncMinute long-data (int length)))))

(defn- eval-date-add-to-long
  "Evaluate date-add returning long[] directly."
  ^longs [unit n col-data ^long length]
  (let [^longs long-data (ensure-longs col-data length)]
    (case unit
      :days    (ColumnOps/arrayDateAddDays long-data (long n) (int length))
      :hours   (ColumnOps/arrayDateAddSeconds long-data (long (* n 3600)) (int length))
      :minutes (ColumnOps/arrayDateAddSeconds long-data (long (* n 60)) (int length))
      :seconds (ColumnOps/arrayDateAddSeconds long-data (long n) (int length))
      :months  (ColumnOps/arrayDateAddMonths long-data (int n) (int length))
      :years   (ColumnOps/arrayDateAddMonths long-data (int (* n 12)) (int length)))))

;; ============================================================================
;; Polymorphic expression evaluation (returns long[] or double[])
;; ============================================================================

(defn eval-expr-polymorphic
  "Evaluate an expression tree returning the natural array type: long[] for
   integer arithmetic, long[] for date ops, double[] for float/division/math.
   Callers must handle both return types. Use eval-expr-vectorized for
   guaranteed double[] return."
  ([expr col-arrays ^long length]
   (eval-expr-polymorphic expr col-arrays length nil))
  ([expr col-arrays ^long length ^java.util.HashMap cache]
   (cond
     (nil? expr)
     (let [a (double-array length)]
       (java.util.Arrays/fill a Double/NaN)
       a)

     (keyword? expr)
     ;; Return raw array (long[] or double[]) — no conversion
     (get col-arrays expr)

     (number? expr)
     ;; Integer literal → long[], float literal → double[]
     (if (integer? expr)
       (ColumnOpsLong/arrayBroadcastLong (long expr) (int length))
       (ColumnOps/arrayBroadcast (double expr) (int length)))

     ;; Unary math functions
     (and (map? expr) (#{:abs :sqrt :log :log10 :exp :round :floor :ceil :sign} (:op expr)))
     (let [op (:op expr)
           arg-result (eval-expr-polymorphic (first (:args expr)) col-arrays length cache)]
       (if (and (#{:abs :sign} op) (long-array? arg-result))
         ;; Integer-preserving unary ops
         (case op
           :abs  (ColumnOpsLong/arrayAbsLong ^longs arg-result (int length))
           :sign (ColumnOpsLong/arraySignLong ^longs arg-result (int length)))
         ;; Fractional-producing or double input → double path
         (let [^doubles a (ensure-double-arr arg-result length)]
           (case op
             :abs   (ColumnOps/arrayAbs a (int length))
             :sqrt  (ColumnOps/arraySqrt a (int length))
             :log   (ColumnOps/arrayLog a (int length))
             :log10 (ColumnOps/arrayLog10 a (int length))
             :exp   (ColumnOps/arrayExp a (int length))
             :round (ColumnOps/arrayRound a (int length))
             :floor (ColumnOps/arrayFloor a (int length))
             :ceil  (ColumnOps/arrayCeil a (int length))
             :sign  (ColumnOps/arraySign a (int length))))))

     ;; Date extraction functions — return double[] (extract produces small ints)
     (and (map? expr) (#{:year :month :day :hour :minute :second :day-of-week :week-of-year} (:op expr)))
     (let [col-key (first (:args expr))
           col-data (if (keyword? col-key)
                      (get col-arrays col-key)
                      (throw (ex-info "Date extraction requires column keyword" {:expr expr})))]
       (if (long-array? col-data)
         (case (:op expr)
           :year         (ColumnOps/arrayExtractYear ^longs col-data (int length))
           :month        (ColumnOps/arrayExtractMonth ^longs col-data (int length))
           :day          (ColumnOps/arrayExtractDay ^longs col-data (int length))
           :hour         (ColumnOps/arrayExtractHour ^longs col-data (int length))
           :minute       (ColumnOps/arrayExtractMinute ^longs col-data (int length))
           :second       (ColumnOps/arrayExtractSecond ^longs col-data (int length))
           :day-of-week  (ColumnOps/arrayExtractDayOfWeek ^longs col-data (int length))
           :week-of-year (ColumnOps/arrayExtractWeekOfYear ^longs col-data (int length)))
         (let [long-data (long-array length)]
           (dotimes [i length] (aset long-data i (long (aget ^doubles col-data i))))
           (case (:op expr)
             :year         (ColumnOps/arrayExtractYear long-data (int length))
             :month        (ColumnOps/arrayExtractMonth long-data (int length))
             :day          (ColumnOps/arrayExtractDay long-data (int length))
             :hour         (ColumnOps/arrayExtractHour long-data (int length))
             :minute       (ColumnOps/arrayExtractMinute long-data (int length))
             :second       (ColumnOps/arrayExtractSecond long-data (int length))
             :day-of-week  (ColumnOps/arrayExtractDayOfWeek long-data (int length))
             :week-of-year (ColumnOps/arrayExtractWeekOfYear long-data (int length))))))

     ;; Date/time arithmetic — return long[] for date-trunc/date-add (integer epoch)
     (and (map? expr) (#{:date-trunc :date-add :date-diff :epoch-days :epoch-seconds} (:op expr)))
     (let [args (:args expr)]
       (case (:op expr)
         :date-trunc
         (let [col-key (second args)
               col-data (get col-arrays col-key)]
           ;; Return long[] directly — no longToDouble conversion
           (eval-date-trunc-to-long (first args) col-data length))

         :date-add
         (let [col-key (nth args 2)
               col-data (get col-arrays col-key)]
           ;; Return long[] directly
           (eval-date-add-to-long (first args) (second args) col-data length))

         :date-diff
         (let [unit (first args)
               col-key1 (second args)
               col-key2 (nth args 2)
               col-data1 (get col-arrays col-key1)
               col-data2 (get col-arrays col-key2)
               ^longs l1 (ensure-longs col-data1 length)
               ^longs l2 (ensure-longs col-data2 length)]
           (case unit
             :days    (ColumnOps/arrayDateDiffDays l1 l2 (int length))
             :seconds (ColumnOps/arrayDateDiffSeconds l1 l2 (int length))))

         :epoch-days
         (let [col-key (first args)
               col-data (get col-arrays col-key)
               ^longs long-data (ensure-longs col-data length)
               r (long-array length)]
           (dotimes [i length] (aset r i (quot (aget long-data i) 86400)))
           r)

         :epoch-seconds
         (let [col-key (first args)
               col-data (get col-arrays col-key)
               ^longs long-data (ensure-longs col-data length)
               r (long-array length)]
           (dotimes [i length] (aset r i (* (aget long-data i) 86400)))
           r)))

     ;; NULL handling expressions — stay in double domain (complex sentinel logic)
     (and (map? expr) (#{:greatest :least} (:op expr)))
     (let [args (:args expr)
           op (:op expr)]
       (reduce (fn [^doubles acc arg]
                 (let [^doubles b (if (number? arg)
                                    (let [v (double arg)
                                          a (double-array length)]
                                      (java.util.Arrays/fill a v)
                                      a)
                                    (ensure-double-arr (eval-expr-polymorphic arg col-arrays length cache) length))
                       ^doubles r (double-array length)]
                   (if (= op :greatest)
                     (dotimes [i length] (aset r i (Math/max (aget acc i) (aget b i))))
                     (dotimes [i length] (aset r i (Math/min (aget acc i) (aget b i)))))
                   r))
               (let [first-arg (first args)]
                 (if (number? first-arg)
                   (let [v (double first-arg)
                         a (double-array length)]
                     (java.util.Arrays/fill a v)
                     a)
                   (ensure-double-arr (eval-expr-polymorphic first-arg col-arrays length cache) length)))
               (rest args)))

     (and (map? expr) (#{:coalesce :nullif} (:op expr)))
     (let [args (:args expr)]
       (case (:op expr)
         :coalesce
         (let [a-raw (first args)
               b-raw (second args)
               a-is-long? (and (keyword? a-raw) (long-array? (get col-arrays a-raw)))]
           (if a-is-long?
             (let [^longs a-data (get col-arrays a-raw)
                   ^longs result (if (number? b-raw)
                                   (ColumnOps/arrayCoalesceLongScalar a-data (long b-raw) (int length))
                                   (if (and (keyword? b-raw) (long-array? (get col-arrays b-raw)))
                                     (ColumnOps/arrayCoalesceLong a-data ^longs (get col-arrays b-raw) (int length))
                                     nil))]
               (if result
                 (ColumnOps/longToDouble result (int length))
                 (let [a (col-as-doubles-cached a-data length cache)]
                   (if (number? b-raw)
                     (ColumnOps/arrayCoalesceScalar a (double b-raw) (int length))
                     (ColumnOps/arrayCoalesce a (ensure-double-arr (eval-expr-polymorphic b-raw col-arrays length cache) length) (int length))))))
             (let [a (ensure-double-arr (eval-expr-polymorphic a-raw col-arrays length cache) length)]
               (if (number? b-raw)
                 (ColumnOps/arrayCoalesceScalar a (double b-raw) (int length))
                 (ColumnOps/arrayCoalesce a (ensure-double-arr (eval-expr-polymorphic b-raw col-arrays length cache) length) (int length))))))
         :nullif
         (let [a (ensure-double-arr (eval-expr-polymorphic (first args) col-arrays length cache) length)
               val (double (second args))]
           (ColumnOps/arrayNullif a val (int length)))))

     ;; String functions — always double[]
     (and (map? expr) (#{:length :upper :lower :substr :replace :trim :concat} (:op expr)))
     (let [col-key (first (:args expr))]
       (case (:op expr)
         :length
         (let [raw-col (get col-arrays col-key)
               col-meta (when *columns-meta* (get *columns-meta* col-key))]
           (cond
             (and (long-array? raw-col) col-meta (:dict col-meta))
             (ColumnOps/arrayStringLength ^longs raw-col ^"[Ljava.lang.String;" (:dict col-meta) (int length))
             (long-array? raw-col)
             (col-as-doubles-cached raw-col length cache)
             :else (col-as-doubles-cached raw-col length cache)))
         (:upper :lower :substr :replace :trim :concat)
         (throw (ex-info "String transform expressions must be pre-materialized" {:expr expr}))))

     ;; CAST expression
     (and (map? expr) (= :cast (:op expr)))
     (let [args (:args expr)
           source-key (first args)
           target-type (second args)
           col-data (get col-arrays source-key)
           col-meta (when *columns-meta* (get *columns-meta* source-key))]
       (case target-type
         :double
         (cond
           (long-array? col-data)
           (if (and col-meta (:dict col-meta) (= :string (:dict-type col-meta)))
             (ColumnOps/arrayStringToDouble ^longs col-data ^"[Ljava.lang.String;" (:dict col-meta) (int length))
             (ColumnOps/arrayLongToDouble ^longs col-data (int length)))
           :else col-data)
         :long
         (cond
           (long-array? col-data)
           (if (and col-meta (:dict col-meta) (= :string (:dict-type col-meta)))
             (let [^longs la (ColumnOps/arrayStringToLong ^longs col-data ^"[Ljava.lang.String;" (:dict col-meta) (int length))]
               (ColumnOps/arrayLongToDouble la (int length)))
             (col-as-doubles-cached col-data length cache))
           :else
           (let [^longs la (ColumnOps/arrayDoubleToLong ^doubles col-data (int length))]
             (ColumnOps/arrayLongToDouble la (int length))))
         :string
         (throw (ex-info "CAST to string must be pre-materialized" {:expr expr}))))

     ;; CASE/WHEN expression — stay double (complex branch mixing)
     (and (map? expr) (= :case (:op expr)))
     (let [branches (:branches expr)
           has-else? (some #(= :else (:op %)) branches)
           result (double-array length)
           _ (java.util.Arrays/fill result (if has-else? 0.0 Double/NaN))
           assigned (long-array length)]
       (doseq [branch branches]
         (if (= :else (:op branch))
           (let [^doubles val-arr (ensure-double-arr (eval-expr-polymorphic (:val branch) col-arrays length cache) length)]
             (dotimes [i length]
               (when (zero? (aget assigned i))
                 (aset result i (aget val-arr i)))))
           (let [mask (eval-case-pred-mask (:pred branch) col-arrays length cache)
                 ^doubles val-arr (ensure-double-arr (eval-expr-polymorphic (:val branch) col-arrays length cache) length)]
             (dotimes [i length]
               (when (and (zero? (aget ^longs assigned i)) (== 1 (aget ^longs mask i)))
                 (aset result i (aget val-arr i))
                 (aset assigned i 1))))))
       result)

     ;; Binary arithmetic ops — type-preserving when both sides are long[]
     (map? expr)
     (let [args (:args expr)
           arg0 (nth args 0)
           arg1 (nth args 1)
           scalar0? (number? arg0)
           scalar1? (number? arg1)
           op (:op expr)]
       ;; Division, pow always produce double[]
       (if (#{:div :pow} op)
         (let [a (ensure-double-arr (eval-expr-polymorphic arg0 col-arrays length cache) length)
               b (ensure-double-arr (eval-expr-polymorphic arg1 col-arrays length cache) length)]
           (case op
             :div (ColumnOps/arrayDiv a b (int length))
             :pow (if (number? arg1)
                    (ColumnOps/arrayPowScalar a (double arg1) (int length))
                    (ColumnOps/arrayPow a b (int length)))))
         ;; mul, add, sub, mod — try long[] path
         (let [;; Evaluate operands polymorphically
               r0 (if scalar0? nil (eval-expr-polymorphic arg0 col-arrays length cache))
               r1 (if scalar1? nil (eval-expr-polymorphic arg1 col-arrays length cache))
               ;; Check if we can stay in long domain
               both-long? (and (or scalar0? (long-array? r0))
                               (or scalar1? (long-array? r1))
                               ;; Scalars must be integer
                               (or (not scalar0?) (integer? arg0))
                               (or (not scalar1?) (integer? arg1)))]
           (if both-long?
             ;; Long path
             (case op
               :mul (cond
                      scalar0? (ColumnOpsLong/arrayMulLongScalar (long arg0) ^longs r1 (int length))
                      scalar1? (ColumnOpsLong/arrayMulLongScalar (long arg1) ^longs r0 (int length))
                      :else (ColumnOpsLong/arrayMulLong ^longs r0 ^longs r1 (int length)))
               :add (cond
                      scalar0? (ColumnOpsLong/arrayAddLongScalar (long arg0) ^longs r1 (int length))
                      scalar1? (ColumnOpsLong/arrayAddLongScalar (long arg1) ^longs r0 (int length))
                      :else (ColumnOpsLong/arrayAddLong ^longs r0 ^longs r1 (int length)))
               :sub (cond
                      scalar0? (ColumnOpsLong/arraySubLongScalar (long arg0) ^longs r1 (int length))
                      :else (ColumnOpsLong/arraySubLong ^longs r0 ^longs r1 (int length)))
               :mod (cond
                      scalar1? (ColumnOpsLong/arrayModLongScalar ^longs r0 (long arg1) (int length))
                      :else (ColumnOpsLong/arrayModLong ^longs r0 ^longs r1 (int length))))
             ;; Double path (mixed types or float scalars)
             (let [a (if scalar0? nil (ensure-double-arr r0 length))
                   b (if scalar1? nil (ensure-double-arr r1 length))]
               (case op
                 :mul (cond
                        scalar0? (ColumnOps/arrayMulScalar (double arg0) b (int length))
                        scalar1? (ColumnOps/arrayMulScalar (double arg1) a (int length))
                        :else (ColumnOps/arrayMul a b (int length)))
                 :add (cond
                        scalar0? (ColumnOps/arrayAddScalar (double arg0) b (int length))
                        scalar1? (ColumnOps/arrayAddScalar (double arg1) a (int length))
                        :else (ColumnOps/arrayAdd a b (int length)))
                 :sub (cond
                        scalar0? (ColumnOps/arraySubScalar (double arg0) b (int length))
                        :else (ColumnOps/arraySub a b (int length)))
                 :mod (if (number? arg1)
                        (ColumnOps/arrayModScalar a (double arg1) (int length))
                        (ColumnOps/arrayMod a b (int length)))))))))

     :else
     (throw (ex-info (str "Unsupported vectorized expr: " (type expr) " " expr) {:expr expr})))))

;; ============================================================================
;; Vectorized expression evaluation (guaranteed double[] return)
;; ============================================================================

(defn eval-expr-vectorized
  "Evaluate an expression tree on entire arrays at once using Java array ops.
   Returns a double[] of length rows. Wrapper around eval-expr-polymorphic
   that ensures double[] return type for backward compatibility.
   Optimizes scalar-array ops to avoid 48MB broadcast allocations.
   Optional cache parameter avoids duplicate longToDouble conversions.
   Bind *columns-meta* for string function dict access."
  (^doubles [expr col-arrays ^long length]
   (eval-expr-vectorized expr col-arrays length nil))
  (^doubles [expr col-arrays ^long length ^java.util.HashMap cache]
   (ensure-double-arr (eval-expr-polymorphic expr col-arrays length cache) length)))

;; ============================================================================
;; Long-returning expression evaluation (avoids double round-trip)
;; ============================================================================

(defn eval-expr-to-long
  "Evaluate an expression returning long[] directly. For date-trunc/date-add
   this avoids wasteful long[]→double[]→long[] round-trip through eval-expr-vectorized.
   For other expressions, falls back to eval-expr-vectorized + double→long conversion."
  ^longs [expr col-arrays ^long length ^java.util.HashMap cache]
  (if-not (map? expr)
    ;; keyword or number — fall back
    (let [result-arr (eval-expr-vectorized expr col-arrays length cache)
          la (long-array length)]
      (dotimes [i length] (aset la i (long (aget ^doubles result-arr i))))
      la)
    (let [args (:args expr)]
      (case (:op expr)
        :date-trunc
        (let [col-key (second args)
              col-data (get col-arrays col-key)]
          (eval-date-trunc-to-long (first args) col-data length))

        :date-add
        (let [col-key (nth args 2)
              col-data (get col-arrays col-key)]
          (eval-date-add-to-long (first args) (second args) col-data length))

        ;; Extract operations — return long[] directly, skip double[] round-trip
        (#{:year :month :day :hour :minute :second :day-of-week :week-of-year} (:op expr))
        (let [col-key (first args)
              col-data (get col-arrays col-key)
              ^longs long-data (ensure-longs col-data length)]
          (case (:op expr)
            :year         (ColumnOpsExt/arrayExtractYearLong long-data (int length))
            :month        (ColumnOpsExt/arrayExtractMonthLong long-data (int length))
            :day          (ColumnOpsExt/arrayExtractDayLong long-data (int length))
            :hour         (ColumnOpsExt/arrayExtractHourLong long-data (int length))
            :minute       (ColumnOpsExt/arrayExtractMinuteLong long-data (int length))
            :second       (ColumnOpsExt/arrayExtractSecondLong long-data (int length))
            :day-of-week  (ColumnOpsExt/arrayExtractDayOfWeekLong long-data (int length))
            :week-of-year (ColumnOpsExt/arrayExtractWeekOfYearLong long-data (int length))))

        ;; Fall back to eval-expr-vectorized + double→long conversion
        (let [result-arr (eval-expr-vectorized expr col-arrays length cache)
              la (long-array length)]
          (dotimes [i length] (aset la i (long (aget ^doubles result-arr i))))
          la)))))

;; ============================================================================
;; String expression pre-materialization
;; ============================================================================

(defn materialize-string-exprs
  "Pre-materialize string-producing expressions in group/agg/select into
   dict-encoded temp columns. Returns [updated-group updated-aggs updated-select updated-columns].
   Non-string expressions and plain keywords are left unchanged."
  [group aggs select columns length]
  (let [counter (atom 0)
        cols (atom columns)
        materialize! (fn [expr-raw]
                       (let [expr (norm/normalize-expr (vec expr-raw))]
                         (if (string-producing-expr? expr)
                           (let [n (swap! counter inc)
                                 col-name (keyword (str "__str_expr_" n))
                                 col-entry (eval-string-expr expr @cols length)]
                             (swap! cols assoc col-name col-entry)
                             col-name)
                           expr-raw)))
        ;; Process group-by columns
        new-group (mapv (fn [g] (if (keyword? g) g (materialize! g))) group)
        ;; Process agg sources that are expressions (already normalized)
        new-aggs (mapv (fn [agg]
                         (if-let [expr (:expr agg)]
                           (if (string-producing-expr? expr)
                             (let [n (swap! counter inc)
                                   col-name (keyword (str "__str_expr_" n))
                                   col-entry (eval-string-expr expr @cols length)]
                               (swap! cols assoc col-name col-entry)
                               (-> agg (dissoc :expr) (assoc :col col-name)))
                             agg)
                           agg))
                       aggs)]
    [new-group new-aggs select @cols]))
