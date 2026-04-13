(ns stratum.sql
  "SQL → Stratum query map translator.

   Parses SQL strings using JSqlParser and translates the AST into Stratum
   query maps that can be executed by `stratum.query/execute`.

   Supported statements: SELECT, INSERT, UPDATE, DELETE, UPSERT (INSERT ON CONFLICT),
   UPDATE FROM (joined updates), CREATE TABLE, DROP TABLE, EXPLAIN.

   Main entry points:
     (parse-sql sql table-registry)  → {:query {...}} or {:ddl {...}} or {:error \"...\"}
     (format-results result columns) → QueryResult for pgwire"
  (:require [clojure.string :as str]
            [stratum.query :as q]
            [stratum.csv :as csv]
            [stratum.parquet :as parquet])
  (:import [net.sf.jsqlparser.parser CCJSqlParserUtil]
           [net.sf.jsqlparser.statement Statement]
           [net.sf.jsqlparser.statement.select
            PlainSelect SelectItem AllColumns AllTableColumns
            OrderByElement GroupByElement Limit Offset Join]
           [net.sf.jsqlparser.statement.select Distinct]
           [net.sf.jsqlparser.schema Column Table]
           [net.sf.jsqlparser.expression
            Alias Function LongValue DoubleValue StringValue NullValue
            Parenthesis NotExpression CaseExpression WhenClause SignedExpression
            CastExpression AnalyticExpression IntervalExpression
            WindowElement WindowElement$Type WindowOffset WindowOffset$Type WindowRange]
           [net.sf.jsqlparser.statement.select
            WithItem ParenthesedSelect SetOperationList UnionOp IntersectOp ExceptOp MinusOp]
           [net.sf.jsqlparser.expression.operators.relational
            GreaterThan GreaterThanEquals MinorThan MinorThanEquals
            EqualsTo NotEqualsTo Between InExpression IsNullExpression
            ExistsExpression LikeExpression ExpressionList ParenthesedExpressionList]
           [net.sf.jsqlparser.expression.operators.conditional
            AndExpression OrExpression]
           [net.sf.jsqlparser.expression.operators.arithmetic
            Addition Subtraction Multiplication Division Modulo]
           [net.sf.jsqlparser.statement.create.table CreateTable ColumnDefinition ColDataType]
           [net.sf.jsqlparser.statement.insert Insert]
           [net.sf.jsqlparser.statement.update Update UpdateSet]
           [net.sf.jsqlparser.statement.delete Delete]
           [net.sf.jsqlparser.statement.insert InsertConflictAction InsertConflictTarget ConflictActionType]
           [net.sf.jsqlparser.statement.drop Drop]
           [net.sf.jsqlparser.statement.select Values]
           [stratum.internal PgWireServer PgWireServer$QueryResult PgWireServer$QueryHandler]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Expression translation (JSqlParser AST → Stratum expressions)
;; ============================================================================

(declare translate-function)
(declare aggregate-function?)
(declare translate-aggregate)
(declare translate-order-element)
(declare filtered-aggregate?)

(defn- translate-column
  "Translate a Column reference to a keyword.
   Preserves table qualifiers as Clojure namespaced keywords for disambiguation
   (e.g., employees.dept_code → :employees/dept_code, EXCLUDED.col → :excluded/col).
   Bare column references become simple keywords (:col)."
  [^Column col]
  (let [table (.getTable col)
        table-name (when table (.getName table))]
    (if table-name
      (keyword (.toLowerCase table-name) (.getColumnName col))
      (keyword (.getColumnName col)))))

(defn- translate-expr
  "Translate a JSqlParser Expression to a Stratum expression."
  [expr]
  (cond
    (instance? Column expr)
    (translate-column expr)

    (instance? AllColumns expr)
    :*  ;; SELECT * or COUNT(*)

    (instance? LongValue expr)
    (.getValue ^LongValue expr)

    (instance? DoubleValue expr)
    (.getValue ^DoubleValue expr)

    (instance? StringValue expr)
    (.getValue ^StringValue expr)

    (instance? NullValue expr)
    nil

    (instance? Parenthesis expr)
    (translate-expr (.getExpression ^Parenthesis expr))

    ;; ParenthesedExpressionList — JSqlParser v5 uses this for (expr) in some contexts
    (instance? ParenthesedExpressionList expr)
    (let [^ParenthesedExpressionList pel expr]
      (if (= 1 (.size pel))
        (translate-expr (.get pel 0))
        (throw (ex-info "Multi-element parenthesized list not supported"
                        {:size (.size pel) :expr (str expr)}))))

    (instance? SignedExpression expr)
    (let [^SignedExpression se expr
          sign (.getSign se)
          inner (translate-expr (.getExpression se))]
      (if (= sign \-)
        (if (number? inner) (- inner) [:* -1 inner])
        inner))

    ;; Arithmetic: +, -, *, /, %
    ;; Constant-fold when both sides are numeric literals (e.g. CAST date + interval)
    (instance? Addition expr)
    (let [^Addition e expr
          l (translate-expr (.getLeftExpression e))
          r (translate-expr (.getRightExpression e))]
      (if (and (number? l) (number? r))
        (if (or (float? l) (float? r) (instance? Double l) (instance? Double r))
          (+ (double l) (double r))
          (+ (long l) (long r)))
        [:+ l r]))

    (instance? Subtraction expr)
    (let [^Subtraction e expr
          l (translate-expr (.getLeftExpression e))
          r (translate-expr (.getRightExpression e))]
      (if (and (number? l) (number? r))
        (if (or (float? l) (float? r) (instance? Double l) (instance? Double r))
          (- (double l) (double r))
          (- (long l) (long r)))
        [:- l r]))

    (instance? Multiplication expr)
    (let [^Multiplication e expr]
      [:* (translate-expr (.getLeftExpression e)) (translate-expr (.getRightExpression e))])

    (instance? Division expr)
    (let [^Division e expr]
      [:/ (translate-expr (.getLeftExpression e)) (translate-expr (.getRightExpression e))])

    (instance? Modulo expr)
    (let [^Modulo e expr]
      [:mod (translate-expr (.getLeftExpression e)) (translate-expr (.getRightExpression e))])

    ;; SQL functions (including aggregates for HAVING clauses)
    (instance? Function expr)
    (if (aggregate-function? expr)
      ;; In HAVING context, aggregate references resolve to their keyword alias
      ;; e.g., COUNT(*) → :count, AVG(price) → :avg, SUM(x) → :sum
      ;; When auto-alias-aggs deduplicates, keys become :sum_price, :sum_qty etc.
      ;; We produce :{op}_{col} when params are present; apply-having falls back
      ;; to the base key (without suffix) for single-agg queries.
      (let [^Function f expr
            agg-name (-> (.getName f) (.toUpperCase))
            params (when-let [p (.getParameters f)]
                     (mapv translate-expr p))
            base-kw (keyword (.toLowerCase agg-name))]
        (if (.isDistinct f)
          :count-distinct
          (if (and (seq params) (keyword? (first params)))
            (keyword (str (name base-kw) "_" (name (first params))))
            base-kw)))
      (translate-function expr))

    ;; CAST
    (instance? CastExpression expr)
    (let [^CastExpression ce expr
          inner (translate-expr (.getLeftExpression ce))
          type-name (some-> (.getColDataType ce) (.getDataType) (.toLowerCase))]
      (case type-name
        ("bigint" "int8" "integer" "int" "int4") [:cast inner :long]
        ("double" "float8" "double precision" "real" "float4" "numeric" "decimal") [:cast inner :double]
        ("text" "varchar" "char" "character varying") [:cast inner :string]
        "date" (if (string? inner)
                 (.toEpochDay (java.time.LocalDate/parse inner))
                 [:cast inner :date])
        ;; Default: pass through unmodified
        inner))

    ;; CASE WHEN
    (instance? CaseExpression expr)
    (let [^CaseExpression ce expr
          switch-expr (when-let [se (.getSwitchExpression ce)]
                        (translate-expr se))
          whens (.getWhenClauses ce)
          else-expr (when-let [e (.getElseExpression ce)]
                      (translate-expr e))]
      ;; Produce [:case [cond1 then1] [cond2 then2] [:else default]] to match normalize-expr
      ;; For simple CASE (CASE x WHEN v THEN r), transform to searched CASE (CASE WHEN x=v THEN r)
      (let [clauses (mapv (fn [^WhenClause w]
                            [(if switch-expr
                               [:= switch-expr (translate-expr (.getWhenExpression w))]
                               (translate-expr (.getWhenExpression w)))
                             (translate-expr (.getThenExpression w))])
                          whens)]
        (into [:case] (concat clauses (when else-expr [[:else else-expr]])))))

    ;; INTERVAL expression — convert to epoch-day count for date arithmetic
    (instance? IntervalExpression expr)
    (let [^IntervalExpression ie expr
          n (if-let [e (.getExpression ie)]
              (translate-expr e)
              (Long/parseLong (str/replace (or (.getParameter ie) "0") "'" "")))
          unit (some-> (.getIntervalType ie) str/upper-case str/trim)]
      (case unit
        ("DAY" "DAYS") n
        ("MONTH" "MONTHS") [:* n 30]
        ("YEAR" "YEARS") [:* n 365]
        n))

    ;; Comparison operators — used in CASE WHEN clauses
    (instance? EqualsTo expr)
    (let [^EqualsTo e expr]
      [:= (translate-expr (.getLeftExpression e)) (translate-expr (.getRightExpression e))])

    (instance? NotEqualsTo expr)
    (let [^NotEqualsTo e expr]
      [:!= (translate-expr (.getLeftExpression e)) (translate-expr (.getRightExpression e))])

    (instance? GreaterThan expr)
    (let [^GreaterThan e expr]
      [:> (translate-expr (.getLeftExpression e)) (translate-expr (.getRightExpression e))])

    (instance? GreaterThanEquals expr)
    (let [^GreaterThanEquals e expr]
      [:>= (translate-expr (.getLeftExpression e)) (translate-expr (.getRightExpression e))])

    (instance? MinorThan expr)
    (let [^MinorThan e expr]
      [:< (translate-expr (.getLeftExpression e)) (translate-expr (.getRightExpression e))])

    (instance? MinorThanEquals expr)
    (let [^MinorThanEquals e expr]
      [:<= (translate-expr (.getLeftExpression e)) (translate-expr (.getRightExpression e))])

    :else
    (throw (ex-info (str "Unsupported SQL expression: " (type expr) " — " expr)
                    {:expr (str expr) :type (str (type expr))}))))

(defn- translate-function
  "Translate a SQL function call to a Stratum expression."
  [^Function func]
  (let [name (-> (.getName func) (.toUpperCase))
        params (when-let [p (.getParameters func)]
                 (mapv translate-expr p))
        n-params (count params)]
    (case name
      ;; Date/time functions
      "DATE_TRUNC"
      (if (and (= 2 n-params) (string? (first params)))
        [:date-trunc (keyword (.toLowerCase ^String (first params))) (second params)]
        (throw (ex-info "DATE_TRUNC requires (precision, column)" {:params params})))

      "DATE_ADD"
      (if (= 3 n-params)
        [:date-add (keyword (.toLowerCase ^String (first params))) (second params) (nth params 2)]
        (throw (ex-info "DATE_ADD requires (unit, interval, column)" {:params params})))

      "DATE_DIFF"
      (if (= 3 n-params)
        [:date-diff (keyword (.toLowerCase ^String (first params))) (second params) (nth params 2)]
        (throw (ex-info "DATE_DIFF requires (unit, col1, col2)" {:params params})))

      "EXTRACT"
      (if (and (= 2 n-params) (string? (first params)))
        [:extract (keyword (.toLowerCase ^String (first params))) (second params)]
        ;; JSqlParser may parse EXTRACT(field FROM col) as Function with special handling
        (throw (ex-info "EXTRACT requires (field, column)" {:params params})))

      ;; String functions
      "UPPER" [:upper (first params)]
      "LOWER" [:lower (first params)]
      "SUBSTR" (case n-params
                 2 [:substr (first params) (second params)]
                 3 [:substr (first params) (second params) (nth params 2)]
                 (throw (ex-info "SUBSTR requires 2-3 args" {:params params})))
      "SUBSTRING" (case n-params
                    2 [:substr (first params) (second params)]
                    3 [:substr (first params) (second params) (nth params 2)]
                    (throw (ex-info "SUBSTRING requires 2-3 args" {:params params})))
      ("LENGTH" "STRLEN") [:length (first params)]

      ;; Math functions
      "ABS" [:abs (first params)]
      "SQRT" [:sqrt (first params)]
      "ROUND" (if (= 2 n-params)
                [:round (first params) (second params)]
                [:round (first params)])

      ;; NULL handling
      "COALESCE" (if (<= (count params) 2)
                   (into [:coalesce] params)
                   ;; Nest: COALESCE(a,b,c) → [:coalesce a [:coalesce b c]]
                   (reduce (fn [acc p] [:coalesce p acc])
                           (last params)
                           (reverse (butlast params))))
      "NULLIF" [:nullif (first params) (second params)]
      "GREATEST" (into [:greatest] params)
      "LEAST" (into [:least] params)

      ;; Epoch conversions
      "EPOCH_DAYS" [:epoch-days (first params)]
      "EPOCH_SECONDS" [:epoch-seconds (first params)]

      ;; Anomaly detection
      "ANOMALY_SCORE"
      (if (>= n-params 2)
        (into [:anomaly-score (first params)] (rest params))
        (throw (ex-info "ANOMALY_SCORE requires (model_name, col1, ...)" {:params params})))

      "ANOMALY_PREDICT"
      (if (>= n-params 2)
        (into [:anomaly-predict (first params)] (rest params))
        (throw (ex-info "ANOMALY_PREDICT requires (model_name, col1, ...)" {:params params})))

      "ANOMALY_PROBA"
      (if (>= n-params 2)
        (into [:anomaly-proba (first params)] (rest params))
        (throw (ex-info "ANOMALY_PROBA requires (model_name, col1, ...)" {:params params})))

      "ANOMALY_CONFIDENCE"
      (if (>= n-params 2)
        (into [:anomaly-confidence (first params)] (rest params))
        (throw (ex-info "ANOMALY_CONFIDENCE requires (model_name, col1, ...)" {:params params})))

      ;; Default: unknown function
      (throw (ex-info (str "Unsupported SQL function: " name)
                      {:function name :params params})))))

;; ============================================================================
;; Window function translation
;; ============================================================================

(defn- window-function?
  "Check if an expression is an AnalyticExpression (window function).
   Returns false for FILTER-only aggregates (no window)."
  [expr]
  (and (instance? AnalyticExpression expr)
       (not (filtered-aggregate? expr))))

(defn- translate-window-bound
  "Translate a WindowOffset to a frame bound keyword or [N :preceding/:following].
   Returns :unbounded-preceding, :current-row, :unbounded-following, or [N :preceding/:following]."
  [^WindowOffset wo default]
  (if (nil? wo)
    default
    (let [type-str (str (.getType wo))
          expr (.getExpression wo)
          ;; Extract numeric value if present (e.g. "3 PRECEDING" → 3)
          n (when expr
              (cond
                (instance? LongValue expr) (.getValue ^LongValue expr)
                (instance? DoubleValue expr) (long (.getValue ^DoubleValue expr))
                :else nil))]
      (case type-str
        "PRECEDING" (if n [n :preceding] :unbounded-preceding)
        "CURRENT"   :current-row
        "FOLLOWING"  (if n [n :following] :unbounded-following)
        default))))

(defn- translate-window-frame
  "Translate a WindowElement into a frame spec map.
   Returns nil if no frame specified (uses default RANGE UNBOUNDED PRECEDING).
   Numeric offsets produce [N :preceding] or [N :following] bounds."
  [^WindowElement we]
  (when we
    (let [frame-type (case (str (.getType we))
                       "ROWS" :rows
                       "RANGE" :range
                       :rows)]
      (if-let [^WindowRange wr (.getRange we)]
        ;; ROWS/RANGE BETWEEN start AND end
        {:type frame-type
         :start (translate-window-bound (.getStart wr) :unbounded-preceding)
         :end   (translate-window-bound (.getEnd wr) :current-row)}
        ;; Single offset (e.g. ROWS UNBOUNDED PRECEDING)
        (when-let [^WindowOffset wo (.getOffset we)]
          {:type frame-type
           :start (translate-window-bound wo :unbounded-preceding)
           :end   :current-row})))))

(defn- translate-window-function
  "Translate an AnalyticExpression to a window spec map.
   When the argument is itself an aggregate (e.g. SUM(SUM(x))), the inner
   aggregate is extracted and returned under :_inner-agg for injection into
   the query's :agg list."
  [^AnalyticExpression ae alias-name]
  (let [name (-> (.getName ae) (.toUpperCase))
        op (case name
             "ROW_NUMBER" :row-number
             "RANK" :rank
             "DENSE_RANK" :dense-rank
             "NTILE" :ntile
             "PERCENT_RANK" :percent-rank
             "CUME_DIST" :cume-dist
             "SUM" :sum
             "COUNT" :count
             "AVG" :avg
             "MIN" :min
             "MAX" :max
             "LAG" :lag
             "LEAD" :lead
             (throw (ex-info (str "Unsupported window function: " name)
                             {:function name})))
        ;; Check if argument is a nested aggregate (e.g. SUM(SUM(x)))
        raw-expr (.getExpression ae)
        nested-agg? (and raw-expr
                         (instance? Function raw-expr)
                         (aggregate-function? ^Function raw-expr))
        ;; For nested agg: extract inner agg spec, assign temp alias
        inner-agg-spec (when nested-agg?
                         (translate-aggregate ^Function raw-expr))
        inner-agg-alias (when nested-agg?
                          (keyword (str "_win_inner_" (clojure.core/name (first inner-agg-spec)))))
        ;; Column: for nested agg, point to materialized inner agg column
        col (cond
              nested-agg? inner-agg-alias
              raw-expr (translate-expr raw-expr)
              :else nil)
        ;; LAG/LEAD offset and default
        offset-val (when-let [o (.getOffset ae)]
                     (translate-expr o))
        default-val (when-let [d (.getDefaultValue ae)]
                      (translate-expr d))
        ;; PARTITION BY
        partition-by (when-let [pel (.getPartitionExpressionList ae)]
                       (mapv translate-expr pel))
        ;; ORDER BY
        order-by (when-let [obs (.getOrderByElements ae)]
                   (mapv translate-order-element obs))
        ;; Window frame — apply SQL-standard defaults:
        ;; With ORDER BY, no frame → ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ;; Without ORDER BY, no frame → ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        frame (or (translate-window-frame (.getWindowElement ae))
                  (if (seq order-by)
                    {:type :rows :start :unbounded-preceding :end :current-row}
                    {:type :rows :start :unbounded-preceding :end :unbounded-following}))
        ;; Output alias
        as-kw (keyword (or alias-name (str "_win_" name)))]
    (cond-> {:op op :as as-kw}
      col (assoc :col col)
      (seq partition-by) (assoc :partition-by partition-by)
      (seq order-by) (assoc :order-by order-by)
      offset-val (assoc :offset offset-val)
      default-val (assoc :default default-val)
      true (assoc :frame frame)
      nested-agg? (assoc :_inner-agg [:as inner-agg-spec inner-agg-alias]))))

;; ============================================================================
;; Aggregate translation
;; ============================================================================

(def ^:private agg-functions
  "Set of SQL aggregate function names."
  #{"SUM" "COUNT" "AVG" "MIN" "MAX" "STDDEV" "VARIANCE" "CORR"
    "STDDEV_POP" "STDDEV_SAMP" "VAR_POP" "VAR_SAMP"
    "MEDIAN" "PERCENTILE_CONT" "PERCENTILE_DISC" "APPROX_QUANTILE"})

(defn- aggregate-function?
  "Check if a Function node is an aggregate."
  [^Function func]
  (contains? agg-functions (-> (.getName func) (.toUpperCase))))

(defn- translate-aggregate
  "Translate an aggregate Function to a Stratum agg spec."
  [^Function func]
  (let [name (-> (.getName func) (.toUpperCase))
        params (when-let [p (.getParameters func)]
                 (mapv translate-expr p))
        distinct? (.isDistinct func)
        all-cols? (.isAllColumns func)]
    (case name
      "COUNT"
      (cond
        distinct? [:count-distinct (first params)]
        all-cols? [:count]
        ;; COUNT(*) may parse as params=[:*] in some JSqlParser forms
        (and (seq params) (not= :* (first params)))
        [:count-non-null (first params)]  ;; COUNT(col) skips NULLs
        :else [:count])

      "SUM"
      (if (= 1 (count params))
        [:sum (first params)]
        (throw (ex-info "SUM requires exactly one argument" {:params params})))

      "AVG"
      (if (= 1 (count params))
        [:avg (first params)]
        (throw (ex-info "AVG requires exactly one argument" {:params params})))

      "MIN"
      (if (= 1 (count params))
        [:min (first params)]
        (throw (ex-info "MIN requires exactly one argument" {:params params})))

      "MAX"
      (if (= 1 (count params))
        [:max (first params)]
        (throw (ex-info "MAX requires exactly one argument" {:params params})))

      ("STDDEV" "STDDEV_SAMP")
      (if (= 1 (count params))
        [:stddev (first params)]
        (throw (ex-info "STDDEV requires exactly one argument" {:params params})))

      "STDDEV_POP"
      (if (= 1 (count params))
        [:stddev-pop (first params)]
        (throw (ex-info "STDDEV_POP requires exactly one argument" {:params params})))

      ("VARIANCE" "VAR_SAMP")
      (if (= 1 (count params))
        [:variance (first params)]
        (throw (ex-info "VARIANCE requires exactly one argument" {:params params})))

      "VAR_POP"
      (if (= 1 (count params))
        [:variance-pop (first params)]
        (throw (ex-info "VAR_POP requires exactly one argument" {:params params})))

      "CORR"
      (if (= 2 (count params))
        [:corr (first params) (second params)]
        (throw (ex-info "CORR requires exactly two arguments" {:params params})))

      "MEDIAN"
      (if (= 1 (count params))
        [:median (first params)]
        (throw (ex-info "MEDIAN requires exactly one argument" {:params params})))

      ("PERCENTILE_CONT" "PERCENTILE_DISC")
      (if (= 2 (count params))
        [:percentile (second params) (first params)]
        (throw (ex-info "PERCENTILE_CONT requires two arguments: fraction, column" {:params params})))

      "APPROX_QUANTILE"
      (if (= 2 (count params))
        [:approx-quantile (first params) (second params)]
        (throw (ex-info "APPROX_QUANTILE requires two arguments: column, fraction" {:params params})))

      ;; Should not reach here since we check agg-functions first
      (throw (ex-info (str "Unknown aggregate: " name) {:function name})))))

;; ============================================================================
;; Predicate (WHERE/HAVING) translation
;; ============================================================================

(defn- translate-predicate
  "Translate a JSqlParser Expression tree to Stratum predicate(s).
   Returns a vector of predicates (flattened ANDs)."
  [expr]
  (cond
    ;; AND → flatten into multiple predicates
    (instance? AndExpression expr)
    (let [^AndExpression e expr]
      (into (translate-predicate (.getLeftExpression e))
            (translate-predicate (.getRightExpression e))))

    ;; OR → wrap (preserve compound AND chains on each side)
    (instance? OrExpression expr)
    (let [^OrExpression e expr
          left-preds (translate-predicate (.getLeftExpression e))
          right-preds (translate-predicate (.getRightExpression e))
          ;; If a side has multiple predicates (AND chain), wrap in :and
          left (if (> (count left-preds) 1)
                 (into [:and] left-preds)
                 (first left-preds))
          right (if (> (count right-preds) 1)
                  (into [:and] right-preds)
                  (first right-preds))]
      [[:or left right]])

    ;; NOT (preserve compound AND chains inside)
    (instance? NotExpression expr)
    (let [^NotExpression e expr
          inner-preds (translate-predicate (.getExpression e))
          inner (if (> (count inner-preds) 1)
                  (into [:and] inner-preds)
                  (first inner-preds))]
      [[:not inner]])

    ;; Comparisons
    (instance? GreaterThan expr)
    (let [^GreaterThan e expr]
      [[:> (translate-expr (.getLeftExpression e)) (translate-expr (.getRightExpression e))]])

    (instance? GreaterThanEquals expr)
    (let [^GreaterThanEquals e expr]
      [[:>= (translate-expr (.getLeftExpression e)) (translate-expr (.getRightExpression e))]])

    (instance? MinorThan expr)
    (let [^MinorThan e expr]
      [[:< (translate-expr (.getLeftExpression e)) (translate-expr (.getRightExpression e))]])

    (instance? MinorThanEquals expr)
    (let [^MinorThanEquals e expr]
      [[:<= (translate-expr (.getLeftExpression e)) (translate-expr (.getRightExpression e))]])

    (instance? EqualsTo expr)
    (let [^EqualsTo e expr]
      [[:= (translate-expr (.getLeftExpression e)) (translate-expr (.getRightExpression e))]])

    (instance? NotEqualsTo expr)
    (let [^NotEqualsTo e expr]
      [[:!= (translate-expr (.getLeftExpression e)) (translate-expr (.getRightExpression e))]])

    ;; BETWEEN
    (instance? Between expr)
    (let [^Between e expr
          col (translate-expr (.getLeftExpression e))
          lo (translate-expr (.getBetweenExpressionStart e))
          hi (translate-expr (.getBetweenExpressionEnd e))]
      (if (.isNot e)
        [[:not [:between col lo hi]]]
        [[:between col lo hi]]))

    ;; IN (possibly with subquery)
    (instance? InExpression expr)
    (let [^InExpression e expr
          col (translate-expr (.getLeftExpression e))
          right (.getRightExpression e)]
      (if (instance? ParenthesedSelect right)
        ;; IN (SELECT ...) — subquery marker (resolved in translate-select)
        [(into [(if (.isNot e) :not-in-subquery :in-subquery) col]
               [{:subquery-select (.getPlainSelect ^ParenthesedSelect right)}])]
        ;; Regular IN (v1, v2, ...)
        (let [vals (if (instance? ExpressionList right)
                     (mapv translate-expr ^ExpressionList right)
                     [(translate-expr right)])]
          (if (.isNot e)
            [(into [:not-in col] vals)]
            [(into [:in col] vals)]))))

    ;; IS NULL / IS NOT NULL
    (instance? IsNullExpression expr)
    (let [^IsNullExpression e expr
          col (translate-expr (.getLeftExpression e))]
      (if (.isNot e)
        [[:is-not-null col]]
        [[:is-null col]]))

    ;; LIKE
    (instance? LikeExpression expr)
    (let [^LikeExpression e expr
          col (translate-expr (.getLeftExpression e))
          pattern (translate-expr (.getRightExpression e))
          case-insensitive? (.isCaseInsensitive e)]
      (cond
        (and case-insensitive? (.isNot e)) [[:not-ilike col pattern]]
        case-insensitive? [[:ilike col pattern]]
        (.isNot e) [[:not-like col pattern]]
        :else [[:like col pattern]]))

    ;; EXISTS / NOT EXISTS
    (instance? ExistsExpression expr)
    (let [^ExistsExpression e expr
          right (.getRightExpression e)]
      (if (instance? ParenthesedSelect right)
        [(into [(if (.isNot e) :not-exists-subquery :exists-subquery)]
               [{:subquery-select (.getPlainSelect ^ParenthesedSelect right)}])]
        (throw (ex-info "EXISTS requires a subquery" {:expr (str expr)}))))

    ;; Parenthesized expression
    (instance? Parenthesis expr)
    (translate-predicate (.getExpression ^Parenthesis expr))

    ;; Fallback — might be a boolean column or expression
    :else
    [[:= (translate-expr expr) 1]]))

;; ============================================================================
;; SELECT item classification
;; ============================================================================

(defn- select-item-is-agg?
  "Check if a SelectItem contains an aggregate function (possibly wrapped in arithmetic).
   Returns false for window functions (AnalyticExpression) — window SUM is not GROUP BY SUM.
   Returns true for FILTER-only AnalyticExpressions (no window)."
  [expr]
  (cond
    (instance? AnalyticExpression expr) (filtered-aggregate? expr)
    (instance? Function expr) (aggregate-function? expr)
    (instance? Addition expr) (or (select-item-is-agg? (.getLeftExpression ^Addition expr))
                                  (select-item-is-agg? (.getRightExpression ^Addition expr)))
    (instance? Subtraction expr) (or (select-item-is-agg? (.getLeftExpression ^Subtraction expr))
                                     (select-item-is-agg? (.getRightExpression ^Subtraction expr)))
    (instance? Multiplication expr) (or (select-item-is-agg? (.getLeftExpression ^Multiplication expr))
                                        (select-item-is-agg? (.getRightExpression ^Multiplication expr)))
    (instance? Division expr) (or (select-item-is-agg? (.getLeftExpression ^Division expr))
                                  (select-item-is-agg? (.getRightExpression ^Division expr)))
    (instance? Parenthesis expr) (select-item-is-agg? (.getExpression ^Parenthesis expr))
    (instance? ParenthesedExpressionList expr)
    (and (= 1 (.size ^ParenthesedExpressionList expr))
         (select-item-is-agg? (.get ^ParenthesedExpressionList expr 0)))
    ;; Comparison operators (for CASE WHEN conditions containing aggregates)
    (instance? GreaterThan expr) (or (select-item-is-agg? (.getLeftExpression ^GreaterThan expr))
                                     (select-item-is-agg? (.getRightExpression ^GreaterThan expr)))
    (instance? GreaterThanEquals expr) (or (select-item-is-agg? (.getLeftExpression ^GreaterThanEquals expr))
                                           (select-item-is-agg? (.getRightExpression ^GreaterThanEquals expr)))
    (instance? MinorThan expr) (or (select-item-is-agg? (.getLeftExpression ^MinorThan expr))
                                   (select-item-is-agg? (.getRightExpression ^MinorThan expr)))
    (instance? MinorThanEquals expr) (or (select-item-is-agg? (.getLeftExpression ^MinorThanEquals expr))
                                         (select-item-is-agg? (.getRightExpression ^MinorThanEquals expr)))
    (instance? EqualsTo expr) (or (select-item-is-agg? (.getLeftExpression ^EqualsTo expr))
                                  (select-item-is-agg? (.getRightExpression ^EqualsTo expr)))
    (instance? NotEqualsTo expr) (or (select-item-is-agg? (.getLeftExpression ^NotEqualsTo expr))
                                     (select-item-is-agg? (.getRightExpression ^NotEqualsTo expr)))
    ;; CASE WHEN with aggregate conditions or values
    (instance? CaseExpression expr)
    (let [^CaseExpression ce expr]
      (or (some (fn [^WhenClause w]
                  (or (select-item-is-agg? (.getWhenExpression w))
                      (select-item-is-agg? (.getThenExpression w))))
                (.getWhenClauses ce))
          (when-let [e (.getElseExpression ce)]
            (select-item-is-agg? e))))
    :else false))

(declare filtered-aggregate? translate-filtered-aggregate)

(defn- collect-aggs-from-expr
  "Collect all aggregate functions from an expression tree.
   Returns a vector of [agg-spec keyword-name] pairs."
  [expr counter-atom]
  (cond
    (instance? Function expr)
    (when (aggregate-function? expr)
      (let [agg-spec (translate-aggregate expr)
            agg-name (keyword (str "_agg" (swap! counter-atom inc)))]
        [[agg-spec agg-name]]))

    (and (instance? AnalyticExpression expr) (filtered-aggregate? expr))
    (let [agg-spec (translate-filtered-aggregate ^AnalyticExpression expr)
          agg-name (keyword (str "_agg" (swap! counter-atom inc)))]
      [[agg-spec agg-name]])

    (instance? Addition expr)
    (into (vec (collect-aggs-from-expr (.getLeftExpression ^Addition expr) counter-atom))
          (collect-aggs-from-expr (.getRightExpression ^Addition expr) counter-atom))

    (instance? Subtraction expr)
    (into (vec (collect-aggs-from-expr (.getLeftExpression ^Subtraction expr) counter-atom))
          (collect-aggs-from-expr (.getRightExpression ^Subtraction expr) counter-atom))

    (instance? Multiplication expr)
    (into (vec (collect-aggs-from-expr (.getLeftExpression ^Multiplication expr) counter-atom))
          (collect-aggs-from-expr (.getRightExpression ^Multiplication expr) counter-atom))

    (instance? Division expr)
    (into (vec (collect-aggs-from-expr (.getLeftExpression ^Division expr) counter-atom))
          (collect-aggs-from-expr (.getRightExpression ^Division expr) counter-atom))

    (instance? Parenthesis expr)
    (collect-aggs-from-expr (.getExpression ^Parenthesis expr) counter-atom)

    (instance? ParenthesedExpressionList expr)
    (when (= 1 (.size ^ParenthesedExpressionList expr))
      (collect-aggs-from-expr (.get ^ParenthesedExpressionList expr 0) counter-atom))

    ;; Comparison operators (inside CASE WHEN conditions)
    (instance? GreaterThan expr)
    (into (vec (collect-aggs-from-expr (.getLeftExpression ^GreaterThan expr) counter-atom))
          (collect-aggs-from-expr (.getRightExpression ^GreaterThan expr) counter-atom))
    (instance? GreaterThanEquals expr)
    (into (vec (collect-aggs-from-expr (.getLeftExpression ^GreaterThanEquals expr) counter-atom))
          (collect-aggs-from-expr (.getRightExpression ^GreaterThanEquals expr) counter-atom))
    (instance? MinorThan expr)
    (into (vec (collect-aggs-from-expr (.getLeftExpression ^MinorThan expr) counter-atom))
          (collect-aggs-from-expr (.getRightExpression ^MinorThan expr) counter-atom))
    (instance? MinorThanEquals expr)
    (into (vec (collect-aggs-from-expr (.getLeftExpression ^MinorThanEquals expr) counter-atom))
          (collect-aggs-from-expr (.getRightExpression ^MinorThanEquals expr) counter-atom))
    (instance? EqualsTo expr)
    (into (vec (collect-aggs-from-expr (.getLeftExpression ^EqualsTo expr) counter-atom))
          (collect-aggs-from-expr (.getRightExpression ^EqualsTo expr) counter-atom))
    (instance? NotEqualsTo expr)
    (into (vec (collect-aggs-from-expr (.getLeftExpression ^NotEqualsTo expr) counter-atom))
          (collect-aggs-from-expr (.getRightExpression ^NotEqualsTo expr) counter-atom))

    ;; CASE WHEN with aggregate conditions
    (instance? CaseExpression expr)
    (let [^CaseExpression ce expr]
      (into []
            (concat
             (mapcat (fn [^WhenClause w]
                       (concat (collect-aggs-from-expr (.getWhenExpression w) counter-atom)
                               (collect-aggs-from-expr (.getThenExpression w) counter-atom)))
                     (.getWhenClauses ce))
             (when-let [e (.getElseExpression ce)]
               (collect-aggs-from-expr e counter-atom)))))

    :else nil))

(defn- build-post-expr
  "Build a post-processing expression template that references agg result keywords.
   Returns a function (fn [row] computed-value) or nil for simple aggs."
  [expr agg-map]
  (cond
    (instance? Function expr)
    (when (aggregate-function? expr)
      (let [kw (get agg-map (translate-aggregate expr))]
        (when kw kw)))

    (and (instance? AnalyticExpression expr) (filtered-aggregate? expr))
    (let [kw (get agg-map (translate-filtered-aggregate ^AnalyticExpression expr))]
      (when kw kw))

    (instance? Addition expr)
    (let [l (build-post-expr (.getLeftExpression ^Addition expr) agg-map)
          r (build-post-expr (.getRightExpression ^Addition expr) agg-map)]
      (when (and l r) [:+ l r]))

    (instance? Subtraction expr)
    (let [l (build-post-expr (.getLeftExpression ^Subtraction expr) agg-map)
          r (build-post-expr (.getRightExpression ^Subtraction expr) agg-map)]
      (when (and l r) [:- l r]))

    (instance? Multiplication expr)
    (let [l (build-post-expr (.getLeftExpression ^Multiplication expr) agg-map)
          r (build-post-expr (.getRightExpression ^Multiplication expr) agg-map)]
      (when (and l r) [:* l r]))

    (instance? Division expr)
    (let [l (build-post-expr (.getLeftExpression ^Division expr) agg-map)
          r (build-post-expr (.getRightExpression ^Division expr) agg-map)]
      (when (and l r) [:/ l r]))

    (instance? Parenthesis expr)
    (build-post-expr (.getExpression ^Parenthesis expr) agg-map)

    (instance? ParenthesedExpressionList expr)
    (when (= 1 (.size ^ParenthesedExpressionList expr))
      (build-post-expr (.get ^ParenthesedExpressionList expr 0) agg-map))

    ;; Comparison operators (for CASE WHEN conditions)
    (instance? GreaterThan expr)
    (let [l (build-post-expr (.getLeftExpression ^GreaterThan expr) agg-map)
          r (build-post-expr (.getRightExpression ^GreaterThan expr) agg-map)]
      (when (and l r) [:> l r]))

    (instance? GreaterThanEquals expr)
    (let [l (build-post-expr (.getLeftExpression ^GreaterThanEquals expr) agg-map)
          r (build-post-expr (.getRightExpression ^GreaterThanEquals expr) agg-map)]
      (when (and l r) [:>= l r]))

    (instance? MinorThan expr)
    (let [l (build-post-expr (.getLeftExpression ^MinorThan expr) agg-map)
          r (build-post-expr (.getRightExpression ^MinorThan expr) agg-map)]
      (when (and l r) [:< l r]))

    (instance? MinorThanEquals expr)
    (let [l (build-post-expr (.getLeftExpression ^MinorThanEquals expr) agg-map)
          r (build-post-expr (.getRightExpression ^MinorThanEquals expr) agg-map)]
      (when (and l r) [:<= l r]))

    (instance? EqualsTo expr)
    (let [l (build-post-expr (.getLeftExpression ^EqualsTo expr) agg-map)
          r (build-post-expr (.getRightExpression ^EqualsTo expr) agg-map)]
      (when (and l r) [:= l r]))

    (instance? NotEqualsTo expr)
    (let [l (build-post-expr (.getLeftExpression ^NotEqualsTo expr) agg-map)
          r (build-post-expr (.getRightExpression ^NotEqualsTo expr) agg-map)]
      (when (and l r) [:!= l r]))

    ;; CASE WHEN with aggregate conditions → post-agg CASE expression
    (instance? CaseExpression expr)
    (let [^CaseExpression ce expr
          whens (mapv (fn [^WhenClause w]
                        {:cond (build-post-expr (.getWhenExpression w) agg-map)
                         :then (let [te (.getThenExpression w)]
                                 (or (build-post-expr te agg-map)
                                     (translate-expr te)))})
                      (.getWhenClauses ce))
          else-val (when-let [e (.getElseExpression ce)]
                     (or (build-post-expr e agg-map)
                         (translate-expr e)))]
      [:case-post whens else-val])

    (instance? LongValue expr) (.getValue ^LongValue expr)
    (instance? DoubleValue expr) (.getValue ^DoubleValue expr)
    (instance? StringValue expr) (.getValue ^StringValue expr)
    (instance? NullValue expr) nil
    (instance? Column expr) (translate-column expr)

    :else nil))

(defn- filtered-aggregate?
  "Check if an AnalyticExpression is a FILTER-only aggregate (no window)."
  [^AnalyticExpression ae]
  (and (.getFilterExpression ae)
       (nil? (.getWindowElement ae))
       (nil? (.getPartitionExpressionList ae))
       (nil? (.getOrderByElements ae))
       (contains? agg-functions (-> (.getName ae) (.toUpperCase)))))

(defn- translate-filtered-aggregate
  "Translate SUM(x) FILTER (WHERE pred) into SUM(CASE WHEN pred THEN x ELSE NULL END).
   For COUNT(*) FILTER, translates to COUNT-non-null of CASE WHEN pred THEN 1 ELSE NULL END."
  [^AnalyticExpression ae]
  (let [name (-> (.getName ae) (.toUpperCase))
        filter-expr (.getFilterExpression ae)
        filter-preds (translate-predicate filter-expr)
        ;; Build CASE WHEN filter THEN col ELSE NULL END as vector form
        ;; [:case [pred val] [:else nil]]
        when-clause (if (> (count filter-preds) 1)
                      (into [:and] filter-preds)
                      (first filter-preds))
        inner-expr (.getExpression ae)
        col-expr (if inner-expr (translate-expr inner-expr) :*)
        then-val (if (= col-expr :*) 1 col-expr)
        case-expr [:case [when-clause then-val]]]
    (case name
      ;; COUNT FILTER → SUM(CASE WHEN pred THEN 1 ELSE 0): avoids NULL/NaN issues
      "COUNT" [:sum [:case [when-clause 1] [:else 0]]]
      "SUM" [:sum case-expr]
      "AVG" [:avg case-expr]
      "MIN" [:min case-expr]
      "MAX" [:max case-expr]
      (throw (ex-info (str "FILTER not supported for: " name) {:function name})))))

(defn- collect-aggs-from-having-expr
  "Walk a JSqlParser Expression tree (HAVING clause) and collect all aggregate
   Function nodes as translated agg specs. Returns a vector of agg specs
   (e.g. [[:avg :a] [:sum :b]])."
  [expr]
  (cond
    (nil? expr) []

    (instance? Function expr)
    (if (aggregate-function? expr)
      [(translate-aggregate ^Function expr)]
      [])

    (instance? AnalyticExpression expr)
    (if (filtered-aggregate? expr)
      [(translate-filtered-aggregate ^AnalyticExpression expr)]
      [])

    ;; Comparison operators — walk both sides
    (instance? GreaterThan expr)
    (into (collect-aggs-from-having-expr (.getLeftExpression ^GreaterThan expr))
          (collect-aggs-from-having-expr (.getRightExpression ^GreaterThan expr)))

    (instance? GreaterThanEquals expr)
    (into (collect-aggs-from-having-expr (.getLeftExpression ^GreaterThanEquals expr))
          (collect-aggs-from-having-expr (.getRightExpression ^GreaterThanEquals expr)))

    (instance? MinorThan expr)
    (into (collect-aggs-from-having-expr (.getLeftExpression ^MinorThan expr))
          (collect-aggs-from-having-expr (.getRightExpression ^MinorThan expr)))

    (instance? MinorThanEquals expr)
    (into (collect-aggs-from-having-expr (.getLeftExpression ^MinorThanEquals expr))
          (collect-aggs-from-having-expr (.getRightExpression ^MinorThanEquals expr)))

    (instance? EqualsTo expr)
    (into (collect-aggs-from-having-expr (.getLeftExpression ^EqualsTo expr))
          (collect-aggs-from-having-expr (.getRightExpression ^EqualsTo expr)))

    (instance? NotEqualsTo expr)
    (into (collect-aggs-from-having-expr (.getLeftExpression ^NotEqualsTo expr))
          (collect-aggs-from-having-expr (.getRightExpression ^NotEqualsTo expr)))

    ;; Boolean connectives
    (instance? AndExpression expr)
    (into (collect-aggs-from-having-expr (.getLeftExpression ^AndExpression expr))
          (collect-aggs-from-having-expr (.getRightExpression ^AndExpression expr)))

    (instance? OrExpression expr)
    (into (collect-aggs-from-having-expr (.getLeftExpression ^OrExpression expr))
          (collect-aggs-from-having-expr (.getRightExpression ^OrExpression expr)))

    (instance? NotExpression expr)
    (collect-aggs-from-having-expr (.getExpression ^NotExpression expr))

    ;; Arithmetic (HAVING SUM(a) + SUM(b) > 100)
    (instance? Addition expr)
    (into (collect-aggs-from-having-expr (.getLeftExpression ^Addition expr))
          (collect-aggs-from-having-expr (.getRightExpression ^Addition expr)))

    (instance? Subtraction expr)
    (into (collect-aggs-from-having-expr (.getLeftExpression ^Subtraction expr))
          (collect-aggs-from-having-expr (.getRightExpression ^Subtraction expr)))

    (instance? Multiplication expr)
    (into (collect-aggs-from-having-expr (.getLeftExpression ^Multiplication expr))
          (collect-aggs-from-having-expr (.getRightExpression ^Multiplication expr)))

    (instance? Division expr)
    (into (collect-aggs-from-having-expr (.getLeftExpression ^Division expr))
          (collect-aggs-from-having-expr (.getRightExpression ^Division expr)))

    (instance? Parenthesis expr)
    (collect-aggs-from-having-expr (.getExpression ^Parenthesis expr))

    (instance? Between expr)
    (into (collect-aggs-from-having-expr (.getLeftExpression ^Between expr))
          (into (collect-aggs-from-having-expr (.getBetweenExpressionStart ^Between expr))
                (collect-aggs-from-having-expr (.getBetweenExpressionEnd ^Between expr))))

    (instance? IsNullExpression expr)
    (collect-aggs-from-having-expr (.getLeftExpression ^IsNullExpression expr))

    (instance? InExpression expr)
    (collect-aggs-from-having-expr (.getLeftExpression ^InExpression expr))

    :else []))

(defn- extract-agg-from-expr
  "Extract aggregate spec from a SELECT expression that may contain an aggregate.
   For simple agg: returns the agg spec.
   For compound agg expressions (MAX(v1)-MIN(v2)): returns nil (handled by collect-aggs)."
  [expr]
  (cond
    (instance? Function expr)
    (when (aggregate-function? expr)
      (translate-aggregate expr))

    ;; FILTER-only AnalyticExpression → rewrite as CASE-wrapped aggregate
    (and (instance? AnalyticExpression expr)
         (filtered-aggregate? expr))
    (translate-filtered-aggregate expr)

    (instance? Parenthesis expr)
    (extract-agg-from-expr (.getExpression ^Parenthesis expr))

    (instance? ParenthesedExpressionList expr)
    (when (= 1 (.size ^ParenthesedExpressionList expr))
      (extract-agg-from-expr (.get ^ParenthesedExpressionList expr 0)))

    :else nil))

;; ============================================================================
;; Main SELECT translation
;; ============================================================================

(defn- resolve-positional-group
  "Resolve GROUP BY positional reference (e.g., GROUP BY 1) to select item expression."
  [pos select-items]
  (let [idx (dec pos)]  ;; 1-based to 0-based
    (when (and (>= idx 0) (< idx (count select-items)))
      (let [^SelectItem item (nth select-items idx)]
        (.getExpression item)))))

(defn- translate-group-expr
  "Translate a GROUP BY expression to a Stratum group spec."
  [expr select-items]
  (cond
    ;; Positional reference: GROUP BY 1
    (instance? LongValue expr)
    (let [pos (.getValue ^LongValue expr)
          resolved (resolve-positional-group pos select-items)]
      (if resolved
        (translate-group-expr resolved select-items)
        (throw (ex-info (str "Invalid GROUP BY position: " pos)
                        {:position pos}))))

    ;; Column reference — may be an alias from SELECT
    (instance? Column expr)
    (let [col-name (.getColumnName ^Column expr)
          ;; Check if this column name matches a SELECT alias
          alias-match (some (fn [^SelectItem item]
                              (when (= col-name (.getAliasName item))
                                (.getExpression item)))
                            select-items)]
      (if alias-match
        (translate-group-expr alias-match select-items)
        (translate-column expr)))

    ;; Function expression in GROUP BY (e.g., DATE_TRUNC('hour', ts))
    (instance? Function expr)
    (let [name (-> (.getName ^Function expr) (.toUpperCase))
          params (when-let [p (.getParameters ^Function expr)]
                   (mapv translate-expr p))]
      (case name
        "DATE_TRUNC"
        [:date-trunc (keyword (.toLowerCase ^String (first params))) (second params)]

        "EXTRACT"
        [:extract (keyword (.toLowerCase ^String (first params))) (second params)]

        ;; Other function expressions in GROUP BY
        (into [(keyword (.toLowerCase name))] params)))

    ;; Arithmetic expression in GROUP BY
    :else
    (translate-expr expr)))

(defn- translate-order-element
  "Translate an OrderByElement to a Stratum order spec."
  [^OrderByElement elem]
  (let [expr (translate-expr (.getExpression elem))
        dir (if (.isAsc elem) :asc :desc)]
    [expr dir]))

(defn- table-name
  "Extract table name from a FromItem."
  [^Table t]
  (let [alias (.getAlias t)]
    (if alias
      (.getName alias)
      (.getName t))))

(defn- translate-join
  "Translate a Join to a Stratum join spec."
  [^Join join table-registry from-table-name]
  (let [join-table (when (instance? Table (.getFromItem join))
                     ^Table (.getFromItem join))
        join-table-name (when join-table (table-name join-table))
        join-real-name (when join-table (.getName join-table))
        join-data (when join-real-name (get table-registry join-real-name))
        join-type (cond
                    (.isLeft join) :left
                    (.isRight join) :right
                    (.isFull join) :full
                    :else :inner)
        on-exprs (.getOnExpressions join)]
    (when (and (nil? join-data) join-table-name)
      (throw (ex-info (str "Unknown table in JOIN: " join-table-name)
                      {:table join-table-name})))
    (let [;; Flatten AND expressions in ON clause (e.g., ON a = b AND c = d)
          flat-exprs (mapcat (fn flatten-on [expr]
                               (if (instance? AndExpression expr)
                                 (let [^AndExpression ae expr]
                                   (concat (flatten-on (.getLeftExpression ae))
                                           (flatten-on (.getRightExpression ae))))
                                 [expr]))
                             on-exprs)
          on-clauses (mapv (fn [on-expr]
                             (when (instance? EqualsTo on-expr)
                               (let [^EqualsTo eq on-expr
                                     left (.getLeftExpression eq)
                                     right (.getRightExpression eq)]
                                 [:= (translate-expr left) (translate-expr right)])))
                           flat-exprs)
          on-spec (if (= 1 (count on-clauses))
                    (first on-clauses)
                    (vec on-clauses))]
      {:with join-data
       :on on-spec
       :type join-type})))

;; ============================================================================
;; Qualified column resolution for JOINs
;; ============================================================================

(defn- build-join-ref-map
  "Build a mapping from qualified/bare column keywords to unique internal keys.
   Detects column name collisions across tables and renames right-side colliding
   columns to :table__col form. Returns [ref-map collision-set renamed-keys-by-table]
   where ref-map is {qualified-kw → internal-key, bare-kw → left-table-key}.
   renamed-keys-by-table is {table-alias → {old-key → new-key}}."
  [from-table-name from-cols join-tables]
  (let [;; join-tables: [{:alias name :cols #{keys}}]
        all-tables (into [{:alias from-table-name :cols from-cols}] join-tables)
        ;; Count how many tables have each column
        col-freq (reduce (fn [m {:keys [cols]}]
                           (reduce (fn [m2 c] (update m2 c (fnil inc 0))) m cols))
                         {} all-tables)
        collision-set (set (keep (fn [[c cnt]] (when (> cnt 1) c)) col-freq))
        ;; Build ref-map and renamed-keys-by-table
        ref-map (atom {})
        renamed-keys (atom {})]
    (doseq [{:keys [alias cols]} all-tables]
      (doseq [c cols]
        (let [qualified (keyword alias (name c))
              is-from? (= alias from-table-name)
              collides? (contains? collision-set c)]
          (if collides?
            (if is-from?
              ;; FROM table keeps original key
              (do
                (swap! ref-map assoc qualified c)
                ;; bare colliding keyword → FROM table's version
                (swap! ref-map (fn [m] (if (contains? m c) m (assoc m c c)))))
              ;; JOIN table gets renamed key
              (let [internal-key (keyword (str alias "__" (name c)))]
                (swap! ref-map assoc qualified internal-key)
                (swap! renamed-keys assoc-in [alias c] internal-key)))
            ;; Non-colliding: qualified → bare, bare → identity
            (do
              (swap! ref-map assoc qualified c)
              (when-not (contains? @ref-map c)
                (swap! ref-map assoc c c)))))))
    [@ref-map collision-set @renamed-keys]))

(defn- rewrite-ref
  "Rewrite a keyword reference using the ref-map. Falls back to strip-ns."
  [ref-map kw]
  (if-let [mapped (get ref-map kw)]
    mapped
    ;; Not in ref-map: strip namespace as fallback (backward compat)
    (if (and (keyword? kw) (namespace kw))
      (keyword (name kw))
      kw)))

(defn- rewrite-refs
  "Recursively rewrite keyword references in an expression/predicate tree."
  [ref-map expr]
  (cond
    (keyword? expr) (rewrite-ref ref-map expr)
    (number? expr) expr
    (string? expr) expr
    (nil? expr) nil
    (vector? expr) (mapv (partial rewrite-refs ref-map) expr)
    (sequential? expr) (map (partial rewrite-refs ref-map) expr)
    (map? expr) (reduce-kv (fn [m k v]
                             (assoc m k (if (#{:col :cols :key :source :as :partition-by} k)
                                          (if (vector? v)
                                            (mapv (partial rewrite-refs ref-map) v)
                                            (rewrite-refs ref-map v))
                                          (rewrite-refs ref-map v))))
                           {} expr)
    :else expr))

(defn- rename-join-data-keys
  "Rename colliding column keys in join :with data map.
   renamed-keys is {old-key → new-key}."
  [with-data renamed-keys]
  (reduce-kv (fn [m old-k new-k]
               (if-let [v (get m old-k)]
                 (-> m (dissoc old-k) (assoc new-k v))
                 m))
             with-data renamed-keys))

(defn- translate-select
  "Translate a PlainSelect AST to a Stratum query map."
  [^PlainSelect select table-registry]
  (let [select-items (.getSelectItems select)
        from-item (.getFromItem select)
        where-expr (.getWhere select)
        group-by (.getGroupBy select)
        having-expr (.getHaving select)
        order-by (.getOrderByElements select)
        limit (.getLimit select)
        offset (.getOffset select)
        distinct? (.getDistinct select)
        joins (.getJoins select)

        ;; Resolve FROM — either a table reference or a subquery
        ;; Use alias if present (e.g., FROM t1 a → "a"), otherwise real name
        from-table-name (when (instance? Table from-item)
                          (let [alias (.getAlias ^Table from-item)]
                            (if alias (.getName alias) (.getName ^Table from-item))))
        ;; Real table name for registry lookup (alias may differ)
        from-real-name (when (instance? Table from-item)
                         (.getName ^Table from-item))
        ;; Handle FROM (SELECT ...) AS alias — subquery in FROM
        [from-data table-registry]
        (cond
          ;; Subquery in FROM
          (instance? ParenthesedSelect from-item)
          (let [^ParenthesedSelect ps from-item
                inner-select (.getPlainSelect ps)
                inner-query (translate-select inner-select table-registry)
                ;; Execute the subquery and materialize to column arrays
                inner-result (q/q inner-query)
                col-map (if (and (map? inner-result) (:n-rows inner-result))
                          inner-result
                          ;; Convert vector of maps to column arrays
                          (q/results->columns inner-result))
                alias-name (when-let [a (.getAlias ps)]
                             (.getName a))]
            [col-map (if alias-name
                       (assoc table-registry alias-name col-map)
                       table-registry)])

          ;; Normal table reference — look up by real name, register under alias
          from-real-name
          (let [data (get table-registry from-real-name)]
            (when (nil? data)
              (throw (ex-info (str "Unknown table: " from-real-name)
                              {:table from-real-name
                               :available (keys table-registry)})))
            [data (if (not= from-table-name from-real-name)
                    (assoc table-registry from-table-name data)
                    table-registry)])

          ;; No FROM clause — synthesize a single-row dummy table
          :else [{:__dummy (long-array [0])} table-registry])

        ;; Classify select items into projections vs aggregates vs window functions
        has-group? (some? group-by)
        has-agg? (some #(select-item-is-agg? (.getExpression ^SelectItem %)) select-items)
        has-window? (some #(window-function? (.getExpression ^SelectItem %)) select-items)
        all-star? (and (= 1 (count select-items))
                       (instance? AllColumns (.getExpression ^SelectItem (first select-items))))

        ;; Extract window function specs
        window-specs (when has-window?
                       (->> select-items
                            (keep (fn [^SelectItem item]
                                    (let [expr (.getExpression item)]
                                      (when (window-function? expr)
                                        (translate-window-function expr (.getAliasName item))))))
                            (vec)))

        ;; Build aggregation specs from SELECT items.
        ;; Compound expressions like MAX(v1)-MIN(v2) are decomposed into individual
        ;; aggs; a post-processing step computes the final expression.
        agg-counter (atom 0)
        agg-items-raw (when (or has-agg? has-group?)
                        (->> select-items
                             (keep (fn [^SelectItem item]
                                     (let [expr (.getExpression item)
                                           alias-name (.getAliasName item)]
                                       (when (select-item-is-agg? expr)
                                         (let [simple-agg (extract-agg-from-expr expr)]
                                           (if simple-agg
                                             ;; Simple aggregate: SUM(x), COUNT(*), etc.
                                             {:aggs [(if alias-name
                                                       [:as simple-agg (keyword alias-name)]
                                                       simple-agg)]}
                                             ;; Compound: MAX(v1) - MIN(v2) AS alias, or CASE with agg
                                             (let [collected (collect-aggs-from-expr expr agg-counter)
                                                   agg-map (into {} (map (fn [[spec kw]] [spec kw]) collected))
                                                   post-expr (build-post-expr expr agg-map)
                                                   eff-alias (keyword (or alias-name
                                                                          (str "_case_" (swap! agg-counter inc))))]
                                               {:aggs (mapv (fn [[spec kw]] [:as spec kw]) collected)
                                                :post-agg {:alias eff-alias
                                                           :expr post-expr
                                                           :sources (mapv second collected)}})))))))
                             (vec)))
        aggs (when (seq agg-items-raw)
               (vec (mapcat :aggs agg-items-raw)))
        post-aggs (vec (keep :post-agg agg-items-raw))

        ;; Collect inner-agg specs from window functions (e.g. SUM(SUM(x)) OVER ...)
        ;; and inject them into the agg list so GROUP BY materializes them
        inner-aggs (when (seq window-specs)
                     (vec (keep :_inner-agg window-specs)))
        aggs (if (seq inner-aggs)
               (into (or aggs []) inner-aggs)
               aggs)
        ;; Strip :_inner-agg from window specs (query engine doesn't need it)
        window-specs (when (seq window-specs)
                       (mapv #(dissoc % :_inner-agg) window-specs))

        ;; Build _select-columns: describes each output column for final projection.
        ;; Used only when literals need injection into agg/group-by queries.
        ;; Agg specs use {:type :agg} without a key — the key is discovered
        ;; at apply-select-columns time by positional matching against result keys.
        select-column-specs
        (->> select-items
             (map-indexed
              (fn [idx ^SelectItem item]
                (let [expr (.getExpression item)
                      alias (.getAliasName item)]
                  (cond
                     ;; SELECT *
                    (instance? AllColumns expr)
                    (mapv (fn [k] {:type :ref :key k}) (keys from-data))

                     ;; Aggregate function
                    (select-item-is-agg? expr)
                    [{:type :agg :alias (when alias (keyword alias))}]

                     ;; Window function
                    (window-function? expr)
                    [{:type :ref :key (keyword (or alias (str "_win_" idx)))}]

                     ;; Column reference, literal, or expression
                    :else
                    (let [col-expr (translate-expr expr)]
                      [(cond
                         (keyword? col-expr)
                         {:type :ref :key (if alias (keyword alias) col-expr)
                          :source col-expr}

                         (number? col-expr)
                         {:type :literal :key (if alias (keyword alias)
                                                  (keyword (str col-expr)))
                          :value col-expr}

                         (string? col-expr)
                         {:type :literal :key (if alias (keyword alias)
                                                  (keyword (str "'" col-expr "'")))
                          :value col-expr}

                         :else ;; expression like [:* :a :b]
                         {:type :ref :key (if alias (keyword alias)
                                              (keyword (str "_expr_" idx)))})])))))
             (mapcat identity)
             (vec))

        ;; For non-aggregate SELECT without GROUP BY (pure projection)
        ;; Exclude window functions — they are handled separately
        projection (cond
                     ;; SELECT * — project all columns from the source table
                     (and (not has-agg?) (not has-group?) all-star?)
                     (vec (keys from-data))

                     ;; Explicit SELECT columns (non-aggregate, non-group)
                     (and (not has-agg?) (not has-group?) (not all-star?))
                     (->> select-items
                          (keep (fn [^SelectItem item]
                                  (let [expr (.getExpression item)]
                                    (when-not (window-function? expr)
                                      (let [alias-name (.getAliasName item)
                                            col-expr (translate-expr expr)]
                                        (if alias-name
                                          [:as col-expr (keyword alias-name)]
                                          col-expr))))))
                          (vec)))

        ;; Build WHERE predicates
        preds-raw (when where-expr
                    (translate-predicate where-expr))

        ;; Resolve subqueries: IN/NOT IN, EXISTS/NOT EXISTS
        exists-false? (atom false)
        preds (when (seq preds-raw)
                (into []
                      (mapcat (fn [pred]
                            ;; Normalize [:not [:exists-subquery ...]] → [:not-exists-subquery ...]
                                (let [pred (if (and (= :not (first pred))
                                                    (#{:exists-subquery :not-exists-subquery} (first (second pred))))
                                             (let [inner (second pred)
                                                   flipped (if (= :exists-subquery (first inner))
                                                             :not-exists-subquery :exists-subquery)]
                                               (into [flipped] (rest inner)))
                                             pred)]
                                  (case (first pred)
                                    (:in-subquery :not-in-subquery)
                                    (let [col (second pred)
                                          {:keys [subquery-select]} (nth pred 2)
                                          inner-query (translate-select subquery-select table-registry)
                                          inner-result (q/q inner-query)
                                          vals (if (sequential? inner-result)
                                                 (vec (distinct (map #(val (first %)) inner-result)))
                                                 [])]
                                      [(into [(if (= :in-subquery (first pred)) :in :not-in) col] vals)])

                                    (:exists-subquery :not-exists-subquery)
                                    (let [{:keys [subquery-select]} (first (rest pred))
                                          inner-query (translate-select subquery-select table-registry)
                                          inner-result (q/q (assoc inner-query :limit 1))
                                          has-rows? (if (sequential? inner-result) (pos? (count inner-result)) false)
                                          cond-met? (if (= :exists-subquery (first pred)) has-rows? (not has-rows?))]
                                      (when-not cond-met? (reset! exists-false? true))
                                      []) ;; EXISTS is resolved at parse time, no runtime predicate needed

                                    [pred])))
                              preds-raw)))

        ;; Build GROUP BY specs
        groups (when group-by
                 (let [group-exprs (.getGroupByExpressionList group-by)]
                   (mapv #(translate-group-expr % select-items) group-exprs)))

        ;; Build HAVING predicates
        having-preds (when having-expr
                       (translate-predicate having-expr))

        ;; Inject HAVING-referenced aggregates that aren't already in the agg list.
        ;; E.g., SELECT g, SUM(a) FROM t GROUP BY g HAVING AVG(a) > 4
        ;; needs AVG(a) computed even though it's not in SELECT.
        having-agg-specs (when having-expr
                           (collect-aggs-from-having-expr having-expr))
        existing-bare-aggs (set (map (fn [a] (if (= :as (first a)) (second a) a))
                                     (or aggs [])))
        having-only-aggs (vec (distinct (remove existing-bare-aggs (or having-agg-specs []))))
        ;; Give each HAVING-only agg an explicit alias matching its HAVING reference
        ;; key (:{op}_{col}). This ensures auto-alias-aggs won't rename it, and the
        ;; key used for stripping matches the actual result key.
        having-only-keys (when (seq having-only-aggs)
                           (set (map (fn [spec]
                                       (let [op-kw (first spec)
                                             col-kw (second spec)]
                                         (if col-kw
                                           (keyword (str (name op-kw) "_" (name col-kw)))
                                           op-kw)))
                                     having-only-aggs)))
        having-only-aliased (mapv (fn [spec]
                                    (let [op-kw (first spec)
                                          col-kw (second spec)
                                          alias (if col-kw
                                                  (keyword (str (name op-kw) "_" (name col-kw)))
                                                  op-kw)]
                                      [:as spec alias]))
                                  having-only-aggs)
        aggs (if (seq having-only-aliased)
               (into (or aggs []) having-only-aliased)
               aggs)

        ;; Build ORDER BY — inject aggregate expressions not already in agg list
        order-agg-injections
        (when (and order-by (or has-agg? has-group?))
          (vec (keep (fn [^OrderByElement elem]
                       (let [expr (.getExpression elem)]
                         (when (and (instance? Function expr)
                                    (aggregate-function? ^Function expr))
                           (let [agg-spec (translate-aggregate ^Function expr)
                                 ;; Build the alias key the same way translate-expr does
                                 ^Function f expr
                                 agg-name-upper (-> (.getName f) (.toUpperCase))
                                 params (when-let [p (.getParameters f)]
                                          (mapv translate-expr p))
                                 alias-kw (if (and (seq params) (keyword? (first params)))
                                            (keyword (str (.toLowerCase agg-name-upper) "_" (name (first params))))
                                            (keyword (.toLowerCase agg-name-upper)))]
                             {:spec agg-spec :alias alias-kw}))))
                     order-by)))
        ;; Filter out aggs already in the list
        order-agg-injections
        (when (seq order-agg-injections)
          (let [existing-aliases (set (map (fn [a]
                                             (if (= :as (first a))
                                               (nth a 2)
                                               (let [spec (if (= :as (first a)) (second a) a)]
                                                 (let [op-kw (first spec)
                                                       col-kw (second spec)]
                                                   (if col-kw
                                                     (keyword (str (name op-kw) "_" (name col-kw)))
                                                     op-kw)))))
                                           (or aggs [])))]
            (vec (remove #(contains? existing-aliases (:alias %)) order-agg-injections))))
        aggs (if (seq order-agg-injections)
               (into (or aggs [])
                     (mapv (fn [{:keys [spec alias]}]
                             [:as spec alias])
                           order-agg-injections))
               aggs)
        orders (when order-by
                 (mapv translate-order-element order-by))

        ;; Build LIMIT/OFFSET
        limit-val (when limit
                    (let [rc (.getRowCount limit)]
                      (when (instance? LongValue rc)
                        (.getValue ^LongValue rc))))
        offset-val (when offset
                     (let [ov (.getOffset offset)]
                       (when (instance? LongValue ov)
                         (.getValue ^LongValue ov))))

        ;; Build JOINs
        join-specs-raw (when (seq joins)
                         (mapv #(translate-join % table-registry from-table-name) joins))

        ;; Qualified column resolution: detect collisions and rewrite refs
        ;; Build join table info for ref-map
        join-table-infos (when (seq join-specs-raw)
                           (mapv (fn [^Join j]
                                   (when (instance? Table (.getFromItem j))
                                     (let [t ^Table (.getFromItem j)
                                           alias (table-name t)
                                           real-name (.getName t)
                                           data (get table-registry real-name)]
                                       {:alias alias
                                        :cols (set (keys data))})))
                                 joins))

        ;; Build ref-map when joins exist and there are collisions
        [ref-map collision-set renamed-keys-by-table]
        (if (seq join-table-infos)
          (build-join-ref-map from-table-name
                              (set (keys from-data))
                              (filterv some? join-table-infos))
          [nil nil nil])

        ;; Apply rewriting when ref-map is non-empty and has actual renames
        has-renames? (and ref-map (seq renamed-keys-by-table))

        ;; Rename join :with data keys for colliding columns
        join-specs (if has-renames?
                     (mapv (fn [spec table-info]
                             (if-let [renames (and table-info
                                                   (get renamed-keys-by-table (:alias table-info)))]
                               (update spec :with rename-join-data-keys renames)
                               spec))
                           join-specs-raw
                           (concat join-table-infos (repeat nil)))
                     join-specs-raw)

        ;; Rewrite all refs through ref-map
        preds (if has-renames? (rewrite-refs ref-map preds) preds)
        projection (if (and has-renames? projection) (rewrite-refs ref-map projection) projection)
        groups (if has-renames? (rewrite-refs ref-map groups) groups)
        having-preds (if has-renames? (rewrite-refs ref-map having-preds) having-preds)
        orders (if has-renames? (rewrite-refs ref-map orders) orders)
        aggs (if has-renames? (rewrite-refs ref-map aggs) aggs)
        window-specs (if has-renames? (rewrite-refs ref-map window-specs) window-specs)
        join-specs (if has-renames?
                     (mapv (fn [spec]
                             (update spec :on (partial rewrite-refs ref-map)))
                           join-specs)
                     join-specs)
        select-column-specs (if has-renames?
                              (mapv (fn [spec]
                                      (cond-> spec
                                        (:source spec) (update :source #(rewrite-ref ref-map %))
                                        (:key spec) (update :key #(rewrite-ref ref-map %))))
                                    select-column-specs)
                              select-column-specs)
        ;; For SELECT * with joins: expand to include renamed join columns
        projection (if (and has-renames? all-star? (not has-agg?) (not has-group?))
                     (let [from-keys (vec (keys from-data))
                           join-keys (mapcat (fn [spec table-info]
                                               (when table-info
                                                 (let [renames (get renamed-keys-by-table (:alias table-info))
                                                       cols (keys (:with spec))]
                                                   (mapv (fn [k]
                                                           (if (and renames (get renames k))
                                                             (get renames k)
                                                             k))
                                                         cols))))
                                             join-specs
                                             (concat join-table-infos (repeat nil)))]
                       (into from-keys join-keys))
                     projection)

        ;; Assemble query map
        query (cond-> {:from from-data}
                (seq preds) (assoc :where (vec preds))
                (seq aggs) (assoc :agg aggs)
                (seq groups) (assoc :group groups)
                (seq having-preds) (assoc :having (vec having-preds))
                (seq orders) (assoc :order orders)
                limit-val (assoc :limit limit-val)
                @exists-false? (assoc :limit 0)
                offset-val (assoc :offset offset-val)
                distinct? (assoc :distinct true)
                (seq join-specs) (assoc :join join-specs)
                projection (assoc :select projection)
                (seq window-specs) (assoc :window window-specs)
                (seq post-aggs) (assoc :_post-aggs post-aggs)
                (seq having-only-keys) (assoc :_having-only-keys having-only-keys)
                (seq order-agg-injections) (assoc :_order-only-keys
                                                  (set (map :alias order-agg-injections)))
                ;; Only attach _select-columns when literals need injection into
                ;; an aggregate/group-by query (the bug: literals are dropped).
                ;; Pure projection queries use :select; don't interfere.
                (and (or has-agg? has-group?)
                     (some #(= :literal (:type %)) select-column-specs))
                (assoc :_select-columns select-column-specs))]
    query))

;; ============================================================================
;; Post-aggregate expression evaluation
;; ============================================================================

(defn- eval-post-expr
  "Evaluate a post-aggregate expression against a result row.
   Returns double for arithmetic, or any value for CASE-post (strings, etc.)."
  [expr row]
  (cond
    (keyword? expr) (let [v (get row expr 0)] (if (number? v) (double v) v))
    (number? expr)  (double expr)
    (string? expr)  expr
    (nil? expr)     nil
    (vector? expr)  (let [[op a b c] expr]
                      (case op
                        :+ (+ (double (eval-post-expr a row)) (double (eval-post-expr b row)))
                        :- (- (double (eval-post-expr a row)) (double (eval-post-expr b row)))
                        :* (* (double (eval-post-expr a row)) (double (eval-post-expr b row)))
                        :/ (let [denom (double (eval-post-expr b row))]
                             (if (zero? denom) Double/NaN (/ (double (eval-post-expr a row)) denom)))
                        ;; Comparison operators for CASE post-agg conditions
                        :>  (> (double (eval-post-expr a row)) (double (eval-post-expr b row)))
                        :>= (>= (double (eval-post-expr a row)) (double (eval-post-expr b row)))
                        :<  (< (double (eval-post-expr a row)) (double (eval-post-expr b row)))
                        :<= (<= (double (eval-post-expr a row)) (double (eval-post-expr b row)))
                        :=  (= (eval-post-expr a row) (eval-post-expr b row))
                        :!= (not= (eval-post-expr a row) (eval-post-expr b row))
                        ;; CASE post-agg expression
                        :case-post
                        (let [whens a  ;; vector of {:cond ... :then ...}
                              else-val b]
                          (or (some (fn [{:keys [cond then]}]
                                      (when (eval-post-expr cond row)
                                        (eval-post-expr then row)))
                                    whens)
                              (eval-post-expr else-val row)))))
    :else 0.0))

(defn apply-post-aggs
  "Apply post-aggregate expressions to query results.
   Removes internal agg keys and adds the computed alias keys."
  [results post-aggs]
  (if (or (empty? post-aggs) (not (sequential? results)))
    results
    (let [source-keys (set (mapcat :sources post-aggs))]
      (mapv (fn [row]
              (let [computed (reduce (fn [r {:keys [alias expr]}]
                                       (assoc r alias (eval-post-expr expr r)))
                                     row post-aggs)]
                ;; Remove internal agg keys that are only used for computation
                (apply dissoc computed source-keys)))
            results))))

(defn apply-select-columns
  "Reshape query results to match SQL SELECT clause.
   Injects literal values into result rows, reorders columns to match SELECT order.
   Agg keys are discovered by positional matching: ref/literal keys are removed from
   the result key set, remaining keys are matched to :agg specs in order."
  [results select-columns]
  (cond
    (or (empty? select-columns) (not (sequential? results)) (empty? results))
    results

    :else
    (let [;; Discover agg keys by removing known ref keys from result keys
          ref-keys (set (keep (fn [spec]
                                (when (= :ref (:type spec))
                                  (or (:source spec) (:key spec))))
                              select-columns))
          first-row (first results)
          result-keys (vec (keys first-row))
          ;; Agg keys = result keys not claimed by :ref specs, in result order
          agg-keys (filterv #(not (contains? ref-keys %)) result-keys)
          ;; Assign agg keys to agg specs positionally
          agg-key-seq (atom (seq agg-keys))
          resolved-specs (mapv (fn [spec]
                                 (if (= :agg (:type spec))
                                   (let [k (first @agg-key-seq)]
                                     (swap! agg-key-seq rest)
                                     (assoc spec :key (or (:alias spec) k)))
                                   spec))
                               select-columns)]
      (mapv (fn [row]
              (reduce (fn [out spec]
                        (case (:type spec)
                          :literal (assoc out (:key spec) (:value spec))
                          :ref (assoc out (:key spec)
                                      (get row (or (:source spec) (:key spec))))
                          :agg (assoc out (:key spec) (get row (:key spec)))
                          out))
                      (array-map) resolved-specs))
            results))))

;; ============================================================================
;; System queries (psql/DBeaver compatibility)
;; ============================================================================

(defn- parse-model-options
  "Parse model OPTIONS string into a keyword map.
   \"n_trees = 200, sample_size = 256, contamination = 0.01\"
   → {:n-trees 200, :sample-size 256, :contamination 0.01}"
  [^String opts-str]
  (when opts-str
    (into {}
          (keep (fn [pair]
                  (let [pair (str/trim pair)]
                    (when-not (str/blank? pair)
                      (let [[k v] (str/split pair #"\s*=\s*" 2)
                            k (keyword (str/replace (str/trim k) "_" "-"))
                            v (str/trim v)]
                        [k (cond
                             (re-matches #"-?\d+" v) (Long/parseLong v)
                             (re-matches #"-?\d+\.\d+" v) (Double/parseDouble v)
                             :else v)])))))
          (str/split opts-str #","))))

(def ^:private system-query-patterns
  "Patterns for system queries that clients send on connect.
   Model management patterns (CREATE/DROP/SHOW/DESCRIBE MODEL) must precede
   the generic SHOW handler."
  [;; --- Model management (before generic SHOW) ---
   {:pattern #"(?is)^\s*CREATE\s+MODEL\s+"
    :handler (fn [sql _reg]
               (if-let [[_ model-name model-type opts-str training-sql]
                        (re-matches #"(?is)^\s*CREATE\s+MODEL\s+(\S+)\s+TYPE\s+(\S+)\s+OPTIONS\s*\(([^)]*)\)\s+AS\s+(SELECT\s+.+?)\s*;?\s*$" sql)]
                 {:ddl {:op :create-model
                        :model-name model-name
                        :model-type (str/upper-case model-type)
                        :options (parse-model-options opts-str)
                        :training-sql training-sql}}
                 ;; Try without OPTIONS clause
                 (if-let [[_ model-name model-type training-sql]
                          (re-matches #"(?is)^\s*CREATE\s+MODEL\s+(\S+)\s+TYPE\s+(\S+)\s+AS\s+(SELECT\s+.+?)\s*;?\s*$" sql)]
                   {:ddl {:op :create-model
                          :model-name model-name
                          :model-type (str/upper-case model-type)
                          :options {}
                          :training-sql training-sql}}
                   {:error "Invalid CREATE MODEL syntax. Expected: CREATE MODEL <name> TYPE <type> [OPTIONS (...)] AS SELECT ..."})))}

   {:pattern #"(?i)^\s*DROP\s+MODEL\s+"
    :handler (fn [sql _reg]
               (let [if-exists? (boolean (re-find #"(?i)IF\s+EXISTS" sql))
                     model-name (if if-exists?
                                  (second (re-find #"(?i)DROP\s+MODEL\s+IF\s+EXISTS\s+(\S+)" sql))
                                  (second (re-find #"(?i)DROP\s+MODEL\s+(\S+)" sql)))]
                 {:ddl {:op :drop-model
                        :model-name (str/replace (or model-name "") #"(?i)\s*;?\s*$" "")
                        :if-exists? if-exists?}}))}

   {:pattern #"(?i)^\s*SHOW\s+MODELS\s*;?\s*$"
    :handler (fn [_sql reg]
               (let [models (get reg "__models__")]
                 {:system true
                  :result {:columns ["name" "type" "n_features" "n_trees" "sample_size"]
                           :oids [25 25 20 20 20]
                           :rows (vec (map (fn [[name model]]
                                             [name
                                              (or (:model-type model) "ISOLATION_FOREST")
                                              (str (:n-features model))
                                              (str (:n-trees model))
                                              (str (:sample-size model))])
                                           models))}
                  :tag "SHOW MODELS"}))}

   {:pattern #"(?i)^\s*DESCRIBE\s+MODEL\s+(\S+)\s*;?\s*$"
    :handler (fn [sql reg]
               (let [model-name (str/replace
                                 (second (re-find #"(?i)DESCRIBE\s+MODEL\s+(\S+)" sql))
                                 #"(?i)\s*;?\s*$" "")
                     model (get-in reg ["__models__" model-name])]
                 (if model
                   {:system true
                    :result {:columns ["property" "value"]
                             :oids [25 25]
                             :rows [["model_name" model-name]
                                    ["model_type" (or (:model-type model) "ISOLATION_FOREST")]
                                    ["n_trees" (str (:n-trees model))]
                                    ["sample_size" (str (:sample-size model))]
                                    ["n_features" (str (:n-features model))]
                                    ["features" (str/join ", " (map name (:feature-names model)))]
                                    ["contamination" (str (or (:contamination model) "not set"))]
                                    ["threshold" (str (or (:threshold model) "not set"))]]}
                    :tag "DESCRIBE MODEL"}
                   {:error (str "Model not found: " model-name)})))}

   ;; --- Standard system queries ---
   {:pattern #"(?i)^\s*SET\s+"
    :handler (fn [_sql _reg] {:system true :tag "SET"})}
   {:pattern #"(?i)^\s*SHOW\s+"
    :handler (fn [sql _reg]
               (let [param (second (re-find #"(?i)SHOW\s+(\S+)" sql))]
                 {:system true
                  :result {:columns ["name" "setting"]
                           :oids [25 25]
                           :rows [[(or param "unknown") ""]]}
                  :tag "SHOW"}))}
   {:pattern #"(?i)^\s*RESET\s+"
    :handler (fn [_sql _reg] {:system true :tag "RESET"})}
   {:pattern #"(?i)^\s*BEGIN"
    :handler (fn [_sql _reg] {:system true :tag "BEGIN"})}
   {:pattern #"(?i)^\s*COMMIT"
    :handler (fn [_sql _reg] {:system true :tag "COMMIT"})}
   {:pattern #"(?i)^\s*ROLLBACK"
    :handler (fn [_sql _reg] {:system true :tag "ROLLBACK"})}
   {:pattern #"(?i)^\s*DISCARD\s+"
    :handler (fn [_sql _reg] {:system true :tag "DISCARD ALL"})}
   {:pattern #"(?i)^\s*LISTEN\s+"
    :handler (fn [_sql _reg] {:system true :tag "LISTEN"})}
   {:pattern #"(?i)^\s*UNLISTEN\s+"
    :handler (fn [_sql _reg] {:system true :tag "UNLISTEN"})}
   {:pattern #"(?i)^\s*DEALLOCATE\s+"
    :handler (fn [_sql _reg] {:system true :tag "DEALLOCATE"})}
   {:pattern #"(?i)^\s*CLOSE\s+"
    :handler (fn [_sql _reg] {:system true :tag "CLOSE"})}])

(defn- check-system-query
  "Check if SQL is a system query (SET, SHOW, etc.). Returns map or nil."
  [sql table-registry]
  (some (fn [{:keys [pattern handler]}]
          (when (re-find pattern sql)
            (handler sql table-registry)))
        system-query-patterns))

(defn- handle-version-query
  "Check for SELECT VERSION() and similar."
  [^PlainSelect select]
  (let [items (.getSelectItems select)]
    (when (= 1 (count items))
      (let [expr (.getExpression ^SelectItem (first items))]
        (when (instance? Function expr)
          (let [^Function f expr
                name (-> (.getName f) (.toUpperCase))]
            (case name
              "VERSION" {:system true
                         :result {:columns ["version"]
                                  :oids [25]
                                  :rows [["Stratum 0.1.0 (PostgreSQL 15.0 compatible)"]]}
                         :tag "SELECT 1"}
              "CURRENT_DATABASE" {:system true
                                  :result {:columns ["current_database"]
                                           :oids [25]
                                           :rows [["stratum"]]}
                                  :tag "SELECT 1"}
              "CURRENT_SCHEMA" {:system true
                                :result {:columns ["current_schema"]
                                         :oids [25]
                                         :rows [["public"]]}
                                :tag "SELECT 1"}
              "CURRENT_USER" {:system true
                              :result {:columns ["current_user"]
                                       :oids [25]
                                       :rows [["stratum"]]}
                              :tag "SELECT 1"}
              nil)))))))

(def ^:private pg-type-oids
  "Common PostgreSQL type OIDs."
  {:int8 20
   :int4 23
   :float8 701
   :float4 700
   :text 25
   :varchar 1043
   :bool 16
   :date 1082
   :timestamp 1114
   :oid 26
   :name 19})

(defn- pg-catalog-table
  "Identify which pg_catalog table is being queried. Returns keyword or nil."
  [sql]
  (let [sql-lower (.toLowerCase ^String sql)]
    (cond
      (or (.contains sql-lower "pg_database")
          (.contains sql-lower "pg_catalog.pg_database"))
      :pg_database

      (or (.contains sql-lower "pg_class")
          (.contains sql-lower "pg_catalog.pg_class"))
      :pg_class

      (or (.contains sql-lower "pg_namespace")
          (.contains sql-lower "pg_catalog.pg_namespace"))
      :pg_namespace

      (or (.contains sql-lower "pg_type")
          (.contains sql-lower "pg_catalog.pg_type"))
      :pg_type

      (or (.contains sql-lower "pg_attribute")
          (.contains sql-lower "pg_catalog.pg_attribute"))
      :pg_attribute

      (or (.contains sql-lower "pg_tables")
          (.contains sql-lower "information_schema.tables"))
      :pg_tables

      (.contains sql-lower "information_schema")
      :information_schema

      (or (.contains sql-lower "pg_settings")
          (.contains sql-lower "pg_roles")
          (.contains sql-lower "pg_stat_"))
      :other_catalog

      :else nil)))

(defn- handle-pg-database
  "Return mock pg_database rows for \\l support."
  []
  (let [rows [["stratum" "10" "6" "en_US.UTF-8" "en_US.UTF-8" "t"]]]
    {:system true
     :result {:columns ["datname" "datdba" "encoding" "datcollate" "datctype" "datistemplate"]
              :oids [(:name pg-type-oids) (:oid pg-type-oids) (:int4 pg-type-oids)
                     (:text pg-type-oids) (:text pg-type-oids) (:bool pg-type-oids)]
              :rows rows}
     :tag (str "SELECT " (count rows))}))

(defn- handle-pg-namespace
  "Return mock pg_namespace rows."
  []
  (let [rows [["2200" "public" "10"]]]
    {:system true
     :result {:columns ["oid" "nspname" "nspowner"]
              :oids [(:oid pg-type-oids) (:name pg-type-oids) (:oid pg-type-oids)]
              :rows rows}
     :tag (str "SELECT " (count rows))}))

(defn- handle-pg-class
  "Return mock pg_class rows for \\dt support using registered tables."
  [table-registry]
  (let [rows (mapv (fn [[table-name _columns]]
                     [table-name "2200" "r" "10"])
                   table-registry)]
    {:system true
     :result {:columns ["relname" "relnamespace" "relkind" "relowner"]
              :oids [(:name pg-type-oids) (:oid pg-type-oids) 18 (:oid pg-type-oids)]
              :rows rows}
     :tag (str "SELECT " (count rows))}))

(defn- handle-pg-type
  "Return minimal pg_type rows for type introspection."
  []
  (let [rows [["20" "int8" "8"]
              ["23" "int4" "4"]
              ["701" "float8" "8"]
              ["25" "text" "-1"]
              ["1043" "varchar" "-1"]
              ["16" "bool" "1"]
              ["1082" "date" "4"]
              ["1114" "timestamp" "8"]]]
    {:system true
     :result {:columns ["oid" "typname" "typlen"]
              :oids [(:oid pg-type-oids) (:name pg-type-oids) (:int4 pg-type-oids)]
              :rows rows}
     :tag (str "SELECT " (count rows))}))

(defn- handle-pg-attribute
  "Return mock pg_attribute rows. Minimal implementation for client compatibility."
  []
  {:system true
   :result {:columns ["attname" "atttypid" "attnum"]
            :oids [(:name pg-type-oids) (:oid pg-type-oids) (:int4 pg-type-oids)]
            :rows []}
   :tag "SELECT 0"})

(defn- handle-pg-tables
  "Return pg_tables view rows for registered tables."
  [table-registry]
  (let [rows (mapv (fn [[table-name _columns]]
                     ["public" table-name "stratum" nil "false" "false" "false" "false"])
                   table-registry)]
    {:system true
     :result {:columns ["schemaname" "tablename" "tableowner" "tablespace"
                        "hasindexes" "hasrules" "hastriggers" "rowsecurity"]
              :oids [(:name pg-type-oids) (:name pg-type-oids) (:name pg-type-oids)
                     (:name pg-type-oids) (:bool pg-type-oids) (:bool pg-type-oids)
                     (:bool pg-type-oids) (:bool pg-type-oids)]
              :rows rows}
     :tag (str "SELECT " (count rows))}))

(defn- handle-pg-catalog
  "Dispatch to appropriate catalog handler based on detected table."
  [sql table-registry]
  (case (pg-catalog-table sql)
    :pg_database (handle-pg-database)
    :pg_class (handle-pg-class table-registry)
    :pg_namespace (handle-pg-namespace)
    :pg_type (handle-pg-type)
    :pg_attribute (handle-pg-attribute)
    :pg_tables (handle-pg-tables table-registry)
    :information_schema {:system true
                         :result {:columns [] :oids [] :rows []}
                         :tag "SELECT 0"}
    :other_catalog {:system true
                    :result {:columns [] :oids [] :rows []}
                    :tag "SELECT 0"}
    {:system true
     :result {:columns [] :oids [] :rows []}
     :tag "SELECT 0"}))

(defn- handle-show-tables
  "Handle \\dt equivalent — list registered tables."
  [table-registry]
  (let [table-names (sort (keys table-registry))
        rows (mapv (fn [name] [name]) table-names)]
    {:system true
     :result {:columns ["table_name"] :oids [25] :rows rows}
     :tag (str "SELECT " (count rows))}))

;; ============================================================================
;; DDL translation (CREATE TABLE, INSERT INTO)
;; ============================================================================

(defn- sql-type->stratum-type
  "Map SQL column type names to Stratum types."
  [^String type-str]
  (let [t (.toUpperCase type-str)]
    (cond
      (or (= t "INTEGER") (= t "INT") (= t "BIGINT") (= t "SMALLINT")
          (= t "TINYINT") (= t "INT4") (= t "INT8") (= t "SERIAL"))
      :int64

      (or (= t "DOUBLE") (= t "FLOAT") (= t "REAL") (= t "NUMERIC")
          (= t "DECIMAL") (= t "DOUBLE PRECISION") (= t "FLOAT8") (= t "FLOAT4"))
      :float64

      (or (= t "VARCHAR") (= t "TEXT") (= t "CHAR") (= t "STRING")
          (.startsWith t "VARCHAR(") (.startsWith t "CHAR("))
      :string

      :else :string)))

(defn- translate-create-table
  "Translate a JSqlParser CreateTable into a DDL descriptor."
  [^CreateTable stmt]
  (let [table-name (.toString (.getTable stmt))
        col-defs (.getColumnDefinitions stmt)]
    {:ddl {:op      :create-table
           :table   table-name
           :columns (mapv (fn [^ColumnDefinition cd]
                            {:name (.getColumnName cd)
                             :type (sql-type->stratum-type
                                    (str (.getColDataType cd)))})
                          col-defs)}}))

(defn- parse-insert-value
  "Convert a JSqlParser expression from INSERT VALUES to a Clojure value."
  [expr]
  (cond
    (instance? LongValue expr)
    (.getValue ^LongValue expr)

    (instance? DoubleValue expr)
    (.getValue ^DoubleValue expr)

    (instance? StringValue expr)
    (.getValue ^StringValue expr)

    (instance? NullValue expr)
    nil

    (instance? SignedExpression expr)
    (let [^SignedExpression se expr
          sign (.getSign se)
          inner (parse-insert-value (.getExpression se))]
      (when inner
        (if (= sign \-)
          (if (instance? Long inner) (- (long inner)) (- (double inner)))
          inner)))

    :else
    (throw (ex-info (str "Unsupported INSERT value expression: " (type expr))
                    {:expr (str expr)}))))

(defn- extract-row-values
  "Extract values from a row expression (ParenthesedExpressionList or flat)."
  [expr]
  (if (instance? ParenthesedExpressionList expr)
    (mapv parse-insert-value
          (.getExpressions ^ParenthesedExpressionList expr))
    ;; Single value in a flat single-row insert
    [(parse-insert-value expr)]))

(defn- translate-insert
  "Translate a JSqlParser Insert into a DDL descriptor.
   Supports INSERT ... ON CONFLICT (UPSERT)."
  [^Insert stmt]
  (let [table-name (.toString (.getTable stmt))
        ^Values vals (.getValues stmt)
        exprs (.getExpressions vals)
        ;; Single-row inserts have flat expressions, multi-row have PELs
        rows (if (instance? ParenthesedExpressionList (first exprs))
               (mapv extract-row-values exprs)
               [(mapv parse-insert-value exprs)])
        conflict-action (.getConflictAction stmt)
        conflict-target (.getConflictTarget stmt)]
    (if conflict-action
      (let [action-type (.getConflictActionType ^InsertConflictAction conflict-action)
            conflict-cols (when conflict-target
                            (mapv keyword (.getIndexColumnNames ^InsertConflictTarget conflict-target)))
            is-update? (= action-type ConflictActionType/DO_UPDATE)
            update-sets (when is-update?
                          (mapv (fn [^UpdateSet us]
                                  (let [col (keyword (.getColumnName ^Column (first (.getColumns us))))
                                        expr (translate-expr (first (.getValues us)))]
                                    {:col col :expr expr}))
                                (.getUpdateSets ^InsertConflictAction conflict-action)))]
        {:ddl {:op            :upsert
               :table         table-name
               :rows          rows
               :conflict-cols conflict-cols
               :action        (if is-update? :do-update :do-nothing)
               :assignments   (or update-sets [])}})
      {:ddl {:op     :insert
             :table  table-name
             :rows   rows}})))

(defn- translate-update
  "Translate a JSqlParser Update into a DDL descriptor.
   Supports UPDATE ... FROM for joined updates."
  [^Update stmt]
  (let [table-obj (.getTable stmt)
        table-name (let [s (.toString table-obj)]
                     ;; Strip alias from table name if present (e.g. "orders o" → "orders")
                     (.getName table-obj))
        table-alias (when-let [a (.getAlias table-obj)] (.getName a))
        from-item (.getFromItem stmt)
        from-table (when (instance? Table from-item)
                     (let [^Table ft from-item]
                       {:table (.getName ft)
                        :alias (when-let [a (.getAlias ft)] (.getName a))}))
        where-expr (.getWhere stmt)
        where-preds (when where-expr (translate-predicate where-expr))
        assignments (mapv (fn [^UpdateSet us]
                            (let [col (keyword (.getColumnName ^Column (first (.getColumns us))))
                                  expr (translate-expr (first (.getValues us)))]
                              {:col col :expr expr}))
                          (.getUpdateSets stmt))]
    {:ddl (cond-> {:op          :update
                   :table       table-name
                   :assignments assignments
                   :where       where-preds}
            table-alias (assoc :table-alias table-alias)
            from-table (assoc :from from-table))}))

(defn- translate-delete
  "Translate a JSqlParser Delete into a DDL descriptor."
  [^Delete stmt]
  (let [table-name (.toString (.getTable stmt))
        where-expr (.getWhere stmt)
        where-preds (when where-expr (translate-predicate where-expr))]
    {:ddl {:op    :delete
           :table table-name
           :where where-preds}}))

;; ============================================================================
;; File path validation
;; ============================================================================

(defn- validate-file-path
  "Validate a file path extracted from SQL to prevent path traversal attacks.
   Rejects paths containing '..' components. Called before any file I/O."
  [^String path]
  (when (re-find #"(^|[/\\])\.\.[/\\]|^\.\.$|(^|[/\\])\.\.$" path)
    (throw (ex-info (str "Path traversal not allowed: " path)
                    {:path path :reason :path-traversal})))
  path)

;; ============================================================================
;; Public API
;; ============================================================================

(defn parse-sql
  "Parse a SQL string and translate to a Stratum query map.

   Returns:
     {:query {...}}       — translated query map for q/execute
     {:ddl {:op ...}}     — DDL/DML statement (create-table, insert, update, delete, upsert)
     {:system true ...}   — system query result (SET, SHOW, VERSION, etc.)
     {:explain {...}}     — EXPLAIN result (execution plan)
     {:error \"message\"} — parse or translation error"
  [sql table-registry]
  (try
    ;; Check for EXPLAIN prefix
    (if-let [[_ inner-sql] (re-matches #"(?is)\s*EXPLAIN\s+(.*)" sql)]
      ;; Parse the inner SQL, return as :explain
      (let [result (parse-sql inner-sql table-registry)]
        (cond
          (:error result) result
          (:system result) {:explain {:strategy :system :tag (:tag result)}}
          (:query result) {:explain (:query result)}
          :else result))

      ;; Normal parsing
      (or
        ;; Check system queries first (SET, SHOW, BEGIN, etc.)
       (check-system-query sql table-registry)

;; Check pg_catalog queries
       (when (pg-catalog-table sql)
         (handle-pg-catalog sql table-registry))

        ;; Parse with JSqlParser
       (let [stmt (CCJSqlParserUtil/parse ^String sql)]
         (cond
           (instance? PlainSelect stmt)
           (let [^PlainSelect select stmt
                  ;; Handle CTEs: WITH cte AS (SELECT ...) SELECT ...
                  ;; CTEs are materialized and added to the table registry
                 enriched-registry
                 (if-let [with-items (.getWithItemsList select)]
                   (reduce (fn [reg ^WithItem wi]
                             (let [cte-name (.getAliasName wi)
                                   inner-select (.getPlainSelect (.getSelect wi))
                                   cte-query (translate-select inner-select reg)
                                   cte-result (q/q cte-query)
                                   cte-cols (q/results->columns cte-result)]
                               (assoc reg cte-name cte-cols)))
                           table-registry
                           with-items)
                   table-registry)]
              ;; Check for VERSION() etc.
             (or (handle-version-query select)

                  ;; Check for "SHOW TABLES" style
                 (when-let [from (.getFromItem select)]
                   (when (and (instance? Table from)
                              (let [name (.getName ^Table from)]
                                (or (= name "pg_tables")
                                    (= name "tables"))))
                     (handle-show-tables enriched-registry)))

                  ;; Check for table functions (read_csv, read_parquet)
                  ;; Note: JSqlParser parses read_csv('path') as a table name string,
                  ;; so we check the raw SQL for the pattern instead
                 (let [table-func (when-let [[_ func path]
                                             (re-find #"(?i)\bFROM\s+(read_csv|read_parquet)\s*\(\s*'([^']+)'\s*\)" sql)]
                                    (validate-file-path path)
                                    [(str func "(" path ")") (.toLowerCase ^String func) path])]
                   (when table-func
                     (let [[full-name func-name path] table-func
                           table-data (case func-name
                                        "read_csv"     (csv/from-csv path)
                                        "read_parquet" (parquet/from-parquet path))
                            ;; Re-parse with the table data in registry
                           fixed-sql (.replace ^String sql ^String full-name "__file_table__")
                           fixed-registry (assoc enriched-registry "__file_table__" table-data)]
                       {:query (translate-select
                                ^PlainSelect (CCJSqlParserUtil/parse ^String fixed-sql)
                                fixed-registry)})))

                  ;; Normal SELECT translation
                 {:query (translate-select select enriched-registry)}))

            ;; UNION / UNION ALL / INTERSECT / EXCEPT
           (instance? SetOperationList stmt)
           (let [^SetOperationList sol stmt
                 selects (.getSelects sol)
                 operations (.getOperations sol)
                 sub-queries (mapv (fn [^net.sf.jsqlparser.statement.select.Select s]
                                     (if (instance? PlainSelect s)
                                       (translate-select ^PlainSelect s table-registry)
                                       (throw (ex-info "Non-PlainSelect in set operation not supported"
                                                       {:type (type s)}))))
                                   selects)
                  ;; Determine operation type from first operation
                 first-op (first operations)
                 op-type (cond
                           (instance? IntersectOp first-op) :intersect
                           (or (instance? ExceptOp first-op)
                               (instance? MinusOp first-op)) :except
                           :else :union)
                 all? (and (= :union op-type)
                           (every? (fn [op] (and (instance? UnionOp op) (.isAll ^UnionOp op)))
                                   operations))]
             {:query {:_set-op {:op op-type :queries sub-queries :all? all?}}})

           ;; CREATE TABLE
           (instance? CreateTable stmt)
           (translate-create-table stmt)

           ;; INSERT INTO
           (instance? Insert stmt)
           (translate-insert stmt)

           ;; UPDATE
           (instance? Update stmt)
           (translate-update stmt)

           ;; DELETE
           (instance? Delete stmt)
           (translate-delete stmt)

           ;; DROP TABLE
           (instance? Drop stmt)
           (let [^Drop d stmt]
             (when (= "TABLE" (.getType d))
               {:ddl {:op :drop-table :table (str (.getName d))}}))

           :else
           {:error (str "Unsupported SQL statement type: " (type stmt))}))))

    (catch Exception e
      {:error (.getMessage e)})))

;; ============================================================================
;; Result formatting for pgwire
;; ============================================================================

;; PostgreSQL type OIDs (matching PgWireServer.java constants)
(def ^:private ^:const OID_INT8 20)
(def ^:private ^:const OID_FLOAT8 701)
(def ^:private ^:const OID_TEXT 25)

(defn- value->string
  "Convert a Clojure value to a string for pgwire text format."
  [v]
  (cond
    (nil? v) nil
    (instance? Double v) (let [d (double v)]
                           (if (Double/isNaN d) nil (str d)))
    (instance? Float v) (let [f (float v)]
                          (if (Float/isNaN f) nil (str f)))
    :else (str v)))

(defn- infer-oid
  "Infer PostgreSQL OID from a result value."
  [v]
  (cond
    (nil? v) OID_TEXT
    (instance? Long v) OID_INT8
    (integer? v) OID_INT8
    (instance? Double v) OID_FLOAT8
    (float? v) OID_FLOAT8
    :else OID_TEXT))

(defn format-results
  "Format Stratum query results into a PgWireServer.QueryResult."
  [results]
  (cond
    ;; System query with pre-formatted result
    (and (:system results) (:result results))
    (let [{:keys [columns oids rows]} (:result results)
          tag (:tag results)]
      (PgWireServer$QueryResult.
       (into-array String columns)
       (int-array oids)
       (into-array (Class/forName "[Ljava.lang.String;")
                   (mapv #(into-array String %) rows))
       (str tag)))

    ;; System query with no result (SET, BEGIN, etc.)
    (:system results)
    (PgWireServer$QueryResult/empty (str (:tag results)))

    ;; Error
    (:error results)
    (PgWireServer$QueryResult. ^String (:error results))

    ;; Columnar result format
    (and (map? results) (:n-rows results))
    (let [n-rows (long (:n-rows results))
          col-keys (vec (remove #{:n-rows} (keys results)))
          col-names (mapv name col-keys)
          ;; Infer OIDs from first row values
          oids (int-array (map (fn [k]
                                 (let [arr (get results k)]
                                   (cond
                                     (instance? (Class/forName "[J") arr) OID_INT8
                                     (instance? (Class/forName "[D") arr) OID_FLOAT8
                                     :else OID_TEXT)))
                               col-keys))
          rows (into-array (Class/forName "[Ljava.lang.String;")
                           (for [i (range n-rows)]
                             (into-array String
                                         (for [k col-keys]
                                           (let [arr (get results k)]
                                             (cond
                                               (instance? (Class/forName "[J") arr)
                                               (let [v (aget ^longs arr (int i))]
                                                 (if (= v Long/MIN_VALUE) nil (str v)))
                                               (instance? (Class/forName "[D") arr)
                                               (let [v (aget ^doubles arr (int i))]
                                                 (if (Double/isNaN v) nil (str v)))
                                               (instance? (Class/forName "[Ljava.lang.String;") arr)
                                               (aget ^"[Ljava.lang.String;" arr (int i))
                                               :else (str (nth (seq arr) i))))))))]
      (PgWireServer$QueryResult.
       (into-array String col-names)
       oids
       rows
       (str "SELECT " n-rows)))

    ;; Vector of maps (standard Stratum result)
    (sequential? results)
    (if (empty? results)
      (PgWireServer$QueryResult.
       (into-array String [])
       (int-array [])
       (into-array (Class/forName "[Ljava.lang.String;") [])
       "SELECT 0")
      (let [first-row (first results)
            col-keys (vec (keys first-row))
            col-names (mapv name col-keys)
            oids (int-array (map #(infer-oid (get first-row %)) col-keys))
            rows (into-array (Class/forName "[Ljava.lang.String;")
                             (mapv (fn [row]
                                     (into-array String
                                                 (mapv #(value->string (get row %)) col-keys)))
                                   results))]
        (PgWireServer$QueryResult.
         (into-array String col-names)
         oids
         rows
         (str "SELECT " (count results)))))

    ;; Single map (non-grouped aggregate)
    (map? results)
    (format-results [results])

    :else
    (PgWireServer$QueryResult. (str "Unexpected result type: " (type results)))))
