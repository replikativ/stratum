(ns stratum.sql.rewrite
  "Pre-parse SQL rewriter for non-standard syntax that JSqlParser doesn't
   recognize natively.

   Currently handles only ASOF JOIN (DuckDB form): the rewriter strips the
   ASOF keyword and returns a side-channel list of markers indicating which
   top-level joins were originally ASOF. The downstream translator consults
   this list by ordinal position to decide whether to build an ASOF join spec.

   The walker tracks paren depth so JOINs inside subqueries and CTEs don't
   shift the top-level marker indices. UNION/INTERSECT/EXCEPT and ASOF inside
   subqueries are not handled — if a real consumer needs either, the rewriter
   can be extended.

   Modeled on pg-datahike's datahike.pg.rewrite — same shape (token-driven
   walker, set of rules, returns rewritten SQL plus metadata) so the two can
   converge into a shared library when a third project requires it.")

(set! *warn-on-reflection* true)

;; Forward declarations: preprocess-sql calls these, but the
;; implementations live further down the file.
(declare preprocess-for-portion-of-valid-time
         preprocess-select-temporal)

;; ============================================================================
;; Low-level scanner — skips strings, comments, identifiers correctly
;; ============================================================================

(defn- skip-line-comment
  "Position at '--'; advance past end-of-line. Returns next index."
  ^long [^String s ^long i]
  (let [n (.length s)]
    (loop [j (+ i 2)]
      (cond
        (>= j n) j
        (= \newline (.charAt s j)) (inc j)
        :else (recur (inc j))))))

(defn- skip-block-comment
  "Position at '/*'; advance past closing '*/'. Returns next index."
  ^long [^String s ^long i]
  (let [n (.length s)]
    (loop [j (+ i 2)]
      (cond
        (>= j n) j
        (and (< (inc j) n)
             (= \* (.charAt s j))
             (= \/ (.charAt s (inc j))))
        (+ j 2)
        :else (recur (inc j))))))

(defn- skip-string
  "Position at a quote char; advance past closing quote, handling doubled-quote
   escape (`''` inside `'...'`, `\"\"` inside `\"...\"`)."
  ^long [^String s ^long i ^Character q]
  (let [n (.length s)]
    (loop [j (inc i)]
      (cond
        (>= j n) j
        (= q (.charAt s j))
        (if (and (< (inc j) n) (= q (.charAt s (inc j))))
          (recur (+ j 2))
          (inc j))
        :else (recur (inc j))))))

(defn- skip-ws-and-comments
  "Advance past whitespace, line comments, and block comments. Returns next index."
  ^long [^String s ^long start]
  (let [n (.length s)]
    (loop [j start]
      (if (>= j n) j
          (let [c (.charAt s j)]
            (cond
              (Character/isWhitespace c) (recur (inc j))
              (and (= \- c) (< (inc j) n) (= \- (.charAt s (inc j))))
              (recur (skip-line-comment s j))
              (and (= \/ c) (< (inc j) n) (= \* (.charAt s (inc j))))
              (recur (skip-block-comment s j))
              :else j))))))

(defn- word-char? [^Character c]
  (or (Character/isLetterOrDigit c) (= \_ c)))

(defn mask-non-code-spans
  "Return a length-preserved copy of `sql` with every character
   inside a SQL line comment (`-- … \\n`), block comment (`/* …
   */`), single-quoted string (`'…'` with `''` escape), or
   double-quoted identifier (`\"…\"` with `\"\"` escape) replaced
   with a space (newlines preserved so line-comment terminators
   stay intact for the regex flag `(?s)` consumers).

   The mask is used to defang regex-based preprocessors that
   otherwise match keyword sequences appearing inside comments or
   string literals — e.g. `re-find #\"FOR ALL VALID_TIME\" sql`
   incorrectly matching a leading SQL comment such as
   `/* migration: FOR ALL VALID_TIME … */`. Run the regex on the
   masked copy; use the matched positions to splice the original.

   Length preservation is critical: callers compute `(.indexOf
   masked …)` and `(subs original 0 idx)` based on the same
   offsets, so any character substitution must keep the index map
   1-to-1."
  ^String [^String sql]
  (let [n  (.length sql)
        sb (StringBuilder. sql)
        space (fn [^long i] (when-not (= \newline (.charAt sql i))
                              (.setCharAt sb i \space)))]
    (loop [i (long 0)]
      (if (>= i n)
        (.toString sb)
        (let [c  (.charAt sql i)
              c1 (when (< (inc i) n) (.charAt sql (inc i)))]
          (cond
            ;; -- line comment
            (and (= \- c) (= \- c1))
            (let [end (loop [j (+ i 2)]
                        (cond
                          (>= j n) n
                          (= \newline (.charAt sql j)) j
                          :else (recur (inc j))))]
              (dotimes [k (- (long end) i)] (space (+ i k)))
              (recur (long end)))
            ;; /* block comment */
            (and (= \/ c) (= \* c1))
            (let [end (loop [j (+ i 2)]
                        (cond
                          (>= j (dec n)) n
                          (and (= \* (.charAt sql j))
                               (= \/ (.charAt sql (inc j)))) (+ j 2)
                          :else (recur (inc j))))]
              (dotimes [k (- (long end) i)] (space (+ i k)))
              (recur (long end)))
            ;; '…' string literal (with '' escape)
            (= \' c)
            (let [end (loop [j (inc i)]
                        (cond
                          (>= j n) n
                          (and (= \' (.charAt sql j))
                               (< (inc j) n)
                               (= \' (.charAt sql (inc j)))) (recur (+ j 2))
                          (= \' (.charAt sql j)) (inc j)
                          :else (recur (inc j))))]
              (dotimes [k (- (long end) i)] (space (+ i k)))
              (recur (long end)))
            ;; "…" delimited identifier (with "" escape)
            (= \" c)
            (let [end (loop [j (inc i)]
                        (cond
                          (>= j n) n
                          (and (= \" (.charAt sql j))
                               (< (inc j) n)
                               (= \" (.charAt sql (inc j)))) (recur (+ j 2))
                          (= \" (.charAt sql j)) (inc j)
                          :else (recur (inc j))))]
              (dotimes [k (- (long end) i)] (space (+ i k)))
              (recur (long end)))
            :else (recur (inc i))))))))

(defn- read-word
  "Read an unquoted identifier or keyword starting at i. Returns [end-index
   lowercase-word] or [i nil] if the char at i isn't a word start."
  [^String s ^long i]
  (let [n (.length s)
        c (when (< i n) (.charAt s i))]
    (if (or (nil? c) (not (or (Character/isLetter ^Character c) (= \_ c))))
      [i nil]
      (loop [j (inc i)]
        (if (and (< j n) (word-char? (.charAt s j)))
          (recur (inc j))
          [j (.toLowerCase (subs s i j))])))))

(defn- next-keyword
  "Skip whitespace+comments then read the next word (lowercase).
   Returns [end-index word-or-nil]."
  [^String s ^long start]
  (let [j (skip-ws-and-comments s start)]
    (read-word s j)))

;; ============================================================================
;; Public: ASOF JOIN preprocessor
;; ============================================================================

(def ^:private join-modifiers
  "Words that can appear between ASOF and JOIN."
  #{"left" "inner" "right" "full" "outer"})

(defn preprocess-sql
  "Strip 'ASOF' before 'JOIN' (or 'ASOF LEFT JOIN' / 'ASOF INNER JOIN').
   Also strip `FOR PORTION OF VALID_TIME FROM x TO y` from DML statements.

   Returns {:sql rewritten-sql
            :asof-markers [marker-or-nil ...]
            :period {:axis :valid_time :from <micros> :to <micros>} | nil}.

   The :asof-markers vector is indexed by top-level join ordinal (0-based);
   each entry is :asof, :asof-left, or nil for non-ASOF joins. The downstream
   translator looks up the marker for each Join in JSqlParser's getJoins()
   list by position.

   :period is attached when the SQL carries a SQL:2011 `FOR PORTION OF
   VALID_TIME` clause; the downstream DML translator picks it up and
   passes it as a temporal slice to the stratum dataset's retract! /
   upsert! primitives.

   Joins inside subqueries (paren-depth > 0) are not counted at the top level
   and are not eligible for ASOF rewriting in this pass."
  [^String sql]
  (let [;; Dispatch FOR-VALID_TIME handling on statement type: DML
        ;; statements (DELETE / UPDATE / INSERT) emit a `:period`
        ;; side channel; SELECT statements get the clause rewritten
        ;; into WHERE predicates inline. Otherwise the FOR ALL form
        ;; would be claimed by both preprocessors.
        leading-pos (long (skip-ws-and-comments sql 0))
        [leading-end leading] (read-word sql leading-pos)
        select-stmt? (= "select" leading)
        erase-stmt? (= "erase" leading)
        ;; `ERASE FROM <t> [WHERE <p>]` — physical purge across
        ;; both temporal axes. Rewrite to `DELETE FROM …` and tag
        ;; with `:erase? true` so the server bypasses any
        ;; logical-retract path and routes directly to
        ;; `ds-delete-rows!`. `ERASE` is the conventional SQL verb
        ;; for GDPR-style right-to-be-forgotten purges across
        ;; temporal axes (vs `DELETE FOR PORTION OF`, which is a
        ;; bounded logical retract).
        ;;
        ;; Use a position-based splice (not `.replaceFirst` with a
        ;; regex) — `skip-ws-and-comments` already gave us the exact
        ;; position of the leading statement keyword, so we replace
        ;; only that substring. A regex would also hit any earlier
        ;; "ERASE FROM" appearing inside a leading SQL comment
        ;; (`/* ERASE FROM foo */ ERASE FROM bar`), leaving the
        ;; actual keyword intact and breaking JSqlParser.
        ;; (Copilot review #4.)
        sql (if erase-stmt?
              (str (subs sql 0 leading-pos) "DELETE" (subs sql leading-end))
              sql)
        sql (if select-stmt? (preprocess-select-temporal sql) sql)
        {:keys [sql period]} (if select-stmt?
                               {:sql sql :period nil}
                               (preprocess-for-portion-of-valid-time sql))
        ;; ERASE is a physical purge across both temporal axes; mixing
        ;; it with `FOR PORTION OF VALID_TIME` is ambiguous (a slice-
        ;; bounded ERASE doesn't have a well-defined semantic — either
        ;; the slice is purged or every matching row is, and the two
        ;; readings disagree). Reject up front rather than silently
        ;; routing through one branch and dropping the other.
        _ (when (and erase-stmt? period)
            (throw (ex-info (str "ERASE does not compose with FOR PORTION OF "
                                 "VALID_TIME — ERASE purges physically across "
                                 "both temporal axes; for a slice-bounded "
                                 "logical retract use DELETE FOR PORTION OF "
                                 "VALID_TIME.")
                            {:error :sql/erase-with-portion-of
                             :period period})))
        ^String sql sql
        n (.length sql)
        sb (StringBuilder. n)
        markers (java.util.ArrayList.)]
    (loop [i (long 0) paren-depth (long 0)]
      (if (>= i n)
        {:sql (.toString sb)
         :asof-markers (vec markers)
         :period period
         :erase? erase-stmt?}
        (let [c (.charAt sql i)]
          (cond
            ;; String literal — copy unchanged
            (= \' c)
            (let [end (long (skip-string sql i \'))]
              (.append sb (subs sql i end))
              (recur end paren-depth))

            ;; Quoted identifier — copy unchanged
            (= \" c)
            (let [end (long (skip-string sql i \"))]
              (.append sb (subs sql i end))
              (recur end paren-depth))

            ;; Line comment
            (and (= \- c) (< (inc i) n) (= \- (.charAt sql (inc i))))
            (let [end (long (skip-line-comment sql i))]
              (.append sb (subs sql i end))
              (recur end paren-depth))

            ;; Block comment
            (and (= \/ c) (< (inc i) n) (= \* (.charAt sql (inc i))))
            (let [end (long (skip-block-comment sql i))]
              (.append sb (subs sql i end))
              (recur end paren-depth))

            ;; Paren tracking
            (= \( c)
            (do (.append sb c) (recur (inc i) (inc paren-depth)))
            (= \) c)
            (do (.append sb c) (recur (inc i) (dec paren-depth)))

            ;; Word / keyword
            (or (Character/isLetter c) (= \_ c))
            (let [[wend* word] (read-word sql i)
                  wend (long wend*)]
              (cond
                ;; "ASOF" only at depth 0 followed by JOIN (or modifier+JOIN)
                (and (zero? paren-depth) (= "asof" word))
                (let [[k1end* k1] (next-keyword sql wend)
                      k1end (long k1end*)
                      [k2end* k2] (when (contains? join-modifiers k1)
                                    (next-keyword sql k1end))
                      k2end (long (or k2end* k1end))]
                  (cond
                    (= "join" k1)
                    (do (.add markers :asof)
                        ;; Strip "ASOF", emit " JOIN" (preserves original case)
                        (.append sb (subs sql wend k1end))
                        (recur k1end paren-depth))

                    (and (= "left" k1) (= "join" k2))
                    (do (.add markers :asof-left)
                        (.append sb (subs sql wend k2end))
                        (recur k2end paren-depth))

                    (and (= "inner" k1) (= "join" k2))
                    (do (.add markers :asof)
                        (.append sb (subs sql wend k2end))
                        (recur k2end paren-depth))

                    :else
                    ;; ASOF not followed by a recognized JOIN form — pass through
                    (do (.append sb (subs sql i wend))
                        (recur wend paren-depth))))

                ;; "JOIN" at depth 0 with no preceding ASOF: record nil marker
                (and (zero? paren-depth) (= "join" word))
                (do (.add markers nil)
                    (.append sb (subs sql i wend))
                    (recur wend paren-depth))

                :else
                (do (.append sb (subs sql i wend))
                    (recur wend paren-depth))))

            :else
            (do (.append sb c) (recur (inc i) paren-depth))))))))

;; ============================================================================
;; Public: SQL:2011 FOR PORTION OF VALID_TIME preprocessor
;; ============================================================================

(defn parse-temporal-literal
  "Parse a stratum-temporal *constant-foldable expression* into a
   `long` in `:micros`. Accepts (case-insensitive):

     'YYYY-MM-DD'                — start-of-day UTC
     DATE 'YYYY-MM-DD'           — same
     TIMESTAMP 'YYYY-MM-DD...'   — full ISO instant
     <number>                    — passthrough (already in :micros)
     CURRENT_TIMESTAMP / NOW / NOW()  — System/currentTimeMillis × 1000
     END_OF_TIME / MAX_VALUE     — `Long/MAX_VALUE`
     START_OF_TIME / MIN_VALUE   — `Long/MIN_VALUE`

   Per-row column references and arbitrary expressions are not
   supported here — the preprocessor needs a single scalar at
   parse time to attach to the DDL `:period`. Column-ref periods
   (`FOR PORTION OF VALID_TIME FROM contract_start TO contract_end`)
   would require per-row period evaluation, deferred to a future
   commit. For now, callers that need a column-derived period must
   compute it in application code and pass a literal.

   Throws ex-info on unparseable input."
  [^String s]
  (let [trimmed (.trim s)]
    (cond
      (re-matches #"-?\d+" trimmed) (Long/parseLong trimmed)

      (re-matches #"(?i)CURRENT_TIMESTAMP|NOW(?:\s*\(\s*\))?" trimmed)
      (* 1000 (System/currentTimeMillis))

      (re-matches #"(?i)END_OF_TIME|MAX_VALUE" trimmed)
      Long/MAX_VALUE

      (re-matches #"(?i)START_OF_TIME|MIN_VALUE" trimmed)
      Long/MIN_VALUE

      :else
      (let [body (or (second (re-find #"(?is)^(?:DATE|TIMESTAMP)?\s*'([^']+)'$" trimmed))
                     (throw (ex-info (str "Unparseable temporal literal in FOR PORTION OF "
                                          "VALID_TIME — only string literals (`'YYYY-MM-DD'`, "
                                          "`DATE`/`TIMESTAMP` prefixes), numeric micros, and "
                                          "the constants CURRENT_TIMESTAMP / NOW / "
                                          "END_OF_TIME / START_OF_TIME are recognized. Column "
                                          "references and per-row expressions are not yet "
                                          "supported.")
                                     {:input s})))]
        (cond
          (re-matches #"\d{4}-\d{2}-\d{2}" body)
          (-> (java.time.LocalDate/parse body)
              (.atStartOfDay java.time.ZoneOffset/UTC)
              .toInstant
              .toEpochMilli
              (* 1000))

          :else
          (-> (java.time.Instant/parse body)
              .toEpochMilli
              (* 1000)))))))

(defn- find-balanced-end
  "Starting at position `i` just past `keyword`, scan forward for the next
   keyword from `terminators` at paren-depth 0. Returns the index of the
   terminator. Used to find where the expr after FROM or TO ends."
  ^long [^String sql ^long i terminators]
  (let [n (.length sql)]
    (loop [j i, paren 0]
      (cond
        (>= j n) j

        (= \' (.charAt sql j))
        (recur (skip-string sql j \') paren)

        (= \" (.charAt sql j))
        (recur (skip-string sql j \") paren)

        (= \( (.charAt sql j)) (recur (inc j) (inc paren))
        (= \) (.charAt sql j)) (recur (inc j) (dec paren))

        (and (zero? paren)
             (or (Character/isLetter (.charAt sql j)) (= \_ (.charAt sql j))))
        (let [[wend* w] (read-word sql j)]
          (if (and (zero? paren) (contains? terminators w))
            j
            (recur (long wend*) paren)))

        :else (recur (inc j) paren)))))

(def ^:private dml-tail-keywords
  "Keywords that terminate a `FOR PORTION OF VALID_TIME` clause when
   walking forward through the SQL. Matches the keyword set the
   SQL:2011 DML grammar uses for FOR PORTION OF VALID_TIME and adds
   the SQL DML tail tokens we want to preserve (WHERE, SET, VALUES,
   RETURNING, etc.). `to` is included so the FROM scan stops at the
   optional TO keyword."
  #{"to" "where" "set" "values" "returning" "from" "using" "select"})

(def ^:private dml-tail-keywords-no-to
  "Same as `dml-tail-keywords` but without `to` — used for the TO
   expression's terminator scan."
  #{"where" "set" "values" "returning" "from" "using" "select"})

(defn preprocess-for-portion-of-valid-time
  "Strip SQL:2011 `FOR PORTION OF VALID_TIME FROM <x> [TO <y>]` from a
   DML statement. Returns `{:sql rewritten :period {:axis :from :to}}`
   if the clause is present, otherwise `{:sql sql :period nil}`. The
   downstream parser then sees a plain INSERT / UPDATE / DELETE and the
   server attaches `:period` back when lowering to stratum primitives.

   The `TO` clause is optional — when absent, the period extends to
   `Long/MAX_VALUE` (the conventional 'end of time' sentinel for the
   valid-time axis). The open-ended form is useful for retracting
   from a point forward.

   Only one occurrence is recognized per statement (DML applies to a
   single target). Recognizes `VALID_TIME` and `SYSTEM_TIME` axes;
   currently only `VALID_TIME` is honored by the server."
  [^String sql]
  ;; `FOR ALL VALID_TIME` / `FOR VALID_TIME ALL` (and the same for
  ;; SYSTEM_TIME) — SQL:2011 temporal DML scope clause. Strip first
  ;; and emit `:period {:axis ... :from MIN :to MAX}` so the lowering
  ;; treats it as a window spanning all time.
  ;;
  ;; Run the regex on `masked` (comments + string literals replaced
  ;; with spaces, character offsets preserved) so a leading comment
  ;; or string containing the literal phrase doesn't get matched.
  ;; Use the matched offsets to splice the ORIGINAL `sql` — that's
  ;; safe because mask preserves length. Agent-discovered pattern,
  ;; same shape as copilot review #4 (ERASE) but in a different
  ;; preprocessor.
  (let [masked (mask-non-code-spans sql)
        ;; **Design choice**: stratum rejects user-controlled
        ;; system-time DML (`FOR (ALL | PORTION OF) SYSTEM_TIME`)
        ;; rather than silently dropping the clause. System-time
        ;; is the auto-stamped recording-fact axis; allowing SQL
        ;; clients to rewrite past system-time slices would
        ;; undermine the audit story stratum's closed-period SCD2
        ;; preserves. Matches SQL Server / MariaDB / Postgres
        ;; (which all support `FOR SYSTEM_TIME AS OF` reads but
        ;; not `FOR PORTION OF SYSTEM_TIME` writes). Db2 / XTDB v2
        ;; / Teradata do support it; consumers needing
        ;; historical-import should use the Clojure DSL
        ;; (`(dataset/append! … {:system-from <past-inst>})`)
        ;; which already exposes the underlying primitive.
        reject-system-axis!
        (fn [axis-str]
          (when (.equalsIgnoreCase ^String axis-str "SYSTEM_TIME")
            (throw (ex-info
                    (str "FOR (ALL | PORTION OF) SYSTEM_TIME DML is not "
                         "supported via SQL — system-time is the auto-"
                         "stamped recording-fact axis; user-controlled "
                         "writes to it would compromise the audit story "
                         "stratum's closed-period SCD2 preserves. For "
                         "historical-import / regulator-replay use cases, "
                         "use the Clojure DSL `(dataset/append! ds row "
                         "{:system-from <past-inst>})` which exposes the "
                         "underlying primitive directly.")
                    {:error :sql/for-portion-of-system-time-unsupported
                     :axis :system}))))]
    (if-let [all-m (re-find #"(?is)\bFOR\s+(?:ALL\s+(VALID_TIME|SYSTEM_TIME)|(VALID_TIME|SYSTEM_TIME)\s+ALL)\b" masked)]
      (let [axis (or (nth all-m 1) (nth all-m 2))
            _ (reject-system-axis! axis)
            full ^String (first all-m)
            start (.indexOf masked full)
            rewritten (str (subs sql 0 start) (subs sql (+ start (.length full))))]
        {:sql rewritten
         :period {:axis (keyword (.toLowerCase ^String axis))
                  :from Long/MIN_VALUE
                  :to   Long/MAX_VALUE}})
      ;; Match WITHOUT a trailing `\s+` — when the regex runs on
      ;; `masked`, any string literal after `FROM` has been
      ;; replaced with spaces; a greedy `\s+` would swallow the
      ;; entire literal up to the next non-space token, producing
      ;; a wrong end-of-prefix offset. We advance past whitespace
      ;; ourselves via `skip-ws-and-comments` on the original.
      (let [m (re-find #"(?is)\bFOR\s+PORTION\s+OF\s+(VALID_TIME|SYSTEM_TIME)\s+FROM\b" masked)]
        (if-not m
          {:sql sql :period nil}
          (let [[full-prefix axis] m
                _ (reject-system-axis! axis)
                start (.indexOf masked ^String full-prefix)
                after-from (long (skip-ws-and-comments sql (+ start (.length ^String full-prefix))))
            ;; Walk forward to either a literal TO keyword or to the
            ;; next DML tail keyword (WHERE/SET/...) — that's where
            ;; the FROM expression ends.
                from-end-pos (long (find-balanced-end sql after-from dml-tail-keywords))
                from-expr (subs sql after-from from-end-pos)
            ;; Peek: is the terminator a TO keyword (open-bounded
            ;; form) or a tail keyword (open-ended form)?
                [_ first-kw] (read-word sql (long (skip-ws-and-comments sql from-end-pos)))
                has-to? (= "to" first-kw)
                [tail-pos to-expr]
                (if has-to?
                  (let [after-to-kw (long (->> (skip-ws-and-comments sql from-end-pos)
                                               (read-word sql)
                                               first))
                        tp (long (find-balanced-end sql after-to-kw dml-tail-keywords-no-to))]
                    [tp (subs sql after-to-kw tp)])
                  [from-end-pos nil])
                vf-val (parse-temporal-literal from-expr)
                vt-val (if has-to?
                         (parse-temporal-literal to-expr)
                         Long/MAX_VALUE)
                rewritten (str (subs sql 0 start) (subs sql tail-pos))]
            {:sql rewritten
             :period {:axis (keyword (.toLowerCase ^String axis))
                      :from vf-val
                      :to vt-val}}))))))

;; ============================================================================
;; Public: SQL:2011 SELECT-side `FOR VALID_TIME (AS OF | BETWEEN |
;; FROM…TO | ALL)` preprocessor.
;;
;; Rewrites `… FROM t [alias?] FOR VALID_TIME <spec> …` by stripping
;; the temporal clause and injecting an equivalent WHERE predicate
;; over the table's `_valid_from` / `_valid_to` columns. The
;; convention is the SQL:2011 column naming; tables that
;; use custom axis column names (via `:bitemporal {:valid {:from-col
;; …}}`) need to expose them under those names for the SELECT
;; surface (or use a view).
;;
;; Supported specs (SQL:2011 application-time-period table grammar):
;;   FOR VALID_TIME AS OF <expr>
;;   FOR VALID_TIME BETWEEN <expr> AND <expr>
;;   FOR VALID_TIME FROM <expr> TO <expr>
;;   FOR (ALL VALID_TIME | VALID_TIME ALL)
;;
;; Multi-table limitation: only **one** `FOR VALID_TIME` clause per
;; SELECT is supported today. The emitted predicate uses unqualified
;; column names (`_valid_from`/`_valid_to`); two clauses on a joined
;; SELECT would both refer to the same unqualified columns and the
;; planner cannot disambiguate them across joined tables. A second
;; clause therefore throws `:sql/multi-for-valid-time-unsupported`
;; at preprocess time. The proper fix is qualifier-aware predicate
;; emission in `spec->predicate-sql`; until then, callers needing
;; per-table temporal filters on a join should write the WHERE
;; predicates explicitly with `<table>._valid_from` / `<table>._valid_to`
;; refs (which the planner DOES handle because the qualifier is part
;; of the column identifier the parser sees).
;; ============================================================================

(def ^:private select-temporal-tail-keywords
  "Keywords that terminate a SELECT-side `FOR VALID_TIME` clause walk."
  #{"where" "group" "order" "limit" "offset" "having" "union" "intersect"
    "except" "join" "inner" "left" "right" "full" "outer" "cross" "on"
    "for"})

(defn- parse-select-temporal-spec
  "Walk forward from `start` past a `FOR VALID_TIME` keyword pair and
   parse the spec. Returns `[new-pos spec]` where `spec` is one of:
     {:kind :as-of  :at v}
     {:kind :between :from a :to b}
     {:kind :from-to :from a :to b}
     {:kind :all}
   or `nil` if no recognized spec follows."
  [^String sql ^long start]
  (let [[wend* w1] (next-keyword sql start)
        wend (long wend*)]
    (cond
      (= "as" w1)
      (let [[w2end* w2] (next-keyword sql wend)]
        (when (= "of" w2)
          (let [w2end (long w2end*)
                expr-end (long (find-balanced-end sql w2end
                                                  select-temporal-tail-keywords))
                expr (subs sql w2end expr-end)]
            [expr-end {:kind :as-of :at (parse-temporal-literal expr)}])))

      (= "between" w1)
      (let [from-end (long (find-balanced-end sql wend #{"and"}))
            from-expr (subs sql wend from-end)
            [after-and* _] (next-keyword sql from-end)
            after-and (long after-and*)
            to-end (long (find-balanced-end sql after-and
                                            select-temporal-tail-keywords))
            to-expr (subs sql after-and to-end)]
        [to-end {:kind :between
                 :from (parse-temporal-literal from-expr)
                 :to   (parse-temporal-literal to-expr)}])

      (= "from" w1)
      (let [from-end (long (find-balanced-end sql wend #{"to"}))
            from-expr (subs sql wend from-end)
            [after-to* _] (next-keyword sql from-end)
            after-to (long after-to*)
            to-end (long (find-balanced-end sql after-to
                                            select-temporal-tail-keywords))
            to-expr (subs sql after-to to-end)]
        [to-end {:kind :from-to
                 :from (parse-temporal-literal from-expr)
                 :to   (parse-temporal-literal to-expr)}])

      :else nil)))

(defn- axis-cols
  "Column-pair names for an axis-kw. Uses the SQL:2011 convention
   names directly; consumers that configured non-standard column
   names via `:bitemporal {axis-kw {:from-col … :to-col …}}` need
   to alias / view to the conventional names for the SQL surface.

   Returns `[from-col to-col]` as strings."
  [axis-kw]
  (case axis-kw
    :valid  ["_valid_from"  "_valid_to"]
    :system ["_system_from" "_system_to"]))

(defn- spec->predicate-sql
  "Build the WHERE-clause SQL fragment for a temporal spec on the
   given axis (`:valid` or `:system`). Uses the SQL:2011
   convention column names per axis-cols.

   Emits unqualified column names. The single-table SELECT case (the
   common MVP) works directly because the planner resolves
   `_valid_from` against the only source. The multi-table case where
   two joined tables each carry a `FOR VALID_TIME` clause is a
   documented limitation — making this fragment qualified
   (`<table>._valid_from`) requires teaching the planner's column
   resolver to strip the qualifier, which is out of scope here. Until
   that lands, two-`FOR <SAME_AXIS>` SELECTs against joined
   bitemporal tables are unsupported; single-table and one-clause-
   per-axis work.

   `qualifier` is captured for the future extension but unused today.

   The half-open inclusion test is `vf <= at AND vt > at` for
   AS OF. Returns nil for `:all` (no filter).

   `:between` (SQL `BETWEEN x AND y`) uses closed-closed
   semantics: `vf <= y AND vt > x` includes any row whose vt-
   window overlaps `[x, y]`.

   `:from-to` (SQL:2011 `FROM x TO y`) uses half-open semantics
   per the spec — `[x, y)`. Two half-open intervals `[vf, vt)`
   and `[x, y)` overlap iff `vf < y AND x < vt`. A row whose `vf`
   equals `y` starts exactly at the period's exclusive upper bound
   and must NOT be included. Round-3 agent P2: pre-fix used
   `vf <= y`, over-including the boundary row."
  [_qualifier axis-kw {:keys [kind at from to]}]
  (let [[from-col to-col] (axis-cols axis-kw)]
    (case kind
      :all     nil
      :as-of   (str from-col " <= " at " AND " to-col " > " at)
      :between (str from-col " <= " to " AND " to-col " > " from)
      :from-to (str from-col " < "  to " AND " to-col " > " from))))

(defn- axis-keyword
  "Map a parser keyword to an axis-kw, or nil if not an axis keyword."
  [w]
  (case w
    "valid_time"  :valid
    "system_time" :system
    nil))

(defn- find-for-time-clause
  "Scan `sql` for the next `FOR (VALID_TIME|SYSTEM_TIME) <spec>`
   or `FOR ALL (VALID_TIME|SYSTEM_TIME)` / `FOR (VALID_TIME|
   SYSTEM_TIME) ALL` clause. Returns `[start end spec axis-kw]`
   for the next such clause, or nil. `start` is the position of
   the `FOR` keyword; `end` is just past the spec; `axis-kw` is
   `:valid` or `:system`."
  [^String sql]
  (let [n (.length sql)]
    (loop [i (long 0)]
      (cond
        (>= i n) nil

        ;; Skip SQL line- and block-comments so a SELECT with a
        ;; leading comment such as
        ;;   /* historical: FOR VALID_TIME AS OF '2024-01-01' */
        ;;   SELECT … FROM t
        ;; doesn't trigger the FOR-VALID_TIME rewrite inside the
        ;; comment. Pattern-hunt P2; same shape as the DML
        ;; preprocessor P1s fixed in 2978a8a but for the
        ;; SELECT-side scanner.
        (and (= \- (.charAt sql i))
             (< (inc i) n) (= \- (.charAt sql (inc i))))
        (recur (long (skip-line-comment sql i)))

        (and (= \/ (.charAt sql i))
             (< (inc i) n) (= \* (.charAt sql (inc i))))
        (recur (long (skip-block-comment sql i)))

        (= \' (.charAt sql i)) (recur (long (skip-string sql i \')))
        (= \" (.charAt sql i)) (recur (long (skip-string sql i \")))

        (and (or (Character/isLetter (.charAt sql i)) (= \_ (.charAt sql i)))
             ;; Lower-case `for` at any letter boundary.
             (or (zero? i)
                 (not (word-char? (.charAt sql (dec i))))))
        (let [[wend* w] (read-word sql i)
              wend (long wend*)]
          (if (= "for" w)
            (let [[k1end* k1] (next-keyword sql wend)
                  k1end (long k1end*)]
              (cond
                ;; `FOR (VALID_TIME|SYSTEM_TIME) …`
                (axis-keyword k1)
                (let [axis (axis-keyword k1)
                      [k2end* k2] (next-keyword sql k1end)]
                  (cond
                    ;; `FOR <axis> ALL`
                    (= "all" k2)
                    [i (long k2end*) {:kind :all} axis]
                    :else
                    (if-let [[spec-end spec] (parse-select-temporal-spec sql k1end)]
                      [i (long spec-end) spec axis]
                      (recur wend))))
                ;; `FOR ALL (VALID_TIME|SYSTEM_TIME)`
                (= "all" k1)
                (let [[k2end* k2] (next-keyword sql k1end)
                      axis (axis-keyword k2)]
                  (if axis
                    [i (long k2end*) {:kind :all} axis]
                    (recur wend)))
                ;; Other FOR keywords (FOR PORTION OF, etc.) —
                ;; not our concern; skip past `for` only.
                :else (recur wend)))
            (recur wend)))

        :else (recur (inc i))))))

(defn- table-qualifier-before
  "Walk backwards from `for-pos` to find the table name (or alias)
   that the `FOR VALID_TIME` clause attaches to. Returns the
   qualifier string, or nil if not discoverable. Pattern: the
   immediately-preceding identifier is the table-or-alias; if the
   one before that is also an identifier (no comma/keyword in
   between) and the closer one isn't a SQL keyword, the closer
   one is the alias.

   Simple heuristic — good for `FROM t FOR VALID_TIME …` and
   `FROM t alias FOR VALID_TIME …` but stops short of fully
   parsing arbitrary FROM expressions."
  [^String sql ^long for-pos]
  (let [;; Find the identifier immediately before for-pos.
        end (long (loop [j (dec for-pos)]
                    (cond
                      (neg? j) 0
                      (Character/isWhitespace (.charAt sql j)) (recur (dec j))
                      :else (inc j))))
        start (long (loop [j (dec end)]
                      (cond
                        (neg? j) 0
                        (word-char? (.charAt sql j)) (recur (dec j))
                        :else (inc j))))]
    (when (and (> end start) (Character/isLetter (.charAt sql start)))
      (subs sql start end))))

(defn- inject-where-predicates
  "Splice `predicates` (a vector of SQL strings) into `sql`'s WHERE
   clause. Creates a WHERE if one isn't present (inserted before
   GROUP BY / ORDER BY / LIMIT / etc.). If WHERE exists, ANDs the
   new predicates onto the front of its expression."
  [^String sql predicates]
  (if (empty? predicates)
    sql
    (let [n (.length sql)
          where-keywords #{"group" "order" "limit" "offset" "having"
                           "union" "intersect" "except"}
          ;; Find WHERE keyword position, if any.
          where-pos
          (loop [i (long 0)]
            (cond
              (>= i n) -1
              (= \' (.charAt sql i)) (recur (long (skip-string sql i \')))
              (= \" (.charAt sql i)) (recur (long (skip-string sql i \")))
              (and (or (Character/isLetter (.charAt sql i)) (= \_ (.charAt sql i)))
                   (or (zero? i)
                       (not (word-char? (.charAt sql (dec i))))))
              (let [[wend* w] (read-word sql i)]
                (if (= "where" w)
                  i
                  (recur (long wend*))))
              :else (recur (inc i))))
          ;; Find a "stop" keyword position (GROUP/ORDER/...) for
          ;; locating where to insert WHERE when none exists.
          stop-pos
          (loop [i (long 0)]
            (cond
              (>= i n) n
              (= \' (.charAt sql i)) (recur (long (skip-string sql i \')))
              (= \" (.charAt sql i)) (recur (long (skip-string sql i \")))
              (and (or (Character/isLetter (.charAt sql i)) (= \_ (.charAt sql i)))
                   (or (zero? i)
                       (not (word-char? (.charAt sql (dec i))))))
              (let [[wend* w] (read-word sql i)]
                (if (contains? where-keywords w)
                  i
                  (recur (long wend*))))
              :else (recur (inc i))))
          pred-str (clojure.string/join " AND " predicates)]
      (if (neg? where-pos)
        ;; No WHERE — insert one before the stop position (or at end).
        (str (subs sql 0 stop-pos)
             " WHERE " pred-str " "
             (subs sql stop-pos))
        ;; WHERE exists — splice " (preds) AND " right after the keyword.
        (let [[after-where-kw* _] (read-word sql where-pos)
              after-where (long after-where-kw*)]
          (str (subs sql 0 after-where) " (" pred-str ") AND"
               (subs sql after-where)))))))

(defn preprocess-select-temporal
  "Rewrite SELECT-side `FOR (VALID_TIME|SYSTEM_TIME) <spec>`
   clauses into equivalent WHERE predicates. Returns the
   rewritten SQL string.

   **Bitemporal SELECT** — one clause per axis is supported and
   composable on the same SELECT:

     SELECT … FROM t
       FOR VALID_TIME  AS OF '2024-04-15'
       FOR SYSTEM_TIME AS OF '2024-04-20'

   Both predicates are ANDed into the WHERE clause.

   **Multi-clause-per-axis limit**: two `FOR VALID_TIME` (or two
   `FOR SYSTEM_TIME`) clauses per SELECT are rejected with
   `:sql/multi-for-<axis>-unsupported`. The emitted predicate
   uses unqualified column names; two clauses on the same axis on
   a joined SELECT would both reference the same columns and the
   planner can't disambiguate. Workaround: write `<table>._valid_from`
   refs explicitly in WHERE.

   See module docstring above for the supported specs."
  [^String sql]
  (loop [sql sql, preds [], seen-axes #{}]
    (if-let [[start end spec axis] (find-for-time-clause sql)]
      (let [qualifier (table-qualifier-before sql start)
            ;; Always emit the predicate when the spec yields one.
            ;; `spec->predicate-sql` returns nil for `:all` (strip
            ;; the clause, no filter). For `:as-of`/`:between`/
            ;; `:from-to` it always returns a predicate even when
            ;; `qualifier` is nil (round-3 agent suppressed
            ;; comment: gating on qualifier silently dropped the
            ;; filter for underscore/quoted/subquery tables).
            new-pred (spec->predicate-sql qualifier axis spec)
            stripped (str (subs sql 0 start) (subs sql end))]
        (when (and new-pred (contains? seen-axes axis))
          (throw (ex-info (str "Multi-table SELECT with more than one "
                               "FOR " (case axis :valid "VALID_TIME"
                                            :system "SYSTEM_TIME")
                               " clause is not yet supported "
                               "— the rewriter emits unqualified column "
                               "refs which the planner cannot disambiguate "
                               "across joined tables. Write the predicates "
                               "explicitly with `<table>." (case axis
                                                             :valid "_valid_from"
                                                             :system "_system_from")
                               "` qualifiers, or split into separate queries.")
                          {:error (case axis
                                    :valid :sql/multi-for-valid-time-unsupported
                                    :system :sql/multi-for-system-time-unsupported)
                           :axis axis})))
        (recur stripped
               (if new-pred (conj preds new-pred) preds)
               (if new-pred (conj seen-axes axis) seen-axes)))
      (inject-where-predicates sql preds))))
