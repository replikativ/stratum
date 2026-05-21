(ns stratum.like-escape-test
  "Regression spec for the SQL `LIKE ... ESCAPE` clause.

   STATUS: the ESCAPE-clause `deftest`s in this file are EXPECTED TO FAIL on
   the current code base — that is the purpose of the file. It is the
   executable specification for the bugfix; the tests turn green once the
   ESCAPE clause is implemented.

   The two control tests (plain LIKE, no ESCAPE) pass today and guard against
   the fix regressing ordinary `%` / `_` wildcard matching.

   --- The limitation -------------------------------------------------------
   SQL `LIKE pattern ESCAPE c` lets the character `c` quote the next
   character, so a *literal* `%` or `_` can be matched. Stratum ignores the
   ESCAPE clause entirely:

     WHERE s LIKE '100\\%' ESCAPE '\\'   should match only the literal
     string \"100%\" — but Stratum still treats the `%` as a wildcard.

   --- Fix touchpoints (for whoever implements it) --------------------------
   * src/stratum/sql.clj  (~ lines 786-795) — the `LikeExpression` branch
     never reads `(.getEscape e)`; the escape character is dropped at parse
     time and never reaches the IR.
   * The `[:like col pattern]` IR (and `:not-like` / `:ilike` / `:not-ilike`)
     has no slot to carry an escape character.
   * src-java/stratum/internal/ColumnOps.java `likeToRegex` and
     src/stratum/query/estimate.clj `like->regex` — both translate `%` / `_`
     unconditionally and must honour an escape character.
   * src-java/stratum/internal/ColumnOpsString.java `matchDictLikeRange` /
     `rawStringLikeRange` — the contains/startsWith/endsWith fast-path
     detection scans for raw `%` / `_` and must not treat an *escaped*
     `%` / `_` as a wildcard.
   * src/stratum/server.clj `like-match` — the server-side eval path.

   NOTE: written from source analysis; not executed (no Stratum REPL was
   running). Run it before relying on the exact expected counts."
  (:require [clojure.test :refer [deftest testing is]]
            [stratum.sql :as sql]
            [stratum.query :as q]))

(set! *warn-on-reflection* true)

(def ^:private fixture-strings
  ;; Literal `%` and `_` in the data, plus the near-misses a wildcard
  ;; interpretation would wrongly catch.
  ["100%" "1000" "100abc" "50%off" "50%" "a_b" "axb" "plain"])

(defn- count-where
  "Run `SELECT COUNT(*) FROM t WHERE <where-sql>` over `fixture-strings`
   (column `s`) and return the number of matched rows."
  ^long [where-sql]
  (let [s   (q/encode-column (into-array String fixture-strings))
        v   (double-array (map double (range (count fixture-strings))))
        reg {"t" {:s s :v v}}
        {:keys [query]} (sql/parse-sql
                         (str "SELECT COUNT(*) FROM t WHERE " where-sql) reg)]
    (long (:count (first (q/q query))))))

;; --- Control tests: plain LIKE, no ESCAPE — pass today --------------------

(deftest plain-like-percent-wildcard-test
  (testing "`%` is a wildcard when there is no ESCAPE clause"
    ;; "100%", "1000", "100abc" all start with "100"
    (is (= 3 (count-where "s LIKE '100%'")))))

(deftest plain-like-underscore-wildcard-test
  (testing "`_` is a single-character wildcard when there is no ESCAPE clause"
    ;; "a_b" and "axb" both match `a?b`
    (is (= 2 (count-where "s LIKE 'a_b'")))))

;; --- Regression tests: LIKE ... ESCAPE — EXPECTED TO FAIL until fixed -----

(deftest escape-literal-percent-test
  (testing "an escaped `%` matches a literal percent sign"
    ;; '100\%' ESCAPE '\'  ->  exactly the literal string "100%"
    (is (= 1 (count-where "s LIKE '100\\%' ESCAPE '\\'")))))

(deftest escape-literal-underscore-test
  (testing "an escaped `_` matches a literal underscore"
    ;; 'a\_b' ESCAPE '\'  ->  exactly "a_b" (must NOT match "axb")
    (is (= 1 (count-where "s LIKE 'a\\_b' ESCAPE '\\'")))))

(deftest escape-then-wildcard-test
  (testing "an escaped `%` and an unescaped `%` can coexist in one pattern"
    ;; '50\%%' ESCAPE '\'  ->  literal "50%" followed by a wildcard
    ;; matches "50%off" and "50%"
    (is (= 2 (count-where "s LIKE '50\\%%' ESCAPE '\\'")))))

(deftest escape-custom-char-test
  (testing "the escape character is configurable, not hard-wired to backslash"
    ;; '100!%' ESCAPE '!'  ->  the literal string "100%"
    (is (= 1 (count-where "s LIKE '100!%' ESCAPE '!'")))))

(deftest escape-not-like-test
  (testing "NOT LIKE honours the ESCAPE clause"
    ;; NOT (literal "100%")  ->  7 of the 8 rows
    (is (= 7 (count-where "s NOT LIKE '100\\%' ESCAPE '\\'")))))

(deftest escape-ilike-test
  (testing "ILIKE honours the ESCAPE clause"
    ;; case-insensitive literal "100%"
    (is (= 1 (count-where "s ILIKE '100\\%' ESCAPE '\\'")))))
