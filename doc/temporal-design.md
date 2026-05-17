# Temporal Design — Bitemporal Stratum

This document captures the design decisions behind stratum's bitemporal
write and read surface.

## Vocabulary

We follow Snodgrass / SQL:2011 with two consistent renames for context:

| Concept | Where | Term |
|---|---|---|
| When a fact was true in the modelled world | everywhere | **valid-time** |
| When the DB recorded the fact (Clojure / EDN APIs) | datahike, stratum query DSL | **tx-time** |
| Same concept, but in SQL surface | pg-datahike, stratum SQL | **system-time** |
| Reading "two axes at once" | everywhere | **bitemporal** |

The axis values use SQL:2011 column names verbatim:
`_valid_from`, `_valid_to`, `_system_from`, `_system_to`. All are
`:int64` `:temporal-unit :micros`. Open-ended intervals use
`Long/MAX_VALUE` as the sentinel for `_to` columns.

## Schema model

A dataset opts into bitemporal semantics via `:metadata`:

```clojure
(st/make-dataset
  cols
  {:metadata
   {:bitemporal
    {:valid  {:from-col :_valid_from
              :to-col   :_valid_to
              :unit     :micros}
     :system {:from-col :_system_from
              :to-col   :_system_to
              :unit     :micros}}}})
```

Either axis is optional. Datasets with only `:valid` are *valid-only*;
datasets with only `:system` are *system-versioned*; datasets with
both are *fully bitemporal*. Datasets with neither are non-temporal —
existing semantics, no overhead.

The two axes are processed *identically* by every code path. The
following all apply per-axis:

- Column tagging with `:temporal-unit` at `make-dataset` / `load`
- Range pruning via zone-maps on `_from` and `_to`
- Close-on-update in SCD2 write primitives
- Overlap detection on writes
- Range predicates in SQL `FOR VALID_TIME` / `FOR SYSTEM_TIME` lowering

`stratum.dataset/bitemporal-config` returns the validated config map
or `nil`. Helpers `valid-time-config` and `system-time-config` return
the per-axis subset.

## Why both axes?

System-time is already implicit at the konserve commit layer — every
`sync!` produces a commit with a timestamp. So why store
`_system_from` / `_system_to` per row?

Granularity. Commit-level system-time answers "what did the whole
dataset look like as of commit C?". Row-level system-time answers
"what did the DB know about *this row* at instant S?" — the
distinction matters whenever a single commit touches many entities
(common in batch loads) or when readers want to ask `(vt, st)` 2D
queries directly off the row.

The cost is two extra `long` columns per row when the dataset opts
in. Adapters that don't need row-level system-time leave the config
unset, and stratum falls back to commit-time. The choice is per-table,
not global.

## System-time semantic

`_system_from` is set at write time and never overwritten — it pins
when the database first knew the row. `_system_to` starts at
`Long/MAX_VALUE` ("still open") and is closed only when an SCD2
correction supersedes the row. Together the pair forms a half-open
period `[from, to)` describing the system-time interval during which
the row's valid-time window was the database's current belief.

Concretely, after a backdated correction:

- The **old row** keeps its original `_valid_from` and `_valid_to`
  untouched. Its `_system_to` is closed to `now`. It still represents
  "what the DB used to believe", reachable by `FOR SYSTEM_TIME AS OF
  <past>` queries.
- One or more **successor rows** are appended with fresh
  `_system_from = now`, carrying the new valid-time window(s) and any
  updated values.

A reader filters with `_system_from <= s AND _system_to > s` to get
the rows the DB believed at system-instant `s`, plus the standard
`_valid_from <= v AND _valid_to > v` for the valid-time slice. Two
half-open intervals, two AND'd predicates, four columns — that's the
whole model.

### Why mutate `_system_to` instead of keeping rows append-only?

Stratum stores rows in a classical SCD2 layout: every row is a tuple
that physically exists at one location in its columnar dataset. To
distinguish "row was the DB's belief at time `s`" from "row has been
superseded", we need *some* per-row marker of the supersession point.
The two natural shapes are:

1. **Closed-period** (what stratum does): write `_system_to` on the
   old row when it's superseded. One predicate at read time
   (`_system_to > s`), one column write per correction.
2. **Append-only**: never touch the old row; rely on the existence of
   a newer row to imply supersession. Requires a per-entity merge at
   read time to pick the row with the latest `_system_from <= s` for
   each `(eid, vt-slice)`.

The closed-period approach is the right fit for stratum's
secondary-index substrate, where reads dominate writes and a single
predicate filter is cheaper than a per-entity merge pass. The
append-only approach is what event-sourced bitemporal stores do; it
trades read-time work for write-time simplicity.

### Caveat: reading `_system_to` at past system-time

The closed-period design has one subtlety. If you query
`SELECT _system_to FROM t FOR SYSTEM_TIME AS OF <past>`, the value
you'll see for the old row is the *closure instant*, not the
`Long/MAX_VALUE` it held at the queried system-time. The
`(from <= s < to)` predicate still answers visibility correctly —
the row's row-visibility at past system-time is preserved — but the
literal column value is the post-mutation one.

Practical impact for accounting / audit workloads: low. Queries ask
"which rows were the DB's belief at time `s`?", not "what was each
row's `_system_to` value at `s`?". The visibility filter is the
audit-relevant question, and it works. Workloads that need the
literal historical `_system_to` (regulatory reconstructions of "what
did the DB *show* at exactly that instant") should derive it from
the next-later row's `_system_from` for the same entity — or use a
storage layer that's append-only by construction.

## Write primitives

Three primitives on `IDataset` (called on a transient dataset, mutate
in-place, return the transient for threading):

```clojure
(append! tds rows-or-cols
         {:valid-from  inst   ; defaults to now (wall-clock or :db/txInstant override)
          :valid-to    inst   ; defaults to Long/MAX_VALUE (open)
          :system-from inst   ; defaults to now (shared with valid-from default)
          :system-to   inst   ; defaults to Long/MAX_VALUE (open)})

(upsert! tds {:where    [pred ...]      ; stratum WHERE clause
              :set      {col val}
              :auto-split? false}
             {:valid-from inst         ; defaults to now
              :valid-to   inst})       ; defaults to Long/MAX_VALUE

(retract! tds {:where    [pred ...]
               :auto-split? false}
              {:valid-from inst        ; defaults to now (close-point)
               :valid-to   inst})      ; explicit presence selects bounded
                                       ; surgery (FOR PORTION OF semantic);
                                       ; absence = open-window close
```

All three accept an optional `tx-meta` second arg with the temporal
keys (`:valid-from`, `:valid-to`, `:system-from`, `:system-to`); any
key absent falls back to wall-clock now for `:_from` and
`Long/MAX_VALUE` for `:_to`. Pinning the clock for deterministic
test runs is done via `:db/txInstant` in tx-meta or by binding the
dynamic var the dataset reads (`*clock-time-millis*`).

**`append!`** writes new rows with the supplied or defaulted vt /
system windows. No close. Pure insertion.

**`upsert!`** is the SCD2 close-and-reopen primitive:
1. Find rows matching `:where` whose `_valid_to` (or `_system_to`)
   still spans the new window.
2. Close their `_valid_to` to the new `:valid-from` (write the close
   intervals).
3. Append new rows with the merged `:set` values and the new vt /
   system windows.

**`retract!`** has two shapes determined by tx-meta presence:
- **Open-window** (no `:valid-to` in tx-meta): "close without reopen"
  — find matching rows whose `_valid_to` is `Long/MAX_VALUE`, set
  `_valid_to` to `:valid-from`. Logical retraction at the close point.
- **Bounded `FOR PORTION OF`** (`:valid-to` in tx-meta): per-row
  surgery — drop / truncate-vt / truncate-vf / split — preserving
  non-overlap parts. Implements SQL:2011 `FOR PORTION OF VALID_TIME …
  DELETE` semantics.

Physical purge stays on `ds-delete-rows!` — never auto-triggered by
`retract!`.

### Why predicate-driven, not entity-keyed?

Stratum core doesn't know what "an entity" is. The `:eid` convention
is application-specific (one consumer uses `:eid`, a text-search
adapter would use a doc-id, a vector-search adapter a vector-id). A
predicate-driven primitive stays generic and reuses the WHERE-clause
DSL the planner already understands — zone-map pruning and parallel
scan come for free.

Callers that *do* have an entity key wrap the primitive trivially:

```clojure
(defn upsert-entity! [ds eid attrs vt-from]
  (-> ds transient
      (upsert! {:where [[:= :eid eid]] :set attrs}
               {:valid-from vt-from})
      persistent!))
```

### Why three primitives instead of one?

INSERT and DELETE both have a "no close needed / no reopen needed"
half-shape. UPDATE has both. Folding them into one `vt-write!` with
mode flags conflates ergonomics and obscures the SCD2 invariant. Three
primitives mirror SQL:2011 DML and SCD2 textbook patterns.

## Overlap policy

A write whose `[vf, vt)` window overlaps an already-closed historical
window on the same matching rows is a *backdated correction*. There
are three reasonable behaviors:

1. **Reject** (default, implemented). Throw with an error citing the
   overlapping rows. Caller must split the write themselves.
2. **Auto-split** (`:auto-split? true`, implemented). Reshape the
   matching rows so the new write slots in without violating the
   without-overlaps invariant. Per row, the action depends on its
   position relative to the new write's `[new-vf, MAX)`:
     - `row-vf < new-vf < row-vt` — partial left overlap; *truncate*
       the row's `vt` down to `new-vf`. The historical prefix
       `[row-vf, new-vf)` is preserved.
     - `row-vf >= new-vf` — row is entirely inside the new write's
       range; *drop* it physically via `ds-delete-rows!` (fans
       `idx-delete!` across every column).
   Then `upsert!` appends the new merged row (or degenerates to a
   plain insert if no `:close-safe` rows matched). `retract!` runs
   the same reshape with no append step. SQL:2011
   "application-time-period tables" allow this surgical behavior
   under the *non-sequenced UPDATE* mode.
3. **Accept** (silent). Not exposed — backdated overlaps that aren't
   handled by the writer become a correctness problem at read time.

### Bounded INSERT / UPDATE FOR PORTION OF — pre-existing-overlap caveat

`INSERT FOR PORTION OF VALID_TIME` and `UPDATE FOR PORTION OF
VALID_TIME` are append-only with respect to pre-existing data.
Stratum follows SQL:2011 non-sequenced semantics: each matching row
gets independently updated (or, for INSERT, the new row is appended
without checking whether the slice was already populated).

If two pre-existing rows already overlap each other within the
target slice (a data-quality issue), bounded UPDATE produces two
overlapping output slices — both with fresh `_system_from`. Reads
at the new system-time will see both. Stratum does not enforce
WITHOUT OVERLAPS at write time. Event-sourced bitemporal stores
can defer this to a read-time merge step that resolves which
event "wins" at each (vt-time, system-time) point; stratum's
mutate-in-place storage leaves the responsibility on the writer.

To get surgical replacement (drop the slice + assert one new row),
compose `DELETE FOR PORTION OF VALID_TIME …` and `INSERT FOR
PORTION OF VALID_TIME …` as two statements in your transaction.

Reject-by-default matches kontor's audit constraints (an accountant
filing a backdated invoice shouldn't have one write produce three
rows of audit trail without explicit opt-in). Auto-split is opt-in
and per-call, not per-dataset, so the policy decision lives at the
edit site.

Classification of matching rows vs the new write's `[new-vf, MAX)`
window (implemented in `upsert!` / `retract!`):

- **`:close-safe`** — open row (`vt = MAX`) AND `vf < new-vf`. Closing
  its `vt` to `new-vf` produces a valid, non-empty window. The SCD2
  close-and-reopen path applies.
- **`:no-conflict`** — closed row entirely before the new write
  (`vt <= new-vf`). Ignored; no overlap.
- **`:overlaps`** — any other matching row. Reject (default) or
  auto-split (`:auto-split? true`). Includes:
    - open row with `vf >= new-vf` (would create a backwards window) —
      auto-split drops it.
    - closed row with `vt > new-vf` (the new write would land inside
      an already-closed historical period) — auto-split truncates if
      `row-vf < new-vf`, drops otherwise.

Right-partial and middle-overlap classes don't appear in the current
API because the new write's `vt` is always `MAX` in `upsert!` /
`retract!`. They become reachable once SQL `FOR PORTION OF VALID_TIME
FROM x TO y` lowers to bounded windows in Phase D.

Cost: one extra full scan over matching rows per write (predicate eval
in pure Clojure). Future Phase C+ work can push the predicate through
the planner's zone-map pruner for sublinear scaling.

**`upsert!` degenerate-to-insert.** When no rows match the `:where`
predicate at all, `upsert!` falls through to a plain `append!` of the
`:set` payload — matching the spirit of SQL `INSERT … ON CONFLICT
UPDATE`. The new row's axis columns are auto-stamped just like a
direct `append!`.

## SQL surface

We extend pg-datahike's grammar to recognise SQL:2011 temporal DML:

```sql
-- session-scoped temporal pins (PG-wire SET vars)
SET datahike.valid_at = '2024-04-15T00:00:00Z';
SET datahike.system_at = '2024-04-20T00:00:00Z';

-- per-query SQL:2011 forms
SELECT * FROM employees
  FOR VALID_TIME AS OF '2024-04-15'
  FOR SYSTEM_TIME AS OF '2024-04-20';

SELECT * FROM employees
  FOR VALID_TIME BETWEEN '2024-01-01' AND '2024-12-31';

-- bitemporal DML
INSERT INTO employees (id, salary, _valid_from, _valid_to)
  VALUES (1, 100000, '2024-01-01', '2024-07-01');

UPDATE employees FOR PORTION OF VALID_TIME FROM '2024-07-01' TO '2024-12-01'
  SET salary = 110000
  WHERE id = 1;

DELETE FROM employees FOR PORTION OF VALID_TIME FROM '2024-09-01' TO '2024-10-01'
  WHERE id = 1;
```

JSqlParser 5.2 doesn't grok these tokens, so the path is a pre-tokenise
rewriter in `stratum.sql.rewrite/preprocess-sql`: strip
`FOR PORTION OF VALID_TIME FROM x TO y` to a side-channel `:period`
map before JSqlParser sees the SQL, then attach `:period` to the
parsed `:ddl` for the translator.

- `DELETE FOR PORTION OF VALID_TIME` lowers to `dataset/retract!`
  with `:valid-from` + `:valid-to` in tx-meta. `retract!` performs
  the bounded surgery (truncate / shift / split / drop) per
  overlapping row.
- `INSERT FOR PORTION OF VALID_TIME` lowers to `dataset/append!` with
  the period values stamping the configured `_valid_from` /
  `_valid_to` columns. The SQL must provide an explicit column list
  (`INSERT INTO t (a, b) VALUES (...)`) — the period fills in the
  temporal columns.
- `UPDATE FOR PORTION OF VALID_TIME` lowers to `dataset/bounded-update!`
  (SQL:2011 non-sequenced UPDATE): per row, the overlap portion gets
  the merged `:set` values, the non-overlap parts retain the
  original values. Internally decomposes into `retract!` over the
  slice + an `append!` per captured overlap.
- `INSERT … ON CONFLICT … FOR PORTION OF VALID_TIME` is explicitly
  rejected — the conflict target's semantics under non-sequenced
  semantics are not well-defined; users can compose `DELETE FOR
  PORTION OF` + `INSERT FOR PORTION OF` (or `INSERT FOR PORTION OF`
  + `UPDATE FOR PORTION OF`) instead.

Plain `DELETE WHERE` / `INSERT VALUES` / `UPDATE WHERE` (without
`FOR PORTION OF`) on an index-backed table also route through
`ds-delete-rows!` / `append!` / array-rebuild appropriately.

### SELECT-side temporal grammar

`SELECT … FROM t FOR VALID_TIME (AS OF x | BETWEEN x AND y |
FROM x TO y | ALL)` is rewritten by the same preprocessor into
equivalent `WHERE` predicates over the table's `_valid_from` /
`_valid_to` columns. Convention is the SQL:2011 default naming; tables that opt into a custom axis via `:bitemporal
{:valid {:from-col …}}` would need a view to expose the canonical
names for SELECT-side use (or extend the preprocessor to consult
the registry — deferred).

```sql
-- point-in-vt
SELECT salary FROM salaries
  FOR VALID_TIME AS OF '2024-04-01'
  WHERE eid = 1;

-- range overlap
SELECT eid, salary FROM salaries
  FOR VALID_TIME BETWEEN '2024-01-01' AND '2024-07-01';

-- explicit half-open
SELECT eid, salary FROM salaries
  FOR VALID_TIME FROM '2024-01-01' TO '2024-07-01';

-- no temporal filter (full vt-history)
SELECT eid, salary FROM salaries FOR ALL VALID_TIME;
```

The rewriter strips the clause and injects the equivalent
predicate into `WHERE`. Multi-table joins each carrying their own
`FOR VALID_TIME` work as well — predicates are accumulated and
ANDed onto the WHERE clause.

### Allen interval predicates

`OVERLAPS`, `EQUALS_PERIOD`, `CONTAINS_PERIOD`, `PRECEDES` /
`STRICTLY_PRECEDES` / `IMMEDIATELY_PRECEDES`, `SUCCEEDS` /
`STRICTLY_SUCCEEDS` / `IMMEDIATELY_SUCCEEDS`, and `MEETS` are
available as 4-arg WHERE-clause functions taking
`(a_from, a_to, b_from, b_to)`. Generic over any int64 column
pair — works for the bitemporal axis, application-domain date
ranges, or arbitrary intervals.

```sql
DELETE FROM events
  WHERE OVERLAPS(event_start, event_end, blackout_start, blackout_end);
```

Allen predicates currently work in DML WHERE clauses (the DML
evaluator handles column-vs-column comparisons natively). SELECT
WHERE with Allen predicates lowers correctly but stratum's main
query planner doesn't yet evaluate col-vs-col predicates — tracked
as a P2 follow-up.

## Four-axis composability

Both axes are wrappers, both compose:

```clojure
(d/q query
     (-> conn d/db
         (d/as-of    tx-T)      ; tx-time AS OF T
         (d/since    tx-S)      ; tx-time > S
         (d/valid-at vt-V)      ; valid-time AS OF V
         (d/system-at st-W)))   ; system-time AS OF W (new in Phase F)
```

Each wrapper sets a `timepred` or `xform-after` on the search-context;
`post-process-datoms` applies them in order; per-datom cost is one
HashMap probe per axis per unique tx-id (cached). Same cost profile
as AsOfDB today, just one more axis.

## Why this design

Stratum is a columnar analytics library that doubles as the storage
substrate for higher-level systems (a Datalog secondary index, a SQL
front-end, application code in other repos). Without a canonical
write-side bitemporal primitive in stratum core, every consumer that
needed SCD2 had to re-invent close-and-reopen on top of
`assoc-column` and `append!` — duplicating ~100-200 LOC each, with no
guarantee they implemented the system-time-symmetry invariant
correctly.

With this work in place:
- One canonical SCD2 surgery (`replace-row-bitemporal!`) lives in
  stratum core, exercised by `append!` / `upsert!` / `retract!` /
  `bounded-update!`.
- Consumers (Datalog secondary indices, SQL surfaces, application
  code) stay thin: thread `:bitemporal` config through `make-
  dataset` and the primitives do the right thing per-axis.
- A consuming PG-wire layer can speak SQL:2011 `FOR PORTION OF
  VALID_TIME` directly without a custom translator.
- Both temporal axes get equal treatment from the planner.

## Phases

The work landed in phases for review-ability:

| Phase | Scope |
|---|---|
| A | Schema redesign: `:bitemporal {:valid :system}` |
| B | Write primitives: `append!` / `upsert!` / `retract!` |
| C | Overlap detection: reject by default |
| C+ | Auto-split: truncate partial-left + drop fully-superseded (via `ds-delete-rows!`) |
| D | SQL grammar `FOR PORTION OF VALID_TIME` (DELETE) + bounded retract! + index-backed SQL DELETE |
| D+ | `FOR PORTION OF VALID_TIME` on UPDATE + INSERT (via `bounded-update!`); UPSERT rejected with clear error |

Downstream consumers (secondary-index adapters, PG-wire SQL passthrough)
live in their own repos and land independently.

## Test coverage

The shipped suite covers:

- Bitemporal config validation (both axes, only-valid, only-system,
  neither).
- `append!` round-trip (insert with explicit window, with defaulted
  window, on both axes).
- `upsert!` SCD2 (close + reopen) — `:where` predicate matching,
  `:set` value merging.
- `retract!` close-only and bounded `FOR PORTION OF` surgery.
- Overlap detection: reject + auto-split equivalence.
- SCD2-on-both-axes: system-time symmetry on every vt mutation
  (`replace-row-bitemporal!`).
- `bounded-update!` 3-way split with shared `sys-now` stamp.
- SQL grammar round-trip via the index-backed DML path.
- All 10 Allen interval predicates as SQL functions.

## Things deferred

- **Per-pattern `{:valid-at t}` annotation** in `:where` so a single
  query can join two relations at different vt instants. Useful for
  reconciliation queries. Open API design call.
- **Time-slicing helpers** (`range-bins`-style: "as of each Friday
  of 2024, what was the trial balance?"). Useful for reporting; out
  of scope for the substrate.
- **System-time DML grammar** in stratum SQL — only auto-stamp
  `_system_from` on writes; bitemporally *correcting* system-time
  the way valid-time can be corrected is a documented limitation.
- **`SET col = <expression>`** in `UPDATE … FOR PORTION OF
  VALID_TIME`: only literal values today. Expression evaluation
  requires SELECT/UPDATE engine convergence.
- **Multi-table SELECT with two `FOR VALID_TIME` clauses**: the
  rewriter emits unqualified `_valid_from` / `_valid_to` refs that
  the planner can't disambiguate across joins. Rejected at parse
  time with a hint to use explicit `<table>.col` qualifiers. Real
  fix requires qualifier-aware column resolution in the planner.

## References

- Snodgrass, R. T. *Developing Time-Oriented Database Applications in
  SQL*. Morgan Kaufmann (2000).
- SQL:2011 §4.15 "Application-time period tables" + §4.16
  "System-versioned tables" for the WITHOUT OVERLAPS invariant and
  the `_valid_from` / `_system_from` column naming convention.
- Public surveys of bitemporal database designs (Allen interval
  algebra, the Snodgrass-style temporal database literature) informed
  the high-level shape; the implementation here is derived from the
  SQL:2011 specification, not from any specific reference codebase.
