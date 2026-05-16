# Temporal Design — Bitemporal Stratum

This document captures the design decisions behind stratum's bitemporal
write and read surface. It supersedes the original "valid-time column
convention" framing of an earlier draft of this PR — bitemporality is
the whole feature, not a column naming guideline.

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

## Write primitives

Three primitives in `stratum.dataset`, all returning a new dataset value:

```clojure
(vt-append! ds rows-or-cols
            {:valid-from inst   ; defaults to (System/currentTimeMillis)
             :valid-to   inst   ; defaults to Long/MAX_VALUE (open)
             :system-from inst  ; defaults to tx-time (now)
             :system-to   inst  ; defaults to Long/MAX_VALUE (open)
             :auto-split? false})

(vt-update! ds {:where    [pred ...]   ; stratum WHERE clause
                :set      {col val}
                :valid-from inst       ; required when :valid is configured
                :valid-to   inst       ; defaults to Long/MAX_VALUE
                :auto-split? false})

(vt-delete! ds {:where    [pred ...]
                :valid-from inst       ; required when :valid is configured
                :valid-to   inst       ; defaults to Long/MAX_VALUE
                :auto-split? false})
```

**`vt-append!`** writes new rows with the supplied or defaulted vt /
system windows. No close. Pure insertion.

**`vt-update!`** is the SCD2 close-and-reopen primitive:
1. Find rows matching `:where` whose `_valid_to` (or `_system_to`)
   still spans the new window.
2. Close their `_valid_to` to the new `:valid-from` (write the close
   intervals).
3. Append new rows with the merged `:set` values and the new vt /
   system windows.

**`vt-delete!`** is "close without reopen": same find step, set
`_valid_to` to `:valid-from`, no new rows. Logically a retraction
over the specified vt slice. Physical purge stays on `:db/purge`
semantics — never auto-triggered by `vt-delete!`.

### Why predicate-driven, not entity-keyed?

Stratum core doesn't know what "an entity" is. The `:eid` convention
is application-specific (kontor uses `:eid`, scriptum would use a
doc-id, proximum a vector-id). A predicate-driven primitive stays
generic and reuses the WHERE-clause DSL the planner already
understands — zone-map pruning and parallel scan come for free.

Callers that *do* have an entity key wrap the primitive trivially:

```clojure
(defn upsert-entity! [ds eid attrs vt-from]
  (vt-update! ds {:where      [[:= :eid eid]]
                  :set        attrs
                  :valid-from vt-from}))
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
-- bitemporal as-of read (already in pg-datahike#7 as session vars)
SET datahike.valid_at = '2024-04-15T00:00:00Z';
SET datahike.system_at = '2024-04-20T00:00:00Z';

-- per-query forms (this PR)
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

## Why now

PR #26's column-convention foundation is correct but inert. Every
non-trivial write surface in stratum is currently vt-blind (see
`doc/research/58-stratum-vt-followup.md` in kontor). Without the
primitives, every adapter has to re-invent SCD2 — which is exactly
what `datahike-bitemporal-v1`'s adapter does today, with 150 LOC of
close-and-reopen logic that belongs in stratum core.

After this PR:
- One canonical SCD2 in stratum.
- Adapters become thin (datahike adapter shrinks by ~100 LOC).
- pg-datahike SQL surface speaks SQL:2011 directly.
- Both axes get equal treatment from the planner.

## Phases (review-able commit boundaries)

| Phase | Scope | Commits |
|---|---|---|
| A | Schema redesign: `:bitemporal {:valid :system}` | 1 |
| B | Write primitives: `vt-append!` / `vt-update!` / `vt-delete!` | 2 |
| C | Overlap detection: reject by default | 1 |
| C+ | Auto-split: truncate partial-left + drop fully-superseded (via `ds-delete-rows!`) | 1 |
| D | SQL grammar `FOR PORTION OF VALID_TIME` (DELETE) + bounded retract! + index-backed SQL DELETE | 1 |
| D+ | `FOR PORTION OF VALID_TIME` on UPDATE + INSERT (via `bounded-update!`); UPSERT rejected with clear error | 1 |
| E | Datahike adapter refactor: use new primitives | 1 |
| F | Datahike `d/system-at` wrapper | 1 |
| G | pg-datahike `datahike.system_at` session vars + SQL passthrough | 1 |

Phases A-D land in stratum on the existing `feature/valid-time` branch
(PR #26 retitled). Phase E lands in `datahike-bitemporal-v1`'s
`feature/bitemporal-v1` (PR #828). Phases F-G land in
pg-datahike's `feature/valid-time` (PR #7).

## Tests we'll add

- Bitemporal config validation (both axes, only-valid, only-system,
  neither).
- `vt-append!` round-trip (insert with explicit window, with defaulted
  window, on both axes).
- `vt-update!` SCD2 (close + reopen) — `:where` predicate matching,
  `:set` value merging.
- `vt-delete!` close-only.
- Overlap detection: rejects + auto-split equivalence.
- Four-axis composition under the planner.
- SQL grammar round-trip via pg-datahike.

## Things deferred

- **Allen interval rules** (the full set: `overlaps`, `meets`,
  `during`, `starts`, `finishes`, `equals` with `STRICTLY` /
  `IMMEDIATELY` variants). Phase H or later — the SQL:2011 stdlib
  defines 7+; we have only `period-overlaps?`. Datalog-only
  addition, no schema change.
- **Per-pattern `{:valid-at t}` annotation** in `:where` so a single
  query can join two relations at different vt instants. Useful for
  reconciliation queries. Open API design call.
- **Time-slicing helpers** (`d/range-bins`-style: "as of each Friday
  of 2024, what was the trial balance?"). Useful for reporting; out
  of scope for the substrate.
- **System-time DML grammar** in stratum SQL (we only auto-stamp
  `_system_from` on writes; users can't bitemporally correct
  *system-time* in the way they correct valid-time). Documented
  limitation; reopen if a real consumer demands it.

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
- Internal kontor research notes (not shipped with stratum)
  enumerated point-in-time gap analyses and terminology surveys
  that motivated this PR's expansion.
