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

1. **Reject** (default). Throw with an error citing the overlapping
   rows. Caller must split the write themselves or pass `:auto-split?
   true`.
2. **Auto-split** (`:auto-split? true`). Split the existing window into
   the pre-overlap and post-overlap pieces, re-assert each, and insert
   the new write in the middle. Mirrors XTDB v2's
   `WITHOUT OVERLAPS` invariant (xtdb2/indexer.clj:106-180).
3. **Accept** (silent). Not exposed — backdated overlaps that aren't
   handled by the writer become a correctness problem at read time.

Reject-by-default matches kontor's audit constraints (an accountant
filing a backdated invoice shouldn't have one write produce three
rows of audit trail without explicit opt-in). Auto-split is opt-in
and per-call, not per-dataset, so the policy decision lives at the
edit site.

Overlap detection runs *before* the write, on the same WHERE-matching
rowset that `vt-update!` would close. Cost: one extra zone-map-pruned
scan per write — same as the close step itself.

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
rewriter in pg-datahike: strip the temporal clauses to a side-channel
map before parsing, lower to `vt-update!` / `vt-delete!` in the
translator.

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
| C | Overlap detection: reject + opt-in auto-split | 1 |
| D | SQL grammar: `FOR PORTION OF VALID_TIME` + `FOR SYSTEM_TIME` | 2 |
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
  `IMMEDIATELY` variants). Phase H or later — XTDB v2 has 7+; we have
  only `period-overlaps?`. Datalog-only addition, no schema change.
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
  "System-versioned tables".
- XTDB v2 docs https://v2-docs.xtdb.com/ for the auto-split / WITHOUT
  OVERLAPS invariant and the `_valid_from`/`_system_from` column
  naming we mirror.
- `doc/research/58-stratum-vt-followup.md` (kontor) — write-path gap
  audit that motivated this PR's expansion.
- `doc/research/59-bitemporality-terminology-and-landscape.md`
  (kontor) — terminology survey.
- `doc/research/60-xtdb-vt-feature-comparison.md` (kontor) —
  feature-by-feature gap list against XTDB v1/v2.
