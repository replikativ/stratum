# 64 — Stratum vs XTDB v2: Bitemporal Architecture & Performance Predictions

**Status**: research, pre-benchmark. Static read of both codebases on 2026-05-17.
**License note**: XTDB v2 is MPL-2.0; this note describes its architecture in our own words. File:line citations only; no verbatim code blocks. Stratum is EPL-1.0.

## 1. Executive summary

1. **The fundamental shape difference**: stratum stores *materialised rows in a closed-period SCD2 layout* with mutate-in-place `_system_to` closure; XTDB v2 stores an *immutable event log of bitemporal puts/deletes/erases* keyed by `_iid` and reconstructs row visibility at read time via a per-entity "ceiling" sweep over events in reverse system-time order (see `core/src/main/kotlin/xtdb/bitemporal/PolygonCalculator.kt:21-45`, `Ceiling.kt:78-122`).
2. **Where stratum should win**: as-of-now point and range scans (no per-entity reconstruction), bulk filter+aggregate workloads (already shown to beat DuckDB), and *any* SQL surface whose hot path is "current snapshot of a wide table" — because stratum reads one row per logical entity and XTDB reads N events per entity and reduces.
3. **Where XTDB should win**: high-cardinality bitemporal-correction workloads that re-state the same entity many times, long-history audits with O(events-per-entity) cost amortised by the trie's IID locality, and any workload that needs distributed scale-out — XTDB is built for object storage + Kafka, stratum is single-process.
4. **SQL:2011 surface is at parity for the temporal core** (both engines now support `FOR (SYSTEM|VALID)_TIME (AS OF | BETWEEN | FROM…TO | ALL)`, `FOR PORTION OF VALID_TIME` DML, Allen predicates). The grammars are equivalent; the engines beneath them are not.
5. **The one thing that matters most**: stratum's bet is that *most read workloads are as-of-now and as-of-recent*, where storing one row per logical entity is dramatically more cache- and SIMD-friendly than reconstructing visibility from an event log. If that bet holds for kontor / accounting and adjacent OLTP-flavoured workloads, stratum should beat XTDB on those by 5–50× and lose on the things XTDB was actually designed for (object-storage scale, distributed replicas).

## 2. XTDB v2 architecture map

**Storage layout.** Apache Arrow files on a `BufferPool` abstraction over local disk or object storage (S3/GCS/Azure; `modules/{aws,azure,google-cloud}`). The physical unit is a **trie** of Arrow data pages keyed by `_iid` (a 16-byte content hash of the user-provided `_id`). Each table has a `data/<trie-key>.arrow` data file and a `meta/<trie-key>.arrow` metadata file with per-page temporal min/max + IID Bloom filter; trie key encodes `(level, recency, part, blockIndex)` (`core/src/main/kotlin/xtdb/trie/Trie.kt:31-117`, `TrieMetadataCalculator.kt:12-50`).

**Data schema is fixed at the storage layer**: `(_iid, _system_from, _valid_from, _valid_to, op)` where `op` is a union of `put` (with a doc struct), `delete`, `erase` (`Trie.kt:96-107`). There is **no `_system_to` column on disk**; system_to is *derived at read time*. There is also no "current vs history" split — everything is one stream of events sorted by `_iid` then descending `_system_from` (`EventRowPointer.kt:41-52`).

**Transaction model.** Append-only Write-Ahead Log (Kafka, or a local-file impl). The log is the single point of serialisation: a single writer thread reads ops, indexes them into the **live index** (an in-memory `MemoryHashTrie` of pending events; `LiveIndex.kt`, `LiveTable.kt:21-120`), and periodically flushes a "block" to durable storage as a new L0 trie file. A bitemporal update is *just one more event row* (`put` op with `_valid_from`/`_valid_to`); the prior row is never mutated. The closest thing to a "supersession" is the next-newer event for the same `_iid` shadowing the older one at read time (`PolygonCalculator.kt`).

**Bitemporal index.** Two structures cooperate:
- The **hash trie on `_iid`** (`HashTrie.kt`, `ArrowHashTrie.kt`, `MemoryHashTrie.kt`) — a HAMT-style radix trie with 2 bits per level (`LEVEL_BITS = 2`, `LEVEL_WIDTH = 4`). Gives O(log₄ N) iid-prefix navigation to the leaf pages that *might* contain a given entity.
- The **per-page temporal min/max + IID Bloom** in the meta file (`TrieMetadataCalculator.kt:23-43`). At read time, `scan.clj` prunes pages by intersecting `TemporalBounds` and the column-pushdown Blooms (`core/src/main/clojure/xtdb/operator/scan.clj:142-162`).
- At the row level, the **`Ceiling` + `Polygon`** calculation (`Ceiling.kt`, `PolygonCalculator.kt`, `Polygon.kt`) walks events for one `_iid` in reverse system-time order, building up a "valid-time ceiling" (the latest sys-time that covers each vt segment) and emitting visibility "polygons" — the bitemporal rectangles still visible at the queried (sys, vt) instant.

**Query path** for `SELECT … WHERE _id = ? FOR SYSTEM_TIME AS OF t1 FOR VALID_TIME AS OF t2`:
1. SQL → logical plan → `scan` operator (`scan.clj:194-292`).
2. `IidSelector` converts `_id` literal to its 16-byte iid, attaches a `path-pred` that walks only the matching trie branch.
3. Across all live tries (L0 + Lₙ + the in-memory `liveTrie`), compute a merge plan (`HashTrieKt.toMergePlan`, `HashTrie.kt:91-129`) — the per-bucket sorted union of trie nodes that touch the matched iid prefix.
4. For each leaf in the merge plan, page-prune via temporal min/max and Bloom (`scan.clj:142-162`, `249-285`).
5. For surviving pages, scan events (already sorted by `_iid` then descending `_system_from`), feed each into `PolygonCalculator.calculate`. The polygon emits the (vt-start, vt-end, sys-ceiling) tuples that are visible at the requested system-time; an outer filter clips to `valid_at(t2)`. The first event with `op = erase` resets the per-iid state and shadows all prior history.

**Write path** for `UPDATE … FOR PORTION OF VALID_TIME FROM x TO y SET col = v WHERE _id = ?`:
1. SQL planner expands the UPDATE into a `LogProcessor` op sequence (`xtdb.indexer.LogProcessor`).
2. The op is appended to the log; on the indexing node, the live indexer reads the op, generates one or more new `put` event rows with the new `_valid_from`/`_valid_to` window and the merged doc (`LiveTable.kt:83-97`).
3. Rows go into the `MemoryHashTrie` keyed by iid (`LiveTable.kt` `transientTrie += pos`). The trie is HAMT — O(log N) insertion that preserves sort order on iid.
4. No closing of any past row. The shadowing is implicit and computed at read time. The new event simply has a higher `_system_from`.

**Persistence and recovery.** The log is durable (Kafka or local-file `LogProcessor`). On startup, a node replays the log from a checkpoint and rebuilds the live index by re-indexing ops. When the live index is "full" (`LiveIndex.isFull`), a `finishBlock` flushes the in-memory trie as a new L0 Arrow file (`TrieWriter.writeLiveTrie`, `TrieWriter.kt:19-43`), and a **compactor** (`compactor.clj`) merges L0 → L1 → L2 with recency partitioning (LSM-style tiering; `compactor.clj:23-80`). Old L0 files are garbage-collected after compaction.

**Distribution model.** Single-writer ACID with N read replicas. All writers serialise on the log; readers fan out across nodes reading the same object-store-backed trie files and replaying the suffix of the log past their snapshot. This is "inside-out database" / Kleppmann-style log architecture, explicitly designed for cloud object storage (`docs/src/content/docs/intro/what-is-xtdb.adoc:15-19`, `concepts/key-concepts.adoc:79-83`). The hard upper bound on write throughput is whatever a single indexer thread + the log's append latency support.

**Marketed workloads** (per docs): bitemporal audit / regulated data, financial use cases (late trades, counterparty risk, backtesting — see `docs/src/content/docs/tutorials/financial-usecase/`), and "immutability walkthroughs" emphasising never-mutate-data as a developer-experience win. Bench page exists but is not central to the pitch — the pitch is *correctness + scale* not *raw throughput*.

## 3. Stratum architecture map

**Storage layout.** Per-column primitive arrays (`long[]` / `double[]`) wrapped in `PersistentColumnIndex` — a PSS tree of 8K-element chunks with per-chunk zone-map stats (`doc/storage-and-indices.md`, `src/stratum/index.clj:852-870`, `src/stratum/column.clj`). Strings are dict-encoded with `:dict-alpha-masks` / `:dict-bigram-masks` for SIMD LIKE/ILIKE. Persistence is konserve-backed (PSS nodes as content-addressed UUID keys; commit graph for branching) — git-like, not LSM.

**Data schema.** User columns + (optional) bitemporal axes via `:bitemporal {:valid {…} :system {…}}` config. The axes are *plain int64 columns* (`_valid_from`, `_valid_to`, `_system_from`, `_system_to`), stored side-by-side with user columns in the same row layout. There is one materialised row per (eid × valid-slice × system-belief) — i.e. SCD2 (`doc/temporal-design.md:23-98`).

**Transaction model.** Mutate-in-place on a transient dataset, `persistent!` returns the new immutable snapshot, optional `sync!` writes the snapshot's dirty chunks to konserve and records a commit in the branch's commit graph (`src/stratum/storage.clj:1-100`). Three write primitives: `append!` (pure insert), `upsert!` (close `_valid_to` on matched rows, append new row — classical SCD2), `retract!` (close-only, or bounded `FOR PORTION OF` surgery). All three accept `:system-from` / `:valid-from` tx-meta for deterministic clock pinning and historical-import.

**Bitemporal index.** Same chunk-level zone-maps as every other column (`src/stratum/index.clj:1129-1180`). A point-in-time predicate `_valid_from <= v AND _valid_to > v` is two zone-pruned SIMD scans over those columns. There is no per-entity hash index — entity lookup is a SIMD-filtered scan over the `:eid` column (typically dict-encoded). Composing the two axes is just four AND'd predicates over four columns in one pass (`doc/temporal-design.md:540-567`).

**Query path** for the same `WHERE eid = ? FOR SYSTEM_TIME AS OF t1 FOR VALID_TIME AS OF t2`:
1. SQL → preprocessor strips `FOR …` clauses to side-channel `:period` maps and injects equivalent `WHERE` predicates (`src/stratum/sql/rewrite.clj:557-720`).
2. Planner lowers to a fused predicate over `(eid, _valid_from, _valid_to, _system_from, _system_to)`.
3. Execution: per chunk, zone-map prune (skip chunks where the eid range can't contain `?`, etc.); on surviving chunks, run a single SIMD pass evaluating the AND of all five predicates and emit matching row indices.

**Write path** for `UPDATE … FOR PORTION OF VALID_TIME FROM x TO y SET col = v WHERE eid = ?`:
1. SQL preprocessor strips `FOR PORTION OF VALID_TIME` to `(x, y)`.
2. Lowers to `dataset/bounded-update!`: classify matching rows into truncate-left / truncate-right / split / drop / replace, then run one `retract!` over the slice + one `append!` per captured non-overlap fragment, all under a shared `sys-now` stamp. The superseded row's `_system_to` is set to `sys-now`; new rows get `_system_from = sys-now`.
3. The whole sequence is one in-memory mutation; durability is the next `sync!`.

**Persistence and recovery.** A konserve store (PSS nodes + branch refs). Recovery is "load the branch HEAD"; there is no log to replay because the commit graph *is* the durable history (`src/stratum/storage.clj:273-295`).

**Distribution model.** Single-process. There is no replication / sharding story today. Konserve has a server mode (`src/stratum/server.clj`) but that's a client-server façade, not a multi-writer cluster.

## 4. Side-by-side operation table

For each operation, "winner" is the *architecturally predicted* winner; the architectural reason is the one-sentence rationale.

| Operation | Predicted winner | Architectural reason |
|---|---|---|
| **Point lookup by PK, as-of-now** (`WHERE id=?`) | **Stratum** (close to flat columnar lookup) | XTDB still does trie navigation + per-iid Polygon sweep over events. Stratum does one zone-pruned SIMD scan over the eid column on the current snapshot. |
| **Point lookup by PK, time-traveled** (`AS OF <past>`) | **Stratum** for moderate history depth; **XTDB** as history depth → ∞ | Both engines need to filter on `_system_from <= t < _system_to`. Stratum just adds two predicate terms in the SIMD pass over the same row layout. XTDB has to walk events back to `t` and project. For very deep history per entity (e.g. 100s of corrections), XTDB's iid-locality + Polygon's reverse linear search become competitive; stratum still wins until the SCD2 row-fanout becomes the limiting factor. |
| **Range scan + filter, as-of-now** (`WHERE salary > 100k`) | **Stratum, by a lot** | Stratum's bread and butter — fused predicate + SIMD over `long[]`. The closed-period SCD2 layout means each entity has exactly one current row; the scan is over current rows only (plus closed history rows that the `_system_to > now` predicate filters in the same pass). XTDB has to merge events across L0..Lₙ tries and reduce per-iid via Polygon before the user predicate can fire. |
| **Range scan, time-traveled** | **Stratum** | Same as above; the past-time predicate is just two more SIMD-friendly column compares. XTDB's reduce-per-iid cost is paid for every entity in the scan range. |
| **Bitemporal point query (both axes non-now)** | **Stratum** | Four predicates over four columns in one pass. XTDB still does per-iid Polygon reconstruction and clips. |
| **Aggregate over a valid-time range** (`SUM(amount) FOR VALID_TIME FROM x TO y`) | **Stratum** | Existing benchmarks show stratum beats DuckDB on filter-aggregate; the temporal filter is two extra predicates evaluated in the same SIMD pass. XTDB has to project per-iid polygons first, then aggregate over the projection. |
| **Single-row supersession** (one `UPDATE FOR PORTION OF VALID_TIME`) | **XTDB** | One append into a HAMT. Stratum does a scan to find matching rows + one column-write per matched row + one append per fragment — strictly more bytes touched. Latency at small batch sizes likely favours XTDB. |
| **Bulk append (100k rows)** | **Stratum**, if the dataset is single-process; **XTDB** if the workload assumes distributed ingest | Stratum appends to in-memory arrays — close to memcpy speed. XTDB has to round-trip through the log, deserialise, index into the live trie, eventually flush a block — durable but slower per-row. If you require durable, replicated bulk ingest, XTDB's architecture is right; if you can `sync!` once at the end, stratum wins. |
| **Bulk supersession (UPDATE WHERE hitting 10k rows)** | **Stratum** | One scan + 10k column-writes + 10k appends, all in-memory. XTDB has 10k events to log, index, and merge-on-read forever after — write cost similar, read cost diverges. |
| **Long history scan** (`FOR SYSTEM_TIME ALL` on hot key) | **XTDB** | This is what the iid trie + Polygon were *built* for: iid-prefix navigation + event-ordered sweep is asymptotically optimal for "give me everything about this entity". Stratum must scan the whole history of the table looking for that eid. |
| **Storage efficiency over N supersessions** | **Depends** | Per supersession, stratum writes one new row (full width) + one `_system_to` field. XTDB writes one event row carrying the new `put`'s doc struct (also full width of changed columns; nullable for unchanged in the struct union). Net: comparable on the wire. Stratum's persistent chunks are CoW so unchanged columns aren't rewritten on `sync!`; XTDB's L0 → Lₙ compaction lets it consolidate event runs into denser pages. Both have roughly N-row growth per N corrections, with engine-specific compaction. |

## 5. SQL:2011 feature matrix

Cells: ✔ = supported, ◐ = partial, ✗ = not supported, — = N/A.

| Feature | Stratum | XTDB v2 |
|---|---|---|
| `_valid_from` / `_valid_to` columns | ✔ (opt-in per dataset, via `:bitemporal {:valid …}`) | ✔ (always present) |
| `_system_from` / `_system_to` columns | ✔ (opt-in per dataset) | ✔ on read; `_system_to` is *derived*, never persisted |
| `SELECT … FOR VALID_TIME AS OF \| BETWEEN \| FROM…TO \| ALL` | ✔ | ✔ |
| `SELECT … FOR SYSTEM_TIME AS OF \| BETWEEN \| FROM…TO \| ALL` | ✔ | ✔ |
| Composable VT + ST clauses on one SELECT | ✔ (single clause per axis) | ✔ |
| Two `FOR VALID_TIME` clauses across joined tables | ✗ (qualifier disambiguation TBD; workaround documented) | ✔ (handled via per-table iid scan) |
| `INSERT … FOR PORTION OF VALID_TIME` | ✔ (via `dataset/append!`) | ✔ |
| `UPDATE … FOR PORTION OF VALID_TIME` | ✔ (via `bounded-update!`; literals only, no expressions) | ✔ |
| `DELETE … FOR PORTION OF VALID_TIME` | ✔ | ✔ |
| `FOR PORTION OF SYSTEM_TIME` DML | ✗ rejected by design (audit integrity); Clojure DSL escape hatch via `tx-meta :system-from` | ✗ in grammar — only `SYSTEM_TIME=<expr>` tx-option (whole-tx pin), analogous to stratum's escape hatch |
| `ERASE` / hard purge | `ds-delete-rows!` (Clojure DSL); no SQL surface | ✔ `ERASE FROM …` SQL |
| `MERGE` / upsert SQL | ✗ (`INSERT … ON CONFLICT … FOR PORTION OF` explicitly rejected) | `PATCH INTO` (analogous, with VT portion) |
| Allen predicates (OVERLAPS, EQUALS, CONTAINS, PRECEDES/SUCCEEDS, IMMEDIATELY_*, MEETS) | ✔ as 4-arg WHERE functions | ✔ as `PERIOD … OVERLAPS …` infix on period-typed exprs |
| `WITHOUT OVERLAPS` constraint enforcement | ◐ (overlap detection in write primitives: reject or auto-split; pre-existing-overlap caveat for bounded INSERT/UPDATE) | ✔ engine-maintained (per docs) |
| Default `VALID_TIME` setting per table | ✗ | ✔ (`SETTING DEFAULT VALID_TIME …`) |
| Snapshot isolation by past system-time | ◐ (per-query) | ✔ (per-tx `SNAPSHOT_TIME` read option) |
| Schemaless / dynamic columns | ✗ (typed columns) | ✔ (Arrow union types; "document" model) |

## 6. Honest weaknesses — where stratum is structurally behind

1. **No distribution story.** XTDB scales reads horizontally over object storage + a log. Stratum is single-process. A consumer workload that needs N read replicas off the same write log is out of scope; the closest analogue is konserve-server, which is a single-process server, not a cluster.
2. **No log; recovery is "load the snapshot".** XTDB can replay the log to any point; stratum can walk the commit graph but the unit is "whole-dataset snapshots", not "individual transactions". For audit, this is fine (the commit graph is the audit trail). For some regulated reconstruction workflows (e.g. "show me the literal database state shown to a user on 2024-08-13 at 14:32:00.123"), the closed-period mutate-of-`_system_to` means the literal column value at that past instant is the closure value, not the `Long/MAX_VALUE` it held at the time (documented at `doc/temporal-design.md:122-139`). XTDB's append-only event store has no such artefact.
3. **No per-entity index.** Stratum's entity lookup is a column scan with zone-map pruning. For a table with many entities and selective `WHERE eid = ?` access, XTDB's iid hash trie is asymptotically better. Stratum's dict-encoded eid + zone maps narrows the scan but still O(matched chunks) where XTDB is O(log N) trie navigation. *Likely irrelevant* for analytical workloads, *very relevant* for OLTP key-value-ish access patterns.
4. **No streaming / log-tailing reads.** XTDB readers can tail the log for fresh writes (subject to indexer latency); stratum readers see snapshots taken at `sync!` boundaries. Real-time consumers need a different pattern.
5. **Single-writer is implicit, not architectural.** Stratum doesn't enforce a global single-writer invariant — concurrent writers on the same dataset are a correctness problem the caller must avoid. XTDB makes single-writer the architecture, which is a feature for serialised audit.
6. **`MERGE` / upsert SQL gap.** XTDB has `PATCH INTO` with VT portions; stratum rejects `INSERT … ON CONFLICT … FOR PORTION OF` as ambiguous. Composing `DELETE FOR PORTION OF` + `INSERT FOR PORTION OF` works but is two statements.

## 7. Where stratum should structurally crush XTDB v2

1. **Filter-aggregate over recent data.** Stratum's SIMD-fused predicate+aggregate path (already 1.5–3.4× faster than DuckDB on TPC-H Q1/Q6, ClickBench-style workloads — see `doc/benchmarks.md`) translates directly to bitemporal `FOR VALID_TIME AS OF NOW` + `FOR SYSTEM_TIME AS OF NOW` aggregates. XTDB's per-iid reduce step has no equivalent fused SIMD path.
2. **Wide as-of-now point queries.** "Give me the current state of entity X across 30 columns" is one row × 30 columns in stratum vs. log scan + Polygon reconstruction in XTDB. The cache-line footprint difference is large.
3. **Bulk SCD2 corrections.** A single tx that supersedes 10k matched rows is *two predicate evaluations + two column-array writes per row + 10k appends* in stratum. In XTDB it's 10k log events plus the future cost of merging them on every read until compaction.
4. **REPL-driven historical-import** (`:system-from` in tx-meta on the Clojure DSL) — stratum's surface is one function call; XTDB's `SYSTEM_TIME=<expr>` tx option is whole-tx-scoped and goes through the log.
5. **Embedded / co-resident-with-application** use cases. Stratum is one JAR + datahike + konserve store. XTDB's smallest deployment is several JVM processes + Kafka or a local-log directory + the indexer cycle. For kontor's "embedded in a JVM Clojure app" target audience, stratum has zero operational footprint.
6. **Anomaly detection / Forest-style ML over recent windows** (`src/stratum/iforest.clj`, `doc/anomaly-detection.md`) — stratum's columnar in-memory layout is ideal for these workloads; XTDB has no equivalent.

## 8. Strategic recommendation

To "blow XTDB out of the water on bitemporal":

**Benchmark.**
1. Define a canonical bitemporal workload mirroring `docs/src/content/docs/tutorials/financial-usecase/*` (late-trade reconstruction, backtest, counterparty risk-as-of-now-and-history). Run both engines with identical Arrow input and report wall-clock per query category.
2. Add SCD2-heavy variants: tables with 5×, 50×, 500× supersessions per logical entity. This stresses XTDB's per-iid reduce; stratum scales linearly in stored rows.
3. Mixed `FOR VALID_TIME AS OF NOW` + `FOR SYSTEM_TIME AS OF <past>` reads — the common audit pattern.
4. Bulk-ingest comparison at fair durability semantics (stratum `sync!` per N rows vs XTDB block flush).
5. Single-tx supersession of 10k matching rows (where stratum's SCD2 should crush per-row event ingest).

**Optimise.**
1. **Per-entity secondary index for selective lookup** — even a dict-encoded eid + Roaring bitmap of "current rows" would close the gap with XTDB's iid trie for the OLTP-flavoured WHERE eid=? case. Could live as an optional `:index :eid` in dataset config.
2. **Push the overlap-detection scan through the planner's zone-map pruner** (already flagged as Phase C+ deferred work in `doc/temporal-design.md:308-311`). Currently it's a linear pass; with zone-maps it's sublinear in supersession-heavy workloads.
3. **Streaming reader cursor** (optional, addresses one of the "structurally behind" items) — for consumers that want long-running queries against a periodically `sync!`-ed dataset, a poll-and-tail API would close the gap with XTDB's log-tailing without forcing a log into stratum's design.
4. **Persisted dataset-wide `_system_to`-aware bitmap** of "live rows" — current rows have `_system_to = MAX`. Maintaining a CoW Roaring bitmap of those positions would make as-of-now queries skip closed history almost for free.

**Advertise.**
1. **"As-of-now is free."** The marketing line. Bitemporal-without-the-tax. Both axes are columns; both predicates evaluate in the same SIMD pass that the user's WHERE already uses.
2. **"Embedded, single-JAR, datahike-only."** Operational contrast with XTDB's distributed footprint. For accounting / OLTP-flavoured workloads inside a JVM application, the deployment story matters as much as the query story.
3. **"SCD2 the way the SQL:2011 spec describes it."** Stratum's storage is the textbook SCD2 layout — one row per (entity × valid-slice × system-belief). XTDB's event-log-reconstructed-at-read is architecturally novel but conceptually heavier for users who already think in SCD2 terms.
4. **Beats DuckDB at the things DuckDB was built for** (already measured) — and the bitemporal axes are columns those same SIMD paths read for free.

## 9. Open questions to resolve by running XTDB v2

1. **Real wall-clock per-iid Polygon cost at common history depths.** Static read of `Ceiling.kt` / `PolygonCalculator.kt` shows reverse linear search + per-event allocation-free updates; that's cheap per event but the *constant* relative to a stratum column scan is what we'd need a benchmark to pin down.
2. **Compaction cadence and its effect on read latency.** XTDB's read path merges across L0 + L1H + L2H tries; with the live trie this is potentially many segments. How does query latency degrade between compactions, especially under steady write load?
3. **Block flush latency.** What's the wall-clock cost of `LiveIndex.finishBlock` at e.g. 1M rows in the live trie? Stratum's equivalent is `sync!` and we know its cost; XTDB's is opaque from static read.
4. **Read-replica freshness lag** in distributed mode. The docs claim eventual consistency on the suffix of the log; the lag under contention is empirical.
5. **Erase semantics under audit pressure.** XTDB's `ERASE` purges all events for an iid; stratum's analog is `ds-delete-rows!`. Both have an audit footprint via the commit/log respectively. Whether `ERASE`-d rows leave a recoverable trace via the log is worth confirming for regulated-audit consumers.
6. **`PATCH INTO`** — what does it lower to at the storage layer? If it's "one event with a partial doc that the reader merges with the prior visible row", that's a per-read merge cost stratum doesn't pay (because stratum stores the full new row).
7. **Schema migration story** when XTDB has no upfront schema. For kontor's strongly-typed `:account/*`, `:posting/*` etc. namespaces this is irrelevant, but a comparison benchmark needs to fix the schema for both engines.
8. **Memory pressure of the live trie** under high write rate before flush. Stratum's equivalent is "the transient dataset"; both are bounded by available heap, but the constants differ.
9. **Whether XTDB exposes the Polygon cost in EXPLAIN.** Stratum's SQL planner already reports zone-map prune stats; XTDB likely has similar instrumentation in its operator layer that we'd want to dump.

---

**References cited (file:line)**
- XTDB v2 (MPL-2.0; described in our own words, no verbatim code):
  - `core/src/main/kotlin/xtdb/bitemporal/{Ceiling,Polygon,PolygonCalculator}.kt`
  - `core/src/main/kotlin/xtdb/trie/{Trie,HashTrie,ArrowHashTrie,MemoryHashTrie,TrieWriter,EventRowPointer}.kt`
  - `core/src/main/kotlin/xtdb/indexer/{LiveIndex,LiveTable,TrieMetadataCalculator}.kt`
  - `core/src/main/clojure/xtdb/operator/scan.clj:142-292`
  - `core/src/main/clojure/xtdb/{log,compactor,metadata}.clj`
  - `core/src/main/antlr/xtdb/antlr/Sql.g4:578-840`
  - `docs/src/content/docs/intro/{what-is-xtdb,data-model}.adoc`
  - `docs/src/content/docs/concepts/key-concepts.adoc`
- Stratum (EPL-1.0):
  - `doc/temporal-design.md`
  - `doc/benchmarks.md`
  - `doc/storage-and-indices.md`
  - `src/stratum/{dataset,storage,column,index}.clj`
  - `src/stratum/sql/rewrite.clj:190-720`
  - `src/stratum/query/predicate.clj`
