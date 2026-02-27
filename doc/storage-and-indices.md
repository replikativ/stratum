# Storage and Indices

Stratum provides persistent columnar data structures with copy-on-write (CoW) semantics, zone map statistics for query optimization, and pluggable storage via Konserve.

## PersistentColChunk

The fundamental data container. Wraps a Java heap array (`long[]` or `double[]`) with Clojure-style persistent/transient semantics:

```clojure
;; Create a chunk
(def chunk (chunk/col-chunk-from-seq [1.0 2.0 3.0]))

;; Fork is O(1) - structural sharing
(def forked (chunk/col-chunk-fork chunk))

;; Mutation requires transient mode
(-> chunk
    chunk/col-chunk-transient
    (chunk/col-chunk-set 0 999.0)    ;; CoW: copies on first write
    chunk/col-chunk-persistent!)
```

CoW behavior:
- `col-chunk-fork` returns a new chunk sharing the same underlying array (increments a reference counter)
- First mutation on a shared chunk copies the array, then mutates the copy
- Subsequent mutations on the same transient modify in-place (amortized O(1))
- `col-chunk-persistent!` freezes the chunk, preventing further mutation

Default chunk size is 8192 elements (~64KB for doubles), chosen for L2 cache residency.

## PersistentColumnIndex

A tree of chunks organized by a persistent sorted set (PSS), providing an ordered columnar index with O(log N) point lookup:

```clojure
;; Create from data
(def idx (index/index-from-seq (range 1000000)))

;; Point lookup
(index/idx-get idx 42)  ;; => 42.0, O(log chunks)

;; Fork (O(1) structural sharing of tree + chunks)
(def idx2 (index/idx-fork idx))

;; Mutation
(-> idx
    index/idx-transient
    (index/idx-set 42 999.0)  ;; CoW on affected chunk only
    index/idx-persistent!)
```

Each node in the PSS tree is a `ChunkEntry` record:

```clojure
{:chunk-id   [start-idx end-idx]    ;; Position range
 :chunk      <PersistentColChunk>    ;; The data
 :stats      <ChunkStats>}          ;; Per-chunk statistics
```

The PSS uses weighted navigation: each chunk knows its element count, enabling O(log chunks) positional access.

### Insert and Delete

Insert and delete operations use `System.arraycopy` for efficient in-chunk shifting and maintain incremental statistics:

```clojure
(-> idx
    index/idx-transient
    (index/idx-insert 500 42.0)     ;; Insert at position 500
    (index/idx-delete 100)          ;; Delete position 100
    index/idx-persistent!)
```

Chunks split when exceeding max size and merge when falling below min size, maintaining balanced chunk sizes.

## ChunkStats and Zone Maps

Every chunk maintains aggregate statistics computed incrementally on mutation:

```clojure
{:count   8192
 :sum     4.12e7
 :sum-sq  2.07e14
 :min     0.01
 :max     9999.99}
```

The query engine uses these statistics for zone map pruning - skipping entire chunks that can't contain matching rows.

Standard zone maps (Parquet row group statistics, DuckDB's `NumericStatsData`) store only min and max per segment, primarily for predicate filter pushdown. Stratum extends this with per-chunk sum and sum-of-squares, following the approach of Apache ORC ColumnStatistics. The addition of sum and count enables a wider class of aggregates to short-circuit without scanning row data - a capability not present in DuckDB, which must do a full scan for `COUNT(*)`, `SUM`, or `AVG` even without a WHERE clause.

### Three-Way Chunk Classification

For each predicate, chunks are classified into three categories:

| Category | Condition | Action |
|----------|-----------|--------|
| **Skip** | No values can match (e.g., `min > threshold` for `< threshold`) | Skip entirely |
| **Stats-only** | All values match (e.g., `max < threshold` for `< threshold`) | Use chunk stats for SUM/COUNT/MIN/MAX |
| **SIMD** | Some values may match | Process with full SIMD evaluation |

Zone map predicates in `stats.clj`:

```clojure
;; Can any value in this chunk satisfy (< col threshold)?
(zone-may-contain-lt? stats threshold)
;; => true if stats.min < threshold

;; Do ALL values satisfy (< col threshold)?
(zone-fully-inside-lt? stats threshold)
;; => true if stats.max < threshold (all values match)
```

### Stats-Only Aggregation

When all chunks in a query are classified as stats-only (all values match all predicates), aggregation results can be computed directly from ChunkStats:

- **SUM**: Sum of per-chunk sums
- **COUNT**: Sum of per-chunk counts
- **MIN**: Min of per-chunk mins
- **MAX**: Max of per-chunk maxes

This provides O(chunks) aggregation instead of O(N).

## Index-Aware Query Execution

The query engine detects PersistentColumnIndex inputs and uses specialized paths:

### Pruned Materialization

`idx-materialize-to-array-pruned` materializes only surviving chunks (those not skipped by zone maps) into a contiguous heap array:

```
Index: [chunk0][chunk1][chunk2][chunk3][chunk4]
                  ↓ zone map pruning
Survive:         [chunk1]       [chunk3][chunk4]
                  ↓ materialize
Array:  [chunk1 data | chunk3 data | chunk4 data]
```

This is applied early in the execution pipeline, before expression pre-computation, so downstream operations work on a shorter array.

### Chunked Streaming Group-By

For group-by queries on indices, `execute-chunked-group-by` streams over chunks without materializing the full array:

```
For each chunk:
  1. Copy chunk data to temp array (64KB, L2-resident)
  2. Scatter-accumulate into dense group accumulators
  3. Discard temp array

Merge: element-wise sum of per-thread accumulators
```

This is 2.5x faster than materializing the full array for compound aggregations (VARIANCE, CORR) because each 64KB chunk stays L2-resident. The full-array path scatters across 48MB, hitting DRAM latency.

Chunk streaming supports all dense-path aggregation operations (SUM, COUNT, MIN, MAX, AVG, SUM_PRODUCT) and extended operations (VARIANCE, CORR) via variable-width accumulators.

## Konserve Storage - PSS-Backed Lazy Loading

Indices are persisted via the persistent sorted set (PSS) library's `IStorage` protocol, backed by konserve. The PSS tree itself - including all chunk data inline in leaf nodes - is stored in konserve and lazy-loaded on demand.

### Memory Modes

An index can be in one of three modes:

**In-memory** - The default after `make-index` or `index-from-seq`. The entire PSS tree lives in JVM heap. The `storage` field is nil. This is the mode all OLAP benchmarks use.

**Storage-backed** - After `idx-sync!`. The PSS tree nodes have been written to konserve and have UUID addresses. The CachedStorage LRU cache still holds all nodes in memory, so access patterns are identical to in-memory mode. Subsequent syncs are incremental: `pss/store` walks the tree and only writes nodes without addresses (dirty nodes from transient mutations).

**Lazy-loaded** - After `restore-index-from-snapshot`. Only the root address is known - no data is loaded. When the query engine iterates chunks (via `pss/slice` or point lookup), each PSS node access triggers `IStorage.restore()` which loads the node from konserve, deserializes it via Fressian, and caches it in the LRU cache. This is the key property for billion-row scale: opening a dataset doesn't require loading all data upfront.

### Usage

```clojure
(require '[konserve.memory :refer [new-mem-store]])
(require '[stratum.index :as index])
(require '[stratum.dataset :as dataset])
(require '[stratum.storage :as storage])

;; Create a store (sync mode)
(def store (new-mem-store (atom {}) {:sync? true}))

;; Index-level persistence (no branches - indices are internal)
(def idx (index/index-from-seq :float64 (range 1000000)))
(def synced (index/idx-sync! idx store))          ;; writes PSS tree to konserve
(def commit-id (get-in (meta synced) [:commit :id]))
(def snapshot (storage/load-index-commit store commit-id))
(def restored (index/restore-index-from-snapshot snapshot store))
;; restored is lazy - no chunks loaded yet

;; Incremental sync: modify and re-sync (only dirty nodes written)
(def modified (-> synced
                  index/idx-transient
                  (index/idx-set! 0 999.0)
                  index/idx-persistent!))
(def synced2 (index/idx-sync! modified store))

;; Dataset-level persistence (datasets own branches)
(def ds (dataset/make-dataset {:price idx} {:name "trades"}))
(def saved (dataset/ds-sync! ds store "main"))
(def loaded (dataset/ds-load store "main"))

;; Garbage collect unreachable PSS nodes from dataset branches
(storage/ds-gc! store)
```

### CachedStorage

`CachedStorage` (in `cached_storage.clj`) implements the PSS `IStorage` protocol:

- **store**: Assigns a UUID address to a PSS node, buffers write in `pending-writes`, caches in LRU
- **restore**: Checks LRU cache first, then loads from konserve and deserializes via Fressian
- **accessed**: Updates LRU cache hit for the address

Fressian handlers serialize PSS nodes (Leaf, Branch), ChunkEntry records (with chunk data inline via `chunk-to-bytes`), and ChunkStats. The branching factor is 64, so each Leaf holds up to 64 ChunkEntries (~4MB serialized). At 1B rows this gives a 3-level tree (~1900 leaves, ~30 branches, 1 root).

### Addressing Modes

**Random UUID** (default): Each `idx-sync!` assigns fresh random UUIDs to new/dirty PSS nodes. Two syncs of identical data produce different addresses.

**Content-addressed (merkle)**: When `{:metadata {:crypto-hash? true}}` is set on index creation, addresses are computed deterministically from content - Branch addresses hash child addresses, Leaf addresses hash chunk-ids and stats. Identical content always produces the same commit ID, enabling deduplication and integrity verification.

### Storage Layout in Konserve

```
<uuid>                        → serialized PSS node (Leaf or Branch)
[:indices :commits <uuid>]    → index snapshot {:pss-root, :total-length, :datatype, ...}
[:datasets :commits <uuid>]   → dataset snapshot {:columns {:col {:index-commit <uuid>}}}
[:datasets :heads <branch>]   → HEAD commit UUID
[:datasets :branches]         → #{"main" "feature"}
```

### Garbage Collection

`ds-gc!` performs mark-and-sweep from dataset branch heads:

1. Walk parent chains from all branch HEADs to find reachable dataset commits
2. Collect referenced index commits from dataset snapshots
3. Restore each index's PSS tree and walk all node addresses via `pss/walk-addresses`
4. Sweep: delete konserve keys not in the live set

Features:
- **Dataset-level branches**: Datasets own branches; indices are internal
- **Atomic commits**: Branch HEAD pointers update atomically
- **Incremental sync**: Only dirty PSS nodes written (path from modified leaf to root)
- **Lazy loading**: Zero startup cost - data loaded on demand via IStorage
- **Structural sharing**: Forked indices share PSS nodes in storage
- **Garbage collection**: Mark-and-sweep from branch heads, walks PSS trees

## Performance Characteristics

| Operation | Complexity | Notes |
|-----------|------------|-------|
| idx-fork | O(1) | Structural sharing of tree + all chunks |
| idx-get | O(log chunks) + O(1) | PSS weighted navigation + array access |
| idx-set (transient) | O(1) amortized | CoW on first write per chunk |
| idx-insert/delete | O(chunk) | System.arraycopy within chunk |
| Zone map pruning | O(chunks) | One stats comparison per chunk per predicate |
| Stats-only aggregation | O(chunks) | No data access needed |
| Chunked group-by | O(N) | 64KB chunks, L2-resident |
| Full materialization | O(N) | Single copy, chunks → contiguous array |

## Related Documentation

- [Architecture](architecture.md) - System overview
- [SIMD Internals](simd-internals.md) - Chunked streaming SIMD details
- [Query Engine](query-engine.md) - Index-aware dispatch logic
