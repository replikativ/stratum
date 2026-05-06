# Changelog

## Unreleased

### Added
- **`stratum.api/parquet-dataset`** (and `close-parquet-dataset!`): zero-copy lazy-decode reader. Constant-time open — only the parquet footer is parsed up front. Each row group becomes a chunk in a `PersistentColumnIndex`, decoded on first touch and cached as a heap `long[]`/`double[]`. Per-row-group min/max/count/null-count from the parquet metadata feed stratum's zone-map pruning, so chunks the planner can prove irrelevant are never decoded. No konserve persistence — the parquet file is the storage. Read-only (`idx-set!`/`idx-append!`/`idx-sync!` throw). Use this for ad-hoc queries against a parquet file; use `index-parquet!` instead when you need persistence to konserve.
- **Streaming Parquet ingest** (`stratum.parquet/index-parquet!`): reads a Parquet file row-group-by-row-group into chunked `PersistentColumnIndex` columns, syncing periodically to konserve so the chunk heap is reclaimable. Memory bounded by `chunk-size × num-cols × 8 B` during reading, independent of file size. Wired into `stratum.files/index-file-into-store!` so the `--index` and SQL `read_parquet`+`--index` paths use it automatically.

### Changed
- **Parquet I/O via memory mapping**: `stratum.parquet` now uses an mmap-based `InputFile` (`stratum.internal.MmapInputFile`) instead of parquet-mr's default `LocalInputFile`. Eliminates the per-call `byte[]` allocation in the `read(ByteBuffer)` path and the kernel→user copy on every read. Affects all three parquet entry points (`parquet-dataset`, `from-parquet`, `index-parquet!`). Files >2 GiB are supported via the foreign-memory API.
- **Bulk-decode for dict-encoded numeric pages**: required (non-null, non-repeated) INT32/INT64/DOUBLE columns whose pages use `PLAIN_DICTIONARY` / `RLE_DICTIONARY` now bypass parquet-mr's per-value `ColumnReader.readDouble()` path. The dictionary page is decoded once per row group into a typed array; data pages are walked directly on their `byte[]` (RLE+bit-packed) and gathered against the dict 8 lanes at a time. Falls back to `ColumnReader` for unsupported encodings, nullable columns, and `DataPageV2`. Roughly 5× faster cold decode on dict-encoded columns; 4–5× on PLAIN columns from the I/O change alone.
- **`stratum.parquet/from-parquet`** is deprecated: `parquet-dataset` (queries) and `index-parquet!` (persistent ingest) cover the same use cases without the eager full-file decode. The function and its public re-export through `stratum.api` have been retired; `stratum.parquet/from-parquet` is still callable directly for backward compatibility but marked `:deprecated` in metadata. SQL `read_parquet()` now uses `parquet-dataset` under the hood.
- **`stratum.parquet/from-parquet`** (heap path) rewritten on top of pre-allocated primitive arrays + dict-encoded strings at ingest. No more `ArrayList<Object>` boxing or per-row `String` materialization. Same public API; ~5× lower peak heap on column-heavy files.

### Removed
- **`stratum.api/from-parquet`** is no longer exposed. Direct use of `stratum.parquet/from-parquet` still works but is deprecated in favor of `stratum.api/parquet-dataset`.

### Fixed
- **Parquet OOM on large files**: a 50M-row × 23-column file with two low-cardinality string columns previously needed ~43 GB just for the boxed-Long/Double/String intermediate state and OOM'd. After the changes above, the streaming path needs ~tens of MB during ingest and the heap path needs ~9 GB.
- **All-NULL int64 chunk encoding**: `chunk/chunk-to-bytes` cast `Double/MAX_VALUE` (compute-stats's "no values seen" sentinel) to `long`, throwing `Value out of range` for chunks where every row is NULL. Now guarded by null-count and encoded as constant-NULL (8 bytes).
- **Self-joins and overlapping column names**: JOINs between tables sharing column names (including self-joins) now produce correct results. The SQL layer rewrites qualified column references to unique internal keys, preventing right-side columns from silently overwriting left-side columns.
- **Window frame dispatch**: `ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING` now correctly routes to the sliding window path instead of falling through to the default running sum.
- **CORR scalar aggregate**: `SELECT CORR(x, y) FROM t` without GROUP BY no longer crashes.
- **COALESCE with NULL literals**: `COALESCE(NULL, 1)` and N-ary `COALESCE(NULL, NULL, 42)` now work correctly. Multi-argument COALESCE is nested into binary pairs.
- **NOT IN with NULL**: `WHERE x NOT IN (1, NULL)` now correctly returns no rows per SQL three-valued logic.

## v0.1.0

Initial public release.
