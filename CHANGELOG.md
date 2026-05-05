# Changelog

## Unreleased

### Added
- **Streaming Parquet ingest** (`stratum.parquet/index-parquet!`): reads a Parquet file row-group-by-row-group into chunked `PersistentColumnIndex` columns, syncing periodically to konserve so the chunk heap is reclaimable. Memory bounded by `chunk-size × num-cols × 8 B` during reading, independent of file size. Wired into `stratum.files/index-file-into-store!` so the `--index` and SQL `read_parquet`+`--index` paths use it automatically.

### Changed
- **`stratum.parquet/from-parquet`** (heap path) rewritten on top of pre-allocated primitive arrays + dict-encoded strings at ingest. No more `ArrayList<Object>` boxing or per-row `String` materialization. Same public API; ~5× lower peak heap on column-heavy files.

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
