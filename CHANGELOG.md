# Changelog

## Unreleased

### Fixed
- **Self-joins and overlapping column names**: JOINs between tables sharing column names (including self-joins) now produce correct results. The SQL layer rewrites qualified column references to unique internal keys, preventing right-side columns from silently overwriting left-side columns.
- **Window frame dispatch**: `ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING` now correctly routes to the sliding window path instead of falling through to the default running sum.
- **CORR scalar aggregate**: `SELECT CORR(x, y) FROM t` without GROUP BY no longer crashes.
- **COALESCE with NULL literals**: `COALESCE(NULL, 1)` and N-ary `COALESCE(NULL, NULL, 42)` now work correctly. Multi-argument COALESCE is nested into binary pairs.
- **NOT IN with NULL**: `WHERE x NOT IN (1, NULL)` now correctly returns no rows per SQL three-valued logic.

## v0.1.0

Initial public release.
