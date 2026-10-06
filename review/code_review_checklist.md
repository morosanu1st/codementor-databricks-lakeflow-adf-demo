# Daily Spark / Databricks / ADF code review checklist

**Correctness**
- Are join keys unique on the side you expect? (check with `groupBy(key).count().filter('count > 1')`)
- Are nulls in keys and filters handled explicitly?
- Is the load incremental *and* idempotent (re-running the same date gives the same result)?

**Performance**
- Any `collect()`, `toPandas()` or Python UDF on large data? Prefer built-in functions.
- Skewed joins: broadcast the small side, or enable AQE skew handling.
- Partitioning matches the main filter column; no small-file explosion (use `OPTIMIZE` / auto-compaction).

**Lakeflow / Delta**
- Expectations defined for business rules (`expect_or_drop` vs `expect` chosen deliberately).
- Streaming tables have watermarks before de-duplication.
- `MERGE` conditions avoid updating unchanged rows.

**ADF**
- Parameters instead of hard-coded paths/dates; retries and timeouts set.
- Failure path alerts someone; secrets come from Key Vault linked service.

**Maintainability**
- New sources added through framework config, not copy-pasted notebooks.
- Tests for transforms; clear naming bronze/silver/gold.
