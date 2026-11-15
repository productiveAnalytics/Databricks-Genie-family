---
name: medallion-from-raw
description: Build and run a bronze, silver, gold medallion Lakeflow Spark Declarative Pipeline (SDP) from raw source tables. Read this skill when the user provides a source schema, a target publish schema, and a description of the gold analytics tables they want, and asks to build/ingest/transform a pipeline from raw tables into a medallion architecture.
---

# Medallion Pipeline from Raw Tables

Turn raw source tables into an analytics-ready medallion SDP. Inputs the user provides:

1. **Source schema** — `catalog.schema` containing raw Delta tables (read-only is fine).
2. **Target schema** — `catalog.schema` to publish bronze/silver/gold tables into.
3. **Gold description** — a short description of each gold table wanted (name + grain + measures).

## Workflow

1. **Preflight (required, do not skip)**
   * Verify every source table exists (`spark.catalog.tableExists("catalog.schema.table")`). If one is missing, stop and report instead of building a partial pipeline.
   * Verify the target catalog and schema exist. If missing, create them:
      ```python
      spark.sql("CREATE CATALOG IF NOT EXISTS <catalog>")
      spark.sql("CREATE SCHEMA IF NOT EXISTS <catalog>.<schema>")
      ```
   * Read each source table's columns with the `readTable` tool before writing any transformation code.

2. **Create the pipeline**
   * `createAsset` with `assetType: "pipeline"`, `catalog: <target catalog>`, descriptive name.
   * Immediately set the publish schema (it defaults to `default`): `editPipelineSettings` with `{"schema": "<target schema>"}`.
   * The pipeline gets an auto-generated `root_path` workspace folder; all dataset files go under `<root_path>/transformations/`.

3. **Author one file per dataset** (Python, batch materialized views — source tables are usually existing Delta/Delta-shared tables where streaming reads are unnecessary or unsupported):
    ```python
    from pyspark import pipelines as dp
    from pyspark.sql import functions as F

    @dp.materialized_view(
        name="bronze_<source_table>",
        comment="Bronze layer: raw as-is ingestion of <source table>.",
    )
    def bronze_<source_table>():
        return spark.read.table("<source_catalog>.<source_schema>.<source_table>")
    ```
    * **Bronze** (`transformations/bronze/`): one as-is copy per source table. No filtering, no renames.
    * **Silver** (`transformations/silver/`):
      * Dimensions: `select` descriptive columns from the bronze tables, `unionByName` across bronzes that carry the entity, `dropDuplicates(["<entity>_id"])` so joins never fan out.
      * Facts: `groupBy("date_key", "<entity>_id", ..., "retailer"-like source column).agg(F.sum(...), F.countDistinct("<row-id>"))` — aggregate to one row per date × entity × source.
    * **Gold** (`transformations/gold/`): one file per requested gold table. Derive measures from the silver facts, join silver dims for descriptive names (LEFT joins so facts are never lost), and add derived classification/status columns with nested `F.when(...)` CASE logic. Round rates to 2-4 decimals (`F.round`).
    * Create each file with `createAsset` (blank), then write content with `editAsset` — create all files in parallel.
    * Data quality (only if the user asks): `@dp.expect_or_drop("valid_<col>", "<col> >= 0")` decorator directly under `@dp.materialized_view`. Count dropped rows afterward three ways: before/after row counts, an independent aggregation predicting failures, and the pipeline event log's `failed_records`.

4. **Validate, then run**
   * Dry run first: `POST /api/2.0/pipelines/{pipeline_id}/updates` with body `{"validate_only": true}`. Fix any parse/validation errors before materializing.
   * Real run: same POST with `{"cause": "USER_ACTION"}`. Poll `GET .../updates/{update_id}` until `COMPLETED` — first runs on multi-year data can take 10+ minutes; sleep in the execution session between polls rather than busy-polling.

5. **Verify before reporting success**
   * Row counts of EVERY published table (`SELECT '<table>', COUNT(*)` UNION ALL): all must be > 0.
   * Sample each gold table (LIMIT 10) and check derived columns against their inputs row-by-row (e.g. status thresholds match the underlying measures).
   * Report per-layer row counts and any data quirks found (negative quantities, null-heavy dimensions, sparse sources).

## Rules learned from the CPG Retail Analytics build

* Set the pipeline `schema` before the first run — otherwise tables publish to `<catalog>.default`.
* Materialized views (batch reads) not streaming tables for existing Delta/Delta-shared sources.
* Never `.write`/`.save` — SDP manages all writes.
* One dataset per file; keep file and dataset names aligned (`gold_sales_velocity.py` defines `gold_sales_velocity`).
* Quantities like `on_hand` can be negative in retail data — decide explicitly whether negative means out-of-stock or invalid, and ask the user if it matters.
* If the pipeline root folder lives under a Git folder, offer to commit the transformations after a successful run (ask before committing).
* When downstream gold tables shrink after a data-quality change, report the before/after impact.
