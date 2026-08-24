# Instructions for developing a model in Rill

## Introduction

Models are resources that specify ETL or transformation logic, outputting a tabular dataset to one of the project's connectors. They are typically found near the root of the project's DAG, referencing only connectors and other models.

By default, models output data as a table with the same name as the model in the project's default OLAP connector. The core of a model is usually a `SELECT` SQL statement, which Rill executes as `CREATE TABLE <name> AS <SELECT statement>`. The SQL should be a plain SELECT query without a trailing semicolon.

Models in Rill are similar to models in dbt, but support additional advanced features:
- **Different input and output connectors:** Run a query in one database (e.g., BigQuery) and output results to another (e.g., DuckDB or ClickHouse).
- **Stateful incremental ingestion:** Track state and load only new or changed data.
- **Partition support:** Define explicit partitions (e.g., Hive-partitioned files in S3) for scalable, idempotent incremental runs.
- **Scheduled refresh:** Use cron expressions to automatically refresh data on a schedule.

### Model categories

When reasoning about a model, consider these attributes:

- **Source model**: References external data, typically reading from a SQL database or object store connector and writing to an OLAP connector.
- **Derived model**: References other models, usually performing joins or formatting columns to prepare denormalized tables for metrics views and dashboards.
- **Incremental model**: Contains logic for incrementally loading data, processing only new or changed records.
- **Partitioned model**: Loads data in well-defined increments (e.g., daily partitions), enabling scalability and idempotent incremental runs.
- **Materialized model**: Outputs a physical table rather than a SQL view.

### Performance considerations

Models are usually expensive resources that can take a long time to run. Create or edit them with caution.

**Exception:** Non-materialized models with the same input and output connector are cheap because they are created as SQL views rather than physical tables.

**Development tip:** Use a "dev partition" to limit data processed during development. This speeds up iteration and avoids unnecessary costs. See the partitions section below for details.

## Materialization

The `materialize:` property controls whether a model creates a physical table or a SQL view:

- `materialize: true`: Creates a physical table. Use this for source models, expensive transformations, or when downstream queries need fast access.
- `materialize: false`: Creates a SQL view. The query re-executes on every access. Only suitable for lightweight transformations where input and output connectors are the same that never reference external data.

If `materialize` is omitted, it defaults to `true` for all cross-connector models and `false` for single-connector models (i.e. where the input and output connector is the same).

In model files with a `.sql` extension, you can materialize by putting this on the first line of the file:
```sql
-- @materialize: true
```

**Best practices:**
- Always materialize models that reference external data sources.
- Always materialize models that perform expensive joins or aggregations.
- Use views only for simple transformations on top of already-materialized tables.

## Referencing other models

Use `{{ ref "model_name" }}` to reference parent models in SQL statements that use templating:

```yaml
sql: SELECT * FROM {{ ref "events_raw" }} WHERE country = 'US'
```

**Note:** If your SQL statement contains no other templating, the `ref` function is optional for DuckDB SQL snippets; Rill can in that case invoke DuckDB's SQL parser to automatically detect model references. This does not apply for non-DuckDB SQL models.

## Refresh schedules

By default, models refresh when a parent model in the DAG is refreshed. For source models that don't have a parent model, you can configure a cron refresh schedule:
```yaml
refresh:
  cron: 0 * * * *
```

By default, cron refreshes are disabled in local development. If you need to test them locally, add `run_in_dev: true` under `refresh:`.

### DuckDB

- **Model references:** When the SQL contains no other templating, `{{ ref "model" }}` is optional; Rill uses DuckDB's SQL parser to detect references.
- **Connector secrets:** By default, all compatible connectors are automatically mounted as DuckDB secrets. Use `create_secrets_from_connectors:` to explicitly control which connectors are available.
- **Pre/post execution:** Use `pre_exec:` and `post_exec:` for setup and teardown queries (e.g., attaching external databases). Some legacy projects configure DuckDB secrets here, but with the automatic secret creation referenced above, it is usually better to create separate connector files instead.
- **Cloud storage paths:** DuckDB can read directly from S3 (`s3://`) and GCS (`gs://`) paths in `read_parquet()`, `read_csv()`, and `read_json()` functions.
- **CSV options:** When reading CSV files, useful options include `auto_detect=true`, `header=true`, `ignore_errors=true`, `union_by_name=true`, and `all_varchar=true` for handling inconsistent schemas.
- **JSON files:** Use `read_json()` with `auto_detect=true` and `format='auto'` for flexible JSON ingestion, including gzipped files.

