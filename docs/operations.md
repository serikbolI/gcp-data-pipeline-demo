# Operations and recovery

## Normal daily run

1. Airflow verifies that raw events arrived in the current data interval.
2. Airflow compiles the configured Dataform Git commit.
3. Dataform runs actions tagged `daily`, including their dependencies.
4. Dataform assertions execute with the models.
5. Airflow runs an independent final quality gate.
6. Airflow writes a success record to the audit table.

## Common failure modes

### No raw data

The first task fails after its configured retries. Before retrying manually, confirm whether the source is genuinely delayed or whether the expected volume is zero.

For sources where zero rows can be valid, replace the hard check with a freshness check against an ingestion-control table.

### Dataform compilation failure

Typical causes:

- invalid SQLX syntax;
- missing source declaration;
- invalid dependency reference;
- incompatible schema change;
- incorrect Git branch or commit.

The run should not proceed to workflow invocation until compilation succeeds.

### Assertion failure

Do not bypass an assertion simply to complete the DAG. First identify the failing rows in the Dataform assertion dataset and decide whether the issue is bad source data, transformation logic, or an expected business-rule change.

### Partial BigQuery/Dataform failure

The models are designed to be rerunnable:

- staging merges by `event_id`;
- the mart merges by `order_id`;
- affected entities are recalculated from deduplicated staging history;
- the audit job has a deterministic Airflow job ID for a given run attempt.

## Backfill

Use a separate backfill change rather than permanently increasing the daily scan window.

Recommended procedure:

1. identify the affected event-time and ingestion-time range;
2. estimate bytes scanned before execution;
3. snapshot the target table when the change is high risk;
4. temporarily parameterize the affected-order selection for the required range;
5. execute in a non-production project when practical;
6. compare row counts, distinct keys, revenue totals, and status distribution;
7. promote the same tested commit to production;
8. restore the normal rolling window.

## Monitoring signals

Recommended production metrics:

- raw data freshness and row count;
- Dataform workflow duration and failed actions;
- duplicate event count;
- late-arriving event count;
- affected-order count per run;
- duplicate corrections older than the configured staging target window;
- BigQuery bytes processed and slot time;
- mart row count and revenue delta;
- audit-table run status.
