# Suggested live walkthrough

## 1. Start with the problem

The source sends append-only order events. Events can be duplicated or arrive late. The analytical requirement is one reliable current record per order without rebuilding the full history every day.

## 2. Explain the architecture

- BigQuery stores raw append-only events.
- Airflow owns cross-service orchestration and the final operational quality gate.
- Dataform owns SQL transformations, model dependencies, documentation, and transformation-level assertions.
- BigQuery audit data provides a simple operational history.

## 3. Walk through the staging model

Key points in `stg_orders.sqlx`:

- `event_id` is the merge key;
- `ROW_NUMBER()` resolves duplicated source events deterministically;
- filtering by `ingested_at` catches late business events;
- partitioning uses `event_ts` for common analytical access;
- `updatePartitionFilter` prevents the merge from scanning the entire target;
- built-in assertions make assumptions executable.

Trade-off: a bounded rolling window is cheaper than full refresh, but events arriving later than the window require an explicit backfill.

## 4. Walk through the mart

`fact_orders.sqlx` first identifies affected order IDs, then recalculates those orders from their complete deduplicated event history. This is safer than aggregating only the latest daily slice.

The current-state table is clustered by `order_id` but not partitioned by `updated_at`. An old order can receive a new event today; pruning the merge to recent target partitions could miss the existing row and insert a duplicate. This is a deliberate correctness-over-cost decision.

Trade-off: recalculating full history for affected keys and matching against the current-state table reads more data than an aggressively partition-pruned design, but it is easier to reason about and recover.

## 5. Walk through Airflow

`gcp_orders_pipeline.py` shows:

- one active run to avoid overlapping daily writes;
- retries and timeouts;
- a raw readiness check;
- Dataform compilation separated from invocation;
- execution of selected tags and transitive dependencies;
- an independent BigQuery quality gate;
- an idempotent audit job;
- no embedded credentials.

## 6. Discuss cost

- Daily runs scan only recent source arrivals and target partitions.
- Partitioning and clustering align with access and merge patterns.
- BigQuery job labels allow cost analysis by pipeline and stage.
- Large backfills should be isolated, estimated, and scheduled separately.

## 7. Discuss what would change in production

- Replace the raw row-count check with a source control-table freshness SLA.
- Send failures to the company alerting platform.
- Use Terraform for datasets, IAM, Composer variables, and Dataform resources.
- Add environment-specific compilation overrides.
- Add integration tests in a dedicated GCP project.
- Define retention and table expiration according to business requirements.
