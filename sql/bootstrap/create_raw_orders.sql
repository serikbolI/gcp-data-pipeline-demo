-- Replace demo-analytics-project with your GCP project ID.
CREATE SCHEMA IF NOT EXISTS `demo-analytics-project.raw`
OPTIONS(location = "US");

CREATE TABLE IF NOT EXISTS `demo-analytics-project.raw.orders` (
  event_id STRING NOT NULL,
  order_id STRING NOT NULL,
  customer_id STRING NOT NULL,
  event_type STRING NOT NULL,
  event_ts TIMESTAMP NOT NULL,
  amount NUMERIC,
  currency STRING,
  ingested_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(event_ts)
CLUSTER BY order_id, customer_id
OPTIONS(
  description = "Append-only order events used by the GCP pipeline demo.",
  partition_expiration_days = 365
);

CREATE SCHEMA IF NOT EXISTS `demo-analytics-project.staging`
OPTIONS(location = "US");

CREATE SCHEMA IF NOT EXISTS `demo-analytics-project.analytics`
OPTIONS(location = "US");

CREATE SCHEMA IF NOT EXISTS `demo-analytics-project.dataform_assertions`
OPTIONS(location = "US");

CREATE SCHEMA IF NOT EXISTS `demo-analytics-project.ops`
OPTIONS(location = "US");
