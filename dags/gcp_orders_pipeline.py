"""Daily BigQuery + Dataform pipeline orchestrated by Cloud Composer / Airflow.

The DAG intentionally keeps configuration in environment variables and contains no
credentials. In production, use Composer's workload identity / service account
rather than JSON service-account keys.
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta
from typing import Any

import pendulum
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)

LOGGER = logging.getLogger(__name__)

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "demo-analytics-project")
BQ_LOCATION = os.getenv("BQ_LOCATION", "US")
DATAFORM_REGION = os.getenv("DATAFORM_REGION", "us-central1")
DATAFORM_REPOSITORY_ID = os.getenv(
    "DATAFORM_REPOSITORY_ID", "gcp-data-pipeline-demo"
)
DATAFORM_GIT_COMMITISH = os.getenv("DATAFORM_GIT_COMMITISH", "main")
RAW_DATASET = os.getenv("RAW_DATASET", "raw")
MART_DATASET = os.getenv("MART_DATASET", "analytics")
AUDIT_DATASET = os.getenv("AUDIT_DATASET", "ops")


def log_failure(context: dict[str, Any]) -> None:
    """Log enough context for an alerting integration without embedding secrets."""
    task_instance = context.get("task_instance")
    exception = context.get("exception")
    LOGGER.error(
        "Pipeline task failed: dag_id=%s task_id=%s run_id=%s exception=%r",
        getattr(task_instance, "dag_id", None),
        getattr(task_instance, "task_id", None),
        context.get("run_id"),
        exception,
    )


DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": log_failure,
}

with DAG(
    dag_id="gcp_orders_dataform_pipeline",
    description="Validate raw order events, run Dataform, and publish quality/audit results.",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="15 3 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    render_template_as_native_obj=True,
    tags=["gcp", "bigquery", "dataform", "demo"],
) as dag:
    wait_for_raw_orders = BigQueryCheckOperator(
        task_id="wait_for_raw_orders",
        sql=f"""
        SELECT COUNT(*) > 0
        FROM `{PROJECT_ID}.{RAW_DATASET}.orders`
        WHERE ingested_at >= TIMESTAMP('{{{{ data_interval_start }}}}')
          AND ingested_at < TIMESTAMP('{{{{ data_interval_end }}}}')
        """,
        use_legacy_sql=False,
        location=BQ_LOCATION,
        labels={"pipeline": "orders_demo", "stage": "raw_check"},
        deferrable=True,
        execution_timeout=timedelta(minutes=20),
    )

    compile_dataform = DataformCreateCompilationResultOperator(
        task_id="compile_dataform",
        project_id=PROJECT_ID,
        region=DATAFORM_REGION,
        repository_id=DATAFORM_REPOSITORY_ID,
        compilation_result={"git_commitish": DATAFORM_GIT_COMMITISH},
        execution_timeout=timedelta(minutes=15),
    )

    run_dataform = DataformCreateWorkflowInvocationOperator(
        task_id="run_dataform",
        project_id=PROJECT_ID,
        region=DATAFORM_REGION,
        repository_id=DATAFORM_REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": (
                "{{ ti.xcom_pull(task_ids='compile_dataform')['name'] }}"
            ),
            "invocation_config": {
                "included_tags": ["daily"],
                "transitive_dependencies_included": True,
                "transitive_dependents_included": False,
                "fully_refresh_incremental_tables_enabled": False,
            },
        },
        execution_timeout=timedelta(hours=1),
    )

    check_fact_orders = BigQueryCheckOperator(
        task_id="check_fact_orders",
        sql=f"""
        SELECT
          COUNT(*) > 0 AS has_rows,
          COUNT(*) = COUNT(DISTINCT order_id) AS unique_order_id,
          COUNTIF(order_id IS NULL OR customer_id IS NULL) = 0 AS keys_are_not_null,
          COUNTIF(gross_amount < 0) = 0 AS revenue_is_valid
        FROM `{PROJECT_ID}.{MART_DATASET}.fact_orders`
        WHERE DATE(updated_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
        """,
        use_legacy_sql=False,
        location=BQ_LOCATION,
        labels={"pipeline": "orders_demo", "stage": "quality_gate"},
        deferrable=True,
        execution_timeout=timedelta(minutes=20),
    )

    write_pipeline_audit = BigQueryInsertJobOperator(
        task_id="write_pipeline_audit",
        configuration={
            "query": {
                "query": f"""
                CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{AUDIT_DATASET}.pipeline_runs` (
                  dag_id STRING,
                  run_id STRING,
                  logical_date TIMESTAMP,
                  status STRING,
                  recorded_at TIMESTAMP
                )
                PARTITION BY DATE(recorded_at)
                CLUSTER BY dag_id, status;

                INSERT INTO `{PROJECT_ID}.{AUDIT_DATASET}.pipeline_runs`
                  (dag_id, run_id, logical_date, status, recorded_at)
                VALUES (
                  '{{{{ dag.dag_id }}}}',
                  '{{{{ run_id }}}}',
                  TIMESTAMP('{{{{ logical_date }}}}'),
                  'SUCCESS',
                  CURRENT_TIMESTAMP()
                );
                """,
                "useLegacySql": False,
            },
            "labels": {"pipeline": "orders_demo", "stage": "audit"},
        },
        location=BQ_LOCATION,
        job_id="orders_demo_audit_{{ ds_nodash }}_{{ ts_nodash }}",
        force_rerun=False,
        reattach_states={"PENDING", "RUNNING"},
        deferrable=True,
        execution_timeout=timedelta(minutes=20),
    )

    (
        wait_for_raw_orders
        >> compile_dataform
        >> run_dataform
        >> check_fact_orders
        >> write_pipeline_audit
    )
