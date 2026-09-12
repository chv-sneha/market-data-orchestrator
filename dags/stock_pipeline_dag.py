"""
dags/stock_pipeline_dag.py
==========================
Airflow DAG that schedules and orchestrates the stock market data pipeline.

This module is responsible *only* for orchestration (when to run, how many
retries, scheduling, etc.). The actual business logic is imported from the
`scripts.stock_pipeline` module.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task

# Import our custom pipeline logic.
# The `scripts/` directory is mounted into the Airflow container and is
# implicitly added to the PYTHONPATH by Airflow.
import sys
import os
sys.path.append("/opt/airflow")

from scripts.stock_pipeline import run_pipeline

# ---------------------------------------------------------------------------
# Default Arguments
# ---------------------------------------------------------------------------
# These arguments apply to all tasks in the DAG.
default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    # Number of times to retry a failed task. This handles transient issues
    # like temporary network failures or brief database downtime.
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------
@dag(
    dag_id="daily_stock_market_pipeline",
    default_args=default_args,
    description="Fetches daily stock market data from Alpha Vantage and stores it in PostgreSQL",
    # Schedule: runs daily at midnight UTC.
    # To run hourly, change to "@hourly" or "0 * * * *".
    schedule_interval="@daily",
    # Start date in the past allows Airflow to establish a schedule.
    # We use a recent date to avoid a huge backlog.
    start_date=datetime(2023, 1, 1),
    # catchup=False means Airflow won't run missed executions from the start_date
    # up to today. We only care about current data.
    catchup=False,
    # Prevent concurrent runs of the DAG to avoid race conditions.
    max_active_runs=1,
    tags=["finance", "stocks", "etl"],
)
def stock_market_pipeline():
    """
    Defines the workflow for the stock market data pipeline.
    """

    @task(task_id="extract_transform_load")
    def run_etl() -> None:
        """
        Executes the full pipeline for all configured symbols.
        The `run_pipeline` function handles fetching, parsing, validating,
        and upserting the data.
        """
        # Call the core pipeline logic.
        # This function reads the STOCK_SYMBOLS env var automatically.
        run_pipeline()

    # Define the execution sequence.
    # (Since we combined the ETL into a single Python function for simplicity
    # and atomicity, there is only one task in this DAG.)
    run_etl()


# Instantiate the DAG
dag_instance = stock_market_pipeline()
