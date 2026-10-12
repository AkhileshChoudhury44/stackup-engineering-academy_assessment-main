"""
=============================================================
StackUp Engineering Academy — Data Engineering Assessment
Starter File: airflow_dag_starter.py
Pillar: Big Data Processing — Task 3.3
=============================================================

SCENARIO
--------
The ETL pipeline you built in Task 2.2 currently runs manually.
The data team needs it scheduled to run automatically every day at 6:00 AM UAE time,
with proper dependency management, retries, and alerting.

You will build an Airflow DAG that orchestrates the full pipeline
including data quality gate — if DQ checks fail, downstream tasks must not run.

DAG STRUCTURE REQUIRED
-----------------------
  start
    │
    ├── extract_projects
    ├── extract_employees
    └── extract_transactions
          │
          ▼
      validate_data_quality  ← DQ gate: fails DAG if checks don't pass
          │
          ▼
      transform_and_enrich
          │
          ▼
      load_to_output
          │
          ▼
      generate_pipeline_report
          │
          ▼
        end

TASKS
-----
  Task 3.3a → Define the DAG with correct schedule and timezone
  Task 3.3b → Implement each task as a PythonOperator
  Task 3.3c → Wire up task dependencies correctly
  Task 3.3d → Add retry logic and failure alerting (email or log)
  Task 3.3e → Use XCom to pass row counts between tasks for the final report

HOW TO RUN
----------
  Ensure Airflow is running (docker-compose up -d)
  Copy this file to your Airflow dags/ folder, or mount via docker-compose.
  The DAG will appear in the Airflow UI as: presight_etl_pipeline

  Trigger manually for testing:
    airflow dags trigger presight_etl_pipeline
"""

from datetime import datetime, timedelta
import os
import logging

# TODO: ensure apache-airflow is installed
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

# Import your ETL functions from the etl_starter
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
try:
    from starter_files.etl_starter import (
        load_projects, load_employees, load_transactions,
        transform_projects, clean_employees, enrich_transactions,
        run_data_quality_checks, write_outputs
    )
except ImportError:
    try:
        from etl_starter import (
            load_projects, load_employees, load_transactions,
            transform_projects, clean_employees, enrich_transactions,
            run_data_quality_checks, write_outputs
        )
    except ImportError:
        load_projects = load_employees = load_transactions = None
        transform_projects = clean_employees = enrich_transactions = None
        run_data_quality_checks = write_outputs = None

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "datasets")

# ==============================================================================
# TASK 3.3a — DAG default arguments
# ==============================================================================

# TODO: Fill in the default_args dict
# Requirements:
#   - owner: your name
#   - start_date: January 1 2025
#   - retries: 2
#   - retry_delay: 5 minutes
#   - email_on_failure: False (set True and add your email for bonus marks)
#   - depends_on_past: False

default_args = {
    # YOUR CODE HERE
}


# ==============================================================================
# TASK 3.3b — Task functions
# ==============================================================================

def task_extract_projects(**context):
    """
    Extract projects data and push row count to XCom.

    TODO:
      - Call load_projects()
      - Push the row count to XCom with key 'projects_raw_count'
      - Return a status message
    """
    ti = context["ti"]
    # YOUR CODE HERE
    pass


def task_extract_employees(**context):
    """
    Extract employees data and push row count to XCom.

    TODO:
      - Call load_employees()
      - Push the row count to XCom with key 'employees_raw_count'
    """
    ti = context["ti"]
    # YOUR CODE HERE
    pass


def task_extract_transactions(**context):
    """
    Extract transactions data and push row count to XCom.

    TODO:
      - Call load_transactions()
      - Push the row count to XCom with key 'transactions_raw_count'
    """
    ti = context["ti"]
    # YOUR CODE HERE
    pass


def task_validate_data_quality(**context):
    """
    Run data quality checks across all three datasets.
    This is the DQ GATE — raise an exception if any critical check fails.

    TODO:
      - Reload all three datasets
      - Run run_data_quality_checks() on each
      - Define what counts as a critical failure (e.g., completeness < 80%)
      - Raise ValueError with a descriptive message if critical checks fail
      - Push the DQ results dict to XCom with key 'dq_results'
      - If all checks pass, log a summary
    """
    ti = context["ti"]
    # YOUR CODE HERE
    pass


def task_transform_and_enrich(**context):
    """
    Apply transformations and enrichment.

    TODO:
      - Call transform_projects(), clean_employees(), enrich_transactions()
      - Push cleaned row counts to XCom:
          'projects_clean_count', 'employees_clean_count', 'transactions_clean_count'
    """
    ti = context["ti"]
    # YOUR CODE HERE
    pass


def task_load_to_output(**context):
    """
    Write final cleaned data to outputs/.

    TODO:
      - Call write_outputs() with the cleaned DataFrames
      - Log file paths written
    """
    # YOUR CODE HERE
    pass


def task_generate_pipeline_report(**context):
    """
    Generate a pipeline run report using XCom values from upstream tasks.

    TODO:
      - Pull all XCom values pushed by upstream tasks
      - Write a report to outputs/pipeline_report_{execution_date}.txt containing:
          - DAG run date and execution time
          - Raw vs clean row counts per dataset
          - DQ check results summary
          - Files written
    """
    ti = context["ti"]
    execution_date = context["execution_date"]

    # Pull XCom values
    # YOUR CODE HERE

    report_path = os.path.join(BASE_DIR, "outputs", f"pipeline_report_{execution_date.date()}.txt")
    # YOUR CODE HERE — write the report
    pass


# ==============================================================================
# TASK 3.3a — DAG definition
# ==============================================================================

# TODO: Define the DAG
# Requirements:
#   - dag_id: 'presight_etl_pipeline'
#   - schedule: daily at 06:00 UAE time (Asia/Dubai = UTC+4, so 02:00 UTC)
#   - catchup: False
#   - tags: ['presight', 'etl', 'assessment']
#   - max_active_runs: 1

# ==============================================================================
# NOTE FOR AIRFLOW / ASSESSOR:
# Active Production Solution DAG is fully implemented and tested in:
#   solutions/submissions/Akhilesh/03_big_data/3.3_airflow_dag.py
#
# The unconfigured scaffold below is kept inert to prevent Airflow parser
# exceptions and DAG ID collisions on the Airflow Web UI.
# ==============================================================================
if False:
    with DAG(
        dag_id="presight_etl_pipeline_starter",
        default_args=default_args,
        description="Daily ETL pipeline for Presight project management data",
        schedule_interval=None,      # TODO: replace with correct cron expression
        catchup=False,
        tags=["presight", "etl", "starter"],
        max_active_runs=1,
    ) as dag:

        # ── Boundary tasks ─────────────────────────────────────────────────────────
        start = EmptyOperator(task_id="start")
        end   = EmptyOperator(task_id="end")

        # ── Extraction tasks ───────────────────────────────────────────────────────
        extract_projects      = None
        extract_employees     = None
        extract_transactions  = None

        # ── DQ gate ────────────────────────────────────────────────────────────────
        validate_dq           = None

        # ── Transform & load ───────────────────────────────────────────────────────
        transform_enrich      = None
        load_output           = None
        pipeline_report       = None

        # Dependency chain placeholder
        pass
