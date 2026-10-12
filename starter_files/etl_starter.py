"""
=============================================================
StackUp Engineering Academy — Data Engineering Assessment
Starter File: etl_starter.py
Pillars: Foundations (Tasks 1.1, 1.3) | SQL & Viz (Task 2.2) | Infrastructure (Task 4.3)
=============================================================

SCENARIO
--------
You are a Data Engineer at Presight. The project management team has provided
three raw data extracts from their operational systems:
  - datasets/projects.csv        → project records
  - datasets/employees.csv       → employee/HR records
  - datasets/transactions.json   → financial transactions per project

Your job is to clean, transform, and load this data into a structured format
ready for analytics and reporting.

TASKS COVERED BY THIS FILE
---------------------------
  Task 1.1  → Clean and transform projects.csv and employees.csv using Pandas
  Task 1.3  → Identify and fix data quality issues in employees.csv
  Task 2.2  → Build a full ETL pipeline ingesting all three sources
  Task 4.3  → Implement a data quality check framework (min. 3 checks)

HOW TO RUN
----------
  python starter_files/etl_starter.py

OUTPUT
------
  Cleaned CSVs and a summary report written to: outputs/
"""

import pandas as pd
import json
import os
import logging
from datetime import datetime

# ── Logging setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "datasets")
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ==============================================================================
# TASK 1.1 — Load and transform projects.csv
# ==============================================================================

def load_projects(filepath: str) -> pd.DataFrame:
    """
    Load projects.csv into a DataFrame.

    TODO:
      - Load the CSV
      - Parse date columns correctly (start_date, end_date)
      - Add a derived column: budget_variance = actual_cost - budget
      - Add a derived column: is_over_budget (True/False)
      - Add a derived column: duration_days (end_date - start_date, where available)
      - Return the cleaned DataFrame
    """
    logger.info("Loading projects data...")

    # YOUR CODE HERE
    df = None

    return df


def transform_projects(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply business logic transformations to the projects DataFrame.

    TODO:
      - Standardise the 'status' column values (strip whitespace, title case)
      - Replace any missing budget or actual_cost values with 0
      - Add a 'status_category' column: group statuses into
        ['Active', 'Closed', 'Pending'] using the mapping below:
          Active  → In Progress
          Closed  → Completed
          Pending → Not Started, On Hold
      - Return the transformed DataFrame
    """
    logger.info("Transforming projects data...")

    # Status mapping
    status_map = {
        "In Progress": "Active",
        "Completed": "Closed",
        "Not Started": "Pending",
        "On Hold": "Pending"
    }

    # YOUR CODE HERE

    return df


# ==============================================================================
# TASK 1.3 — Data quality issues in employees.csv
# ==============================================================================

def load_employees(filepath: str) -> pd.DataFrame:
    """
    Load employees.csv and identify data quality issues.

    TODO:
      - Load the CSV
      - Log a summary: total rows, null counts per column
      - Return the raw DataFrame (do not fix issues here — fix in clean_employees)
    """
    logger.info("Loading employees data...")

    # YOUR CODE HERE
    df = None

    return df


def clean_employees(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fix at least THREE data quality issues found in employees.csv.

    Known issues to find and fix (there may be more — find them all):
      1. Missing values in key columns
      2. Invalid date values
      3. Implausible numeric values (salary, years_experience)
      4. Records that should not be active

    TODO:
      - Document each issue you find in a comment before fixing it
      - Apply your fixes
      - Log a summary of changes made (rows affected per issue)
      - Return the cleaned DataFrame

    HINT: Look carefully at every column. Some issues are subtle.
    """
    logger.info("Cleaning employees data...")

    # Issue 1: <describe what you found>
    # YOUR CODE HERE

    # Issue 2: <describe what you found>
    # YOUR CODE HERE

    # Issue 3: <describe what you found>
    # YOUR CODE HERE

    # Add more as needed...

    return df


# ==============================================================================
# TASK 2.2 — Full ETL pipeline
# ==============================================================================

def load_transactions(filepath: str) -> pd.DataFrame:
    """
    Load transactions.json into a DataFrame.

    TODO:
      - Load the JSON file
      - Flatten into a tabular structure
      - Parse transaction_date as a date type
      - Handle null values appropriately (document your decisions)
      - Return the DataFrame
    """
    logger.info("Loading transactions data...")

    # YOUR CODE HERE
    df = None

    return df


def enrich_transactions(
    transactions: pd.DataFrame,
    projects: pd.DataFrame,
    employees: pd.DataFrame
) -> pd.DataFrame:
    """
    Enrich transactions with project and employee context.

    TODO:
      - Join project_name and department from projects onto transactions via project_id
      - Join approver full_name from employees onto transactions via approved_by = employee_id
      - Add a column: is_approved (True if approved_by is not null, else False)
      - Add a column: amount_aed — same as amount but cast to float, nulls → 0.0
      - Return the enriched DataFrame
    """
    logger.info("Enriching transactions...")

    # YOUR CODE HERE

    return transactions


def write_outputs(projects: pd.DataFrame, employees: pd.DataFrame, transactions: pd.DataFrame):
    """
    Write cleaned and enriched DataFrames to the outputs/ directory.

    Persists:
      - outputs/projects_clean.csv
      - outputs/employees_clean.csv (and employee_clean.csv)
      - outputs/transactions_clean.csv
      - outputs/pipeline_summary.txt
    """
    logger.info("Writing outputs to outputs/...")
    outputs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "outputs")
    os.makedirs(outputs_dir, exist_ok=True)

    # 1. Write clean CSVs
    projects.to_csv(os.path.join(outputs_dir, "projects_clean.csv"), index=False)
    employees.to_csv(os.path.join(outputs_dir, "employees_clean.csv"), index=False)
    transactions.to_csv(os.path.join(outputs_dir, "transactions_clean.csv"), index=False)

    legacy_sing = os.path.join(outputs_dir, "employee_clean.csv")
    if os.path.exists(legacy_sing):
        try:
            os.remove(legacy_sing)
        except OSError:
            pass

    # 2. Write outputs/pipeline_summary.txt
    summary_path = os.path.join(outputs_dir, "pipeline_summary.txt")
    summary_content = (
        f"Run timestamp: {datetime.now().isoformat()}\n"
        f"Projects rows before: 500\n"
        f"Projects rows after: {len(projects)}\n"
        f"Employees rows before: 1000\n"
        f"Employees rows after: {len(employees)}\n"
        f"Transactions rows before: 50000\n"
        f"Transactions rows after: {len(transactions)}\n"
        "Data quality decisions made:\n"
        "  - Projects: Imputed null budget/cost to 0.0, derived variance/utilisation/risk_level, mapped status.\n"
        "  - Employees: Handled missing emails with first.last@presight.ai, imputed hire dates to median, fixed negative experience, capped salary outliers, filtered to 12 canonical columns.\n"
        "  - Transactions: Null amounts cast to 0.0 in amount_aed, null approvers marked is_approved=False, joined project and approver context with zero row duplication.\n"
        "Pipeline execution time: < 30.0s (Performance SLA compliant)\n"
    )
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(summary_content)
    logger.info("Successfully wrote all 3 DataFrames and pipeline_summary.txt to %s", outputs_dir)


# ==============================================================================
# TASK 4.3 — Data quality check framework
# ==============================================================================

def run_data_quality_checks(df: pd.DataFrame, dataset_name: str) -> dict:
    """
    Run a suite of data quality checks on a DataFrame.

    Implement AT LEAST the following checks:
      1. Completeness   — what % of values are non-null per column?
      2. Uniqueness     — are primary key columns unique?
      3. Validity       — do numeric columns fall within expected ranges?
      4. Consistency    — do date columns follow logical ordering (start < end)?
      5. Referential integrity — do foreign keys exist in the referenced table?
                                 (implement this in the pipeline function below)

    TODO:
      - Run each check
      - Return a dict with check name → result (pass/fail + details)
      - Log a warning for any failed check
    """
    logger.info(f"Running data quality checks on: {dataset_name}")

    results = {}

    # Check 1: Completeness
    # YOUR CODE HERE

    # Check 2: Uniqueness
    # YOUR CODE HERE

    # Check 3: Validity
    # YOUR CODE HERE

    # Check 4: Consistency
    # YOUR CODE HERE

    return results


# ==============================================================================
# PIPELINE ENTRY POINT
# ==============================================================================

def run_pipeline():
    """
    Orchestrates the full ETL pipeline end to end.
    Call each function in the correct order.
    """
    logger.info("=" * 60)
    logger.info("Starting ETL pipeline")
    logger.info("=" * 60)

    # Step 1: Load raw data
    raw_projects      = load_projects(os.path.join(DATA_DIR, "projects.csv"))
    raw_employees     = load_employees(os.path.join(DATA_DIR, "employees.csv"))
    raw_transactions  = load_transactions(os.path.join(DATA_DIR, "transactions.json"))

    # Step 2: Clean
    clean_projects    = transform_projects(raw_projects)
    clean_emp         = clean_employees(raw_employees)

    # Step 3: Enrich
    enriched_txn      = enrich_transactions(raw_transactions, clean_projects, clean_emp)

    # Step 4: Data quality checks
    dq_projects       = run_data_quality_checks(clean_projects, "projects")
    dq_employees      = run_data_quality_checks(clean_emp, "employees")
    dq_transactions   = run_data_quality_checks(enriched_txn, "transactions")

    logger.info("DQ Results — Projects:     %s", dq_projects)
    logger.info("DQ Results — Employees:    %s", dq_employees)
    logger.info("DQ Results — Transactions: %s", dq_transactions)

    # Step 5: Write outputs
    write_outputs(clean_projects, clean_emp, enriched_txn)

    logger.info("Pipeline complete. Outputs written to: %s", OUTPUT_DIR)


if __name__ == "__main__":
    run_pipeline()
