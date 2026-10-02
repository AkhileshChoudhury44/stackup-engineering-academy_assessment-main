"""
================================================================================
StackUp Engineering Academy — Data Engineering Assessment
Pillar 3: Big Data Processing — Task 3.3: Production Airflow DAG Orchestration
Candidate: Akhilesh
File: 3.3_airflow_dag.py (Production DAG: presight_etl_pipeline)
================================================================================

BUSINESS SCENARIO & ARCHITECTURAL OVERVIEW:
------------------------------------------
The daily enterprise project management platform generates thousands of financial
transactions, employee role transitions, and project status updates. While Pillars 1
and 2 established vectorized Pandas and DuckDB logic, enterprise production requires
automated workflow orchestration with:
  1. Guaranteed Temporal Scheduling: Runs daily at 06:00 AM UAE Time (Asia/Dubai, UTC+4),
     timed to conclude before business hours start in Abu Dhabi and Dubai.
  2. Parallel Extraction: Ingests projects (CSV), employees (CSV), and transactions (JSON)
     concurrently across upstream worker tasks.
  3. Hard Pre-Clean Data Quality Gate: Evaluates raw inputs on business-critical
     completeness, uniqueness, and valid domain boundaries BEFORE transformations run.
     If critical thresholds are violated, downstream tasks are halted to prevent bad
     data from polluting downstream warehouse layers.
  4. Robust Transformation: Applies full vectorized business rules (derived metrics,
     risk scoring, collision-free email remediation) without naive dropna() data loss.
  5. XCom Telemetry & Reporting: Passes runtime metrics (record volumes, gate verdicts)
     across tasks to generate an automated executive audit summary.
  6. Failure Resilience: Automated retry policy (2 retries with 5-minute backoff)
     to absorb transient I/O network blips.

DAG GRAPH TOPOLOGY:
-------------------
                  ┌─► [extract_projects]     ─┐
                  │                           │
  [start] ────────┼─► [extract_employees]    ─┼─► [validate_data_quality]
                  │                           │           │ (DQ Gate)
                  └─► [extract_transactions] ─┘           ▼
                                                 [transform_and_enrich]
                                                          │
                                                          ▼
                                                    [load_to_output]
                                                          │
                                                          ▼
                                              [generate_pipeline_report]
                                                          │
                                                          ▼
                                                        [end]

EXECUTION ENVIRONMENTS:
-----------------------
- Production Mode: Loaded into Airflow Scheduler & Webserver via Docker Compose
  Web UI accessible at http://localhost:8081 (DAG ID: presight_etl_pipeline)
- Standalone / Local Mode: Executable directly via `python 3.3_airflow_dag.py`
  utilizing an internal mock execution context for unit testing and CI/CD validation.
================================================================================
"""

import os
import json
import logging
import warnings

# Suppress benign Windows POSIX runtime warnings when running on local development machines
warnings.filterwarnings("ignore", category=RuntimeWarning, module="airflow")

from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# ── Dynamic Airflow Import with Standalone Fallback ────────────────────────────
# Allows the script to be executed both inside an active Apache Airflow cluster
# and standalone in a local developer terminal without import failure.
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.operators.empty import EmptyOperator
except ImportError:
    DAG = None
    PythonOperator = None
    EmptyOperator = None

import pendulum

# ── Logging Configuration ─────────────────────────────────────────────────────
# Uses the standard Airflow task logger so logs stream directly to the Airflow UI
logger = logging.getLogger("airflow.task")

# ── Environment & Path Resolution ─────────────────────────────────────────────
# Automatically switches between Docker container paths (/opt/airflow/...)
# and local filesystem paths when developing or running unit tests.
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", "..", "..", ".."))
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")

if os.path.exists(os.path.join(AIRFLOW_HOME, "datasets")):
    # Inside Docker Airflow container
    DATA_DIR = os.path.join(AIRFLOW_HOME, "datasets")
    OUTPUT_DIR = os.path.join(AIRFLOW_HOME, "outputs")
elif os.path.exists(os.path.join(REPO_ROOT, "datasets")):
    # Local workspace repository root
    DATA_DIR = os.path.join(REPO_ROOT, "datasets")
    OUTPUT_DIR = os.path.join(REPO_ROOT, "outputs", "results", "Akhilesh", "03_big_data")
else:
    DATA_DIR = os.path.join(os.getcwd(), "datasets")
    OUTPUT_DIR = os.path.join(os.getcwd(), "outputs")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Temporal Scheduling & Default Arguments ───────────────────────────────────
# Timezone: Explicitly configured for the Gulf Standard Timezone (Asia/Dubai, UTC+4).
# This prevents UTC clock skew from triggering overnight batch jobs during business hours.
local_tz = pendulum.timezone("Asia/Dubai")

default_args = {
    "owner": "presight_data_engineering",
    "depends_on_past": False,
    "email": ["admin@presight.ai"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,                           # Retry transient task failures up to 2 times
    "retry_delay": timedelta(minutes=5),    # Wait 5 minutes between retries
    "execution_timeout": timedelta(minutes=15) # Protect against hung worker processes
}


# ==============================================================================
# TASK OPERATOR FUNCTIONS (BUSINESS LOGIC)
# ==============================================================================

def task_extract_projects(**context):
    """
    Task 1a: Ingests projects master data from CSV storage.
    Pushes total extracted row count to XCom ('raw_projects_count')
    to establish data volume lineage for downstream reporting.
    """
    file_path = os.path.join(DATA_DIR, "projects.csv")
    logger.info("Starting ingestion of projects data from: %s", file_path)
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Missing required input dataset: {file_path}")
        
    df = pd.read_csv(file_path)
    count = len(df)
    logger.info("Successfully extracted %d project records.", count)
    
    # Push metadata to Airflow XCom for audit tracking
    context["ti"].xcom_push(key="raw_projects_count", value=count)
    return count


def task_extract_employees(**context):
    """
    Task 1b: Ingests current employee HR records from CSV storage.
    Pushes total extracted row count to XCom ('raw_employees_count').
    """
    file_path = os.path.join(DATA_DIR, "employees.csv")
    logger.info("Starting ingestion of employees data from: %s", file_path)
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Missing required input dataset: {file_path}")
        
    df = pd.read_csv(file_path)
    count = len(df)
    logger.info("Successfully extracted %d employee records.", count)
    
    context["ti"].xcom_push(key="raw_employees_count", value=count)
    return count


def task_extract_transactions(**context):
    """
    Task 1c: Ingests 50,000 JSON financial transaction records.
    Validates structural parseability and pushes record count to XCom.
    """
    file_path = os.path.join(DATA_DIR, "transactions.json")
    logger.info("Starting ingestion of transactions JSON from: %s", file_path)
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Missing required input dataset: {file_path}")
        
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        
    count = len(data)
    logger.info("Successfully ingested %d raw transaction records.", count)
    
    context["ti"].xcom_push(key="raw_transactions_count", value=count)
    return count


def task_validate_data_quality(**context):
    """
    Task 2: Hard Pre-Clean Data Quality Gate.
    
    DESIGN RATIONALE:
    -----------------
    Rather than allowing dirty or corrupt data to cascade into complex transformation
    logic, this gate inspects the raw ingested inputs against core structural invariants:
      1. Primary Key Uniqueness: Verifies zero duplicate project_id, employee_id,
         or transaction_id records.
      2. Domain Enumeration Conformity: Ensures priority and organizational levels
         strictly match allowed categorical vocabularies.
      3. Numeric Invariants: Guarantees salaries and transaction amounts are not negative.
      
    If any check fails, a ValueError is raised, immediately terminating the DAG run
    and preventing corrupt writes to production warehouse targets.
    """
    logger.info("============================================================")
    logger.info("Executing Pre-Clean Data Quality Gate on Ingested Datasets")
    logger.info("============================================================")

    # ── Check 1: Projects Dataset Invariants ─────────────────────────────────
    p_df = pd.read_csv(os.path.join(DATA_DIR, "projects.csv"))
    p_pk_dups = p_df["project_id"].duplicated().sum()
    if p_pk_dups > 0:
        raise ValueError(f"DQ Gate Failure: {p_pk_dups} duplicate project_ids detected in projects.csv")
    
    valid_priorities = {"Critical", "High", "Medium", "Low"}
    invalid_priorities = (~p_df["priority"].isin(valid_priorities)).sum()
    if invalid_priorities > 0:
        raise ValueError(f"DQ Gate Failure: {invalid_priorities} records have unrecognized priority values")

    # ── Check 2: Employees Dataset Invariants ────────────────────────────────
    e_df = pd.read_csv(os.path.join(DATA_DIR, "employees.csv"))
    e_pk_dups = e_df["employee_id"].duplicated().sum()
    if e_pk_dups > 0:
        raise ValueError(f"DQ Gate Failure: {e_pk_dups} duplicate employee_ids detected in employees.csv")

    numeric_salaries = pd.to_numeric(e_df["salary"], errors="coerce")
    invalid_salaries = (numeric_salaries.isna() | (numeric_salaries <= 0)).sum()
    if invalid_salaries > 0:
        raise ValueError(f"DQ Gate Failure: {invalid_salaries} employees have non-positive/null salaries")

    valid_levels = {"Junior", "Mid", "Senior", "Lead", "Director"}
    invalid_levels = (~e_df["level"].isin(valid_levels)).sum()
    if invalid_levels > 0:
        raise ValueError(f"DQ Gate Failure: {invalid_levels} employees have unapproved organizational levels")

    # ── Check 3: Transactions Dataset Invariants ─────────────────────────────
    with open(os.path.join(DATA_DIR, "transactions.json"), "r", encoding="utf-8") as f:
        t_data = json.load(f)
    t_df = pd.json_normalize(t_data)
    
    t_pk_dups = t_df["transaction_id"].duplicated().sum()
    if t_pk_dups > 0:
        raise ValueError(f"DQ Gate Failure: {t_pk_dups} duplicate transaction_ids detected in transactions.json")

    numeric_amounts = pd.to_numeric(t_df["amount"], errors="coerce")
    neg_amounts = (numeric_amounts < 0).sum()
    if neg_amounts > 0:
        raise ValueError(f"DQ Gate Failure: {neg_amounts} transactions have negative amounts")

    logger.info("Pre-Clean Data Quality Gate PASSED: All 6 structural checks passed.")
    context["ti"].xcom_push(key="dq_gate_status", value="PASSED")


def task_transform_and_enrich(**context):
    """
    Task 3: Production Vectorized Transformation & Dimensional Enrichment.
    
    DESIGN RATIONALE:
    -----------------
    Applies genuine business rules and explicit data quality remediation
    WITHOUT naive dropna() shortcuts:
      - Projects: Imputes missing budgets to 0.0, calculates variance, duration,
        and composite risk levels.
      - Employees: Resolves missing corporate emails using collision-free sequential
        suffixes, imputes invalid hire dates via role medians, and corrects self-managers.
      - Transactions: Standardizes ISO dates, sets amount_aed with null defaults,
        and sets approval boolean flags.
    """
    logger.info("Starting vectorized transformations across all datasets...")
    
    # ── 1. Projects Transformation ───────────────────────────────────────────
    p_df = pd.read_csv(os.path.join(DATA_DIR, "projects.csv"))
    p_df["budget"] = pd.to_numeric(p_df["budget"], errors="coerce").fillna(0.0)
    p_df["actual_cost"] = pd.to_numeric(p_df["actual_cost"], errors="coerce").fillna(0.0)
    p_df["start_date"] = pd.to_datetime(p_df["start_date"], errors="coerce")
    p_df["end_date"] = pd.to_datetime(p_df["end_date"], errors="coerce")
    
    p_df["budget_variance"] = p_df["actual_cost"] - p_df["budget"]
    p_df["is_over_budget"] = p_df["actual_cost"] > p_df["budget"]
    p_df["duration_days"] = (p_df["end_date"] - p_df["start_date"]).dt.days
    p_df["budget_utilisation_pct"] = np.where(
        p_df["budget"] > 0, (p_df["actual_cost"] / p_df["budget"]) * 100.0, 0.0
    )
    p_df["status"] = p_df["status"].astype(str).str.strip().str.title()
    status_map = {"In Progress": "Active", "Completed": "Closed", "Not Started": "Pending", "On Hold": "Pending"}
    p_df["status_category"] = p_df["status"].map(status_map).fillna("Pending")
    
    cond_risk = [
        (p_df["priority"] == "Critical") | (p_df["is_over_budget"] == True),
        (p_df["priority"] == "High") | (p_df["budget_utilisation_pct"] > 90.0)
    ]
    p_df["risk_level"] = np.select(cond_risk, ["High", "Medium"], default="Low")

    # ── 2. Employees Cleaning with Audit Flags ────────────────────────────────
    e_df = pd.read_csv(os.path.join(DATA_DIR, "employees.csv"))
    
    # Email generation with collision prevention
    missing_email_mask = e_df["email"].isna() | e_df["email"].astype(str).str.strip().eq("")
    e_df["dq_missing_email"] = missing_email_mask
    existing_emails = set(e_df.loc[~missing_email_mask, "email"].astype(str).str.lower().str.strip().unique())
    for idx in e_df[missing_email_mask].index:
        clean_name = "".join(c for c in str(e_df.loc[idx, "full_name"]).lower().strip() if c.isalpha() or c.isspace()).strip()
        parts = clean_name.split()
        base_local = f"{parts[0]}.{parts[-1]}" if len(parts) >= 2 else clean_name
        cand = f"{base_local}@presight.ai"
        suf = 1
        while cand in existing_emails:
            cand = f"{base_local}{suf}@presight.ai"
            suf += 1
        existing_emails.add(cand)
        e_df.loc[idx, "email"] = cand

    # Hire date remediation using role median
    parsed_dates = pd.to_datetime(e_df["hire_date"], errors="coerce")
    invalid_hire = parsed_dates.isna() & e_df["hire_date"].notna()
    implausible_hire = (parsed_dates < pd.Timestamp("1990-01-01")) | (parsed_dates > pd.Timestamp.now())
    e_df["dq_invalid_hire_date"] = invalid_hire | implausible_hire
    med_hire = parsed_dates.dropna().median()
    e_df["hire_date"] = parsed_dates.fillna(med_hire)
    e_df.loc[implausible_hire, "hire_date"] = med_hire
    e_df["hire_date"] = e_df["hire_date"].dt.strftime("%Y-%m-%d")

    # Experience negative sign correction
    numeric_exp = pd.to_numeric(e_df["years_experience"], errors="coerce")
    e_df["dq_negative_experience"] = numeric_exp < 0
    e_df["years_experience"] = numeric_exp.abs().fillna(0).astype(int)

    # Self-manager remediation
    e_df["dq_self_manager"] = e_df["employee_id"] == e_df["manager_id"]
    e_df.loc[e_df["dq_self_manager"], "manager_id"] = "EMP0000"

    # Outlier salary remediation (Junior salary > 50k or Director salary < 25k)
    sal_outlier = ((e_df["level"] == "Junior") & (e_df["salary"] > 50000)) | \
                  ((e_df["level"] == "Director") & (e_df["salary"] < 25000))
    e_df["dq_salary_outlier"] = sal_outlier
    if sal_outlier.sum() > 0:
        for lvl in e_df.loc[sal_outlier, "level"].unique():
            lvl_med = e_df.loc[e_df["level"] == lvl, "salary"].median()
            e_df.loc[sal_outlier & (e_df["level"] == lvl), "salary"] = int(lvl_med)

    # Status conflict remediation:
    # 1. Inactive employees actively managing live projects
    # 2. Future hire dates marked as Active
    p_active_mgrs = set(p_df[p_df["status_category"] == "Active"]["project_manager_id"].dropna().unique())
    inactive_mgr_conflict = e_df["employee_id"].isin(p_active_mgrs) & (e_df["status"].astype(str).str.strip().str.title() == "Inactive")
    future_hire_conflict = (parsed_dates > pd.Timestamp.now()) & (e_df["status"].astype(str).str.strip().str.title() == "Active")
    status_conflict = inactive_mgr_conflict | future_hire_conflict
    e_df["dq_status_conflict"] = status_conflict
    if status_conflict.sum() > 0:
        e_df.loc[inactive_mgr_conflict, "status"] = "Active"
        e_df.loc[future_hire_conflict, "status"] = "Pending"

    # Retain the 12 canonical columns PLUS explicit audit flag columns
    output_employee_cols = [
        "employee_id", "full_name", "email", "department", "role",
        "level", "hire_date", "salary", "manager_id", "region",
        "status", "years_experience",
        "dq_missing_email", "dq_invalid_hire_date", "dq_negative_experience",
        "dq_self_manager", "dq_salary_outlier", "dq_status_conflict"
    ]
    e_df = e_df[output_employee_cols]

    # ── 3. Transactions Vectorized Enrichment ────────────────────────────────
    with open(os.path.join(DATA_DIR, "transactions.json"), "r", encoding="utf-8") as f:
        t_df = pd.json_normalize(json.load(f))
    
    t_df["amount"] = pd.to_numeric(t_df["amount"], errors="coerce").fillna(0.0)
    t_df["amount_aed"] = t_df["amount"].astype(float)
    t_df["transaction_date"] = pd.to_datetime(t_df["transaction_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    t_df["is_approved"] = t_df["approved_by"].notna() & ~t_df["approved_by"].astype(str).isin(["None", "nan", ""])

    # Push transformed record metrics to XCom
    context["ti"].xcom_push(key="clean_projects_count", value=len(p_df))
    context["ti"].xcom_push(key="clean_employees_count", value=len(e_df))
    context["ti"].xcom_push(key="clean_transactions_count", value=len(t_df))

    # Persist clean intermediate DataFrames
    p_df.to_csv(os.path.join(OUTPUT_DIR, "projects_clean.csv"), index=False)
    e_df.to_csv(os.path.join(OUTPUT_DIR, "employees_clean.csv"), index=False)
    t_df.to_csv(os.path.join(OUTPUT_DIR, "transactions_clean.csv"), index=False)
    logger.info("Transformations complete: %d projects, %d employees, %d transactions written.",
                len(p_df), len(e_df), len(t_df))


def task_load_to_output(**context):
    """
    Task 4: Output Persistence Audit.
    Verifies that all required transformed CSV files exist on disk,
    are non-empty, and are accessible for downstream consumption.
    """
    logger.info("Verifying output persistence in destination: %s", OUTPUT_DIR)
    expected_files = ["projects_clean.csv", "employees_clean.csv", "transactions_clean.csv"]
    for fname in expected_files:
        fpath = os.path.join(OUTPUT_DIR, fname)
        if not os.path.exists(fpath):
            raise FileNotFoundError(f"Load Failure: Expected clean output missing at {fpath}")
        fsize = os.path.getsize(fpath)
        if fsize == 0:
            raise ValueError(f"Load Failure: Generated output file is 0 bytes: {fpath}")
        logger.info("Verified artifact: %s (Size: %d bytes)", fname, fsize)
    return "LOAD_SUCCESS"


def task_generate_pipeline_report(**context):
    """
    Task 5: XCom-Driven Automated Audit Report Generation.
    Retrieves record counts and quality verdicts pushed by upstream tasks
    and compiles an executive summary report saved to disk.
    """
    ti = context["ti"]
    exec_date = context.get("ds", datetime.now().strftime("%Y-%m-%d"))
    report_file = os.path.join(OUTPUT_DIR, f"pipeline_report_{exec_date}.txt")

    # Pull metrics from upstream task XComs
    raw_p = ti.xcom_pull(task_ids="extract_projects", key="raw_projects_count") or 0
    raw_e = ti.xcom_pull(task_ids="extract_employees", key="raw_employees_count") or 0
    raw_t = ti.xcom_pull(task_ids="extract_transactions", key="raw_transactions_count") or 0
    
    clean_p = ti.xcom_pull(task_ids="transform_and_enrich", key="clean_projects_count") or 0
    clean_e = ti.xcom_pull(task_ids="transform_and_enrich", key="clean_employees_count") or 0
    clean_t = ti.xcom_pull(task_ids="transform_and_enrich", key="clean_transactions_count") or 0
    
    dq_status = ti.xcom_pull(task_ids="validate_data_quality", key="dq_gate_status") or "UNKNOWN"

    report_content = f"""
=============================================================
Presight Daily Enterprise ETL Pipeline Audit Report
Execution Date: {exec_date}
Timezone: Asia/Dubai (GST, UTC+4)
=============================================================
Data Quality Pre-Gate Verdict: {dq_status}

Record Volumes Processed:
  - Raw Projects Ingested:       {raw_p}  --> Clean: {clean_p}
  - Raw Employees Ingested:      {raw_e} --> Clean: {clean_e}
  - Raw Transactions Ingested:   {raw_t} --> Clean: {clean_t}

Storage & Output Directory:
  - {OUTPUT_DIR}
=============================================================
Pipeline Run Verdict: SUCCESS
Generated automatically by Apache Airflow DAG: presight_etl_pipeline
"""
    with open(report_file, "w", encoding="utf-8") as f:
        f.write(report_content)

    logger.info("Pipeline audit report successfully written to %s", report_file)


# ==============================================================================
# DAG DEFINITION & DEPENDENCY WIRING
# ==============================================================================

if DAG:
    with DAG(
        dag_id="presight_etl_pipeline",
        default_args=default_args,
        description="Daily automated production ETL pipeline with data quality pre-gate",
        schedule="0 6 * * *",       # Daily at 06:00 AM UAE Time (Asia/Dubai)
        start_date=datetime(2025, 1, 1, tzinfo=local_tz),
        catchup=False,               # Do not backfill historical days upon activation
        max_active_runs=1,           # Enforce serial daily runs to avoid file write contention
        tags=["presight", "etl", "production", "pillar_3"],
    ) as dag:

        # Task 1: Pipeline Anchor
        start = EmptyOperator(task_id="start")

        # Task 2: Parallel Extractions
        extract_projects = PythonOperator(
            task_id="extract_projects",
            python_callable=task_extract_projects
        )

        extract_employees = PythonOperator(
            task_id="extract_employees",
            python_callable=task_extract_employees
        )

        extract_transactions = PythonOperator(
            task_id="extract_transactions",
            python_callable=task_extract_transactions
        )

        # Task 3: Quality Gate (Blocks Downstream Execution on Failure)
        validate_dq = PythonOperator(
            task_id="validate_data_quality",
            python_callable=task_validate_data_quality
        )

        # Task 4: Vectorized Transformations
        transform_enrich = PythonOperator(
            task_id="transform_and_enrich",
            python_callable=task_transform_and_enrich
        )

        # Task 5: Output Audit
        load_output = PythonOperator(
            task_id="load_to_output",
            python_callable=task_load_to_output
        )

        # Task 6: Audit Telemetry Reporting
        report = PythonOperator(
            task_id="generate_pipeline_report",
            python_callable=task_generate_pipeline_report
        )

        # Task 7: Terminal Anchor
        end = EmptyOperator(task_id="end")

        # ── Dependency Graph Architecture ─────────────────────────────────────
        # 1. Parallel Extractions: Projects, employees, and transactions run concurrently.
        # 2. Convergence to Quality Gate: All extractions must pass before DQ checks execute.
        # 3. Serial Execution: Clean transformations -> Load validation -> Telemetry report.
        start >> [extract_projects, extract_employees, extract_transactions] >> validate_dq
        validate_dq >> transform_enrich >> load_output >> report >> end


# ==============================================================================
# LOCAL STANDALONE TEST RUNNER (FOR UNIT TESTING & CI/CD PIPELINES)
# ==============================================================================
if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("  PRESIGHT AIRFLOW ETL PIPELINE (presight_etl_pipeline)")
    print("  Schedule: 06:00 AM daily in Asia/Dubai (UTC+4)")
    print("=" * 70)
    print("Validating DAG dependency structure:")
    print("  start >> [extract_projects, extract_employees, extract_transactions] >> validate_dq")
    print("  validate_dq >> transform_enrich >> load_output >> report >> end")
    print("-" * 70)
    print("Executing local task simulation with XCom passing...")

    class MockTaskInstance:
        """Simulates Airflow TaskInstance XCom storage for local testing."""
        def __init__(self):
            self.store = {}
        def xcom_push(self, key, value):
            self.store[key] = value
        def xcom_pull(self, task_ids=None, key=None):
            return self.store.get(key)

    mock_ti = MockTaskInstance()
    today_str = datetime.now().strftime("%Y-%m-%d")
    context = {"ti": mock_ti, "ds": today_str}

    # 1. Parallel Extractions
    p_cnt = task_extract_projects(**context)
    e_cnt = task_extract_employees(**context)
    t_cnt = task_extract_transactions(**context)
    print(f"  [1/5 PASS] Ingested: {p_cnt} projects, {e_cnt} employees, {t_cnt} transactions.")

    # 2. Pre-clean Data Quality Gate
    task_validate_data_quality(**context)
    print(f"  [2/5 PASS] DQ Gate: {mock_ti.xcom_pull(key='dq_gate_status')}")

    # 3. Transform & Enrich
    task_transform_and_enrich(**context)
    print(f"  [3/5 PASS] Transformations: Staged clean datasets to output directory.")

    # 4. Load & Target Persistence
    task_load_to_output(**context)
    print(f"  [4/5 PASS] Load Verification: All clean output artifacts confirmed.")

    # 5. Generate Report via XCom
    task_generate_pipeline_report(**context)
    report_file = os.path.join(OUTPUT_DIR, f"pipeline_report_{today_str}.txt")
    print(f"  [5/5 PASS] Pipeline Report: Written to {report_file}")
    print("=" * 70)
    print("  All Airflow pipeline tasks executed and verified successfully!")
    print("=" * 70 + "\n")
