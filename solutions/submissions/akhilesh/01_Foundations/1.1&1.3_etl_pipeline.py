"""
StackUp Engineering Academy — Data Engineering Assessment
Pillar 1: Foundations (Tasks 1.1 & 1.3)
Trainee: Akhilesh
File: 1.1&1.3_etl_pipeline.py
"""

import os
import logging
import numpy as np
import pandas as pd
from datetime import datetime

# ── Logging setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# ── Paths ──────────────────────────────────────────────────────────────────────
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
# Navigate to repo root (three levels up from solutions/submissions/Akhilesh/01_foundations)
REPO_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", "..", "..", ".."))
DATA_DIR = os.path.join(REPO_ROOT, "datasets")
OUTPUT_DIR = os.path.join(REPO_ROOT, "outputs", "results", "Akhilesh", "01_foundations")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ==============================================================================
# TASK 1.1 — Load and transform projects.csv
# ==============================================================================

def load_projects(filepath: str) -> pd.DataFrame:
    """
    Load projects.csv into a Pandas DataFrame with correct dtypes.
    """
    logger.info("Loading projects data from: %s", filepath)
    df = pd.read_csv(filepath)
    
    # Clean budget and actual_cost (replace nulls with 0 first to ensure proper numeric calculation)
    df['budget'] = pd.to_numeric(df['budget'], errors='coerce').fillna(0.0)
    df['actual_cost'] = pd.to_numeric(df['actual_cost'], errors='coerce').fillna(0.0)
    
    # Parse dates
    df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
    df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce')
    
    return df


def transform_projects(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply business logic transformations to projects DataFrame (Vectorized).
    """
    logger.info("Transforming projects data (Vectorized)...")
    df = df.copy()

    # 1. Budget variance = actual_cost - budget
    df['budget_variance'] = df['actual_cost'] - df['budget']

    # 2. is_over_budget = True if actual_cost > budget, else False
    df['is_over_budget'] = df['actual_cost'] > df['budget']

    # 3. duration_days = days between start_date and end_date where both exist
    duration = (df['end_date'] - df['start_date']).dt.days
    df['duration_days'] = duration

    # 4. budget_utilisation_pct = actual_cost / budget * 100 (handle div-by-zero safely)
    df['budget_utilisation_pct'] = np.where(
        df['budget'] > 0,
        (df['actual_cost'] / df['budget']) * 100.0,
        0.0
    )

    # 5. Standardise status values
    df['status'] = df['status'].astype(str).str.strip().str.title()

    # 6. Map status to status_category
    status_map = {
        "In Progress": "Active",
        "Completed": "Closed",
        "Not Started": "Pending",
        "On Hold": "Pending"
    }
    df['status_category'] = df['status'].map(status_map).fillna("Pending")

    # 7. Add risk_level based on combined logic:
    # High: priority == 'Critical' OR is_over_budget == True
    # Medium: priority == 'High' OR budget_utilisation_pct > 90
    # Low: otherwise
    conditions = [
        (df['priority'] == 'Critical') | (df['is_over_budget'] == True),
        (df['priority'] == 'High') | (df['budget_utilisation_pct'] > 90.0)
    ]
    choices = ['High', 'Medium']
    df['risk_level'] = np.select(conditions, choices, default='Low')

    logger.info("Transformed projects: %d rows, %d columns", len(df), len(df.columns))
    return df


# ==============================================================================
# TASK 1.3 — Data quality detection and fixing in employees.csv
# ==============================================================================

def load_employees(filepath: str) -> pd.DataFrame:
    """
    Load employees.csv and log a baseline quality summary.
    """
    logger.info("Loading employees data from: %s", filepath)
    df = pd.read_csv(filepath)
    null_counts = df.isna().sum().to_dict()
    logger.info("Raw employee count: %d rows", len(df))
    logger.info("Null counts per column: %s", null_counts)
    return df


def clean_employees(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Find and fix ALL data quality issues across employees.csv using vectorized logic.
    Returns (cleaned_df, quality_report).
    """
    logger.info("Detecting and fixing data quality issues in employees.csv...")
    df = df.copy()
    dq_report = {}

    # ── Issue 1: Missing or blank emails ──────────────────────────────────────
    missing_email_mask = df['email'].isna() | df['email'].astype(str).str.strip().eq('')
    count_missing_email = int(missing_email_mask.sum())
    dq_report['missing_emails'] = count_missing_email
    df['dq_missing_email'] = missing_email_mask
    logger.info("DQ Issue 1 - Missing emails detected: %d", count_missing_email)
    
    # Fix: derive valid, collision-free email from full_name: first.last[suffix]@presight.ai
    existing_emails = set(df.loc[~missing_email_mask, 'email'].astype(str).str.lower().str.strip().unique())
    for idx in df[missing_email_mask].index:
        raw_name = str(df.loc[idx, 'full_name']).lower().strip()
        clean_name = "".join(c for c in raw_name if c.isalpha() or c.isspace()).strip()
        name_parts = clean_name.split()
        if len(name_parts) >= 2:
            base_local = f"{name_parts[0]}.{name_parts[-1]}"
        else:
            base_local = clean_name or f"employee_{df.loc[idx, 'employee_id'].lower()}"
        
        candidate = f"{base_local}@presight.ai"
        suffix = 1
        while candidate in existing_emails:
            candidate = f"{base_local}{suffix}@presight.ai"
            suffix += 1
        
        existing_emails.add(candidate)
        df.loc[idx, 'email'] = candidate

    # ── Issue 2: Invalid / Unparseable hire dates ─────────────────────────────
    parsed_dates = pd.to_datetime(df['hire_date'], errors='coerce')
    invalid_dates_mask = parsed_dates.isna() & df['hire_date'].notna()
    count_invalid_dates = int(invalid_dates_mask.sum())
    dq_report['invalid_hire_dates'] = count_invalid_dates
    logger.info("DQ Issue 2 - Invalid hire dates detected: %d", count_invalid_dates)

    # Fix: Impute with median hire date of valid records
    median_hire_date = parsed_dates.dropna().median()
    parsed_dates = parsed_dates.fillna(median_hire_date)
    df['hire_date'] = parsed_dates

    # ── Issue 3: Implausible dates (future dates or pre-company era) ─────────
    implausible_mask = (df['hire_date'] < pd.Timestamp('1990-01-01')) | (df['hire_date'] > pd.Timestamp.now())
    count_implausible = int(implausible_mask.sum())
    dq_report['implausible_hire_dates'] = count_implausible
    df['dq_invalid_hire_date'] = invalid_dates_mask | implausible_mask
    if count_implausible > 0:
        logger.info("DQ Issue 3 - Implausible hire dates detected: %d", count_implausible)
        df.loc[implausible_mask, 'hire_date'] = median_hire_date

    # ── Issue 4: Negative years of experience ─────────────────────────────────
    numeric_exp = pd.to_numeric(df['years_experience'], errors='coerce')
    neg_exp_mask = numeric_exp < 0
    count_neg_exp = int(neg_exp_mask.sum())
    dq_report['negative_years_experience'] = count_neg_exp
    df['dq_negative_experience'] = neg_exp_mask
    logger.info("DQ Issue 4 - Negative years of experience detected: %d", count_neg_exp)
    
    # Fix: Take absolute value of years of experience
    df['years_experience'] = numeric_exp.abs().fillna(0).astype(int)

    # ── Issue 5: Self-referencing manager_id ──────────────────────────────────
    self_mgr_mask = df['employee_id'] == df['manager_id']
    count_self_mgr = int(self_mgr_mask.sum())
    dq_report['employee_is_own_manager'] = count_self_mgr
    df['dq_self_manager'] = self_mgr_mask
    logger.info("DQ Issue 5 - Self-referencing managers detected: %d", count_self_mgr)
    
    # Fix: Reset self-manager to company root executive 'EMP0000'
    df.loc[self_mgr_mask, 'manager_id'] = 'EMP0000'

    # ── Issue 6: Salary vs Level mismatches (outliers) ───────────────────────
    # Detect Junior salary exceeding Lead/Senior levels (e.g. Junior with salary > 50,000)
    salary_mismatch_mask = (df['level'] == 'Junior') & (df['salary'] > 50000)
    count_salary_mismatch = int(salary_mismatch_mask.sum())
    dq_report['salary_level_mismatches'] = count_salary_mismatch
    df['dq_salary_outlier'] = salary_mismatch_mask
    if count_salary_mismatch > 0:
        logger.info("DQ Issue 6 - Salary/level outliers detected: %d", count_salary_mismatch)
        # Fix: Adjust to median salary for Junior level
        junior_median_salary = df.loc[df['level'] == 'Junior', 'salary'].median()
        df.loc[salary_mismatch_mask, 'salary'] = int(junior_median_salary)

    # ── Issue 7: Duplicate employee IDs & Status Conflicts ───────────────────
    dup_id_mask = df['employee_id'].duplicated()
    count_dups = int(dup_id_mask.sum())
    dq_report['duplicate_employee_ids'] = count_dups
    df['dq_status_conflict'] = False
    if count_dups > 0:
        logger.info("DQ Issue 7 - Duplicate employee IDs detected: %d", count_dups)
        df = df.drop_duplicates(subset=['employee_id'], keep='first')

    # Formatting: Convert hire_date to string YYYY-MM-DD
    df['hire_date'] = df['hire_date'].dt.strftime('%Y-%m-%d')

    # Retain strictly the 12 canonical schema columns
    canonical_columns = [
        'employee_id', 'full_name', 'email', 'department', 'role',
        'level', 'hire_date', 'salary', 'manager_id', 'region',
        'status', 'years_experience'
    ]
    df = df[canonical_columns]

    logger.info("Employee cleaning complete. Total cleaned records: %d", len(df))
    return df, dq_report


# ==============================================================================
# PIPELINE EXECUTION
# ==============================================================================

def run_foundations_pipeline():
    logger.info("=" * 60)
    logger.info("Starting Pillar 1 Foundations Pipeline")
    logger.info("=" * 60)

    # Task 1.1: Projects
    projects_file = os.path.join(DATA_DIR, "projects.csv")
    raw_projects = load_projects(projects_file)
    clean_proj = transform_projects(raw_projects)
    proj_out_path = os.path.join(OUTPUT_DIR, "projects_clean.csv")
    clean_proj.to_csv(proj_out_path, index=False)
    logger.info("Wrote clean projects to: %s", proj_out_path)

    # Task 1.3: Employees
    employees_file = os.path.join(DATA_DIR, "employees.csv")
    raw_employees = load_employees(employees_file)
    clean_emp, dq_report = clean_employees(raw_employees)
    emp_out_path = os.path.join(OUTPUT_DIR, "employees_clean.csv")
    clean_emp.to_csv(emp_out_path, index=False)
    logger.info("Wrote clean employees to: %s", emp_out_path)

    # Mirror to root outputs/ directory
    root_outputs = os.path.join(REPO_ROOT, "outputs")
    os.makedirs(root_outputs, exist_ok=True)
    try:
        clean_proj.to_csv(os.path.join(root_outputs, "projects_clean.csv"), index=False)
        clean_emp.to_csv(os.path.join(root_outputs, "employees_clean.csv"), index=False)
    except Exception:
        pass

    # Summary report
    summary_path = os.path.join(OUTPUT_DIR, "data_quality_report.txt")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(f"Data Quality Report - Generated {datetime.now().isoformat()}\n")
        f.write("=" * 60 + "\n\n")
        f.write(f"Projects processed: {len(clean_proj)}\n")
        f.write(f"Employees processed: {len(clean_emp)}\n\n")
        f.write("Employee DQ Issues Identified & Remediated:\n")
        for issue, count in dq_report.items():
            f.write(f"  - {issue}: {count} rows fixed\n")
    logger.info("Wrote quality report to: %s", summary_path)
    logger.info("Pillar 1 Foundations Pipeline complete!")


if __name__ == "__main__":
    run_foundations_pipeline()
