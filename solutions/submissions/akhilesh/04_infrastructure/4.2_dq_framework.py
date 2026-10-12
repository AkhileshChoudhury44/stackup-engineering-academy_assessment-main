"""
StackUp Engineering Academy — Data Engineering Assessment
Pillar 4: Infrastructure & Governance — Task 4.3: Configurable Data Quality Framework
Trainee: Akhilesh
File: 4.2_dq_framework.py
"""

import os
import json
import logging
from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np

# ── Logging Setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("DataQualityFramework")

# ── Paths ─────────────────────────────────────────────────────────────────────
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", "..", "..", ".."))

if not os.path.exists(os.path.join(REPO_ROOT, "datasets")):
    REPO_ROOT = os.getcwd()

DATA_DIR = os.path.join(REPO_ROOT, "datasets")
OUTPUT_DIR = os.path.join(REPO_ROOT, "outputs", "results", "Akhilesh", "04_infrastructure")
ROOT_OUTPUT_DIR = os.path.join(REPO_ROOT, "outputs")
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(ROOT_OUTPUT_DIR, exist_ok=True)


# ── Configurable Data Quality Rules ──────────────────────────────────────────
# Declarative configuration defining rules, thresholds, ranges, and foreign keys.
# Extensible without modifying underlying pipeline or framework code.
DQ_CONFIG: Dict[str, Any] = {
    "projects": {
        "completeness_threshold": 90.0,
        "pk_columns": ["project_id"],
        "numeric_ranges": {
            "budget": {"min": 0, "max": 10_000_000},
            "actual_cost": {"min": 0, "max": 10_000_000}
        },
        "date_columns": ["start_date", "end_date"],
        "consistency_rules": [
            {"type": "before", "columns": ["start_date", "end_date"]},
            {"type": "non_negative", "column": "actual_cost"}
        ],
        "foreign_keys": {
            "project_manager_id": ("employees", "employee_id")
        }
    },
    "employees": {
        "completeness_threshold": 85.0,
        "pk_columns": ["employee_id"],
        "date_columns": ["hire_date"],
        "numeric_ranges": {
            "salary": {"min": 1000, "max": 250_000},
            "years_experience": {"min": 0, "max": 50}
        },
        "consistency_rules": [
            {"type": "non_negative", "column": "years_experience"}
        ],
        "permitted_values": {
            "level": ["Junior", "Mid", "Senior", "Lead", "Director"],
            "status": ["Active", "Inactive", "On Leave", "Terminated"]
        }
    },
    "transactions": {
        "completeness_threshold": 95.0,
        "pk_columns": ["transaction_id"],
        "numeric_ranges": {
            "amount": {"min": 0.0, "max": 10_000_000.0}
        },
        "consistency_rules": [
            {"type": "non_negative", "column": "amount"}
        ],
        "date_columns": ["transaction_date"],
        "foreign_keys": {
            "project_id": ("projects", "project_id"),
            "approved_by": ("employees", "employee_id")
        },
        "permitted_values": {
            "payment_status": ["Paid", "Pending", "Disputed", "Cancelled"]
        }
    },
    "employees_salary_history": {
        "completeness_threshold": 85.0,
        "foreign_keys": {
            "employee_id": ("employees", "employee_id")
        },
        "date_columns": ["effective_date"],
        "numeric_ranges": {
            "new_salary": {"min": 1000, "max": 250_000}
        },
        "consistency_rules": [
            {"type": "non_negative", "column": "new_salary"}
        ],
        "permitted_values": {
            "new_level": ["Junior", "Mid", "Senior", "Lead", "Director"],
            "change_type": ["Hire", "Annual Raise", "Promotion", "Market Adjustment", "Role Change"]
        }
    }
}


class DataQualityFramework:
    """
    Configurable, production-grade Data Quality check engine.
    Supports 6 core data quality dimensions:
      1. Completeness (Cell-level & column-level thresholds)
      2. Uniqueness (Primary key & composite uniqueness)
      3. Validity (Numeric ranges, date formats, string syntax)
      4. Consistency (Cross-column date ordering & non-negativity)
      5. Referential Integrity (Foreign key verification against parent tables)
      6. Permitted Value Set (Domain categorical conformity)
    """

    def __init__(self, dataset_name: str, df: pd.DataFrame):
        self.dataset_name = dataset_name
        self.df = df
        self.total_rows = len(df)
        self.results = {
            "dataset_name": dataset_name,
            "total_rows": self.total_rows,
            "checks_run": 0,
            "checks_passed": 0,
            "checks_failed": 0,
            "passed_checks": 0,
            "failed_checks": 0,
            "results": {},
            "details": []
        }

    def _record_result(
        self,
        check_name: str,
        passed: bool,
        failed_rows: int,
        details: str,
        metric_name: str,
        metric_value: str,
        expected: str,
        severity: str = "WARNING"
    ):
        self.results["checks_run"] += 1
        status = "PASS" if passed else "FAIL"

        if passed:
            self.results["checks_passed"] += 1
            self.results["passed_checks"] += 1
            logger.info("[%s] PASS: %s - %s", self.dataset_name, check_name, details)
        else:
            self.results["checks_failed"] += 1
            self.results["failed_checks"] += 1
            logger.warning("[%s] WARNING: %s - %s (Failed Rows: %d, Metric: %s, Expected: %s)",
                           self.dataset_name, check_name, details, failed_rows, metric_value, expected)

        self.results["results"][check_name] = {
            "status": status,
            "details": details,
            "failed_rows": failed_rows,
            "metric_name": metric_name,
            "metric_value": metric_value,
            "expected": expected
        }

        self.results["details"].append({
            "check": check_name,
            "dataset": self.dataset_name,
            "passed": passed,
            "failed_rows": failed_rows,
            "details": details,
            "metric_name": metric_name,
            "metric_value": metric_value,
            "expected": expected
        })

    # 1. Overall Dataset Completeness
    def check_table_completeness(self, min_threshold_pct: float = 90.0) -> "DataQualityFramework":
        total_cells = self.df.size
        null_cells = int(self.df.isna().sum().sum())
        non_null_cells = total_cells - null_cells
        completeness_pct = round((non_null_cells / max(total_cells, 1)) * 100.0, 2)
        passed = completeness_pct >= min_threshold_pct
        self._record_result(
            check_name="completeness",
            passed=passed,
            failed_rows=null_cells,
            details="Measures how much of the table is populated across all cells.",
            metric_name="completeness",
            metric_value=f"{completeness_pct:.2f}%",
            expected=f"At least {min_threshold_pct:.2f}% populated"
        )
        return self

    # 2. Primary Key Uniqueness & Non-nullness
    def check_primary_key(self, pk_column: str) -> "DataQualityFramework":
        if pk_column not in self.df.columns:
            self._record_result(
                check_name=f"primary_key:{pk_column}",
                passed=False,
                failed_rows=self.total_rows,
                details=f"Column '{pk_column}' not found in table schema.",
                metric_name=f"null_or_duplicate_rows_in_{pk_column}",
                metric_value=f"{self.total_rows} missing",
                expected="Zero nulls and zero duplicates"
            )
            return self

        null_count = int(self.df[pk_column].isna().sum())
        dup_count = int(self.df[pk_column].dropna().duplicated().sum())
        bad_count = null_count + dup_count
        passed = bad_count == 0
        self._record_result(
            check_name=f"primary_key:{pk_column}",
            passed=passed,
            failed_rows=bad_count,
            details="Checks that the business key is present once per row.",
            metric_name=f"null_or_duplicate_rows_in_{pk_column}",
            metric_value=str(bad_count),
            expected="Zero nulls and zero duplicates"
        )
        return self

    # 3. Numeric Range Validity
    def check_numeric_range(
        self,
        column: str,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None
    ) -> "DataQualityFramework":
        if column not in self.df.columns:
            self._record_result(
                check_name=f"numeric_range:{column}",
                passed=False,
                failed_rows=self.total_rows,
                details=f"Column '{column}' missing from table.",
                metric_name=column,
                metric_value="Missing",
                expected=f"Values between {min_val} and {max_val}"
            )
            return self

        series = pd.to_numeric(self.df[column], errors="coerce")
        violations = 0
        if min_val is not None:
            violations += int((series < min_val).sum())
        if max_val is not None:
            violations += int((series > max_val).sum())

        passed = violations == 0
        range_str = f"{min_val}..{max_val}"
        self._record_result(
            check_name=f"numeric_range:{column}",
            passed=passed,
            failed_rows=violations,
            details="Checks whether numeric values fall inside the expected business range.",
            metric_name=column,
            metric_value=range_str,
            expected=f"Values between {min_val} and {max_val}"
        )
        return self

    # 4. Valid Date Format Parsing
    def check_valid_date(self, column: str) -> "DataQualityFramework":
        if column not in self.df.columns:
            self._record_result(
                check_name=f"valid_date:{column}",
                passed=False,
                failed_rows=self.total_rows,
                details=f"Column '{column}' missing from table.",
                metric_name=column,
                metric_value="Missing",
                expected="Valid date values"
            )
            return self

        not_null_series = self.df[column].dropna()
        parsed = pd.to_datetime(not_null_series, errors="coerce")
        invalid_count = int(parsed.isna().sum())
        valid_count = int(parsed.notna().sum())
        passed = invalid_count == 0
        self._record_result(
            check_name=f"valid_date:{column}",
            passed=passed,
            failed_rows=invalid_count,
            details="Checks whether the date can be parsed correctly.",
            metric_name=column,
            metric_value=f"{valid_count} valid / {invalid_count} invalid",
            expected="Valid date values"
        )
        return self

    # 5. Date Consistency Ordering (start_date <= end_date)
    def check_date_ordering(self, start_col: str, end_col: str) -> "DataQualityFramework":
        if start_col not in self.df.columns or end_col not in self.df.columns:
            self._record_result(
                check_name=f"before:{start_col}:{end_col}",
                passed=False,
                failed_rows=self.total_rows,
                details=f"Date columns '{start_col}' or '{end_col}' missing.",
                metric_name=f"{start_col} <= {end_col}",
                metric_value="Missing columns",
                expected=f"{start_col} must be on or before {end_col}"
            )
            return self

        starts = pd.to_datetime(self.df[start_col], errors="coerce")
        ends = pd.to_datetime(self.df[end_col], errors="coerce")
        both_present = starts.notna() & ends.notna()
        bad_rows = int((starts[both_present] > ends[both_present]).sum())
        passed = bad_rows == 0
        self._record_result(
            check_name=f"before:{start_col}:{end_col}",
            passed=passed,
            failed_rows=bad_rows,
            details="Checks that the first date happens before the second date.",
            metric_name=f"{start_col} <= {end_col}",
            metric_value=f"{bad_rows} bad rows",
            expected=f"{start_col} must be on or before {end_col}"
        )
        return self

    # 6. Non-Negative Value Check
    def check_non_negative(self, column: str) -> "DataQualityFramework":
        if column not in self.df.columns:
            self._record_result(
                check_name=f"non_negative:{column}",
                passed=False,
                failed_rows=self.total_rows,
                details=f"Column '{column}' missing from table.",
                metric_name=column,
                metric_value="Missing",
                expected="Zero negative values"
            )
            return self

        series = pd.to_numeric(self.df[column], errors="coerce").dropna()
        neg_count = int((series < 0).sum())
        passed = neg_count == 0
        self._record_result(
            check_name=f"non_negative:{column}",
            passed=passed,
            failed_rows=neg_count,
            details="Checks that values are not below zero.",
            metric_name=column,
            metric_value=f"{neg_count} negative rows",
            expected="Zero negative values"
        )
        return self

    # 7. Referential Integrity Check
    def check_foreign_key(
        self,
        child_col: str,
        parent_df: pd.DataFrame,
        parent_col: str
    ) -> "DataQualityFramework":
        if child_col not in self.df.columns or parent_col not in parent_df.columns:
            self._record_result(
                check_name=f"foreign_key:{child_col}",
                passed=False,
                failed_rows=self.total_rows,
                details="Foreign key or parent primary key column missing.",
                metric_name=child_col,
                metric_value="Missing key",
                expected=f"Match values in {parent_col}"
            )
            return self

        child_vals = self.df[child_col].dropna().astype(str).str.strip()
        # Filter out empty or unassigned strings
        valid_child_vals = child_vals[~child_vals.isin(["", "None", "nan", "NULL"])]
        parent_vals = set(parent_df[parent_col].dropna().astype(str).str.strip())
        missing_count = int((~valid_child_vals.isin(parent_vals)).sum())
        passed = missing_count == 0
        self._record_result(
            check_name=f"foreign_key:{child_col}",
            passed=passed,
            failed_rows=missing_count,
            details="Checks whether the reference key exists in the related table.",
            metric_name=child_col,
            metric_value=f"{missing_count} missing rows",
            expected=f"Match values in {parent_col}"
        )
        return self

    # 8. Categorical Value Set Check
    def check_permitted_values(self, column: str, allowed_values: List[str]) -> "DataQualityFramework":
        if column not in self.df.columns:
            self._record_result(
                check_name=f"permitted_values:{column}",
                passed=False,
                failed_rows=self.total_rows,
                details=f"Column '{column}' missing from table.",
                metric_name=column,
                metric_value="Missing",
                expected=f"Values in {allowed_values}"
            )
            return self

        series = self.df[column].dropna().astype(str).str.strip()
        allowed_set = set(allowed_values)
        invalid_count = int((~series.isin(allowed_set)).sum())
        passed = invalid_count == 0
        self._record_result(
            check_name=f"permitted_values:{column}",
            passed=passed,
            failed_rows=invalid_count,
            details=f"Checks that values belong to the allowed category list {allowed_values}.",
            metric_name=column,
            metric_value=f"{invalid_count} invalid values",
            expected=f"Values in {allowed_values}"
        )
        return self

    def apply_config(
        self,
        config: Dict[str, Any],
        context_dfs: Optional[Dict[str, pd.DataFrame]] = None
    ) -> "DataQualityFramework":
        """
        Execute checks defined in a declarative configuration dictionary.
        Enables adding rules and thresholds without modifying pipeline code.
        """
        if context_dfs is None:
            context_dfs = {}

        # 1. Completeness
        if "completeness_threshold" in config:
            self.check_table_completeness(float(config["completeness_threshold"]))

        # 2. Uniqueness (PKs)
        for pk in config.get("pk_columns", []):
            self.check_primary_key(pk)

        # 3. Numeric ranges
        for col, r_cfg in config.get("numeric_ranges", {}).items():
            self.check_numeric_range(col, min_val=r_cfg.get("min"), max_val=r_cfg.get("max"))

        # 4. Valid dates
        for col in config.get("date_columns", []):
            self.check_valid_date(col)

        # 5. Consistency rules
        for rule in config.get("consistency_rules", []):
            rule_type = rule.get("type")
            if rule_type == "before":
                cols = rule.get("columns", [])
                if len(cols) == 2:
                    self.check_date_ordering(cols[0], cols[1])
            elif rule_type == "non_negative":
                col = rule.get("column")
                if col:
                    self.check_non_negative(col)

        # 6. Referential integrity (FKs)
        for child_col, parent_ref in config.get("foreign_keys", {}).items():
            parent_table, parent_col = parent_ref
            if parent_table in context_dfs:
                self.check_foreign_key(child_col, context_dfs[parent_table], parent_col)

        # 7. Permitted categorical values
        for col, allowed in config.get("permitted_values", {}).items():
            self.check_permitted_values(col, allowed)

        return self

    def generate_markdown_report(self, target_dir: str) -> str:
        os.makedirs(target_dir, exist_ok=True)
        report_file = os.path.join(target_dir, f"dq_report_{self.dataset_name}.md")

        lines = [
            "# Data Quality Report",
            "",
            f"Checks run: {self.results['checks_run']}",
            f"Checks passed: {self.results['passed_checks']}",
            f"Checks failed: {self.results['failed_checks']}",
            "",
            "Each row below explains what was tested, the measured value, the expected rule, and whether the row-level check passed.",
            "",
            "| check | dataset | passed | failed_rows | details | metric_name | metric_value | expected |",
            "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |"
        ]

        for item in self.results["details"]:
            check_str = item["check"]
            ds_str = item["dataset"]
            passed_str = str(item["passed"])
            failed_str = str(item["failed_rows"])
            details_str = item["details"].replace("|", "/")
            metric_name_str = item["metric_name"].replace("|", "/")
            metric_val_str = item["metric_value"].replace("|", "/")
            expected_str = item["expected"].replace("|", "/")

            lines.append(
                f"| {check_str} | {ds_str} | {passed_str} | {failed_str} | {details_str} | {metric_name_str} | {metric_val_str} | {expected_str} |"
            )

        lines.append("")
        content = "\n".join(lines)
        with open(report_file, "w", encoding="utf-8") as f:
            f.write(content)

        logger.info("Markdown report written to: %s", report_file)
        return report_file


def run_all_data_quality_reports():
    logger.info("=" * 65)
    logger.info("Starting Enterprise Data Quality Framework Audit")
    logger.info("=" * 65)

    # Ingest all 4 operational datasets
    projects_file = os.path.join(DATA_DIR, "projects.csv")
    employees_file = os.path.join(DATA_DIR, "employees.csv")
    transactions_file = os.path.join(DATA_DIR, "transactions.json")
    salary_hist_file = os.path.join(DATA_DIR, "employees_salary_history.csv")

    projects_df = pd.read_csv(projects_file)
    employees_df = pd.read_csv(employees_file)
    with open(transactions_file, "r", encoding="utf-8") as f:
        transactions_df = pd.json_normalize(json.load(f))
    salary_hist_df = pd.read_csv(salary_hist_file)

    reports_generated = []
    context_dfs = {
        "projects": projects_df,
        "employees": employees_df,
        "transactions": transactions_df,
        "employees_salary_history": salary_hist_df
    }

    # ── 1. Projects Suite (Config-driven) ────────────────────────────────────
    p_suite = DataQualityFramework("projects", projects_df)
    p_suite.apply_config(DQ_CONFIG["projects"], context_dfs)
    p_rep = p_suite.generate_markdown_report(OUTPUT_DIR)
    reports_generated.append(p_rep)

    # ── 2. Employees Suite (Config-driven) ───────────────────────────────────
    e_suite = DataQualityFramework("employees", employees_df)
    e_suite.apply_config(DQ_CONFIG["employees"], context_dfs)
    e_rep = e_suite.generate_markdown_report(OUTPUT_DIR)
    reports_generated.append(e_rep)

    # ── 3. Transactions Suite (Config-driven) ────────────────────────────────
    t_suite = DataQualityFramework("transactions", transactions_df)
    t_suite.apply_config(DQ_CONFIG["transactions"], context_dfs)
    t_rep = t_suite.generate_markdown_report(OUTPUT_DIR)
    reports_generated.append(t_rep)

    # ── 4. Employees Salary History Suite (Config-driven) ────────────────────
    h_suite = DataQualityFramework("employees_salary_history", salary_hist_df)
    h_suite.apply_config(DQ_CONFIG["employees_salary_history"], context_dfs)
    h_rep = h_suite.generate_markdown_report(OUTPUT_DIR)
    reports_generated.append(h_rep)

    print("\n" + "=" * 65)
    print("  ENTERPRISE DATA QUALITY AUDIT COMPLETED")
    print("=" * 65)
    print(f"  [1/4] Projects:               {p_suite.results['passed_checks']}/{p_suite.results['checks_run']} checks passed")
    print(f"  [2/4] Employees:              {e_suite.results['passed_checks']}/{e_suite.results['checks_run']} checks passed")
    print(f"  [3/4] Transactions:           {t_suite.results['passed_checks']}/{t_suite.results['checks_run']} checks passed")
    print(f"  [4/4] Salary History:         {h_suite.results['passed_checks']}/{h_suite.results['checks_run']} checks passed")
    print("-" * 65)
    print("Generated Data Quality Markdown Reports:")
    for rep in reports_generated:
        print(f"  -> {rep}")
    print("=" * 65 + "\n")


if __name__ == "__main__":
    run_all_data_quality_reports()
