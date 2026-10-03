# Pillar 4: Infrastructure & Governance

**Candidate:** Akhilesh  
**Assessment:** StackUp Engineering Academy — Data Engineering Assessment

---

## Deliverables in this Directory

This directory contains the production containerisation, enterprise data quality framework, and regulatory data governance documentation for Pillar 4:

1. **`4.1_Dockerfile`**: Multi-stage production Dockerfile based on `python:3.11-slim`, non-root security execution (`presight:presight`), and pinned minimal dependencies (`pandas==2.2.2`, `numpy==1.26.4`, `pyarrow==15.0.2`, `duckdb==0.10.2`) ensuring image size < 250 MB.
2. **`docker-compose.override.yml`** *(Repo Root)*: Docker Compose override configuration (Task 4.1 Bonus) providing single-command execution via `docker-compose run etl` with automated volume mounting to `./outputs`.
3. **`4.2_dq_framework.py`**: Configurable, declarative Data Quality Framework supporting 8 dimensions (Completeness, Uniqueness, Numeric Range Validity, Date Format Validity, Date Ordering Consistency, Non-negative Consistency, Referential Integrity, and Permitted Value Sets) driven by `DQ_CONFIG`.
4. **`4.3_data_governance_document.md`**: Enterprise Data Governance Framework comprehensively cataloging all 45 canonical columns across operational files, with PII classifications, UAE PDPL / GDPR mappings, 10-year compensation retention, and RBAC matrix.
5. **`requirements.txt`**: Pinned dependencies for the lightweight production runtime container.
6. **`README.md`**: Infrastructure documentation and execution guide.

---

## 1. Multi-Stage Production Container (`4.1_Dockerfile` & `docker-compose.override.yml`)

### Build & Run via Docker CLI
From the repository root:
```powershell
docker build -f solutions/submissions/Akhilesh/04_infrastructure/4.1_Dockerfile -t presight-etl:latest .
docker run --rm -v "${PWD}/outputs:/app/outputs" presight-etl:latest
```

### Build & Run via Docker Compose (Task 4.1 Bonus)
```powershell
docker-compose run etl
# or
docker compose run etl
```

### Key Engineering Highlights
- **Image Optimization (< 250MB):** Separates build dependencies (gcc, build tools) in Stage 1 and copies only runtime artifacts into Stage 2.
- **Security Hardening:** Enforces non-root user `presight:presight`.
- **Healthcheck & Entrypoint:** Built-in Python healthcheck and default execution of `starter_files/etl_starter.py` (mandated by Task 4.1 Requirement 3).
- **Compose Override Integration:** Automatically detected by Docker Compose; mounts output directory to `./outputs`.

---

## 2. Configurable Data Quality Framework (`4.2_dq_framework.py`)

### Execution
Run directly in Python:
```powershell
python solutions/submissions/Akhilesh/04_infrastructure/4.2_dq_framework.py
```

### Supported Data Quality Dimensions
1. **Completeness:** Validates non-null cell ratios against configurable percentage thresholds (e.g. $\ge 85\%$, $\ge 90\%$, $\ge 95\%$).
2. **Uniqueness:** Validates primary key and composite candidate key uniqueness without nulls or duplicates.
3. **Validity (Numeric Ranges):** Enforces bounds on budgets, salaries ($1,000 \le \text{salary} \le 250,000$), and non-negative transaction amounts.
4. **Validity (Date Parsing):** Validates date format parseability on `start_date`, `end_date`, `hire_date`, `effective_date`, `transaction_date`.
5. **Consistency (Temporal Ordering & Non-negativity):** Validates chronological sequence across date pairs ($\text{start\_date} \le \text{end\_date}$) and non-negative values.
6. **Referential Integrity:** Verifies child-to-parent foreign key relationships without orphaned records across projects, employees, and transactions.
7. **Permitted Value Sets:** Validates categorical domain conformity (e.g. project priorities, payment statuses, organizational levels).

### Framework Design & Outputs
- **Declarative Configuration (`DQ_CONFIG`):** Rules and thresholds are defined in a configuration dictionary rather than hardcoded in pipeline logic.
- **Alerting & Return Format:** Logs `WARNING` for any failing check; returns a standardized results dictionary with `dataset_name`, `checks_run`, `checks_passed`, `checks_failed`, and check details.
- **Markdown Reports:** Generates single, human-readable markdown audit reports per dataset directly in `outputs/results/Akhilesh/04_infrastructure/`:
  - `dq_report_projects.md`
  - `dq_report_employees.md`
  - `dq_report_transactions.md`
  - `dq_report_employees_salary_history.md`

---

## 3. Enterprise Data Governance Framework (`4.3_data_governance_document.md`)

- **Canonical Column Coverage:** All 45 canonical columns across `projects.csv` (11), `employees.csv` (12), `transactions.json` (12), and `employees_salary_history.csv` (10) cataloged with zero phantom fields.
- **Regulatory Framework:** Compliance with UAE Federal Decree Law No. 45/2021 (UAE PDPL), EU GDPR, and ISO/IEC 27001.
- **Retention Schedule:** Statutory 10-year retention policy for compensation progression and 7-year retention for financial transactions.
- **Security & Access Control:** Complete Role-Based Access Control (RBAC) matrix defining permissions across Data Engineering, BI, HR, PMO, and Audit roles.
