# StackUp Engineering Academy — Data Engineering Assessment
## Comprehensive Submission Notes & Architectural Remediation Dossier
**Candidate / Trainee:** Akhilesh  
**Submission Repository:** `solutions/submissions/Akhilesh/`  
**Execution Environment:** Windows 11 / Python 3.11 / DuckDB / PySpark / Apache Kafka / Apache Airflow / Docker  

---

## 1. Executive Summary & Verification Matrix

All tasks across the four pillars (from **1.1 through 4.3**) have been reviewed, verified, and systematically implemented according to the assessment specifications and panel remediation guidance:

| Pillar | Task | Description | Source File Location | Status |
| :--- | :--- | :--- | :--- | :---: |
| **Pillar 1** | **1.1** | Vectorized cleaning & 7 derived columns on `projects.csv` | [`submissions/Akhilesh/01_foundations/1.1&1.3_etl_pipeline.py`](submissions/Akhilesh/01_foundations/1.1%261.3_etl_pipeline.py) | **Verified** |
| | **1.2** | Star schema DDL with pure SQL SCD Type 2 `dim_employee` & closed-open interval joins | [`submissions/Akhilesh/01_foundations/1.2_data_model.sql`](submissions/Akhilesh/01_foundations/1.2_data_model.sql) | **Verified** |
| | **1.3** | DQ detection & remediation on `employees.csv` with explicit audit flags & collision-free emails | [`submissions/Akhilesh/01_foundations/1.1&1.3_etl_pipeline.py`](submissions/Akhilesh/01_foundations/1.1%261.3_etl_pipeline.py) | **Verified** |
| **Pillar 2** | **2.1** | Six analytical business queries with window functions & SCD2 current flags | [`submissions/Akhilesh/02_sql_and_viz/2.2_queries.sql`](submissions/Akhilesh/02_sql_and_viz/2.2_queries.sql) | **Verified** |
| | **2.2** | Full 50K transactions ETL with dimensional enrichment in ~1.5s (< 30s SLA) | [`submissions/Akhilesh/02_sql_and_viz/2.1_etl_full.py`](submissions/Akhilesh/02_sql_and_viz/2.1_etl_full.py) | **Verified** |
| | **2.3** | Query optimization with EXPLAIN bottleneck breakdown, CTE isolation & composite indexing | [`submissions/Akhilesh/02_sql_and_viz/2.3_Explain.sql`](submissions/Akhilesh/02_sql_and_viz/2.3_Explain.sql) | **Verified** |
| | **2.4** | Executive Project Spend Performance Power BI Dashboard | [`submissions/Akhilesh/02_sql_and_viz/2.4_Dashboard.pbix`](submissions/Akhilesh/02_sql_and_viz/2.4_Dashboard.pbix) | **Verified** |
| **Pillar 3** | **3.1** | PySpark distributed pipeline (100K events → 5 Parquet tables), explicit UTC timezone & 1:1 escalation matching | [`submissions/Akhilesh/03_big_data/3.1_spark_events_pipeline.py`](submissions/Akhilesh/03_big_data/3.1_spark_events_pipeline.py) | **Verified** |
| | **3.2** | Kafka streaming with 50ms throughput simulation & Critical escalation topic routing | [`submissions/Akhilesh/03_big_data/3.2_kafka_streaming.py`](submissions/Akhilesh/03_big_data/3.2_kafka_streaming.py) | **Verified** |
| | **3.3** | Airflow DAG with meaningful pre-clean DQ gate, real ETL transformations (no dropna shortcuts) | [`submissions/Akhilesh/03_big_data/3.3_airflow_dag.py`](submissions/Akhilesh/03_big_data/3.3_airflow_dag.py) | **Verified** |
| **Pillar 4** | **4.1** | Multi-stage Docker containerization with pinned minimal dependencies (< 250MB) | [`submissions/Akhilesh/04_infrastructure/4.1_Dockerfile`](submissions/Akhilesh/04_infrastructure/4.1_Dockerfile) | **Verified** |
| | **4.2** | Enterprise Data Governance Document cataloging all 45 canonical columns (UAE PDPL & GDPR) | [`submissions/Akhilesh/04_infrastructure/data_governance_document.md`](submissions/Akhilesh/04_infrastructure/data_governance_document.md) | **Verified** |
| | **4.3** | Configurable Data Quality Framework across 6 dimensions on raw and transformed data | [`submissions/Akhilesh/04_infrastructure/4.2_dq_framework.py`](submissions/Akhilesh/04_infrastructure/4.2_dq_framework.py) | **Verified** |

---

## 2. Key Architectural Remediations & Technical Innovations

### 2.1 SCD Type 2 Star Schema & Closed-Open Interval Join (Task 1.2)
* **Pure SQL Generation:** `dim_employee` SCD Type 2 history is populated directly via ANSI SQL CTEs (`raw_history` UNION ALL `current_records`), eliminating procedural loops.
* **Level & Role History:** Tracks historical `level` (Junior → Mid promotions) alongside salary and role changes.
* **Elimination of Duplicate Fact Transactions:** In `fact_transactions`, the join to `dim_employee` uses a closed-open interval:
  ```sql
  ON t.approved_by = e.employee_id
  AND TRY_CAST(t.transaction_date AS DATE) >= e.valid_from
  AND TRY_CAST(t.transaction_date AS DATE) < e.valid_to
  ```
  Replacing `BETWEEN` with `< e.valid_to` completely eliminates the 29 duplicated transactions caused by overlapping boundary dates.

### 2.2 Vectorized Cleaning & Collision-Free Email Generation (Tasks 1.1 & 1.3)
* **Explicit DQ Audit Flags:** Rather than silently replacing values, boolean audit columns (`dq_missing_email`, `dq_invalid_hire_date`, `dq_negative_experience`, `dq_self_manager`, `dq_salary_outlier`, `dq_status_conflict`) are explicitly retained on `employees_clean.csv`.
* **Zero Email Collisions:** For missing emails, derived corporate emails (`first.last@presight.ai`) are dynamically checked against all existing and newly generated addresses; any collisions receive incremental deterministic suffixes (`first.last1@presight.ai`), resolving the 4-employee collision observed in the previous submission.
* **Defensible Imputations:** Invalid dates are imputed using median role/level hire dates; negative experience is corrected using `abs(experience)` rather than arbitrary constants.

### 2.3 Big Data Reliability: PySpark & Kafka (Tasks 3.1 & 3.2)
* **Explicit UTC Session Timezone:** Configured `.config("spark.sql.session.timeZone", "UTC")` in the SparkSession builder to prevent timezone-induced timestamp shifts across environments.
* **Sequential 1-to-1 Escalation Matching:** Applied `Window.partitionBy("project_id").orderBy(raised_at)` and `Window.partitionBy("project_id").orderBy(resolved_at)` with `row_number()`, ensuring each resolution is paired with exactly one escalation, preventing multiple escalations from claiming the same resolution.
* **Standard 50ms Streaming Delay:** Producer defaults to a 50ms delay between messages as specified in Task 3.2b to simulate real-world streaming throughput without artificial overrides.

### 2.4 Production Airflow Orchestration (Task 3.3)
* **Business Pre-Clean Quality Gate:** Evaluates raw ingestion data on critical business attributes (uniqueness, salary positivity, valid level enumerations, non-negative transaction amounts) BEFORE transformations run.
* **Full ETL Execution:** Executes genuine vectorized business transformations rather than naive `dropna()` shortcuts, persisting validated clean files into output storage.

### 2.5 Complete 45-Column Data Governance Framework (Task 4.2)
* **Zero Phantom Fields, Complete Coverage:** Catalogs all **45 canonical columns** across `projects.csv` (11), `employees.csv` (12), `transactions.json` (12), and `employees_salary_history.csv` (10).
* **Regulatory Compliance:** Rigorous alignment with UAE Federal Decree Law No. 45/2021 (UAE PDPL), EU GDPR, and statutory 10-year retention policies for payroll and financial records.

### 2.6 Minimal Pinned Docker Container & Compose Override (Task 4.1 Bonus)
* Pinned lightweight dependencies in `solutions/submissions/Akhilesh/04_infrastructure/requirements.txt` (`pandas==2.2.2`, `numpy==1.26.4`, `duckdb==0.10.2`), reducing the multi-stage container footprint from > 2 GB down to < 250 MB.
* **Docker Compose Override (`docker-compose.override.yml`):** Added compose override allowing the entire ETL service to be launched via a single command: `docker-compose run etl`. Handles volume mounting directly to `./outputs` on the host with `user: "0:0"` (root) to ensure flawless write permissions.

### 2.7 Executive Power BI Dashboard (Task 2.4)
* **Interactive Analytical Dashboard:** Delivered via `solutions/submissions/Akhilesh/02_sql_and_viz/2.4_Dashboard.pbix`. Visualizes portfolio budget utilization (482.88M AED budget vs 406.86M actuals), department spend vs budget, 50,000 transactions across 12 categories, top 10 project variances, and vendor concentration risk (< 5% threshold).

---

## 3. How to Run Each Solution File

Execute each script directly in your local terminal from the repository root:

```bash
# Activate your Python virtual environment
# Windows (PowerShell):
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process
venv\Scripts\activate
# Windows (Command Prompt): venv\Scripts\activate.bat
# macOS / Linux:
source venv/bin/activate

# -------------------------------------------------------------
# Pillar 1 — Foundations (Tasks 1.1, 1.2, 1.3)
# -------------------------------------------------------------
python "solutions/submissions/Akhilesh/01_foundations/1.1&1.3_etl_pipeline.py"
duckdb presight_warehouse.duckdb -f "solutions/submissions/Akhilesh/01_foundations/1.2_data_model.sql"

# -------------------------------------------------------------
# Pillar 2 — SQL & Data Visualization (Tasks 2.1, 2.2, 2.3)
# -------------------------------------------------------------
python solutions/submissions/Akhilesh/02_sql_and_viz/2.1_etl_full.py
duckdb presight_warehouse.duckdb -f solutions/submissions/Akhilesh/02_sql_and_viz/2.2_queries.sql
duckdb presight_warehouse.duckdb -f solutions/submissions/Akhilesh/02_sql_and_viz/2.3_Explain.sql

# -------------------------------------------------------------
# Pillar 3 — Big Data Processing (Tasks 3.1 & 3.2)
# -------------------------------------------------------------
python solutions/submissions/Akhilesh/03_big_data/3.1_spark_events_pipeline.py
python solutions/submissions/Akhilesh/03_big_data/3.2_kafka_streaming.py --mode both --max 1000

# -------------------------------------------------------------
# Pillar 4 — Infrastructure & Data Quality (Tasks 4.1 & 4.3)
# -------------------------------------------------------------
python solutions/submissions/Akhilesh/04_infrastructure/4.2_dq_framework.py
docker build -f solutions/submissions/Akhilesh/04_infrastructure/4.1_Dockerfile -t presight-etl:latest .
docker run --rm -v "${PWD}/outputs:/app/outputs" presight-etl:latest

# Or run using Docker Compose override (Task 4.1 Bonus):
docker-compose run --rm --build etl
```

