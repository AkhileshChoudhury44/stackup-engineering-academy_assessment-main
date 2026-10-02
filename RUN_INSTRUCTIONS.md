# How to Execute This Assessment Project

This guide provides step-by-step instructions to run the entire assessment project locally across **Windows (PowerShell & Command Prompt)** and **macOS / Linux (bash & zsh)** using the solutions in `solutions/submissions/Akhilesh/`.

---

## 1. Open the Project Folder
Open your terminal or IDE (such as VS Code) and navigate to the repository root directory:

```bash
# Verify you are in the project root containing datasets/, starter_files/, and requirements.txt
cd stackup-engineering-academy_assessment-main
```

---

## 2. Create and Activate a Python Virtual Environment

Select the instructions corresponding to your operating system and preferred shell:

### Option A: Windows (PowerShell)
```powershell
# 1. Create the virtual environment
python -m venv venv

# 2. Allow script execution for this terminal session (prevents PSSecurityException / UnauthorizedAccess)
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process

# 3. Activate the virtual environment
venv\Scripts\activate
```

### Option B: Windows (Command Prompt / CMD)
```cmd
:: 1. Create the virtual environment
python -m venv venv

:: 2. Activate the virtual environment (no execution policy required)
venv\Scripts\activate.bat
```

### Option C: macOS / Linux (bash / zsh)
```bash
# 1. Create the virtual environment
python3 -m venv venv

# 2. Activate the virtual environment
source venv/bin/activate
```

*(Once activated, you will see `(venv)` prepended to your command line prompt.)*

---

## 3. Install Python Dependencies

With your virtual environment activated, install the required packages:

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

---

## 4. Start Docker Background Services

If Docker Desktop is running, start the background infrastructure services (Kafka, Zookeeper, Airflow, PostgreSQL):

```bash
docker compose up -d
# or: docker-compose up -d
```

### Service Health & Web Consoles
Once initialized, access the management interfaces:
- **Kafka UI:** [http://localhost:8080](http://localhost:8080)
- **Airflow Webserver UI:** [http://localhost:8081](http://localhost:8081) (`admin` / `admin`)
- **PostgreSQL Database:** `localhost:5432` (`presight` / `presight123`)

---

## 5. Pillar 1 — Foundations

### 5.1 Run Python ETL (Projects & Employees Cleaning)
> **Note:** Enclose file paths containing `&` in quotes to prevent shell interpretation issues.

```bash
python "solutions/submissions/Akhilesh/01_foundations/1.1&1.3_etl_pipeline.py"
```
**Artifacts Generated:**
- `outputs/results/Akhilesh/01_foundations/projects_clean.csv`
- `outputs/results/Akhilesh/01_foundations/employees_clean.csv`
- `outputs/results/Akhilesh/01_foundations/data_quality_report.txt`

### 5.2 Build Star Schema & SCD Type 2 DDL (`1.2_data_model.sql`)
Run directly via the DuckDB CLI:
```bash
duckdb presight_warehouse.duckdb -f "solutions/submissions/Akhilesh/01_foundations/1.2_data_model.sql"
```
*(If DuckDB CLI is not installed, run via Python: `python -c "import duckdb; con=duckdb.connect('presight_warehouse.duckdb'); con.execute(open('solutions/submissions/Akhilesh/01_foundations/1.2_data_model.sql', encoding='utf-8').read()); print('Data model created successfully!')"`)*

---

## 6. Pillar 2 — SQL & Data Visualization

### 6.1 Run Full 50K Transactions ETL
```bash
python "solutions/submissions/Akhilesh/02_sql_and_viz/2.1_etl_full.py"
```
**Artifacts Generated:**
- All three cleaned DataFrames persisted as CSVs directly in `outputs/` (and `outputs/results/Akhilesh/02_sql_and_viz/`):
  - `outputs/projects_clean.csv`
  - `outputs/employees_clean.csv`
  - `outputs/transactions_clean.csv`
- Comprehensive run audit report:
  - `outputs/pipeline_summary.txt` (with before/after counts, DQ decisions, and execution timing)

### 6.2 Execute Six Analytical Business Queries
```bash
duckdb presight_warehouse.duckdb -f "solutions/submissions/Akhilesh/02_sql_and_viz/2.2_queries.sql"
```
*(Python alternative: `python -c "import duckdb; con=duckdb.connect('presight_warehouse.duckdb'); con.execute(open('solutions/submissions/Akhilesh/02_sql_and_viz/2.2_queries.sql', encoding='utf-8').read()); print('All queries executed successfully!')"`)*

### 6.3 Execute Query Optimization & EXPLAIN Benchmarks
```bash
duckdb presight_warehouse.duckdb -f "solutions/submissions/Akhilesh/02_sql_and_viz/2.3_Explain.sql"
```

### 6.4 Power BI Dashboard
Open in Power BI Desktop (Windows):
- Primary file: `solutions/submissions/Akhilesh/02_sql_and_viz/2.4_Dashboard.pbix`
- Mirrored output: `outputs/presight_dashboard.pbix` (per Task 2.4 specification)

---

## 7. Pillar 3 — Big Data Processing

### 7.1 Run PySpark Batch Events Pipeline (100K Events)
```bash
python "solutions/submissions/Akhilesh/03_big_data/3.1_spark_events_pipeline.py"
```
**Artifacts Generated:**
Creates 5 partitioned Parquet tables in `outputs/results/Akhilesh/03_big_data/spark/`.

### 7.2 Run Kafka Streaming Producer & Consumer
```bash
python "solutions/submissions/Akhilesh/03_big_data/3.2_kafka_streaming.py" --mode both --max 1000
```
**Artifacts Generated:**
Routes critical events to `presight.escalations.critical` and writes:
- `outputs/results/Akhilesh/03_big_data/kafka/summary.json`

### 7.3 Run Airflow DAG Pipeline (`3.3_airflow_dag.py`)

The pipeline can be executed in two modes: within the live Docker Airflow cluster or via local standalone simulation.

#### Option A: Docker Airflow Cluster (Primary Production Mode)
1. **Start Airflow Services:**
   Ensure Docker background services are initialized:
   ```bash
   docker compose up -d
   ```
2. **Access Airflow Web UI:**
   Open your browser and navigate to:
   - **URL:** [http://localhost:8081](http://localhost:8081)
   - **Username:** `admin`
   - **Password:** `admin`
3. **Verify DAG Deployment:**
   The production DAG is registered as **`presight_etl_pipeline`**:
   - **Schedule:** `0 6 * * *` (Daily at 06:00 AM UAE Time / `Asia/Dubai`)
   - **Owner:** `presight_data_engineering`
   - **Tags:** `presight`, `etl`, `production`, `pillar_3`
4. **Trigger DAG Execution:**
   - **Via Web UI:** Toggle the DAG to **Active (Unpause)**, then click the **Trigger DAG (▶)** button on the right.
   - **Via Docker CLI:** Run the trigger command directly from your terminal:
     ```bash
     docker exec presight-airflow-webserver airflow dags trigger presight_etl_pipeline
     ```
5. **Inspect Execution & Graph Flow:**
   In the Airflow Graph View, observe the pipeline stages:
   ```text
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
   ```
   - **Quality Gate:** If raw inputs contain duplicate primary keys, negative amounts, or unapproved domains, `validate_data_quality` halts the run immediately.
   - **XCom Telemetry:** Passes extracted row counts across tasks and outputs `pipeline_report_<DATE>.txt` directly to `outputs/`.

#### Option B: Standalone Local Simulation Runner (Quick Unit Test)
To test and verify the complete DAG logic locally without running Docker:
```bash
python "solutions/submissions/Akhilesh/03_big_data/3.3_airflow_dag.py"
```
This executes all 5 stages sequentially using an internal mock execution context, validates the DQ gate, performs vectorized transformations, and generates `pipeline_report_<DATE>.txt`.

---

## 8. Pillar 4 — Infrastructure & Governance

### 8.1 Run Data Quality Check Framework
```bash
python "solutions/submissions/Akhilesh/04_infrastructure/4.2_dq_framework.py"
```
**Artifacts Generated:**
Markdown quality reports written to `outputs/results/Akhilesh/04_infrastructure/`:
- `dq_report_projects.md`
- `dq_report_employees.md`
- `dq_report_transactions.md`
- `dq_report_employees_salary_history.md`

### 8.2 Containerized ETL Execution (Task 4.1)

#### Option 1: Docker Compose (Recommended / Task 4.1 Bonus)
Run with automatic volume mounting in a single command:
```bash
docker-compose run etl
# or: docker compose run etl
```

#### Option 2: Standard Docker CLI
* **Windows (PowerShell):**
  ```powershell
  docker build -f "solutions/submissions/Akhilesh/04_infrastructure/4.1_Dockerfile" -t presight-etl:latest .
  docker run --rm -v "${PWD}/outputs:/app/outputs" presight-etl:latest
  ```
* **Windows (Command Prompt):**
  ```cmd
  docker build -f "solutions/submissions/Akhilesh/04_infrastructure/4.1_Dockerfile" -t presight-etl:latest .
  docker run --rm -v "%cd%/outputs:/app/outputs" presight-etl:latest
  ```
* **macOS / Linux:**
  ```bash
  docker build -f "solutions/submissions/Akhilesh/04_infrastructure/4.1_Dockerfile" -t presight-etl:latest .
  docker run --rm -v "$(pwd)/outputs:/app/outputs" presight-etl:latest
  ```

### 8.3 Data Governance Documentation
Review the comprehensive 45-column compliance document (UAE PDPL & GDPR):
- Primary artifact: `outputs/data_governance_document.md`
- Source file: `solutions/submissions/Akhilesh/04_infrastructure/data_governance_document.md` (and `4.3_data_governance_document.md`)

---

## 9. Inspect Generated Outputs

All solution artifacts are organized by pillar:
- `outputs/results/Akhilesh/01_foundations/` (and root `outputs/projects_clean.csv`, `employees_clean.csv`)
- `outputs/results/Akhilesh/02_sql_and_viz/` (and root `outputs/transactions_clean.csv`, `pipeline_summary.txt`, `presight_dashboard.pbix`)
- `outputs/results/Akhilesh/03_big_data/` (and Parquet tables in `spark/`, root `outputs/kafka/summary.json`, `pipeline_report_*.txt`)
- `outputs/results/Akhilesh/04_infrastructure/` (and root `outputs/data_governance_document.md`, `dq_report_*.md`)

---

## 10. Troubleshooting & Common Fixes

| Issue | Root Cause | Solution |
| :--- | :--- | :--- |
| **PowerShell script execution blocked** (`PSSecurityException` / `UnauthorizedAccess`) | Windows PowerShell script execution policy defaults to restricted | In PowerShell, run `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process` before activating, or use Command Prompt (`venv\Scripts\activate.bat`). |
| **Command syntax error on `1.1&1.3_etl_pipeline.py`** | `&` is a reserved shell operator | Always quote the path: `python "solutions/.../1.1&1.3_etl_pipeline.py"`. |
| **`python` command not found on macOS/Linux** | Systems default to `python3` | Use `python3 -m venv venv` and `python3 <script_name>.py`. |
| **Docker volume path resolution error** | Platform syntax differences for current directory | Use `docker-compose run etl` (platform-independent), or use `${PWD}` in PowerShell, `%cd%` in CMD, and `$(pwd)` in macOS/Linux. |
| **DuckDB CLI command not recognized** | Standalone binary not added to PATH | Use the provided inline Python alternative under each SQL step. |

---

## 11. Quick Execution Summary

### Windows (PowerShell)
```powershell
python -m venv venv
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process
venv\Scripts\activate
pip install -r requirements.txt
docker compose up -d

python "solutions/submissions/Akhilesh/01_foundations/1.1&1.3_etl_pipeline.py"
duckdb presight_warehouse.duckdb -f "solutions/submissions/Akhilesh/01_foundations/1.2_data_model.sql"
python "solutions/submissions/Akhilesh/02_sql_and_viz/2.1_etl_full.py"
duckdb presight_warehouse.duckdb -f "solutions/submissions/Akhilesh/02_sql_and_viz/2.2_queries.sql"
duckdb presight_warehouse.duckdb -f "solutions/submissions/Akhilesh/02_sql_and_viz/2.3_Explain.sql"
python "solutions/submissions/Akhilesh/03_big_data/3.1_spark_events_pipeline.py"
python "solutions/submissions/Akhilesh/03_big_data/3.2_kafka_streaming.py" --mode both --max 1000
python "solutions/submissions/Akhilesh/03_big_data/3.3_airflow_dag.py"
python "solutions/submissions/Akhilesh/04_infrastructure/4.2_dq_framework.py"
docker-compose run etl
```

### macOS / Linux (bash / zsh)
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
docker compose up -d

python3 "solutions/submissions/Akhilesh/01_foundations/1.1&1.3_etl_pipeline.py"
duckdb presight_warehouse.duckdb -f "solutions/submissions/Akhilesh/01_foundations/1.2_data_model.sql"
python3 "solutions/submissions/Akhilesh/02_sql_and_viz/2.1_etl_full.py"
duckdb presight_warehouse.duckdb -f "solutions/submissions/Akhilesh/02_sql_and_viz/2.2_queries.sql"
duckdb presight_warehouse.duckdb -f "solutions/submissions/Akhilesh/02_sql_and_viz/2.3_Explain.sql"
python3 "solutions/submissions/Akhilesh/03_big_data/3.1_spark_events_pipeline.py"
python3 "solutions/submissions/Akhilesh/03_big_data/3.2_kafka_streaming.py" --mode both --max 1000
python3 "solutions/submissions/Akhilesh/03_big_data/3.3_airflow_dag.py"
python3 "solutions/submissions/Akhilesh/04_infrastructure/4.2_dq_framework.py"
docker-compose run etl
```
