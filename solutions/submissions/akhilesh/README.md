# StackUp Engineering Academy — Data Engineering Assessment
## Trainee Submission Dossier

**Candidate:** Akhilesh  
**Learning Pathway:** Fullstack Data Engineering  
**Submission Directory:** `solutions/submissions/Akhilesh/`  
**Target Database:** DuckDB / PostgreSQL / PySpark / Apache Kafka / Apache Airflow / Docker  
**Master Execution Guide:** [RUN_INSTRUCTIONS.md](../../../RUN_INSTRUCTIONS.md)  
**Detailed Architectural Dossier:** [SUBMISSION_NOTES.md](SUBMISSION_NOTES.md)

---

## 📁 Repository Structure & Deliverables

All solutions have been structured strictly into their designated pillar directories:

```text
solutions/submissions/Akhilesh/
│
├── SUBMISSION_NOTES.md              # Complete architectural decisions & audit notes
├── README.md                        # Master submission index
│
├── 01_foundations/                  # Pillar 1 — Foundations
│   ├── 1.1&1.3_etl_pipeline.py      # Tasks 1.1 & 1.3: Projects & Employees Vectorized ETL
│   ├── 1.2_data_model.sql           # Task 1.2: Star Schema DDL & SCD Type 2 History
│   ├── notebooks/exploration.ipynb  # Exploratory Data Analysis (EDA)
│   └── README.md                    # Pillar 1 Architecture & Execution Guide
│
├── 02_sql_and_viz/                  # Pillar 2 — SQL & Data Visualization
│   ├── 2.1_etl_full.py              # Task 2.2: Full-Scale Transaction ETL (50K in ~1.5s)
│   ├── 2.2_queries.sql              # Task 2.1: Six Business Queries with Window Functions
│   ├── 2.3_Explain.sql              # Task 2.3: Query Optimisation & EXPLAIN ANALYZE
│   ├── 2.4_Dashboard.pbix           # Task 2.4: Power BI Executive Dashboard
│   └── README.md                    # Pillar 2 Architecture & Execution Guide
│
├── 03_big_data/                     # Pillar 3 — Big Data Processing
│   ├── 3.1_spark_events_pipeline.py # Task 3.1: PySpark Event Processing (100K JSONL → Parquet)
│   ├── 3.2_kafka_streaming.py       # Task 3.2: Kafka Real-Time Ingestion & Critical Routing
│   ├── 3.3_airflow_dag.py           # Task 3.3: Production Airflow DAG (Dubai Timezone)
│   └── README.md                    # Pillar 3 Architecture & Execution Guide
│
└── 04_infrastructure/               # Pillar 4 — Infrastructure & Governance
    ├── 4.1_Dockerfile               # Task 4.1: Production Multi-Stage Container (< 250MB)
    ├── 4.2_dq_framework.py          # Task 4.3: Extensible 8-Dimension Data Quality Framework
    ├── 4.3_data_governance_document.md # Task 4.2: 45-Column Compliance Catalog (UAE PDPL & GDPR)
    ├── requirements.txt             # Pinned Container Runtime Dependencies
    └── README.md                    # Pillar 4 Architecture & Execution Guide
```

---

## 🎯 Verification & Deliverables Matrix

| Pillar | Task | Description | Output Artifact Location | Status |
| :--- | :--- | :--- | :--- | :---: |
| **Pillar 1** | **1.1** | Projects cleaning with 7 derived columns | `outputs/results/Akhilesh/01_foundations/projects_clean.csv` | **Complete** |
| | **1.2** | Star schema & SCD Type 2 employee history | `presight_warehouse.duckdb` (Star Schema Tables) | **Complete** |
| | **1.3** | Employees DQ audit & collision-free email remediation | `outputs/results/Akhilesh/01_foundations/employees_clean.csv` | **Complete** |
| **Pillar 2** | **2.1** | Six analytical business queries with window functions | Formatted Query Output in `outputs/` | **Complete** |
| | **2.2** | 50K transaction ETL pipeline (< 30s SLA) | All 3 Clean CSVs & `pipeline_summary.txt` in `outputs/` | **Complete** |
| | **2.3** | EXPLAIN ANALYZE bottleneck analysis & 20.8x optimization | Plan & Composite Indexes in `2.3_Explain.sql` | **Complete** |
| | **2.4** | Power BI Executive Spend Dashboard | `solutions/submissions/Akhilesh/02_sql_and_viz/2.4_Dashboard.pbix` | **Complete** |
| **Pillar 3** | **3.1** | PySpark distributed pipeline (100K events → 5 Parquet tables) | `outputs/results/Akhilesh/03_big_data/spark/` | **Complete** |
| | **3.2** | Real-time Kafka streaming (50ms delay, critical routing) | `outputs/results/Akhilesh/03_big_data/kafka/summary.json` | **Complete** |
| | **3.3** | Production Airflow DAG with business DQ gate | `3.3_airflow_dag.py` | **Complete** |
| **Pillar 4** | **4.1** | Multi-stage Dockerfile (< 250MB) & Compose Override | `4.1_Dockerfile` & `docker-compose.override.yml` | **Complete** |
| | **4.2** | Enterprise Data Governance Document (45 columns) | `outputs/data_governance_document.md` | **Complete** |
| | **4.3** | Configurable Data Quality Framework (8 dimensions) | `outputs/results/Akhilesh/04_infrastructure/` | **Complete** |

---

## 🚀 Quick Execution Guide

For detailed OS-specific execution instructions across Windows PowerShell, Command Prompt, and macOS/Linux, refer directly to:
👉 **[RUN_INSTRUCTIONS.md](../../../RUN_INSTRUCTIONS.md)**

### Fast Verification Commands (Windows PowerShell)

```powershell
# 1. Activate Virtual Environment
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process
venv\Scripts\activate

# 2. Run Pillar 1 (Foundations ETL & Data Model)
python "solutions/submissions/Akhilesh/01_foundations/1.1&1.3_etl_pipeline.py"
duckdb presight_warehouse.duckdb -f "solutions/submissions/Akhilesh/01_foundations/1.2_data_model.sql"

# 3. Run Pillar 2 (Transactions ETL, Queries, & Query Optimization)
python "solutions/submissions/Akhilesh/02_sql_and_viz/2.1_etl_full.py"
duckdb presight_warehouse.duckdb -f "solutions/submissions/Akhilesh/02_sql_and_viz/2.2_queries.sql"
duckdb presight_warehouse.duckdb -f "solutions/submissions/Akhilesh/02_sql_and_viz/2.3_Explain.sql"

# 4. Run Pillar 3 (Big Data Spark, Kafka, & Airflow Pipelines)
python "solutions/submissions/Akhilesh/03_big_data/3.1_spark_events_pipeline.py"
python "solutions/submissions/Akhilesh/03_big_data/3.2_kafka_streaming.py" --mode both --max 1000
python "solutions/submissions/Akhilesh/03_big_data/3.3_airflow_dag.py"
# or trigger in live Airflow: docker exec presight-airflow-webserver airflow dags trigger presight_etl_pipeline

# 5. Run Pillar 4 (Data Quality Framework & Container Run)
python "solutions/submissions/Akhilesh/04_infrastructure/4.2_dq_framework.py"
docker-compose run etl
```
