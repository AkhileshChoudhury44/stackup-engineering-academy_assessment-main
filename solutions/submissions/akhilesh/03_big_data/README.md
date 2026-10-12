# Pillar 3: Big Data Processing

**Candidate:** Akhilesh  
**Assessment:** StackUp Engineering Academy — Data Engineering Assessment

---

## Deliverables in this Directory

This directory contains the distributed big data processing, real-time event streaming, and workflow orchestration solutions for Pillar 3:

1. **`3.1_spark_events_pipeline.py`**: Distributed PySpark batch processing pipeline that ingests all 12 monthly JSONL event files (100,000 events) with an explicit schema, session UTC timezone, 1-to-1 sequential escalation pairing, and writes 5 analytical Parquet tables.
2. **`3.2_kafka_streaming.py`**: Real-time Kafka streaming producer and consumer pipeline with standard 50ms throughput simulation, automated topic provisioning, critical escalation routing, and JSON performance summary generation.
3. **`3.3_airflow_dag.py`**: Production Airflow DAG orchestrating daily ETL in `Asia/Dubai` time (06:00 UAE) with parallel ingestion, hard pre-clean business Data Quality gate, genuine vectorized transformations (no `dropna()` shortcuts), and automated pipeline execution reports.
4. **`README.md`**: Architecture documentation and execution guide.

---

## 1. PySpark Distributed Processing (`3.1_spark_events_pipeline.py`)

### Execution
From the repository root, run via Python or `spark-submit`:
```powershell
python solutions/submissions/Akhilesh/03_big_data/3.1_spark_events_pipeline.py
# or via spark-submit:
spark-submit solutions/submissions/Akhilesh/03_big_data/3.1_spark_events_pipeline.py
```

### Key Engineering Remediations
- **Explicit UTC Session Timezone:** Configured `.config("spark.sql.session.timeZone", "UTC")` in the SparkSession builder, eliminating timezone drift across execution environments.
- **Sequential 1-to-1 Escalation Matching:** Applied `Window.partitionBy("project_id").orderBy(raised_at)` and `Window.partitionBy("project_id").orderBy(resolved_at)` with `row_number()`, ensuring each resolution is paired with exactly one escalation, preventing multiple escalations from claiming the same resolution.
- **Analytical Tables Generated:** Persisted to `outputs/results/Akhilesh/03_big_data/spark/`:
  - `project_activity_summary` (aggregation by project)
  - `user_activity_summary` (user engagement and action metrics)
  - `escalation_log` (lifecycle tracking, partitioned by `severity`)
  - `daily_event_volume` (daily activity, partitioned by `event_date`)
  - `peak_usage_analysis` (hourly utilization and concurrency hotspots)

---

## 2. Kafka Real-time Streaming (`3.2_kafka_streaming.py`)

### Execution
Ensure the Docker background services are running, then run producer and consumer:
```powershell
docker compose up -d
python solutions/submissions/Akhilesh/03_big_data/3.2_kafka_streaming.py --mode both --max 1000
```

### Key Highlights
- **Standard Delay:** Default delay of 50ms (`delay_ms = 50` / `--delay 50`) per Task 3.2b specification to simulate realistic streaming throughput.
- **Critical Topic Routing:** Escalations with severity `'Critical'` are dynamically filtered and routed to `presight.escalations.critical`.
- **Output:** Performance metrics, event-type distributions, and message throughput are written to `outputs/results/Akhilesh/03_big_data/kafka/summary.json`.

---

## 3. Production Airflow Orchestration (`3.3_airflow_dag.py`)

### Architecture & Graph Topology
The pipeline automates daily end-to-end extraction, validation, transformation, and reporting:

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

### Execution & Deployment Options

#### Option A: Within Live Docker Airflow Cluster (Production Mode)
1. **Start Services:** `docker compose up -d`
2. **Access Web UI:** Open [http://localhost:8081](http://localhost:8081) (`admin` / `admin`).
3. **DAG Identification:** Registered as **`presight_etl_pipeline`** (tagged: `presight`, `etl`, `production`, `pillar_3`).
4. **Trigger Manually:**
   - Click the **Play (▶)** button in the Airflow UI, or run via Docker CLI:
     ```powershell
     docker exec presight-airflow-webserver airflow dags trigger presight_etl_pipeline
     ```

#### Option B: Standalone Local Runner (Unit Testing & CI/CD)
To validate the DAG structure and simulate XCom telemetry locally without Docker:
```powershell
python solutions/submissions/Akhilesh/03_big_data/3.3_airflow_dag.py
```

### Key Engineering Features
1. **Gulf Standard Time Scheduling:** Scheduled via `Asia/Dubai` timezone (`0 6 * * *` / 06:00 AM UAE Time, UTC+4), ensuring batch processing completes before business operating hours across UAE offices.
2. **Hard Pre-Clean Data Quality Gate:** Intercepts raw data before any transformations. Verifies:
   - Primary key uniqueness on `project_id`, `employee_id`, and `transaction_id`.
   - Domain enumeration validity on project priorities (`Critical`, `High`, `Medium`, `Low`) and employee levels (`Junior`, `Mid`, `Senior`, `Lead`, `Director`).
   - Numeric range validity ensuring non-negative transaction amounts and positive salaries.
   - Raises an explicit `ValueError` to abort downstream tasks if critical data quality standards fail.
3. **Full Production Transformations:** Executes genuine vectorized business logic (derived budget utilization, risk categories, collision-free corporate email generation) rather than naive `dropna()` shortcuts.
4. **XCom Telemetry & Audit Reporting:** Upstream tasks push extraction volumes and gate statuses to Airflow XCom. The terminal reporting operator pulls these metrics to compile an executive audit summary (`outputs/pipeline_report_<DATE>.txt`).
5. **Fault-Tolerant Retry Policy:** Enforces 2 retries with a 5-minute backoff (`retry_delay = timedelta(minutes=5)`) and a 15-minute execution timeout to handle transient worker bottlenecks.
