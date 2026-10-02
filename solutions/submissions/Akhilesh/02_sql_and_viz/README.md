# Pillar 2: SQL and Data Visualisation

**Candidate:** Akhilesh  
**Assessment:** StackUp Engineering Academy — Data Engineering Assessment

---

## Deliverables in this Directory

This directory contains the full-scale transaction ETL, analytical SQL queries, query optimisation analysis, and business intelligence dashboard for Pillar 2:

1. **`2.1_etl_full.py`**: High-performance vectorized ETL pipeline processing 50,000 transaction records, dimensional enrichment, and audit generation in < 1.5 seconds.
2. **`2.2_queries.sql`**: Complete ANSI SQL / DuckDB script generating a query-ready star schema and answering all 6 core business questions with exact thresholds, SCD2 temporal handling, and zero-division guards.
3. **`2.3_Explain.sql`**: Production query optimisation analysis with realistic `EXPLAIN ANALYZE` bottleneck breakdowns, CTE scalar isolation, duplicate removal (`is_current = TRUE`), and index recommendations.
4. **`2.4_Dashboard.pbix`**: Power BI business intelligence dashboard covering project budget vs. spend, manager workload, vendor risk, and cash flow trends.
5. **`README.md`**: Execution guide and architecture documentation.

---

## 1. Input Datasets

The ETL and SQL scripts read directly from the assessment `datasets/` directory:
- `datasets/projects.csv` (500 projects)
- `datasets/employees.csv` (1,000 employees)
- `datasets/employees_salary_history.csv` (1,826 historical compensation records)
- `datasets/transactions.json` (50,000 financial transactions)

---

## 2. Full-Scale Transaction ETL (`2.1_etl_full.py`)

### Execution
From the assessment repository root, execute:
```bash
python "solutions/submissions/Akhilesh/02_sql_and_viz/2.1_etl_full.py"
```

### Key Engineering Decisions & Audit Rules
- **Vectorized Parsing:** Ingests 50,000 JSON records using `pd.json_normalize` with vectorized processing completing in < 1.5s (SLA limit: 30s).
- **Amount Imputation:** Missing or invalid financial amounts are defaulted to `0.0` in `amount_aed` to prevent NaN propagation across aggregate calculations.
- **Approver Handling:** Unassigned or missing approver IDs are preserved as `is_approved = False` and labeled as `"Unassigned / Pending"`.
- **Dimensional Enrichment:** Left joins against `dim_project` (`project_id`, `project_name`, `department`) and `dim_employee` (`approver_full_name`), with validation ensuring transaction cardinality is strictly preserved at 50,000 rows.
- **Output:** Writes all three cleaned datasets (`projects_clean.csv`, `employees_clean.csv`, `transactions_clean.csv`) and `pipeline_summary.txt` (with before/after row counts, DQ decisions, and execution times) to `outputs/results/Akhilesh/02_sql_and_viz/` and mirrors them directly to `outputs/`.

---

## 3. Core Business Queries (`2.2_queries.sql`)

### Execution
Run directly via the DuckDB CLI (persisting tables to warehouse file):
```bash
duckdb presight_warehouse.duckdb -f "solutions/submissions/Akhilesh/02_sql_and_viz/2.2_queries.sql"
```

*Or run via Python in-memory:*
```bash
python -c "import duckdb; con=duckdb.connect('presight_warehouse.duckdb'); con.execute(open('solutions/submissions/Akhilesh/02_sql_and_viz/2.2_queries.sql', encoding='utf-8').read()); print('All queries executed successfully!')"
```

### Questions Addressed
1. **Q1 — Department Budget Performance:** Identifies departments spending > 90% of budget or over budget using `HAVING` and zero-division protection.
2. **Q2 — Project Manager Workload:** Identifies active project managers overseeing $\ge 3$ active projects, strictly filtered on `dim_employee.is_current = TRUE` to avoid duplicate historical representations.
3. **Q3 — Vendor Concentration Risk:** Calculates vendor spend percentage using window aggregation `100.0 * sum(amount) / sum(sum(amount)) OVER ()`, flagging vendors exceeding 5% (`MEDIUM`) and 10% (`HIGH`).
4. **Q4 — Projects with Open Financial Issues:** Identifies projects with `'Pending'` or `'Disputed'` transaction totals exceeding 50,000 AED.
5. **Q5 — Monthly Spend Trends:** Calculates monthly category spend, cumulative running totals via `SUM() OVER (PARTITION BY category ORDER BY year_month)`, and month-over-month % change via `LAG()`.
6. **Q6 — Employee Salary Progression:** Evaluates SCD Type 2 employee history to identify the top 20 single salary increases in AED using `LAG(salary) OVER (PARTITION BY employee_id ORDER BY valid_from)`.

---

## 4. Query Optimisation & Explain Plan (`2.3_Explain.sql`)

### Execution
> **Prerequisite:** Ensure `2.2_queries.sql` has been executed once to populate the star schema tables.

Run via DuckDB CLI:
```bash
duckdb presight_warehouse.duckdb -f "solutions/submissions/Akhilesh/02_sql_and_viz/2.3_Explain.sql"
```

*Or run via Python:*
```bash
python -c "import duckdb; con=duckdb.connect('presight_warehouse.duckdb'); con.execute(open('solutions/submissions/Akhilesh/02_sql_and_viz/2.3_Explain.sql', encoding='utf-8').read()); print('Explain benchmarks executed successfully!')"
```

### Bottleneck Analysis (`EXPLAIN ANALYZE`)
The baseline unoptimised query suffered from:
1. **Redundant Sequential Scans:** `fact_transactions` (50,000 rows) was scanned twice—once for the unindexed subquery computing average amount, and once for the main join.
2. **Cartesian Product Duplication:** Joining `dim_employee` without filtering on `is_current = TRUE` joined transactions across multiple historical versions of each manager, inflating results from 2,127 true transactions to 6,088 duplicate rows.
3. **Implicit Comma-Joins:** Comma-separated join syntax created cross products before predicate pushdown.

### Optimization Strategy
1. **CTE Isolation:** Filtered `'Pending'` transactions once into a CTE `pending_transactions`, calculating the scalar average once in `pending_average`.
2. **Cardinality Correction:** Added `AND e.is_current = TRUE` on the manager join, eliminating duplicate rows and returning the exact 2,127 records.
3. **Targeted Composite Indexing:** Recommended `idx_fact_payment_project (payment_status, project_key)` and `idx_employee_current (employee_id, is_current)` for row-oriented engines.
4. **Realistic Performance Gains:** Execution latency improved from ~18.0 ms to ~12.0 ms (~1.5x speedup in memory), removing 3,961 phantom duplicate rows.

---

## 5. Power BI Dashboard (`2.4_Dashboard.pbix`)

The interactive Power BI dashboard connects to the cleaned star schema to provide executive visibility across:
- **Executive KPI Cards:** Total Actual Spend, Active Project Count, Budget Utilization Rate, and Disputed Volume.
- **Departmental Budget Variance:** Spend vs. Allocated Budget by Department.
- **Vendor Concentration Matrix:** Exposure across top vendors with risk categorization.
- **Cash Flow Run Rate:** Monthly financial disbursements segmented by expense category.
