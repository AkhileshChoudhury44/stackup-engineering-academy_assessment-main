-- =============================================================
-- StackUp Engineering Academy — Data Engineering Assessment
-- Pillar 1: Foundations — Task 1.2: Star Schema with SCD Type 2
-- Trainee: Akhilesh
-- Database target: DuckDB (fully ANSI-SQL compliant for PostgreSQL / SQLite)
-- File: 1.2_data_model.sql
-- =============================================================

-- ===========================================================================
-- SECTION 1 — Star Schema DDL Design
-- ===========================================================================

-- Clean reset of warehouse objects to guarantee proper PRIMARY KEY catalog registration
DROP TABLE IF EXISTS fact_transactions CASCADE;
DROP TABLE IF EXISTS bridge_employee_project CASCADE;
DROP TABLE IF EXISTS dim_project CASCADE;
DROP TABLE IF EXISTS dim_employee CASCADE;
DROP TABLE IF EXISTS dim_vendor CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;

-- ---------------------------------------------------------------------------
-- 1. dim_date
-- Design Rationale:
-- Pre-calculating calendar attributes in a dedicated date dimension eliminates
-- expensive date math, timezone conversions, and week/month formatting during
-- runtime analytical queries. It is joined to facts via integer date keys (YYYYMMDD).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY,               -- Integer format YYYYMMDD (e.g., 20240115)
    full_date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    week INT NOT NULL,
    day INT NOT NULL,
    day_of_week VARCHAR(15) NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- ---------------------------------------------------------------------------
-- 2. dim_project
-- Design Rationale:
-- Preserves natural project_id from source systems while creating an integer
-- surrogate key (project_key). Stores slowly-changing project metadata,
-- budget, actual costs, status, and computed risk indicators.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_project (
    project_key INTEGER PRIMARY KEY,        -- Surrogate key
    project_id VARCHAR(20) NOT NULL UNIQUE, -- Natural key from projects.csv
    project_name VARCHAR(255) NOT NULL,
    department VARCHAR(100) NOT NULL,
    status VARCHAR(50) NOT NULL,
    status_category VARCHAR(50) NOT NULL,   -- Active, Closed, Pending
    start_date DATE,
    end_date DATE,
    duration_days INTEGER,
    budget NUMERIC(15, 2) DEFAULT 0.0,
    actual_cost NUMERIC(15, 2) DEFAULT 0.0,
    budget_variance NUMERIC(15, 2) DEFAULT 0.0,
    is_over_budget BOOLEAN DEFAULT FALSE,
    budget_utilisation_pct NUMERIC(6, 2) DEFAULT 0.0,
    risk_level VARCHAR(20) DEFAULT 'Low',   -- High, Medium, Low
    priority VARCHAR(50),
    region VARCHAR(100),
    project_manager_id VARCHAR(20)          -- FK linking to dim_employee(employee_id)
);

-- ---------------------------------------------------------------------------
-- 3. dim_employee (Slowly Changing Dimension Type 2)
-- Design Rationale:
-- Implements SCD Type 2 to preserve employee salary, role, level, and department
-- progression over time without destroying history.
-- - employee_key: Surrogate key uniquely identifying each historical snapshot.
-- - employee_id: Natural key identifying the person across versions.
-- - valid_from / valid_to: Closed-open date intervals [valid_from, valid_to).
-- - sentinel valid_to ('9999-12-31') and is_current = TRUE for active records.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_employee (
    employee_key INTEGER PRIMARY KEY,       -- Surrogate key for each version
    employee_id VARCHAR(20) NOT NULL,       -- Natural key
    full_name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    department VARCHAR(100) NOT NULL,
    role VARCHAR(100) NOT NULL,
    level VARCHAR(50) NOT NULL,             -- Junior, Mid, Senior, Lead, Director
    salary NUMERIC(12, 2) NOT NULL,
    manager_id VARCHAR(20),
    region VARCHAR(100),
    years_experience INT,
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL DEFAULT '9999-12-31',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    change_reason VARCHAR(255) DEFAULT 'Current Snapshot'
);

-- Index for fast SCD2 lookups by natural key and active flag
CREATE INDEX IF NOT EXISTS idx_dim_employee_current ON dim_employee (employee_id, is_current);

-- ---------------------------------------------------------------------------
-- 4. dim_vendor
-- Design Rationale:
-- Extracted from transaction records to conform vendor identity across projects.
-- Allows concentration risk and spending aggregation at the vendor level.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_vendor (
    vendor_key INTEGER PRIMARY KEY,         -- Surrogate key
    vendor_id VARCHAR(20) NOT NULL UNIQUE,  -- Natural key
    vendor_name VARCHAR(255) NOT NULL
);

-- ---------------------------------------------------------------------------
-- 5. bridge_employee_project
-- Design Rationale:
-- Handles the many-to-many relationship between projects and team members/managers.
-- Decouples project management and participation from the core fact grain.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bridge_employee_project (
    bridge_key INTEGER PRIMARY KEY,
    project_key INT NOT NULL REFERENCES dim_project(project_key),
    employee_key INT NOT NULL REFERENCES dim_employee(employee_key),
    role_in_project VARCHAR(100) DEFAULT 'Team Member',
    allocation_pct NUMERIC(5, 2) DEFAULT 100.0
);

-- ---------------------------------------------------------------------------
-- 6. fact_transactions
-- Design Rationale:
-- Central transactional grain fact table. Contains numerical measures (amount,
-- amount_aed) and foreign keys to all dimensions. Enables drill-down by date,
-- department, vendor, project, and employee.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_key INTEGER PRIMARY KEY,    -- Surrogate key
    transaction_id VARCHAR(50) NOT NULL UNIQUE,
    project_key INT NOT NULL REFERENCES dim_project(project_key),
    employee_key INT NOT NULL REFERENCES dim_employee(employee_key), -- approver
    vendor_key INT NOT NULL REFERENCES dim_vendor(vendor_key),
    date_key INT NOT NULL REFERENCES dim_date(date_key),
    amount NUMERIC(15, 2) NOT NULL,
    currency VARCHAR(10) DEFAULT 'AED',
    amount_aed NUMERIC(15, 2) NOT NULL,
    category VARCHAR(100) NOT NULL,
    payment_status VARCHAR(50) NOT NULL,    -- Paid, Pending, Disputed, Cancelled
    invoice_ref VARCHAR(100),
    is_approved BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_fact_txn_project ON fact_transactions(project_key);
CREATE INDEX IF NOT EXISTS idx_fact_txn_date ON fact_transactions(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_txn_vendor ON fact_transactions(vendor_key);


-- ===========================================================================
-- SECTION 2 — Data Ingestion & Pure SQL SCD Type 2 Population
-- ===========================================================================

-- 1. Populate dim_date (2020 through 2026)
INSERT INTO dim_date
SELECT
    CAST(strftime(d, '%Y%m%d') AS INT) AS date_key,
    CAST(d AS DATE) AS full_date,
    EXTRACT(year FROM d) AS year,
    EXTRACT(quarter FROM d) AS quarter,
    EXTRACT(month FROM d) AS month,
    strftime(d, '%B') AS month_name,
    EXTRACT(week FROM d) AS week,
    EXTRACT(day FROM d) AS day,
    strftime(d, '%A') AS day_of_week,
    CASE WHEN EXTRACT(dayofweek FROM d) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM (
    SELECT UNNEST(generate_series(DATE '2020-01-01', DATE '2026-12-31', INTERVAL '1 DAY')) AS d
) cal
ON CONFLICT (date_key) DO NOTHING;

-- 2. Populate dim_project
INSERT INTO dim_project
SELECT
    ROW_NUMBER() OVER () AS project_key,
    project_id,
    project_name,
    department,
    TRIM(status) AS status,
    CASE 
        WHEN TRIM(status) = 'In Progress' THEN 'Active'
        WHEN TRIM(status) = 'Completed' THEN 'Closed'
        ELSE 'Pending'
    END AS status_category,
    TRY_CAST(start_date AS DATE) AS start_date,
    TRY_CAST(end_date AS DATE) AS end_date,
    DATEDIFF('day', TRY_CAST(start_date AS DATE), TRY_CAST(end_date AS DATE)) AS duration_days,
    COALESCE(TRY_CAST(budget AS NUMERIC(15,2)), 0.0) AS budget,
    COALESCE(TRY_CAST(actual_cost AS NUMERIC(15,2)), 0.0) AS actual_cost,
    COALESCE(TRY_CAST(actual_cost AS NUMERIC(15,2)), 0.0) - COALESCE(TRY_CAST(budget AS NUMERIC(15,2)), 0.0) AS budget_variance,
    COALESCE(TRY_CAST(actual_cost AS NUMERIC(15,2)), 0.0) > COALESCE(TRY_CAST(budget AS NUMERIC(15,2)), 0.0) AS is_over_budget,
    CASE 
        WHEN COALESCE(TRY_CAST(budget AS NUMERIC(15,2)), 0.0) > 0 
        THEN ROUND((COALESCE(TRY_CAST(actual_cost AS NUMERIC(15,2)), 0.0) / TRY_CAST(budget AS NUMERIC(15,2))) * 100.0, 2)
        ELSE 0.0 
    END AS budget_utilisation_pct,
    CASE 
        WHEN priority = 'Critical' OR COALESCE(TRY_CAST(actual_cost AS NUMERIC(15,2)), 0.0) > COALESCE(TRY_CAST(budget AS NUMERIC(15,2)), 0.0) THEN 'High'
        WHEN priority = 'High' OR (COALESCE(TRY_CAST(budget AS NUMERIC(15,2)), 0.0) > 0 AND (COALESCE(TRY_CAST(actual_cost AS NUMERIC(15,2)), 0.0) / TRY_CAST(budget AS NUMERIC(15,2))) * 100.0 > 90.0) THEN 'Medium'
        ELSE 'Low'
    END AS risk_level,
    priority,
    region,
    project_manager_id
FROM read_csv_auto('datasets/projects.csv')
ON CONFLICT (project_id) DO NOTHING;

-- 3. Populate dim_employee with Pure SQL SCD Type 2 History
-- Combines historical snapshots from employees_salary_history.csv with current state from employees.csv
WITH history_versions AS (
    SELECT 
        h.employee_id, 
        COALESCE(e.full_name, 'Staff Member') AS full_name, 
        COALESCE(e.email, 'unknown@presight.ai') AS email, 
        e.department,
        h.new_role AS role, 
        h.new_level AS level, 
        CAST(h.new_salary AS NUMERIC(12,2)) AS salary,
        e.manager_id, 
        COALESCE(e.region, 'Abu Dhabi') AS region, 
        COALESCE(TRY_CAST(e.years_experience AS INT), 0) AS years_experience, 
        CAST(h.effective_date AS DATE) AS valid_from,
        COALESCE(h.change_reason, 'Historical Revision') AS change_reason, 
        FALSE AS is_current
    FROM read_csv_auto('datasets/employees_salary_history.csv') h 
    JOIN read_csv_auto('datasets/employees.csv') e USING (employee_id)
),
latest_history AS (
    SELECT 
        employee_id, 
        MAX(CAST(effective_date AS DATE)) AS latest_effective_date
    FROM read_csv_auto('datasets/employees_salary_history.csv') 
    GROUP BY employee_id
),
current_versions AS (
    SELECT 
        e.employee_id, 
        e.full_name, 
        e.email, 
        e.department, 
        e.role, 
        e.level,
        CAST(e.salary AS NUMERIC(12,2)) AS salary, 
        e.manager_id, 
        e.region, 
        ABS(COALESCE(TRY_CAST(e.years_experience AS INT), 0)) AS years_experience,
        COALESCE(h.latest_effective_date + INTERVAL '1 DAY', TRY_CAST(e.hire_date AS DATE), DATE '2020-01-01') AS valid_from,
        'Current Position' AS change_reason, 
        TRUE AS is_current
    FROM read_csv_auto('datasets/employees.csv') e 
    LEFT JOIN latest_history h USING (employee_id)
),
versions AS (
    SELECT * FROM history_versions 
    UNION ALL 
    SELECT * FROM current_versions
),
dated AS (
    SELECT 
        *, 
        LEAD(valid_from) OVER (PARTITION BY employee_id ORDER BY valid_from, is_current) AS next_valid_from
    FROM versions
)
INSERT INTO dim_employee
SELECT 
    ROW_NUMBER() OVER (ORDER BY employee_id, valid_from, is_current) AS employee_key,
    employee_id, 
    full_name, 
    email, 
    department, 
    role, 
    level, 
    salary, 
    manager_id,
    region, 
    years_experience, 
    valid_from,
    COALESCE(next_valid_from, DATE '9999-12-31') AS valid_to,
    next_valid_from IS NULL AS is_current, 
    change_reason
FROM dated
ON CONFLICT (employee_key) DO NOTHING;

-- 4. Populate dim_vendor
INSERT INTO dim_vendor
SELECT
    ROW_NUMBER() OVER () AS vendor_key,
    vendor_id,
    vendor_name
FROM (
    SELECT DISTINCT vendor_id, vendor_name
    FROM read_json_auto('datasets/transactions.json')
    WHERE vendor_id IS NOT NULL
) v
ON CONFLICT (vendor_id) DO NOTHING;

-- 5. Populate bridge_employee_project
INSERT INTO bridge_employee_project
SELECT
    ROW_NUMBER() OVER () AS bridge_key,
    p.project_key,
    e.employee_key,
    'Project Manager' AS role_in_project,
    100.0 AS allocation_pct
FROM dim_project p
INNER JOIN dim_employee e
    ON p.project_manager_id = e.employee_id
    AND e.is_current = TRUE
ON CONFLICT (bridge_key) DO NOTHING;

-- 6. Populate fact_transactions
-- Critical Architectural Fix: Join dim_employee using [valid_from, valid_to) closed-open interval.
-- NEVER use BETWEEN on end-exclusive date intervals, which caused 29 duplicated records.
INSERT INTO fact_transactions
SELECT
    ROW_NUMBER() OVER () AS transaction_key,
    t.transaction_id,
    p.project_key,
    COALESCE(e.employee_key, 1) AS employee_key,
    v.vendor_key,
    d.date_key,
    COALESCE(TRY_CAST(t.amount AS NUMERIC(15,2)), 0.0) AS amount,
    COALESCE(t.currency, 'AED') AS currency,
    COALESCE(TRY_CAST(t.amount AS NUMERIC(15,2)), 0.0) AS amount_aed,
    t.category,
    t.payment_status,
    t.invoice_ref,
    CASE WHEN t.approved_by IS NOT NULL AND TRIM(t.approved_by) NOT IN ('', 'None', 'nan') THEN TRUE ELSE FALSE END AS is_approved
FROM read_json_auto('datasets/transactions.json') t
INNER JOIN dim_project p 
    ON t.project_id = p.project_id
INNER JOIN dim_vendor v 
    ON t.vendor_id = v.vendor_id
INNER JOIN dim_date d 
    ON TRY_CAST(t.transaction_date AS DATE) = d.full_date
LEFT JOIN dim_employee e
    ON t.approved_by = e.employee_id
    AND TRY_CAST(t.transaction_date AS DATE) >= e.valid_from
    AND TRY_CAST(t.transaction_date AS DATE) < e.valid_to
ON CONFLICT (transaction_id) DO NOTHING;


-- ===========================================================================
-- SECTION 3 — SCD Type 2 Validation Queries (Task 1.2 Verification)
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- Q1: Verify integrity — No employee has more than one current record
-- EXPECTED: Zero rows returned
-- ---------------------------------------------------------------------------
SELECT employee_id, COUNT(*) AS current_count
FROM dim_employee
WHERE is_current = TRUE
GROUP BY employee_id
HAVING COUNT(*) > 1;

-- ---------------------------------------------------------------------------
-- Q2: Show employees with version history
-- EXPECTED: Multiple employees with 2 to 5 versions
-- ---------------------------------------------------------------------------
SELECT employee_id, COUNT(*) AS version_count, MIN(valid_from) AS first_start, MAX(valid_to) AS latest_end
FROM dim_employee
GROUP BY employee_id
ORDER BY version_count DESC
LIMIT 10;

-- ---------------------------------------------------------------------------
-- Q3: Self-join to detect any overlapping periods per employee
-- Integrity check: Interval A overlaps with B if (A.start < B.end) AND (A.end > B.start)
-- EXPECTED: Zero rows returned
-- ---------------------------------------------------------------------------
SELECT
    a.employee_id,
    a.employee_key AS ver_1_key,
    b.employee_key AS ver_2_key,
    a.valid_from   AS ver_1_from,
    a.valid_to     AS ver_1_to,
    b.valid_from   AS ver_2_from,
    b.valid_to     AS ver_2_to
FROM dim_employee a
INNER JOIN dim_employee b
    ON a.employee_id = b.employee_id
    AND a.employee_key < b.employee_key
WHERE a.valid_from < b.valid_to
  AND a.valid_to > b.valid_from;
