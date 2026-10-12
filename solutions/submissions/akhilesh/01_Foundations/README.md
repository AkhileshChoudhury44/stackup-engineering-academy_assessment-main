# Pillar 1 - Foundations

This folder contains the foundational ETL and data modelling work for the assessment.

## Files

- `1.1&1.3_etl_pipeline.py` cleans and transforms the projects and employees datasets with vectorized Pandas operations and explicit DQ audit flags.
- `1.2_data_model.sql` creates a DuckDB star schema with dimensions, facts, bridge table, and a pure SQL SCD Type 2 employee dimension.
- `notebooks/exploration.ipynb` contains the exploratory data analysis notebook.

## Input Data

The ETL expects the assessment datasets folder to contain:

- `datasets/projects.csv`
- `datasets/employees.csv`

The SQL model also expects:

- `datasets/employees_salary_history.csv`
- `datasets/transactions.json`

## How to Run the Python ETL

Run this from the repository root:

```powershell
python "solutions/submissions/Akhilesh/01_foundations/1.1&1.3_etl_pipeline.py"
```

The script automatically searches upward for the repository root by finding the `datasets` folder.

Optional environment variables:

- `DATA_DIR` can point to a custom datasets folder.
- `OUTPUT_DIR` can point to a custom output folder.

Example:

```powershell
$env:DATA_DIR="datasets"
$env:OUTPUT_DIR="outputs/results/Akhilesh/01_foundations"
python "solutions/submissions/Akhilesh/01_foundations/1.1&1.3_etl_pipeline.py"
```

## Python ETL Outputs

By default, the cleaned files are written to:

```text
outputs/results/Akhilesh/01_foundations/
```

Expected outputs:

- `projects_clean.csv`
- `employees_clean.csv`
- `data_quality_report.txt`

## How to Run the SQL Model

Open `presight_warehouse.duckdb` in the DuckDB VS Code extension, or run from terminal:

```powershell
duckdb presight_warehouse.duckdb -f "solutions/submissions/Akhilesh/01_foundations/1.2_data_model.sql"
```

## SCD Type 2 Design

The `dim_employee` table stores employee history instead of overwriting old values. When an employee's salary, role, or level changes, a new row is created with a new `valid_from` date. The previous row is closed using `valid_to`, and only the newest row has `is_current = TRUE`.

This allows analysis to answer both current-state questions and historical questions, with closed-open joins (`>= valid_from AND < valid_to`) ensuring zero transaction duplication.
