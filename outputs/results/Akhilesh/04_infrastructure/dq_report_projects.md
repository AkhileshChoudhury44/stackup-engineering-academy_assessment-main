# Data Quality Report

Checks run: 9
Checks passed: 9
Checks failed: 0

Each row below explains what was tested, the measured value, the expected rule, and whether the row-level check passed.

| check | dataset | passed | failed_rows | details | metric_name | metric_value | expected |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| completeness | projects | True | 443 | Measures how much of the table is populated across all cells. | completeness | 91.95% | At least 90.00% populated |
| primary_key:project_id | projects | True | 0 | Checks that the business key is present once per row. | null_or_duplicate_rows_in_project_id | 0 | Zero nulls and zero duplicates |
| numeric_range:budget | projects | True | 0 | Checks whether numeric values fall inside the expected business range. | budget | 0..10000000 | Values between 0 and 10000000 |
| numeric_range:actual_cost | projects | True | 0 | Checks whether numeric values fall inside the expected business range. | actual_cost | 0..10000000 | Values between 0 and 10000000 |
| valid_date:start_date | projects | True | 0 | Checks whether the date can be parsed correctly. | start_date | 435 valid / 0 invalid | Valid date values |
| valid_date:end_date | projects | True | 0 | Checks whether the date can be parsed correctly. | end_date | 214 valid / 0 invalid | Valid date values |
| before:start_date:end_date | projects | True | 0 | Checks that the first date happens before the second date. | start_date <= end_date | 0 bad rows | start_date must be on or before end_date |
| non_negative:actual_cost | projects | True | 0 | Checks that values are not below zero. | actual_cost | 0 negative rows | Zero negative values |
| foreign_key:project_manager_id | projects | True | 0 | Checks whether the reference key exists in the related table. | project_manager_id | 0 missing rows | Match values in employee_id |
