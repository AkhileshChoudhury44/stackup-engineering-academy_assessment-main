# Data Quality Report

Checks run: 8
Checks passed: 5
Checks failed: 3

Each row below explains what was tested, the measured value, the expected rule, and whether the row-level check passed.

| check | dataset | passed | failed_rows | details | metric_name | metric_value | expected |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| completeness | employees | True | 10 | Measures how much of the table is populated across all cells. | completeness | 99.92% | At least 85.00% populated |
| primary_key:employee_id | employees | True | 0 | Checks that the business key is present once per row. | null_or_duplicate_rows_in_employee_id | 0 | Zero nulls and zero duplicates |
| numeric_range:salary | employees | True | 0 | Checks whether numeric values fall inside the expected business range. | salary | 1000..250000 | Values between 1000 and 250000 |
| numeric_range:years_experience | employees | False | 5 | Checks whether numeric values fall inside the expected business range. | years_experience | 0..50 | Values between 0 and 50 |
| valid_date:hire_date | employees | False | 8 | Checks whether the date can be parsed correctly. | hire_date | 992 valid / 8 invalid | Valid date values |
| non_negative:years_experience | employees | False | 5 | Checks that values are not below zero. | years_experience | 5 negative rows | Zero negative values |
| permitted_values:level | employees | True | 0 | Checks that values belong to the allowed category list ['Junior', 'Mid', 'Senior', 'Lead', 'Director']. | level | 0 invalid values | Values in ['Junior', 'Mid', 'Senior', 'Lead', 'Director'] |
| permitted_values:status | employees | True | 0 | Checks that values belong to the allowed category list ['Active', 'Inactive', 'On Leave', 'Terminated']. | status | 0 invalid values | Values in ['Active', 'Inactive', 'On Leave', 'Terminated'] |
