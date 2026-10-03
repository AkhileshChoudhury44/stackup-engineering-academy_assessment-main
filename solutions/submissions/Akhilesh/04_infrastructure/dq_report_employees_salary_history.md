# Data Quality Report

Checks run: 7
Checks passed: 7
Checks failed: 0

Each row below explains what was tested, the measured value, the expected rule, and whether the row-level check passed.

| check | dataset | passed | failed_rows | details | metric_name | metric_value | expected |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| completeness | employees_salary_history | True | 1782 | Measures how much of the table is populated across all cells. | completeness | 90.24% | At least 85.00% populated |
| numeric_range:new_salary | employees_salary_history | True | 0 | Checks whether numeric values fall inside the expected business range. | new_salary | 1000..250000 | Values between 1000 and 250000 |
| valid_date:effective_date | employees_salary_history | True | 0 | Checks whether the date can be parsed correctly. | effective_date | 1826 valid / 0 invalid | Valid date values |
| non_negative:new_salary | employees_salary_history | True | 0 | Checks that values are not below zero. | new_salary | 0 negative rows | Zero negative values |
| foreign_key:employee_id | employees_salary_history | True | 0 | Checks whether the reference key exists in the related table. | employee_id | 0 missing rows | Match values in employee_id |
| permitted_values:new_level | employees_salary_history | True | 0 | Checks that values belong to the allowed category list ['Junior', 'Mid', 'Senior', 'Lead', 'Director']. | new_level | 0 invalid values | Values in ['Junior', 'Mid', 'Senior', 'Lead', 'Director'] |
| permitted_values:change_type | employees_salary_history | True | 0 | Checks that values belong to the allowed category list ['Hire', 'Annual Raise', 'Promotion', 'Market Adjustment', 'Role Change']. | change_type | 0 invalid values | Values in ['Hire', 'Annual Raise', 'Promotion', 'Market Adjustment', 'Role Change'] |
