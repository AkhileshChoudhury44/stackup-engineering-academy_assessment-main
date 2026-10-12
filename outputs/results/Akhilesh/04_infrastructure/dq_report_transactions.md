# Data Quality Report

Checks run: 8
Checks passed: 8
Checks failed: 0

Each row below explains what was tested, the measured value, the expected rule, and whether the row-level check passed.

| check | dataset | passed | failed_rows | details | metric_name | metric_value | expected |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| completeness | transactions | True | 13935 | Measures how much of the table is populated across all cells. | completeness | 97.68% | At least 95.00% populated |
| primary_key:transaction_id | transactions | True | 0 | Checks that the business key is present once per row. | null_or_duplicate_rows_in_transaction_id | 0 | Zero nulls and zero duplicates |
| numeric_range:amount | transactions | True | 0 | Checks whether numeric values fall inside the expected business range. | amount | 0.0..10000000.0 | Values between 0.0 and 10000000.0 |
| valid_date:transaction_date | transactions | True | 0 | Checks whether the date can be parsed correctly. | transaction_date | 50000 valid / 0 invalid | Valid date values |
| non_negative:amount | transactions | True | 0 | Checks that values are not below zero. | amount | 0 negative rows | Zero negative values |
| foreign_key:project_id | transactions | True | 0 | Checks whether the reference key exists in the related table. | project_id | 0 missing rows | Match values in project_id |
| foreign_key:approved_by | transactions | True | 0 | Checks whether the reference key exists in the related table. | approved_by | 0 missing rows | Match values in employee_id |
| permitted_values:payment_status | transactions | True | 0 | Checks that values belong to the allowed category list ['Paid', 'Pending', 'Disputed', 'Cancelled']. | payment_status | 0 invalid values | Values in ['Paid', 'Pending', 'Disputed', 'Cancelled'] |
