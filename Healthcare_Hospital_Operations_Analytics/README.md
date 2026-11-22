# Healthcare – Hospital Operations Analytics (Single Hospital)

Updated starter project aligned to `Hospital_Operations_Analytics_Single_Hospital_Dataset.zip`.

## Dataset
The `data/raw/` folder contains five CSV files:
- `patients.csv` — 1,000 synthetic patient records; primary key `patient_id`.
- `departments.csv` — 10 departments; primary key `department_id`.
- `admissions.csv` — 2,800 admission records; primary key `admission_id`; foreign keys `patient_id`, `department_id`.
- `daily_bed_occupancy.csv` — 3,650 department-day records; composite key `(department_id, occupancy_date)`; foreign key `department_id`.
- `data_dictionary.csv` — field definitions and key roles.

This is a single-hospital synthetic dataset. It is suitable for a student analytics project, not clinical or operational decision-making. No real patient information should be added to a public repository.

## Data model
`patients (1) -> (many) admissions (many) -> (1) departments`
`departments (1) -> (many) daily_bed_occupancy`

Join keys:
- `admissions.patient_id = patients.patient_id`
- `admissions.department_id = departments.department_id`
- `daily_bed_occupancy.department_id = departments.department_id`

Do not join admissions to daily occupancy on department alone: use department plus a matching date (for example, admission_date/occupancy_date) and be mindful that one department-day can have many admissions.

## Run order
1. Upload all five CSVs from `data/raw/` to the Databricks volume `/Volumes/workspace/healthcare_ops/raw/`.
2. Open and run `databricks/01_bronze.py`.
3. Run `databricks/02_silver.py`.
4. Run `databricks/03_gold.py`.
5. Run `databricks/04_quality_checks.py`.

These are Python notebook source files. In Databricks, create a Python notebook for each file and paste its contents into a cell (or import the source file if your workspace supports it). Change `CATALOG` if your catalog is not `workspace`.

## Gold tables for reporting
- `gold_hospital_kpis`
- `gold_monthly_admissions`
- `gold_department_performance`
- `gold_daily_bed_occupancy`
- `gold_patient_demographics`
- `gold_readmission_summary`

Use these Gold tables in Power BI. If you are using Power BI in a Mac browser and cannot connect directly to Databricks, export each Gold table as CSV and upload the CSVs into Power BI Service. See `docs/power_bi_browser_guide.md`.

## Notes on metric definitions
- Admissions are counted by `admission_id`; use `admission_status` to distinguish discharged/current statuses.
- Bed occupancy comes from `daily_bed_occupancy.csv`; calculate occupancy as occupied_beds / total_beds where needed. The source also provides `occupancy_rate`.
- Department admission performance uses admissions assigned to that department.
- Waiting time from admissions and daily bed occupancy are separate measures; do not blend them without explaining the grain.
- Synthetic data values and patterns are illustrative only.
