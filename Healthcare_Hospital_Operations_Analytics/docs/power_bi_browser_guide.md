# Power BI dashboard using a Mac browser

Power BI Service in a browser can be used for report authoring in supported workspaces, but some data-import and modeling options vary by tenant, license and current UI. This project avoids requiring Power BI Desktop.

## 1. Export Gold tables from Databricks as CSV

Run the following in a Python cell in Databricks, one table at a time. It writes a single CSV file into a temporary DBFS location; use the Databricks file browser / supported download action to download the resulting CSV. If your Free Edition workspace does not expose that download action, use the table UI's Export/Download CSV option where available.

```python
CATALOG = "workspace"
SCHEMA = "healthcare_ops"
TABLES = [
    "gold_hospital_kpis",
    "gold_monthly_admissions",
    "gold_department_performance",
    "gold_daily_bed_occupancy",
    "gold_patient_demographics",
    "gold_readmission_summary",
]

for table in TABLES:
    df = spark.table(f"{CATALOG}.{SCHEMA}.{table}")
    out = f"dbfs:/FileStore/healthcare_exports/{table}"
    (df.coalesce(1).write.mode("overwrite").option("header", True).csv(out))
    print(table, "exported to", out)
```

Spark writes a folder containing a `part-....csv` file and metadata, not a single file named exactly after the table. Download the `part-....csv` from each folder and rename it locally to the table name, e.g. `gold_hospital_kpis.csv`. If DBFS FileStore is unavailable in your workspace, use the table's result grid export or SQL query download option.

## 2. Upload CSVs into Power BI Service

1. Open `https://app.powerbi.com/` in your Mac browser and open your workspace.
2. Choose **New** / **Create** and look for **Semantic model** or **Upload a file** (the exact wording varies by tenant).
3. Upload each Gold CSV. If the UI only lets you create one semantic model at a time, create/import each CSV and then use the model/report authoring experience to build visuals. If it offers a multi-file upload or a dataflow, load all six together.
4. Check that numeric fields are numeric and date fields are dates. In particular, set `admission_month` as text or date consistently, and `occupancy_date` as Date.
5. Open a new report from the imported semantic model.

If your Power BI tenant does not offer CSV upload or browser report editing, do not install Windows-only Power BI Desktop on the Mac. Use your organization's supported Power BI web workspace or ask the project supervisor for access to a workspace that supports web authoring.

## 3. Suggested dashboard pages

### Page 1 — Hospital overview
- Card: `total_admissions`
- Card: `unique_admitted_patients`
- Card: `overall_weighted_occupancy_pct`
- Card: `avg_admission_wait_minutes`
- Card: `avg_length_of_stay_days`
- Line chart: `occupancy_date` vs `weighted_occupancy_rate` from `gold_daily_bed_occupancy`

### Page 2 — Department performance
- Clustered bar: `department_name` vs `total_admissions`
- Bar: `department_name` vs `avg_admission_wait_minutes`
- Bar: `department_name` vs `avg_length_of_stay_days`
- Table: admissions, unique patients, readmissions, total bill amount

### Page 3 — Bed occupancy
- Line chart: `occupancy_date` vs `occupied_beds` and `available_beds`
- Line chart: `occupancy_date` vs `weighted_occupancy_rate`
- Cards: latest `total_beds`, `occupied_beds`, `available_beds` (filter to latest date if needed)

### Page 4 — Patient and readmission profile
- Donut: `gender` vs `total_patients` from `gold_patient_demographics`
- Bar: `state` vs `total_patients`
- Bar/table: `department_name`, `readmissions`, `readmission_rate_pct`

## 4. Modeling advice

The Gold tables are separate aggregates at different grains. Do not create arbitrary relationships between all six Gold tables. Build visuals from the appropriate single Gold table, or create a proper shared Date/Department dimension if you later combine them. Keep the synthetic-data disclaimer visible in the report.
