# Databricks setup (Free Edition)

1. In Catalog Explorer, open catalog `workspace` → schema `healthcare_ops` → volume `raw`.
2. Confirm the volume path is `/Volumes/workspace/healthcare_ops/raw/` (as shown in your workspace). If your catalog differs, change `CATALOG` in all four notebook files and use the matching volume path.
3. Upload these five files from this project's `data/raw/` folder: `patients.csv`, `departments.csv`, `admissions.csv`, `daily_bed_occupancy.csv`, `data_dictionary.csv`.
4. Create four Python notebooks in your Workspace, named `01_bronze_layer`, `02_silver_layer`, `03_gold_layer`, and `04_quality_checks`.
5. Paste the matching `.py` file contents into each notebook. Run the notebooks in the order above. Attach Serverless compute if prompted.
6. Verify `SHOW TABLES IN workspace.healthcare_ops` lists five Bronze tables, five Silver tables and six Gold tables.

Important: run each notebook top-to-bottom. The `SHOW TABLES` command is Spark SQL executed inside Python, so use `spark.sql("SHOW TABLES IN workspace.healthcare_ops").show(truncate=False)` in a Python cell, not a bare SQL statement in a Python cell.
