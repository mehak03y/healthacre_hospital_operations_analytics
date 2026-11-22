
from pyspark.sql import functions as F
from pyspark.sql import types as T
import re

# --------------------------------------------------
# 1. CONFIGURATION
# --------------------------------------------------

CATALOG = "workspace"
SCHEMA = "healthcare_ops"

BASE = f"{CATALOG}.{SCHEMA}"

print("Silver Layer started")


# --------------------------------------------------
# 2. TABLES TO PROCESS
# --------------------------------------------------

tables = [
    "patients",
    "departments",
    "admissions",
    "daily_bed_occupancy",
    "data_dictionary"
]


# --------------------------------------------------
# 3. FUNCTION TO STANDARDIZE COLUMN NAMES
# --------------------------------------------------

def clean_column_name(name):
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    name = re.sub(r"_+", "_", name)
    name = name.strip("_")
    return name


# --------------------------------------------------
# 4. FUNCTION TO CLEAN A DATAFRAME
# --------------------------------------------------

def clean_dataframe(df, table_name):

    # Standardize column names
    for old_name in df.columns:
        new_name = clean_column_name(old_name)
        if old_name != new_name:
            df = df.withColumnRenamed(old_name, new_name)

    # Trim spaces and convert blank strings to NULL
    for column_name, data_type in df.dtypes:

        if data_type == "string":
            df = df.withColumn(
                column_name,
                F.when(
                    F.trim(F.col(column_name)) == "",
                    None
                ).otherwise(F.trim(F.col(column_name)))
            )

    # Remove exact duplicate rows
    df = df.dropDuplicates()

    # Identify likely primary key columns
    key_candidates = [
        "patient_id",
        "department_id",
        "admission_id",
        "occupancy_id",
        "id"
    ]

    primary_key = next(
        (col for col in key_candidates if col in df.columns),
        None
    )

    # Remove duplicate records based on identified key
    if primary_key:
        df = df.dropDuplicates([primary_key])

    # Convert relevant date/time columns
    for column_name in df.columns:

        if any(word in column_name for word in [
            "date", "admission_date", "discharge_date"
        ]):
            if column_name not in ["update_date"]:
                df = df.withColumn(
                    column_name,
                    F.to_date(F.col(column_name))
                )

        elif any(word in column_name for word in [
            "timestamp", "datetime"
        ]):
            df = df.withColumn(
                column_name,
                F.to_timestamp(F.col(column_name))
            )

    # Add ETL metadata
    df = df.withColumn(
        "_silver_processed_at",
        F.current_timestamp()
    )

    df = df.withColumn(
        "_silver_source_table",
        F.lit(f"bronze_{table_name}")
    )

    return df


# --------------------------------------------------
# 5. READ BRONZE AND CREATE SILVER TABLES
# --------------------------------------------------

for table_name in tables:

    bronze_table = f"{BASE}.bronze_{table_name}"
    silver_table = f"{BASE}.silver_{table_name}"

    print(f"Processing: {bronze_table}")

    # Read Bronze table
    bronze_df = spark.table(bronze_table)

    # Clean and transform
    silver_df = clean_dataframe(
        bronze_df,
        table_name
    )

    # Save Silver Delta table
    silver_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(silver_table)

    print(f"Created: {silver_table}")
    print(f"Rows: {silver_df.count()}")


# --------------------------------------------------
# 6. VERIFY SILVER TABLES
# --------------------------------------------------

print("\nSILVER TABLES CREATED SUCCESSFULLY!")

spark.sql(
    f"SHOW TABLES IN {BASE}"
).show(truncate=False)

#command
for table in [
    "silver_patients",
    "silver_departments",
    "silver_admissions",
    "silver_daily_bed_occupancy",
    "silver_encounters"
]:
    print(f"\n===== {table} =====")
    spark.table(f"workspace.healthcare_ops.{table}").printSchema()

#command
spark.table(
    "workspace.healthcare_ops.silver_admissions"
).show(5, truncate=False)

spark.table(
    "workspace.healthcare_ops.silver_daily_bed_occupancy"
).show(5, truncate=False)

spark.table(
    "workspace.healthcare_ops.silver_departments"
).show(5, truncate=False)    