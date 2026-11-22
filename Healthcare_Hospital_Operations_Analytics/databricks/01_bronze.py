CATALOG = "workspace"
SCHEMA = "healthcare_ops"

VOLUME_PATH = "/Volumes/workspace/healthcare_ops/raw"

print("Configuration loaded successfully!")
print("Volume path:", VOLUME_PATH)

#command
from pyspark.sql.functions import current_timestamp, input_file_name

#command
from pyspark.sql.functions import current_timestamp, lit

files = {
    "patients": "patients.csv",
    "departments": "departments.csv",
    "admissions": "admissions.csv",
    "daily_bed_occupancy": "daily_bed_occupancy.csv",
    "data_dictionary": "data_dictionary.csv"
}

for table, filename in files.items():

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"{VOLUME_PATH}/{filename}")
    )

    df = (
        df.withColumn("_source_file", lit(filename))
          .withColumn("_ingested_at", current_timestamp())
    )

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{CATALOG}.{SCHEMA}.bronze_{table}")

    print(f"Created bronze_{table}: {df.count()} rows")

print("All Bronze Layer tables created successfully!")

#command
CATALOG = "workspace"
SCHEMA = "healthcare_ops"
VOLUME_PATH = "/Volumes/workspace/healthcare_ops/raw"

#command
spark.sql(
    "SHOW TABLES IN workspace.healthcare_ops"
).show(truncate=False)
