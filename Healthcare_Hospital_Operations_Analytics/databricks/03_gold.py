# ============================================================
# GOLD LAYER - HEALTHCARE HOSPITAL OPERATIONS ANALYTICS
# ============================================================

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "healthcare_ops"

BASE = f"{CATALOG}.{SCHEMA}"

print("Gold Layer Started")

# ------------------------------------------------------------
# 1. LOAD SILVER TABLES
# ------------------------------------------------------------

admissions = spark.table(f"{BASE}.silver_admissions")
bed_occupancy = spark.table(f"{BASE}.silver_daily_bed_occupancy")
departments = spark.table(f"{BASE}.silver_departments")
patients = spark.table(f"{BASE}.silver_patients")

print("Silver tables loaded successfully")

# ------------------------------------------------------------
# 2. HELPER FUNCTIONS
# ------------------------------------------------------------

def find_column(df, possible_names):
    """
    Find an actual column name using a list of possible names.
    This prevents errors when column names differ slightly.
    """
    lookup = {c.lower(): c for c in df.columns}

    for name in possible_names:
        if name.lower() in lookup:
            return lookup[name.lower()]

    return None


def save_gold(df, table_name):
    """
    Save Gold DataFrame as a Delta table.
    """
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{BASE}.{table_name}")

    print(f"Created {table_name} successfully")


# ------------------------------------------------------------
# 3. GOLD - MONTHLY ADMISSIONS
# ------------------------------------------------------------

admission_date_col = find_column(
    admissions,
    ["admission_date", "admit_date", "admission_datetime"]
)

patient_id_col = find_column(
    admissions,
    ["patient_id"]
)

department_id_col = find_column(
    admissions,
    ["department_id"]
)

if admission_date_col is None:
    raise ValueError("No admission date column found in silver_admissions")

monthly_admissions = (
    admissions
    .withColumn(
        "admission_date_clean",
        F.to_date(F.col(admission_date_col))
    )
    .filter(F.col("admission_date_clean").isNotNull())
    .withColumn("year", F.year("admission_date_clean"))
    .withColumn("month", F.month("admission_date_clean"))
    .withColumn("year_month", F.date_format("admission_date_clean", "yyyy-MM"))
    .groupBy("year", "month", "year_month")
    .agg(
        F.count("*").alias("total_admissions"),
        (
            F.countDistinct(patient_id_col)
            if patient_id_col else F.count("*")
        ).alias("unique_patients")
    )
    .orderBy("year", "month")
)

save_gold(monthly_admissions, "gold_monthly_admissions")


# ------------------------------------------------------------
# 4. GOLD - DAILY BED OCCUPANCY
# ------------------------------------------------------------

occupancy_date_col = find_column(
    bed_occupancy,
    ["occupancy_date", "date", "record_date"]
)

occupied_col = find_column(
    bed_occupancy,
    ["occupied_beds"]
)

total_beds_col = find_column(
    bed_occupancy,
    ["total_beds"]
)

available_col = find_column(
    bed_occupancy,
    ["available_beds"]
)

waiting_col = find_column(
    bed_occupancy,
    ["average_waiting_time_minutes", "avg_waiting_time_minutes"]
)

occupancy_rate_col = find_column(
    bed_occupancy,
    ["occupancy_rate"]
)

if occupancy_date_col is None:
    raise ValueError("No occupancy date column found")

daily_bed_occupancy = bed_occupancy.withColumn(
    "occupancy_date_clean",
    F.to_date(F.col(occupancy_date_col))
)

# Create a standardized waiting time field if available
if waiting_col:
    daily_bed_occupancy = daily_bed_occupancy.withColumn(
        "average_waiting_time_minutes",
        F.col(waiting_col).cast("double")
    )

# Create a standardized occupancy rate field if available
if occupancy_rate_col:
    daily_bed_occupancy = daily_bed_occupancy.withColumn(
        "occupancy_rate_clean",
        F.col(occupancy_rate_col).cast("double")
    )
elif occupied_col and total_beds_col:
    daily_bed_occupancy = daily_bed_occupancy.withColumn(
        "occupancy_rate_clean",
        F.when(
            F.col(total_beds_col) > 0,
            F.col(occupied_col) / F.col(total_beds_col) * 100
        )
    )

# Aggregate by date, department if present
group_columns = ["occupancy_date_clean"]

if department_id_col:
    occ_department_col = find_column(
        bed_occupancy,
        ["department_id"]
    )
    if occ_department_col:
        group_columns.append(occ_department_col)

aggregate_expressions = []

if occupied_col:
    aggregate_expressions.append(
        F.sum(F.col(occupied_col)).alias("occupied_beds")
    )

if total_beds_col:
    aggregate_expressions.append(
        F.sum(F.col(total_beds_col)).alias("total_beds")
    )

if available_col:
    aggregate_expressions.append(
        F.sum(F.col(available_col)).alias("available_beds")
    )

if waiting_col:
    aggregate_expressions.append(
        F.avg(F.col(waiting_col)).alias("average_waiting_time_minutes")
    )

if occupancy_rate_col:
    aggregate_expressions.append(
        F.avg(F.col(occupancy_rate_col)).alias("occupancy_rate")
    )
elif occupied_col and total_beds_col:
    aggregate_expressions.append(
        (
            F.sum(F.col(occupied_col)) /
            F.sum(F.col(total_beds_col)) * 100
        ).alias("occupancy_rate")
    )

if aggregate_expressions:
    daily_bed_occupancy_gold = (
        daily_bed_occupancy
        .groupBy(*group_columns)
        .agg(*aggregate_expressions)
        .withColumnRenamed(
            "occupancy_date_clean",
            "occupancy_date"
        )
    )
else:
    daily_bed_occupancy_gold = daily_bed_occupancy

save_gold(
    daily_bed_occupancy_gold,
    "gold_daily_bed_occupancy"
)


# ------------------------------------------------------------
# 5. GOLD - DEPARTMENT PERFORMANCE
# ------------------------------------------------------------

# Admission count per department
department_admission_metrics = (
    admissions
    .groupBy(department_id_col)
    .agg(
        F.count("*").alias("total_admissions"),
        (
            F.countDistinct(patient_id_col)
            if patient_id_col else F.count("*")
        ).alias("unique_patients")
    )
) if department_id_col else None

# Bed metrics per department, if available
occ_department_col = find_column(
    bed_occupancy,
    ["department_id"]
)

department_bed_metrics = None

if occ_department_col:

    bed_aggs = []

    if occupied_col:
        bed_aggs.append(
            F.avg(F.col(occupied_col)).alias("avg_occupied_beds")
        )

    if total_beds_col:
        bed_aggs.append(
            F.avg(F.col(total_beds_col)).alias("avg_total_beds")
        )

    if waiting_col:
        bed_aggs.append(
            F.avg(F.col(waiting_col)).alias("average_waiting_time_minutes")
        )

    if occupancy_rate_col:
        bed_aggs.append(
            F.avg(F.col(occupancy_rate_col)).alias("average_occupancy_rate")
        )
    elif occupied_col and total_beds_col:
        bed_aggs.append(
            (
                F.sum(F.col(occupied_col)) /
                F.sum(F.col(total_beds_col)) * 100
            ).alias("average_occupancy_rate")
        )

    if bed_aggs:
        department_bed_metrics = (
            bed_occupancy
            .groupBy(occ_department_col)
            .agg(*bed_aggs)
        )

# Start with departments master data
department_performance = departments

# Join admissions metrics
if department_admission_metrics is not None:
    department_performance = department_performance.join(
        department_admission_metrics,
        on=department_id_col,
        how="left"
    )

# Join bed metrics
if department_bed_metrics is not None:
    department_performance = department_performance.join(
        department_bed_metrics,
        on=occ_department_col,
        how="left"
    )

save_gold(
    department_performance,
    "gold_department_performance"
)


# ------------------------------------------------------------
# 6. GOLD - PATIENT DEMOGRAPHICS
# ------------------------------------------------------------

patient_gender_col = find_column(
    patients,
    ["gender", "sex"]
)

patient_age_col = find_column(
    patients,
    ["age", "patient_age"]
)

patient_age_band_col = find_column(
    patients,
    ["age_band", "age_group"]
)

patient_state_col = find_column(
    patients,
    ["state", "province", "region"]
)

demographic_group_columns = []

if patient_gender_col:
    demographic_group_columns.append(patient_gender_col)

if patient_age_band_col:
    demographic_group_columns.append(patient_age_band_col)

if patient_state_col:
    demographic_group_columns.append(patient_state_col)

if demographic_group_columns:

    patient_demographics_gold = (
        patients
        .groupBy(*demographic_group_columns)
        .agg(
            F.count("*").alias("total_patients")
        )
    )

elif patient_age_col:

    patient_demographics_gold = (
        patients
        .withColumn("age", F.col(patient_age_col).cast("double"))
        .groupBy(
            F.when(F.col("age") < 18, "Under 18")
             .when(F.col("age") < 30, "18-29")
             .when(F.col("age") < 45, "30-44")
             .when(F.col("age") < 60, "45-59")
             .otherwise("60+")
             .alias("age_band")
        )
        .agg(F.count("*").alias("total_patients"))
    )

else:
    patient_demographics_gold = patients

save_gold(
    patient_demographics_gold,
    "gold_patient_demographics"
)


# ------------------------------------------------------------
# 7. GOLD - HOSPITAL KPIs
# ------------------------------------------------------------

# Total admissions
total_admissions = admissions.count()

# Unique patients admitted
total_unique_patients = (
    admissions.select(patient_id_col).distinct().count()
    if patient_id_col else 0
)

# Total registered patients
total_registered_patients = patients.count()

# Average bed occupancy percentage
avg_occupancy_rate = (
    bed_occupancy
    .agg(F.avg(F.col(occupancy_rate_col)))
    .first()[0]
    if occupancy_rate_col else None
)

# Average waiting time (minutes)
avg_waiting_time = (
    bed_occupancy
    .agg(F.avg(F.col(waiting_col)))
    .first()[0]
    if waiting_col else None
)

# Average length of stay
discharge_date_col = find_column(
    admissions,
    ["discharge_date", "discharge_datetime"]
)

avg_length_of_stay = None

if admission_date_col and discharge_date_col:
    avg_length_of_stay = (
        admissions
        .withColumn(
            "stay_days",
            F.datediff(
                F.to_date(F.col(discharge_date_col)),
                F.to_date(F.col(admission_date_col))
            )
        )
        .filter(F.col("stay_days") >= 0)
        .agg(F.avg("stay_days"))
        .first()[0]
    )

# Create a single-row KPI table
hospital_kpis = spark.createDataFrame(
    [(
        total_admissions,
        total_unique_patients,
        total_registered_patients,
        avg_occupancy_rate,
        avg_waiting_time,
        avg_length_of_stay
    )],
    [
        "total_admissions",
        "unique_admitted_patients",
        "total_registered_patients",
        "average_occupancy_rate",
        "average_waiting_time_minutes",
        "average_length_of_stay_days"
    ]
)

save_gold(hospital_kpis, "gold_hospital_kpis")


# ------------------------------------------------------------
# 8. DISPLAY GOLD TABLES
# ------------------------------------------------------------

print("\n--- HOSPITAL KPIs ---")
spark.table(f"{BASE}.gold_hospital_kpis").show(truncate=False)

print("\n--- MONTHLY ADMISSIONS ---")
spark.table(f"{BASE}.gold_monthly_admissions").show(10, truncate=False)

print("\n--- DEPARTMENT PERFORMANCE ---")
spark.table(f"{BASE}.gold_department_performance").show(10, truncate=False)

print("\n--- DAILY BED OCCUPANCY ---")
spark.table(f"{BASE}.gold_daily_bed_occupancy").show(10, truncate=False)

print("\n--- PATIENT DEMOGRAPHICS ---")
spark.table(f"{BASE}.gold_patient_demographics").show(10, truncate=False)

print("\n===================================")
print("GOLD LAYER COMPLETED SUCCESSFULLY!")
print("===================================")




# ==========================================
# EXPORT GOLD TABLES FOR POWER BI
# Healthcare Hospital Operations Analytics
# ==========================================

BASE = "workspace.healthcare_ops"

tables = [
    "gold_hospital_kpis",
    "gold_monthly_admissions",
    "gold_department_performance",
    "gold_daily_bed_occupancy",
    "gold_patient_demographics"
]

EXPORT_PATH = "/Volumes/workspace/healthcare_ops/raw/powerbi_exports"

for table in tables:
    df = spark.table(f"{BASE}.{table}")

    # Convert to Pandas (these are summary tables)
    pdf = df.toPandas()

    # Convert dataframe to CSV text
    csv_text = pdf.to_csv(index=False)

    # Export CSV file into the volume
    file_path = f"{EXPORT_PATH}/{table}.csv"

    dbutils.fs.put(file_path, csv_text, overwrite=True)

    print(f"Exported: {table}.csv | Rows: {len(pdf)}")

print("ALL GOLD TABLES EXPORTED SUCCESSFULLY!")
