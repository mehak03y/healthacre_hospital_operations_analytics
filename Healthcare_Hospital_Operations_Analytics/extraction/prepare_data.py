"""Validate and stage the supplied single-hospital CSV files for Databricks upload."""
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
REQUIRED = {
    "patients.csv": ["patient_id", "gender", "date_of_birth", "age", "city", "state", "registration_date", "blood_group"],
    "departments.csv": ["department_id", "department_name", "department_type", "bed_capacity", "head_of_department", "operating_hours", "is_emergency_department"],
    "admissions.csv": ["admission_id", "patient_id", "department_id", "admission_date", "discharge_date", "admission_type", "admission_status", "admission_source", "discharge_type", "waiting_time_minutes", "length_of_stay_days", "attending_physician", "total_bill_amount", "readmission_flag"],
    "daily_bed_occupancy.csv": ["department_id", "occupancy_date", "total_beds", "occupied_beds", "available_beds", "reserved_beds", "out_of_service_beds", "occupancy_rate", "average_waiting_time_minutes"],
    "data_dictionary.csv": ["file_name", "attribute", "pandas_data_type", "key_role", "description"],
}
for filename, columns in REQUIRED.items():
    path = RAW / filename
    if not path.exists():
        raise FileNotFoundError(f"Missing {path}")
    df = pd.read_csv(path)
    missing = sorted(set(columns) - set(df.columns))
    if missing:
        raise ValueError(f"{filename} is missing columns: {missing}")
    print(f"{filename}: {len(df):,} rows; columns validated")

patients = pd.read_csv(RAW / "patients.csv")
departments = pd.read_csv(RAW / "departments.csv")
admissions = pd.read_csv(RAW / "admissions.csv")
occupancy = pd.read_csv(RAW / "daily_bed_occupancy.csv")
print("\nKey checks (local validation):")
print("Duplicate patient IDs:", patients.patient_id.duplicated().sum())
print("Duplicate department IDs:", departments.department_id.duplicated().sum())
print("Duplicate admission IDs:", admissions.admission_id.duplicated().sum())
print("Admissions with unknown patient:", (~admissions.patient_id.isin(patients.patient_id)).sum())
print("Admissions with unknown department:", (~admissions.department_id.isin(departments.department_id)).sum())
print("Occupancy rows with unknown department:", (~occupancy.department_id.isin(departments.department_id)).sum())
print("\nReady to upload data/raw/*.csv to the Databricks raw volume.")
