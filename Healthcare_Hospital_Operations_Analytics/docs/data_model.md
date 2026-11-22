# Relational data model

## Tables and keys

| File / table | Primary key | Foreign keys | Grain |
|---|---|---|---|
| patients | `patient_id` | — | One row per synthetic patient |
| departments | `department_id` | — | One row per hospital department |
| admissions | `admission_id` | `patient_id` → patients; `department_id` → departments | One row per admission |
| daily_bed_occupancy | (`department_id`, `occupancy_date`) | `department_id` → departments | One row per department per calendar day |
| data_dictionary | (`file_name`, `attribute`) conceptually | — | One row per documented field |

## Relationships

- `patients.patient_id` 1 — many `admissions.patient_id`
- `departments.department_id` 1 — many `admissions.department_id`
- `departments.department_id` 1 — many `daily_bed_occupancy.department_id`

## Analytical cautions

- `daily_bed_occupancy` is at department-day grain; admissions is at admission grain. Joining on department only multiplies rows. To analyze admissions alongside occupancy, aggregate each to the intended common grain first (typically department and date), then join.
- `available_beds` may not equal `total_beds - occupied_beds` if reserved/out-of-service beds are represented separately. Preserve the source fields and validate the intended business definition before deriving a new metric.
- `occupancy_rate` is provided in the source. Gold's `weighted_occupancy_rate` is calculated as sum(occupied_beds) / sum(total_beds) * 100 across departments for each date.
- The source includes both admission-level `waiting_time_minutes` and department-day `average_waiting_time_minutes`; these are different grains and must be named distinctly in reports.
- `length_of_stay_days` is supplied by the dataset. Do not silently recalculate it from timestamps without reconciling the definition.
