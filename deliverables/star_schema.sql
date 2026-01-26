-- =====================================================
-- HEALTHCARE ANALYTICS LAB - STAR SCHEMA INCREMENTAL ETL
-- Strategy: Delta Load with High Watermarks + SCD Type 2
-- Run Mode: Idempotent (Safe to re-run multiple times)
-- =====================================================
-- =====================================================
-- SECTION 0: ETL INITIALIZATION & WATERMARK SETUP
-- =====================================================
SET @etl_start_time = NOW();
-- Get High Watermark: Last successfully loaded encounter date
-- Default to 1900-01-01 if no successful load exists
SET @last_watermark = (
        SELECT COALESCE(MAX(high_watermark), '1900-01-01 00:00:00')
        FROM etl_metadata
        WHERE table_name = 'fact_encounters'
            AND last_etl_status = 'SUCCESS'
    );
-- Get Timestamp Watermark: Last ETL timestamp for SCD Type 2 tracking
-- This tracks the last time we checked for patient/provider changes
SET @last_etl_timestamp = (
        SELECT COALESCE(MAX(last_etl_timestamp), '1900-01-01 00:00:00')
        FROM etl_metadata
        WHERE table_name = 'dim_patient'
            AND last_etl_status = 'SUCCESS'
    );
-- Log ETL batch start
INSERT INTO etl_log (etl_step, status, error_message)
VALUES (
        'incremental_etl_batch',
        'RUNNING',
        CONCAT(
            'Encounter Watermark: ',
            @last_watermark,
            ' | Timestamp Watermark: ',
            @last_etl_timestamp
        )
    );
SET @batch_log_id = LAST_INSERT_ID();
-- Step 1: Pre-ETL Data Quality Validation
CALL validate_source_data();
-- =====================================================
-- SECTION 1: DIMENSION LOADS (Incremental + SCD Type 2)
-- =====================================================
START TRANSACTION;
INSERT INTO etl_log (etl_step, status)
VALUES ('load_dimensions_incremental', 'RUNNING');
SET @dim_log_id = LAST_INSERT_ID();
-- -----------------------------------------------------
-- dim_date: Generated via stored procedure
-- Strategy: Populate range covering expected data
-- -----------------------------------------------------
DROP PROCEDURE IF EXISTS populate_dim_date;
DELIMITER // 

CREATE PROCEDURE populate_dim_date(IN start_date DATE, IN end_date DATE) BEGIN
DECLARE current_date_val DATE;
SET current_date_val = start_date;
-- Avoid nested transactions if called within one
-- START TRANSACTION; 
WHILE current_date_val <= end_date DO
INSERT IGNORE INTO dim_date (
        calendar_date,
        `year`,
        `quarter`,
        quarter_name,
        `month`,
        month_name,
        `year_month`,
        week_of_year,
        day_of_month,
        day_of_week,
        day_name,
        is_weekend,
        is_holiday
    )
VALUES (
        current_date_val,
        YEAR(current_date_val),
        QUARTER(current_date_val),
        CONCAT('Q', QUARTER(current_date_val)),
        MONTH(current_date_val),
        DATE_FORMAT(current_date_val, '%M'),
        DATE_FORMAT(current_date_val, '%Y-%m'),
        WEEK(current_date_val, 1),
        DAY(current_date_val),
        DAYOFWEEK(current_date_val),
        DATE_FORMAT(current_date_val, '%W'),
        CASE
            WHEN DAYOFWEEK(current_date_val) IN (1, 7) THEN TRUE
            ELSE FALSE
        END,
        FALSE
    );
SET current_date_val = DATE_ADD(current_date_val, INTERVAL 1 DAY);
END WHILE;
-- COMMIT;
-- SELECT CONCAT('Successfully populated dim_date from ', start_date, ' to ', end_date) AS status;
END // 

DELIMITER ;
CALL populate_dim_date('2024-01-01', '2026-12-31');
-- -----------------------------------------------------
-- dim_specialty: Upsert (SCD Type 1 - Overwrite)
-- -----------------------------------------------------
INSERT INTO dim_specialty (
        specialty_id,
        specialty_name,
        specialty_code,
        specialty_category
    )
SELECT specialty_id,
    specialty_name,
    specialty_code,
    CASE
        WHEN specialty_code IN ('SURG', 'ORTH', 'NEUR') THEN 'Surgical'
        WHEN specialty_code IN ('RAD', 'PATH', 'ANES') THEN 'Diagnostic/Support'
        ELSE 'Medical'
    END AS specialty_category
FROM specialties ON DUPLICATE KEY
UPDATE specialty_name =
VALUES(specialty_name),
    specialty_code =
VALUES(specialty_code),
    specialty_category =
VALUES(specialty_category);
-- -----------------------------------------------------
-- dim_department: Upsert (SCD Type 1 - Overwrite)
-- -----------------------------------------------------
INSERT INTO dim_department (
        department_id,
        department_name,
        floor,
        capacity,
        department_type
    )
SELECT department_id,
    department_name,
    floor,
    capacity,
    CASE
        WHEN department_name LIKE '%ICU%'
        OR department_name LIKE '%Inpatient%' THEN 'Inpatient'
        WHEN department_name LIKE '%Clinic%'
        OR department_name LIKE '%Outpatient%' THEN 'Outpatient'
        WHEN department_name LIKE '%Emergency%' THEN 'ER'
        WHEN department_name LIKE '%Surgical%' THEN 'Surgical'
        ELSE 'Other'
    END AS department_type
FROM departments ON DUPLICATE KEY
UPDATE department_name =
VALUES(department_name),
    floor =
VALUES(floor),
    capacity =
VALUES(capacity),
    department_type =
VALUES(department_type);
-- -----------------------------------------------------
-- dim_patient: SCD Type 2 (History Tracking)
-- Strategy: Timestamp-Based Change Detection
--   1. Expire changed records (identified by last_update > @last_etl_timestamp)
--   2. Insert new versions for changed records
--   3. Insert brand new patients
-- NO TEMPORARY TABLES USED
-- -----------------------------------------------------
-- Step 1: Expire changed records (detected via timestamp)
UPDATE dim_patient d
    INNER JOIN patients p ON d.patient_id = p.patient_id
SET d.is_current = FALSE,
    d.expiration_date = CURDATE()
WHERE d.is_current = TRUE
    AND p.last_update > @last_etl_timestamp;
-- Timestamp-based change detection
-- Step 2: Insert new versions for changed records
INSERT INTO dim_patient (
        patient_id,
        mrn,
        first_name,
        last_name,
        full_name,
        date_of_birth,
        age,
        age_group,
        gender,
        effective_date,
        expiration_date,
        is_current
    )
SELECT p.patient_id,
    p.mrn,
    p.first_name,
    p.last_name,
    CONCAT(p.first_name, ' ', p.last_name) AS full_name,
    p.date_of_birth,
    TIMESTAMPDIFF(YEAR, p.date_of_birth, CURDATE()) AS age,
    CASE
        WHEN TIMESTAMPDIFF(YEAR, p.date_of_birth, CURDATE()) < 18 THEN '0-17'
        WHEN TIMESTAMPDIFF(YEAR, p.date_of_birth, CURDATE()) BETWEEN 18 AND 34 THEN '18-34'
        WHEN TIMESTAMPDIFF(YEAR, p.date_of_birth, CURDATE()) BETWEEN 35 AND 54 THEN '35-54'
        WHEN TIMESTAMPDIFF(YEAR, p.date_of_birth, CURDATE()) BETWEEN 55 AND 74 THEN '55-74'
        ELSE '75+'
    END AS age_group,
    p.gender,
    CURDATE() AS effective_date,
    '9999-12-31' AS expiration_date,
    TRUE AS is_current
FROM patients p
    INNER JOIN dim_patient d ON p.patient_id = d.patient_id
WHERE d.expiration_date = CURDATE() -- Just expired today (from Step 1)
    AND d.is_current = FALSE;
-- Step 3: Insert brand NEW patients (not in dimension at all)
INSERT INTO dim_patient (
        patient_id,
        mrn,
        first_name,
        last_name,
        full_name,
        date_of_birth,
        age,
        age_group,
        gender,
        effective_date,
        expiration_date,
        is_current
    )
SELECT p.patient_id,
    p.mrn,
    p.first_name,
    p.last_name,
    CONCAT(p.first_name, ' ', p.last_name) AS full_name,
    p.date_of_birth,
    TIMESTAMPDIFF(YEAR, p.date_of_birth, CURDATE()) AS age,
    CASE
        WHEN TIMESTAMPDIFF(YEAR, p.date_of_birth, CURDATE()) < 18 THEN '0-17'
        WHEN TIMESTAMPDIFF(YEAR, p.date_of_birth, CURDATE()) BETWEEN 18 AND 34 THEN '18-34'
        WHEN TIMESTAMPDIFF(YEAR, p.date_of_birth, CURDATE()) BETWEEN 35 AND 54 THEN '35-54'
        WHEN TIMESTAMPDIFF(YEAR, p.date_of_birth, CURDATE()) BETWEEN 55 AND 74 THEN '55-74'
        ELSE '75+'
    END AS age_group,
    p.gender,
    CURDATE() AS effective_date,
    '9999-12-31' AS expiration_date,
    TRUE AS is_current
FROM patients p
WHERE NOT EXISTS (
        SELECT 1
        FROM dim_patient d
        WHERE d.patient_id = p.patient_id
    );
-- -----------------------------------------------------
-- dim_provider: SCD Type 2 (History Tracking)
-- Strategy: Timestamp-Based Change Detection + Denormalization
--   1. Expire changed records (identified by last_update > @last_etl_timestamp)
--   2. Insert new versions with DENORMALIZED specialty/department data
--   3. Insert brand new providers with denormalized data
-- NO TEMPORARY TABLES USED
-- NO FOREIGN KEYS (specialty_id, department_id removed from dim_provider)
-- -----------------------------------------------------
-- Step 1: Expire changed providers (detected via timestamp)
UPDATE dim_provider d
    INNER JOIN providers p ON d.provider_id = p.provider_id
SET d.is_current = FALSE,
    d.expiration_date = CURDATE()
WHERE d.is_current = TRUE
    AND p.last_update > @last_etl_timestamp;
-- Timestamp-based change detection
-- Step 2: Insert new versions for changed providers WITH DENORMALIZED DATA
INSERT INTO dim_provider (
        provider_id,
        first_name,
        last_name,
        full_name,
        credential,
        specialty_name,
        specialty_code,
        specialty_category,
        department_name,
        department_floor,
        department_type,
        effective_date,
        expiration_date,
        is_current
    )
SELECT p.provider_id,
    p.first_name,
    p.last_name,
    CONCAT(p.first_name, ' ', p.last_name) AS full_name,
    p.credential,
    -- Denormalize specialty attributes
    s.specialty_name,
    s.specialty_code,
    CASE
        WHEN s.specialty_code IN ('SURG', 'ORTH', 'NEUR') THEN 'Surgical'
        WHEN s.specialty_code IN ('RAD', 'PATH', 'ANES') THEN 'Diagnostic/Support'
        ELSE 'Medical'
    END AS specialty_category,
    -- Denormalize department attributes
    dept.department_name,
    dept.floor AS department_floor,
    CASE
        WHEN dept.department_name LIKE '%ICU%'
        OR dept.department_name LIKE '%Inpatient%' THEN 'Inpatient'
        WHEN dept.department_name LIKE '%Clinic%'
        OR dept.department_name LIKE '%Outpatient%' THEN 'Outpatient'
        WHEN dept.department_name LIKE '%Emergency%' THEN 'ER'
        WHEN dept.department_name LIKE '%Surgical%' THEN 'Surgical'
        ELSE 'Other'
    END AS department_type,
    CURDATE() AS effective_date,
    '9999-12-31' AS expiration_date,
    TRUE AS is_current
FROM providers p
    LEFT JOIN specialties s ON p.specialty_id = s.specialty_id
    LEFT JOIN departments dept ON p.department_id = dept.department_id
    INNER JOIN dim_provider d ON p.provider_id = d.provider_id
WHERE d.expiration_date = CURDATE() -- Just expired today (from Step 1)
    AND d.is_current = FALSE;
-- Step 3: Insert brand NEW providers WITH DENORMALIZED DATA
INSERT INTO dim_provider (
        provider_id,
        first_name,
        last_name,
        full_name,
        credential,
        specialty_name,
        specialty_code,
        specialty_category,
        department_name,
        department_floor,
        department_type,
        effective_date,
        expiration_date,
        is_current
    )
SELECT p.provider_id,
    p.first_name,
    p.last_name,
    CONCAT(p.first_name, ' ', p.last_name) AS full_name,
    p.credential,
    -- Denormalize specialty attributes
    s.specialty_name,
    s.specialty_code,
    CASE
        WHEN s.specialty_code IN ('SURG', 'ORTH', 'NEUR') THEN 'Surgical'
        WHEN s.specialty_code IN ('RAD', 'PATH', 'ANES') THEN 'Diagnostic/Support'
        ELSE 'Medical'
    END AS specialty_category,
    -- Denormalize department attributes
    dept.department_name,
    dept.floor AS department_floor,
    CASE
        WHEN dept.department_name LIKE '%ICU%'
        OR dept.department_name LIKE '%Inpatient%' THEN 'Inpatient'
        WHEN dept.department_name LIKE '%Clinic%'
        OR dept.department_name LIKE '%Outpatient%' THEN 'Outpatient'
        WHEN dept.department_name LIKE '%Emergency%' THEN 'ER'
        WHEN dept.department_name LIKE '%Surgical%' THEN 'Surgical'
        ELSE 'Other'
    END AS department_type,
    CURDATE() AS effective_date,
    '9999-12-31' AS expiration_date,
    TRUE AS is_current
FROM providers p
    LEFT JOIN specialties s ON p.specialty_id = s.specialty_id
    LEFT JOIN departments dept ON p.department_id = dept.department_id
WHERE NOT EXISTS (
        SELECT 1
        FROM dim_provider d
        WHERE d.provider_id = p.provider_id
    );
-- -----------------------------------------------------
-- dim_encounter_type: Upsert (Static dimension)
-- -----------------------------------------------------
INSERT INTO dim_encounter_type (
        encounter_type,
        encounter_type_category,
        expected_los_days
    )
VALUES ('Outpatient', 'Ambulatory', 0),
    ('Inpatient', 'Acute', 5),
    ('ER', 'Emergency', 1) ON DUPLICATE KEY
UPDATE encounter_type_category =
VALUES(encounter_type_category),
    expected_los_days =
VALUES(expected_los_days);
-- -----------------------------------------------------
-- dim_diagnosis: Upsert (SCD Type 1)
-- -----------------------------------------------------
INSERT INTO dim_diagnosis (
        diagnosis_id,
        icd10_code,
        icd10_description,
        icd10_category
    )
SELECT diagnosis_id,
    icd10_code,
    icd10_description,
    SUBSTRING(icd10_code, 1, 1) AS icd10_category
FROM diagnoses ON DUPLICATE KEY
UPDATE icd10_code =
VALUES(icd10_code),
    icd10_description =
VALUES(icd10_description),
    icd10_category =
VALUES(icd10_category);
-- -----------------------------------------------------
-- dim_procedure: Upsert (SCD Type 1)
-- -----------------------------------------------------
INSERT INTO dim_procedure (
        procedure_id,
        cpt_code,
        cpt_description,
        procedure_category
    )
SELECT procedure_id,
    cpt_code,
    cpt_description,
    CASE
        WHEN cpt_code LIKE '99%' THEN 'Evaluation & Management'
        WHEN cpt_code LIKE '9%' THEN 'Medicine'
        WHEN cpt_code LIKE '7%' THEN 'Radiology'
        WHEN cpt_code LIKE '8%' THEN 'Laboratory'
        ELSE 'Procedure'
    END AS procedure_category
FROM procedures ON DUPLICATE KEY
UPDATE cpt_code =
VALUES(cpt_code),
    cpt_description =
VALUES(cpt_description),
    procedure_category =
VALUES(procedure_category);
COMMIT;
-- Update dimension ETL log
UPDATE etl_log
SET end_time = NOW(),
    status = 'SUCCESS',
    rows_affected = (
        SELECT COUNT(*)
        FROM dim_patient
        WHERE is_current = TRUE
    )
WHERE log_id = @dim_log_id;
-- =====================================================
-- SECTION 2: FACT TABLE INCREMENTAL LOAD
-- Strategy: Only load encounters AFTER the high watermark (by DATE)
-- =====================================================
START TRANSACTION;
INSERT INTO etl_log (etl_step, status)
VALUES ('load_fact_incremental', 'RUNNING');
SET @fact_log_id = LAST_INSERT_ID();
-- Incremental Fact Load: Encounters > @last_watermark
INSERT INTO fact_encounters (
        date_key,
        patient_key,
        provider_key,
        specialty_key,
        department_key,
        encounter_type_key,
        encounter_id,
        patient_id,
        provider_id,
        encounter_datetime,
        discharge_datetime,
        diagnosis_count,
        procedure_count,
        length_of_stay_days,
        total_claim_amount,
        total_allowed_amount,
        has_billing
    )
SELECT dd.date_key,
    dp.patient_key,
    dpr.provider_key,
    ds.specialty_key,
    ddept.department_key,
    det.encounter_type_key,
    e.encounter_id,
    e.patient_id,
    e.provider_id,
    e.encounter_date AS encounter_datetime,
    e.discharge_date AS discharge_datetime,
    COALESCE(diag_cnt.cnt, 0) AS diagnosis_count,
    COALESCE(proc_cnt.cnt, 0) AS procedure_count,
    DATEDIFF(e.discharge_date, e.encounter_date) AS length_of_stay_days,
    b.claim_amount AS total_claim_amount,
    b.allowed_amount AS total_allowed_amount,
    CASE
        WHEN b.billing_id IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS has_billing
FROM encounters e -- ===========================================================
    -- INCREMENTAL FILTER: Only load NEW encounters based on DATE
    -- ===========================================================
    LEFT JOIN fact_encounters existing ON e.encounter_id = existing.encounter_id
    INNER JOIN dim_date dd ON DATE(e.encounter_date) = dd.calendar_date
    INNER JOIN dim_patient dp ON e.patient_id = dp.patient_id
    AND dp.is_current = TRUE
    INNER JOIN dim_provider dpr ON e.provider_id = dpr.provider_id
    AND dpr.is_current = TRUE
    INNER JOIN dim_specialty ds ON dpr.specialty_code = ds.specialty_code
    INNER JOIN dim_department ddept ON e.department_id = ddept.department_id
    INNER JOIN dim_encounter_type det ON e.encounter_type = det.encounter_type
    LEFT JOIN billing b ON e.encounter_id = b.encounter_id
    LEFT JOIN (
        SELECT encounter_id,
            COUNT(*) AS cnt
        FROM encounter_diagnoses
        GROUP BY encounter_id
    ) diag_cnt ON e.encounter_id = diag_cnt.encounter_id
    LEFT JOIN (
        SELECT encounter_id,
            COUNT(*) AS cnt
        FROM encounter_procedures
        GROUP BY encounter_id
    ) proc_cnt ON e.encounter_id = proc_cnt.encounter_id
WHERE existing.encounter_id IS NULL -- Not already loaded
    AND (
        e.encounter_date > @last_watermark -- After last successful loaded DATE
        OR @last_watermark = '1900-01-01 00:00:00' -- First run: load all
    );
-- Store count of newly loaded records
SET @new_fact_count = ROW_COUNT();
-- CALCULATE NEW HIGH WATERMARK
SET @new_watermark_value = (
        SELECT MAX(encounter_datetime)
        FROM fact_encounters
        WHERE created_date >= @etl_start_time
    );
-- If nothing loaded, keep old watermark
SET @final_watermark = COALESCE(@new_watermark_value, @last_watermark);
-- Compute is_readmission flag for ALL encounters (including newly loaded)
-- A new encounter might trigger readmission for a previous one
-- NO TEMPORARY TABLES - Direct timestamp-based query
SET SQL_SAFE_UPDATES = 0;
-- Reset readmission flags for patients with NEW encounters in this ETL batch
-- Use derived table wrapper to avoid MySQL error 1093
UPDATE fact_encounters
SET is_readmission = FALSE
WHERE patient_id IN (
        SELECT patient_id
        FROM (
                SELECT DISTINCT patient_id
                FROM fact_encounters
                WHERE created_date >= @etl_start_time
            ) AS affected_patients
    );
-- Directly compute and set readmission flag (no temp tables)
UPDATE fact_encounters f1
    INNER JOIN fact_encounters f2 ON f1.patient_id = f2.patient_id
    AND f1.encounter_datetime > f2.discharge_datetime
    AND DATEDIFF(f1.encounter_datetime, f2.discharge_datetime) <= 30
    INNER JOIN dim_encounter_type det1 ON f1.encounter_type_key = det1.encounter_type_key
    INNER JOIN dim_encounter_type det2 ON f2.encounter_type_key = det2.encounter_type_key
SET f1.is_readmission = TRUE
WHERE det1.encounter_type = 'Inpatient'
    AND det2.encounter_type = 'Inpatient'
    AND f1.patient_id IN (
        SELECT patient_id
        FROM (
                SELECT DISTINCT patient_id
                FROM fact_encounters
                WHERE created_date >= @etl_start_time
            ) AS affected_patients2
    );
SET SQL_SAFE_UPDATES = 1;
COMMIT;
-- Update fact ETL log
UPDATE etl_log
SET end_time = NOW(),
    status = 'SUCCESS',
    rows_affected = @new_fact_count
WHERE log_id = @fact_log_id;
-- =====================================================
-- SECTION 3: BRIDGE TABLE INCREMENTAL LOAD
-- Strategy: Only load bridges for newly inserted facts
-- =====================================================
START TRANSACTION;
INSERT INTO etl_log (etl_step, status)
VALUES ('load_bridge_incremental', 'RUNNING');
SET @bridge_log_id = LAST_INSERT_ID();
-- Load bridge_encounter_diagnoses (Only for new facts)
INSERT IGNORE INTO bridge_encounter_diagnoses (
        encounter_key,
        diagnosis_key,
        diagnosis_sequence,
        diagnosis_date
    )
SELECT f.encounter_key,
    dd.diagnosis_key,
    ed.diagnosis_sequence,
    DATE(f.encounter_datetime) AS diagnosis_date
FROM encounter_diagnoses ed
    INNER JOIN fact_encounters f ON ed.encounter_id = f.encounter_id
    INNER JOIN dim_diagnosis dd ON ed.diagnosis_id = dd.diagnosis_id
WHERE f.created_date >= @etl_start_time;
-- Only new facts
-- Load bridge_encounter_procedures (Only for new facts)
INSERT IGNORE INTO bridge_encounter_procedures (
        encounter_key,
        procedure_key,
        procedure_sequence,
        procedure_date
    )
SELECT f.encounter_key,
    dp.procedure_key,
    ROW_NUMBER() OVER (
        PARTITION BY ep.encounter_id
        ORDER BY ep.procedure_date
    ) AS procedure_sequence,
    ep.procedure_date
FROM encounter_procedures ep
    INNER JOIN fact_encounters f ON ep.encounter_id = f.encounter_id
    INNER JOIN dim_procedure dp ON ep.procedure_id = dp.procedure_id
WHERE f.created_date >= @etl_start_time;
-- Only new facts
COMMIT;
-- Update bridge ETL log
UPDATE etl_log
SET end_time = NOW(),
    status = 'SUCCESS',
    rows_affected = (
        SELECT COUNT(*)
        FROM bridge_encounter_diagnoses
        WHERE diagnosis_date >= DATE(@etl_start_time)
    )
WHERE log_id = @bridge_log_id;
-- =====================================================
-- SECTION 4: POST-ETL RECONCILIATION & METADATA UPDATE
-- =====================================================
-- Run reconciliation checks
CALL reconcile_etl_data();
-- Update ETL Metadata with NEW High Watermark (Encounter Date) AND Timestamp Watermark
INSERT INTO etl_metadata (
        table_name,
        last_etl_timestamp,
        high_watermark,
        last_etl_status,
        rows_processed
    )
VALUES (
        'fact_encounters',
        NOW(),
        @final_watermark,
        'SUCCESS',
        @new_fact_count
    ),
    (
        'dim_patient',
        NOW(),
        NOW(),
        -- Store current timestamp as watermark for SCD Type 2
        'SUCCESS',
        (
            SELECT COUNT(*)
            FROM dim_patient
            WHERE is_current = TRUE
        )
    ),
    (
        'dim_provider',
        NOW(),
        NOW(),
        -- Store current timestamp as watermark for SCD Type 2
        'SUCCESS',
        (
            SELECT COUNT(*)
            FROM dim_provider
            WHERE is_current = TRUE
        )
    ) ON DUPLICATE KEY
UPDATE last_etl_timestamp = NOW(),
    high_watermark =
VALUES(high_watermark),
    last_etl_status = 'SUCCESS',
    rows_processed =
VALUES(rows_processed);
-- Complete main ETL log entry
UPDATE etl_log
SET end_time = NOW(),
    status = 'SUCCESS',
    rows_affected = @new_fact_count
WHERE log_id = @batch_log_id;
-- =====================================================
-- SECTION 5: ETL SUMMARY REPORT
-- =====================================================
SELECT 'INCREMENTAL ETL COMPLETED' AS status,
    @last_watermark AS previous_watermark,
    @final_watermark AS new_watermark,
    @new_fact_count AS new_encounters_loaded,
    (
        SELECT COUNT(*)
        FROM fact_encounters
    ) AS total_encounters,
    (
        SELECT COUNT(*)
        FROM dim_patient
        WHERE is_current = TRUE
    ) AS active_patients,
    (
        SELECT COUNT(*)
        FROM dim_patient
        WHERE is_current = FALSE
    ) AS historical_patient_versions,
    (
        SELECT COUNT(*)
        FROM dim_provider
        WHERE is_current = TRUE
    ) AS active_providers,
    (
        SELECT SUM(total_allowed_amount)
        FROM fact_encounters
    ) AS total_revenue,
    (
        SELECT COUNT(*)
        FROM fact_encounters
        WHERE is_readmission = TRUE
    ) AS total_readmissions,
    TIMEDIFF(NOW(), @etl_start_time) AS execution_time;