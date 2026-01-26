-- ================================================================
-- Healthcare Analytics Lab: Incremental Data Generator
-- Purpose: Add 3,000 NEW records to simulate transactional growth
-- Database: MySQL
-- Usage: Run AFTER generated_10k_sample_data.sql
-- ================================================================
USE healthcare_analytics_lab;
-- Increase recursion depth for CTEs
SET SESSION cte_max_recursion_depth = 5000;

-- ================================================================
-- 1. Generate 3,000 NEW Patients (IDs 15001 - 18000)
-- ================================================================
INSERT INTO patients (
        patient_id,
        first_name,
        last_name,
        date_of_birth,
        gender,
        mrn
    ) WITH RECURSIVE numbers AS (
        SELECT 1 AS n
        UNION ALL
        SELECT n + 1
        FROM numbers
        WHERE n < 3000
    )
SELECT n + 15000 AS patient_id,
    -- distinct IDs (shifted to avoid collisions with readmission data)
    CASE
        WHEN n % 40 = 1 THEN 'Kafui'
        WHEN n % 40 = 2 THEN 'Naa'
        WHEN n % 40 = 3 THEN 'Yooku'
        WHEN n % 40 = 4 THEN 'Aseye'
        WHEN n % 40 = 5 THEN 'Kwaku'
        WHEN n % 40 = 6 THEN 'Efua'
        WHEN n % 40 = 7 THEN 'Selorm'
        WHEN n % 40 = 8 THEN 'Fafali'
        WHEN n % 40 = 9 THEN 'Gifty'
        WHEN n % 40 = 10 THEN 'Borketey'
        WHEN n % 40 = 11 THEN 'Sedinam'
        WHEN n % 40 = 12 THEN 'Korkor'
        WHEN n % 40 = 13 THEN 'Aduke'
        WHEN n % 40 = 14 THEN 'Dzifa'
        WHEN n % 40 = 15 THEN 'Kobby'
        ELSE 'Enyonam'
    END AS first_name,
    CASE
        WHEN n % 30 = 1 THEN 'Oppong'
        WHEN n % 30 = 2 THEN 'Kyeremateng'
        WHEN n % 30 = 3 THEN 'Kumi'
        WHEN n % 30 = 4 THEN 'Sackey'
        WHEN n % 30 = 5 THEN 'Blay'
        WHEN n % 30 = 6 THEN 'Dery'
        WHEN n % 30 = 7 THEN 'Mahama'
        WHEN n % 30 = 8 THEN 'Bawumia'
        WHEN n % 30 = 9 THEN 'Darko'
        ELSE 'Kusi'
    END AS last_name,
    DATE_ADD('1950-01-01', INTERVAL FLOOR(RAND() * 25000) DAY) AS date_of_birth,
    IF(n % 2 = 0, 'F', 'M') AS gender,
    CONCAT('GHA-', LPAD(n + 15000, 7, '0')) AS mrn
FROM numbers;
-- ================================================================
-- 2. Generate 3,000 NEW Encounters (IDs 15001 - 18000)
-- Note: Dates are set in 2025 to simulate new year data
-- ================================================================
INSERT INTO encounters (
        encounter_id,
        patient_id,
        provider_id,
        encounter_type,
        encounter_date,
        discharge_date,
        department_id
    ) WITH RECURSIVE numbers AS (
        SELECT 1 AS n
        UNION ALL
        SELECT n + 1
        FROM numbers
        WHERE n < 3000
    ),
    base_encounters AS (
        SELECT n,
            CASE
                WHEN n % 3 = 1 THEN 'Outpatient'
                WHEN n % 3 = 2 THEN 'Inpatient'
                ELSE 'ER'
            END AS encounter_type,
            -- Generate dates in 2025
            DATE_ADD(
                DATE_ADD('2025-01-01', INTERVAL (n % 365) DAY),
                INTERVAL (n % 24) HOUR
            ) AS encounter_date
        FROM numbers
    )
SELECT n + 15000 AS encounter_id,
    n + 15000 AS patient_id,
    -- Maps 1:1 to new patients (offset to avoid collisions)
    ((n - 1) % 1000) + 1 AS provider_id,
    -- Reuses existing 1000 providers
    encounter_type,
    encounter_date,
    -- Discharge ALWAYS after encounter
    CASE
        WHEN encounter_type = 'Outpatient' THEN DATE_ADD(encounter_date, INTERVAL ((n % 4) + 1) HOUR)
        WHEN encounter_type = 'Inpatient' THEN DATE_ADD(encounter_date, INTERVAL ((n % 10) + 1) DAY)
        ELSE DATE_ADD(encounter_date, INTERVAL ((n % 12) + 1) HOUR)
    END AS discharge_date,
    ((n - 1) % 15) + 1 AS department_id -- Reuses existing 15 departments
FROM base_encounters;
-- ================================================================
-- 3. Generate 3,000 NEW Billing Records (IDs 15001 - 18000)
-- ================================================================
INSERT INTO billing (
        billing_id,
        encounter_id,
        claim_amount,
        allowed_amount,
        claim_date,
        claim_status
    ) WITH RECURSIVE numbers AS (
        SELECT 1 AS n
        UNION ALL
        SELECT n + 1
        FROM numbers
        WHERE n < 3000
    )
SELECT n + 15000 AS billing_id,
    n + 15000 AS encounter_id,
    ROUND(500 + (RAND() * 50000), 2) AS claim_amount,
    ROUND((500 + (RAND() * 50000)) * 0.8, 2) AS allowed_amount,
    DATE_ADD('2025-01-01', INTERVAL FLOOR(RAND() * 365) DAY) AS claim_date,
    CASE
        WHEN n % 10 = 1 THEN 'Pending'
        WHEN n % 10 = 2 THEN 'Denied'
        WHEN n % 10 = 3 THEN 'Under Review'
        ELSE 'Paid'
    END AS claim_status
FROM numbers;
-- ================================================================
-- 4. Generate 3,000 NEW Encounter Diagnoses (IDs 15001 - 18000)
-- ================================================================
INSERT INTO encounter_diagnoses (
        encounter_diagnosis_id,
        encounter_id,
        diagnosis_id,
        diagnosis_sequence
    ) WITH RECURSIVE numbers AS (
        SELECT 1 AS n
        UNION ALL
        SELECT n + 1
        FROM numbers
        WHERE n < 3000
    )
SELECT n + 15000 AS encounter_diagnosis_id,
    n + 15000 AS encounter_id,
    ((n - 1) % 10000) + 1 AS diagnosis_id,
    -- Reuse existing 10,000 diagnoses
    (n % 5) + 1 AS diagnosis_sequence
FROM numbers;
-- ================================================================
-- 5. Generate 3,000 NEW Encounter Procedures (IDs 15001 - 18000)
-- ================================================================
INSERT INTO encounter_procedures (
        encounter_procedure_id,
        encounter_id,
        procedure_id,
        procedure_date
    ) WITH RECURSIVE numbers AS (
        SELECT 1 AS n
        UNION ALL
        SELECT n + 1
        FROM numbers
        WHERE n < 3000
    )
SELECT n + 15000 AS encounter_procedure_id,
    n + 15000 AS encounter_id,
    ((n - 1) % 10000) + 1 AS procedure_id,
    -- Reuse existing 10,000 procedures
    DATE_ADD('2025-01-01', INTERVAL (n % 365) DAY) AS procedure_date
FROM numbers;
SELECT 'Incremental data load complete: 3,000 new records added (IDs 15001-18000)' AS status;