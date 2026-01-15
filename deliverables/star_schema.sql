-- =====================================================
-- HEALTHCARE ANALYTICS LAB - STAR SCHEMA DDL
-- Optimized Dimensional Model for Analytics
-- =====================================================
-- Grain: One row per encounter | 8 Dimensions + 1 Fact + 2 Bridges
-- =====================================================

DROP TABLE IF EXISTS bridge_encounter_procedures;
DROP TABLE IF EXISTS bridge_encounter_diagnoses;
DROP TABLE IF EXISTS fact_encounters;
DROP TABLE IF EXISTS dim_encounter_type;
DROP TABLE IF EXISTS dim_department;
DROP TABLE IF EXISTS dim_specialty;
DROP TABLE IF EXISTS dim_provider;
DROP TABLE IF EXISTS dim_patient;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_diagnosis;
DROP TABLE IF EXISTS dim_procedure;

-- =====================================================
-- DIMENSION: dim_date
-- =====================================================

CREATE TABLE dim_date (
    date_key INT AUTO_INCREMENT PRIMARY KEY,
    calendar_date DATE NOT NULL UNIQUE,
    `year` INT NOT NULL,
    `quarter` INT NOT NULL,
    quarter_name VARCHAR(2),
    `month` INT NOT NULL,
    month_name VARCHAR(20),
    `year_month` VARCHAR(7) NOT NULL,
    week_of_year INT,
    day_of_month INT,
    day_of_week INT,
    day_name VARCHAR(20),
    is_weekend BOOLEAN DEFAULT FALSE,
    is_holiday BOOLEAN DEFAULT FALSE,
    INDEX idx_calendar_date (calendar_date),
    INDEX idx_year_month (`year_month`)
);

-- =====================================================
-- DIMENSION: dim_patient
-- =====================================================

CREATE TABLE dim_patient (
    patient_key INT AUTO_INCREMENT PRIMARY KEY,
    patient_id INT NOT NULL,
    mrn VARCHAR(20) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(200),
    date_of_birth DATE,
    age INT,
    age_group VARCHAR(20),
    gender CHAR(1),
    effective_date DATE DEFAULT (CURRENT_DATE),
    expiration_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,
    INDEX idx_patient_id (patient_id),
    INDEX idx_mrn (mrn),
    INDEX idx_is_current (is_current)
);

-- =====================================================
-- DIMENSION: dim_specialty
-- =====================================================

CREATE TABLE dim_specialty (
    specialty_key INT AUTO_INCREMENT PRIMARY KEY,
    specialty_id INT NOT NULL UNIQUE,
    specialty_name VARCHAR(100) NOT NULL,
    specialty_code VARCHAR(10),
    specialty_category VARCHAR(50),
    INDEX idx_specialty_id (specialty_id)
);

-- =====================================================
-- DIMENSION: dim_department
-- =====================================================

CREATE TABLE dim_department (
    department_key INT AUTO_INCREMENT PRIMARY KEY,
    department_id INT NOT NULL UNIQUE,
    department_name VARCHAR(100) NOT NULL,
    floor INT,
    capacity INT,
    department_type VARCHAR(50),
    INDEX idx_department_id (department_id)
);

-- =====================================================
-- DIMENSION: dim_provider (DENORMALIZED)
-- =====================================================

CREATE TABLE dim_provider (
    provider_key INT AUTO_INCREMENT PRIMARY KEY,
    provider_id INT NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(200),
    credential VARCHAR(20),
    specialty_id INT,
    specialty_name VARCHAR(100),
    specialty_code VARCHAR(10),
    department_id INT,
    department_name VARCHAR(100),
    effective_date DATE DEFAULT (CURRENT_DATE),
    expiration_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,
    INDEX idx_provider_id (provider_id),
    INDEX idx_specialty_id (specialty_id),
    INDEX idx_is_current (is_current)
);

-- =====================================================
-- DIMENSION: dim_encounter_type
-- =====================================================

CREATE TABLE dim_encounter_type (
    encounter_type_key INT AUTO_INCREMENT PRIMARY KEY,
    encounter_type VARCHAR(50) NOT NULL UNIQUE,
    encounter_type_category VARCHAR(50),
    expected_los_days INT,
    INDEX idx_encounter_type (encounter_type)
);

-- =====================================================
-- DIMENSION: dim_diagnosis
-- =====================================================

CREATE TABLE dim_diagnosis (
    diagnosis_key INT AUTO_INCREMENT PRIMARY KEY,
    diagnosis_id INT NOT NULL UNIQUE,
    icd10_code VARCHAR(10) NOT NULL,
    icd10_description VARCHAR(200),
    icd10_category VARCHAR(100),
    INDEX idx_diagnosis_id (diagnosis_id),
    INDEX idx_icd10_code (icd10_code)
);

-- =====================================================
-- DIMENSION: dim_procedure
-- =====================================================

CREATE TABLE dim_procedure (
    procedure_key INT AUTO_INCREMENT PRIMARY KEY,
    procedure_id INT NOT NULL UNIQUE,
    cpt_code VARCHAR(10) NOT NULL,
    cpt_description VARCHAR(200),
    procedure_category VARCHAR(100),
    INDEX idx_procedure_id (procedure_id),
    INDEX idx_cpt_code (cpt_code)
);

-- =====================================================
-- FACT TABLE: fact_encounters
-- =====================================================

CREATE TABLE fact_encounters (
    encounter_key INT AUTO_INCREMENT PRIMARY KEY,
    date_key INT NOT NULL,
    patient_key INT NOT NULL,
    provider_key INT NOT NULL,
    specialty_key INT NOT NULL,
    department_key INT NOT NULL,
    encounter_type_key INT NOT NULL,
    encounter_id INT NOT NULL UNIQUE,
    patient_id INT,
    provider_id INT,
    encounter_datetime DATETIME,
    discharge_datetime DATETIME,
    diagnosis_count INT DEFAULT 0,
    procedure_count INT DEFAULT 0,
    length_of_stay_days INT,
    total_claim_amount DECIMAL(12, 2),
    total_allowed_amount DECIMAL(12, 2),
    is_readmission BOOLEAN DEFAULT FALSE,
    has_billing BOOLEAN DEFAULT FALSE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_fact_date FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    CONSTRAINT fk_fact_patient FOREIGN KEY (patient_key) REFERENCES dim_patient(patient_key),
    CONSTRAINT fk_fact_provider FOREIGN KEY (provider_key) REFERENCES dim_provider(provider_key),
    CONSTRAINT fk_fact_specialty FOREIGN KEY (specialty_key) REFERENCES dim_specialty(specialty_key),
    CONSTRAINT fk_fact_department FOREIGN KEY (department_key) REFERENCES dim_department(department_key),
    CONSTRAINT fk_fact_encounter_type FOREIGN KEY (encounter_type_key) REFERENCES dim_encounter_type(encounter_type_key),
    INDEX idx_date_key (date_key),
    INDEX idx_patient_key (patient_key),
    INDEX idx_provider_key (provider_key),
    INDEX idx_specialty_key (specialty_key),
    INDEX idx_encounter_type_key (encounter_type_key),
    INDEX idx_encounter_datetime (encounter_datetime),
    INDEX idx_is_readmission (is_readmission),
    INDEX idx_date_specialty (date_key, specialty_key),
    INDEX idx_date_encounter_type (date_key, encounter_type_key),
    INDEX idx_specialty_encounter_type (specialty_key, encounter_type_key)
);

-- =====================================================
-- BRIDGE: bridge_encounter_diagnoses
-- =====================================================

CREATE TABLE bridge_encounter_diagnoses (
    encounter_key INT NOT NULL,
    diagnosis_key INT NOT NULL,
    diagnosis_sequence INT,
    diagnosis_date DATE,
    PRIMARY KEY (encounter_key, diagnosis_key),
    CONSTRAINT fk_bridge_diag_encounter FOREIGN KEY (encounter_key) REFERENCES fact_encounters(encounter_key),
    CONSTRAINT fk_bridge_diag_diagnosis FOREIGN KEY (diagnosis_key) REFERENCES dim_diagnosis(diagnosis_key),
    INDEX idx_encounter_key (encounter_key),
    INDEX idx_diagnosis_key (diagnosis_key)
);

-- =====================================================
-- BRIDGE: bridge_encounter_procedures
-- =====================================================

CREATE TABLE bridge_encounter_procedures (
    encounter_key INT NOT NULL,
    procedure_key INT NOT NULL,
    procedure_sequence INT,
    procedure_date DATE,
    PRIMARY KEY (encounter_key, procedure_key),
    CONSTRAINT fk_bridge_proc_encounter FOREIGN KEY (encounter_key) REFERENCES fact_encounters(encounter_key),
    CONSTRAINT fk_bridge_proc_procedure FOREIGN KEY (procedure_key) REFERENCES dim_procedure(procedure_key),
    INDEX idx_encounter_key (encounter_key),
    INDEX idx_procedure_key (procedure_key)
);

-- =====================================================
-- ETL: POPULATE DIMENSIONS
-- =====================================================

-- Load dim_date (2024 calendar year)
INSERT INTO dim_date (calendar_date, `year`, `quarter`, quarter_name, `month`, month_name, `year_month`, week_of_year, day_of_month, day_of_week, day_name, is_weekend)
WITH RECURSIVE date_range AS (
    SELECT DATE('2024-01-01') AS dt
    UNION ALL
    SELECT DATE_ADD(dt, INTERVAL 1 DAY)
    FROM date_range
    WHERE dt < '2024-12-31'
)
SELECT 
    dt AS calendar_date,
    YEAR(dt) AS `year`,
    QUARTER(dt) AS `quarter`,
    CONCAT('Q', QUARTER(dt)) AS quarter_name,
    MONTH(dt) AS `month`,
    DATE_FORMAT(dt, '%M') AS month_name,
    DATE_FORMAT(dt, '%Y-%m') AS `year_month`,
    WEEK(dt, 1) AS week_of_year,
    DAY(dt) AS day_of_month,
    DAYOFWEEK(dt) AS day_of_week,
    DATE_FORMAT(dt, '%W') AS day_name,
    DAYOFWEEK(dt) IN (1, 7) AS is_weekend
FROM date_range;

-- Load dim_specialty from OLTP
INSERT INTO dim_specialty (specialty_id, specialty_name, specialty_code, specialty_category)
SELECT 
    specialty_id,
    specialty_name,
    specialty_code,
    CASE 
        WHEN specialty_code IN ('SURG', 'ORTH', 'NEUR') THEN 'Surgical'
        WHEN specialty_code IN ('RAD', 'PATH', 'ANES') THEN 'Diagnostic/Support'
        ELSE 'Medical'
    END AS specialty_category
FROM specialties;

-- Load dim_department from OLTP
INSERT INTO dim_department (department_id, department_name, floor, capacity, department_type)
SELECT 
    department_id,
    department_name,
    floor,
    capacity,
    CASE 
        WHEN department_name LIKE '%ICU%' OR department_name LIKE '%Inpatient%' THEN 'Inpatient'
        WHEN department_name LIKE '%Clinic%' OR department_name LIKE '%Outpatient%' THEN 'Outpatient'
        WHEN department_name LIKE '%Emergency%' THEN 'ER'
        WHEN department_name LIKE '%Surgical%' THEN 'Surgical'
        ELSE 'Other'
    END AS department_type
FROM departments;

-- Load dim_patient from OLTP with age calculations
INSERT INTO dim_patient (patient_id, mrn, first_name, last_name, full_name, date_of_birth, age, age_group, gender, is_current)
SELECT 
    patient_id,
    mrn,
    first_name,
    last_name,
    CONCAT(first_name, ' ', last_name) AS full_name,
    date_of_birth,
    TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) AS age,
    CASE 
        WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) < 18 THEN '0-17'
        WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) BETWEEN 18 AND 34 THEN '18-34'
        WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) BETWEEN 35 AND 54 THEN '35-54'
        WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) BETWEEN 55 AND 74 THEN '55-74'
        ELSE '75+'
    END AS age_group,
    gender,
    TRUE AS is_current
FROM patients;

-- Load dim_provider with DENORMALIZED specialty and department
INSERT INTO dim_provider (provider_id, first_name, last_name, full_name, credential, specialty_id, specialty_name, specialty_code, department_id, department_name, is_current)
SELECT 
    p.provider_id,
    p.first_name,
    p.last_name,
    CONCAT(p.first_name, ' ', p.last_name) AS full_name,
    p.credential,
    p.specialty_id,
    s.specialty_name,
    s.specialty_code,
    p.department_id,
    d.department_name,
    TRUE AS is_current
FROM providers p
LEFT JOIN specialties s ON p.specialty_id = s.specialty_id
LEFT JOIN departments d ON p.department_id = d.department_id;

-- Load dim_encounter_type (static values)
INSERT INTO dim_encounter_type (encounter_type, encounter_type_category, expected_los_days)
VALUES 
    ('Outpatient', 'Ambulatory', 0),
    ('Inpatient', 'Acute', 5),
    ('ER', 'Emergency', 1);

-- Load dim_diagnosis from OLTP
INSERT INTO dim_diagnosis (diagnosis_id, icd10_code, icd10_description, icd10_category)
SELECT 
    diagnosis_id,
    icd10_code,
    icd10_description,
    SUBSTRING(icd10_code, 1, 1) AS icd10_category
FROM diagnoses;

-- Load dim_procedure from OLTP
INSERT INTO dim_procedure (procedure_id, cpt_code, cpt_description, procedure_category)
SELECT 
    procedure_id,
    cpt_code,
    cpt_description,
    CASE 
        WHEN cpt_code LIKE '99%' THEN 'Evaluation & Management'
        WHEN cpt_code LIKE '9%' THEN 'Medicine'
        WHEN cpt_code LIKE '7%' THEN 'Radiology'
        WHEN cpt_code LIKE '8%' THEN 'Laboratory'
        ELSE 'Procedure'
    END AS procedure_category
FROM procedures;

-- =====================================================
-- ETL: POPULATE FACT TABLE
-- =====================================================

INSERT INTO fact_encounters (
    date_key, patient_key, provider_key, specialty_key, department_key, encounter_type_key,
    encounter_id, patient_id, provider_id,
    encounter_datetime, discharge_datetime,
    diagnosis_count, procedure_count, length_of_stay_days,
    total_claim_amount, total_allowed_amount, has_billing
)
SELECT 
    dd.date_key,
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
    CASE WHEN b.billing_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_billing
FROM encounters e
INNER JOIN dim_date dd ON DATE(e.encounter_date) = dd.calendar_date
INNER JOIN dim_patient dp ON e.patient_id = dp.patient_id AND dp.is_current = TRUE
INNER JOIN dim_provider dpr ON e.provider_id = dpr.provider_id AND dpr.is_current = TRUE
INNER JOIN dim_specialty ds ON dpr.specialty_id = ds.specialty_id
INNER JOIN dim_department ddept ON e.department_id = ddept.department_id
INNER JOIN dim_encounter_type det ON e.encounter_type = det.encounter_type
LEFT JOIN billing b ON e.encounter_id = b.encounter_id
LEFT JOIN (
    SELECT encounter_id, COUNT(*) AS cnt 
    FROM encounter_diagnoses 
    GROUP BY encounter_id
) diag_cnt ON e.encounter_id = diag_cnt.encounter_id
LEFT JOIN (
    SELECT encounter_id, COUNT(*) AS cnt 
    FROM encounter_procedures 
    GROUP BY encounter_id
) proc_cnt ON e.encounter_id = proc_cnt.encounter_id;

-- Compute is_readmission flag (30-day readmission)
SET SQL_SAFE_UPDATES = 0;

UPDATE fact_encounters fe
INNER JOIN (
    SELECT DISTINCT f1.encounter_key
    FROM fact_encounters f1
    INNER JOIN fact_encounters f2 
        ON f1.patient_id = f2.patient_id
        AND f1.encounter_datetime > f2.discharge_datetime
        AND DATEDIFF(f1.encounter_datetime, f2.discharge_datetime) <= 30
    INNER JOIN dim_encounter_type det1 ON f1.encounter_type_key = det1.encounter_type_key
    INNER JOIN dim_encounter_type det2 ON f2.encounter_type_key = det2.encounter_type_key
    WHERE det1.encounter_type = 'Inpatient' AND det2.encounter_type = 'Inpatient'
) readmissions ON fe.encounter_key = readmissions.encounter_key
SET fe.is_readmission = TRUE;

SET SQL_SAFE_UPDATES = 1;

-- =====================================================
-- ETL: POPULATE BRIDGE TABLES
-- =====================================================

-- Load bridge_encounter_diagnoses
INSERT INTO bridge_encounter_diagnoses (encounter_key, diagnosis_key, diagnosis_sequence, diagnosis_date)
SELECT 
    f.encounter_key,
    dd.diagnosis_key,
    ed.diagnosis_sequence,
    DATE(f.encounter_datetime) AS diagnosis_date
FROM encounter_diagnoses ed
INNER JOIN fact_encounters f ON ed.encounter_id = f.encounter_id
INNER JOIN dim_diagnosis dd ON ed.diagnosis_id = dd.diagnosis_id;

-- Load bridge_encounter_procedures
INSERT INTO bridge_encounter_procedures (encounter_key, procedure_key, procedure_sequence, procedure_date)
SELECT 
    f.encounter_key,
    dp.procedure_key,
    ROW_NUMBER() OVER (PARTITION BY ep.encounter_id ORDER BY ep.procedure_date) AS procedure_sequence,
    ep.procedure_date
FROM encounter_procedures ep
INNER JOIN fact_encounters f ON ep.encounter_id = f.encounter_id
INNER JOIN dim_procedure dp ON ep.procedure_id = dp.procedure_id;

