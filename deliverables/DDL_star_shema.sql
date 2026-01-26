-- =====================================================
-- HEALTHCARE ANALYTICS LAB - STAR SCHEMA DDL
-- Dimensional Model for Analytics
-- Features:
--   - SCD Type 2 support (effective_date, expiration_date, is_current)
--   - High Watermark tracking (etl_metadata table)
--   - Incremental Load Ready
-- Grain: One row per encounter | 8 Dimensions + 1 Fact + 2 Bridges
-- =====================================================
-- =====================================================
-- SECTION 0: ETL METADATA & DATA QUALITY INFRASTRUCTURE
-- =====================================================
-- Drop existing tables in dependency order (not done in production)
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
DROP TABLE IF EXISTS etl_metadata;
DROP TABLE IF EXISTS etl_log;
-- Drop existing stored procedures
DROP PROCEDURE IF EXISTS validate_source_data;
DROP PROCEDURE IF EXISTS reconcile_etl_data;
-- ETL Metadata Table: Track incremental load watermarks
CREATE TABLE etl_metadata (
    metadata_id INT AUTO_INCREMENT PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    last_etl_timestamp TIMESTAMP NOT NULL,
    last_etl_status ENUM('SUCCESS', 'FAILED', 'RUNNING', 'NOT_STARTED') DEFAULT 'NOT_STARTED',
    rows_processed INT DEFAULT 0,
    high_watermark DATETIME DEFAULT '1900-01-01 00:00:00',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE INDEX idx_table_name (table_name)
);
-- ETL Log Table: Detailed execution logging
CREATE TABLE etl_log (
    log_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    etl_step VARCHAR(100) NOT NULL,
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP NULL,
    rows_affected INT DEFAULT 0,
    status ENUM('RUNNING', 'SUCCESS', 'FAILED', 'NOT_STARTED') DEFAULT 'NOT_STARTED',
    error_message TEXT NULL,
    INDEX idx_etl_step (etl_step),
    INDEX idx_start_time (start_time),
    INDEX idx_status (status)
);
-- Data Quality Validation Stored Procedure
DELIMITER // 

CREATE PROCEDURE validate_source_data() BEGIN
DECLARE error_count INT DEFAULT 0;
DECLARE error_msg TEXT;
-- Validation 1: No encounters with discharge before admission
SELECT COUNT(*) INTO error_count
FROM encounters
WHERE discharge_date IS NOT NULL
    AND discharge_date < encounter_date;
IF error_count > 0 THEN
SET error_msg = CONCAT(
        'DATA QUALITY FAIL: ',
        error_count,
        ' encounters have discharge_date < encounter_date'
    );
SIGNAL SQLSTATE '45000'
SET MESSAGE_TEXT = error_msg;
END IF;
-- Validation 2: No negative billing amounts
SELECT COUNT(*) INTO error_count
FROM billing
WHERE allowed_amount < 0
    OR claim_amount < 0;
IF error_count > 0 THEN
SET error_msg = CONCAT(
        'DATA QUALITY FAIL: ',
        error_count,
        ' billing records have negative amounts'
    );
SIGNAL SQLSTATE '45000'
SET MESSAGE_TEXT = error_msg;
END IF;
-- Validation 3: All encounters have valid patient references
SELECT COUNT(*) INTO error_count
FROM encounters e
    LEFT JOIN patients p ON e.patient_id = p.patient_id
WHERE p.patient_id IS NULL;
IF error_count > 0 THEN
SET error_msg = CONCAT(
        'DATA QUALITY FAIL: ',
        error_count,
        ' encounters have invalid patient_id (orphaned records)'
    );
SIGNAL SQLSTATE '45000'
SET MESSAGE_TEXT = error_msg;
END IF;
-- Validation 4: All encounters have valid provider references
SELECT COUNT(*) INTO error_count
FROM encounters e
    LEFT JOIN providers p ON e.provider_id = p.provider_id
WHERE p.provider_id IS NULL;
IF error_count > 0 THEN
SET error_msg = CONCAT(
        'DATA QUALITY FAIL: ',
        error_count,
        ' encounters have invalid provider_id'
    );
SIGNAL SQLSTATE '45000'
SET MESSAGE_TEXT = error_msg;
END IF;
-- All validations passed
SELECT 'SUCCESS: All pre-ETL data quality checks passed' AS validation_status;
END // 


CREATE PROCEDURE reconcile_etl_data() BEGIN
DECLARE oltp_encounter_count INT;
DECLARE star_encounter_count INT;
DECLARE oltp_revenue DECIMAL(15, 2);
DECLARE star_revenue DECIMAL(15, 2);
DECLARE revenue_diff DECIMAL(15, 2);
-- Row count reconciliation
SELECT COUNT(*) INTO oltp_encounter_count
FROM encounters;
SELECT COUNT(*) INTO star_encounter_count
FROM fact_encounters;
IF oltp_encounter_count != star_encounter_count THEN SIGNAL SQLSTATE '45000'
SET MESSAGE_TEXT = 'RECONCILIATION FAIL: Row count mismatch between OLTP and Star';
END IF;
-- Revenue reconciliation
SELECT COALESCE(SUM(allowed_amount), 0) INTO oltp_revenue
FROM billing;
SELECT COALESCE(SUM(total_allowed_amount), 0) INTO star_revenue
FROM fact_encounters;
SET revenue_diff = ABS(oltp_revenue - star_revenue);
IF revenue_diff > 0.01 THEN SIGNAL SQLSTATE '45000'
SET MESSAGE_TEXT = 'RECONCILIATION FAIL: Revenue mismatch exceeds tolerance';
END IF;
-- Success message
SELECT 'SUCCESS: ETL Reconciliation Passed' AS status,
    oltp_encounter_count AS oltp_encounters,
    star_encounter_count AS star_encounters,
    oltp_revenue AS oltp_total_revenue,
    star_revenue AS star_total_revenue,
    revenue_diff AS revenue_difference;
END // 

DELIMITER ;

-- =====================================================
-- SECTION 1: DIMENSION TABLES (DDL)
-- =====================================================
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
-- Purpose: Provider information with denormalized specialty/department
-- Note: No foreign keys - all specialty and department data is denormalized
-- =====================================================
CREATE TABLE dim_provider (
    provider_key INT AUTO_INCREMENT PRIMARY KEY,
    provider_id INT NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(200),
    credential VARCHAR(20),
    specialty_name VARCHAR(100),
    specialty_code VARCHAR(10),
    specialty_category VARCHAR(50),
    department_name VARCHAR(100),
    department_floor INT,
    department_type VARCHAR(50),
    effective_date DATE DEFAULT (CURRENT_DATE),
    expiration_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,
    INDEX idx_provider_id (provider_id),
    INDEX idx_is_current (is_current),
    INDEX idx_specialty_name (specialty_name),
    INDEX idx_department_name (department_name)
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
    INDEX idx_specialty_encounter_type (specialty_key, encounter_type_key),
    INDEX idx_created_date (created_date)
);
-- =====================================================
-- BRIDGE: bridge_encounter_diagnoses
-- =====================================================
CREATE TABLE bridge_encounter_diagnoses (
    bridge_id INT AUTO_INCREMENT PRIMARY KEY,
    encounter_key INT NOT NULL,
    diagnosis_key INT NOT NULL,
    diagnosis_sequence INT,
    diagnosis_date DATE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_encounter_diagnosis (encounter_key, diagnosis_key),
    CONSTRAINT fk_bridge_diag_encounter FOREIGN KEY (encounter_key) REFERENCES fact_encounters(encounter_key),
    CONSTRAINT fk_bridge_diag_diagnosis FOREIGN KEY (diagnosis_key) REFERENCES dim_diagnosis(diagnosis_key),
    INDEX idx_encounter_key (encounter_key),
    INDEX idx_diagnosis_key (diagnosis_key),
    INDEX idx_diagnosis_date (diagnosis_date)
);
-- =====================================================
-- BRIDGE: bridge_encounter_procedures
-- =====================================================
CREATE TABLE bridge_encounter_procedures (
    bridge_id INT AUTO_INCREMENT PRIMARY KEY,
    encounter_key INT NOT NULL,
    procedure_key INT NOT NULL,
    procedure_sequence INT,
    procedure_date DATE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_encounter_procedure (encounter_key, procedure_key),
    CONSTRAINT fk_bridge_proc_encounter FOREIGN KEY (encounter_key) REFERENCES fact_encounters(encounter_key),
    CONSTRAINT fk_bridge_proc_procedure FOREIGN KEY (procedure_key) REFERENCES dim_procedure(procedure_key),
    INDEX idx_encounter_key (encounter_key),
    INDEX idx_procedure_key (procedure_key),
    INDEX idx_procedure_date (procedure_date)
);