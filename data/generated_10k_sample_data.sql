-- ================================================================
-- Healthcare Analytics Lab: Sample Data Generator
-- Purpose: Expand sample data to 10,000 records per table
-- Database: MySQL
-- ================================================================
-- ================================================================
-- Healthcare Analytics Lab: OLTP Database Setup
-- ================================================================
CREATE DATABASE IF NOT EXISTS healthcare_analytics_lab;
USE healthcare_analytics_lab;
-- Drop tables in reverse dependency order (!not done in production!)
DROP TABLE IF EXISTS encounter_procedures;
DROP TABLE IF EXISTS encounter_diagnoses;
DROP TABLE IF EXISTS billing;
DROP TABLE IF EXISTS procedures;
DROP TABLE IF EXISTS diagnoses;
DROP TABLE IF EXISTS encounters;
DROP TABLE IF EXISTS providers;
DROP TABLE IF EXISTS departments;
DROP TABLE IF EXISTS specialties;
DROP TABLE IF EXISTS patients;
-- Create base tables
CREATE TABLE patients (
    patient_id INT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    date_of_birth DATE,
    gender CHAR(1),
    mrn VARCHAR(20) UNIQUE
);
CREATE TABLE specialties (
    specialty_id INT PRIMARY KEY,
    specialty_name VARCHAR(100),
    specialty_code VARCHAR(10)
);
CREATE TABLE departments (
    department_id INT PRIMARY KEY,
    department_name VARCHAR(100),
    floor INT,
    capacity INT
);
CREATE TABLE diagnoses (
    diagnosis_id INT PRIMARY KEY,
    icd10_code VARCHAR(10),
    icd10_description VARCHAR(200)
);
CREATE TABLE procedures (
    procedure_id INT PRIMARY KEY,
    cpt_code VARCHAR(10),
    cpt_description VARCHAR(200)
);
CREATE TABLE providers (
    provider_id INT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    credential VARCHAR(20),
    specialty_id INT,
    department_id INT,
    FOREIGN KEY (specialty_id) REFERENCES specialties(specialty_id),
    FOREIGN KEY (department_id) REFERENCES departments(department_id)
);
CREATE TABLE encounters (
    encounter_id INT PRIMARY KEY,
    patient_id INT,
    provider_id INT,
    encounter_type VARCHAR(50),
    encounter_date DATETIME,
    discharge_date DATETIME,
    department_id INT,
    FOREIGN KEY (patient_id) REFERENCES patients(patient_id),
    FOREIGN KEY (provider_id) REFERENCES providers(provider_id),
    FOREIGN KEY (department_id) REFERENCES departments(department_id),
    INDEX idx_encounter_date (encounter_date)
);
CREATE TABLE encounter_diagnoses (
    encounter_diagnosis_id INT PRIMARY KEY,
    encounter_id INT,
    diagnosis_id INT,
    diagnosis_sequence INT,
    FOREIGN KEY (encounter_id) REFERENCES encounters(encounter_id),
    FOREIGN KEY (diagnosis_id) REFERENCES diagnoses(diagnosis_id)
);
CREATE TABLE encounter_procedures (
    encounter_procedure_id INT PRIMARY KEY,
    encounter_id INT,
    procedure_id INT,
    procedure_date DATE,
    FOREIGN KEY (encounter_id) REFERENCES encounters(encounter_id),
    FOREIGN KEY (procedure_id) REFERENCES procedures(procedure_id)
);
CREATE TABLE billing (
    billing_id INT PRIMARY KEY,
    encounter_id INT,
    claim_amount DECIMAL(12, 2),
    allowed_amount DECIMAL(12, 2),
    claim_date DATE,
    claim_status VARCHAR(50),
    FOREIGN KEY (encounter_id) REFERENCES encounters(encounter_id),
    INDEX idx_claim_date (claim_date)
);
-- ================================================================
-- ADD TIMESTAMP-BASED CHANGE DATA CAPTURE (CDC) COLUMNS
-- Purpose: Track changes to source tables for incremental ETL
-- ================================================================
-- Add last_update column to patients table
ALTER TABLE patients
ADD COLUMN last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;
-- Add last_update column to providers table
ALTER TABLE providers
ADD COLUMN last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;
-- ================================================================
-- DATA POPULATION STARTS BELOW
-- ================================================================
-- increase max recursion depth
SET SESSION cte_max_recursion_depth = 10001;
-- Generate 10,000 Specialties
-- Clear old data to avoid ID conflicts
INSERT INTO specialties (specialty_id, specialty_name, specialty_code) WITH RECURSIVE numbers AS (
        SELECT 1 AS n
        UNION ALL
        SELECT n + 1
        FROM numbers
        WHERE n < 20
    )
SELECT n,
    CASE
        n
        WHEN 1 THEN 'Cardiology'
        WHEN 2 THEN 'Internal Medicine'
        WHEN 3 THEN 'Emergency'
        WHEN 4 THEN 'Orthopedics'
        WHEN 5 THEN 'Pediatrics'
        WHEN 6 THEN 'Neurology'
        WHEN 7 THEN 'Radiology'
        WHEN 8 THEN 'Oncology'
        WHEN 9 THEN 'Dermatology'
        WHEN 10 THEN 'Psychiatry'
        WHEN 11 THEN 'Anesthesiology'
        WHEN 12 THEN 'Pathology'
        WHEN 13 THEN 'Ophthalmology'
        WHEN 14 THEN 'Urology'
        WHEN 15 THEN 'Gastroenterology'
        WHEN 16 THEN 'Nephrology'
        WHEN 17 THEN 'Pulmonology'
        WHEN 18 THEN 'Endocrinology'
        WHEN 19 THEN 'Rheumatology'
        ELSE 'General Surgery'
    END,
    CASE
        n
        WHEN 1 THEN 'CARD'
        WHEN 2 THEN 'IM'
        WHEN 3 THEN 'ER'
        WHEN 4 THEN 'ORTH'
        WHEN 5 THEN 'PED'
        WHEN 6 THEN 'NEUR'
        WHEN 7 THEN 'RAD'
        WHEN 8 THEN 'ONC'
        WHEN 9 THEN 'DERM'
        WHEN 10 THEN 'PSY'
        WHEN 11 THEN 'ANES'
        WHEN 12 THEN 'PATH'
        WHEN 13 THEN 'OPHT'
        WHEN 14 THEN 'URO'
        WHEN 15 THEN 'GI'
        WHEN 16 THEN 'NEPH'
        WHEN 17 THEN 'PULM'
        WHEN 18 THEN 'ENDO'
        WHEN 19 THEN 'RHEUM'
        ELSE 'SURG'
    END
FROM numbers;
-- Generate 15 Departments
INSERT INTO departments (department_id, department_name, floor, capacity) WITH RECURSIVE numbers AS (
        SELECT 1 AS n
        UNION ALL
        SELECT n + 1
        FROM numbers
        WHERE n < 15
    )
SELECT n,
    CASE
        n
        WHEN 1 THEN 'Emergency Department'
        WHEN 2 THEN 'Intensive Care Unit (ICU)'
        WHEN 3 THEN 'Labor & Delivery'
        WHEN 4 THEN 'Pediatric Ward'
        WHEN 5 THEN 'Surgical Suite'
        WHEN 6 THEN 'Radiology & Imaging'
        WHEN 7 THEN 'Outpatient Clinic'
        WHEN 8 THEN 'Cardiology Wing'
        WHEN 9 THEN 'Neurology Unit'
        WHEN 10 THEN 'Oncology Center'
        WHEN 11 THEN 'Inpatient Pharmacy'
        WHEN 12 THEN 'Dialysis Unit'
        WHEN 13 THEN 'Physical Therapy Lab'
        WHEN 14 THEN 'Laboratory Services'
        ELSE 'Administration'
    END,
    (n % 5) + 1,
    -- Spreads departments across 5 floors
    CASE
        WHEN n = 1 THEN 50 -- Higher capacity for ER
        WHEN n = 2 THEN 15 -- Lower capacity for ICU
        ELSE 30
    END
FROM numbers;
-- Generate 1000 Providers
INSERT INTO providers (
        provider_id,
        first_name,
        last_name,
        credential,
        specialty_id,
        department_id
    ) WITH RECURSIVE numbers AS (
        SELECT 1 AS n
        UNION ALL
        SELECT n + 1
        FROM numbers
        WHERE n < 1000
    )
SELECT n,
    CASE
        WHEN n % 25 = 1 THEN 'Kwabena'
        WHEN n % 25 = 2 THEN 'Esi'
        WHEN n % 25 = 3 THEN 'Nii'
        WHEN n % 25 = 4 THEN 'Akosua'
        WHEN n % 25 = 5 THEN 'Fifi'
        WHEN n % 25 = 6 THEN 'Mawuena'
        WHEN n % 25 = 7 THEN 'Kojo'
        WHEN n % 25 = 8 THEN 'Eunice'
        WHEN n % 25 = 9 THEN 'Lumi'
        WHEN n % 25 = 10 THEN 'Nana'
        WHEN n % 25 = 11 THEN 'Adwoa'
        WHEN n % 25 = 12 THEN 'Kwataye'
        WHEN n % 25 = 13 THEN 'Sena'
        WHEN n % 25 = 14 THEN 'Baaba'
        WHEN n % 25 = 15 THEN 'Kekeli'
        WHEN n % 25 = 16 THEN 'Paapa'
        WHEN n % 25 = 17 THEN 'Yaa'
        WHEN n % 25 = 18 THEN 'Kalu'
        WHEN n % 25 = 19 THEN 'Dela'
        WHEN n % 25 = 20 THEN 'Mansa'
        WHEN n % 25 = 21 THEN 'Tetteh'
        WHEN n % 25 = 22 THEN 'Araba'
        WHEN n % 25 = 23 THEN 'Kwesi'
        WHEN n % 25 = 24 THEN 'Abiba'
        ELSE 'Jibril'
    END,
    CASE
        WHEN n % 20 = 1 THEN 'Okorie'
        WHEN n % 20 = 2 THEN 'Addy'
        WHEN n % 20 = 3 THEN 'Agyemang'
        WHEN n % 20 = 4 THEN 'Dogbe'
        WHEN n % 20 = 5 THEN 'Quartey'
        WHEN n % 20 = 6 THEN 'Tawiah'
        WHEN n % 20 = 7 THEN 'Asante'
        WHEN n % 20 = 8 THEN 'Nortey'
        WHEN n % 20 = 9 THEN 'Abubakari'
        WHEN n % 20 = 10 THEN 'Bimpong'
        WHEN n % 20 = 11 THEN 'Lartey'
        WHEN n % 20 = 12 THEN 'Lamptey'
        WHEN n % 20 = 13 THEN 'Kusi'
        WHEN n % 20 = 14 THEN 'Donkor'
        WHEN n % 20 = 15 THEN 'Tsikata'
        WHEN n % 20 = 16 THEN 'Ankrah'
        WHEN n % 20 = 17 THEN 'Frimpong'
        WHEN n % 20 = 18 THEN 'Amankwah'
        WHEN n % 20 = 19 THEN 'Bonsu'
        ELSE 'Dako'
    END,
    IF(n % 4 = 0, 'FWACS', 'MBChB'),
    (n % 20) + 1,
    (n % 15) + 1
FROM numbers;
-- Generate 10,000 Patients
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
        WHERE n < 10000
    )
SELECT n,
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
    END,
    -- Simplified for brevity, you can add more CASE branches
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
    END,
    DATE_ADD('1950-01-01', INTERVAL FLOOR(RAND() * 25000) DAY),
    IF(n % 2 = 0, 'F', 'M'),
    CONCAT('GHA-', LPAD(n, 7, '0'))
FROM numbers;
-- Generate 10,000 Diagnoses
INSERT INTO diagnoses (diagnosis_id, icd10_code, icd10_description) WITH RECURSIVE numbers AS (
        SELECT 1 AS n
        UNION ALL
        SELECT n + 1
        FROM numbers
        WHERE n < 10000
    )
SELECT n AS diagnosis_id,
    CASE
        WHEN n % 50 = 1 THEN 'I10'
        WHEN n % 50 = 2 THEN 'E11.9'
        WHEN n % 50 = 3 THEN 'I50.9'
        WHEN n % 50 = 4 THEN 'J44.9'
        WHEN n % 50 = 5 THEN 'N18.9'
        WHEN n % 50 = 6 THEN 'F41.9'
        WHEN n % 50 = 7 THEN 'M79.3'
        WHEN n % 50 = 8 THEN 'K21.9'
        WHEN n % 50 = 9 THEN 'E78.5'
        WHEN n % 50 = 10 THEN 'I25.10'
        WHEN n % 50 = 11 THEN 'M17.9'
        WHEN n % 50 = 12 THEN 'J18.9'
        WHEN n % 50 = 13 THEN 'N39.0'
        WHEN n % 50 = 14 THEN 'I48.91'
        WHEN n % 50 = 15 THEN 'G89.29'
        WHEN n % 50 = 16 THEN 'E66.9'
        WHEN n % 50 = 17 THEN 'F32.9'
        WHEN n % 50 = 18 THEN 'I73.9'
        WHEN n % 50 = 19 THEN 'K59.00'
        WHEN n % 50 = 20 THEN 'R51'
        WHEN n % 50 = 21 THEN 'M25.50'
        WHEN n % 50 = 22 THEN 'E03.9'
        WHEN n % 50 = 23 THEN 'G47.00'
        WHEN n % 50 = 24 THEN 'K80.20'
        WHEN n % 50 = 25 THEN 'I63.9'
        WHEN n % 50 = 26 THEN 'C50.9'
        WHEN n % 50 = 27 THEN 'J45.909'
        WHEN n % 50 = 28 THEN 'N40.0'
        WHEN n % 50 = 29 THEN 'M81.0'
        WHEN n % 50 = 30 THEN 'H25.9'
        WHEN n % 50 = 31 THEN 'I21.9'
        WHEN n % 50 = 32 THEN 'E05.90'
        WHEN n % 50 = 33 THEN 'K50.90'
        WHEN n % 50 = 34 THEN 'M06.9'
        WHEN n % 50 = 35 THEN 'G20'
        WHEN n % 50 = 36 THEN 'B18.2'
        WHEN n % 50 = 37 THEN 'I71.4'
        WHEN n % 50 = 38 THEN 'L40.9'
        WHEN n % 50 = 39 THEN 'D50.9'
        WHEN n % 50 = 40 THEN 'E10.9'
        WHEN n % 50 = 41 THEN 'I20.9'
        WHEN n % 50 = 42 THEN 'J12.9'
        WHEN n % 50 = 43 THEN 'M54.5'
        WHEN n % 50 = 44 THEN 'R07.9'
        WHEN n % 50 = 45 THEN 'G43.909'
        WHEN n % 50 = 46 THEN 'K29.70'
        WHEN n % 50 = 47 THEN 'F10.20'
        WHEN n % 50 = 48 THEN 'I49.9'
        WHEN n % 50 = 49 THEN 'N20.0'
        ELSE CONCAT('Z', LPAD((n % 100), 2, '0'), '.', (n % 10))
    END AS icd10_code,
    CASE
        WHEN n % 50 = 1 THEN 'Hypertension'
        WHEN n % 50 = 2 THEN 'Type 2 Diabetes'
        WHEN n % 50 = 3 THEN 'Heart Failure'
        WHEN n % 50 = 4 THEN 'COPD'
        WHEN n % 50 = 5 THEN 'Chronic Kidney Disease'
        WHEN n % 50 = 6 THEN 'Anxiety Disorder'
        WHEN n % 50 = 7 THEN 'Fibromyalgia'
        WHEN n % 50 = 8 THEN 'GERD'
        WHEN n % 50 = 9 THEN 'Hyperlipidemia'
        WHEN n % 50 = 10 THEN 'Coronary Artery Disease'
        WHEN n % 50 = 11 THEN 'Osteoarthritis of Knee'
        WHEN n % 50 = 12 THEN 'Pneumonia'
        WHEN n % 50 = 13 THEN 'Urinary Tract Infection'
        WHEN n % 50 = 14 THEN 'Atrial Fibrillation'
        WHEN n % 50 = 15 THEN 'Chronic Pain'
        WHEN n % 50 = 16 THEN 'Obesity'
        WHEN n % 50 = 17 THEN 'Major Depression'
        WHEN n % 50 = 18 THEN 'Peripheral Vascular Disease'
        WHEN n % 50 = 19 THEN 'Constipation'
        WHEN n % 50 = 20 THEN 'Headache'
        WHEN n % 50 = 21 THEN 'Joint Pain'
        WHEN n % 50 = 22 THEN 'Hypothyroidism'
        WHEN n % 50 = 23 THEN 'Insomnia'
        WHEN n % 50 = 24 THEN 'Cholelithiasis'
        WHEN n % 50 = 25 THEN 'Cerebral Infarction'
        WHEN n % 50 = 26 THEN 'Breast Cancer'
        WHEN n % 50 = 27 THEN 'Asthma'
        WHEN n % 50 = 28 THEN 'Benign Prostatic Hyperplasia'
        WHEN n % 50 = 29 THEN 'Osteoporosis'
        WHEN n % 50 = 30 THEN 'Cataract'
        WHEN n % 50 = 31 THEN 'Myocardial Infarction'
        WHEN n % 50 = 32 THEN 'Hyperthyroidism'
        WHEN n % 50 = 33 THEN 'Crohn Disease'
        WHEN n % 50 = 34 THEN 'Rheumatoid Arthritis'
        WHEN n % 50 = 35 THEN 'Parkinson Disease'
        WHEN n % 50 = 36 THEN 'Hepatitis C'
        WHEN n % 50 = 37 THEN 'Abdominal Aortic Aneurysm'
        WHEN n % 50 = 38 THEN 'Psoriasis'
        WHEN n % 50 = 39 THEN 'Iron Deficiency Anemia'
        WHEN n % 50 = 40 THEN 'Type 1 Diabetes'
        WHEN n % 50 = 41 THEN 'Angina Pectoris'
        WHEN n % 50 = 42 THEN 'Viral Pneumonia'
        WHEN n % 50 = 43 THEN 'Low Back Pain'
        WHEN n % 50 = 44 THEN 'Chest Pain'
        WHEN n % 50 = 45 THEN 'Migraine'
        WHEN n % 50 = 46 THEN 'Gastritis'
        WHEN n % 50 = 47 THEN 'Alcohol Dependence'
        WHEN n % 50 = 48 THEN 'Cardiac Arrhythmia'
        WHEN n % 50 = 49 THEN 'Kidney Stone'
        ELSE CONCAT('Health Status Code ', n)
    END AS icd10_description
FROM numbers;
-- Generate 10,000 Procedures
INSERT INTO procedures (procedure_id, cpt_code, cpt_description) WITH RECURSIVE numbers AS (
        SELECT 1 AS n
        UNION ALL
        SELECT n + 1
        FROM numbers
        WHERE n < 10000
    )
SELECT n AS procedure_id,
    CASE
        WHEN n % 40 = 1 THEN '99213'
        WHEN n % 40 = 2 THEN '93000'
        WHEN n % 40 = 3 THEN '71020'
        WHEN n % 40 = 4 THEN '99214'
        WHEN n % 40 = 5 THEN '99215'
        WHEN n % 40 = 6 THEN '99232'
        WHEN n % 40 = 7 THEN '99233'
        WHEN n % 40 = 8 THEN '36415'
        WHEN n % 40 = 9 THEN '80053'
        WHEN n % 40 = 10 THEN '85025'
        WHEN n % 40 = 11 THEN '93306'
        WHEN n % 40 = 12 THEN '76700'
        WHEN n % 40 = 13 THEN '73610'
        WHEN n % 40 = 14 THEN '70450'
        WHEN n % 40 = 15 THEN '45378'
        WHEN n % 40 = 16 THEN '43239'
        WHEN n % 40 = 17 THEN '97110'
        WHEN n % 40 = 18 THEN '97140'
        WHEN n % 40 = 19 THEN '90834'
        WHEN n % 40 = 20 THEN '90837'
        WHEN n % 40 = 21 THEN '92014'
        WHEN n % 40 = 22 THEN '66984'
        WHEN n % 40 = 23 THEN '27447'
        WHEN n % 40 = 24 THEN '29881'
        WHEN n % 40 = 25 THEN '47562'
        WHEN n % 40 = 26 THEN '43644'
        WHEN n % 40 = 27 THEN '52000'
        WHEN n % 40 = 28 THEN '55700'
        WHEN n % 40 = 29 THEN '19307'
        WHEN n % 40 = 30 THEN '77427'
        WHEN n % 40 = 31 THEN '96413'
        WHEN n % 40 = 32 THEN '00140'
        WHEN n % 40 = 33 THEN '01402'
        WHEN n % 40 = 34 THEN '64483'
        WHEN n % 40 = 35 THEN '62311'
        WHEN n % 40 = 36 THEN '99285'
        WHEN n % 40 = 37 THEN '99291'
        WHEN n % 40 = 38 THEN '31500'
        WHEN n % 40 = 39 THEN '94640'
        ELSE CONCAT('999', LPAD((n % 100), 2, '0'))
    END AS cpt_code,
    CASE
        WHEN n % 40 = 1 THEN 'Office Visit - Established'
        WHEN n % 40 = 2 THEN 'EKG'
        WHEN n % 40 = 3 THEN 'Chest X-ray'
        WHEN n % 40 = 4 THEN 'Office Visit - Level 4'
        WHEN n % 40 = 5 THEN 'Office Visit - Level 5'
        WHEN n % 40 = 6 THEN 'Hospital Visit - Subsequent'
        WHEN n % 40 = 7 THEN 'Hospital Visit - Subsequent High'
        WHEN n % 40 = 8 THEN 'Venipuncture'
        WHEN n % 40 = 9 THEN 'Comprehensive Metabolic Panel'
        WHEN n % 40 = 10 THEN 'Complete Blood Count'
        WHEN n % 40 = 11 THEN 'Echocardiogram'
        WHEN n % 40 = 12 THEN 'Abdominal Ultrasound'
        WHEN n % 40 = 13 THEN 'Ankle X-ray'
        WHEN n % 40 = 14 THEN 'CT Head without Contrast'
        WHEN n % 40 = 15 THEN 'Colonoscopy'
        WHEN n % 40 = 16 THEN 'EGD'
        WHEN n % 40 = 17 THEN 'Physical Therapy - Therapeutic Exercise'
        WHEN n % 40 = 18 THEN 'Manual Therapy'
        WHEN n % 40 = 19 THEN 'Psychotherapy - 45 min'
        WHEN n % 40 = 20 THEN 'Psychotherapy - 60 min'
        WHEN n % 40 = 21 THEN 'Eye Exam - Established'
        WHEN n % 40 = 22 THEN 'Cataract Surgery'
        WHEN n % 40 = 23 THEN 'Total Knee Replacement'
        WHEN n % 40 = 24 THEN 'Knee Arthroscopy'
        WHEN n % 40 = 25 THEN 'Laparoscopic Cholecystectomy'
        WHEN n % 40 = 26 THEN 'Gastric Bypass'
        WHEN n % 40 = 27 THEN 'Cystoscopy'
        WHEN n % 40 = 28 THEN 'Prostate Biopsy'
        WHEN n % 40 = 29 THEN 'Mastectomy'
        WHEN n % 40 = 30 THEN 'Radiation Therapy'
        WHEN n % 40 = 31 THEN 'Chemotherapy IV'
        WHEN n % 40 = 32 THEN 'Anesthesia - Head'
        WHEN n % 40 = 33 THEN 'Anesthesia - Knee'
        WHEN n % 40 = 34 THEN 'Transforaminal Epidural'
        WHEN n % 40 = 35 THEN 'Epidural Injection'
        WHEN n % 40 = 36 THEN 'ER Visit - High Severity'
        WHEN n % 40 = 37 THEN 'Critical Care - First Hour'
        WHEN n % 40 = 38 THEN 'Intubation'
        WHEN n % 40 = 39 THEN 'Nebulizer Treatment'
        ELSE CONCAT('Medical Procedure ', n)
    END AS cpt_description
FROM numbers;
-- Generate 10,000 Encounters (Deterministic dates to guarantee discharge > encounter)
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
        WHERE n < 10000
    ),
    base_encounters AS (
        SELECT n,
            CASE
                WHEN n % 3 = 1 THEN 'Outpatient'
                WHEN n % 3 = 2 THEN 'Inpatient'
                ELSE 'ER'
            END AS encounter_type,
            -- Deterministic encounter_date based on n
            DATE_ADD(
                DATE_ADD('2024-01-01', INTERVAL (n % 365) DAY),
                INTERVAL (n % 24) HOUR
            ) AS encounter_date
        FROM numbers
    )
SELECT n AS encounter_id,
    ((n - 1) % 10000) + 1 AS patient_id,
    ((n - 1) % 1000) + 1 AS provider_id,
    encounter_type,
    encounter_date,
    -- Discharge is ALWAYS after encounter_date (adds 1+ hours/days)
    CASE
        WHEN encounter_type = 'Outpatient' THEN DATE_ADD(encounter_date, INTERVAL ((n % 4) + 1) HOUR)
        WHEN encounter_type = 'Inpatient' THEN DATE_ADD(encounter_date, INTERVAL ((n % 10) + 1) DAY)
        ELSE DATE_ADD(encounter_date, INTERVAL ((n % 12) + 1) HOUR)
    END AS discharge_date,
    ((n - 1) % 15) + 1 AS department_id
FROM base_encounters;
-- Generate 10,000 Billing Records
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
        WHERE n < 10000
    )
SELECT n AS billing_id,
    n AS encounter_id,
    ROUND(500 + (RAND() * 50000), 2) AS claim_amount,
    ROUND((500 + (RAND() * 50000)) * 0.8, 2) AS allowed_amount,
    DATE_ADD('2024-01-01', INTERVAL FLOOR(RAND() * 365) DAY) AS claim_date,
    CASE
        WHEN n % 10 = 1 THEN 'Pending'
        WHEN n % 10 = 2 THEN 'Denied'
        WHEN n % 10 = 3 THEN 'Under Review'
        ELSE 'Paid'
    END AS claim_status
FROM numbers;
-- Generate 10,000 Encounter Diagnoses
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
        WHERE n < 10000
    )
SELECT n AS encounter_diagnosis_id,
    ((n - 1) % 10000) + 1 AS encounter_id,
    ((n - 1) % 10000) + 1 AS diagnosis_id,
    (n % 5) + 1 AS diagnosis_sequence
FROM numbers;
-- Generate 10,000 Encounter Procedures
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
        WHERE n < 10000
    )
SELECT n AS encounter_procedure_id,
    ((n - 1) % 10000) + 1 AS encounter_id,
    ((n - 1) % 10000) + 1 AS procedure_id,
    DATE_ADD('2024-01-01', INTERVAL FLOOR(RAND() * 365) DAY) AS procedure_date
FROM numbers;
-- ================================================================
-- Data Generation Complete
-- Each table now has 10,000 records
-- ================================================================


-- Inject deterministic inpatient readmissions (200) and matching rows
-- Purpose: create reproducible Inpatient -> Inpatient readmission pairs
-- New encounter ids begin at 10001 to avoid PK collisions with the
-- original 10,000 generated rows.
-- ================================================================
-- Capture the exact source encounter ids in a temporary table so
-- downstream inserts reference the same originals (prevents FK mismatches).
DROP TEMPORARY TABLE IF EXISTS temp_readmission_sources;
CREATE TEMPORARY TABLE temp_readmission_sources AS
SELECT encounter_id,
    patient_id,
    provider_id,
    discharge_date,
    department_id
FROM encounters
WHERE encounter_type = 'Inpatient'
ORDER BY encounter_id
LIMIT 200;
-- Insert new readmission encounters (IDs offset by 10000 using the source id)
INSERT INTO encounters (
        encounter_id,
        patient_id,
        provider_id,
        encounter_type,
        encounter_date,
        discharge_date,
        department_id
    )
SELECT 10000 + trs.encounter_id AS encounter_id,
    trs.patient_id,
    ((trs.provider_id % 1000) + 1) AS provider_id,
    'Inpatient' AS encounter_type,
    DATE_ADD(
        trs.discharge_date,
        INTERVAL ((trs.encounter_id % 30) + 1) DAY
    ) AS encounter_date,
    DATE_ADD(
        DATE_ADD(
            trs.discharge_date,
            INTERVAL ((trs.encounter_id % 30) + 1) DAY
        ),
        INTERVAL ((trs.encounter_id % 10) + 1) DAY
    ) AS discharge_date,
    trs.department_id
FROM temp_readmission_sources trs;
-- Add matching billing rows only for those source encounters (preserves FK)
INSERT INTO billing (
        billing_id,
        encounter_id,
        claim_amount,
        allowed_amount,
        claim_date,
        claim_status
    )
SELECT 10000 + b.billing_id AS billing_id,
    10000 + b.encounter_id AS encounter_id,
    ROUND(500 + (RAND() * 50000), 2) AS claim_amount,
    ROUND((500 + (RAND() * 50000)) * 0.8, 2) AS allowed_amount,
    DATE_ADD(
        '2024-01-01',
        INTERVAL (b.encounter_id % 365) DAY
    ) AS claim_date,
    CASE
        WHEN b.billing_id % 10 = 1 THEN 'Pending'
        WHEN b.billing_id % 10 = 2 THEN 'Denied'
        WHEN b.billing_id % 10 = 3 THEN 'Under Review'
        ELSE 'Paid'
    END AS claim_status
FROM billing b
    JOIN temp_readmission_sources trs ON b.encounter_id = trs.encounter_id;
-- Add matching encounter_diagnoses for the new encounters (ids offset by 10000)
INSERT INTO encounter_diagnoses (
        encounter_diagnosis_id,
        encounter_id,
        diagnosis_id,
        diagnosis_sequence
    )
SELECT 10000 + ed.encounter_diagnosis_id,
    10000 + ed.encounter_id,
    ed.diagnosis_id,
    ed.diagnosis_sequence
FROM encounter_diagnoses ed
    JOIN temp_readmission_sources trs ON ed.encounter_id = trs.encounter_id;
-- Add matching encounter_procedures for the new encounters (ids offset by 10000)
INSERT INTO encounter_procedures (
        encounter_procedure_id,
        encounter_id,
        procedure_id,
        procedure_date
    )
SELECT 10000 + ep.encounter_procedure_id,
    10000 + ep.encounter_id,
    ep.procedure_id,
    DATE_ADD(
        ep.procedure_date,
        INTERVAL ((ep.encounter_procedure_id % 7) + 1) DAY
    )
FROM encounter_procedures ep
    JOIN temp_readmission_sources trs ON ep.encounter_id = trs.encounter_id;
-- Clean up
DROP TEMPORARY TABLE IF EXISTS temp_readmission_sources;