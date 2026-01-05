-- ================================================================
-- Healthcare Analytics Lab: Sample Data Generator
-- Purpose: Expand sample data to 10,000 records per table
-- Database: MySQL
-- ================================================================

-- increase max recursion depth
SET SESSION cte_max_recursion_depth = 10001;

-- Generate 10,000 Specialties
INSERT INTO specialties (specialty_id, specialty_name, specialty_code) WITH RECURSIVE numbers AS (
        SELECT 1 AS n
        UNION ALL
        SELECT n + 1
        FROM numbers
        WHERE n < 10000
    )
SELECT n AS specialty_id,
    CASE
        WHEN n % 20 = 1 THEN 'Cardiology'
        WHEN n % 20 = 2 THEN 'Internal Medicine'
        WHEN n % 20 = 3 THEN 'Emergency'
        WHEN n % 20 = 4 THEN 'Orthopedics'
        WHEN n % 20 = 5 THEN 'Pediatrics'
        WHEN n % 20 = 6 THEN 'Neurology'
        WHEN n % 20 = 7 THEN 'Radiology'
        WHEN n % 20 = 8 THEN 'Oncology'
        WHEN n % 20 = 9 THEN 'Dermatology'
        WHEN n % 20 = 10 THEN 'Psychiatry'
        WHEN n % 20 = 11 THEN 'Anesthesiology'
        WHEN n % 20 = 12 THEN 'Pathology'
        WHEN n % 20 = 13 THEN 'Ophthalmology'
        WHEN n % 20 = 14 THEN 'Urology'
        WHEN n % 20 = 15 THEN 'Gastroenterology'
        WHEN n % 20 = 16 THEN 'Nephrology'
        WHEN n % 20 = 17 THEN 'Pulmonology'
        WHEN n % 20 = 18 THEN 'Endocrinology'
        WHEN n % 20 = 19 THEN 'Rheumatology'
        ELSE 'General Surgery'
    END AS specialty_name,
    CASE
        WHEN n % 20 = 1 THEN 'CARD'
        WHEN n % 20 = 2 THEN 'IM'
        WHEN n % 20 = 3 THEN 'ER'
        WHEN n % 20 = 4 THEN 'ORTH'
        WHEN n % 20 = 5 THEN 'PED'
        WHEN n % 20 = 6 THEN 'NEUR'
        WHEN n % 20 = 7 THEN 'RAD'
        WHEN n % 20 = 8 THEN 'ONC'
        WHEN n % 20 = 9 THEN 'DERM'
        WHEN n % 20 = 10 THEN 'PSY'
        WHEN n % 20 = 11 THEN 'ANES'
        WHEN n % 20 = 12 THEN 'PATH'
        WHEN n % 20 = 13 THEN 'OPHT'
        WHEN n % 20 = 14 THEN 'URO'
        WHEN n % 20 = 15 THEN 'GI'
        WHEN n % 20 = 16 THEN 'NEPH'
        WHEN n % 20 = 17 THEN 'PULM'
        WHEN n % 20 = 18 THEN 'ENDO'
        WHEN n % 20 = 19 THEN 'RHEUM'
        ELSE 'SURG'
    END AS specialty_code
FROM numbers;
-- Generate 10,000 Departments
INSERT INTO departments (department_id, department_name, floor, capacity) WITH RECURSIVE numbers AS (
        SELECT 1 AS n
        UNION ALL
        SELECT n + 1
        FROM numbers
        WHERE n < 10000
    )
SELECT n AS department_id,
    CONCAT(
        CASE
            WHEN n % 15 = 1 THEN 'Cardiology Unit'
            WHEN n % 15 = 2 THEN 'Internal Medicine'
            WHEN n % 15 = 3 THEN 'Emergency'
            WHEN n % 15 = 4 THEN 'Orthopedics Ward'
            WHEN n % 15 = 5 THEN 'Pediatrics Unit'
            WHEN n % 15 = 6 THEN 'Neurology Wing'
            WHEN n % 15 = 7 THEN 'Radiology Center'
            WHEN n % 15 = 8 THEN 'Oncology Unit'
            WHEN n % 15 = 9 THEN 'ICU'
            WHEN n % 15 = 10 THEN 'Surgery Center'
            WHEN n % 15 = 11 THEN 'Outpatient Clinic'
            WHEN n % 15 = 12 THEN 'Dialysis Center'
            WHEN n % 15 = 13 THEN 'Psychiatric Ward'
            WHEN n % 15 = 14 THEN 'Maternity Ward'
            ELSE 'Recovery Unit'
        END,
        ' ',
        FLOOR((n -1) / 15) + 1
    ) AS department_name,
    (n % 10) + 1 AS floor,
    (n % 40) + 15 AS capacity
FROM numbers;
-- Generate 10,000 Providers
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
        WHERE n < 10000
    )
SELECT n AS provider_id,
    CASE
        WHEN n % 30 = 1 THEN 'James'
        WHEN n % 30 = 2 THEN 'Sarah'
        WHEN n % 30 = 3 THEN 'Michael'
        WHEN n % 30 = 4 THEN 'Jennifer'
        WHEN n % 30 = 5 THEN 'David'
        WHEN n % 30 = 6 THEN 'Emily'
        WHEN n % 30 = 7 THEN 'Robert'
        WHEN n % 30 = 8 THEN 'Jessica'
        WHEN n % 30 = 9 THEN 'William'
        WHEN n % 30 = 10 THEN 'Ashley'
        WHEN n % 30 = 11 THEN 'Richard'
        WHEN n % 30 = 12 THEN 'Amanda'
        WHEN n % 30 = 13 THEN 'Thomas'
        WHEN n % 30 = 14 THEN 'Lisa'
        WHEN n % 30 = 15 THEN 'Daniel'
        WHEN n % 30 = 16 THEN 'Mary'
        WHEN n % 30 = 17 THEN 'Christopher'
        WHEN n % 30 = 18 THEN 'Karen'
        WHEN n % 30 = 19 THEN 'Matthew'
        WHEN n % 30 = 20 THEN 'Nancy'
        WHEN n % 30 = 21 THEN 'Anthony'
        WHEN n % 30 = 22 THEN 'Betty'
        WHEN n % 30 = 23 THEN 'Mark'
        WHEN n % 30 = 24 THEN 'Sandra'
        WHEN n % 30 = 25 THEN 'Donald'
        WHEN n % 30 = 26 THEN 'Patricia'
        WHEN n % 30 = 27 THEN 'Steven'
        WHEN n % 30 = 28 THEN 'Linda'
        WHEN n % 30 = 29 THEN 'Paul'
        ELSE 'Margaret'
    END AS first_name,
    CASE
        WHEN n % 25 = 1 THEN 'Chen'
        WHEN n % 25 = 2 THEN 'Williams'
        WHEN n % 25 = 3 THEN 'Rodriguez'
        WHEN n % 25 = 4 THEN 'Smith'
        WHEN n % 25 = 5 THEN 'Johnson'
        WHEN n % 25 = 6 THEN 'Brown'
        WHEN n % 25 = 7 THEN 'Jones'
        WHEN n % 25 = 8 THEN 'Garcia'
        WHEN n % 25 = 9 THEN 'Miller'
        WHEN n % 25 = 10 THEN 'Davis'
        WHEN n % 25 = 11 THEN 'Martinez'
        WHEN n % 25 = 12 THEN 'Hernandez'
        WHEN n % 25 = 13 THEN 'Lopez'
        WHEN n % 25 = 14 THEN 'Gonzalez'
        WHEN n % 25 = 15 THEN 'Wilson'
        WHEN n % 25 = 16 THEN 'Anderson'
        WHEN n % 25 = 17 THEN 'Thomas'
        WHEN n % 25 = 18 THEN 'Taylor'
        WHEN n % 25 = 19 THEN 'Moore'
        WHEN n % 25 = 20 THEN 'Jackson'
        WHEN n % 25 = 21 THEN 'Martin'
        WHEN n % 25 = 22 THEN 'Lee'
        WHEN n % 25 = 23 THEN 'Perez'
        WHEN n % 25 = 24 THEN 'Thompson'
        ELSE 'White'
    END AS last_name,
    CASE
        WHEN n % 5 = 1 THEN 'MD'
        WHEN n % 5 = 2 THEN 'DO'
        WHEN n % 5 = 3 THEN 'PA'
        WHEN n % 5 = 4 THEN 'NP'
        ELSE 'MD, PhD'
    END AS credential,
    ((n - 1) % 10000) + 1 AS specialty_id,
    ((n - 1) % 10000) + 1 AS department_id
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
SELECT n AS patient_id,
    CASE
        WHEN n % 35 = 1 THEN 'John'
        WHEN n % 35 = 2 THEN 'Jane'
        WHEN n % 35 = 3 THEN 'Robert'
        WHEN n % 35 = 4 THEN 'Maria'
        WHEN n % 35 = 5 THEN 'William'
        WHEN n % 35 = 6 THEN 'Patricia'
        WHEN n % 35 = 7 THEN 'James'
        WHEN n % 35 = 8 THEN 'Jennifer'
        WHEN n % 35 = 9 THEN 'Michael'
        WHEN n % 35 = 10 THEN 'Linda'
        WHEN n % 35 = 11 THEN 'David'
        WHEN n % 35 = 12 THEN 'Elizabeth'
        WHEN n % 35 = 13 THEN 'Richard'
        WHEN n % 35 = 14 THEN 'Susan'
        WHEN n % 35 = 15 THEN 'Joseph'
        WHEN n % 35 = 16 THEN 'Jessica'
        WHEN n % 35 = 17 THEN 'Thomas'
        WHEN n % 35 = 18 THEN 'Sarah'
        WHEN n % 35 = 19 THEN 'Charles'
        WHEN n % 35 = 20 THEN 'Karen'
        WHEN n % 35 = 21 THEN 'Christopher'
        WHEN n % 35 = 22 THEN 'Nancy'
        WHEN n % 35 = 23 THEN 'Daniel'
        WHEN n % 35 = 24 THEN 'Betty'
        WHEN n % 35 = 25 THEN 'Matthew'
        WHEN n % 35 = 26 THEN 'Margaret'
        WHEN n % 35 = 27 THEN 'Anthony'
        WHEN n % 35 = 28 THEN 'Sandra'
        WHEN n % 35 = 29 THEN 'Mark'
        WHEN n % 35 = 30 THEN 'Ashley'
        WHEN n % 35 = 31 THEN 'Donald'
        WHEN n % 35 = 32 THEN 'Kimberly'
        WHEN n % 35 = 33 THEN 'Steven'
        WHEN n % 35 = 34 THEN 'Emily'
        ELSE 'Paul'
    END AS first_name,
    CASE
        WHEN n % 30 = 1 THEN 'Doe'
        WHEN n % 30 = 2 THEN 'Smith'
        WHEN n % 30 = 3 THEN 'Johnson'
        WHEN n % 30 = 4 THEN 'Williams'
        WHEN n % 30 = 5 THEN 'Brown'
        WHEN n % 30 = 6 THEN 'Jones'
        WHEN n % 30 = 7 THEN 'Garcia'
        WHEN n % 30 = 8 THEN 'Miller'
        WHEN n % 30 = 9 THEN 'Davis'
        WHEN n % 30 = 10 THEN 'Rodriguez'
        WHEN n % 30 = 11 THEN 'Martinez'
        WHEN n % 30 = 12 THEN 'Hernandez'
        WHEN n % 30 = 13 THEN 'Lopez'
        WHEN n % 30 = 14 THEN 'Gonzalez'
        WHEN n % 30 = 15 THEN 'Wilson'
        WHEN n % 30 = 16 THEN 'Anderson'
        WHEN n % 30 = 17 THEN 'Thomas'
        WHEN n % 30 = 18 THEN 'Taylor'
        WHEN n % 30 = 19 THEN 'Moore'
        WHEN n % 30 = 20 THEN 'Jackson'
        WHEN n % 30 = 21 THEN 'Martin'
        WHEN n % 30 = 22 THEN 'Lee'
        WHEN n % 30 = 23 THEN 'Perez'
        WHEN n % 30 = 24 THEN 'Thompson'
        WHEN n % 30 = 25 THEN 'White'
        WHEN n % 30 = 26 THEN 'Harris'
        WHEN n % 30 = 27 THEN 'Sanchez'
        WHEN n % 30 = 28 THEN 'Clark'
        WHEN n % 30 = 29 THEN 'Ramirez'
        ELSE 'Lewis'
    END AS last_name,
    DATE_ADD(
        '1930-01-01',
        INTERVAL FLOOR(RAND() * 365 * 80) DAY
    ) AS date_of_birth,
    IF(n % 2 = 0, 'F', 'M') AS gender,
    CONCAT('MRN', LPAD(n, 6, '0')) AS mrn
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
-- Generate 10,000 Encounters
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
    )
SELECT n AS encounter_id,
    ((n - 1) % 10000) + 1 AS patient_id,
    ((n - 1) % 10000) + 1 AS provider_id,
    CASE
        WHEN n % 3 = 1 THEN 'Outpatient'
        WHEN n % 3 = 2 THEN 'Inpatient'
        ELSE 'ER'
    END AS encounter_type,
    DATE_ADD('2024-01-01', INTERVAL FLOOR(RAND() * 365) DAY) + INTERVAL FLOOR(RAND() * 24) HOUR AS encounter_date,
    CASE
        WHEN n % 3 = 1 THEN DATE_ADD(
            DATE_ADD('2024-01-01', INTERVAL FLOOR(RAND() * 365) DAY),
            INTERVAL FLOOR(RAND() * 4) HOUR
        )
        WHEN n % 3 = 2 THEN DATE_ADD(
            DATE_ADD('2024-01-01', INTERVAL FLOOR(RAND() * 365) DAY),
            INTERVAL FLOOR(1 + RAND() * 10) DAY
        )
        ELSE DATE_ADD(
            DATE_ADD('2024-01-01', INTERVAL FLOOR(RAND() * 365) DAY),
            INTERVAL FLOOR(RAND() * 24) HOUR
        )
    END AS discharge_date,
    ((n - 1) % 10000) + 1 AS department_id
FROM numbers;
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