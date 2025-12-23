-- IN THIS WE WE WILL IMPLEMENTING BOTH SCD2 AND CDM LOGIC FOR THE SILVER TABLES

-- 1. Create table departments by Merging Data from Hospital A & B  
CREATE TABLE IF NOT EXISTS `gcp-healthcare-project-481608.silver_dataset.departments` (
    Dept_Id STRING,
    SRC_Dept_Id STRING,
    Name STRING,
    datasource STRING,
    is_quarantined BOOLEAN
);

-- 2. Truncate Silver Table Before Inserting 
TRUNCATE TABLE `gcp-healthcare-project-481608.silver_dataset.departments`;

-- 3. full load by Inserting merged Data 
INSERT INTO `gcp-healthcare-project-481608.silver_dataset.departments`
SELECT DISTINCT 
    CONCAT(deptid, '-', datasource) AS Dept_Id,
    deptid AS SRC_Dept_Id,
    Name,
    datasource,
    CASE 
        WHEN deptid IS NULL OR Name IS NULL THEN TRUE 
        ELSE FALSE 
    END AS is_quarantined
FROM (
    SELECT DISTINCT *, 'hosa' AS datasource FROM `gcp-healthcare-project-481608.bronze_dataset.departments_ha`
    UNION ALL
    SELECT DISTINCT *, 'hosb' AS datasource FROM `gcp-healthcare-project-481608.bronze_dataset.departments_hb`
);

-------------------------------------------------------------------------------------------------------

-- 1. Create table providers by Merge Data from Hospital A & B  
CREATE TABLE IF NOT EXISTS `gcp-healthcare-project-481608.silver_dataset.providers` (
    ProviderID STRING,
    FirstName STRING,
    LastName STRING,
    Specialization STRING,
    DeptID STRING,
    NPI INT64,
    datasource STRING,
    is_quarantined BOOLEAN
);

-- 2. Truncate Silver Table Before Inserting 
TRUNCATE TABLE `gcp-healthcare-project-481608.silver_dataset.providers`;

-- 3. full load by Inserting merged Data 
INSERT INTO `gcp-healthcare-project-481608.silver_dataset.providers`
SELECT DISTINCT 
    ProviderID,
    FirstName,
    LastName,
    Specialization,
    DeptID,
    CAST(NPI AS INT64) AS NPI,
    datasource,
    CASE 
        WHEN ProviderID IS NULL OR DeptID IS NULL THEN TRUE 
        ELSE FALSE 
    END AS is_quarantined
FROM (
    SELECT DISTINCT *, 'hosa' AS datasource FROM `gcp-healthcare-project-481608.bronze_dataset.providers_ha`
    UNION ALL
    SELECT DISTINCT *, 'hosb' AS datasource FROM `gcp-healthcare-project-481608.bronze_dataset.providers_hb`
);

-------------------------------------------------------------------------------------------------------

-- 1. Create patients Table in BigQuery
CREATE TABLE IF NOT EXISTS `gcp-healthcare-project-481608.silver_dataset.patients` (
    Patient_Key STRING,
    SRC_PatientID STRING,
    FirstName STRING,
    LastName STRING,
    MiddleName STRING,
    SSN STRING,
    PhoneNumber STRING,
    Gender STRING,
    DOB INT64,
    Address STRING,
    SRC_ModifiedDate INT64,
    datasource STRING,
    is_quarantined BOOL,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_current BOOL
);

--Create a quality_checks temp table
CREATE OR REPLACE TABLE `gcp-healthcare-project-481608.silver_dataset.quality_checks` AS
SELECT DISTINCT 
    CONCAT(SRC_PatientID, '-', datasource) AS Patient_Key,
    SRC_PatientID,
    FirstName,
    LastName,
    MiddleName,
    SSN,
    PhoneNumber,
    Gender,
    DOB,
    Address,
    ModifiedDate AS SRC_ModifiedDate,
    datasource,
    CASE 
        WHEN SRC_PatientID IS NULL OR DOB IS NULL OR FirstName IS NULL OR LOWER(FirstName) = 'null' THEN TRUE
        ELSE FALSE
    END AS is_quarantined
FROM (
    SELECT DISTINCT 
        PatientID AS SRC_PatientID,
        FirstName,
        LastName,
        MiddleName,
        SSN,
        PhoneNumber,
        Gender,
        DOB,
        Address,
        ModifiedDate,
        'hosa' AS datasource
    FROM `gcp-healthcare-project-481608.bronze_dataset.patients_ha`
    
    UNION ALL

    SELECT DISTINCT 
        ID AS SRC_PatientID,
        F_Name as FirstName,
        L_Name as LastName,
        M_Name as MiddleName,
        SSN,
        PhoneNumber,
        Gender,
        DOB,
        Address,
        ModifiedDate,
        'hosb' AS datasource
    FROM `gcp-healthcare-project-481608.bronze_dataset.patients_hb`
);

-- 3. Apply SCD Type 2 Logic with MERGE
MERGE INTO `gcp-healthcare-project-481608.silver_dataset.patients` AS target
USING `gcp-healthcare-project-481608.silver_dataset.quality_checks` AS source
ON target.Patient_Key = source.Patient_Key
AND target.is_current = TRUE 

-- Step 1: Mark existing records as historical if any column has changed
WHEN MATCHED AND (
    target.SRC_PatientID <> source.SRC_PatientID OR
    target.FirstName <> source.FirstName OR
    target.LastName <> source.LastName OR
    target.MiddleName <> source.MiddleName OR
    target.SSN <> source.SSN OR
    target.PhoneNumber <> source.PhoneNumber OR
    target.Gender <> source.Gender OR
    target.DOB <> source.DOB OR
    target.Address <> source.Address OR
    target.SRC_ModifiedDate <> source.SRC_ModifiedDate OR
    target.datasource <> source.datasource OR
    target.is_quarantined <> source.is_quarantined
)
THEN UPDATE SET 
    target.is_current = FALSE,
    target.modified_date = CURRENT_TIMESTAMP()

-- Step 2: Insert new and updated records as the latest active records
WHEN NOT MATCHED 
THEN INSERT (
    Patient_Key,
    SRC_PatientID,
    FirstName,
    LastName,
    MiddleName,
    SSN,
    PhoneNumber,
    Gender,
    DOB,
    Address,
    SRC_ModifiedDate,
    datasource,
    is_quarantined,
    inserted_date,
    modified_date,
    is_current
)
VALUES (
    source.Patient_Key,
    source.SRC_PatientID,
    source.FirstName,
    source.LastName,
    source.MiddleName,
    source.SSN,
    source.PhoneNumber,
    source.Gender,
    source.DOB,
    source.Address,
    source.SRC_ModifiedDate,
    source.datasource,
    source.is_quarantined,
    CURRENT_TIMESTAMP(),  
    CURRENT_TIMESTAMP(),  
    TRUE 
);

-- DROP quality_check table
DROP TABLE IF EXISTS `gcp-healthcare-project-481608.silver_dataset.quality_checks`;

-------------------------------------------------------------------------------------------------------

-- 1. Create transactions Table in BigQuery
CREATE TABLE IF NOT EXISTS `gcp-healthcare-project-481608.silver_dataset.transactions` (
    Transaction_Key STRING,
    SRC_TransactionID STRING,
    EncounterID STRING,
    PatientID STRING,
    ProviderID STRING,
    DeptID STRING,
    VisitDate INT64,
    ServiceDate INT64,
    PaidDate INT64,
    VisitType STRING,
    Amount FLOAT64,
    AmountType STRING,
    PaidAmount FLOAT64,
    ClaimID STRING,
    PayorID STRING,
    ProcedureCode INT64,
    ICDCode STRING,
    LineOfBusiness STRING,
    MedicaidID STRING,
    MedicareID STRING,
    SRC_InsertDate INT64,
    SRC_ModifiedDate INT64,
    datasource STRING,
    is_quarantined BOOL,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_current BOOL
);

-- 2. Create a quality_checks temp table
CREATE OR REPLACE TABLE `gcp-healthcare-project-481608.silver_dataset.quality_checks` AS
SELECT DISTINCT 
    CONCAT(TransactionID, '-', datasource) AS Transaction_Key,
    TransactionID AS SRC_TransactionID,
    EncounterID,
    PatientID,
    ProviderID,
    DeptID,
    VisitDate,
    ServiceDate,
    PaidDate,
    VisitType,
    Amount,
    AmountType,
    PaidAmount,
    ClaimID,
    PayorID,
    ProcedureCode,
    ICDCode,
    LineOfBusiness,
    MedicaidID,
    MedicareID,
    InsertDate AS SRC_InsertDate,
    ModifiedDate AS SRC_ModifiedDate,
    datasource,
    CASE 
        WHEN EncounterID IS NULL OR PatientID IS NULL OR TransactionID IS NULL OR VisitDate IS NULL THEN TRUE
        ELSE FALSE
    END AS is_quarantined
FROM (
    SELECT DISTINCT *, 'hosa' AS datasource FROM `gcp-healthcare-project-481608.bronze_dataset.transactions_ha`
    UNION ALL
    SELECT DISTINCT *, 'hosb' AS datasource FROM `gcp-healthcare-project-481608.bronze_dataset.transactions_hb`
);

-- 3. Apply SCD Type 2 Logic with MERGE
MERGE INTO `gcp-healthcare-project-481608.silver_dataset.transactions` AS target
USING `gcp-healthcare-project-481608.silver_dataset.quality_checks` AS source
ON target.Transaction_Key = source.Transaction_Key
AND target.is_current = TRUE 

-- Step 1: Mark existing records as historical if any column has changed
WHEN MATCHED AND (
    target.SRC_TransactionID <> source.SRC_TransactionID OR
    target.EncounterID <> source.EncounterID OR
    target.PatientID <> source.PatientID OR
    target.ProviderID <> source.ProviderID OR
    target.DeptID <> source.DeptID OR
    target.VisitDate <> source.VisitDate OR
    target.ServiceDate <> source.ServiceDate OR
    target.PaidDate <> source.PaidDate OR
    target.VisitType <> source.VisitType OR
    target.Amount <> source.Amount OR
    target.AmountType <> source.AmountType OR
    target.PaidAmount <> source.PaidAmount OR
    target.ClaimID <> source.ClaimID OR
    target.PayorID <> source.PayorID OR
    target.ProcedureCode <> source.ProcedureCode OR
    target.ICDCode <> source.ICDCode OR
    target.LineOfBusiness <> source.LineOfBusiness OR
    target.MedicaidID <> source.MedicaidID OR
    target.MedicareID <> source.MedicareID OR
    target.SRC_InsertDate <> source.SRC_InsertDate OR
    target.SRC_ModifiedDate <> source.SRC_ModifiedDate OR
    target.datasource <> source.datasource OR
    target.is_quarantined <> source.is_quarantined
)
THEN UPDATE SET 
    target.is_current = FALSE,
    target.modified_date = CURRENT_TIMESTAMP()

-- Step 2: Insert new and updated records as the latest active records
WHEN NOT MATCHED 
THEN INSERT (
    Transaction_Key,
    SRC_TransactionID,
    EncounterID,
    PatientID,
    ProviderID,
    DeptID,
    VisitDate,
    ServiceDate,
    PaidDate,
    VisitType,
    Amount,
    AmountType,
    PaidAmount,
    ClaimID,
    PayorID,
    ProcedureCode,
    ICDCode,
    LineOfBusiness,
    MedicaidID,
    MedicareID,
    SRC_InsertDate,
    SRC_ModifiedDate,
    datasource,
    is_quarantined,
    inserted_date,
    modified_date,
    is_current
)
VALUES (
    source.Transaction_Key,
    source.SRC_TransactionID,
    source.EncounterID,
    source.PatientID,
    source.ProviderID,
    source.DeptID,
    source.VisitDate,
    source.ServiceDate,
    source.PaidDate,
    source.VisitType,
    source.Amount,
    source.AmountType,
    source.PaidAmount,
    source.ClaimID,
    source.PayorID,
    source.ProcedureCode,
    source.ICDCode,
    source.LineOfBusiness,
    source.MedicaidID,
    source.MedicareID,
    source.SRC_InsertDate,
    source.SRC_ModifiedDate,
    source.datasource,
    source.is_quarantined,
    CURRENT_TIMESTAMP(),  
    CURRENT_TIMESTAMP(),  
    TRUE 
);

-- 4. DROP quality_check table
DROP TABLE IF EXISTS `gcp-healthcare-project-481608.silver_dataset.quality_checks`;

-------------------------------------------------------------------------------------------------------

-- 1. Create the encounters Table in BigQuery
CREATE TABLE IF NOT EXISTS `gcp-healthcare-project-481608.silver_dataset.encounters` (
    Encounter_Key STRING,
    SRC_EncounterID STRING,
    PatientID STRING,
    ProviderID STRING,
    DepartmentID STRING,
    EncounterDate INT64,
    EncounterType STRING,
    ProcedureCode INT64,
    SRC_ModifiedDate INT64,
    datasource STRING,
    is_quarantined BOOL,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_current BOOL
);

-- 2. Create a quality_checks temp table for encounters
CREATE OR REPLACE TABLE `gcp-healthcare-project-481608.silver_dataset.quality_checks_encounters` AS
SELECT DISTINCT 
    CONCAT(SRC_EncounterID, '-', datasource) AS Encounter_Key,
    SRC_EncounterID,
    PatientID,
    ProviderID,
    DepartmentID,
    EncounterDate,
    EncounterType,
    ProcedureCode,
    ModifiedDate AS SRC_ModifiedDate,
    datasource,
    CASE 
        WHEN SRC_EncounterID IS NULL OR PatientID IS NULL OR EncounterDate IS NULL OR LOWER(EncounterType) = 'null' THEN TRUE
        ELSE FALSE
    END AS is_quarantined
FROM (
    SELECT DISTINCT 
        EncounterID AS SRC_EncounterID,
        PatientID,
        ProviderID,
        DepartmentID,
        EncounterDate,
        EncounterType,
        ProcedureCode,
        ModifiedDate,
        'hosa' AS datasource
    FROM `gcp-healthcare-project-481608.bronze_dataset.encounters_ha`
    
    UNION ALL

    SELECT DISTINCT 
        EncounterID AS SRC_EncounterID,
        PatientID,
        ProviderID,
        DepartmentID,
        EncounterDate,
        EncounterType,
        ProcedureCode,
        ModifiedDate,
        'hosb' AS datasource
    FROM `gcp-healthcare-project-481608.bronze_dataset.encounters_hb`
);

-- 3. Apply SCD Type 2 Logic with MERGE
MERGE INTO `gcp-healthcare-project-481608.silver_dataset.encounters` AS target
USING `gcp-healthcare-project-481608.silver_dataset.quality_checks_encounters` AS source
ON target.Encounter_Key = source.Encounter_Key
AND target.is_current = TRUE 

-- Step 1: Mark existing records as historical if any column has changed
WHEN MATCHED AND (
    target.SRC_EncounterID <> source.SRC_EncounterID OR
    target.PatientID <> source.PatientID OR
    target.ProviderID <> source.ProviderID OR
    target.DepartmentID <> source.DepartmentID OR
    target.EncounterDate <> source.EncounterDate OR
    target.EncounterType <> source.EncounterType OR
    target.ProcedureCode <> source.ProcedureCode OR
    target.SRC_ModifiedDate <> source.SRC_ModifiedDate OR
    target.datasource <> source.datasource OR
    target.is_quarantined <> source.is_quarantined
)
THEN UPDATE SET 
    target.is_current = FALSE,
    target.modified_date = CURRENT_TIMESTAMP()

-- Step 2: Insert new and updated records as the latest active records
WHEN NOT MATCHED 
THEN INSERT (
    Encounter_Key,
    SRC_EncounterID,
    PatientID,
    ProviderID,
    DepartmentID,
    EncounterDate,
    EncounterType,
    ProcedureCode,
    SRC_ModifiedDate,
    datasource,
    is_quarantined,
    inserted_date,
    modified_date,
    is_current
)
VALUES (
    source.Encounter_Key,
    source.SRC_EncounterID,
    source.PatientID,
    source.ProviderID,
    source.DepartmentID,
    source.EncounterDate,
    source.EncounterType,
    source.ProcedureCode,
    source.SRC_ModifiedDate,
    source.datasource,
    source.is_quarantined,
    CURRENT_TIMESTAMP(),  
    CURRENT_TIMESTAMP(),  
    TRUE 
);

-- 4. DROP quality_check table
DROP TABLE IF EXISTS `gcp-healthcare-project-481608.silver_dataset.quality_checks_encounters`;

-------------------------------------------------------------------------------------------------------

-- 1. Create the Claims Table in BigQuery
CREATE TABLE IF NOT EXISTS `gcp-healthcare-project-481608.silver_dataset.claims` (
    Claim_Key STRING,
    SRC_ClaimID STRING,
    TransactionID STRING,
    PatientID STRING,
    EncounterID STRING,
    ProviderID STRING,
    DeptID STRING,
    ServiceDate STRING,
    ClaimDate STRING,
    PayorID STRING,
    ClaimAmount STRING,
    PaidAmount STRING,
    ClaimStatus STRING,
    PayorType STRING,
    Deductible STRING,
    Coinsurance STRING,
    Copay STRING,
    SRC_InsertDate STRING,
    SRC_ModifiedDate STRING,
    datasource STRING,
    is_quarantined BOOLEAN,
    row_hash STRING,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_current BOOLEAN
);

-- SCD type II implementation of Claims table.

MERGE INTO `gcp-healthcare-project-481608.silver_dataset.claims` AS target
USING (
  WITH raw_source AS (
    SELECT 
      CONCAT(ClaimID, '-', datasource) AS Claim_Key,
      ClaimID AS SRC_ClaimID,
      TransactionID, PatientID, EncounterID, ProviderID, DeptID,
      ServiceDate, ClaimDate, PayorID, ClaimAmount, PaidAmount,
      ClaimStatus, PayorType, Deductible, Coinsurance, Copay,
      InsertDate AS SRC_InsertDate,
      ModifiedDate AS SRC_ModifiedDate,
      datasource,
      CASE 
          WHEN ClaimID IS NULL OR PatientID IS NULL OR TransactionID IS NULL OR LOWER(ClaimStatus) = 'null' THEN TRUE
          ELSE FALSE
      END AS is_quarantined
    FROM `gcp-healthcare-project-481608.bronze_dataset.claims`
  )
  
  -- PART 1: The New Version (INSERT)
  -- Only includes brand new records OR records where a change was detected
  SELECT 
    NULL AS join_key, 
    'INSERT' AS action_type,
    s.*
  FROM raw_source s
  LEFT JOIN `gcp-healthcare-project-481608.silver_dataset.claims` t
    ON s.Claim_Key = t.Claim_Key AND t.is_current = TRUE
  WHERE t.Claim_Key IS NULL -- Brand new record
     OR ( -- Existing record but data has changed
        t.TransactionID <> s.TransactionID OR
        t.PatientID <> s.PatientID OR
        t.EncounterID <> s.EncounterID OR
        t.ProviderID <> s.ProviderID OR
        t.DeptID <> s.DeptID OR
        t.ServiceDate <> s.ServiceDate OR
        t.ClaimDate <> s.ClaimDate OR
        t.PayorID <> s.PayorID OR
        t.ClaimAmount <> s.ClaimAmount OR
        t.PaidAmount <> s.PaidAmount OR
        t.ClaimStatus <> s.ClaimStatus OR
        t.PayorType <> s.PayorType OR
        t.Deductible <> s.Deductible OR
        t.Coinsurance <> s.Coinsurance OR
        t.Copay <> s.Copay OR
        t.SRC_ModifiedDate <> s.SRC_ModifiedDate OR
        t.is_quarantined <> s.is_quarantined
     )

  UNION ALL

  -- PART 2: The Old Version (UPDATE/EXPIRE)
  -- Only includes the key for records that already exist but have changes
  SELECT 
    t.Claim_Key AS join_key,
    'UPDATE' AS action_type,
    s.*
  FROM raw_source s
  JOIN `gcp-healthcare-project-481608.silver_dataset.claims` t
    ON s.Claim_Key = t.Claim_Key AND t.is_current = TRUE
  WHERE (
        t.TransactionID <> s.TransactionID OR
        t.PatientID <> s.PatientID OR
        t.EncounterID <> s.EncounterID OR
        t.ProviderID <> s.ProviderID OR
        t.DeptID <> s.DeptID OR
        t.ServiceDate <> s.ServiceDate OR
        t.ClaimDate <> s.ClaimDate OR
        t.PayorID <> s.PayorID OR
        t.ClaimAmount <> s.ClaimAmount OR
        t.PaidAmount <> s.PaidAmount OR
        t.ClaimStatus <> s.ClaimStatus OR
        t.PayorType <> s.PayorType OR
        t.Deductible <> s.Deductible OR
        t.Coinsurance <> s.Coinsurance OR
        t.Copay <> s.Copay OR
        t.SRC_ModifiedDate <> s.SRC_ModifiedDate OR
        t.is_quarantined <> s.is_quarantined
  )
) AS source
ON target.Claim_Key = source.join_key
AND target.is_current = TRUE

-- ACTION 1: Expire the record that triggered the JOIN
WHEN MATCHED AND source.action_type='UPDATE' THEN 
  UPDATE SET 
    target.is_current = FALSE,
    target.modified_date = CURRENT_TIMESTAMP()

-- ACTION 2: Insert the new version (because join_key is NULL)
WHEN NOT MATCHED AND source.action_type='INSERT' THEN
  INSERT (
    Claim_Key, SRC_ClaimID, TransactionID, PatientID, EncounterID, ProviderID, DeptID, 
    ServiceDate, ClaimDate, PayorID, ClaimAmount, PaidAmount, ClaimStatus, 
    PayorType, Deductible, Coinsurance, Copay, SRC_InsertDate, SRC_ModifiedDate, 
    datasource, is_quarantined, inserted_date, modified_date, is_current
  )
  VALUES (
    source.Claim_Key, source.SRC_ClaimID, source.TransactionID, source.PatientID, source.EncounterID, source.ProviderID, source.DeptID, 
    source.ServiceDate, source.ClaimDate, source.PayorID, source.ClaimAmount, source.PaidAmount, source.ClaimStatus, 
    source.PayorType, source.Deductible, source.Coinsurance, source.Copay, source.SRC_InsertDate, source.SRC_ModifiedDate, 
    source.datasource, source.is_quarantined, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TRUE
  );

-------------------------------------------------------------------------------------------------------

-- 1. Create the CP Codes Silver Table in BigQuery
CREATE TABLE IF NOT EXISTS `gcp-healthcare-project-481608.silver_dataset.cpt_codes` (
    CP_Code_Key STRING,
    procedure_code_category STRING,
    cpt_codes STRING,
    procedure_code_descriptions STRING,
    code_status STRING,
    datasource STRING,
    is_quarantined BOOLEAN,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_current BOOLEAN
);

-- 2. Apply SCD Type 2 Logic with MERGE
MERGE INTO `gcp-healthcare-project-481608.silver_dataset.cpt_codes` AS target
USING (
  WITH
    RAW_CPT_CODES AS (
      SELECT
        CONCAT(cpt_codes, '-', 'hosa') AS CP_Code_Key,
        procedure_code_category,
        cpt_codes,
        procedure_code_descriptions,
        code_status,
        'hosa' AS datasource,
        -- Define a quarantine condition (null values in key fields)
        CASE
          WHEN cpt_codes IS NULL OR LOWER(code_status) = 'null' THEN TRUE
          ELSE FALSE
          END AS is_quarantined
      FROM `gcp-healthcare-project-481608.bronze_dataset.cpt_codes`
    )
  SELECT NULL AS join_key, 'INSERT' AS action_type, A.*
  FROM RAW_CPT_CODES A
  LEFT JOIN `gcp-healthcare-project-481608.silver_dataset.cpt_codes` B
    ON A.CP_Code_Key = B.CP_Code_Key AND IS_CURRENT = TRUE
  WHERE
    B.CP_Code_Key IS NULL  -- NEW INSERTS
    OR (  -- SCD TYPE 2 INSERTS
      A.procedure_code_category <> B.procedure_code_category
      OR A.cpt_codes <> B.cpt_codes
      OR A.procedure_code_descriptions <> B.procedure_code_descriptions
      OR A.code_status <> B.code_status
      OR A.datasource <> B.datasource
      OR A.is_quarantined <> B.is_quarantined)
  UNION ALL
  SELECT A.CP_Code_Key AS join_key, 'UPDATE' AS action_type, A.*
  FROM RAW_CPT_CODES A
  JOIN `gcp-healthcare-project-481608.silver_dataset.cpt_codes` B
    ON A.CP_Code_Key = B.CP_Code_Key AND IS_CURRENT = TRUE
  WHERE
    (  -- SCD TYPE 2 UPDATES
      A.procedure_code_category <> B.procedure_code_category
      OR A.cpt_codes <> B.cpt_codes
      OR A.procedure_code_descriptions <> B.procedure_code_descriptions
      OR A.code_status <> B.code_status
      OR A.datasource <> B.datasource
      OR A.is_quarantined <> B.is_quarantined)
) AS source
ON
  target.CP_Code_Key = source.join_key
  AND target.is_current = TRUE

    -- Step 1: Mark existing records as historical if any column has changed
    WHEN MATCHED AND source.action_type = 'UPDATE'
        THEN UPDATE
    SET
    target.is_current = FALSE,
    target.modified_date = CURRENT_TIMESTAMP()

    -- Step 2: Insert new and updated records as the latest active records
    WHEN NOT MATCHED AND source.action_type = 'INSERT'
      THEN
        INSERT(
          CP_Code_Key,
          procedure_code_category,
          cpt_codes,
          procedure_code_descriptions,
          code_status,
          datasource,
          is_quarantined,
          inserted_date,
          modified_date,
          is_current)
          VALUES(
            source.CP_Code_Key,
            source.procedure_code_category,
            source.cpt_codes,
            source.procedure_code_descriptions,
            source.code_status,
            source.datasource,
            source.is_quarantined,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP(),
            TRUE);
