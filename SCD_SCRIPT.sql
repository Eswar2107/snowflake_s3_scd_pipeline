/*==============================================================================
 Project Name : Snowflake S3 Batch Ingestion with SCD Type 1 & Type 2
 Description  : End-to-end batch ingestion pipeline using Snowflake, AWS S3,
                Tasks, and Streams implementing Slowly Changing Dimensions.
 Author       : <Your Name>
==============================================================================*/

/*==============================================================================
 1. FILE FORMAT
==============================================================================*/
CREATE OR REPLACE FILE FORMAT CSV_TYPE
TYPE = 'CSV'
SKIP_HEADER = 1
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n'
FIELD_OPTIONALLY_ENCLOSED_BY = '"';


/*==============================================================================
 2. STORAGE INTEGRATION (AWS S3)
==============================================================================*/
CREATE OR REPLACE STORAGE INTEGRATION S3_INT
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::476483571909:role/test'
STORAGE_ALLOWED_LOCATIONS = ('s3://source-2107/source/');

DESC INTEGRATION S3_INT;


/*==============================================================================
 3. EXTERNAL STAGE
==============================================================================*/
CREATE OR REPLACE STAGE S3_STAGE
URL = 's3://source-2107/source/'
STORAGE_INTEGRATION = S3_INT
FILE_FORMAT = CSV_TYPE;

LIST @S3_STAGE;


/*==============================================================================
 4. TABLES
==============================================================================*/

-- Staging Table
CREATE OR REPLACE TABLE STG_EMPLOYEE (
    EMPID        VARCHAR(10),
    NAME         VARCHAR(100),
    EMAIL        VARCHAR(150),
    PHONENO      VARCHAR(20),
    ADDRESS      VARCHAR(200),
    COMPANY      VARCHAR(100),
    EXPERIENCE   NUMBER(3)
);

-- SCD Type 1 Table
CREATE OR REPLACE TABLE EMPLOYEE (
    EMPID        VARCHAR(10),
    NAME         VARCHAR(100),
    EMAIL        VARCHAR(150),
    PHONENO      VARCHAR(20),
    ADDRESS      VARCHAR(200),
    COMPANY      VARCHAR(100),
    EXPERIENCE   NUMBER(3)
);

-- SCD Type 2 Dimension Table
CREATE OR REPLACE TABLE DIM_EMPLOYEE (
    EMPID        VARCHAR(10),
    NAME         VARCHAR(100),
    EMAIL        VARCHAR(150),
    PHONENO      VARCHAR(20),
    ADDRESS      VARCHAR(200),
    COMPANY      VARCHAR(100),
    EXPERIENCE   NUMBER(3),
    START_DATE   DATE,
    END_DATE     DATE DEFAULT DATE '2999-12-31',
    IS_ACTIVE    BOOLEAN
);


/*==============================================================================
 5. TASKS – STAGE CLEAN & LOAD
==============================================================================*/

-- Task 1: Clean staging table
CREATE OR REPLACE TASK CLEAN_STAGE_TABLE
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
AS
TRUNCATE TABLE STG_EMPLOYEE;

-- Task 2: Load data from S3 into staging
CREATE OR REPLACE TASK LOAD_STAGE_DATA
WAREHOUSE = COMPUTE_WH
AFTER CLEAN_STAGE_TABLE
AS
COPY INTO STG_EMPLOYEE
FROM @S3_STAGE;


/*==============================================================================
 6. SCD TYPE 1 IMPLEMENTATION
==============================================================================*/

CREATE OR REPLACE TASK EMP_SCD1_LOAD
WAREHOUSE = COMPUTE_WH
AFTER LOAD_STAGE_DATA
AS
MERGE INTO EMPLOYEE emp
USING STG_EMPLOYEE src
ON emp.EMPID = src.EMPID
AND emp.NAME = src.NAME

WHEN MATCHED
AND (
    emp.EMAIL <> src.EMAIL OR
    emp.PHONENO <> src.PHONENO OR
    emp.ADDRESS <> src.ADDRESS
)
THEN
UPDATE SET
    EMAIL = src.EMAIL,
    PHONENO = src.PHONENO,
    ADDRESS = src.ADDRESS

WHEN NOT MATCHED
THEN
INSERT (
    EMPID, NAME, EMAIL, PHONENO, ADDRESS, COMPANY, EXPERIENCE
)
VALUES (
    src.EMPID, src.NAME, src.EMAIL, src.PHONENO, src.ADDRESS,
    src.COMPANY, src.EXPERIENCE
);


/*==============================================================================
 7. STREAM FOR CHANGE DATA CAPTURE
==============================================================================*/

CREATE OR REPLACE STREAM STR_EMP
ON TABLE EMPLOYEE;


/*==============================================================================
 8. SCD TYPE 2 IMPLEMENTATION
==============================================================================*/

CREATE OR REPLACE TASK DIM_EMP_LOAD
WAREHOUSE = COMPUTE_WH
AFTER EMP_SCD1_LOAD
WHEN SYSTEM$STREAM_HAS_DATA('STR_EMP')
AS
MERGE INTO DIM_EMPLOYEE dim
USING STR_EMP str
ON dim.EMPID = str.EMPID
AND dim.ADDRESS = str.ADDRESS
AND dim.IS_ACTIVE = TRUE

-- Close existing record
WHEN MATCHED
AND str.METADATA$ACTION = 'DELETE'
THEN
UPDATE SET
    END_DATE = CURRENT_DATE,
    IS_ACTIVE = FALSE

-- Insert new record
WHEN NOT MATCHED
AND str.METADATA$ACTION = 'INSERT'
THEN
INSERT (
    EMPID, NAME, EMAIL, PHONENO, ADDRESS,
    COMPANY, EXPERIENCE, START_DATE, IS_ACTIVE
)
VALUES (
    str.EMPID, str.NAME, str.EMAIL, str.PHONENO, str.ADDRESS,
    str.COMPANY, str.EXPERIENCE, CURRENT_DATE, TRUE
);


/*==============================================================================
 9. TASK MANAGEMENT
==============================================================================*/

-- Resume tasks
ALTER TASK CLEAN_STAGE_TABLE RESUME;
ALTER TASK LOAD_STAGE_DATA RESUME;
ALTER TASK EMP_SCD1_LOAD RESUME;
ALTER TASK DIM_EMP_LOAD RESUME;

-- View task status
SHOW TASKS;

-- Task execution history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY());

/*==============================================================================
 END OF SCRIPT
==============================================================================*/
