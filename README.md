# snowflake_s3_scd_pipeline

This SQL file creates and orchestrates all required Snowflake objects to load employee data from **AWS S3** into Snowflake and maintain both **current** and **historical** records.

---

## 🏗️ Architecture Overview

![Architecture](architecture/snowflake_s3_scd_architecture.png)

**High-level flow:**

1. CSV files are uploaded to AWS S3  
2. Snowflake reads data using a secure storage integration  
3. Data is loaded into a staging table  
4. SCD Type 1 logic updates the base employee table  
5. Streams capture data changes  
6. SCD Type 2 logic maintains full history in a dimension table  

---

## 🔄 End-to-End Data Flow

1. **AWS S3** stores incoming CSV files  
2. **File Format** defines CSV parsing rules  
3. **Storage Integration** securely connects Snowflake to S3  
4. **External Stage** references S3 data  
5. **Staging Table (`STG_EMPLOYEE`)** holds raw data  
6. **Task Chain** automates execution  
7. **SCD Type 1 (`EMPLOYEE`)** overwrites changed attributes  
8. **Stream (`STR_EMP`)** captures row-level changes  
9. **SCD Type 2 (`DIM_EMPLOYEE`)** tracks historical versions  

---

## 🧱 Objects Created in the SQL File

### File Format
- CSV with header skipped
- Comma-delimited
- Quoted fields supported

### Storage Integration
- Secure IAM role-based access to S3
- No hardcoded credentials

### External Stage
- Reads data from S3
- Uses defined file format

### Tables

| Table | Purpose |
|------|--------|
| STG_EMPLOYEE | Staging table |
| EMPLOYEE | SCD Type 1 (current data) |
| DIM_EMPLOYEE | SCD Type 2 (historical data) |

---

## ⏱️ Tasks (Automation)

| Task Name | Description |
|----------|-------------|
| CLEAN_STAGE_TABLE | Truncates staging table |
| LOAD_STAGE_DATA | Loads data from S3 |
| EMP_SCD1_LOAD | Applies SCD Type 1 logic |
| DIM_EMP_LOAD | Applies SCD Type 2 logic |

Tasks are chained using the `AFTER` clause for controlled execution.

---

## 🔁 SCD Type 1 Logic

- Updates changed attributes (email, phone, address)
- No history maintained
- Stores only the latest record

Used for **operational reporting**.

---

## 🕒 SCD Type 2 Logic

- Uses Snowflake Streams for CDC
- Closes old records using `END_DATE`
- Inserts new records with `START_DATE`
- Tracks full history

Used for **auditing and historical analysis**.

---

## 🚀 How to Run

1. Update S3 bucket path and IAM role ARN in the SQL file  
2. Execute the SQL script in Snowflake  
3. Resume all tasks  
4. Upload CSV files to S3  
5. Monitor task execution  

---

## 📊 Monitoring

- `SHOW TASKS`
- `INFORMATION_SCHEMA.TASK_HISTORY`
- Query staging and dimension tables

---

## 🛠️ Technologies Used

- Snowflake  
- AWS S3  
- Snowflake Tasks  
- Snowflake Streams  
- SQL  

---

## 👤 Author

Eswar Karthikeyan
Email: eshukarthikeyan2107@gmail.com  


⭐ Star this repository if you find it useful!
