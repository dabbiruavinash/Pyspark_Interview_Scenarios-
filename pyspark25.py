🔹 1. Data Ingestion
Q: In your healthcare project, how did you ingest large EHR (Electronic Health Records) data from different sources like S3, Azure Blob, or APIs into Databricks?
 What they’re looking for: Use of Autoloader, batch ingestion, or API connectors (e.g., REST, FHIR API).

%python
from pyspark.sql.functions import *
from databricks import sql

# Multi-source ingestion strategy
def ingest_healthcare_data():
    # 1. S3 - EHR data using Auto Loader
    ehr_data = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "dbfs:/schema/ehr/")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("s3://healthcare-bucket/ehr-data/"))
    
    # 2. Azure Blob - Medical images metadata
    medical_images = (spark.read
        .format("azure-blob")
        .option("container", "medical-images")
        .option("storageAccount", "healthcare-storage")
        .load("wasbs://images@healthcare-storage.blob.core.windows.net/"))
    
    # 3. FHIR API - Patient records
    fhir_patients = (spark.read
        .format("databricks-api")
        .option("url", "https://fhir-api.com/Patient")
        .option("authType", "oauth2")
        .load())
    
    # 4. HL7 messages - Real-time streaming
    hl7_stream = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka-health:9092")
        .option("subscribe", "hl7-messages")
        .load()
        .select(from_json(col("value").cast("string"), hl7_schema).alias("data"))
        .select("data.*"))
    
    return ehr_data, medical_images, fhir_patients, hl7_stream

🔹 2. Dealing with PHI (Protected Health Information)
Q: How did you ensure sensitive data like PHI/PII was secured during processing in Databricks?
 Expected: Masking, encryption at rest/in transit, column-level access controls using Unity Catalog.

%python
# HIPAA-compliant data processing
def secure_phi_processing(df):
    # Encryption at rest
    spark.conf.set("spark.sql.encryption.enabled", "true")
    spark.conf.set("spark.sql.encryption.algorithm", "AES-GCM")
    
    # Column-level masking
    secured_df = df.withColumn(
        "patient_id_hashed", 
        sha2(col("patient_id"), 256)).withColumn("ssn_masked", 
        regexp_replace(col("ssn"), r"(\d{3})-(\d{2})-(\d{4})", "***-**-$3")).withColumn(
        "email_masked",
        regexp_replace(col("email"), r"(\w{3})[\w.-]+@([\w.]+)", "$1***@$2")).drop("patient_id", "ssn", "email")  # Remove original PHI
    
    # Unity Catalog access controls
    spark.sql("""
    GRANT SELECT ON TABLE healthcare_silver.patient_records 
    TO `data-scientists`""")
    
    spark.sql("""
    CREATE MASK healthcare_silver.mask_dob 
    ON healthcare_silver.patient_records.date_of_birth
    AS (CASE WHEN is_account_group_member('hipaa_auditors') 
             THEN date_of_birth
             ELSE DATE_ADD(date_of_birth, 365 * 5) END)""")
    
    return secured_df

🔹 3. Data Quality and Validation
Q: Describe how you validated medical records or patient claim data before transforming them.
 Expected: Schema enforcement, null checks, type validation, using Delta Live Tables’ expectations.

%python
# Using Delta Live Tables for data quality
import dlt

@dlt.table(
    name="validated_patient_records",
    comment="Patient records with data quality checks"
)
@dlt.expect("valid_patient_id", "patient_id IS NOT NULL")
@dlt.expect("valid_dob", "date_of_birth <= current_date()")
@dlt.expect("valid_ssn", "ssn RLIKE '^[0-9]{3}-[0-9]{2}-[0-9]{4}$'")
@dlt.expect_or_drop("valid_gender", "gender IN ('M', 'F', 'O')")
@dlt.expect_or_drop("valid_age", "age BETWEEN 0 AND 120")
def validate_patient_data():
    return (
        dlt.read_stream("raw_patient_records")
        .withColumn("age", years_between(current_date(), col("date_of_birth")))
    )

# Additional programmatic validation
def comprehensive_validation(df):
    validation_results = df.agg(
        count(when(col("patient_id").isNull(), True)).alias("null_patient_ids"),
        count(when(col("diagnosis_code").isNull(), True)).alias("null_diagnosis"),
        count(when(col("admission_date") > col("discharge_date"), True)).alias("invalid_date_ranges"),
        count(when(~col("icd10_code").rlike("^[A-Z][0-9]{2}(\.[0-9]{1,2})?$"), True)).alias("invalid_icd10")).collect()[0]
    
    if any([validation_results[f] > 0 for f in validation_results.asDict()]):
        # Quarantine invalid records
        invalid_records = df.filter(
            col("patient_id").isNull() |
            col("diagnosis_code").isNull() |
            (col("admission_date") > col("discharge_date")) |
            (~col("icd10_code").rlike("^[A-Z][0-9]{2}(\.[0-9]{1,2})?$")))
        invalid_records.write.mode("append").saveAsTable("quarantined_records")
        
    return df.filter(~col("patient_id").isNull())  # Return valid records

🔹 4. Delta Lake Use
Q: Explain how you implemented Delta Lake in your healthcare data pipeline.
 Expected: Bronze (raw EHR), Silver (validated/labeled), Gold (aggregated for BI/reporting).

%python
# Medallion architecture for healthcare data
# Bronze layer - raw data
raw_ehr = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .load("s3://healthcare-raw/ehr/")
    .writeStream
    .format("delta")
    .option("checkpointLocation", "dbfs:/checkpoints/bronze/ehr/")
    .table("bronze_ehr_records"))

# Silver layer - validated and enriched
@dlt.table
def silver_patient_records():
    return (
        dlt.read("bronze_ehr_records")
        .transform(secure_phi_processing)
        .transform(comprehensive_validation)
        .withColumn("ingestion_timestamp", current_timestamp()))

# Gold layer - aggregated for analytics
@dlt.table
def gold_patient_analytics():
    return (
        dlt.read("silver_patient_records")
        .groupBy("diagnosis_category", "hospital_id", "month")
        .agg(
            count("*").alias("patient_count"),
            avg("length_of_stay").alias("avg_stay_days"),
            sum("treatment_cost").alias("total_cost")))

🔹 5. Real-time Data Processing
Q: Have you processed real-time streaming data from medical devices or logs? How did you manage schema drift?
 Expected: Structured Streaming with Autoloader, schema evolution in Delta Lake.

%python
# Real-time medical device data processing
def process_medical_device_stream():
    device_stream = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "iot-devices:9092")
        .option("subscribe", "medical-devices")
        .load()
        .select(
            from_json(col("value").cast("string"), device_schema).alias("data"),
            col("timestamp"))select("data.*", "timestamp"))
    
    # Handle schema evolution
    processed_stream = (device_stream
        .withWatermark("timestamp", "10 minutes")
        .groupBy(
            window(col("timestamp"), "5 minutes"),
            "device_id", "patient_id")
        .agg(
            avg("heart_rate").alias("avg_heart_rate"),
            max("blood_pressure").alias("max_bp"),
            count("*").alias("readings_count")))
    
    # Write with schema evolution support
    query = (processed_stream
        .writeStream
        .format("delta")
        .option("checkpointLocation", "dbfs:/checkpoints/medical-devices/")
        .option("mergeSchema", "true")
        .outputMode("update")
        .table("gold_medical_device_metrics"))
    
    return query

🔹 6. Data Deduplication
Q: Healthcare datasets often have duplicate records. How did you handle deduplication in Databricks?
 Expected: Use of row_number() + window function, or dropDuplicates() with composite keys.

%python
# Healthcare data deduplication strategy
def deduplicate_patient_records():
    window_spec = Window.partitionBy(
        "patient_id", "date_of_birth", "ssn_last_four").orderBy(col("ingestion_timestamp").desc())
    
    deduplicated = (spark.table("silver_patient_records")
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num"))
    
    # Merge to handle updates
    delta_table = DeltaTable.forName("gold_patient_master")
    
    delta_table.alias("target").merge(
        deduplicated.alias("source"),
        "target.patient_id = source.patient_id AND target.ssn_last_four = source.ssn_last_four").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    
    return deduplicated

🔹 7. Handling Slowly Changing Dimensions (SCD)
Q: How did you implement SCD Type 2 for patient or hospital master data in Databricks?
 Expected: Use of merge/upsert with Delta Lake and effective_start_date, effective_end_date fields.

%python
# SCD Type 2 for patient master data
def scd_type_2_patient_data():
    current_data = spark.table("gold_patient_master")
    new_data = spark.table("silver_patient_updates")
    
    # Identify changes
    changes = new_data.join(
        current_data.filter(col("is_current") == True),
        ["patient_id"],
        "left_outer"
    ).where(
        (current_data["first_name"] != new_data["first_name"]) |
        (current_data["last_name"] != new_data["last_name"]) |
        (current_data["address"] != new_data["address"]) |
        (current_data["phone"] != new_data["phone"]))
    
    # Apply SCD Type 2
    delta_table = DeltaTable.forName("gold_patient_master")
    
    delta_table.alias("target").merge(
        changes.alias("source"),
        "target.patient_id = source.patient_id AND target.is_current = true"
    ).whenMatchedUpdate(
        set = {
            "is_current": "false",
            "end_date": "source.update_date"}
    ).whenNotMatchedInsert(
        values = {
            "patient_id": "source.patient_id",
            "first_name": "source.first_name",
            "last_name": "source.last_name",
            "address": "source.address",
            "phone": "source.phone",
            "start_date": "source.update_date",
            "end_date": "null",
            "is_current": "true"}).execute()

🔹 8. Pipeline Orchestration
Q: How did you automate your daily ETL jobs for ingesting and processing patient data?
 Expected: Use of Databricks Jobs, Workflows, Task dependencies, or external tools like Airflow.

%python
# Databricks Workflows for healthcare ETL
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

w = WorkspaceClient()

# Create daily ETL pipeline
pipeline_def = {
    "name": "daily-healthcare-etl",
    "tasks": [
        {
            "task_key": "ingest_raw_data",
            "existing_cluster_id": "cluster-123",
            "spark_jar_task": {
                "main_class_name": "com.healthcare.IngestionJob"
            }
        },
        {
            "task_key": "validate_and_clean",
            "depends_on": [{"task_key": "ingest_raw_data"}],
            "existing_cluster_id": "cluster-123",
            "notebook_task": {
                "notebook_path": "/Validate-Healthcare-Data"
            }
        },
        {
            "task_key": "load_to_silver",
            "depends_on": [{"task_key": "validate_and_clean"}],
            "existing_cluster_id": "cluster-123",
            "notebook_task": {
                "notebook_path": "/Load-Silver-Layer"
            }
        },
        {
            "task_key": "aggregate_gold",
            "depends_on": [{"task_key": "load_to_silver"}],
            "existing_cluster_id": "cluster-123",
            "notebook_task": {
                "notebook_path": "/Aggregate-Gold-Layer"
            }
        }
    ],
    "schedule": {
        "quartz_cron_expression": "0 0 2 * * ?",  # Daily at 2 AM
        "timezone_id": "America/New_York"}}

# Create the workflow
pipeline = w.jobs.create(**pipeline_def)

🔹 9. Data Governance
Q: How did you manage access to sensitive lab test results or patient details in a shared Databricks workspace?
 Expected: Unity Catalog, row-level/column-level permissions, secure clusters.

%python
# Unity Catalog governance for healthcare data
def setup_healthcare_governance():
    # Create catalogs
    spark.sql("CREATE CATALOG IF NOT EXISTS healthcare_prod")
    spark.sql("CREATE CATALOG IF NOT EXISTS healthcare_dev")
    
    # Grant access with fine-grained controls
    spark.sql("""
    GRANT USAGE ON CATALOG healthcare_prod TO `doctors_group`""")
    
    spark.sql("""
    GRANT SELECT ON TABLE healthcare_prod.patient.lab_results TO `lab_technicians`""")
    
    # Row-level security
    spark.sql("""
    CREATE ROW FILTER healthcare_prod.patient.patient_filter 
    ON healthcare_prod.patient.records
    AS (hospital_id = current_hospital_id())""")
    
    # Column-level security for sensitive data
    spark.sql("""
    CREATE MASK healthcare_prod.patient.mask_ssn 
    ON healthcare_prod.patient.records.ssn
    AS (CASE WHEN is_member('hipaa_compliant') THEN ssn
             ELSE CONCAT('***-**-', SUBSTRING(ssn, 8, 4)) END)""")
    
    # Audit logging
    spark.conf.set("spark.databricks.audit.logging.enabled", "true")
    spark.conf.set("spark.databricks.audit.logLevel", "INFO")

🔹 10. Optimization Techniques
Q: Healthcare data is usually large. How did you optimize the performance of your Databricks job?
 Expected: Partitioning, caching, Z-ordering in Delta Lake, and optimized cluster sizing.

%python
# Healthcare data optimization strategies
def optimize_healthcare_tables():
    # 1. Partition large tables
    spark.sql("""
    OPTIMIZE healthcare_prod.patient.records 
    WHERE admission_date >= '2024-01-01'
    """)
    
    # 2. Z-Ordering for common queries
    spark.sql("""
    OPTIMIZE healthcare_prod.patient.records 
    ZORDER BY (patient_id, hospital_id, admission_date)
    """)
    
    # 3. Enable caching for frequently accessed data
    spark.sql("CACHE TABLE healthcare_prod.patient.active_records")
    
    # 4. Auto-compaction
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    
    # 5. Cluster optimization
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    # 6. File size optimization
    spark.conf.set("spark.databricks.delta.targetFileSize", "128000000")  # 128MB
    
    # 7. Statistics collection
    spark.sql("ANALYZE TABLE healthcare_prod.patient.records COMPUTE STATISTICS FOR ALL COLUMNS")
