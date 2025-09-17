How would you filter out invalid prescriptions from daily transactional data using PySpark?

# How can PySpark help detect doctor shopping behavior by analyzing prescription patterns?

%python
from pyspark.sql.functions import col, to_date, datediff, current_date, when

def filter_invalid_prescriptions(prescriptions_df):
    invalid_prescriptions = prescriptions_df.filter(
        (col("prescription_id").isNull()) |
        (col("patient_id").isNull()) |
        (col("doctor_npi").isNull()) |
        (col("drug_ndc").isNull()) |
        (col("prescription_date") > current_date()) |
        (col("refills_authorized") < 0) |
        (col("quantity") <= 0) |
        (col("days_supply") <= 0) |
        (datediff(current_date(), col("prescription_date")) > 365) |  # Older than 1 year
        (~col("drug_ndc").rlike("^[0-9]{11}$"))  # Invalid NDC format)
    
    valid_prescriptions = prescriptions_df.subtract(invalid_prescriptions)
    return valid_prescriptions, invalid_prescriptions

# How would you build a streaming job to detect multiple controlled substance pickups by the same patient within 24 hours?

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import countDistinct, datediff, collect_set

def detect_doctor_shopping(prescriptions_df, days_window=30, min_doctors=3):
    window_spec = Window.partitionBy("patient_id").orderBy("prescription_date")
    
    doctor_shopping = (prescriptions_df
        .withColumn("prev_date", lag("prescription_date").over(window_spec))
        .withColumn("days_between", 
                   datediff(col("prescription_date"), col("prev_date")))
        .filter(col("days_between") <= days_window)
        .groupBy("patient_id")
        .agg(
            countDistinct("doctor_npi").alias("unique_doctors"),
            collect_set("drug_ndc").alias("prescribed_drugs"))
        .filter(col("unique_doctors") >= min_doctors))
    return doctor_shopping

# How do you de-identify or mask PHI (e.g., SSN, name, address) in compliance with HIPAA using PySpark?

%python
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import window, count

def controlled_substance_monitoring():
    controlled_drugs = [" opioid_ndc_codes ", " benzodiazepine_ndc_codes "]
    
    stream_df = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "pharmacy-kafka:9092")
        .option("subscribe", "prescription-pickups")
        .load()
        .select(from_json(col("value").cast("string"), pickup_schema).alias("data"))
        .select("data.*")
        .filter(col("drug_ndc").isin(controlled_drugs)))
    
    alerts = (stream_df
        .withWatermark("pickup_time", "1 hour")
        .groupBy(
            window(col("pickup_time"), "24 hours"),
            "patient_id")
        .agg(count("*").alias("pickup_count"))
        .filter(col("pickup_count") > 1))
    
    query = alerts.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("checkpointLocation", "/checkpoints/controlled-substances/") \
        .start()
    
    return query

# How would you remove duplicate claims data while keeping only the most recent one?

%python
from pyspark.sql.functions import sha2, regexp_replace, lit

def deidentify_phi_data(patient_df):
    deidentified = patient_df.withColumn(
        "patient_id_hashed", sha2(col("patient_id"), 256)
    ).withColumn(
        "ssn_masked", 
        regexp_replace(col("ssn"), r"(\d{3})-(\d{2})-(\d{4})", "***-**-$3")
    ).withColumn(
        "name_initial", 
        concat(substring(col("first_name"), 1, 1), lit("."), substring(col("last_name"), 1, 1))
    ).withColumn(
        "address_generalized",
        regexp_replace(col("address"), r"^\d+\s+", "*** ")  # Remove house number
    ).drop("patient_id", "ssn", "first_name", "last_name", "address", "email", "phone")
    
    return deidentified

# How do you calculate the Medication Possession Ratio (MPR) for patients using PySpark?

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

def remove_duplicate_claims(claims_df):
    window_spec = Window.partitionBy(
        "claim_id", "patient_id", "service_date"
    ).orderBy(col("processing_date").desc())
    
    deduplicated = (claims_df
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num"))
    return deduplicated

# How would you analyze whether patients with abnormal lab results are being prescribed the appropriate medications using PySpark?

%python
from pyspark.sql.functions import sum, datediff, greatest, least

def calculate_mpr(patient_id, fill_history):
    fills = fill_history.filter(col("patient_id") == patient_id)
    
    mpr_calculation = fills.agg(
        sum("days_supply").alias("total_days_supply"),
        datediff(
            max("fill_date"), 
            min("fill_date")).alias("observation_period")).withColumn("mpr", col("total_days_supply") / greatest(col("observation_period"), lit(1)))
    
    return mpr_calculation

# How do you implement incremental data loading using watermarks or last updated timestamps in PySpark?

%python
def analyze_lab_prescription_correlation(labs_df, prescriptions_df):
    abnormal_labs = labs_df.filter(
        (col("lab_value") > col("normal_upper_limit")) |
        (col("lab_value") < col("normal_lower_limit"))
    )
    
    analysis = (abnormal_labs
        .join(prescriptions_df, 
              (abnormal_labs.patient_id == prescriptions_df.patient_id) &
              (abnormal_labs.lab_date <= prescriptions_df.prescription_date) &
              (abnormal_labs.lab_date >= date_sub(prescriptions_df.prescription_date, 30))),
              "left")
        .groupBy("lab_test", "abnormal_direction")
        .agg(
            count("*").alias("total_abnormal"),
            count(when(col("drug_ndc").isNotNull(), True)).alias("treated_cases"),
            (count(when(col("drug_ndc").isNotNull(), True)) / count("*")).alias("treatment_rate")))
    return analysis

# How would you join CVS membership data with refill data to analyze refill behavior across different customer groups?

%python
def incremental_load_with_watermark():
    incremental_data = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/schema/incremental/")
        .load("s3://cvs-data/incremental/")
        .withWatermark("last_updated", "1 hour")
    )
    
    # Merge with existing data
    def incremental_merge(batch_df, batch_id):
        delta_table = DeltaTable.forPath(spark, "/delta/patient_records")
        
        delta_table.alias("target").merge(
            batch_df.alias("source"),
            "target.patient_id = source.patient_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    
    query = incremental_data.writeStream \
        .foreachBatch(incremental_merge) \
        .option("checkpointLocation", "/checkpoints/incremental/") \
        .start()

# How can you optimize a slow PySpark job that processes millions of healthcare claims each day?

%python
def optimize_claims_processing():
    # 1. Enable adaptive query execution
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    # 2. Optimize Delta tables
    spark.sql("OPTIMIZE claims_table ZORDER BY (patient_id, service_date)")
    
    # 3. Use broadcast joins for small dimension tables
    claims_enriched = claims_df.join(broadcast(providers_df), "provider_id")
    
    # 4. Cache frequently used datasets
    active_patients = claims_df.filter(col("service_date") >= "2024-01-01").cache()
    
    # 5. Partition data effectively
    claims_df.write.partitionBy("service_year", "service_month").format("delta").save()
    
    # 6. Use efficient file formats
    spark.conf.set("spark.sql.parquet.filterPushdown", "true")

# How would you handle schema evolution when reading JSON medical data from a dynamic source like S3?

%python
def handle_schema_evolution():
    evolving_data = (spark.read
        .format("json")
        .option("multiLine", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .load("s3://cvs-medical-data/")
        .withColumn("ingestion_time", current_timestamp()))
    
    # Define schema merge policy
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    
    # Write with schema evolution
    evolving_data.write \
        .format("delta") \
        .option("mergeSchema", "true") \
        .mode("append") \
        .save("/delta/medical_records/")

# How would you process real-time pharmacy sales data using Structured Streaming in PySpark?

%python
def real_time_pharmacy_sales():
    sales_stream = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "pharmacy-sales-kafka:9092")
        .option("subscribe", "real-time-sales")
        .option("startingOffsets", "latest")
        .load()
        .select(
            from_json(col("value").cast("string"), sales_schema).alias("data"),
            col("timestamp")
        )
        .select("data.*", "timestamp")
    )
    
    # Real-time analytics
    real_time_metrics = (sales_stream
        .withWatermark("timestamp", "5 minutes")
        .groupBy(
            window(col("timestamp"), "1 hour", "30 minutes"),
            "store_id", "product_category"
        )
        .agg(
            sum("amount").alias("hourly_sales"),
            count("*").alias("transaction_count"),
            approx_count_distinct("customer_id").alias("unique_customers")))
    
    query = real_time_metrics.writeStream \
        .outputMode("update") \
        .format("delta") \
        .option("checkpointLocation", "/checkpoints/sales-realtime/") \
        .start()

# How do you handle skewed joins when joining patient demographics and prescription history in PySpark?

%python
def handle_skewed_joins(patients_df, prescriptions_df):
    # Identify skewed patient IDs (high prescription counts)
    skewed_patients = prescriptions_df.groupBy("patient_id").count() \
        .filter(col("count") > 1000).select("patient_id").rdd.flatMap(lambda x: x).collect()
    
    # Salting technique
    salted_prescriptions = prescriptions_df.withColumn(
        "salted_patient_id",
        when(col("patient_id").isin(skewed_patients),
             concat(col("patient_id"), lit("_"), (rand() * 10).cast("int")))
        .otherwise(col("patient_id"))
    )
    
    # Create salted patient dimension
    salted_patients = patients_df.filter(col("patient_id").isin(skewed_patients)) \
        .withColumn("join_key", explode(array([lit(f"{pid}_{i}") for i in range(10) for pid in skewed_patients]))) \
        .union(patients_df.filter(~col("patient_id").isin(skewed_patients)))
    
    # Perform join
    result = salted_prescriptions.join(salted_patients, 
                                      salted_prescriptions.salted_patient_id == salted_patients.join_key,
                                      "inner")
    return result

# How do you handle late-arriving medical event data in a structured streaming pipeline?

%python
def handle_late_arriving_data():
    medical_events = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "medical-events-kafka:9092")
        .option("subscribe", "patient-events")
        .load()
        .select(from_json(col("value").cast("string"), event_schema).alias("data"))
        .select("data.*")
        .withWatermark("event_time", "2 hours")  # Allow 2 hours late data
    )
    
    # Windowed aggregation with late data handling
    event_analysis = (medical_events
        .groupBy(
            window(col("event_time"), "1 hour"),
            "patient_id", "event_type"
        )
        .agg(count("*").alias("event_count"))
    )
    
    query = event_analysis.writeStream \
        .outputMode("update") \
        .format("delta") \
        .option("checkpointLocation", "/checkpoints/medical-events/") \
        .start()

# How would you build a PySpark pipeline that anonymizes, validates, and loads patient insurance claims?

%python
def claims_processing_pipeline():
    # 1. Ingestion
    raw_claims = spark.read.format("json").load("s3://cvs-claims-raw/")
    
    # 2. Anonymization
    anonymized = raw_claims.transform(deidentify_phi_data)
    
    # 3. Validation
    validated_claims = anonymized.filter(
        col("claim_id").isNotNull() &
        col("service_date").isNotNull() &
        col("amount").isNotNull() &
        (col("amount") > 0)
    )
    
    # 4. Enrichment
    enriched_claims = (validated_claims
        .join(broadcast(providers_df), "provider_id", "left")
        .join(broadcast(patients_df), "patient_id", "left")
    )
    
    # 5. Load to Delta Lake
    enriched_claims.write \
        .format("delta") \
        .partitionBy("service_year", "service_month") \
        .mode("append") \
        .save("/delta/processed_claims/")
    
    # 6. Data Quality Checks
    run_data_quality_checks(enriched_claims)

# How can you enrich prescription data with drug pricing and insurance eligibility tables using PySpark?

%python
def enrich_prescription_data(prescriptions_df):
    # Join with drug pricing
    with_pricing = prescriptions_df.join(
        broadcast(drug_pricing_df),
        prescriptions_df.drug_ndc == drug_pricing_df.ndc_code,
        "left"
    )
    
    # Join with insurance eligibility
    with_insurance = with_pricing.join(
        broadcast(insurance_eligibility_df),
        (with_pricing.patient_id == insurance_eligibility_df.patient_id) &
        (with_pricing.service_date >= insurance_eligibility_df.effective_date) &
        (with_pricing.service_date <= insurance_eligibility_df.expiration_date),
        "left")
    
    # Calculate patient responsibility
    enriched = with_insurance.withColumn(
        "patient_responsibility",
        when(col("coverage_percentage").isNotNull(),
             col("drug_price") * (1 - col("coverage_percentage")/100))
        .otherwise(col("drug_price")))
    
    return enriched

# How would you implement change data capture (CDC) logic in PySpark for the claims system?

%python
def cdc_for_claims():
    cdc_stream = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "claims-cdc-kafka:9092")
        .option("subscribe", "claims-cdc")
        .load()
        .select(from_json(col("value").cast("string"), cdc_schema).alias("data"))
        .select("data.*")
    )
    
    def apply_cdc(batch_df, batch_id):
        delta_table = DeltaTable.forPath(spark, "/delta/claims")
        
        delta_table.alias("target").merge(
            batch_df.alias("source"),
            "target.claim_id = source.claim_id"
        ).whenMatchedUpdate(
            condition = "source.operation = 'update'",
            set = {
                "amount": "source.amount",
                "status": "source.status",
                "updated_date": "source.cdc_timestamp"
            }
        ).whenMatchedDelete(
            condition = "source.operation = 'delete'"
        ).whenNotMatchedInsert(
            condition = "source.operation = 'insert'",
            values = {
                "claim_id": "source.claim_id",
                "patient_id": "source.patient_id",
                "amount": "source.amount",
                "status": "source.status",
                "created_date": "source.cdc_timestamp"
            }
        ).execute()
    
    query = cdc_stream.writeStream \
        .foreachBatch(apply_cdc) \
        .option("checkpointLocation", "/checkpoints/claims-cdc/") \
        .start()

# How would you track patient medication adherence over a 6-month period using refill and dosage data in PySpark?

%python
def track_medication_adherence(patient_id, start_date, end_date):
    patient_refills = refills_df.filter(
        (col("patient_id") == patient_id) &
        (col("fill_date") >= start_date) &
        (col("fill_date") <= end_date)
    ).orderBy("fill_date")
    
    adherence_calculation = patient_refills.agg(
        sum("days_supply").alias("total_days_covered"),
        datediff(max("fill_date"), min("fill_date")).alias("observation_period")
    ).withColumn(
        "pdc",  # Proportion of Days Covered
        col("total_days_covered") / greatest(col("observation_period"), lit(1))
    ).withColumn(
        "adherence_status",
        when(col("pdc") >= 0.8, "Adherent")
        .when(col("pdc") >= 0.5, "Partially Adherent")
        .otherwise("Non-Adherent")
    )
    
    return adherence_calculation

# How do you integrate PySpark jobs with Airflow to orchestrate ETL workflows in a pharmacy analytics project?

%python
# airflow_dag.py
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'cvs-analytics',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('cvs_pharmacy_etl', default_args=default_args, schedule_interval='@daily')

process_claims = DatabricksSubmitRunOperator(
    task_id='process_pharmacy_claims',
    databricks_conn_id='databricks_default',
    existing_cluster_id='cluster-123',
    notebook_task={
        'notebook_path': '/Production/ProcessClaims'
    },
    dag=dag
)

generate_reports = DatabricksSubmitRunOperator(
    task_id='generate_daily_reports',
    databricks_conn_id='databricks_default',
    existing_cluster_id='cluster-123',
    notebook_task={
        'notebook_path': '/Production/GenerateReports'},
    dag=dag)

process_claims >> generate_reports

# How would you implement audit logging for a PySpark job that processes sensitive patient billing data?

%python
def create_audit_logging(job_name, processed_df):
    audit_log = processed_df.agg(
        count("*").alias("total_records"),
        count(when(col("patient_id").isNull(), True)).alias("null_patient_ids"),
        count(when(col("amount").isNull(), True)).alias("null_amounts"),
        min("processing_date").alias("min_processing_date"),
        max("processing_date").alias("max_processing_date")
    ).withColumn("job_name", lit(job_name)) \
     .withColumn("run_id", lit(uuid.uuid4().hex)) \
     .withColumn("timestamp", current_timestamp())
    
    # Write audit log to secure location
    audit_log.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save("/audit/logs/")
    
    # Alert on data quality issues
    if audit_log.filter(col("null_patient_ids") > 0).count() > 0:
        send_alert(f"Data quality issue in {job_name}: null patient IDs detected")
    
    return audit_log