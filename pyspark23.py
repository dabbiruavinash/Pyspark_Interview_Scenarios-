# Broadcast Join Optimization

%python
from pyspark.sql.functions import broadcast

# Since ICD-10 lookup table is very small (300 rows), use broadcast join
enriched_claims = claims_df.join(
    broadcast(icd10_lookup_df),
    claims_df.diagnosis_code == icd10_lookup_df.icd10_code,
    "left")

# Alternatively, set auto-broadcast threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")  # ~300 rows easily fits

# Patient Demographics Enrichment

%python
# Pre-cache the small demographics lookup
demographics_df.cache().count()  # Force caching

# Use broadcast join for efficient enrichment
enriched_claims = claims_df.join(
    broadcast(demographics_df),
    claims_df.member_id == demographics_df.member_id,
    "left")

# Additional optimization: Filter claims first, then join
recent_claims = claims_df.filter(col("service_date") > "2024-01-01")
enriched_recent = recent_claims.join(
    broadcast(demographics_df),
    "member_id")

# Claims Skew Problem

%python
# Solution: Salting technique for skewed provider
from pyspark.sql.functions import concat, lit, rand

# Add salt to the skewed provider ID
salted_claims = claims_df.withColumn(
    "salted_provider_id",
    concat(col("provider_id"), lit("_"), (rand() * 10).cast("int")))

# Create salted provider directory
provider_ids = ["SKEWED_PROVIDER_ID"]  # The problematic provider
salted_providers = provider_dir_df.withColumn(
    "join_key",
    explode(array([concat(lit(pid), lit("_"), lit(i)) for i in range(10) for pid in provider_ids]))).union(provider_dir_df)  # Keep original for non-skewed providers

result = salted_claims.join(salted_providers, salted_claims.salted_provider_id == salted_providers.join_key)

# Fraud Detection with Streaming

%python
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import window, current_timestamp

# Use stateful processing with watermark for fraud detection
fraud_stream = spark.readStream \
    .schema(claims_schema) \
    .option("maxFilesPerTrigger", 100) \
    .json("s3://cigna-claims-stream/")

fraud_detection = fraud_stream \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window("event_time", "5 minutes", "1 minute"),
        "provider_id",
        "member_id").agg(count("*").alias("claim_count")) \
    .filter(col("claim_count") > 10)  # Threshold for suspicious activity

query = fraud_detection.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("checkpointLocation", "s3://checkpoints/fraud-detection/") \
    .start()

# Partitioning Claims Data

%python
# Optimal partitioning strategy
claims_df.write.partitionBy("service_date", "member_id_prefix") \
    .parquet("s3://cigna-claims-optimized/")

# Why this strategy?
# 1. service_date: Most queries filter by date range
# 2. member_id_prefix: Distributes data evenly (e.g., first 2 chars of member_id)
# 3. Avoid provider_id partitioning due to skew issues

# Repeated Queries for BI

%python
# Cache oncology claims for repeated queries
oncology_claims = claims_df.filter(col("diagnosis_code").like("C%"))  # ICD-10 cancer codes
oncology_claims.cache().count()  # Force caching

# Create optimized view for BI team
oncology_claims.createOrReplaceTempView("oncology_claims_optimized")

# Use for dashboards
spark.sql("""
    SELECT provider_id, COUNT(*), AVG(claim_amount)
    FROM oncology_claims_optimized
    WHERE service_date >= '2024-01-01'
    GROUP BY provider_id""")

# HIPAA Compliance

%python
# HIPAA-compliant Spark caching
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Use encrypted caching and secure data handling
claims_df = claims_df.withColumn(
    "member_id_hashed", 
    sha2(col("member_id"), 256)).drop("member_id")  # Remove original PHI

# Only cache non-PHI columns
non_phi_data = claims_df.select("claim_id", "service_date", "amount", "provider_id")
non_phi_data.cache()

# Enable encryption at rest and in transit
spark.conf.set("spark.ssl.enabled", "true")
spark.conf.set("spark.sql.warehouse.dir", "s3://encrypted-bucket/")

# Average Claim Cost Calculation

%python
# Optimize with map-side aggregation
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", "true")

# Use two-phase aggregation for better distribution
avg_claim_cost = claims_df.groupBy("provider_id") \
    .agg(
        sum("claim_amount").alias("total_amount"),
        count("*").alias("claim_count")).withColumn("avg_amount", col("total_amount") / col("claim_count"))

# Why shuffle occurs: Grouping by provider_id requires data redistribution

# Top Medical Procedures

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

window_spec = Window.partitionBy("region").orderBy(desc("procedure_count"))

top_procedures = claims_df.groupBy("region", "procedure_code") \
    .count().alias("procedure_count") \
    .withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") <= 5) \
    .drop("rank")

# Data Quality Checks

%python
from pyspark.sql.functions import when, count, col

data_quality_checks = claims_df.agg(
    count("*").alias("total_records"),
    count(when(col("claim_id").isNull(), True)).alias("null_claim_ids"),
    count(when(col("provider_id").isNull(), True)).alias("null_provider_ids"),
    count(when(col("service_date").isNull(), True)).alias("null_dates"),
    count(when(col("service_date") > current_date(), True)).alias("future_dates"),
    countDistinct("claim_id").alias("unique_claim_ids")).withColumn("duplicate_rate", (col("total_records") - col("unique_claim_ids")) / col("total_records"))

# File Format for Claims Storage

%python
# Convert from CSV to Parquet with optimization
claims_df.write \
    .partitionBy("service_year", "service_month") \
    .option("compression", "snappy") \
    .parquet("s3://cigna-claims-parquet/")

# Why Parquet?
# 1. Columnar storage - efficient for analytical queries
# 2. Compression - reduces storage costs by ~75%
# 3. Predicate pushdown - faster filtering
# 4. Schema evolution - handles changing data structures
# 5. HIPAA-friendly - supports encryption

# Batch + Streaming Hybrid

%python
# Batch processing for historical data
def process_batch_claims():
    historical_claims = spark.read.parquet("s3://cigna-claims-batch/")
    # ETL processing
    return historical_claims

# Streaming for real-time fraud detection
def process_streaming_claims():
    stream_df = spark.readStream.parquet("s3://cigna-claims-stream/")
    fraud_alerts = stream_df.transform(detect_fraud)  # Your fraud detection logic
    return fraud_alerts

# Unified pipeline
batch_results = process_batch_claims()
streaming_query = process_streaming_claims().writeStream \
    .outputMode("update") \
    .format("delta") \
    .start()

# Use Delta Lake for ACID compliance

# Slow Job Troubleshooting

%python
# Debug steps:

# 1. Check execution plan
claims_df.explain(True)

# 2. Check partition distribution
print(f"Partition count: {claims_df.rdd.getNumPartitions()}")
claims_df.rdd.glom().map(len).collect()  # Check partition sizes

# 3. Check data skew
claims_df.groupBy("provider_id").count().orderBy(desc("count")).show(10)

# 4. Monitor shuffle spill
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", "true")

# 5. Check storage level
print(claims_df.storageLevel)

# 6. Use Spark UI to identify bottlenecks
enriched_claims.createOrReplaceTempView("claims_analytics")
return enriched_claims




