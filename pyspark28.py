# Processing Millions of Transactions with Skew Handling

%python
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def process_transactions_with_skew_handling(transactions_df):
    # Identify skewed branch
    branch_distribution = transactions_df.groupBy("branch_id").count().orderBy(desc("count"))
    skewed_branch = branch_distribution.first()["branch_id"]
    
    # Salting technique for skewed branch
    salted_transactions = transactions_df.withColumn(
        "salted_branch_id",
        when(col("branch_id") == skewed_branch,
             concat(col("branch_id"), lit("_"), (rand() * 10).cast("int")))
        .otherwise(col("branch_id"))
    )
    
    # Process with optimized partitioning
    processed = (salted_transactions
        .repartition(100, "salted_branch_id", "transaction_date")  # Even distribution
        .withColumn("processing_timestamp", current_timestamp())
        .withColumn("transaction_category",
                   when(col("amount") > 10000, "LARGE")
                   .when(col("amount") > 1000, "MEDIUM")
                   .otherwise("SMALL")))
    
    return processed

# Daily processing pipeline
daily_transactions = spark.read.parquet("s3://transactions-daily/")
processed_data = process_transactions_with_skew_handling(daily_transactions)
processed_data.write.partitionBy("transaction_date").parquet("s3://processed-transactions/")

# Cleaning Financial Data with Null Values

%python
def clean_financial_data(transactions_df):
    # Strategy 1: Fill nulls with default values for categorical columns
    cleaned_df = transactions_df.fillna({
        "transaction_type": "UNKNOWN",
        "currency": "USD",
        "status": "PENDING"
    })
    
    # Strategy 2: Use mode for categorical columns with many nulls
    mode_transaction_type = transactions_df.groupBy("transaction_type").count() \
        .orderBy(desc("count")).first()["transaction_type"]
    
    cleaned_df = cleaned_df.fillna({"transaction_type": mode_transaction_type})
    
    # Strategy 3: For numerical columns, use mean/median or business logic
    mean_amount = transactions_df.agg(avg("amount")).first()[0]
    cleaned_df = cleaned_df.fillna({"amount": mean_amount})
    
    # Strategy 4: For critical records, keep but flag them
    cleaned_df = cleaned_df.withColumn(
        "data_quality_issue",
        when(col("transaction_id").isNull() | col("amount").isNull(), "CRITICAL_NULL")
        .when(col("transaction_type").isNull(), "MISSING_TYPE")
        .otherwise("CLEAN")
    )
    
    return cleaned_df

# Alternative: Use Delta Live Tables expectations
@dlt.table
@dlt.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
@dlt.expect_or_drop("valid_amount", "amount IS NOT NULL AND amount > 0")
@dlt.expect("valid_transaction_type", "transaction_type IS NOT NULL")
def cleaned_transactions():
    return spark.read.table("raw_transactions")

# Join Large Transactions with Small Customer Table

%python
from pyspark.sql.functions import broadcast

def join_transactions_customers(transactions_df, customers_df):
    # Since customers table is small, use broadcast join
    enriched_transactions = transactions_df.join(
        broadcast(customers_df),
        transactions_df.customer_id == customers_df.customer_id,
        "left"  # Keep all transactions even if customer not found
    )
    
    # Alternative: If customers table is moderately sized but still smaller
    # Enable automatic broadcast join threshold
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50MB")
    
    return enriched_transactions

# Usage
transactions = spark.table("transactions")
customers = spark.table("dim_customers")  # Small dimension table
result = join_transactions_customers(transactions, customers)

# Handling Schema Changes in Nightly Pipeline

%python
def handle_schema_changes_nightly():
    # Read with schema evolution support
    new_data = (spark.read
        .format("parquet")
        .option("mergeSchema", "true")  # Auto-merge new columns
        .load("s3://nightly-transactions/")
    )
    
    # Get current schema of target table
    target_schema = spark.read.table("processed_transactions").schema
    
    # Align schemas
    for field in target_schema:
        if field.name not in new_data.columns:
            new_data = new_data.withColumn(field.name, lit(None).cast(field.dataType))
    
    # Add new columns to target schema if they exist in source
    for col_name in new_data.columns:
        if col_name not in [f.name for f in target_schema]:
            spark.sql(f"ALTER TABLE processed_transactions ADD COLUMN {col_name} STRING")
    
    # Write with schema evolution
    new_data.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable("processed_transactions")

# Debugging and Fixing Out-of-Memory Errors

%python
def debug_and_fix_memory_issues():
    # 1. Check partition count and size
    print(f"Original partitions: {transactions_df.rdd.getNumPartitions()}")
    transactions_df.rdd.glom().map(len).collect()  # Check partition sizes
    
    # 2. Optimize partitioning
    optimized_df = transactions_df.repartition(200, "transaction_date")
    
    # 3. Use efficient persistence
    optimized_df.persist(StorageLevel.MEMORY_AND_DISK_SER)  # Serialized storage
    
    # 4. Adjust Spark configuration
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    spark.conf.set("spark.executor.memory", "8g")
    spark.conf.set("spark.memory.fraction", "0.8")
    
    # 5. Use column pruning and filter pushdown
    essential_cols = optimized_df.select("transaction_id", "amount", "date", "customer_id")
    filtered = essential_cols.filter(col("date") >= "2024-01-01")
    
    # 6. Monitor with Spark UI
    # Check storage tab for cached data size
    # Check executors tab for memory usage
    
    return filtered

# Additional: Use spill-to-disk for large operations
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")

# Masking Sensitive Fields for Data Lake

%python
from pyspark.sql.functions import sha2, regexp_replace

def mask_sensitive_data(df):
    masked_df = df.withColumn(
        "ssn_masked", 
        regexp_replace(col("ssn"), r"(\d{3})-(\d{2})-(\d{4})", "***-**-$3")
    ).withColumn(
        "card_number_masked",
        regexp_replace(col("card_number"), r"(\d{4})-(\d{4})-(\d{4})-(\d{4})", "****-****-****-$4")
    ).withColumn(
        "email_masked",
        regexp_replace(col("email"), r"(\w{3})[\w.-]+@([\w.]+)", "$1***@$2")
    ).withColumn(
        "customer_id_hashed", 
        sha2(col("customer_id"), 256)
    ).drop("ssn", "card_number", "email", "customer_id")  # Remove original sensitive data
    
    return masked_df

# For compliance logging
def create_audit_trail(original_df, masked_df):
    audit_log = original_df.select("transaction_id", "customer_id").join(
        masked_df.select("transaction_id", "customer_id_hashed"),
        "transaction_id")
    audit_log.write.parquet("s3://audit-logs/masking/")

# Handling Data Duplication Complaints

%python
def investigate_and_fix_duplicates():
    # 1. Identify duplicates
    duplicate_check = transactions_df.groupBy("transaction_id", "transaction_time").count() \
        .filter(col("count") > 1)
    
    if duplicate_check.count() > 0:
        print(f"Found {duplicate_check.count()} duplicate groups")
        
        # 2. Use window function to keep latest record
        window_spec = Window.partitionBy("transaction_id").orderBy(desc("processing_timestamp"))
        
        deduplicated = transactions_df.withColumn("row_num", row_number().over(window_spec)) \
            .filter(col("row_num") == 1) \
            .drop("row_num")
        
        # 3. Validate no duplicates
        final_count = deduplicated.groupBy("transaction_id").count() \
            .filter(col("count") > 1).count()
        
        if final_count == 0:
            print("Successfully removed duplicates")
            return deduplicated
        else:
            raise Exception("Still found duplicates after deduplication")
    
    return transactions_df

# Prevention: Use UPSERT with Delta Lake
def write_with_deduplication(new_data):
    delta_table = DeltaTable.forPath(spark, "/delta/transactions")
    
    delta_table.alias("target").merge(
        new_data.alias("source"),
        "target.transaction_id = source.transaction_id AND target.transaction_time = source.transaction_time"
    ).whenNotMatchedInsertAll().execute()

# Hourly Aggregation with Late-Arriving Data

%python
def hourly_aggregation_with_late_data():
    # For batch processing with late data
    hourly_agg = (transactions_df
        .withColumn("hour_window", date_trunc("hour", col("transaction_time")))
        .groupBy("hour_window", "branch_id", "transaction_type")
        .agg(
            sum("amount").alias("total_amount"),
            count("*").alias("transaction_count"),
            approx_count_distinct("customer_id").alias("unique_customers")
        )
    )
    
    # For streaming with watermarking
    stream_agg = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "transactions")
        .load()
        .select(from_json(col("value").cast("string"), transaction_schema).alias("data"))
        .select("data.*")
        .withWatermark("transaction_time", "2 hours")  # Handle data up to 2 hours late
        .groupBy(
            window(col("transaction_time"), "1 hour"),
            "branch_id"
        )
        .agg(sum("amount").alias("hourly_total")))
    
    return hourly_agg

# Optimizing Multiple Joins and Aggregations

%python
def optimize_financial_job():
    # 1. Enable adaptive query execution
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    # 2. Use broadcast joins for small tables
    transactions = spark.table("transactions")
    customers = broadcast(spark.table("dim_customers"))
    products = broadcast(spark.table("dim_products"))
    
    # 3. Column pruning - select only needed columns
    essential_data = transactions.select(
        "transaction_id", "customer_id", "product_id", "amount", "date"
    )
    
    # 4. Filter pushdown - filter early
    recent_data = essential_data.filter(col("date") >= "2024-01-01")
    
    # 5. Efficient joins
    enriched = (recent_data
        .join(customers, "customer_id", "left")
        .join(products, "product_id", "left")
    )
    
    # 6. Optimize aggregations
    spark.conf.set("spark.sql.shuffle.partitions", "100")
    
    # 7. Use Delta Lake optimization
    spark.sql("OPTIMIZE transactions ZORDER BY (date, customer_id)")
    
    # 8. Cache intermediate results if reused
    enriched.persist(StorageLevel.MEMORY_AND_DISK)
    
    return enriched

# SCD Type 2 Implementation for Customer Changes

%python
from delta.tables import DeltaTable

def scd_type_2_customer_tracking():
    # Current dimension table
    dim_customers = DeltaTable.forPath(spark, "/delta/dim_customers")
    
    # New customer updates
    customer_updates = spark.table("customer_updates")
    
    # SCD Type 2 implementation
    dim_customers.alias("target").merge(
        customer_updates.alias("source"),
        "target.customer_id = source.customer_id AND target.is_current = true"
    ).whenMatchedUpdate(
        condition = "target.customer_status != source.customer_status OR target.credit_limit != source.credit_limit",
        set = {
            "is_current": "false",
            "end_date": "current_date()"
        }
    ).whenNotMatchedInsert(
        values = {
            "customer_id": "source.customer_id",
            "customer_status": "source.customer_status",
            "credit_limit": "source.credit_limit",
            "start_date": "current_date()",
            "end_date": "null",
            "is_current": "true"
        }
    ).execute()
    
    # Insert new versions for changed records
    changed_customers = customer_updates.join(
        dim_customers.toDF().filter("is_current = true"),
        ["customer_id"],
        "inner"
    ).filter(
        (col("source.customer_status") != col("target.customer_status")) |
        (col("source.credit_limit") != col("target.credit_limit"))
    )
    
    dim_customers.alias("target").insert(
        changed_customers.select(
            "customer_id",
            "customer_status",
            "credit_limit",
            current_date().alias("start_date"),
            lit(None).cast("date").alias("end_date"),
            lit(True).alias("is_current")
        )
    )

# Fault-Tolerant Spark Pipeline for Trading Data

%python
def fault_tolerant_trading_pipeline():
    # Read trading data from S3
    trading_data = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "s3://schema/trading/")
        .load("s3://citi-trading-data/hourly/")
    )
    
    # Process with fault tolerance
    processed_data = trading_data.withColumn("processing_time", current_timestamp()) \
        .withColumn("data_quality_check", 
                   when(col("trade_amount").isNull() | (col("trade_amount") <= 0), "INVALID")
                   .otherwise("VALID"))
    
    # Write to Delta Lake with checkpointing
    query = (processed_data.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "s3://checkpoints/trading-pipeline/")
        .option("path", "s3://processed-trading-data/")
        .start()
    )
    
    # Monitor and alert
    def monitor_pipeline():
        if query.status['message'] == 'Query terminated with exception':
            send_alert("Trading pipeline failed: " + query.exception())
            # Auto-restart logic
            query.awaitTermination()
            query.start()
    
    return query

# Exactly-Once Processing Kafka to Redshift

%python
def exactly_once_kafka_redshift():
    # Read from Kafka with exactly-once semantics
    kafka_df = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "transactions")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    
    # Process data
    processed = kafka_df.select(
        from_json(col("value").cast("string"), transaction_schema).alias("data")
    ).select("data.*")
    
    # Write to Redshift with exactly-once
    def write_to_redshift(batch_df, batch_id):
        (batch_df.write
            .format("jdbc")
            .option("url", "jdbc:redshift://redshift-cluster:5439/dev")
            .option("dbtable", "transactions")
            .option("user", "username")
            .option("password", "password")
            .option("aws_iam_role", "arn:aws:iam::123456789012:role/RedshiftRole")
            .mode("append")
            .save()
        )
        
        # Commit Kafka offsets (implementation depends on your offset management)
        commit_kafka_offsets(batch_id)
    
    query = processed.writeStream \
        .foreachBatch(write_to_redshift) \
        .option("checkpointLocation", "s3://checkpoints/kafka-redshift/") \
        .start()
    
    return query

# Handling Missing S3 Folders Gracefully

%python
def process_with_missing_folders():
    base_path = "s3://branch-data/"
    branches = ["branch_01", "branch_02", ..., "branch_30"]
    
    all_data = None
    
    for branch in branches:
        branch_path = f"{base_path}{branch}/"
        
        try:
            branch_data = spark.read.parquet(branch_path)
            if all_data is None:
                all_data = branch_data
            else:
                all_data = all_data.union(branch_data)
                
        except Exception as e:
            print(f"Failed to read {branch_path}: {str(e)}")
            # Log missing branch but continue processing
            log_missing_branch(branch, str(e))
    
    if all_data is None:
        raise Exception("No branch data available to process")
    
    return all_data

# Alternative: Use S3 list and filter existing paths
def get_existing_branch_paths():
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket='my-bucket', Prefix='branch-data/')
    
    existing_paths = [f"s3://my-bucket/{item['Key']}" 
                     for item in response.get('Contents', []) 
                     if item['Key'].endswith('.parquet')]
    
    return existing_paths

# Read all existing paths
existing_paths = get_existing_branch_paths()
if existing_paths:
    all_data = spark.read.parquet(*existing_paths)

# Daily Summary Report with Pivoting

%python
def create_daily_summary_report():
    daily_data = spark.read.parquet("s3://daily-transactions/")
    
    # Branch-wise and account-type-wise summary
    summary = (daily_data
        .groupBy("branch_id", "account_type", "transaction_date")
        .agg(
            sum("amount").alias("total_amount"),
            count("*").alias("transaction_count"),
            approx_count_distinct("customer_id").alias("unique_customers"),
            avg("amount").alias("average_transaction")
        )
    )
    
    # Pivot for better reporting
    pivot_summary = (summary
        .groupBy("branch_id", "transaction_date")
        .pivot("account_type", ["CHECKING", "SAVINGS", "BUSINESS", "PREMIUM"])
        .agg(
            sum("total_amount").alias("total_amount"),
            sum("transaction_count").alias("transaction_count")
        )
    )
    
    # Add overall totals
    final_report = pivot_summary.withColumn(
        "grand_total_amount",
        coalesce(col("CHECKING_total_amount"), lit(0)) +
        coalesce(col("SAVINGS_total_amount"), lit(0)) +
        coalesce(col("BUSINESS_total_amount"), lit(0)) +
        coalesce(col("PREMIUM_total_amount"), lit(0))
    )
    
    return final_report

# Designing for Data Rollback Capability

%python
def design_for_rollback():
    # Use Delta Lake time travel for rollbacks
    def rollback_last_batch():
        # Get current version
        current_version = spark.sql("DESCRIBE HISTORY transactions").agg(max("version")).first()[0]
        
        if current_version > 0:
            # Restore to previous version
            spark.sql("RESTORE TABLE transactions TO VERSION AS OF {}".format(current_version - 1))
            print(f"Rolled back from version {current_version} to {current_version - 1}")
        else:
            print("No previous version to roll back to")
    
    # Alternative: Batch-based processing with versioning
    def process_with_batch_tracking():
        batch_id = generate_batch_id()  # UUID or timestamp
        
        processed_data = process_transactions().withColumn("batch_id", lit(batch_id))
        
        # Write with batch tracking
        processed_data.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable("processed_transactions")
        
        return batch_id
    
    def rollback_batch(batch_id):
        # Delete specific batch
        spark.sql(f"DELETE FROM processed_transactions WHERE batch_id = '{batch_id}'")
        print(f"Rolled back batch {batch_id}")
    
    # Use transactional writes with Delta Lake
    delta_table = DeltaTable.forPath(spark, "/delta/transactions")
    
    def atomic_batch_processing(new_data, batch_id):
        # Atomic operation - all or nothing
        delta_table.alias("target").merge(
            new_data.alias("source"),
            "target.transaction_id = source.transaction_id"
        ).whenNotMatchedInsertAll().execute()
        
        return batch_id