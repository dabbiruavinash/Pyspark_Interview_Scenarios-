# Real-Time Inventory Management System

%python
from delta.tables import DeltaTable
from pyspark.sql.functions import *

# Create Delta Lake tables for inventory
spark.sql("""
CREATE TABLE IF NOT EXISTS inventory_silver USING DELTA
LOCATION 's3://retail-bucket/inventory/silver' AS SELECT * FROM bronze_inventory""")

# Real-time inventory updates
def update_inventory(stream_df, batch_id):
    delta_table = DeltaTable.forPath(spark, 's3://retail-bucket/inventory/silver')
    
    # Merge logic for inventory updates
    delta_table.alias("target").merge(
        stream_df.alias("source"),
        "target.sku = source.sku AND target.store_id = source.store_id").whenMatchedUpdate(set = {
        "quantity": "source.quantity",
        "last_updated": current_timestamp(),
        "status": "CASE WHEN source.quantity <= 0 THEN 'OUT_OF_STOCK' ELSE 'IN_STOCK' END"}).whenNotMatchedInsertAll().execute()

# Stream processing
inventory_stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka-broker:9092")
    .option("subscribe", "inventory-updates")
    .load()
    .select(from_json(col("value").cast("string"), inventory_schema).alias("data"))
    .select("data.*")
    .writeStream
    .foreachBatch(update_inventory)
    .option("checkpointLocation", "s3://checkpoints/inventory/")
    .start())

# Stockout Detection Across Stores

%python
# Real-time stockout detection
stockout_alerts = (spark.readStream
    .table("inventory_silver")
    .withWatermark("timestamp", "5 minutes")
    .filter(col("quantity") <= 0)
    .groupBy(
        window(col("timestamp"), "10 minutes", "5 minutes"),
        "sku", "store_id").agg(
        count("*").alias("out_of_stock_events"),
        max("timestamp").alias("last_event_time"))
    .filter(col("out_of_stock_events") >= 3)  # Multiple confirmations
)

# Send alerts
alert_query = (stockout_alerts
    .writeStream
    .format("delta")
    .outputMode("update")
    .option("checkpointLocation", "s3://checkpoints/stockout-alerts/")
    .table("stockout_alerts_gold"))

# CDC from POS Systems

%python
# Using Auto Loader for CDC files
pos_cdc_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "s3://schema/pos-cdc/")
    .option("cloudFiles.inferColumnTypes", "true")
    .load("s3://pos-cdc-files/")
    .withColumn("processing_time", current_timestamp()))

# Apply CDC to Delta table
def apply_cdc_to_sales(stream_df, batch_id):
    sales_table = DeltaTable.forPath(spark, 's3://retail-bucket/sales/silver')
    
    sales_table.alias("target").merge(
        stream_df.alias("source"),
        "target.sale_id = source.sale_id").whenMatchedUpdate(
        condition = "source.operation = 'update'",
        set = {
            "amount": "source.amount",
            "items": "source.items",
            "updated_at": "source.processing_time"}
    ).whenMatchedDelete(condition = "source.operation = 'delete'"
    ).whenNotMatchedInsert(
        condition = "source.operation = 'insert'",
        values = {
            "sale_id": "source.sale_id",
            "store_id": "source.store_id",
            "amount": "source.amount",
            "items": "source.items",
            "created_at": "source.processing_time"}).execute()

cdc_query = (pos_cdc_stream
    .writeStream
    .foreachBatch(apply_cdc_to_sales)
    .option("checkpointLocation", "s3://checkpoints/pos-cdc/")
    .start())

# Optimizing Slow Retail ETL Pipeline

%python
# Optimization strategies:

# 1. Enable Adaptive Query Execution
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# 2. Use Delta Lake optimization
spark.sql("OPTIMIZE sales_silver ZORDER BY (date, store_id)")

# 3. Implement predicate pushdown
spark.conf.set("spark.sql.parquet.filterPushdown", "true")

# 4. Cache frequently used datasets
dim_products = spark.table("dim_products").cache()
dim_stores = spark.table("dim_stores").cache()

# 5. Use broadcast joins for small tables
sales_enriched = (sales_fact
    .join(broadcast(dim_products), "product_id")
    .join(broadcast(dim_stores), "store_id"))

# 6. Partition wisely
sales_fact.write.partitionBy("sale_date", "region").format("delta").save()

# 7. Enable file pruning
spark.conf.set("spark.databricks.io.skipping.enabled", "true")

# Handling Late-Arriving Data

%python
# Structured Streaming with watermark and allowedLateness
late_data_handling = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka-broker:9092")
    .option("subscribe", "online-orders")
    .load()
    .select(from_json(col("value").cast("string"), order_schema).alias("data"))
    .select("data.*")
    .withWatermark("event_time", "2 hours")  # Watermark for late data
    .groupBy(window(col("event_time"), "1 hour"),"product_id")
    .agg(sum("quantity").alias("total_quantity"))
    .withColumn("late_data_flag", 
                when(col("window.end") < current_timestamp() - expr("INTERVAL 2 hours"), 
                     "LATE").otherwise("ON_TIME")))

# Process with allowed lateness
query = (late_data_handling
    .writeStream
    .outputMode("update")
    .format("delta")
    .option("checkpointLocation", "s3://checkpoints/late-orders/")
    .option("mergeSchema", "true")
    .start())

# SKU-Level Sales Forecasting with MLflow

%python
import mlflow
from mlflow.tracking import MlflowClient
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor

# Track experiments with MLflow
mlflow.set_experiment("sku-sales-forecasting")

def train_sku_forecast(sku_data):
    with mlflow.start_run():
        # Feature engineering
        assembler = VectorAssembler(
            inputCols=["day_of_week", "month", "promo_flag", "lag_7", "lag_14"],
            outputCol="features"
        )
        
        # Train model
        rf = RandomForestRegressor(
            featuresCol="features",
            labelCol="sales",
            numTrees=100,
            maxDepth=10
        )
        
        # Log parameters and metrics
        mlflow.log_param("sku_id", sku_data.first().sku_id)
        mlflow.log_param("model_type", "RandomForest")
        
        model = rf.fit(assembler.transform(sku_data))
        
        # Log model
        mlflow.spark.log_model(model, "sku-forecast-model")
        
        return model

# Parallel training for multiple SKUs
sku_forecast_models = (sales_by_sku
    .groupBy("sku_id")
    .applyInPandas(train_sku_forecast, schema=model_schema))

# Unity Catalog for Regional Data Access

%python
# Create catalogs for different regions
spark.sql("CREATE CATALOG IF NOT EXISTS us_retail")
spark.sql("CREATE CATALOG IF NOT EXISTS eu_retail")

# Grant access using Unity Catalog
spark.sql("""
GRANT SELECT ON TABLE us_retail.sales.fact_sales TO `us-sales-team`""")

spark.sql("""
GRANT SELECT ON TABLE eu_retail.sales.fact_sales TO `eu-sales-team""")

# Row-level security for regional data
spark.sql("""
CREATE ROW FILTER us_retail.sales.region_filter ON us_retail.sales.fact_sales AS (region IN ('north_america'))""")

# Column-level security for sensitive data
spark.sql("""
CREATE MASK us_retail.sales.mask_customer_email ON us_retail.sales.fact_sales.customer_email
AS (CASE WHEN current_user() = 'admin' THEN customer_email ELSE CONCAT('***', SUBSTRING(customer_email, 4)) END)""")

# Aggregating Clickstream Data

%python
# Real-time clickstream aggregation
clickstream_agg = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka-broker:9092")
    .option("subscribe", "clickstream-events")
    .load()
    .select(
        from_json(col("value").cast("string"), clickstream_schema).alias("data"),
        col("timestamp").alias("processing_time")
    )
    .select("data.*", "processing_time")
    .withWatermark("processing_time", "5 minutes")
    .groupBy(
        window(col("processing_time"), "1 minute", "30 seconds"),
        "page_url", "user_id"
    )
    .agg(
        count("*").alias("page_views"),
        avg("time_on_page").alias("avg_time_on_page"),
        collect_set("click_events").alias("user_actions")
    )
)

# Write to Delta Lake with optimized schema
query = (clickstream_agg
    .writeStream
    .outputMode("update")
    .format("delta")
    .option("checkpointLocation", "s3://checkpoints/clickstream/")
    .option("mergeSchema", "true")
    .table("clickstream_aggregates"))

# Gold Layer for Marketing Campaigns

%python
# Create customer 360 view for marketing
customer_360_gold = spark.sql("""
CREATE TABLE IF NOT EXISTS gold_customer_360
USING DELTA
LOCATION 's3://retail-bucket/gold/customer_360'
AS
SELECT 
    c.customer_id,
    c.demographics,
    c.contact_info,
    SUM(s.amount) AS lifetime_value,
    COUNT(s.sale_id) AS total_orders,
    MAX(s.sale_date) AS last_purchase_date,
    AVG(s.amount) AS avg_order_value,
    COLLECT_SET(s.product_category) AS purchased_categories FROM silver_customers c LEFT JOIN silver_sales s ON c.customer_id = s.customer_id WHERE s.sale_date >= DATE_SUB(CURRENT_DATE(), 365) GROUP BY c.customer_id, c.demographics, c.contact_info""")

# High-value customer segmentation
high_value_customers = spark.sql("""
SELECT * FROM gold_customer_360 WHERE lifetime_value > 1000 AND last_purchase_date >= DATE_SUB(CURRENT_DATE(), 90) AND ARRAY_CONTAINS(purchased_categories, 'premium')""")

# Create optimized table for marketing
high_value_customers.write.format("delta").saveAsTable("gold_marketing_high_value")

# Ingesting Third-Party Data

%python
# Weather data ingestion
weather_data = (spark.read
    .format("json")
    .option("multiline", "true")
    .load("s3://third-party/weather-data/*.json")
    .withColumn("ingestion_time", current_timestamp())
    .withColumn("date", to_date(col("timestamp"))))

# Holiday calendar ingestion
holiday_calendar = (spark.read
    .format("csv")
    .option("header", "true")
    .load("s3://third-party/holidays/*.csv")
    .withColumn("holiday_date", to_date(col("date")))
    .withColumn("ingestion_time", current_timestamp()))

# Join with sales data for enrichment
sales_with_external = (sales_fact
    .join(weather_data, 
          (sales_fact.store_zipcode == weather_data.zipcode) & 
          (sales_fact.sale_date == weather_data.date),
          "left").join(holiday_calendar,
          sales_fact.sale_date == holiday_calendar.holiday_date,
          "left").withColumn("is_holiday", when(col("holiday_date").isNotNull(), True).otherwise(False)))

# Create optimized table
sales_with_external.write.format("delta").saveAsTable("gold_sales_enriched")

# Managing Schema Evolution

%python
# Auto Loader with schema evolution
evolving_schema_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "s3://schema/retail-transactions/")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # Handle new columns
    .option("cloudFiles.allowOverwrites", "false")
    .load("s3://retail-transactions/raw/")
)

# Define schema merge policy
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# Write with schema evolution support
schema_evolution_query = (evolving_schema_stream
    .writeStream
    .format("delta")
    .option("checkpointLocation", "s3://checkpoints/schema-evolution/")
    .option("mergeSchema", "true")  # Auto-merge new columns
    .table("bronze_retail_transactions")
)

# Handle breaking changes with custom logic
def handle_schema_changes(df, batch_id):
    expected_columns = ["transaction_id", "amount", "store_id", "timestamp"]
    
    for col_name in expected_columns:
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None).cast("string"))
    
    # Write to Delta table
    df.write.format("delta").mode("append").saveAsTable("bronze_retail_transactions")

# Data Quality Checks

%python
from pyspark.sql.functions import when, count, col

# Comprehensive data quality framework
def run_data_quality_checks(table_name):
    df = spark.table(table_name)
    
    quality_metrics = df.agg(
        count("*").alias("total_records"),
        count(when(col("sale_amount").isNull() | (col("sale_amount") <= 0), True)).alias("invalid_amounts"),
        count(when(col("customer_id").isNull(), True)).alias("missing_customer_ids"),
        count(when(col("store_id").isNull(), True)).alias("missing_store_ids"),
        count(when(col("sale_date") > current_date(), True)).alias("future_dates"),
        countDistinct("transaction_id").alias("unique_transactions")
    ).withColumn("duplicate_rate", 
                (col("total_records") - col("unique_transactions")) / col("total_records"))
    
    # Alert on quality issues
    quality_issues = quality_metrics.filter(
        (col("invalid_amounts") > 0) |
        (col("missing_customer_ids") > 0) |
        (col("duplicate_rate") > 0.01)
    )
    
    if quality_issues.count() > 0:
        # Send alert and quarantine bad data
        spark.table(table_name).filter(
            col("sale_amount").isNull() |
            col("customer_id").isNull() |
            col("sale_date") > current_date()
        ).write.format("delta").mode("append").saveAsTable("quarantined_transactions")
        
    return quality_metrics

# Apply to BI tables
bi_tables = ["gold_sales", "gold_customers", "gold_inventory"]
for table in bi_tables:
    run_data_quality_checks(table)

# Monitoring Failed Ingestion Pipelines

%python
# Pipeline monitoring framework
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

w = WorkspaceClient()

def monitor_pipeline(pipeline_id):
    # Get pipeline status
    pipeline = w.pipelines.get(pipeline_id)
    
    if pipeline.state == "FAILED":
        # Get latest error details
        updates = w.pipelines.list_updates(pipeline_id, max_results=1)
        latest_update = updates[0] if updates else None
        
        if latest_update and latest_update.error:
            error_details = {
                "pipeline_id": pipeline_id,
                "error_message": latest_update.error.message,
                "error_code": latest_update.error.code,
                "timestamp": latest_update.creation_time
            }
            
            # Send alert
            send_alert(f"Pipeline {pipeline_id} failed: {error_details}")
            
            # Attempt auto-recovery
            if "TIMEOUT" in error_details["error_code"]:
                w.pipelines.reset(pipeline_id)
                send_alert(f"Pipeline {pipeline_id} restarted after timeout")

# Continuous monitoring
pipelines_to_monitor = ["sales-ingestion", "inventory-updates", "customer-cdc"]
for pipeline_id in pipelines_to_monitor:
    monitor_pipeline(pipeline_id)

# Recommendation Engine with Collaborative Filtering

%python
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

# Prepare collaborative filtering data
user_item_ratings = (spark.table("gold_customer_purchases")
    .groupBy("customer_id", "product_id")
    .agg(
        count("*").alias("purchase_count"),
        sum("amount").alias("total_spent")
    )
    .withColumn("implicit_rating", 
                log1p(col("purchase_count")) * log1p(col("total_spent")))
)

# Train ALS model
als = ALS(
    maxIter=10,
    regParam=0.01,
    userCol="customer_id",
    itemCol="product_id",
    ratingCol="implicit_rating",
    coldStartStrategy="drop"
)

model = als.fit(user_item_ratings)

# Generate recommendations
user_recs = model.recommendForAllUsers(10)  # Top 10 recommendations per user

# Save to Delta Lake
user_recs.write.format("delta").saveAsTable("gold_product_recommendations")

# Register model with MLflow
with mlflow.start_run():
    mlflow.spark.log_model(model, "collaborative_filtering")
    mlflow.log_metric("rmse", evaluator.evaluate(predictions))

# Customer 360 View Implementation

%python
# Comprehensive customer 360 view
customer_360 = spark.sql("""
CREATE OR REPLACE TABLE gold_customer_360
USING DELTA
LOCATION 's3://retail-bucket/gold/customer_360'
AS

WITH customer_profile AS (
    SELECT 
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.phone,
        c.signup_date,
        DATEDIFF(CURRENT_DATE(), c.signup_date) AS days_as_customer,
        c.demographics
    FROM silver_customers c
),

purchase_behavior AS (
    SELECT
        customer_id,
        COUNT(DISTINCT transaction_id) AS total_orders,
        SUM(amount) AS lifetime_value,
        AVG(amount) AS avg_order_value,
        MAX(sale_date) AS last_purchase_date,
        DATEDIFF(CURRENT_DATE(), MAX(sale_date)) AS days_since_last_purchase,
        COLLECT_SET(product_category) AS purchased_categories
    FROM silver_sales
    GROUP BY customer_id
),

engagement_metrics AS (
    SELECT
        customer_id,
        COUNT(*) AS total_clicks,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS conversion_events,
        AVG(time_on_site) AS avg_session_duration
    FROM silver_clickstream
    GROUP BY customer_id
)

SELECT 
    p.*,
    pb.total_orders,
    pb.lifetime_value,
    pb.avg_order_value,
    pb.last_purchase_date,
    pb.days_since_last_purchase,
    pb.purchased_categories,
    em.total_clicks,
    em.conversion_events,
    em.avg_session_duration,
    CASE 
        WHEN pb.lifetime_value > 5000 THEN 'Platinum'
        WHEN pb.lifetime_value > 1000 THEN 'Gold'
        WHEN pb.lifetime_value > 500 THEN 'Silver'
        ELSE 'Bronze'
    END AS customer_tier
FROM customer_profile p
LEFT JOIN purchase_behavior pb ON p.customer_id = pb.customer_id
LEFT JOIN engagement_metrics em ON p.customer_id = em.customer_id""")

# Create optimized Z-Ordered table
spark.sql("OPTIMIZE gold_customer_360 ZORDER BY (customer_tier, lifetime_value)")

