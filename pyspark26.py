1. How do you identify slow-moving products in a retail inventory?
 → Aggregate total units sold per product and filter products below a sales threshold over a given period.

%python
from pyspark.sql.functions import sum, col, datediff, current_date

# Identify slow-moving products (sold less than 10 units in last 90 days)
slow_moving_products = (sales_df
    .filter(col("sale_date") >= date_sub(current_date(), 90))
    .groupBy("product_id")
    .agg(sum("quantity").alias("total_units_sold"))
    .filter(col("total_units_sold") < 10)
    .join(products_df, "product_id")
    .select("product_id", "product_name", "total_units_sold"))

# Alternative: Calculate days of inventory coverage
inventory_coverage = (inventory_df
    .join(sales_df.groupBy("product_id").agg(sum("quantity").alias("daily_sales_avg") / 90), 
          "product_id")
    .withColumn("days_coverage", col("current_stock") / col("daily_sales_avg"))
    .filter(col("days_coverage") > 60)  # More than 60 days of inventory)

2. How would you handle missing category data in a product catalog?
 → Use fillna() for default values or join with a reference/master dataset to fill in missing categories.

%python
from pyspark.sql.functions import coalesce, lit

# Method 1: Fill with default values
product_catalog_filled = product_catalog.fillna(
    {"category": "UNCATEGORIZED", "subcategory": "UNKNOWN"}
)

# Method 2: Join with master category reference
master_categories = spark.table("ref_product_categories")

product_catalog_complete = (product_catalog
    .join(master_categories, 
          product_catalog.product_id == master_categories.product_id, 
          "left")
    .withColumn("category", 
                coalesce(product_catalog["category"], 
                         master_categories["default_category"],
                         lit("MISCELLANEOUS")))
    .drop(master_categories["product_id"]))

# Method 3: Use ML to predict missing categories (if historical data exists)

3. A product is priced incorrectly in multiple stores. How do you update only those records in PySpark?
 → Use MERGE INTO with Delta Lake or filter and apply a withColumn + when() logic for update.

%python
from delta.tables import DeltaTable
from pyspark.sql.functions import when

# Method 1: Using Delta Lake MERGE (Recommended)
delta_table = DeltaTable.forPath(spark, "/delta/inventory")

# Create DataFrame with correct prices
correct_prices_df = spark.createDataFrame([
    ("store_1", "prod_123", 29.99),
    ("store_2", "prod_123", 29.99),
    ("store_3", "prod_123", 29.99)
], ["store_id", "product_id", "correct_price"])

delta_table.alias("inventory").merge(
    correct_prices_df.alias("updates"),
    "inventory.store_id = updates.store_id AND inventory.product_id = updates.product_id"
).whenMatchedUpdate(set = {
    "price": "updates.correct_price",
    "updated_timestamp": current_timestamp()}).execute()

# Method 2: Using filter and withColumn (if not using Delta)
updated_inventory = inventory_df.withColumn(
    "price",
    when((col("store_id").isin(["store_1", "store_2", "store_3"])) & 
         (col("product_id") == "prod_123"), 29.99)
    .otherwise(col("price")))

4. What’s your approach to remove duplicate transactions from a retail sales dataset?
 → Use dropDuplicates() based on columns like transaction_id, timestamp, and customer_id.

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Method 1: Using dropDuplicates (simple cases)
deduplicated_sales = sales_df.dropDuplicates(["transaction_id", "sale_timestamp"])

# Method 2: Using window functions (keep most recent)
window_spec = Window.partitionBy("transaction_id").orderBy(col("sale_timestamp").desc())

deduplicated_sales = (sales_df
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num"))

# Method 3: Complex deduplication with multiple criteria
deduplicated_complex = sales_df.dropDuplicates([
    "transaction_id", 
    "customer_id", 
    "product_id", 
    "sale_date"])

5. How do you detect and handle data skew during a join on store_id?
 → Use salting technique or broadcast() join if one table (e.g., store dimension) is small.

%python
from pyspark.sql.functions import concat, lit, rand

# Method 1: Salting technique for skewed keys
skewed_stores = ["store_123"]  # Identify skewed store IDs

# Add salt to the large table
salted_sales = sales_df.withColumn(
    "salted_store_id",
    when(col("store_id").isin(skewed_stores),
         concat(col("store_id"), lit("_"), (rand() * 10).cast("int")))
    .otherwise(col("store_id"))
)

# Create salted dimension table
salted_stores = store_df.filter(col("store_id").isin(skewed_stores)) \
    .withColumn("join_key", explode(array([lit(f"{store_id}_{i}") for i in range(10)]))) \
    .union(store_df.filter(~col("store_id").isin(skewed_stores)))

# Perform join
result = salted_sales.join(salted_stores, 
                          salted_sales.salted_store_id == salted_stores.join_key,
                          "left")

# Method 2: Broadcast join if store dimension is small
result = sales_df.join(broadcast(store_df), "store_id")

6. You receive daily CSV sales files. How do you automate and optimize ingestion to a Delta table?
 → Use Auto Loader or a scheduled PySpark job with schema inference, then write in append mode to Delta.

%python
from pyspark.sql.functions import input_file_name, current_timestamp

# Using Auto Loader for efficient CSV ingestion
sales_bronze = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("cloudFiles.schemaLocation", "/schema/sales_csv/")
    .load("s3://retail-bucket/raw-sales/csv/")
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", input_file_name()))

# Write to Delta Lake with optimized settings
query = (sales_bronze.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/checkpoints/sales_ingestion/")
    .option("mergeSchema", "true")
    .table("bronze_sales"))

# Schedule with Databricks Workflows or Airflow

7. How do you implement SCD Type 2 to track product price changes over time?
 → Use MERGE INTO with Delta Lake along with effective/expiry dates and flags for current version.

%python
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit

# Initialize Delta table
delta_table = DeltaTable.forPath(spark, "/delta/products_scd2")

# New price data
new_prices_df = spark.read.table("new_product_prices")

# SCD Type 2 implementation
delta_table.alias("target").merge(
    new_prices_df.alias("source"),
    "target.product_id = source.product_id AND target.is_current = true"
).whenMatchedUpdate(
    condition = "target.price != source.price",
    set = {
        "is_current": "false",
        "end_date": "current_date()"
    }
).whenNotMatchedInsert(
    values = {
        "product_id": "source.product_id",
        "price": "source.price",
        "start_date": "current_date()",
        "end_date": "null",
        "is_current": "true"
    }
).execute()

# Insert new versions for changed prices
changed_products = new_prices_df.join(
    delta_table.toDF().filter("is_current = true"),
    ["product_id"],
    "inner").filter("source.price != target.price")

delta_table.alias("target").insert(
    changed_products.select(
        "product_id",
        "price",
        current_timestamp().alias("start_date"),
        lit(None).cast("timestamp").alias("end_date"),
        lit(True).alias("is_current")))

8. You need to find top 5 products sold per store per day. How will you do this in PySpark?
 → Use window functions with partitionBy(store_id, date) and orderBy(sales DESC) + rank().

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col, sum

window_spec = Window.partitionBy("store_id", "sale_date").orderBy(col("total_sales").desc())

top_products = (sales_df
    .groupBy("store_id", "sale_date", "product_id")
    .agg(sum("quantity").alias("total_sales"))
    .withColumn("rank", rank().over(window_spec))
    .filter(col("rank") <= 5)
    .join(products_df, "product_id")
    .select("store_id", "sale_date", "product_name", "total_sales", "rank"))

# Alternative using dense_rank() if ties should have same rank

9. What’s your approach to calculate daily stockout rate for each product?
 → Join inventory and sales data, count days when stock = 0, divide by total number of days.

%python
from pyspark.sql.functions import avg, when, count, col

# Calculate stockout days for each product
stockout_analysis = (inventory_df
    .filter(col("date") >= date_sub(current_date(), 365))  # Last year
    .groupBy("product_id")
    .agg(
        count("*").alias("total_days"),
        count(when(col("stock_quantity") == 0, True)).alias("stockout_days")
    )
    .withColumn("stockout_rate", col("stockout_days") / col("total_days"))
    .join(products_df, "product_id")
    .select("product_id", "product_name", "stockout_days", "total_days", "stockout_rate")
)

# Daily stockout rate across all products
daily_stockout_rate = (inventory_df
    .groupBy("date")
    .agg(
        count("*").alias("total_products_tracked"),
        count(when(col("stock_quantity") == 0, True)).alias("out_of_stock_products"))
    .withColumn("daily_stockout_rate", col("out_of_stock_products") / col("total_products_tracked")))

10. You want to detect loyal customers. What data points and logic would you use?
 → Use frequency (number of visits), recency (last visit), and monetary value (total spend) – RFM model.

%python
from pyspark.sql.functions import datediff, current_date, sum, count, max

# RFM Analysis
rfm_customers = (sales_df
    .groupBy("customer_id")
    .agg(
        datediff(current_date(), max("sale_date")).alias("recency"),
        count("*").alias("frequency"),
        sum("amount").alias("monetary")
    )
    .withColumn("recency_score", 
                when(col("recency") <= 30, 5)
                .when(col("recency") <= 60, 4)
                .when(col("recency") <= 90, 3)
                .when(col("recency") <= 180, 2)
                .otherwise(1))
    .withColumn("frequency_score",
                when(col("frequency") >= 50, 5)
                .when(col("frequency") >= 20, 4)
                .when(col("frequency") >= 10, 3)
                .when(col("frequency") >= 5, 2)
                .otherwise(1))
    .withColumn("monetary_score",
                when(col("monetary") >= 5000, 5)
                .when(col("monetary") >= 2000, 4)
                .when(col("monetary") >= 1000, 3)
                .when(col("monetary") >= 500, 2)
                .otherwise(1))
    .withColumn("rfm_score", 
                col("recency_score") + col("frequency_score") + col("monetary_score"))
    .filter(col("rfm_score") >= 12)  # Loyal customers threshold)

# Segment loyal customers
loyal_customers = rfm_customers.filter(col("rfm_score") >= 12)

11. Explain a situation where you used structured streaming in retail.
 → Real-time order tracking or detecting fraud transactions using Kafka source and Spark Structured Streaming.

%python
from pyspark.sql.streaming import StreamingQuery

# Real-time order processing pipeline
order_stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka-broker:9092")
    .option("subscribe", "real-time-orders")
    .option("startingOffsets", "latest")
    .load()
    .select(
        from_json(col("value").cast("string"), order_schema).alias("data"),
        col("timestamp").alias("processing_time")).select("data.*", "processing_time"))

# Fraud detection logic
fraud_detection = (order_stream
    .withWatermark("processing_time", "5 minutes")
    .groupBy(
        window(col("processing_time"), "10 minutes", "5 minutes"),
        "customer_id").agg(count("*").alias("order_count"), sum("amount").alias("total_amount")).filter((col("order_count") > 10) | (col("total_amount") > 1000)))

# Write alerts
query = fraud_detection.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("checkpointLocation", "/checkpoints/fraud-detection/") \
    .start()

12. How do you optimize a slow-running product sales aggregation job?
 → Persist intermediate DataFrames, coalesce/shuffle partitions, avoid wide transformations, and use cache() or broadcast().

%python
# Optimization strategies for slow aggregation jobs

# 1. Cache intermediate DataFrames
filtered_sales = sales_df.filter(col("sale_date") >= "2024-01-01").cache()

# 2. Coalesce partitions before aggregation
coalesced_sales = filtered_sales.coalesce(50)

# 3. Use map-side aggregation
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# 4. Optimize Delta table
spark.sql("OPTIMIZE sales_table ZORDER BY (store_id, product_id)")

# 5. Broadcast small dimension tables
enriched_sales = coalesced_sales.join(broadcast(products_df), "product_id")

# 6. Use efficient aggregation functions
sales_agg = (enriched_sales
    .groupBy("store_id", "product_category")
    .agg(
        sum("quantity").alias("total_quantity"),
        sum("amount").alias("total_sales"),
        approx_count_distinct("customer_id").alias("unique_customers")  # Faster than exact count))

# 7. Adjust shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", 100)

# 8. Use efficient file format
sales_agg.write.format("parquet").save("optimized_output/")