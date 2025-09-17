# Customer Purchase Analysis

%python
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, desc

spark = SparkSession.builder.appName("CustomerSpendAnalysis").getOrCreate()

# Calculate total spend per customer
customer_spend = transactions_df.groupBy("customer_id") \
    .agg(sum("price").alias("total_spend"))

# Get top 10 highest-spending customers
top_customers = customer_spend.orderBy(desc("total_spend")).limit(10)

# Daily Sales Trends

%python
from pyspark.sql.functions import sum, date_format, to_date
import matplotlib.pyplot as plt

# Calculate daily sales revenue
daily_sales = orders_df.withColumn("date", to_date("timestamp")) \
    .groupBy("date") \
    .agg(sum("price").alias("daily_revenue")) \
    .orderBy("date")

# Collect last 30 days data and plot
last_30_days = daily_sales.filter(
    col("date") >= date_sub(current_date(), 30)).toPandas()

plt.figure(figsize=(12, 6))
plt.plot(last_30_days['date'], last_30_days['daily_revenue'])
plt.title('Daily Sales Revenue - Last 30 Days')
plt.xlabel('Date')
plt.ylabel('Revenue')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Product Recommendation (Co-Purchase)

%python
from pyspark.sql.functions import collect_list
from pyspark.ml.fpm import FPGrowth

# Get products per order
order_products = orders_df.groupBy("order_id") \
    .agg(collect_list("product_id").alias("items"))

# Use FP-Growth algorithm for frequent itemset mining
fp_growth = FPGrowth(itemsCol="items", minSupport=0.01, minConfidence=0.5)
model = fp_growth.fit(order_products)

# Get frequent item pairs (association rules)
frequent_items = model.associationRules
frequent_items.show(truncate=False)

# Sessionization of User Clickstream

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, unix_timestamp, when, sum as sql_sum
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Define session gap (30 minutes in seconds)
session_gap = 30 * 60

window_spec = Window.partitionBy("user_id").orderBy("timestamp")

# Calculate time difference between consecutive clicks
sessionized = clickstream_df.withColumn(
    "prev_timestamp", lag("timestamp").over(window_spec)).withColumn(
    "time_diff", 
    unix_timestamp("timestamp") - unix_timestamp("prev_timestamp")).withColumn(
    "new_session", 
    when((col("time_diff") > session_gap) | (col("prev_timestamp").isNull()), 1).otherwise(0)).withColumn(
    "session_id", 
    sql_sum("new_session").over(window_spec.rowsBetween(Window.unboundedPreceding, 0)))


# Handling Late Orders in Streaming

%python
from pyspark.sql.functions import window, current_timestamp

# Define watermark and window for handling late data
streaming_orders = spark.readStream \
    .schema(order_schema) \
    .option("maxFilesPerTrigger", 1) \
    .json("s3://orders-stream/")

# Use watermark to handle late-arriving data (up to 1 hour late)
processed_stream = streaming_orders \
    .withWatermark("event-time", "1 hour")\
    .groupBy(window("event_time", "5 minutes"),"product_id").agg(sum("price").alias("total_sales"))

query = processed_stream.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

# Optimizing Joins (Products + Orders)

%python
# Broadcast the small product lookup table to avoid shuffle
from pyspark.sql.functions import broadcast

# Assuming products_df is small (<100MB)
optimized_join = orders_df.join(
    broadcast(products_df), 
    orders_df.product_id == products_df.product_id,
    "inner")

# Alternatively, if products_df is slightly larger but still small compared to orders
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")

# Or use bucketing if products_df is medium-sized
products_df.write.bucketBy(16, "product_id").saveAsTable("products_bucketed")
orders_df.write.bucketBy(16, "product_id").saveAsTable("orders_bucketed")

bucketed_join = spark.table("orders_bucketed").join(
    spark.table("products_bucketed"),
    "product_id")

# Detecting Fraudulent Transactions

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import count, unix_timestamp

# Define window for fraud detection (5 minutes)
fraud_window = Window.partitionBy("user_id") \
    .orderBy(unix_timestamp("timestamp")) \
    .rangeBetween(-300, 0)  # 5 minutes in seconds

# Count orders within 5-minute window
fraud_detection = transactions_df.withColumn("order_count", count("order_id").over(fraud_window)).withColumn("is_fraudulent", when(col("order_count") >= 20, True).otherwise(False))

fraudulent_orders = fraud_detection.filter(col("is_fraudulent") == True)

# Abandoned Cart Detection

%python
from pyspark.sql.functions import max, when, col

# Get user events with cart additions and purchases
user_events = events_df.groupBy("user_id").agg(
    max(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("added_cart"),
    max(when(col("event_type") == "purchase", 1).otherwise(0)).alias("made_purchase"))

# Identify abandoned carts (added to cart but no purchase)
abandoned_carts = user_events.filter(
    (col("added_cart") == 1) & (col("made_purchase") == 0))

# File Format Optimization

%python
# Convert from CSV to Parquet
df = spark.read.csv("s3://raw-orders/", header=True)
df.write.partitionBy("product_category", "region").parquet("s3://optimized-orders/")

# Data Skew in Popular Products

%python
# Solution 1: Salting technique
from pyspark.sql.functions import concat, lit, rand

# Add salt to the skewed key
salted_orders = orders_df.withColumn(
    "salted_product_id", 
    concat(col("product_id"), lit("_"), (rand() * 10).cast("int")))

salted_products = products_df.withColumn(
    "join_key", 
    explode(array([concat(lit(product_id), lit("_"), lit(i)) for i in range(10)])))

# Join on salted keys
result = salted_orders.join(
    salted_products,
    salted_orders.salted_product_id == salted_products.join_key)

# Solution 2: Broadcast if one side is small
result = orders_df.join(broadcast(products_df), "product_id")

# Solution 3: Adaptive Query Execution (AQE) in Spark 3.0+
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", "true")