%python

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize Spark Session
spark = SparkSession.builder.appName("Day40").getOrCreate()

# Product Schema and Data
product_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", IntegerType(), True),
    StructField("discount", IntegerType(), True)
])

product_data = [
    ("P001", "Laptop A", "Electronics", 85000, 10),
    ("P002", "Smartphone B", "Electronics", 40000, 5),
    ("P003", "Headphones C", "Accessories", 3000, 15),
    ("P004", "Blender D", "Home Appliance", 4500, 8),
    ("P005", "Watch E", "Fashion", 7000, 12)
]

# Review Schema and Data
review_schema = StructType([
    StructField("review_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("rating", FloatType(), True),
    StructField("review_text", StringType(), True),
    StructField("review_date", StringType(), True)
])

review_data = [
    ("R001", "P001", "C101", 5.0, "Excellent performance and battery life.", "2025-01-15"),
    ("R002", "P002", "C102", 4.0, "Good phone but a bit overpriced.", "2025-01-18"),
    ("R003", "P001", "C103", 3.0, "Average laptop for the price.", "2025-02-01"),
    ("R004", "P003", "C104", 4.0, "Great sound quality.", "2025-02-10"),
    ("R005", "P004", "C105", 2.0, "Not durable. Stopped working in a month.", "2025-02-15"),
    ("R006", "P005", "C106", 5.0, "Stylish and works well.", "2025-02-20"),
    ("R007", "P002", "C107", 1.0, "Poor camera quality.", "2025-03-05"),
    ("R008", "P003", "C108", 4.0, "Value for money.", "2025-03-10"),
    ("R009", "P004", "C109", 3.0, "Decent for basic use.", "2025-03-15"),
    ("R010", "P001", "C110", 5.0, "Superb laptop. Worth every penny.", "2025-03-25")
]

# Create DataFrames
df_products = spark.createDataFrame(product_data, product_schema)
df_reviews = spark.createDataFrame(review_data, review_schema)

# Q1: Load and show schema
print("Product Schema:")
df_products.printSchema()
print("Review Schema:")
df_reviews.printSchema()

print("Products Data:")
df_products.show()
print("Reviews Data:")
df_reviews.show()

# Q2: Average rating for each product
avg_ratings = df_reviews.groupBy("product_id").agg(
    avg("rating").alias("average_rating")
)

# Q3: Join DataFrames
df_joined = df_products.join(avg_ratings, "product_id").select(
    "product_name", "category", "price", "discount", "average_rating"
)

# Q4: Top 3 highest-rated products
top_rated = df_joined.orderBy(col("average_rating").desc()).limit(3)

# Q5: Effective price after discount
df_effective_price = df_joined.withColumn(
    "effective_price", 
    col("price") - (col("price") * col("discount") / 100)
)

# Q6: Products with average rating < 4
low_rated_products = df_joined.filter(col("average_rating") < 4).select("product_name", "category")

# Q7: Rating category
df_rating_category = df_joined.withColumn("rating_category",
    when(col("average_rating") >= 4.5, "Excellent")
    .when((col("average_rating") >= 3.5) & (col("average_rating") < 4.5), "Good")
    .when((col("average_rating") >= 2.5) & (col("average_rating") < 3.5), "Average")
    .otherwise("Poor")
)

# Q8: Category-wise insights
category_insights = df_rating_category.join(df_reviews, "product_id").groupBy("category").agg(
    count("review_id").alias("total_reviews"),
    avg("rating").alias("avg_rating"),
    avg("effective_price").alias("avg_effective_price")
)

# Q9: Filter reviews with "poor" or "average"
negative_reviews = df_reviews.filter(
    lower(col("review_text")).contains("poor") | 
    lower(col("review_text")).contains("average")
)

# Q10: Most reviewed product
most_reviewed = df_reviews.groupBy("product_id").agg(
    count("review_id").alias("review_count")
).orderBy(col("review_count").desc()).first()

# Display results
print("Q2 - Average Ratings:")
avg_ratings.show()

print("Q3 - Joined Data:")
df_joined.show()

print("Q4 - Top 3 Rated Products:")
top_rated.show()

print("Q5 - Effective Price:")
df_effective_price.show()

print("Q6 - Low Rated Products:")
low_rated_products.show()

print("Q7 - Rating Category:")
df_rating_category.show()

print("Q8 - Category Insights:")
category_insights.show()

print("Q9 - Negative Reviews:")
negative_reviews.show()

print(f"Q10 - Most Reviewed Product: Product ID {most_reviewed['product_id']} with {most_reviewed['review_count']} reviews")

----------------------------------------------------------------------------------------------------------

%python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Data and Schema
data_day39 = [
    (101, "2024-10-01", "Laptop", 1, 65000, "Delhi", None),
    (102, "2024-10-02", "Mobile", 2, 25000, "Mumbai", 1000),
    (103, "2024-10-03", "Laptop", 1, None, "Delhi", None),
    (104, "2024-10-03", "Tablet", 3, 15000, "Chennai", 500),
    (105, "2024-10-04", "Laptop", 1, 65000, "Delhi", None),
    (106, "2024-10-05", "Mobile", 2, 25000, "Mumbai", 1000),
    (107, "2024-10-06", "Earphones", 5, 1200, "Pune", 0),
    (108, "2024-10-07", "Laptop", 1, 65000, "Delhi", None),
    (108, "2024-10-07", "Laptop", 1, 65000, "Delhi", None)  # Duplicate
]

columns_day39 = ["OrderID", "OrderDate", "Product", "Quantity", "UnitPrice", "City", "Discount"]

df_day39 = spark.createDataFrame(data_day39, columns_day39)

# Question 1: Remove Duplicates
df_deduped = df_day39.dropDuplicates()

# Question 2: Handle Missing Values
# Calculate average price per product
avg_prices = df_deduped.filter(col("UnitPrice").isNotNull()).groupBy("Product").agg(
    avg("UnitPrice").alias("avg_price")
)

# Replace missing values
df_filled = df_deduped.join(avg_prices, "Product", "left") \
    .withColumn("UnitPrice", 
               when(col("UnitPrice").isNull(), col("avg_price"))
               .otherwise(col("UnitPrice"))) \
    .fillna(0, subset=["Discount"]) \
    .drop("avg_price")

# Question 3: Compute Derived Columns
df_final = df_filled.withColumn(
    "TotalAmount", 
    (col("Quantity") * col("UnitPrice")) - col("Discount")
)

# Question 4: City-Wise Summary
city_summary = df_final.groupBy("City").agg(
    sum("TotalAmount").alias("TotalSales"),
    avg("Discount").alias("AvgDiscount")
)

# Question 5: Product-Wise Insights
product_insights = df_final.groupBy("Product").agg(
    sum("Quantity").alias("TotalQuantitySold"),
    sum("TotalAmount").alias("TotalRevenue"),
    avg("Discount").alias("AvgDiscount")
).orderBy(col("TotalRevenue").desc())

# Display results
print("Question 1 - Deduplicated Data:")
df_deduped.show()

print("Question 2 - Filled Missing Values:")
df_filled.show()

print("Question 3 - Derived Columns:")
df_final.show()

print("Question 4 - City Summary:")
city_summary.show()

print("Question 5 - Product Insights:")
product_insights.show()

------------------------------------------------------------------------------------------------------------

%python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Schema and Data
schema_day38 = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("products", ArrayType(
        StructType([
            StructField("product_id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("qty", IntegerType(), True)
        ])
    ), True)
])

data_day38 = [
    (1, "2025-10-10", "Online", [
        {"product_id": 101, "name": "Laptop", "price": 60000, "qty": 1},
        {"product_id": 102, "name": "Mouse", "price": 700, "qty": 2}
    ]),
    (2, "2025-10-10", "Offline", [
        {"product_id": 103, "name": "Keyboard", "price": 1500, "qty": 1}
    ]),
    (3, "2025-10-11", "Online", [
        {"product_id": 104, "name": "Monitor", "price": None, "qty": 1},
        {"product_id": 105, "name": None, "price": 4500, "qty": 2}
    ]),
    (4, "2025-10-11", "Offline", None),
    (5, "2025-10-12", "Online", [
        {"product_id": 106, "name": "USB Cable", "price": 300, "qty": 5}
    ])
]

df_day38 = spark.createDataFrame(data_day38, schema_day38)

# Question 1: Explode products array
df_exploded = df_day38.withColumn("product", explode(col("products"))) \
    .select(
        "order_id", "order_date", "channel",
        col("product.product_id").alias("product_id"),
        col("product.name").alias("product_name"),
        col("product.price").alias("price"),
        col("product.qty").alias("quantity")
    )

# Question 2: Calculate total_amount
df_with_total = df_exploded.withColumn("total_amount", col("price") * col("quantity"))

# Question 3: Handle missing values
avg_price = df_with_total.agg(avg("price")).collect()[0][0]
df_cleaned = df_with_total \
    .fillna(avg_price, subset=["price"]) \
    .fillna("Unknown Product", subset=["product_name"])

# Question 4: Total sales per channel
channel_sales = df_cleaned.groupBy("channel").agg(
    sum("total_amount").alias("total_sales")
)

# Question 5: Most sold product
most_sold_product = df_cleaned.groupBy("product_id", "product_name").agg(
    sum("quantity").alias("total_quantity")
).orderBy(col("total_quantity").desc()).first()

# Question 6: Add category column
df_categorized = df_cleaned.withColumn("category",
    when(lower(col("product_name")).contains("laptop") | 
         lower(col("product_name")).contains("monitor"), "Electronics")
    .when(lower(col("product_name")).contains("mouse") | 
          lower(col("product_name")).contains("keyboard") | 
          lower(col("product_name")).contains("usb"), "Accessories")
    .otherwise("Others")
)

# Display results
print("Question 1 - Exploded Data:")
df_exploded.show()

print("Question 2 - With Total Amount:")
df_with_total.show()

print("Question 3 - Cleaned Data:")
df_cleaned.show()

print("Question 4 - Channel Sales:")
channel_sales.show()

print(f"Question 5 - Most Sold Product: {most_sold_product['product_name']} with {most_sold_product['total_quantity']} units")

print("Question 6 - Categorized Data:")
df_categorized.show()

--------------------------------------------------------------------------------------------------------------
%python

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Data and Columns
data_day37 = [
    ("U001", "M101", "2024-05-20", 120, 4.5, "USA", "Premium"),
    ("U002", "M103", "2024-06-11", 45, 3.0, "India", "Free"),
    ("U003", "M101", "2024-06-15", None, 5.0, "India", "Premium"),
    ("U004", "M104", "2024-06-10", 85, None, "Canada", "Premium"),
    ("U002", "M105", "2024-06-20", 60, 4.0, "India", "Free"),
    ("U003", "M103", "2024-06-18", 50, 2.5, "India", "Free"),
    ("U001", "M101", "2024-05-20", 120, 4.5, "USA", "Premium"),  # duplicate
    ("U005", "M102", "2024-07-01", 95, 4.2, "UK", "Premium"),
    ("U006", "M106", "2024-07-05", 100, 5.0, "India", "Premium"),
    ("U007", "M107", "2024-06-25", 75, 3.8, "USA", "Free"),
]

cols_day37 = ["user_id", "movie_id", "watch_date", "watch_time_min", "rating", "country", "subscription_type"]

df_day37 = spark.createDataFrame(data_day37, cols_day37)

# Question 1: Data Cleaning
# Remove duplicates
df_clean = df_day37.dropDuplicates()

# Calculate averages for imputation
country_avg_watch = df_clean.filter(col("watch_time_min").isNotNull()).groupBy("country").agg(
    avg("watch_time_min").alias("avg_watch_time_country")
)

subscription_avg_rating = df_clean.filter(col("rating").isNotNull()).groupBy("subscription_type").agg(
    avg("rating").alias("avg_rating_subscription")
)

# Replace missing values
df_filled = df_clean \
    .join(country_avg_watch, "country", "left") \
    .join(subscription_avg_rating, "subscription_type", "left") \
    .withColumn("watch_time_min", 
               when(col("watch_time_min").isNull(), col("avg_watch_time_country"))
               .otherwise(col("watch_time_min"))) \
    .withColumn("rating", 
               when(col("rating").isNull(), col("avg_rating_subscription"))
               .otherwise(col("rating"))) \
    .drop("avg_watch_time_country", "avg_rating_subscription")

# Question 2: Analysis Queries
# Total watch time and average rating by country
country_stats = df_filled.groupBy("country").agg(
    sum("watch_time_min").alias("total_watch_time"),
    avg("rating").alias("avg_rating")
)

# Top 3 most-watched movies
top_movies = df_filled.groupBy("movie_id").agg(
    sum("watch_time_min").alias("total_watch_time")
).orderBy(col("total_watch_time").desc()).limit(3)

# User statistics
user_stats = df_filled.groupBy("user_id").agg(
    count("movie_id").alias("total_movies_watched"),
    avg("rating").alias("avg_rating")
)

# Rank users within each country by total watch time
window_country = Window.partitionBy("country").orderBy(col("total_watch_time").desc())
user_country_rank = df_filled.groupBy("user_id", "country").agg(
    sum("watch_time_min").alias("total_watch_time")
).withColumn("rank", rank().over(window_country))

# Subscription type with highest average rating
best_subscription = df_filled.groupBy("subscription_type").agg(
    avg("rating").alias("avg_rating")
).orderBy(col("avg_rating").desc()).first()

# Question 3: Engagement Score
df_engagement = df_filled.withColumn(
    "engagement_score", 
    (col("watch_time_min") * col("rating")) / 10
)

top_engaged_users = df_engagement.groupBy("user_id").agg(
    sum("engagement_score").alias("total_engagement_score")
).orderBy(col("total_engagement_score").desc()).limit(5)

# Display results
print("Question 1 - Cleaned Data:")
df_filled.show()

print("Question 2 - Country Stats:")
country_stats.show()

print("Question 2 - Top 3 Movies:")
top_movies.show()

print("Question 2 - User Stats:")
user_stats.show()

print("Question 2 - User Country Rank:")
user_country_rank.show()

print(f"Question 2 - Best Subscription: {best_subscription['subscription_type']} with rating: {best_subscription['avg_rating']}")

print("Question 3 - Top 5 Engaged Users:")
top_engaged_users.show()

----------------------------------------------------------------------------------------------------------------

%python

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Data and Columns
data_day36 = [
    ("O101", "C001", "2025-09-20", "Electronics", 1, 15000, 10, "N", "Credit Card"),
    ("O102", "C002", "2025-09-20", "Clothing", 2, 1200, 5, "N", "Cash"),
    ("O103", "C003", "2025-09-21", "Groceries", 10, 50, None, "N", "UPI"),
    ("O104", "C002", "2025-09-22", "Electronics", 1, 18000, -5, "N", "Credit Card"),
    ("O105", "C004", "2025-09-22", "Clothing", 3, 800, 15, "Y", "UPI"),
    ("O106", "C001", "2025-09-23", "Clothing", 1, 1000, 5, "N", "Credit Card"),
    ("O107", "C005", "2025-09-23", "Groceries", 6, 70, 10, "N", "UPI"),
    ("O105", "C004", "2025-09-22", "Clothing", 3, 800, 15, "Y", "UPI"),  # Duplicate
    ("O108", None, "2025-09-24", "Electronics", 2, 20000, 20, "N", "Cash"),
    ("O109", "C006", "2025-09-24", "Groceries", 5, 60, 200, "N", "UPI"),
]

cols_day36 = ["order_id", "customer_id", "order_date", "product_category", "quantity", "unit_price", "discount", "is_returned", "payment_mode"]

df_day36 = spark.createDataFrame(data_day36, cols_day36)

# Question 1: Data Cleaning
# Remove duplicates
df_clean = df_day36.dropDuplicates()

# Handle null customer_id
df_clean = df_clean.fillna("Unknown", subset=["customer_id"])

# Fix invalid discount values
avg_discount = df_clean.filter(
    (col("discount").isNotNull()) & 
    (col("discount") >= 0) & 
    (col("discount") <= 100)
).agg(avg("discount")).collect()[0][0]

df_clean = df_clean.withColumn("discount",
    when(col("discount").isNull(), avg_discount)
    .when((col("discount") < 0) | (col("discount") > 100), 0)
    .otherwise(col("discount"))
)

# Question 2: Data Transformation
df_transformed = df_clean \
    .withColumn("final_amount", 
               col("quantity") * col("unit_price") * (1 - col("discount") / 100)) \
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")) \
    .filter(col("is_returned") == "N")

# Other Analysis Questions
# Total revenue per product category
category_revenue = df_transformed.groupBy("product_category").agg(
    sum("final_amount").alias("total_revenue")
)

# Top 2 product categories by revenue
top_categories = category_revenue.orderBy(col("total_revenue").desc()).limit(2)

# Average order value per customer
avg_order_value = df_transformed.groupBy("customer_id").agg(
    avg("final_amount").alias("avg_order_value")
)

# Most used payment mode
payment_mode_usage = df_transformed.groupBy("payment_mode").agg(
    count("order_id").alias("order_count")
).orderBy(col("order_count").desc()).first()

# Category with highest average discount
category_avg_discount = df_transformed.groupBy("product_category").agg(
    avg("discount").alias("avg_discount")
).orderBy(col("avg_discount").desc()).first()

# Display results
print("Question 1 - Cleaned Data:")
df_clean.show()

print("Question 2 - Transformed Data:")
df_transformed.show()

print("Total Revenue per Category:")
category_revenue.show()

print("Top 2 Categories:")
top_categories.show()

print("Average Order Value per Customer:")
avg_order_value.show()

print(f"Most Used Payment Mode: {payment_mode_usage['payment_mode']} with {payment_mode_usage['order_count']} orders")

print(f"Category with Highest Avg Discount: {category_avg_discount['product_category']} with {category_avg_discount['avg_discount']}%")

---------------------------------------------------------------------------------------------------------------------------------

%python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Sample nested JSON data (in practice, you would load from a file)
json_data = [
    {
        "order_id": 1001,
        "customer": {"id": "C001", "name": "Ramesh Kumar", "city": "Delhi"},
        "items": [
            {"item_id": "I001", "product": "Laptop", "price": 55000, "quantity": 1},
            {"item_id": "I002", "product": "Mouse", "price": 800, "quantity": 2}
        ],
        "payment": {"mode": "Credit Card", "status": "Success"},
        "order_date": "2023-06-10"
    },
    {
        "order_id": 1002,
        "customer": {"id": "C002", "name": "Priya Singh", "city": "Mumbai"},
        "items": [
            {"item_id": "I003", "product": "Keyboard", "price": 1200, "quantity": 1},
            {"item_id": "I004", "product": "Monitor", "price": 10000, "quantity": 1}
        ],
        "payment": {"mode": "UPI", "status": "Success"},
        "order_date": "2023-06-12"
    },
    {
        "order_id": 1003,
        "customer": {"id": "C003", "name": "Arjun Patel", "city": "Pune"},
        "items": [
            {"item_id": "I005", "product": "Tablet", "price": 15000, "quantity": 1}
        ],
        "payment": {"mode": "Credit Card", "status": "Failed"},
        "order_date": "2023-06-15"
    }
]

# In practice, load from JSON file:
# df = spark.read.json("path/to/nested_data.json")

# For this example, create DataFrame from RDD
import json
rdd = spark.sparkContext.parallelize([json.dumps(record) for record in json_data])
df_nested = spark.read.json(rdd)

print("Original Nested Schema:")
df_nested.printSchema()

# Question 1 & 2: Flatten and explode
df_flattened = df_nested \
    .select(
        "order_id",
        col("customer.id").alias("customer_id"),
        col("customer.name").alias("customer_name"),
        col("customer.city").alias("city"),
        explode("items").alias("item"),
        col("payment.mode").alias("payment_mode"),
        col("payment.status").alias("payment_status"),
        "order_date"
    ) \
    .select(
        "order_id", "customer_id", "customer_name", "city",
        col("item.item_id").alias("item_id"),
        col("item.product").alias("product"),
        col("item.price").alias("price"),
        col("item.quantity").alias("quantity"),
        "payment_mode", "payment_status", "order_date"
    )

# Question 3: Total item value
df_with_total = df_flattened.withColumn(
    "total_item_value", 
    col("price") * col("quantity")
)

# Question 4: Total amount spent per customer (successful payments only)
customer_spending = df_with_total.filter(col("payment_status") == "Success") \
    .groupBy("customer_id", "customer_name", "city") \
    .agg(sum("total_item_value").alias("total_amount_spent"))

# Question 5: Most frequently purchased product
popular_products = df_with_total.groupBy("product").agg(
    count("order_id").alias("purchase_count")
).orderBy(col("purchase_count").desc())

# Question 6: Failed payments and products
failed_payments = df_with_total.filter(col("payment_status") == "Failed") \
    .select("customer_id", "customer_name", "product", "price", "quantity")

# Display results
print("Question 1 & 2 - Flattened Data:")
df_flattened.show()

print("Question 3 - With Total Value:")
df_with_total.show()

print("Question 4 - Customer Spending (Successful):")
customer_spending.show()

print("Question 5 - Popular Products:")
popular_products.show()

print("Question 6 - Failed Payments:")
failed_payments.show()

---------------------------------------------------------------------------------------------------------

%python

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Create DataFrames
orders_data = [
    (101, "C001", "2023-05-10", 1200, "Delivered"),
    (102, "C002", "2023-05-11", 800, "Returned"),
    (103, "C001", "2023-05-15", 2500, "Delivered"),
    (104, "C003", "2023-05-18", 900, "Delivered"),
    (105, "C002", "2023-05-19", 600, "Delivered"),
    (106, "C004", "2023-05-20", 700, "Returned"),
    (107, "C005", "2023-05-22", 2000, "Delivered"),
    (108, "C001", "2023-05-25", 3000, "Delivered")
]

refunds_data = [
    ("R101", 102, "2023-05-13", 800),
    ("R102", 106, "2023-05-22", 700)
]

customers_data = [
    ("C001", "Ramesh Kumar", "Delhi", 1200),
    ("C002", "Priya Singh", "Mumbai", 800),
    ("C003", "Arjun Patel", "Pune", 500),
    ("C004", "Meena Devi", "Jaipur", 200),
    ("C005", "Kiran Joshi", "Hyderabad", 1000)
]

orders_df = spark.createDataFrame(orders_data, ["order_id", "customer_id", "order_date", "amount", "status"])
refunds_df = spark.createDataFrame(refunds_data, ["refund_id", "order_id", "refund_date", "refund_amount"])
customers_df = spark.createDataFrame(customers_data, ["customer_id", "name", "city", "loyalty_points"])

# Question 1: Join all datasets
df_joined = orders_df \
    .join(refunds_df, orders_df.order_id == refunds_df.order_id, "left") \
    .join(customers_df, "customer_id") \
    .select(
        orders_df.order_id, "customer_id", "name", "city", "order_date", 
        orders_df.amount, "status", "refund_id", "refund_date", "refund_amount", "loyalty_points"
    )

# Question 2: Customer spending summary
customer_summary = df_joined.groupBy("customer_id", "name").agg(
    sum("amount").alias("total_purchase"),
    sum(coalesce("refund_amount", 0)).alias("total_refund"),
    (sum("amount") - sum(coalesce("refund_amount", 0))).alias("net_spending")
)

# Question 3: Customers with refund percentage > 20%
high_refund_customers = customer_summary.filter(
    (col("total_refund") / col("total_purchase") * 100) > 20
)

# Question 4: Update loyalty points
updated_loyalty = customer_summary.withColumn("updated_loyalty_points",
    when(col("net_spending") > 4000, col("loyalty_points") * 1.10)
    .when(col("net_spending") < 1000, col("loyalty_points") * 0.95)
    .otherwise(col("loyalty_points"))
).join(customers_df.select("customer_id", "loyalty_points"), "customer_id") \
 .select("customer_id", "name", "loyalty_points", "updated_loyalty_points")

# Question 5: Rank customers by net spending
window_net_spending = Window.orderBy(col("net_spending").desc())
customer_ranking = customer_summary.withColumn("rank", rank().over(window_net_spending))

# Display results
print("Question 1 - Joined Data:")
df_joined.show()

print("Question 2 - Customer Summary:")
customer_summary.show()

print("Question 3 - High Refund Customers:")
high_refund_customers.show()

print("Question 4 - Updated Loyalty Points:")
updated_loyalty.show()

print("Question 5 - Customer Ranking:")
customer_ranking.show()

----------------------------------------------------------------------------------------------------

%python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Create DataFrames
sales_data = [
    ("S001", "ST01", "P001", 3, 120, 10, "2023-06-12"),
    ("S002", "ST02", "P002", 2, 200, 0, "2023-06-12"),
    ("S003", "ST01", "P003", 1, 150, 5, "2023-06-13"),
    ("S004", "ST03", "P002", 5, 200, 20, "2023-06-13"),
    ("S005", "ST02", "P001", 4, 120, 10, "2023-06-14"),
    ("S006", "ST01", "P004", 2, 180, 0, "2023-06-14")
]

store_info = [
    ("ST01", "Mumbai", "Rajesh", 2018),
    ("ST02", "Delhi", "Priya", 2020),
    ("ST03", "Bangalore", "Manish", 2019)
]

product_info = [
    ("P001", "Electronics", "Sony", 90),
    ("P002", "Furniture", "IKEA", 150),
    ("P003", "Electronics", "Samsung", 100),
    ("P004", "Appliances", "Philips", 120)
]

sales_df = spark.createDataFrame(sales_data, ["sale_id", "store_id", "product_id", "quantity", "unit_price", "discount", "sale_date"])
store_df = spark.createDataFrame(store_info, ["store_id", "city", "manager", "open_year"])
product_df = spark.createDataFrame(product_info, ["product_id", "category", "brand", "cost_price"])

# Question 1: Basic Transformations
# Join all datasets
df_master = sales_df \
    .join(store_df, "store_id") \
    .join(product_df, "product_id") \
    .withColumn("total_amount", col("quantity") * col("unit_price")) \
    .withColumn("discounted_amount", 
               col("total_amount") - (col("total_amount") * col("discount") / 100))

# Question 2: Aggregations
# Total revenue per store after discount
store_revenue = df_master.groupBy("store_id", "city").agg(
    sum("discounted_amount").alias("total_revenue")
)

# Average discount and total quantity per category
category_stats = df_master.groupBy("category").agg(
    avg("discount").alias("avg_discount"),
    sum("quantity").alias("total_quantity")
)

# Top 2 stores by revenue
top_stores = store_revenue.orderBy(col("total_revenue").desc()).limit(2)

# Question 3: Window Functions
# Rank stores by total revenue
window_store = Window.orderBy(col("total_revenue").desc())
store_ranking = store_revenue.withColumn("rank", rank().over(window_store))

# Most sold product per category
window_category = Window.partitionBy("category").orderBy(col("total_quantity").desc())
top_products_per_category = df_master.groupBy("category", "product_id", "brand").agg(
    sum("quantity").alias("total_quantity")
).withColumn("rn", row_number().over(window_category)) \
 .filter(col("rn") == 1).drop("rn")

# Question 4: Conditional Logic
# Add profit margin
df_with_profit = df_master.withColumn(
    "profit_margin", 
    col("discounted_amount") - (col("quantity") * col("cost_price"))
)

# Categorize stores based on profit margin
store_profit_category = df_with_profit.groupBy("store_id", "city").agg(
    avg("profit_margin").alias("avg_profit_margin")
).withColumn("profit_category",
    when(col("avg_profit_margin") > 500, "High Profit")
    .when((col("avg_profit_margin") >= 200) & (col("avg_profit_margin") <= 500), "Medium Profit")
    .otherwise("Low Profit")
)

# Display results
print("Question 1 - Master Dataset:")
df_master.show()

print("Question 2 - Store Revenue:")
store_revenue.show()

print("Question 2 - Category Stats:")
category_stats.show()

print("Question 2 - Top 2 Stores:")
top_stores.show()

print("Question 3 - Store Ranking:")
store_ranking.show()

print("Question 3 - Top Products per Category:")
top_products_per_category.show()

print("Question 4 - Store Profit Categories:")
store_profit_category.show()

------------------------------------------------------------------------------------------------

%python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Data and Columns
data_day32 = [
    ("T001", "C001", "P101", "Electronics", 2, 20000, 10, "N", "2025-08-01"),
    ("T002", "C002", "P102", "Electronics", 1, 15000, 5, "N", "2025-08-02"),
    ("T003", "C003", "P103", "Furniture", 3, 8000, 0, "Y", "2025-08-02"),
    ("T004", "C001", "P104", "Grocery", 10, 300, 2, "N", "2025-08-03"),
    ("T005", "C002", "P105", "Grocery", 8, 400, 0, "N", "2025-08-04"),
    ("T006", "C001", "P106", "Electronics", 1, 25000, 15, "N", "2025-08-05"),
    ("T007", "C004", "P107", "Furniture", 2, 12000, 5, "N", "2025-08-06"),
    ("T008", "C002", "P108", "Grocery", 5, 600, 3, "N", "2025-08-06"),
    ("T009", "C003", "P109", "Electronics", 1, 30000, 10, "Y", "2025-08-07"),
    ("T010", "C004", "P110", "Grocery", 6, 550, 2, "N", "2025-08-07")
]

cols_day32 = ["TransactionID", "CustomerID", "ProductID", "Category", "Quantity", "Price", "Discount", "ReturnFlag", "TransactionDate"]

df_day32 = spark.createDataFrame(data_day32, cols_day32)

# Question 1: Load and Prepare Data
df_prepared = df_day32.withColumn("TransactionDate", to_date(col("TransactionDate"), "yyyy-MM-dd"))

# Question 2: Calculate Total Revenue
df_with_revenue = df_prepared.withColumn(
    "TotalRevenue", 
    col("Quantity") * col("Price") * (1 - col("Discount") / 100)
)

# Question 3: Category-Wise Analysis
category_analysis = df_with_revenue.groupBy("Category").agg(
    sum("TotalRevenue").alias("total_revenue"),
    avg("Discount").alias("avg_discount")
).orderBy(col("total_revenue").desc())

# Question 4: Customer Purchase Frequency
customer_frequency = df_with_revenue.groupBy("CustomerID").agg(
    count("TransactionID").alias("transaction_count")
).withColumn("rank", rank().over(Window.orderBy(col("transaction_count").desc())))

# Question 5: Window Function Challenge
window_category = Window.partitionBy("Category")

df_window_analysis = df_with_revenue \
    .withColumn("max_revenue", max("TotalRevenue").over(window_category)) \
    .withColumn("avg_revenue", avg("TotalRevenue").over(window_category)) \
    .withColumn("revenue_rank", rank().over(Window.partitionBy("Category").orderBy(col("TotalRevenue").desc())))

# Question 6: Filter and Insights
# Customers who spent more than ₹30,000
high_spending_customers = df_with_revenue.groupBy("CustomerID").agg(
    sum("TotalRevenue").alias("total_spent")
).filter(col("total_spent") > 30000)

# Total refund loss
refund_loss = df_with_revenue.filter(col("ReturnFlag") == "Y") \
    .agg(sum("TotalRevenue").alias("total_refund_loss"))

# Display results
print("Question 1 - Prepared Data:")
df_prepared.show()

print("Question 2 - With Revenue:")
df_with_revenue.show()

print("Question 3 - Category Analysis:")
category_analysis.show()

print("Question 4 - Customer Frequency:")
customer_frequency.show()

print("Question 5 - Window Analysis:")
df_window_analysis.show()

print("Question 6 - High Spending Customers:")
high_spending_customers.show()

print("Question 6 - Refund Loss:")
refund_loss.show()

---------------------------------------------------------------------------------------------

%python

# Day 31 Solution
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Products Data
products_data = [
    ("P001", "Electronics", "Mobile", "Galaxy S22", "Samsung", 75000),
    ("P002", "Electronics", "Laptop", "MacBook Air M2", "Apple", 120000),
    ("P003", "Home", "Furniture", "Office Chair", "GreenSoul", 7000),
    ("P004", "Home", "Kitchen", "Mixer Grinder", "Philips", 4000),
    ("P005", "Fashion", "Men's Clothing", "Denim Jeans", "Levis", 3500)
]

products_columns = ["product_id", "category", "sub_category", "product_name", "brand", "price"]

# Sales Data
sales_data = [
    ("O001", "P001", 2, 10, "2025-09-01", "North"),
    ("O002", "P002", 1, 5, "2025-09-03", "West"),
    ("O003", "P003", 5, 0, "2025-09-03", "South"),
    ("O004", "P004", 3, 5, "2025-09-04", "East"),
    ("O005", "P005", 4, 15, "2025-09-04", "North"),
    ("O006", "P001", 1, 0, "2025-09-05", "South"),
    ("O007", "P002", 2, 10, "2025-09-06", "West")
]

sales_columns = ["order_id", "product_id", "quantity", "discount", "order_date", "store_region"]

# Create DataFrames
df_products = spark.createDataFrame(products_data, products_columns)
df_sales = spark.createDataFrame(sales_data, sales_columns)

# Question 1: Load and inspect data
print("Products Schema:")
df_products.printSchema()
print("Sales Schema:")
df_sales.printSchema()

print("Products Data:")
df_products.show()
print("Sales Data:")
df_sales.show()

# Question 2: Join and calculate revenue
df_joined = df_sales.join(df_products, "product_id")
df_with_revenue = df_joined.withColumn(
    "net_revenue", 
    (col("price") * col("quantity")) - (col("price") * col("quantity") * col("discount") / 100)
)

# Question 3: Region-wise revenue
region_revenue = df_with_revenue.groupBy("store_region").agg(
    sum("net_revenue").alias("total_revenue")
).orderBy(col("total_revenue").desc())

# Question 4: Category-wise performance
category_performance = df_with_revenue.groupBy("category").agg(
    sum("net_revenue").alias("total_revenue"),
    sum("quantity").alias("total_quantity")
).orderBy(col("total_revenue").desc())

# Question 5: Top 3 brands by revenue
top_brands = df_with_revenue.groupBy("brand").agg(
    sum("net_revenue").alias("total_revenue")
).orderBy(col("total_revenue").desc()).limit(3)

# Question 6: Date-based aggregation
df_with_date_parts = df_with_revenue \
    .withColumn("order_month", month(col("order_date"))) \
    .withColumn("order_weekday", dayofweek(col("order_date")))

daily_revenue_per_region = df_with_date_parts.groupBy("store_region", "order_date").agg(
    sum("net_revenue").alias("daily_revenue")
).groupBy("store_region").agg(
    avg("daily_revenue").alias("avg_daily_revenue")
)

# Display results
print("Question 2 - Joined with Revenue:")
df_with_revenue.show()

print("Question 3 - Region-wise Revenue:")
region_revenue.show()

print("Question 4 - Category Performance:")
category_performance.show()

print("Question 5 - Top 3 Brands:")
top_brands.show()

print("Question 6 - Average Daily Revenue per Region:")
daily_revenue_per_region.show()