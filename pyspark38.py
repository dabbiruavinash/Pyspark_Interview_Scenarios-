%python

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("Day30").getOrCreate()

# Define schema and data
schema = StructType([
    StructField("TransactionID", StringType(), True),
    StructField("StoreID", StringType(), True),
    StructField("ProductID", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("UnitPrice", DoubleType(), True),
    StructField("Discount", DoubleType(), True),
    StructField("Returned", BooleanType(), True)
])

data = [
    ("T001", "S001", "P001", "2025-07-01", 3, 250.0, 0.10, False),
    ("T002", "S001", "P002", "2025-07-01", 1, 400.0, 0.00, True),
    ("T003", "S002", "P003", "2025-07-03", 5, 100.0, 0.05, False),
    ("T004", "S002", "P002", "2025-07-05", 2, 450.0, 0.15, False),
    ("T005", "S003", "P004", "2025-07-06", 4, 300.0, 0.10, False),
    ("T006", "S003", "P001", "2025-07-08", 6, 250.0, 0.00, False),
    ("T007", "S001", "P003", "2025-07-10", 3, 100.0, 0.00, False),
    ("T008", "S002", "P004", "2025-07-11", 2, 300.0, 0.10, True),
    ("T009", "S003", "P002", "2025-07-12", 1, 450.0, 0.05, False)
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Question 1: Load and Clean Data
df_clean = df \
    .withColumn("Date", to_date(col("Date"), "yyyy-MM-dd")) \
    .filter(col("Returned") == False)

# Question 2: Add Calculated Columns
df_with_total = df_clean.withColumn(
    "TotalAmount", 
    round(col("Quantity") * col("UnitPrice") * (1 - col("Discount")), 2)
)

# Question 3: Store-level Insights
store_insights = df_with_total.groupBy("StoreID").agg(
    sum("TotalAmount").alias("StoreRevenue"),
    avg("Discount").alias("AvgDiscount")
)

# Rank stores by total revenue
store_ranking = store_insights.withColumn(
    "Rank", 
    rank().over(Window.orderBy(col("StoreRevenue").desc()))
)

# Question 4: Product-level Insights
product_insights = df_with_total.groupBy("ProductID").agg(
    sum("Quantity").alias("TotalQuantitySold"),
    sum("TotalAmount").alias("TotalRevenue")
)

# Top 2 best-selling products
top_products = product_insights.orderBy(col("TotalQuantitySold").desc()).limit(2)

# Question 5: Date-based Insights
date_insights = df_with_total \
    .withColumn("Weekday", date_format(col("Date"), "EEEE")) \
    .groupBy("Date").agg(
        sum("TotalAmount").alias("DailyRevenue")
    ).orderBy("Date")

# Weekday with highest average revenue
weekday_revenue = df_with_total \
    .withColumn("Weekday", date_format(col("Date"), "EEEE")) \
    .groupBy("Weekday").agg(
        avg("TotalAmount").alias("AvgDailyRevenue")
    ).orderBy(col("AvgDailyRevenue").desc())

# Display results
print("Question 1 - Cleaned Data:")
df_clean.show()

print("Question 2 - With Total Amount:")
df_with_total.show()

print("Question 3 - Store Insights:")
store_insights.show()

print("Question 3 - Store Ranking:")
store_ranking.show()

print("Question 4 - Product Insights:")
product_insights.show()

print("Question 4 - Top 2 Products:")
top_products.show()

print("Question 5 - Daily Revenue:")
date_insights.show()

print("Question 5 - Weekday Revenue:")
weekday_revenue.show()

------------------------------------------------------------------------------------------------------------
%python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Data and columns
data_day29 = [
    ("T001", "C001", "2025-01-03", "Deposit", 2500, "Mumbai"),
    ("T002", "C002", "2025-01-04", "Withdrawal", 1500, "Delhi"),
    ("T003", "C001", "2025-01-06", "Withdrawal", 500, "Mumbai"),
    ("T004", "C003", "2025-01-07", "Deposit", 6000, "Chennai"),
    ("T005", "C002", "2025-01-09", "Deposit", 10000, "Delhi"),
    ("T006", "C001", "2025-01-11", "Deposit", 12000, "Mumbai"),
    ("T007", "C004", "2025-01-12", "Withdrawal", 20000, "Bangalore"),
    ("T008", "C003", "2025-01-14", "Withdrawal", 800, "Chennai"),
    ("T009", "C002", "2025-01-15", "Deposit", 15000, "Delhi"),
    ("T010", "C001", "2025-01-16", "Withdrawal", 7000, "Mumbai"),
]

columns_day29 = ["TransactionID", "CustomerID", "TransactionDate", "TransactionType", "Amount", "Branch"]

df_day29 = spark.createDataFrame(data_day29, columns_day29)

# 1. Total Deposits & Withdrawals per Customer
customer_totals = df_day29.groupBy("CustomerID").pivot("TransactionType").agg(
    sum("Amount")
).fillna(0)

# 2. Running Balance per Customer
window_customer = Window.partitionBy("CustomerID").orderBy("TransactionDate")
running_balance = df_day29.withColumn(
    "AmountSigned",
    when(col("TransactionType") == "Deposit", col("Amount"))
    .otherwise(-col("Amount"))
).withColumn(
    "RunningBalance",
    sum("AmountSigned").over(window_customer)
)

# 3. Top 2 Highest Deposits per Branch
window_branch = Window.partitionBy("Branch").orderBy(col("Amount").desc())
top_deposits_per_branch = df_day29.filter(col("TransactionType") == "Deposit") \
    .withColumn("rank", rank().over(window_branch)) \
    .filter(col("rank") <= 2) \
    .select("Branch", "CustomerID", "Amount", "rank")

# 4. Detect Outlier Transactions
customer_stats = df_day29.groupBy("CustomerID").agg(
    avg("Amount").alias("AvgAmount"),
    stddev("Amount").alias("StdDevAmount")
)

outlier_transactions = df_day29.join(customer_stats, "CustomerID") \
    .withColumn("IsOutlier", 
               col("Amount") > (col("AvgAmount") + 2 * col("StdDevAmount"))) \
    .filter(col("IsOutlier") == True)

# 5. Customer Ranking by Total Deposits
customer_deposit_ranking = df_day29.filter(col("TransactionType") == "Deposit") \
    .groupBy("CustomerID").agg(
        sum("Amount").alias("TotalDeposits")
    ).withColumn("Rank", rank().over(Window.orderBy(col("TotalDeposits").desc())))

# Display results
print("1. Customer Totals:")
customer_totals.show()

print("2. Running Balance:")
running_balance.select("CustomerID", "TransactionDate", "TransactionType", "Amount", "RunningBalance").show()

print("3. Top 2 Deposits per Branch:")
top_deposits_per_branch.show()

print("4. Outlier Transactions:")
outlier_transactions.show()

print("5. Customer Deposit Ranking:")
customer_deposit_ranking.show()

---------------------------------------------------------------------------------------------------------

%python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Sample Data
data_day28 = [
    (1001, "C001", "2023-01-15", "Electronics", 2, 1500, 0.10, "N"),
    (1002, "C002", "2023-01-17", "Clothing", 3, 800, 0.05, "N"),
    (1003, "C003", "2023-02-02", "Electronics", 1, 2000, 0.15, "Y"),
    (1004, "C001", "2023-02-05", "Groceries", 10, 500, 0.00, "N"),
    (1005, "C004", "2023-02-20", "Clothing", 5, 1200, 0.20, "N"),
    (1006, "C002", "2023-03-01", "Electronics", 1, 1800, 0.05, "N"),
    (1007, "C005", "2023-03-15", "Groceries", 8, 400, 0.00, "N"),
    (1008, "C001", "2023-03-18", "Clothing", 2, 1000, 0.10, "Y"),
    (1009, "C003", "2023-04-01", "Electronics", 3, 2500, 0.05, "N")
]

columns_day28 = ["OrderID", "CustomerID", "OrderDate", "ProductCategory", "Quantity", "Price", "Discount", "Returned"]

df_day28 = spark.createDataFrame(data_day28, columns_day28)

# 1. Load dataset (already done)

# 2. Calculate total revenue per product category
category_revenue = df_day28.withColumn(
    "Revenue", 
    col("Quantity") * col("Price") * (1 - col("Discount"))
).groupBy("ProductCategory").agg(
    sum("Revenue").alias("TotalRevenue")
)

# 3. Top 2 customers with highest total spending
top_customers = df_day28.withColumn(
    "Revenue", 
    col("Quantity") * col("Price") * (1 - col("Discount"))
).groupBy("CustomerID").agg(
    sum("Revenue").alias("TotalSpending")
).orderBy(col("TotalSpending").desc()).limit(2)

# 4. Return percentage per product category
return_stats = df_day28.groupBy("ProductCategory").agg(
    count("*").alias("TotalOrders"),
    sum(when(col("Returned") == "Y", 1).otherwise(0)).alias("ReturnedOrders")
).withColumn(
    "ReturnPercentage", 
    (col("ReturnedOrders") / col("TotalOrders")) * 100
)

# 5. Monthly sales trend
monthly_trend = df_day28 \
    .withColumn("Revenue", col("Quantity") * col("Price") * (1 - col("Discount"))) \
    .withColumn("Month", date_format(col("OrderDate"), "yyyy-MM")) \
    .groupBy("Month").agg(
        sum("Revenue").alias("MonthlyRevenue")
    ).orderBy("Month")

# Display results
print("2. Category Revenue:")
category_revenue.show()

print("3. Top 2 Customers:")
top_customers.show()

print("4. Return Percentage by Category:")
return_stats.show()

print("5. Monthly Sales Trend:")
monthly_trend.show()

----------------------------------------------------------------------------------------------------------

%python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Data and columns
data_day27 = [
    (1001, "C001", "2025-01-10", "Shoes", 2, 2000, 0.10),
    (1002, "C002", "2025-01-11", "Shirt", 1, 1200, None),
    (1003, "C003", "2025-01-12", "Shoes", 1, 2000, 0.05),
    (1004, "C001", "2025-01-13", "Jeans", 2, 2500, 0.15),
    (1005, "C002", "2025-01-13", "Shoes", 1, 2000, 0.00),
    (1005, "C002", "2025-01-13", "Shoes", 1, 2000, 0.00),  # duplicate
    (1006, "C004", "2025-01-14", "Shirt", None, 1200, 0.20),
    (1007, "C003", "2025-01-15", "Shoes", 2, 2000, 0.10),
    (1008, "C005", "2025-01-16", "Shoes", 1, 2000, "Returned"),
]

cols_day27 = ["OrderID", "CustomerID", "OrderDate", "Product", "Quantity", "UnitPrice", "Discount"]

df_day27 = spark.createDataFrame(data_day27, cols_day27)

# 1. Clean & Prepare Data
# Remove duplicates
df_clean = df_day27.dropDuplicates()

# Replace null discounts with 0.0 and handle "Returned"
df_prepared = df_clean \
    .withColumn("Discount", 
               when(col("Discount") == "Returned", 1.0)
               .when(col("Discount").isNull(), 0.0)
               .otherwise(col("Discount"))) \
    .withColumn("SalesAmount",
               when(col("Discount") == 1.0, -col("UnitPrice") * coalesce(col("Quantity"), 0))
               .otherwise(col("Quantity") * col("UnitPrice") * (1 - col("Discount"))))

# 2. Sales Analysis
# Add SalesAmount for valid rows (already done above)

# Total revenue per product
product_revenue = df_prepared.filter(col("Discount") != 1.0).groupBy("Product").agg(
    sum("SalesAmount").alias("TotalRevenue")
)

# Top 2 customers who spent the most
top_customers = df_prepared.filter(col("Discount") != 1.0).groupBy("CustomerID").agg(
    sum("SalesAmount").alias("TotalSpent")
).orderBy(col("TotalSpent").desc()).limit(2)

# 3. Running total revenue of Shoes over time
shoes_sales = df_prepared.filter(
    (col("Product") == "Shoes") & (col("Discount") != 1.0)
).withColumn("OrderDate", to_date(col("OrderDate")))

window_shoes = Window.orderBy("OrderDate")
shoes_running_total = shoes_sales.withColumn(
    "RunningTotal", 
    sum("SalesAmount").over(window_shoes)
).select("OrderDate", "SalesAmount", "RunningTotal")

# Display results
print("1. Cleaned Data:")
df_prepared.show()

print("2. Product Revenue:")
product_revenue.show()

print("2. Top 2 Customers:")
top_customers.show()

print("3. Shoes Running Total:")
shoes_running_total.show()

------------------------------------------------------------------------------------------

%python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Load dataset
data_day26 = [
    (1001, "Shoes", "Footwear", 2, 1200.50, 0.1, "2025-01-05", "Store_A"),
    (1002, "Shirt", "Apparel", None, 800, 0.05, "2025-01-06", "Store_B"),
    (1003, "Laptop", "Electronics", 1, -55000, 0.2, "2025/01/06", "Store_C"),
    (1004, None, "Apparel", 3, 500, 0.0, "2025-01-07", "Store_A"),
    (1005, "Headphones", "Electronics", 2, 3000, 0.15, "2025-01-08", "Store_B"),
    (1006, "Shoes", "Footwear", 1, 1500, None, "2025-01-08", "Store_XYZ"),
    (1007, "Jacket", "Apparel", 2, 3500, 0.1, "invalid", "Store_C"),
    (1008, "TV", "Electronics", None, 42000, 0.05, "2025-01-09", "Store_B")
]

columns_day26 = ["TransactionID", "Product", "Category", "Quantity", "Price", "Discount", "Date", "Store"]

df_day26 = spark.createDataFrame(data_day26, columns_day26)

# 1. Load Data (already done)

# 2. Handle Missing Values
df_filled = df_day26 \
    .fillna(0, subset=["Quantity"]) \
    .fillna(0.0, subset=["Discount"]) \
    .fillna("Unknown", subset=["Product"])

# 3. Data Cleaning
df_clean = df_filled \
    .filter(col("Price") > 0) \
    .withColumn("Date", 
               when(col("Date").rlike("^\\d{4}-\\d{2}-\\d{2}$"), to_date(col("Date"), "yyyy-MM-dd"))
               .when(col("Date").rlike("^\\d{4}/\\d{2}/\\d{2}$"), to_date(col("Date"), "yyyy/MM/dd"))
               .otherwise(None)) \
    .withColumn("Store", 
               when(col("Store") == "Store_XYZ", "Store_C")
               .otherwise(col("Store")))

# 4. Transformations
df_transformed = df_clean \
    .withColumn("TotalAmount", col("Quantity") * col("Price") * (1 - col("Discount"))) \
    .withColumn("Year", year(col("Date"))) \
    .withColumn("Month", month(col("Date")))

# 5. Analysis
# Top 3 categories by TotalAmount
top_categories = df_transformed.groupBy("Category").agg(
    sum("TotalAmount").alias("TotalSales")
).orderBy(col("TotalSales").desc()).limit(3)

# Store with maximum sales
top_store = df_transformed.groupBy("Store").agg(
    sum("TotalAmount").alias("TotalSales")
).orderBy(col("TotalSales").desc()).first()

# Average discount per category
avg_discount_by_category = df_transformed.groupBy("Category").agg(
    avg("Discount").alias("AvgDiscount")
)

# Count transactions with invalid/missing dates
invalid_date_count = df_transformed.filter(col("Date").isNull()).count()

# Display results
print("2. Filled Data:")
df_filled.show()

print("3. Cleaned Data:")
df_clean.show()

print("4. Transformed Data:")
df_transformed.show()

print("5. Top 3 Categories:")
top_categories.show()

print(f"5. Top Store: {top_store['Store']} with sales: {top_store['TotalSales']}")

print("5. Average Discount by Category:")
avg_discount_by_category.show()

print(f"5. Invalid/Missing Date Count: {invalid_date_count}")

-------------------------------------------------------------------------------------------

%python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Sample data
data_day25 = [
    (1001, "C001", "Laptop", "Electronics", 1, 50000, 0.10, "N", "2025-01-10"),
    (1002, "C002", "Shoes", "Fashion", 2, 2000, 0.05, "N", "2025-01-12"),
    (1003, "C003", "Phone", "Electronics", 1, 30000, None, "N", "2025-01-15"),
    (1004, "C002", "Shoes", "Fashion", 2, 2000, 0.05, "Y", "2025-01-16"),
    (1005, "C001", "Laptop", "Electronics", 1, 50000, 0.15, "N", "2025-01-18"),
    (1005, "C001", "Laptop", "Electronics", 1, 50000, 0.15, "N", "2025-01-18")  # duplicate
]

columns_day25 = ["OrderID", "CustomerID", "Product", "Category", "Quantity", "Price", "Discount", "ReturnFlag", "OrderDate"]

df_day25 = spark.createDataFrame(data_day25, columns_day25)

# 1. Remove Duplicates & Handle Nulls
df_clean = df_day25.dropDuplicates().fillna(0.0, subset=["Discount"])

# 2. Compute Net Sales per Order (ignore returns)
df_net_sales = df_clean.filter(col("ReturnFlag") == "N") \
    .withColumn("NetSales", 
               (col("Quantity") * col("Price")) - (col("Discount") * col("Quantity") * col("Price")))

# 3. Category-Wise Sales Summary
category_summary = df_net_sales.groupBy("Category").agg(
    sum("NetSales").alias("TotalSales"),
    avg("Discount").alias("AvgDiscount"),
    countDistinct("CustomerID").alias("UniqueCustomers")
)

# 4. Top 2 Customers by Spend
top_customers = df_net_sales.groupBy("CustomerID").agg(
    sum("NetSales").alias("TotalSpend")
).orderBy(col("TotalSpend").desc()).limit(2)

# 5. Monthly Sales Trend
monthly_trend = df_net_sales \
    .withColumn("Month", date_format(col("OrderDate"), "yyyy-MM")) \
    .groupBy("Month").agg(
        sum("NetSales").alias("MonthlySales")
    ).orderBy("Month")

# Display results
print("1. Cleaned Data:")
df_clean.show()

print("2. Net Sales (Excluding Returns):")
df_net_sales.show()

print("3. Category Summary:")
category_summary.show()

print("4. Top 2 Customers:")
top_customers.show()

print("5. Monthly Sales Trend:")
monthly_trend.show()
---------------------------------------------------------------------------------------

%python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Sample data
data_day24 = [
    ("T1", "C102", "P1001", "Electronics", 2, 500.0, 0.1, "N", "2023-01-15"),
    ("T2", "C356", "P2002", "Clothing", 3, 200.0, 0.05, "N", "2023-02-12"),
    ("T3", "C110", "P3003", "Home Decor", 1, 1500.0, 0.2, "Y", "2023-02-20"),
    ("T4", "C221", "P4004", "Electronics", 5, 120.0, 0.0, "N", "2023-03-05"),
    ("T5", "C178", "P5005", "Clothing", 4, 800.0, 0.15, "N", "2023-03-22"),
]

columns_day24 = ["transaction_id", "customer_id", "product_id", "category", "quantity", "price", "discount", "return_flag", "transaction_date"]

df_day24 = spark.createDataFrame(data_day24, columns_day24)

# 1. Clean & Preprocess
df_clean = df_day24.dropDuplicates().filter(
    col("quantity").isNotNull() & col("price").isNotNull()
)

# 2. Net Sales Calculation
df_net_sales = df_clean.filter(col("return_flag") == "N") \
    .withColumn("net_sales", 
               col("quantity") * col("price") * (1 - col("discount")))

# 3. Top 5 Customers by Spending
top_customers = df_net_sales.groupBy("customer_id").agg(
    sum("net_sales").alias("total_spending")
).orderBy(col("total_spending").desc()).limit(5)

# 4. Monthly Sales Trend
monthly_trend = df_net_sales \
    .withColumn("month", date_format(col("transaction_date"), "yyyy-MM")) \
    .groupBy("month").agg(
        sum("net_sales").alias("monthly_sales")
    ).orderBy("month")

# 5. Category Contribution
total_sales = df_net_sales.agg(sum("net_sales")).collect()[0][0]

category_contribution = df_net_sales.groupBy("category").agg(
    sum("net_sales").alias("category_sales")
).withColumn(
    "contribution_percentage",
    (col("category_sales") / total_sales) * 100
)

# Display results
print("1. Cleaned Data:")
df_clean.show()

print("2. Net Sales (Excluding Returns):")
df_net_sales.show()

print("3. Top 5 Customers:")
top_customers.show()

print("4. Monthly Sales Trend:")
monthly_trend.show()

print("5. Category Contribution:")
category_contribution.show()

-----------------------------------------------------------------------------------------------

%python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Data
data_day23 = [
    (1001, "C001", "P001", 2, 500, 0.1, "N", "2023-01-10"),
    (1002, "C002", "P002", 1, 1200, None, "N", "2023-01-11"),
    (1003, "C001", "P001", 2, 500, 0.1, "N", "2023-01-10"),  # Duplicate
    (1004, "C003", "P003", None, 700, 0.05, "Y", "2023-01-15"),
    (1005, "C004", "P002", 3, 1200, 0.2, "N", "2023-01-16"),
    (1006, None, "P004", 1, 300, 0.0, "N", "2023-01-18")
]

columns_day23 = ["TransactionID", "CustomerID", "ProductID", "Quantity", "Price", "Discount", "ReturnFlag", "TransactionDate"]

df_day23 = spark.createDataFrame(data_day23, columns_day23)

# 1. Remove Duplicates
df_deduped = df_day23.dropDuplicates()

# 2. Handle Null Values
df_clean = df_deduped \
    .fillna(0, subset=["Discount"]) \
    .filter(col("CustomerID").isNotNull() & col("Quantity").isNotNull())

# 3. Calculate Net Amount
df_net_amount = df_clean.withColumn(
    "NetAmount", 
    col("Quantity") * col("Price") * (1 - col("Discount"))
)

# 4. Exclude Returns
df_final = df_net_amount.filter(col("ReturnFlag") == "N")

# 5. Find Top 2 Customers by Sales
top_customers = df_final.groupBy("CustomerID").agg(
    sum("NetAmount").alias("TotalSales")
).orderBy(col("TotalSales").desc()).limit(2)

# Display results
print("1. Deduplicated Data:")
df_deduped.show()

print("2. Cleaned Data:")
df_clean.show()

print("3. With Net Amount:")
df_net_amount.show()

print("4. Final Data (Excluding Returns):")
df_final.show()

print("5. Top 2 Customers:")
top_customers.show()

-----------------------------------------------------------------------------------------------

%python

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Orders Data
orders_data = [
    (1, 101, "2025-01-10", 250),
    (2, 102, "2025-01-11", 450),
    (3, 101, "2025-02-05", 300),
    (4, 103, "2025-02-07", 500),
    (5, 104, "2025-03-01", 700),
    (6, 102, "2025-03-03", 350),
    (7, 101, "2025-03-05", 200),
    (8, 103, "2025-03-10", 600)
]

orders_columns = ["order_id", "customer_id", "order_date", "order_amount"]

# Customers Data
customers_data = [
    (101, "Alice", "Mumbai"),
    (102, "Bob", "Delhi"),
    (103, "Charlie", "Bangalore"),
    (104, "David", "Chennai")
]

customers_columns = ["customer_id", "customer_name", "city"]

# Create DataFrames
orders_df = spark.createDataFrame(orders_data, orders_columns)
customers_df = spark.createDataFrame(customers_data, customers_columns)

# 1. Join datasets
df_joined = orders_df.join(customers_df, "customer_id")

# 2. Total spending per customer
customer_spending = df_joined.groupBy("customer_id", "customer_name").agg(
    sum("order_amount").alias("total_spending")
)

# 3. Top 2 customers by total spending
top_customers = customer_spending.orderBy(col("total_spending").desc()).limit(2)

# 4. Monthly total order amount
monthly_sales = df_joined \
    .withColumn("month", date_format(col("order_date"), "yyyy-MM")) \
    .groupBy("month").agg(
        sum("order_amount").alias("monthly_total")
    ).orderBy("month")

# 5. Average order value per customer
avg_order_value = df_joined.groupBy("customer_id", "customer_name").agg(
    avg("order_amount").alias("average_order_value")
)

# Display results
print("1. Joined Data:")
df_joined.show()

print("2. Customer Spending:")
customer_spending.show()

print("3. Top 2 Customers:")
top_customers.show()

print("4. Monthly Sales:")
monthly_sales.show()

print("5. Average Order Value:")
avg_order_value.show()

-------------------------------------------------------------------------------------------

%python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Data
data_day21 = [
    ("C1", "2025-01-10", 500),
    ("C2", "2025-01-12", 150),
    ("C1", "2025-01-15", 200),
    ("C3", "2025-02-01", 1000),
    ("C2", "2025-02-05", 300),
]

columns_day21 = ["CustomerID", "TransactionDate", "Amount"]

df_day21 = spark.createDataFrame(data_day21, columns_day21)

# Find customers with more than 2 transactions in January 2025
january_transactions = df_day21 \
    .withColumn("TransactionDate", to_date(col("TransactionDate"))) \
    .filter(
        (year(col("TransactionDate")) == 2025) & 
        (month(col("TransactionDate")) == 1)
    )

customer_transaction_count = january_transactions.groupBy("CustomerID").agg(
    count("*").alias("TransactionCount")
)

customers_more_than_2 = customer_transaction_count.filter(col("TransactionCount") > 2)

# Display results
print("January Transactions:")
january_transactions.show()

print("Customer Transaction Count in January:")
customer_transaction_count.show()

print("Customers with more than 2 transactions in January 2025:")
customers_more_than_2.show()