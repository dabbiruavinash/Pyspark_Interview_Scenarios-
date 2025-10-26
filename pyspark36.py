%python

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("Day48").getOrCreate()

# Schema and Data
schema = StructType([
    StructField("emp_id", StringType(), True),
    StructField("department", StringType(), True),
    StructField("month", StringType(), True),
    StructField("sales_amount", IntegerType(), True),
    StructField("customer_feedback", FloatType(), True),
    StructField("working_days", IntegerType(), True),
    StructField("region", StringType(), True)
])

data = [
    ("E001", "Sales", "2025-01", 12000, 4.2, 22, "East"),
    ("E001", "Sales", "2025-02", 18000, 4.8, 20, "East"),
    ("E002", "Sales", "2025-01", 15000, 4.5, 21, "North"),
    ("E002", "Sales", "2025-02", 16000, 4.1, 22, "North"),
    ("E003", "HR", "2025-01", 0, 4.7, 20, "South"),
    ("E003", "HR", "2025-02", 0, 4.9, 19, "South"),
    ("E004", "Marketing", "2025-01", 8000, 3.9, 20, "West"),
    ("E004", "Marketing", "2025-02", 9500, 4.3, 22, "West"),
    ("E005", "Sales", "2025-01", 20000, 4.9, 23, "East"),
    ("E005", "Sales", "2025-02", 21000, 4.8, 21, "East")
]

df = spark.createDataFrame(data, schema)

# Question 1: Calculate Monthly Performance Score
df_q1 = df.withColumn("performance_score", 
                     (col("sales_amount") / col("working_days")) * col("customer_feedback"))

# Question 2: Rank Employees Within Each Department
window_dept = Window.partitionBy("department").orderBy(col("performance_score").desc())
df_q2 = df_q1.withColumn("dept_rank", rank().over(window_dept))

# Question 3: Calculate Performance Growth
window_emp = Window.partitionBy("emp_id").orderBy("month")
df_q3 = df_q2.withColumn("prev_performance", 
                        lag("performance_score").over(window_emp))
df_q3 = df_q3.withColumn("performance_growth",
                        when(col("prev_performance").isNull(), 0)
                        .else_(((col("performance_score") - col("prev_performance")) / col("prev_performance")) * 100))

# Question 4: Department-Level Insights
dept_avg = df_q3.groupBy("department").agg(
    avg("performance_score").alias("avg_performance_score"))

emp_avg = df_q3.groupBy("emp_id", "department").agg(
    avg("performance_score").alias("avg_emp_performance"))

window_top = Window.partitionBy("department").orderBy(col("avg_emp_performance").desc())
top_performer = emp_avg.withColumn("rn", row_number().over(window_top)).filter(col("rn") == 1)

dept_insights = dept_avg.join(top_performer.select("department", "emp_id"), "department")

# Question 5: Regional Trend Analysis
region_monthly = df.groupBy("region", "month").agg(
    sum("sales_amount").alias("total_sales"))

window_region = Window.partitionBy("region").orderBy("month")
region_trend = region_monthly.withColumn("prev_sales", lag("total_sales").over(window_region))
region_trend = region_trend.withColumn("sales_growth_pct",
                                      when(col("prev_sales").isNull(), 0)
                                      .else_(((col("total_sales") - col("prev_sales")) / col("prev_sales")) * 100))

# Question 6: Detect Performance Drop > 10%
performance_drop = df_q3.filter(
    (col("performance_growth") < -10) & 
    (col("prev_performance").isNotNull())).select("emp_id", "month", "performance_growth")

# Display results
print("Question 1 - Performance Score:")
df_q1.show()

print("Question 2 - Department Rank:")
df_q2.show()

print("Question 3 - Performance Growth:")
df_q3.show()

print("Question 4 - Department Insights:")
dept_insights.show()

print("Question 5 - Regional Trends:")
region_trend.show()

print("Question 6 - Performance Drops:")
performance_drop.show()

%sql

-- Question 1: Calculate Monthly Performance Score
WITH performance_data AS (
    SELECT 
        emp_id,
        department,
        month,
        sales_amount,
        customer_feedback,
        working_days,
        region,
        (sales_amount / working_days) * customer_feedback AS performance_score
    FROM employee_sales
)
SELECT * FROM performance_data;

-- Question 2: Rank Employees Within Each Department
SELECT 
    emp_id,
    department,
    month,
    performance_score,
    RANK() OVER (PARTITION BY department ORDER BY performance_score DESC) AS dept_rank
FROM performance_data;

-- Question 3: Calculate Performance Growth
WITH performance_data AS (
    SELECT 
        emp_id,
        department,
        month,
        sales_amount,
        customer_feedback,
        working_days,
        region,
        (sales_amount / working_days) * customer_feedback AS performance_score
    FROM employee_sales
),
growth_data AS (
    SELECT 
        *,
        LAG(performance_score) OVER (PARTITION BY emp_id ORDER BY month) AS prev_performance
    FROM performance_data
)
SELECT 
    *,
    CASE 
        WHEN prev_performance IS NULL THEN 0
        ELSE ((performance_score - prev_performance) / prev_performance) * 100
    END AS performance_growth
FROM growth_data;

-- Question 4: Department-Level Insights
WITH performance_data AS (
    SELECT 
        emp_id,
        department,
        month,
        (sales_amount / working_days) * customer_feedback AS performance_score
    FROM employee_sales
),
dept_avg AS (
    SELECT 
        department,
        AVG(performance_score) AS avg_performance_score
    FROM performance_data
    GROUP BY department
),
emp_avg AS (
    SELECT 
        emp_id,
        department,
        AVG(performance_score) AS avg_emp_performance
    FROM performance_data
    GROUP BY emp_id, department
),
top_performers AS (
    SELECT 
        department,
        emp_id,
        avg_emp_performance,
        ROW_NUMBER() OVER (PARTITION BY department ORDER BY avg_emp_performance DESC) AS rn
    FROM emp_avg
)
SELECT 
    d.department,
    d.avg_performance_score,
    t.emp_id AS top_performer_emp_id
FROM dept_avg d
JOIN top_performers t ON d.department = t.department AND t.rn = 1;

-- Question 5: Regional Trend Analysis
WITH regional_sales AS (
    SELECT 
        region,
        month,
        SUM(sales_amount) AS total_sales
    FROM employee_sales
    GROUP BY region, month
),
sales_growth AS (
    SELECT 
        region,
        month,
        total_sales,
        LAG(total_sales) OVER (PARTITION BY region ORDER BY month) AS prev_sales
    FROM regional_sales
)
SELECT 
    region,
    month,
    total_sales,
    CASE 
        WHEN prev_sales IS NULL THEN 0
        ELSE ((total_sales - prev_sales) / prev_sales) * 100
    END AS sales_growth_pct
FROM sales_growth;

-- Question 6: Detect Performance Drop > 10%
WITH performance_growth AS (
    -- Include the performance_growth calculation from Question 3
    -- This is a simplified version
    SELECT 
        emp_id,
        month,
        performance_growth
    FROM (
        -- Your complete query from Question 3 would go here
        SELECT * FROM performance_growth_calculation
    )
)
SELECT 
    emp_id,
    month,
    performance_growth
FROM performance_growth
WHERE performance_growth < -10;

-----------------------------------------------------------------------------------------------------------------------------------------

%python

# Day 47 Solution
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Schema and Data
schema_day47 = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("discount", DoubleType(), True),
    StructField("refund_amount", DoubleType(), True),
    StructField("order_date", StringType(), True)
])

data_day47 = [
    ("O1001", "C001", "P001", 2, 500.0, 0.10, 0.0, "2025-01-12"),
    ("O1002", "C002", "P002", 1, 1200.0, 0.15, 0.0, "2025-01-13"),
    ("O1003", "C001", "P003", 3, 800.0, 0.0, 400.0, "2025-01-14"),
    ("O1004", "C003", "P001", 1, 500.0, 0.05, 0.0, "2025-01-15"),
    ("O1005", "C002", "P002", 2, 1200.0, 0.10, 300.0, "2025-01-15"),
    ("O1006", "C004", "P004", 5, 300.0, 0.20, 0.0, "2025-01-16"),
    ("O1007", "C001", "P002", 1, 1200.0, 0.10, 0.0, "2025-01-17"),
    ("O1008", "C003", "P003", 2, 800.0, 0.0, 200.0, "2025-01-18")
]

df_day47 = spark.createDataFrame(data_day47, schema_day47)

# Question 1: Read and clean
df_clean = df_day47.fillna(0, subset=["discount"])

# Question 2: Calculate effective transaction amount
df_net = df_clean.withColumn("net_amount", 
                           (col("quantity") * col("price") * (1 - col("discount"))) - col("refund_amount"))

# Question 3: Find top 3 customers by total spending
top_customers = df_net.groupBy("customer_id") \
    .agg(sum("net_amount").alias("total_spending")) \
    .orderBy(col("total_spending").desc()) \
    .limit(3)

# Question 4: Calculate daily total sales
daily_sales = df_net.groupBy("order_date") \
    .agg(sum("net_amount").alias("daily_total_sales")) \
    .orderBy("order_date")

# Question 5: Detect refunds > 25% of total order value
df_refund_flag = df_net.withColumn("refund_flag", 
                                 col("refund_amount") > (col("quantity") * col("price") * 0.25))

# Display results
print("Question 1 - Cleaned Data:")
df_clean.show()

print("Question 2 - Net Amount:")
df_net.show()

print("Question 3 - Top 3 Customers:")
top_customers.show()

print("Question 4 - Daily Sales:")
daily_sales.show()

print("Question 5 - Refund Flag:")
df_refund_flag.show()

-----------------------------------------------------------------------------------------------------------------

%python

# Day 46 Solution
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Data and Columns
data_day46 = [
    ("P001", "2025-10-01", 3, 120.5, "Electronics"),
    ("P002", "2025-10-01", None, 450.0, "Appliances"),
    ("P003", "2025-10-02", 2, -50.0, "Toys"),
    ("P004", "2025-10-02", 1, 250.0, None),
    ("P001", "2025-10-03", 4, 130.0, "Electronics"),
    ("P005", "2025-10-03", None, None, "Books"),
    ("P006", "2025-10-03", 1, 2000.0, "Electronics"),
    ("P002", "2025-10-04", 5, 470.0, "Appliances"),
    ("P003", "2025-10-04", 2, 60.0, "Toys"),
    ("P004", "2025-10-04", 3, 0.0, "Clothing")
]

columns_day46 = ["product_id", "date", "quantity", "price", "category"]

df_day46 = spark.createDataFrame(data_day46, columns_day46)

# Question 1: Data Cleaning
# Calculate averages per category
avg_by_category = df_day46.filter(col("quantity").isNotNull() & col("price").isNotNull()) \
    .groupBy("category") \
    .agg(avg("quantity").alias("avg_quantity"), 
         avg("price").alias("avg_price"))

# Replace nulls with category averages and handle other cleaning
df_cleaned = df_day46 \
    .join(avg_by_category, "category", "left") \
    .withColumn("quantity", 
               when(col("quantity").isNull(), col("avg_quantity"))
               .otherwise(col("quantity"))) \
    .withColumn("price", 
               when(col("price").isNull(), col("avg_price"))
               .otherwise(col("price"))) \
    .withColumn("category", 
               when(col("category").isNull(), "Unknown")
               .otherwise(col("category"))) \
    .filter(col("price") > 0) \
    .drop("avg_quantity", "avg_price")

# Question 2: Outlier Detection
# Calculate IQR for price
price_stats = df_cleaned.select(
    percentile_approx("price", 0.25).alias("Q1"),
    percentile_approx("price", 0.75).alias("Q3")
).collect()[0]

Q1 = price_stats["Q1"]
Q3 = price_stats["Q3"]
IQR = Q3 - Q1
upper_bound = Q3 + 1.5 * IQR

df_no_outliers = df_cleaned.filter(col("price") <= upper_bound)

# Question 3: Analysis
# Total revenue per category
revenue_by_category = df_no_outliers.withColumn("revenue", col("quantity") * col("price")) \
    .groupBy("category") \
    .agg(sum("revenue").alias("total_revenue"))

# Top 3 categories by revenue
top_categories = revenue_by_category.orderBy(col("total_revenue").desc()).limit(3)

# Average daily revenue per category
avg_daily_revenue = df_no_outliers.withColumn("revenue", col("quantity") * col("price")) \
    .groupBy("category", "date") \
    .agg(sum("revenue").alias("daily_revenue")) \
    .groupBy("category") \
    .agg(avg("daily_revenue").alias("avg_daily_revenue"))

# Question 4: Revenue Band
df_revenue_band = df_no_outliers.withColumn("revenue", col("quantity") * col("price")) \
    .withColumn("revenue_band",
               when(col("revenue") < 500, "Low")
               .when((col("revenue") >= 500) & (col("revenue") < 1500), "Medium")
               .otherwise("High"))

# Display results
print("Question 1 - Cleaned Data:")
df_cleaned.show()

print("Question 2 - No Outliers:")
df_no_outliers.show()

print("Question 3 - Revenue by Category:")
revenue_by_category.show()

print("Question 3 - Top 3 Categories:")
top_categories.show()

print("Question 3 - Average Daily Revenue:")
avg_daily_revenue.show()

print("Question 4 - Revenue Band:")
df_revenue_band.show()

--------------------------------------------------------------------------------------------------------------------

%python
# Day 45 Solution
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Data and Columns
data_day45 = [
    ("T001", "2025-01-02", "S001", "P101", 3, 250, 0.1, "N"),
    ("T002", "2025-01-02", "S002", "P102", 2, 300, None, "N"),
    ("T003", "2025-01-03", "S001", "P101", 1, 250, 0.15, "Y"),
    ("T004", "2025-01-03", "S001", "P103", None, 150, 0.05, "N"),
    ("T004", "2025-01-03", "S001", "P103", None, 150, 0.05, "N"),
    ("T005", "2025-01-04", "S003", "P104", 5, 100, 0.2, "N"),
    ("T006", "2025-01-04", "S002", "P105", 2, 400, 0.1, "Y")
]

columns_day45 = ["TransactionID", "Date", "StoreID", "ProductID", "Quantity", "Price", "Discount", "Returned"]

df_day45 = spark.createDataFrame(data_day45, columns_day45)

# Question 2: Remove duplicates
df_deduped = df_day45.dropDuplicates(["TransactionID"])

# Question 3: Handle missing values
avg_quantity = df_deduped.agg(avg("Quantity")).collect()[0][0]
df_filled = df_deduped.fillna({
    "Quantity": avg_quantity,
    "Discount": 0.0
})

# Question 4: Calculate NetAmount
df_net = df_filled.withColumn("NetAmount", 
                            col("Quantity") * col("Price") * (1 - col("Discount")))

# Question 5: Exclude returned items
df_no_returns = df_net.filter(col("Returned") == "N")

# Question 6: Total revenue per StoreID
store_revenue = df_no_returns.groupBy("StoreID") \
    .agg(sum("NetAmount").alias("TotalRevenue"))

# Question 7: Product with highest total sales
top_product = df_no_returns.groupBy("ProductID") \
    .agg(sum("NetAmount").alias("TotalSales")) \
    .orderBy(col("TotalSales").desc()) \
    .first()

# Question 8: Top 3 stores by revenue
top_stores = store_revenue.orderBy(col("TotalRevenue").desc()).limit(3)

# Display results
print("Question 2 - Deduplicated Data:")
df_deduped.show()

print("Question 3 - Filled Missing Values:")
df_filled.show()

print("Question 4 - Net Amount:")
df_net.show()

print("Question 6 - Store Revenue:")
store_revenue.show()

print(f"Question 7 - Top Product: {top_product['ProductID']} with sales: {top_product['TotalSales']}")

print("Question 8 - Top 3 Stores:")
top_stores.show()

------------------------------------------------------------------------------------------------------------------------------------

%python

# Day 44 Solution
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Sales Data
sales_data = [
    (1, "North", "Store_X", "Laptop", "Electronics", "2025-10-01", 2, 60000, 0.10),
    (2, "North", "Store_X", "Mouse", "Electronics", "2025-10-01", 10, 800, 0.05),
    (3, "North", "Store_Y", "Shirt", "Apparel", "2025-10-02", 5, 1200, 0.20),
    (4, "South", "Store_Z", "TV", "Electronics", "2025-10-02", 3, 40000, 0.15),
    (5, "South", "Store_Z", "Mixer", "Appliances", "2025-10-02", 4, 3000, 0.10),
    (6, "West", "Store_W", "Jeans", "Apparel", "2025-10-03", 6, 2500, 0.25),
    (7, "West", "Store_W", "Watch", "Accessories", "2025-10-03", 2, 5000, 0.10),
    (8, "East", "Store_Q", "Mobile", "Electronics", "2025-10-03", 5, 20000, 0.05),
    (9, "East", "Store_Q", "Mobile", "Electronics", "2025-10-04", 3, 21000, 0.05),
    (10, "South", "Store_Z", "TV", "Electronics", "2025-10-04", 2, 38000, 0.20)
]

targets_data = [
    ("Store_X", 150000),
    ("Store_Y", 50000),
    ("Store_Z", 180000),
    ("Store_W", 100000),
    ("Store_Q", 160000)
]

cols_sales = ["sale_id", "region", "store", "product", "category", "date", "quantity", "price", "discount"]
cols_targets = ["store", "target_revenue"]

sales_df = spark.createDataFrame(sales_data, cols_sales)
targets_df = spark.createDataFrame(targets_data, cols_targets)

# Question 2: Compute Effective Revenue
df_effective = sales_df.withColumn("effective_revenue", 
                                 col("quantity") * col("price") * (1 - col("discount")))

# Question 3: Daily Store Revenue
daily_revenue = df_effective.groupBy("store", "date") \
    .agg(sum("effective_revenue").alias("daily_revenue"))

# Question 4: Cumulative Store Revenue
window_store = Window.partitionBy("store").orderBy("date")
cumulative_revenue = daily_revenue.withColumn("cumulative_revenue", 
                                            sum("daily_revenue").over(window_store))

# Question 5: Target Achievement %
latest_cumulative = cumulative_revenue.withColumn("rn", 
                                                row_number().over(Window.partitionBy("store").orderBy(col("date").desc()))) \
    .filter(col("rn") == 1) \
    .drop("rn")

target_achievement = latest_cumulative.join(targets_df, "store") \
    .withColumn("target_achievement", 
               (col("cumulative_revenue") / col("target_revenue")) * 100)

# Question 6: Rank Stores by Revenue
store_total_revenue = df_effective.groupBy("store") \
    .agg(sum("effective_revenue").alias("total_revenue"))

store_ranked = store_total_revenue.withColumn("rank", 
                                            dense_rank().over(Window.orderBy(col("total_revenue").desc())))

# Question 7: Detect Sales Drop Days
daily_revenue_with_lag = daily_revenue.withColumn("prev_daily_revenue", 
                                                lag("daily_revenue").over(Window.partitionBy("store").orderBy("date")))

sales_drop = daily_revenue_with_lag.withColumn("drop_pct", 
                                             ((col("prev_daily_revenue") - col("daily_revenue")) / col("prev_daily_revenue")) * 100) \
    .filter((col("drop_pct") > 30) & col("prev_daily_revenue").isNotNull())

# Display results
print("Question 2 - Effective Revenue:")
df_effective.show()

print("Question 3 - Daily Revenue:")
daily_revenue.show()

print("Question 4 - Cumulative Revenue:")
cumulative_revenue.show()

print("Question 5 - Target Achievement:")
target_achievement.show()

print("Question 6 - Store Ranking:")
store_ranked.show()

print("Question 7 - Sales Drop Days:")
sales_drop.show()

--------------------------------------------------------------------------------------------------------------------------------

%python

# Day 43 Solution
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Data and Columns
data_day43 = [
    (101, "Rahul Verma", "IT", 75000, "2019-04-15", 201, 5000, "Bangalore"),
    (102, "Sneha Patel", "HR", 52000, "2020-07-10", 203, 2000, "Mumbai"),
    (103, "Arjun Mehta", "Finance", 68000, "2018-02-20", 204, 3500, "Delhi"),
    (104, "Priya Singh", "IT", 80000, "2017-11-01", 201, 6000, "Hyderabad"),
    (105, "Mohan Das", "Finance", 45000, "2021-01-05", 204, 1500, "Bangalore"),
    (106, "Kavita Rao", "HR", 56000, "2022-05-19", 203, 2500, "Mumbai"),
    (107, "Rajesh Kumar", "IT", 70000, "2019-09-23", 201, 4000, "Chennai"),
    (108, "Neha Gupta", "Finance", 72000, "2018-06-14", 204, 5000, "Pune"),
    (109, "Deepak Yadav", "HR", 49000, "2023-02-01", 203, 1800, "Bangalore"),
    (110, "Anjali Nair", "IT", 65000, "2020-08-18", 201, 3000, "Mumbai")
]

columns_day43 = ["Emp_ID", "Name", "Department", "Salary", "Join_Date", "Manager_ID", "Bonus", "Location"]

df_day43 = spark.createDataFrame(data_day43, columns_day43)

# Question 1: Display schema
print("Schema:")
df_day43.printSchema()

# Question 2: Add Total Compensation
df_comp = df_day43.withColumn("Total_Compensation", col("Salary") + col("Bonus"))

# Question 3: Average salary and bonus per department
dept_avg = df_comp.groupBy("Department").agg(
    avg("Salary").alias("Avg_Salary"),
    avg("Bonus").alias("Avg_Bonus")
)

# Question 4: Employees joined before 2020 with salary > 70000
old_high_earners = df_comp.filter((year(col("Join_Date")) < 2020) & (col("Salary") > 70000))

# Question 5: Highest-paid employee in each department
window_dept = Window.partitionBy("Department").orderBy(col("Salary").desc())
highest_paid = df_comp.withColumn("rn", row_number().over(window_dept)) \
    .filter(col("rn") == 1) \
    .drop("rn")

# Question 6: Employees per location
employees_per_location = df_comp.groupBy("Location") \
    .agg(count("Emp_ID").alias("Employee_Count")) \
    .orderBy(col("Employee_Count").desc())

# Question 7: Experience and max experience per department
df_experience = df_comp.withColumn("Experience_Years", 2025 - year(col("Join_Date")))

window_dept_exp = Window.partitionBy("Department").orderBy(col("Experience_Years").desc())
max_exp_per_dept = df_experience.withColumn("rn", row_number().over(window_dept_exp)) \
    .filter(col("rn") == 1) \
    .drop("rn")

# Display results
print("Question 2 - Total Compensation:")
df_comp.show()

print("Question 3 - Department Averages:")
dept_avg.show()

print("Question 4 - Pre-2020 High Earners:")
old_high_earners.show()

print("Question 5 - Highest Paid per Department:")
highest_paid.show()

print("Question 6 - Employees per Location:")
employees_per_location.show()

print("Question 7 - Max Experience per Department:")
max_exp_per_dept.show()

------------------------------------------------------------------------------------------------------------------------

%python

# Day 42 Solution
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Data and Columns
data_day42 = [
    (1001, "P001", "Laptop", "Electronics", "North", 2, 55000, 0.05, "No", "2024-07-12"),
    (1002, "P002", "Headphones", "Electronics", "East", 5, 1500, 0.10, "No", "2024-07-13"),
    (1003, "P003", "Mixer Grinder", "Home Appliances", "South", 3, 4000, 0.00, "Yes", "2024-07-13"),
    (1004, "P004", "Office Chair", "Furniture", "West", 1, 7000, 0.15, "No", "2024-07-14"),
    (1005, "P005", "Smartphone", "Electronics", "North", 4, 30000, 0.05, "No", "2024-07-15"),
    (1006, "P006", "Air Cooler", "Home Appliances", "South", 2, 9000, 0.10, "No", "2024-07-15"),
    (1007, "P007", "Table Lamp", "Furniture", "East", 6, 1200, 0.05, "No", "2024-07-16"),
    (1008, "P008", "Smartwatch", "Electronics", "West", 3, 7000, 0.00, "Yes", "2024-07-16"),
    (1009, "P009", "Microwave", "Home Appliances", "North", 1, 10000, 0.05, "No", "2024-07-17"),
    (1010, "P010", "Sofa Set", "Furniture", "East", 1, 30000, 0.10, "No", "2024-07-17")
]

columns_day42 = ["TransactionID", "ProductID", "ProductName", "Category", "Region",
                 "Quantity", "UnitPrice", "Discount", "ReturnFlag", "Date"]

df_day42 = spark.createDataFrame(data_day42, columns_day42)

# Question 1: Load and Inspect
print("Schema:")
df_day42.printSchema()
print("First 5 rows:")
df_day42.show(5)

# Question 2: Data Cleaning
df_clean = df_day42.dropDuplicates().fillna(0, subset=["Discount"])

# Question 3: Derived Columns
df_derived = df_clean \
    .withColumn("TotalAmount", col("Quantity") * col("UnitPrice") * (1 - col("Discount"))) \
    .withColumn("Returned", when(col("ReturnFlag") == "Yes", 1).otherwise(0))

# Question 4: Aggregations
# Total revenue by Region
revenue_by_region = df_derived.groupBy("Region").agg(
    sum("TotalAmount").alias("TotalRevenue")
)

# Average discount and total quantity by Category
category_stats = df_derived.groupBy("Category").agg(
    avg("Discount").alias("AvgDiscount"),
    sum("Quantity").alias("TotalQuantity")
)

# Top 3 products by total sales amount
top_products = df_derived.groupBy("ProductID", "ProductName").agg(
    sum("TotalAmount").alias("TotalSales")
).orderBy(col("TotalSales").desc()).limit(3)

# Question 5: Regional Insights
regional_insights = df_derived.groupBy("Region").agg(
    count("TransactionID").alias("TransactionCount"),
    sum("TotalAmount").alias("TotalRevenue"),
    (sum("Returned") / count("TransactionID") * 100).alias("ReturnPercentage")
)

# Question 6: Bonus Questions
# Most profitable category
most_profitable_category = df_derived.groupBy("Category").agg(
    sum("TotalAmount").alias("TotalRevenue")
).orderBy(col("TotalRevenue").desc()).first()

# Day with highest sales
day_highest_sales = df_derived.groupBy("Date").agg(
    sum("TotalAmount").alias("DailySales")
).orderBy(col("DailySales").desc()).first()

# Display results
print("Question 2 - Cleaned Data:")
df_clean.show()

print("Question 3 - Derived Columns:")
df_derived.show()

print("Question 4 - Revenue by Region:")
revenue_by_region.show()

print("Question 4 - Category Stats:")
category_stats.show()

print("Question 4 - Top 3 Products:")
top_products.show()

print("Question 5 - Regional Insights:")
regional_insights.show()

print(f"Question 6 - Most Profitable Category: {most_profitable_category['Category']} with revenue: {most_profitable_category['TotalRevenue']}")
print(f"Question 6 - Day with Highest Sales: {day_highest_sales['Date']} with sales: {day_highest_sales['DailySales']}")

-------------------------------------------------------------------------------------------------------------------------------------------------

%python
# Day 41 Solution
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Master Data
data_master = [
    (101, "Ananya Rao", "HR", 50000, "2020-02-01", "Bengaluru"),
    (102, "Ravi Patel", "IT", 75000, "2019-06-15", "Hyderabad"),
    (103, "Meera Nair", "Finance", 67000, "2021-04-20", "Mumbai"),
    (104, "Rajesh Kumar", "IT", 72000, "2020-11-10", "Chennai"),
    (105, "Priya Singh", "Marketing", 58000, "2018-12-01", "Delhi"),
    (106, "Arjun Mehta", "Finance", 69000, "2022-05-25", "Mumbai")
]
columns_master = ["Emp_ID", "Name", "Department", "Salary", "Join_Date", "City"]

df_master = spark.createDataFrame(data_master, columns_master)

# Performance Data
data_perf = [
    (101, "Jan", 2024, 85, 3000),
    (101, "Feb", 2024, 90, 4000),
    (102, "Jan", 2024, 78, 3500),
    (102, "Feb", 2024, 81, 3800),
    (103, "Jan", 2024, 92, 4200),
    (104, "Jan", 2024, 88, 3900),
    (105, "Jan", 2024, 75, 2500),
    (106, "Feb", 2024, 89, 4100)
]
columns_perf = ["Emp_ID", "Month", "Year", "Performance_Score", "Bonus"]

df_perf = spark.createDataFrame(data_perf, columns_perf)

# Question 2: Join datasets
df_joined = df_master.join(df_perf, "Emp_ID", "inner")

# Question 3: Calculate Total Earnings
df_earnings = df_joined.withColumn("Total_Earnings", col("Salary") + col("Bonus"))

# Question 4: Average performance score per department
dept_avg_performance = df_joined.groupBy("Department").agg(
    avg("Performance_Score").alias("Avg_Performance_Score")
)

# Question 5: Top-performing employee in each department
emp_avg_performance = df_joined.groupBy("Emp_ID", "Name", "Department").agg(
    avg("Performance_Score").alias("Avg_Performance_Score")
)

window_dept = Window.partitionBy("Department").orderBy(col("Avg_Performance_Score").desc())
top_performers = emp_avg_performance.withColumn("rn", row_number().over(window_dept)) \
    .filter(col("rn") == 1) \
    .drop("rn")

# Question 6: Employees joined after 2020 with avg performance > 85
high_performers_recent = emp_avg_performance.join(
    df_master.select("Emp_ID", "Join_Date"), "Emp_ID"
).filter(
    (year(col("Join_Date")) > 2020) & (col("Avg_Performance_Score") > 85)
)

# Question 7: Total bonuses distributed per city
bonus_by_city = df_joined.groupBy("City").agg(
    sum("Bonus").alias("Total_Bonus")
)

# Additional Questions:
# 1. Performance Level
df_perf_level = df_joined.withColumn("Performance_Level",
    when(col("Performance_Score") >= 90, "Excellent")
    .when((col("Performance_Score") >= 80) & (col("Performance_Score") < 90), "Good")
    .otherwise("Needs Improvement")
)

# 2. Count of employees in each performance level
performance_level_count = df_perf_level.groupBy("Performance_Level").agg(
    countDistinct("Emp_ID").alias("Employee_Count")
)

# Display results
print("Question 2 - Joined Data:")
df_joined.show()

print("Question 3 - Total Earnings:")
df_earnings.show()

print("Question 4 - Department Average Performance:")
dept_avg_performance.show()

print("Question 5 - Top Performers per Department:")
top_performers.show()

print("Question 6 - High Performers Joined After 2020:")
high_performers_recent.show()

print("Question 7 - Bonus by City:")
bonus_by_city.show()

print("Additional - Performance Level Count:")
performance_level_count.show()