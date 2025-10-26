%python

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("Day20").getOrCreate()

# Employees data
employees = [
    (1, "Alice",   60000, "2020-01-15", 101),
    (2, "Bob",     55000, "2019-03-20", 102),
    (3, "Charlie", 70000, "2021-07-10", 101),
    (4, "David",   80000, "2018-11-05", 103),
    (5, "Eva",     75000, "2020-05-25", 102),
    (6, "Frank",   72000, "2022-02-14", 101),
    (7, "Grace",   50000, "2021-09-01", 103),
    (8, "Henry",   65000, "2019-12-12", 102),
]

employees_columns = ["emp_id", "emp_name", "salary", "hire_date", "dept_id"]

# Departments data
departments = [
    (101, "IT"),
    (102, "HR"),
    (103, "Finance"),
]
dept_columns = ["dept_id", "dept_name"]

# Create DataFrames
employees_df = spark.createDataFrame(employees, employees_columns)
departments_df = spark.createDataFrame(departments, dept_columns)

# Join DataFrames
df_joined = employees_df.join(departments_df, "dept_id")

# 1. Average Salary per Department
avg_salary_per_dept = df_joined.groupBy("dept_name").agg(
    avg("salary").alias("avg_salary")
)

# 2. Highest Paid Employee per Department
window_dept = Window.partitionBy("dept_name").orderBy(col("salary").desc())
highest_paid_per_dept = df_joined.withColumn("rn", row_number().over(window_dept)) \
    .filter(col("rn") == 1) \
    .select("dept_name", "emp_name", "salary")

# 3. Employees Hired After 2020
employees_after_2020 = df_joined.filter(year(col("hire_date")) > 2020)

# 4. Department with the Highest Total Salary
dept_total_salary = df_joined.groupBy("dept_name").agg(
    sum("salary").alias("total_salary")
).orderBy(col("total_salary").desc()).limit(1)

# 5. Rank Employees by Salary within Department
employees_ranked = df_joined.withColumn(
    "salary_rank", 
    rank().over(Window.partitionBy("dept_name").orderBy(col("salary").desc()))
)

# Display results
print("1. Average Salary per Department:")
avg_salary_per_dept.show()

print("2. Highest Paid Employee per Department:")
highest_paid_per_dept.show()

print("3. Employees Hired After 2020:")
employees_after_2020.show()

print("4. Department with Highest Total Salary:")
dept_total_salary.show()

print("5. Employees Ranked by Salary within Department:")
employees_ranked.select("dept_name", "emp_name", "salary", "salary_rank").show()

------------------------------------------------------------------------------------------------

%python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Data
data_day19 = [
    (1001, "Alice",   "Laptop", 75000, "2024-01-15"),
    (1002, "Bob",     "Mobile", 25000, "2024-01-18"),
    (1003, "Alice",   "Tablet", 30000, "2024-02-02"),
    (1004, "Charlie", "Laptop", 80000, "2024-02-15"),
    (1005, "Bob",     "Tablet", 28000, "2024-03-05"),
    (1006, "Alice",   "Mobile", 22000, "2024-03-10"),
    (1007, "David",   "Laptop", 90000, "2024-03-12"),
    (1008, "Alice",   "Laptop", 76000, "2024-04-01"),
]

columns_day19 = ["order_id", "customer", "product", "amount", "order_dt"]

df_day19 = spark.createDataFrame(data_day19, columns_day19)

# Q1. Monthly Sales Summary
monthly_sales = df_day19 \
    .withColumn("month", date_format(col("order_dt"), "yyyy-MM")) \
    .groupBy("month").agg(
        sum("amount").alias("total_sales")
    ).orderBy("month")

# Q2. Top Customer per Month
window_month = Window.partitionBy("month").orderBy(col("total_amount").desc())
top_customer_per_month = df_day19 \
    .withColumn("month", date_format(col("order_dt"), "yyyy-MM")) \
    .groupBy("month", "customer").agg(
        sum("amount").alias("total_amount")
    ).withColumn("rn", row_number().over(window_month)) \
    .filter(col("rn") == 1) \
    .select("month", "customer", "total_amount")

# Q3. Running Total of Each Customer
window_customer = Window.partitionBy("customer").orderBy("order_dt")
running_total = df_day19.withColumn(
    "running_total", 
    sum("amount").over(window_customer)
).select("customer", "order_dt", "amount", "running_total")

# Q4. Product Popularity Rank
product_rank = df_day19.groupBy("product").agg(
    sum("amount").alias("total_sales")
).withColumn("rank", rank().over(Window.orderBy(col("total_sales").desc())))

# Q5. High-Value Orders
avg_order_value = df_day19.agg(avg("amount")).collect()[0][0]
high_value_orders = df_day19.filter(col("amount") > avg_order_value)

# Display results
print("Q1. Monthly Sales Summary:")
monthly_sales.show()

print("Q2. Top Customer per Month:")
top_customer_per_month.show()

print("Q3. Running Total per Customer:")
running_total.show()

print("Q4. Product Popularity Rank:")
product_rank.show()

print("Q5. High-Value Orders:")
high_value_orders.show()

------------------------------------------------------------------------------------------
%python

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Data
data_day18 = [
    (1, 101, "P01", "Electronics", 1, 500, "2025-01-10"),
    (2, 102, "P02", "Grocery", 5, 20, "2025-01-11"),
    (3, 101, "P03", "Grocery", 2, 15, "2025-01-15"),
    (4, 103, "P01", "Electronics", 1, 500, "2025-01-20"),
    (5, 104, "P04", "Clothing", 3, 50, "2025-01-22"),
    (6, 101, "P05", "Clothing", 2, 40, "2025-02-05"),
    (7, 102, "P03", "Grocery", 1, 15, "2025-02-08"),
    (8, 104, "P06", "Electronics", 1, 700, "2025-02-15"),
    (9, 105, "P02", "Grocery", 10, 20, "2025-02-18"),
    (10, 101, "P07", "Grocery", 7, 25, "2025-02-20"),
]

columns_day18 = ["transaction_id", "customer_id", "product_id", "category", "quantity", "price", "transaction_date"]

df_day18 = spark.createDataFrame(data_day18, columns_day18)

# 1) Total spend per customer
customer_spend = df_day18.withColumn(
    "amount", col("quantity") * col("price")
).groupBy("customer_id").agg(
    sum("amount").alias("total_spend")
)

# 2) Top category per customer
window_customer = Window.partitionBy("customer_id").orderBy(col("category_spend").desc())
top_category_per_customer = df_day18.withColumn(
    "amount", col("quantity") * col("price")
).groupBy("customer_id", "category").agg(
    sum("amount").alias("category_spend")
).withColumn("rn", row_number().over(window_customer)) \
 .filter(col("rn") == 1) \
 .select("customer_id", "category", "category_spend")

# 3) Monthly revenue trend
monthly_revenue = df_day18 \
    .withColumn("amount", col("quantity") * col("price")) \
    .withColumn("month", date_format(col("transaction_date"), "yyyy-MM")) \
    .groupBy("month").agg(
        sum("amount").alias("monthly_revenue")
    ).orderBy("month")

# 4) Most loyal customer (most transactions)
most_loyal_customer = df_day18.groupBy("customer_id").agg(
    count("transaction_id").alias("transaction_count")
).orderBy(col("transaction_count").desc()).limit(1)

# 5) High-value transactions
high_value_transactions = df_day18.filter(
    col("quantity") * col("price") > 200
)

# Display results
print("1. Total Spend per Customer:")
customer_spend.show()

print("2. Top Category per Customer:")
top_category_per_customer.show()

print("3. Monthly Revenue Trend:")
monthly_revenue.show()

print("4. Most Loyal Customer:")
most_loyal_customer.show()

print("5. High-Value Transactions:")
high_value_transactions.show()

----------------------------------------------------------------------------------------

%python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Data
data_day17 = [
    ("North", "2025-01-01", 100),
    ("North", "2025-01-02", 200),
    ("North", "2025-01-03", 50),
    ("South", "2025-01-01", 400),
    ("South", "2025-01-02", 300),
    ("South", "2025-01-03", 100)
]
columns_day17 = ["Region", "Date", "Sales"]

df_day17 = spark.createDataFrame(data_day17, columns_day17)

# Running Total of Sales per Region
window_region = Window.partitionBy("Region").orderBy("Date")
running_total = df_day17.withColumn(
    "RunningTotal", 
    sum("Sales").over(window_region)
)

# Display results
print("Running Total of Sales per Region:")
running_total.show()

---------------------------------------------------------------------------------------

%python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Customers Data
customers = [
    (1, "Alice", "New York"),
    (2, "Bob", "Chicago"),
    (3, "Charlie", "Houston"),
    (4, "Daisy", "New York"),
    (5, "Eve", "Chicago")
]
customers_df = spark.createDataFrame(customers, ["customer_id", "first_name", "city"])

# Orders Data
orders = [
    (101, 1, 250, "2023-01-10"),
    (102, 1, 450, "2023-02-14"),
    (103, 2, 300, "2023-01-12"),
    (104, 2, 150, "2023-03-01"),
    (105, 3, 500, "2023-01-20"),
    (106, 4, 700, "2023-02-25"),
    (107, 4, 200, "2023-03-05"),
    (108, 5, 100, "2023-01-15")
]
orders_df = spark.createDataFrame(orders, ["order_id", "customer_id", "amount", "order_date"])

# 1. Join datasets
df_joined = orders_df.join(customers_df, "customer_id")

# 2. Total amount spent per customer
customer_total_spend = df_joined.groupBy("customer_id", "first_name").agg(
    sum("amount").alias("total_spent")
)

# 3. Highest order amount per city
max_order_per_city = df_joined.groupBy("city").agg(
    max("amount").alias("highest_order_amount")
)

# 4. Customer's rank of orders based on amount
window_customer = Window.partitionBy("customer_id").orderBy(col("amount").desc())
orders_ranked = df_joined.withColumn(
    "order_rank", 
    rank().over(window_customer)
).select("customer_id", "first_name", "order_id", "amount", "order_rank")

# Display results
print("1. Joined Data:")
df_joined.show()

print("2. Total Amount Spent per Customer:")
customer_total_spend.show()

print("3. Highest Order Amount per City:")
max_order_per_city.show()

print("4. Orders Ranked by Amount per Customer:")
orders_ranked.show()

----------------------------------------------------------------------------------------

%python

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Customers Data
customers = [
    (1, "Ramesh", "Hyderabad"),
    (2, "Sita", "Bangalore"),
    (3, "Arjun", "Chennai"),
    (4, "Priya", "Mumbai")
]
customers_df = spark.createDataFrame(customers, ["customer_id", "name", "city"])

# Transactions Data
transactions = [
    (101, 1, 500, "2025-01-01"),
    (102, 2, 1000, "2025-01-05"),
    (103, 1, 200, "2025-02-10"),
    (104, 3, 1500, "2025-02-15"),
    (105, 2, 700, "2025-03-05"),
    (106, 4, 1200, "2025-03-10"),
    (107, 3, 600, "2025-03-15"),
]
transactions_df = spark.createDataFrame(transactions, ["txn_id", "customer_id", "amount", "txn_date"])

# 1. Load datasets (already done)

# 2. Join customers with transactions
df_joined = transactions_df.join(customers_df, "customer_id")

# 3. Total transaction amount per customer
customer_total = df_joined.groupBy("customer_id", "name").agg(
    sum("amount").alias("total_amount")
)

# 4. Highest single transaction for each customer
window_customer = Window.partitionBy("customer_id").orderBy(col("amount").desc())
highest_transaction_per_customer = df_joined.withColumn("rn", row_number().over(window_customer)) \
    .filter(col("rn") == 1) \
    .select("customer_id", "name", "amount", "txn_date")

# 5. City with highest overall transaction amount
city_total = df_joined.groupBy("city").agg(
    sum("amount").alias("total_amount")
).orderBy(col("total_amount").desc()).limit(1)

# 6. Monthly transaction totals
monthly_totals = df_joined \
    .withColumn("month", date_format(col("txn_date"), "yyyy-MM")) \
    .groupBy("month").agg(
        sum("amount").alias("monthly_total")
    ).orderBy("month")

# Display results
print("2. Joined Data:")
df_joined.show()

print("3. Total Transaction Amount per Customer:")
customer_total.show()

print("4. Highest Single Transaction per Customer:")
highest_transaction_per_customer.show()

print("5. City with Highest Overall Transaction Amount:")
city_total.show()

print("6. Monthly Transaction Totals:")
monthly_totals.show()

--------------------------------------------------------------------------------------------

%python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Data
data_day14 = [
    (1, "John", 30, "New York"),
    (2, "Alice", 25, None),
    (3, "Mark", None, "London"),
    (4, "Sara", 28, None),
]
df_day14 = spark.createDataFrame(data_day14, ["id", "name", "age", "city"])

# Calculate average age
avg_age = df_day14.agg(avg("age")).collect()[0][0]

# Replace missing values
df_filled = df_day14 \
    .fillna("Unknown", subset=["city"]) \
    .fillna(avg_age, subset=["age"])

# Display results
print("Original Data:")
df_day14.show()

print("After Handling Missing Values:")
df_filled.show()

-------------------------------------------------------------------------------------

%python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Data
data_day13 = [
    (1, "IT", 60000),
    (2, "IT", 75000),
    (3, "IT", 50000),
    (4, "HR", 45000),
    (5, "HR", 55000),
    (6, "HR", 40000),
    (7, "Sales", 70000),
    (8, "Sales", 72000),
    (9, "Sales", 65000),
]
columns_day13 = ["emp_id", "dept", "salary"]

df_day13 = spark.createDataFrame(data_day13, columns_day13)

# 1. Rank employees within each department by salary
window_dept = Window.partitionBy("dept").orderBy(col("salary").desc())
df_ranked = df_day13.withColumn("rank", rank().over(window_dept))

# 2. Top 2 highest-paid employees from each department
top_2_per_dept = df_ranked.filter(col("rank") <= 2)

# 3. Difference from department average salary
window_dept_avg = Window.partitionBy("dept")
df_with_avg_diff = df_day13.withColumn("dept_avg_salary", avg("salary").over(window_dept_avg)) \
    .withColumn("salary_diff", col("salary") - col("dept_avg_salary"))

# Display results
print("1. Employees Ranked by Salary within Department:")
df_ranked.show()

print("2. Top 2 Highest-Paid Employees per Department:")
top_2_per_dept.show()

print("3. Salary Difference from Department Average:")
df_with_avg_diff.show()

----------------------------------------------------------------------------------------

%python

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Sentences data
sentences = [
    "spark makes big data processing easy",
    "pyspark is built on top of spark",
    "big data is the future"
]

# Create DataFrame
df_sentences = spark.createDataFrame([(sentence,) for sentence in sentences], ["sentence"])

# Word count implementation
word_counts = df_sentences \
    .select(explode(split(col("sentence"), " ")).alias("word")) \
    .groupBy("word") \
    .agg(count("*").alias("count")) \
    .orderBy(col("count").desc())

# Display results
print("Word Count Results:")
word_counts.show()

--------------------------------------------------------------------------------------

%python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Data
data_day11 = [
    (101, "John Smith", "Sales", 50000, "2020-01-15", "john.smith@company.com"),
    (102, "Alice Johnson", None, 60000, "2019-03-10", "alice.johnson@company.com"),
    (103, "Robert Brown", "IT", None, "2021-07-23", "robert.brown@company.com"),
    (104, None, "HR", 45000, "2022-05-19", "hr.team@company.com"),
    (105, "Charlie Williams", "IT", 70000, None, "charlie.w@company.com"),
    (106, "Emma Davis", "Sales", 55000, "2021-11-11", "emmadavis@company.com")
]

columns_day11 = ["emp_id", "name", "dept", "salary", "joining_date", "email"]

df_day11 = spark.createDataFrame(data_day11, columns_day11)

# 1. Handle Missing Data
# Calculate average salary
avg_salary = df_day11.agg(avg("salary")).collect()[0][0]

df_filled = df_day11 \
    .fillna("Unknown", subset=["dept"]) \
    .fillna(avg_salary, subset=["salary"]) \
    .fillna("2020-01-01", subset=["joining_date"]) \
    .fillna("No Name", subset=["name"])

# 2. Transform Columns
df_transformed = df_filled \
    .withColumn("email_domain", regexp_extract(col("email"), "@(.+)", 1)) \
    .withColumn("experience_years", 2025 - year(col("joining_date")))

# 3. Analysis
# Average salary per department
avg_salary_per_dept = df_transformed.groupBy("dept").agg(
    avg("salary").alias("avg_salary")
)

# Top 3 highest paid employees
top_3_paid = df_transformed.orderBy(col("salary").desc()).limit(3)

# Count employees per department
employees_per_dept = df_transformed.groupBy("dept").agg(
    count("emp_id").alias("employee_count")
)

# Display results
print("1. After Handling Missing Data:")
df_filled.show()

print("2. After Transformations:")
df_transformed.show()

print("3. Average Salary per Department:")
avg_salary_per_dept.show()

print("3. Top 3 Highest Paid Employees:")
top_3_paid.show()

print("3. Employees per Department:")
employees_per_dept.show()