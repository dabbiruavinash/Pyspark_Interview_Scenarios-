%python

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("Day10").getOrCreate()

# Data
data_day10 = [
    (1, "John", "Sales", 5000, "2020-01-15"),
    (2, "Alice", "Sales", 6000, "2019-03-20"),
    (3, "Bob", "HR", 4500, "2021-07-30"),
    (4, "Carol", "HR", 7000, "2018-11-10"),
    (5, "David", "IT", 8000, "2019-09-25"),
    (6, "Eva", "IT", 7500, "2020-02-17"),
    (7, "Frank", "Sales", 6500, "2021-05-05"),
    (8, "Grace", "IT", 9000, "2017-06-12")
]

columns_day10 = ["emp_id", "emp_name", "dept", "salary", "hire_date"]

df_day10 = spark.createDataFrame(data_day10, columns_day10)

# 1. Find the highest-paid employee in each department
window_dept_salary = Window.partitionBy("dept").orderBy(col("salary").desc())
highest_paid_per_dept = df_day10.withColumn("rn", row_number().over(window_dept_salary)) \
    .filter(col("rn") == 1) \
    .select("dept", "emp_name", "salary")

# 2. Rank employees within each department based on salary
df_ranked = df_day10.withColumn(
    "salary_rank", 
    rank().over(Window.partitionBy("dept").orderBy(col("salary").desc()))
)

# 3. Find average salary per department and assign to each employee
df_with_avg = df_day10.withColumn(
    "dept_avg_salary", 
    avg("salary").over(Window.partitionBy("dept"))
)

# 4. Find the earliest hired employee in each department
window_dept_hire = Window.partitionBy("dept").orderBy("hire_date")
earliest_hired_per_dept = df_day10.withColumn("rn", row_number().over(window_dept_hire)) \
    .filter(col("rn") == 1) \
    .select("dept", "emp_name", "hire_date")

# 5. Show salary difference compared to highest salary in department
df_with_diff = df_day10.withColumn(
    "max_salary_in_dept", 
    max("salary").over(Window.partitionBy("dept"))
).withColumn(
    "salary_diff_from_max", 
    col("max_salary_in_dept") - col("salary")
)

# Display results
print("1. Highest Paid Employee in Each Department:")
highest_paid_per_dept.show()

print("2. Employees Ranked by Salary within Department:")
df_ranked.select("dept", "emp_name", "salary", "salary_rank").show()

print("3. Employees with Department Average Salary:")
df_with_avg.select("dept", "emp_name", "salary", "dept_avg_salary").show()

print("4. Earliest Hired Employee in Each Department:")
earliest_hired_per_dept.show()

print("5. Salary Difference from Department Maximum:")
df_with_diff.select("dept", "emp_name", "salary", "salary_diff_from_max").show()

------------------------------------------------------------------------------------------------

%python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Data
data_day9 = [
    (1, "C001", "Laptop", "Electronics", 75000, "2024-01-10"),
    (2, "C002", "Mouse", "Electronics", 1200, "2024-01-11"),
    (3, "C001", "Keyboard", "Electronics", 3500, "2024-02-05"),
    (4, "C003", "Chair", "Furniture", 4500, "2024-02-20"),
    (5, "C002", "Table", "Furniture", 15000, "2024-03-02"),
    (6, "C003", "Phone", "Electronics", 30000, "2024-03-18"),
    (7, "C001", "Sofa", "Furniture", 22000, "2024-04-01"),
    (8, "C004", "TV", "Electronics", 40000, "2024-04-15"),
]

columns_day9 = ["transaction_id", "customer_id", "product", "category", "amount", "transaction_date"]

df_day9 = spark.createDataFrame(data_day9, columns_day9)

# 1. Total Spend per Customer
total_spend_per_customer = df_day9.groupBy("customer_id").agg(
    sum("amount").alias("total_spend")
)

# 2. Most Expensive Purchase per Customer
max_purchase_per_customer = df_day9.groupBy("customer_id").agg(
    max("amount").alias("max_purchase_amount")
)

# 3. Top Category per Customer
window_customer_category = Window.partitionBy("customer_id").orderBy(col("category_spend").desc())
top_category_per_customer = df_day9.groupBy("customer_id", "category").agg(
    sum("amount").alias("category_spend")
).withColumn("rn", row_number().over(window_customer_category)) \
 .filter(col("rn") == 1) \
 .select("customer_id", "category", "category_spend")

# 4. Monthly Revenue
monthly_revenue = df_day9 \
    .withColumn("month", date_format(col("transaction_date"), "yyyy-MM")) \
    .groupBy("month").agg(
        sum("amount").alias("monthly_revenue")
    ).orderBy("month")

# 5. Top 2 Customers by Spending
top_2_customers = total_spend_per_customer.orderBy(col("total_spend").desc()).limit(2)

# Display results
print("1. Total Spend per Customer:")
total_spend_per_customer.show()

print("2. Most Expensive Purchase per Customer:")
max_purchase_per_customer.show()

print("3. Top Category per Customer:")
top_category_per_customer.show()

print("4. Monthly Revenue:")
monthly_revenue.show()

print("5. Top 2 Customers by Spending:")
top_2_customers.show()

------------------------------------------------------------------------------------

%python

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Data
data_day8 = [
    ("T001", "C01", 250, "Grocery", "2025-08-01", "Delhi"),
    ("T002", "C02", 1200, "Electronics", "2025-08-02", "Mumbai"),
    ("T003", "C01", 450, "Clothing", "2025-08-03", "Delhi"),
    ("T004", "C03", 800, "Grocery", "2025-08-04", "Chennai"),
    ("T005", "C02", 600, "Clothing", "2025-08-05", "Mumbai"),
    ("T006", "C04", 200, "Grocery", "2025-08-06", "Delhi"),
    ("T007", "C01", 1500, "Electronics", "2025-08-07", "Bangalore"),
    ("T008", "C03", 300, "Clothing", "2025-08-08", "Chennai"),
    ("T009", "C04", 750, "Electronics", "2025-08-09", "Delhi"),
    ("T010", "C02", 100, "Grocery", "2025-08-10", "Mumbai"),
]

columns_day8 = ["txn_id", "cust_id", "amount", "category", "txn_date", "city"]

df_day8 = spark.createDataFrame(data_day8, columns_day8)

# 1. Total spending per customer
total_spending_per_customer = df_day8.groupBy("cust_id").agg(
    sum("amount").alias("total_spent")
)

# 2. Most popular category by sales
most_popular_category = df_day8.groupBy("category").agg(
    sum("amount").alias("total_sales")
).orderBy(col("total_sales").desc()).limit(1)

# 3. Highest single transaction per city
highest_transaction_per_city = df_day8.groupBy("city").agg(
    max("amount").alias("highest_transaction")
)

# 4. Customer with maximum transactions
customer_max_transactions = df_day8.groupBy("cust_id").agg(
    count("txn_id").alias("transaction_count")
).orderBy(col("transaction_count").desc()).limit(1)

# 5. Running total of spending per customer (date-wise)
window_customer_date = Window.partitionBy("cust_id").orderBy("txn_date")
running_total = df_day8.withColumn(
    "running_total", 
    sum("amount").over(window_customer_date)
).select("cust_id", "txn_date", "amount", "running_total")

# Display results
print("1. Total Spending per Customer:")
total_spending_per_customer.show()

print("2. Most Popular Category by Sales:")
most_popular_category.show()

print("3. Highest Single Transaction per City:")
highest_transaction_per_city.show()

print("4. Customer with Maximum Transactions:")
customer_max_transactions.show()

print("5. Running Total per Customer:")
running_total.show()

---------------------------------------------------------------------------------------

%python

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Orders Data
orders_data = [
    (101, "john smith"),
    (102, "ALICE JONES"),
    (103, "BoB Brown"),
    (104, "miCHAel claRk"),
]
orders_cols = ["order_id", "customer_name"]

df_orders = spark.createDataFrame(orders_data, orders_cols)

# Clean customer names (Title Case)
df_cleaned = df_orders.withColumn(
    "customer_name_clean", 
    initcap(col("customer_name"))
)

# Display results
print("Original Data:")
df_orders.show()

print("Cleaned Data:")
df_cleaned.show()

-------------------------------------------------------------------------------------------

%python

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# For CSV data, we'll create the DataFrame directly
employee_data = [
    (1, "John", "Sales", 50000, 5000, "2020-01-10"),
    (2, "Sarah", "HR", 60000, 6000, "2019-03-15"),
    (3, "David", "IT", 75000, 7000, "2021-07-23"),
    (4, "Emma", "Finance", 80000, 10000, "2020-11-05"),
    (5, "Michael", "Sales", 55000, 4500, "2021-05-19"),
    (6, "Linda", "IT", 72000, 6500, "2022-02-14"),
    (7, "James", "Finance", 90000, 12000, "2018-09-10"),
    (8, "Sophia", "HR", 58000, 5200, "2021-08-21"),
    (9, "Robert", "Sales", 62000, 4000, "2019-12-01"),
    (10, "Olivia", "IT", 70000, 6000, "2020-06-25")
]

columns_day6 = ["emp_id", "emp_name", "dept", "salary", "bonus", "joining_date"]

df_day6 = spark.createDataFrame(employee_data, columns_day6)

# 1. Find the average salary per department
avg_salary_per_dept = df_day6.groupBy("dept").agg(
    avg("salary").alias("avg_salary")
)

# 2. Find the employee with the highest bonus in each department
window_dept_bonus = Window.partitionBy("dept").orderBy(col("bonus").desc())
highest_bonus_per_dept = df_day6.withColumn("rn", row_number().over(window_dept_bonus)) \
    .filter(col("rn") == 1) \
    .select("dept", "emp_name", "bonus")

# 3. Calculate total compensation and rank within department
df_with_compensation = df_day6.withColumn(
    "total_compensation", 
    col("salary") + col("bonus")
)

df_ranked = df_with_compensation.withColumn(
    "dept_rank", 
    rank().over(Window.partitionBy("dept").orderBy(col("total_compensation").desc()))
)

# Display results
print("1. Average Salary per Department:")
avg_salary_per_dept.show()

print("2. Employee with Highest Bonus per Department:")
highest_bonus_per_dept.show()

print("3. Employees with Total Compensation and Department Rank:")
df_ranked.select("dept", "emp_name", "salary", "bonus", "total_compensation", "dept_rank").show()

-------------------------------------------------------------------------------------

%python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Data
data_day5 = [
    (101, "John", "IT", 70000, 85, 5000),
    (102, "Alice", "HR", 55000, 78, None),
    (103, "Bob", "IT", 60000, 90, 4000),
    (104, "Carol", "Finance", 80000, 92, 7000),
    (105, "David", "IT", 65000, 70, None),
    (106, "Eva", "Finance", 75000, 88, 6000),
    (107, "Frank", "HR", 50000, 75, 3000),
    (108, "Grace", "IT", 72000, 95, 8000),
    (109, "Helen", "Finance", 78000, 89, 6500),
    (110, "Ian", "HR", 58000, 82, None)
]

columns_day5 = ["emp_id", "emp_name", "dept", "salary", "performance_score", "bonus"]

df_day5 = spark.createDataFrame(data_day5, columns_day5)

# 1. Read data (already done)

# 2. Fill missing bonus values with department average
dept_avg_bonus = df_day5.filter(col("bonus").isNotNull()).groupBy("dept").agg(
    avg("bonus").alias("dept_avg_bonus")
)

df_filled = df_day5.join(dept_avg_bonus, "dept", "left") \
    .withColumn("bonus", 
               when(col("bonus").isNull(), col("dept_avg_bonus"))
               .otherwise(col("bonus"))) \
    .drop("dept_avg_bonus")

# 3. Top 2 employees per department based on performance score
window_dept_performance = Window.partitionBy("dept").orderBy(col("performance_score").desc())
top_2_per_dept = df_filled.withColumn("rn", row_number().over(window_dept_performance)) \
    .filter(col("rn") <= 2) \
    .select("dept", "emp_name", "performance_score", "rn")

# 4. Average salary and performance_score per department
dept_avg_stats = df_filled.groupBy("dept").agg(
    avg("salary").alias("avg_salary"),
    avg("performance_score").alias("avg_performance_score")
)

# 5. Rank employees within each department by salary
df_salary_ranked = df_filled.withColumn(
    "salary_rank", 
    rank().over(Window.partitionBy("dept").orderBy(col("salary").desc()))
)

# 6. Create total_compensation column
df_with_compensation = df_filled.withColumn(
    "total_compensation", 
    col("salary") + col("bonus")
)

# 7. Employee with highest total compensation overall
highest_compensation_emp = df_with_compensation.orderBy(col("total_compensation").desc()).first()

# Display results
print("2. After Filling Missing Bonuses:")
df_filled.show()

print("3. Top 2 Employees per Department by Performance:")
top_2_per_dept.show()

print("4. Department Average Statistics:")
dept_avg_stats.show()

print("5. Employees Ranked by Salary within Department:")
df_salary_ranked.select("dept", "emp_name", "salary", "salary_rank").show()

print("6. Employees with Total Compensation:")
df_with_compensation.select("emp_name", "dept", "salary", "bonus", "total_compensation").show()

print(f"7. Employee with Highest Total Compensation: {highest_compensation_emp['emp_name']} with {highest_compensation_emp['total_compensation']}")

--------------------------------------------------------------------------------------

%python

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Sales Data
sales_data = [
    (1, "John", "2024-01-01", 500),
    (2, "John", "2024-01-05", 700),
    (3, "Sara", "2024-01-02", 300),
    (4, "Sara", "2024-01-06", 800),
    (5, "Mike", "2024-01-03", 600),
    (6, "Mike", "2024-01-07", 900),
]

sales_df = spark.createDataFrame(sales_data, ["sale_id", "emp_name", "date", "amount"])

# 1. Total sales amount by each employee
total_sales_per_employee = sales_df.groupBy("emp_name").agg(
    sum("amount").alias("total_sales")
)

# 2. Rank employees by total sales
window_total_sales = Window.orderBy(col("total_sales").desc())
employee_ranking = total_sales_per_employee.withColumn(
    "rank", 
    rank().over(window_total_sales)
)

# 3. Highest sale amount for each employee
highest_sale_per_employee = sales_df.groupBy("emp_name").agg(
    max("amount").alias("highest_sale")
)

# 4. Cumulative sales per employee by date
window_employee_date = Window.partitionBy("emp_name").orderBy("date")
cumulative_sales = sales_df.withColumn(
    "cumulative_sales", 
    sum("amount").over(window_employee_date)
).select("emp_name", "date", "amount", "cumulative_sales")

# 5. Top 2 employees with highest sales overall
top_2_employees = total_sales_per_employee.orderBy(col("total_sales").desc()).limit(2)

# Display results
print("1. Total Sales per Employee:")
total_sales_per_employee.show()

print("2. Employee Ranking by Total Sales:")
employee_ranking.show()

print("3. Highest Sale Amount per Employee:")
highest_sale_per_employee.show()

print("4. Cumulative Sales per Employee:")
cumulative_sales.show()

print("5. Top 2 Employees:")
top_2_employees.show()

---------------------------------------------------------------------------------------

%python

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Customers Data
customers_data = [
    (1, "John", "USA"),
    (2, "Alice", "UK"),
    (3, "Bob", "USA"),
    (4, "Carol", "Canada"),
    (5, "David", "UK")
]
customers_df = spark.createDataFrame(customers_data, ["customer_id", "name", "country"])

# Sales Data
sales_data = [
    (101, 1, "Laptop", 1000),
    (102, 1, "Mouse", 50),
    (103, 2, "Tablet", 500),
    (104, 3, "Phone", 800),
    (105, 2, "Keyboard", 100),
    (106, 4, "Monitor", 300),
    (107, 5, "Laptop", 1200),
    (108, 3, "Tablet", 450)
]
sales_df = spark.createDataFrame(sales_data, ["order_id", "customer_id", "product", "amount"])

# 1. Load datasets (already done)

# 2. Join customers and sales
df_joined = sales_df.join(customers_df, "customer_id")

# 3. Total amount spent by each customer
total_spent_per_customer = df_joined.groupBy("customer_id", "name").agg(
    sum("amount").alias("total_spent")
)

# 4. Highest spending customer
highest_spending_customer = total_spent_per_customer.orderBy(col("total_spent").desc()).first()

# 5. Average order value by country
avg_order_by_country = df_joined.groupBy("country").agg(
    avg("amount").alias("avg_order_value")
)

# Display results
print("2. Joined Data:")
df_joined.show()

print("3. Total Amount Spent per Customer:")
total_spent_per_customer.show()

print(f"4. Highest Spending Customer: {highest_spending_customer['name']} with {highest_spending_customer['total_spent']}")

print("5. Average Order Value by Country:")
avg_order_by_country.show()

--------------------------------------------------------------------------------------

%python

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Data
data_day2 = [
    (1, "John", "Sales", 3000),
    (2, "Sara", "HR", 4500),
    (3, "Mike", "IT", 6000),
    (4, "Linda", "Sales", 2000),
    (5, "James", "IT", 7000)
]

columns_day2 = ["id", "name", "department", "salary"]
df_day2 = spark.createDataFrame(data_day2, columns_day2)

# Problem 1: Filter Employees by Salary (> 4000)
high_earners = df_day2.filter(col("salary") > 4000)

# Problem 2: Count Employees in Each Department
dept_count = df_day2.groupBy("department").agg(
    count("*").alias("employee_count")
)

# Problem 3: Add Bonus Column (10% of salary)
df_with_bonus = df_day2.withColumn("bonus", col("salary") * 0.10)

# Problem 4: Sort Employees by Salary (Descending)
df_sorted = df_day2.orderBy(col("salary").desc())

# Problem 5: Maximum Salary per Department
max_salary_per_dept = df_day2.groupBy("department").agg(
    max("salary").alias("max_salary")
)

# Display results
print("1. Employees Earning More Than 4000:")
high_earners.show()

print("2. Employee Count per Department:")
dept_count.show()

print("3. Employees with Bonus Column:")
df_with_bonus.show()

print("4. Employees Sorted by Salary (Descending):")
df_sorted.show()

print("5. Maximum Salary per Department:")
max_salary_per_dept.show()

----------------------------------------------------------------------------------------

%python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Data
data_day1 = [
    (1, "John", "HR", 3000),
    (2, "Mike", "IT", 4000),
    (3, "Sara", "HR", 3500),
    (4, "David", "Finance", 4500),
    (5, "Anna", "IT", 4200),
]
columns_day1 = ["emp_id", "name", "department", "salary"]

df_day1 = spark.createDataFrame(data_day1, columns_day1)

# Find highest-paid employee in each department
# Using window functions to handle ties
window_dept = Window.partitionBy("department").orderBy(col("salary").desc())

# Option 1: Using row_number() - will give only one employee per department even if ties
highest_paid_row_number = df_day1.withColumn("rn", row_number().over(window_dept)) \
    .filter(col("rn") == 1) \
    .select("emp_id", "name", "department", "salary")

# Option 2: Using rank() - will include all employees with top salary if there are ties
highest_paid_rank = df_day1.withColumn("rnk", rank().over(window_dept)) \
    .filter(col("rnk") == 1) \
    .select("emp_id", "name", "department", "salary")

# Display results
print("Using row_number() - One employee per department:")
highest_paid_row_number.show()

print("Using rank() - Includes ties:")
highest_paid_rank.show()

# Answer to the question:
print("\nAnswer to the question:")
print("If two employees in the same department have the same top salary:")
print("- row_number() would pick only one employee (arbitrary)")
print("- rank() would include all employees with the top salary")
print("- dense_rank() would also include all employees with the top salary")
print("For handling ties, use rank() or dense_rank() instead of row_number()")