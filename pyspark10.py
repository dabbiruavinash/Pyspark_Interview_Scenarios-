# Find the 2nd highest salary from employees table

%sql
SELECT salaryFROM (
SELECT salary, DENSE_RANK() OVER (ORDER BY salary DESC) AS rank FROM employees) WHERE rank = 2;

%python
from pyspark.sql import Window
from pyspark.sql.functions import dense_rank

window_spec = Window.orderBy(col("salary").desc())
ranked_df = employees.select("salary", dense_rank().over(window_spec).alias("rank"))
result_df = ranked_df.filter(col("rank") == 2).select("salary")

# Running total of sales for each salesperson

%sql
SELECT
    salesperson_id,
    sale_date,
    sale_amount,
    SUM(sale_amount) OVER (PARTITION BY salesperson_id ORDER BY sale_date) AS running_total FROM sales;

%python
from pyspark.sql import Window
from pyspark.sql.functions import sum

window_spec = Window.partitionBy("salesperson_id").orderBy("sale_date")
result_df = sales.select(
    "salesperson_id",
    "sale_date",
    "sale_amount",
    sum("sale_amount").over(window_spec).alias("running_total"))

# Top 3 salaries for each department

%sql
SELECT department_id, employee_id, salary FROM (
SELECT department_id, employee_id, salary, DENSE_RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) AS rank FROM employees) WHERE rank <= 3;

%python
from pyspark.sql import Window
from pyspark.sql.functions import dense_rank

window_spec = Window.partitionBy("department_id").orderBy(col("salary").desc())
ranked_df = employees.select(
    "department_id",
    "employee_id",
    "salary",
    dense_rank().over(window_spec).alias("rank"))
result_df = ranked_df.filter(col("rank") <= 3)

# 3-month moving average of salaries

%sql
SELECT employee_id, hire_date, salary, AVG(salary) OVER (PARTITION BY employee_id ORDER BY hire_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg FROM employees;

%python
from pyspark.sql import Window
from pyspark.sql.functions import avg

window_spec = Window.partitionBy("employee_id").orderBy("hire_date").rowsBetween(-2, 0)
result_df = employees.select(
    "employee_id",
    "hire_date",
    "salary",
    avg("salary").over(window_spec).alias("moving_avg"))

# Rank employees by salary within department

%sql
SELECT
    employee_id,
    department_id,
    salary,
    RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) AS salary_rank FROM employees;

%python
from pyspark.sql import Window
from pyspark.sql.functions import rank

window_spec = Window.partitionBy("department_id").orderBy(col("salary").desc())
result_df = employees.select(
    "employee_id",
    "department_id",
    "salary",
    rank().over(window_spec).alias("salary_rank"))

# Employees with highest salary in each department

%sql
SELECT employee_id, department_id, salary FROM (
SELECT employee_id, department_id, salary, RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) AS salary_rank FROM employees) WHERE salary_rank = 1;

%python
from pyspark.sql import Window
from pyspark.sql.functions import rank

window_spec = Window.partitionBy("department_id").orderBy(col("salary").desc())
ranked_df = employees.select(
    "employee_id",
    "department_id",
    "salary",
    rank().over(window_spec).alias("salary_rank"))
result_df = ranked_df.filter(col("salary_rank") == 1)

# Cumulative percentage of sales

%sql
SELECT
    product_id,
    sale_amount,
    SUM(sale_amount) OVER (ORDER BY sale_amount DESC) AS cumulative_sales,
    SUM(sale_amount) OVER () AS total_sales,
    (SUM(sale_amount) OVER (ORDER BY sale_amount DESC) * 100) /
    SUM(sale_amount) OVER () AS cumulative_percentage FROM sales;

%python
from pyspark.sql import Window
from pyspark.sql.functions import sum

total_window = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
cumulative_window = Window.orderBy(col("sale_amount").desc()).rowsBetween(Window.unboundedPreceding, 0)

result_df = sales.select(
    "product_id",
    "sale_amount",
    sum("sale_amount").over(cumulative_window).alias("cumulative_sales"),
    sum("sale_amount").over(total_window).alias("total_sales"),
    (sum("sale_amount").over(cumulative_window) * 100 / sum("sale_amount").over(total_window)).alias("cumulative_percentage"))

# Gap in days between consecutive sales

%sql
SELECT
    product_id,
    sale_date,
    LAG(sale_date) OVER (PARTITION BY product_id ORDER BY sale_date) AS previous_sale_date,
    sale_date - LAG(sale_date) OVER (PARTITION BY product_id ORDER BY sale_date) AS gap_in_days FROM sales;

%python
from pyspark.sql import Window
from pyspark.sql.functions import lag, datediff, col

window_spec = Window.partitionBy("product_id").orderBy("sale_date")
result_df = sales.select(
    "product_id",
    "sale_date",
    lag("sale_date").over(window_spec).alias("previous_sale_date"),
    datediff(col("sale_date"), lag("sale_date").over(window_spec)).alias("gap_in_days"))

# Identify duplicate rows in a table

%sql
SELECT * FROM (
SELECT t.*, ROW_NUMBER() OVER (PARTITION BY column1, column2 ORDER BY column1) AS row_num FROM table_name t) WHERE row_num > 1;

%python
from pyspark.sql import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("column1", "column2").orderBy("column1")
result_df = table_name.select("*", row_number().over(window_spec).alias("row_num")).filter(col("row_num") > 1).drop("row_num")