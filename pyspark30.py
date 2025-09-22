from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("SQLQueries").getOrCreate()

# Query 1: Employees with same manager
def query1_same_manager(employees_df):
    e1 = employees_df.alias("e1")
    e2 = employees_df.alias("e2")
    return e1.join(e2, col("e1.manager_id") == col("e2.manager_id")) \
             .filter(col("e1.employee_id") != col("e2.employee_id")) \
             .select(col("e1.employee_id"), col("e1.employee_name"), col("e1.manager_id"))

# Query 2: Most recent transaction per customer
def query2_recent_transaction(transactions_df):
    window_spec = Window.partitionBy("customer_id").orderBy(col("transaction_date").desc())
    return transactions_df.withColumn("rn", row_number().over(window_spec)) \
                         .filter(col("rn") == 1) \
                         .drop("rn")

# Query 3: Department total salary > threshold
def query3_department_salary(employees_df, threshold=50000):
    return employees_df.groupBy("department_id") \
                      .agg(sum("salary").alias("total_salary")) \
                      .filter(col("total_salary") > threshold)

# Query 4: Running total of orders
def query4_running_total(orders_df):
    window_spec = Window.partitionBy("customer_id").orderBy("order_date")
    return orders_df.withColumn("running_total", 
                               sum("order_amount").over(window_spec))

# Query 5: Employees hired per month/year
def query5_hires_per_month(employees_df):
    return employees_df.withColumn("year", year("hire_date")) \
                      .withColumn("month", month("hire_date")) \
                      .groupBy("year", "month") \
                      .agg(count("*").alias("total_hires"))

# Query 6: Employees earning more than department average
def query6_above_avg_salary(employees_df):
    window_spec = Window.partitionBy("department_id")
    return employees_df.withColumn("dept_avg_salary", 
                                  avg("salary").over(window_spec)) \
                      .filter(col("salary") > col("dept_avg_salary"))

# Query 7: Products never ordered
def query7_products_never_ordered(products_df, orders_df):
    return products_df.join(orders_df, "product_id", "left_anti")

# Query 8: Employees who are managers
def query8_employee_managers(employees_df):
    managers = employees_df.select("manager_id").distinct() \
                          .filter(col("manager_id").isNotNull())
    return employees_df.join(managers, 
                           employees_df.employee_id == managers.manager_id)

# Query 9: Employees joined in same month/year
def query9_same_join_date(employees_df):
    e1 = employees_df.alias("e1")
    e2 = employees_df.alias("e2")
    return e1.join(e2, (year("e1.hire_date") == year("e2.hire_date")) & 
                       (month("e1.hire_date") == month("e2.hire_date"))) \
             .filter(col("e1.employee_id") != col("e2.employee_id")) \
             .select(col("e1.employee_id"), col("e1.employee_name"), col("e1.hire_date"))

# Query 10: Products ordered together
def query10_products_ordered_together(orders_df):
    o1 = orders_df.alias("o1")
    o2 = orders_df.alias("o2")
    return o1.join(o2, (col("o1.order_id") == col("o2.order_id")) & 
                      (col("o1.product_id") < col("o2.product_id"))) \
             .groupBy(col("o1.product_id").alias("product1"), 
                     col("o2.product_id").alias("product2")) \
             .agg(count("*").alias("frequency")) \
             .filter(col("frequency") > 0)

# Query 11: Average order value by customer per month
def query11_avg_order_value(orders_df):
    return orders_df.withColumn("year", year("order_date")) \
                   .withColumn("month", month("order_date")) \
                   .groupBy("customer_id", "year", "month") \
                   .agg(avg("order_amount").alias("avg_order_value"))

# Query 12: Total revenue per region per quarter
def query12_revenue_by_region_quarter(orders_df):
    return orders_df.withColumn("quarter", quarter("order_date")) \
                   .groupBy("region", "quarter") \
                   .agg(sum("order_amount").alias("total_revenue"))

--------------------------------------------------------------------------------------------------------
-- Query 1: Employees with same manager
SELECT e1.employee_id, e1.employee_name, e1.manager_id
FROM employees e1
JOIN employees e2 ON e1.manager_id = e2.manager_id
WHERE e1.employee_id <> e2.employee_id;

-- Query 2: Most recent transaction per customer
SELECT *
FROM transactions t1
WHERE transaction_date = (
    SELECT MAX(transaction_date)
    FROM transactions t2
    WHERE t1.customer_id = t2.customer_id
);

-- Query 3: Department total salary > threshold
SELECT department_id, SUM(salary) AS total_salary
FROM employees
GROUP BY department_id
HAVING SUM(salary) > 50000;

-- Query 4: Running total of orders
SELECT customer_id, order_id, order_date,
       SUM(order_amount) OVER (PARTITION BY customer_id ORDER BY order_date) AS running_total
FROM orders;

-- Query 5: Employees hired per month/year
SELECT EXTRACT(YEAR FROM hire_date) AS year, 
       EXTRACT(MONTH FROM hire_date) AS month, 
       COUNT(*) AS total_hires
FROM employees
GROUP BY EXTRACT(YEAR FROM hire_date), EXTRACT(MONTH FROM hire_date);

-- Query 6: Employees earning more than department average
SELECT *
FROM employees e
WHERE salary > (
    SELECT AVG(salary)
    FROM employees
    WHERE department_id = e.department_id
);

-- Query 7: Products never ordered
SELECT p.product_id, p.product_name
FROM products p
LEFT JOIN orders o ON p.product_id = o.product_id
WHERE o.product_id IS NULL;

-- Query 8: Employees who are managers
SELECT DISTINCT e.*
FROM employees e
JOIN employees m ON e.employee_id = m.manager_id;

-- Query 9: Employees joined in same month/year
SELECT e1.employee_id, e1.employee_name, e1.hire_date
FROM employees e1
JOIN employees e2 ON EXTRACT(YEAR FROM e1.hire_date) = EXTRACT(YEAR FROM e2.hire_date)
                 AND EXTRACT(MONTH FROM e1.hire_date) = EXTRACT(MONTH FROM e2.hire_date)
WHERE e1.employee_id <> e2.employee_id;

-- Query 10: Products ordered together
SELECT o1.product_id AS product1, o2.product_id AS product2, COUNT(*) AS frequency
FROM orders o1
JOIN orders o2 ON o1.order_id = o2.order_id AND o1.product_id < o2.product_id
GROUP BY o1.product_id, o2.product_id
HAVING COUNT(*) > 0;

-- Query 11: Average order value by customer per month
SELECT customer_id, 
       EXTRACT(YEAR FROM order_date) AS year, 
       EXTRACT(MONTH FROM order_date) AS month,
       AVG(order_amount) AS avg_order_value
FROM orders
GROUP BY customer_id, EXTRACT(YEAR FROM order_date), EXTRACT(MONTH FROM order_date);

-- Query 12: Total revenue per region per quarter
SELECT region, 
       EXTRACT(QUARTER FROM order_date) AS quarter,
       SUM(order_amount) AS total_revenue
FROM orders
GROUP BY region, EXTRACT(QUARTER FROM order_date);