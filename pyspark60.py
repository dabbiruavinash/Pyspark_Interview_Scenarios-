1. Find duplicate customer 

%sql
-- Method 1: Using GROUP BY
SELECT customer_id, COUNT(*) as duplicate_count FROM customers GROUP BY customer_id HAVING COUNT(*) > 1;

-- Method 2: With all details
SELECT * FROM (
    SELECT c.*, 
           COUNT(*) OVER (PARTITION BY customer_id) as dup_count FROM customers c) WHERE dup_count > 1;

%python
from pyspark.sql.functions import col, count

# Method 1
duplicates = customers_df.groupBy("customer_id") \
    .agg(count("*").alias("duplicate_count")) \
    .filter(col("duplicate_count") > 1)

# Method 2: Keep all duplicate rows
from pyspark.sql.window import Window
window_spec = Window.partitionBy("customer_id")
duplicates_df = customers_df.withColumn("dup_count", count("*").over(window_spec)) \
    .filter(col("dup_count") > 1)

2. Top3 salaries per department with ties

%sql
-- Using DENSE_RANK for ties
SELECT department_id, employee_id, salary
FROM (
    SELECT department_id, employee_id, salary,
           DENSE_RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) as rnk
    FROM employees) WHERE rnk <= 3 ORDER BY department_id, salary DESC;

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, col

window_spec = Window.partitionBy("department_id").orderBy(col("salary").desc())
top3_df = employees_df.withColumn("rnk", dense_rank().over(window_spec)) \
    .filter(col("rnk") <= 3) \
    .select("department_id", "employee_id", "salary")

3. Find users with a 7-day login streak

%sql
WITH login_dates AS (
    SELECT DISTINCT user_id, TRUNC(login_timestamp) as login_date
    FROM logins),
streaks AS (
    SELECT user_id, login_date,
           login_date - ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date) as grp FROM login_dates),
consecutive_groups AS (
    SELECT user_id, grp, COUNT(*) as streak_length
    FROM streaks
    GROUP BY user_id, grp
    HAVING COUNT(*) >= 7)
SELECT DISTINCT user_id FROM consecutive_groups;

%python
from pyspark.sql.functions import col, datediff, row_number, count as spark_count, to_date

login_dates = logins_df.select("user_id", to_date("login_timestamp").alias("login_date")).distinct()

window_spec = Window.partitionBy("user_id").orderBy("login_date")
streaks = login_dates.withColumn("rn", row_number().over(window_spec)) \
    .withColumn("grp", datediff(col("login_date"), col("rn")))

consecutive_groups = streaks.groupBy("user_id", "grp") \
    .agg(spark_count("*").alias("streak_length")) \
    .filter(col("streak_length") >= 7)

users_with_streak = consecutive_groups.select("user_id").distinct()

4. Find missing dates in a sales table

%sql
WITH date_range AS (
    SELECT MIN(sale_date) as start_date, MAX(sale_date) as end_date
    FROM sales),
all_dates AS (
    SELECT start_date + LEVEL - 1 as date_val
    FROM date_range
    CONNECT BY LEVEL <= (end_date - start_date + 1))
SELECT d.date_val as missing_date FROM all_dates d LEFT JOIN (SELECT DISTINCT sale_date FROM sales) s ON d.date_val = s.sale_date WHERE s.sale_date IS NULL ORDER BY d.date_val;

%python
from pyspark.sql.functions import min as spark_min, max as spark_max, explode, sequence, col

date_range = sales_df.agg(spark_min("sale_date").alias("min_date"), 
                          spark_max("sale_date").alias("max_date")).collect()[0]

all_dates = spark.range(0, (date_range.max_date - date_range.min_date).days + 1) \
    .select(explode(sequence(date_range.min_date, date_range.max_date)).alias("date_val"))

existing_dates = sales_df.select("sale_date").distinct()
missing_dates = all_dates.join(existing_dates, all_dates.date_val == existing_dates.sale_date, "left_anti")

5. Running total with reset logic

%sql
-- Reset running total when value exceeds 100
SELECT transaction_id, transaction_date, amount,
       SUM(amount) OVER (
           ORDER BY transaction_date
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_total,
       CASE 
           WHEN SUM(amount) OVER (ORDER BY transaction_date) > 100 
           THEN 'Reset'
           ELSE 'Continue' END as status FROM transactions;

-- With actual reset logic using MOD
SELECT transaction_id, amount,
       SUM(amount) OVER (PARTITION BY grp ORDER BY transaction_id) as reset_running_total
FROM (
    SELECT transaction_id, amount,
           SUM(amount) OVER (ORDER BY transaction_id) as running_total,
           FLOOR(SUM(amount) OVER (ORDER BY transaction_id) / 100) as grp FROM transactions);

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as spark_sum, floor

# Running total with reset logic
window_spec = Window.orderBy("transaction_id")
running_total_df = transactions_df.withColumn("running_total", spark_sum("amount").over(window_spec)) \
    .withColumn("grp", floor(col("running_total") / 100))

reset_window = Window.partitionBy("grp").orderBy("transaction_id")
reset_running_total = running_total_df.withColumn("reset_running_total", 
                                                   spark_sum("amount").over(reset_window))

6. Firest non-null phone number per use

%sql
SELECT user_id,
       MIN(phone_number) KEEP (DENSE_RANK FIRST ORDER BY 
           CASE WHEN phone_number IS NOT NULL THEN 0 ELSE 1 END, 
           created_date) as first_phone_number
FROM user_phones
GROUP BY user_id;

-- Alternative using window function
SELECT DISTINCT user_id,
       FIRST_VALUE(phone_number) OVER (
           PARTITION BY user_id 
           ORDER BY CASE WHEN phone_number IS NOT NULL THEN 0 ELSE 1 END, created_date
           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
       ) as first_phone
FROM user_phones
WHERE phone_number IS NOT NULL;

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import first, col, when

window_spec = Window.partitionBy("user_id") \
    .orderBy(when(col("phone_number").isNotNull(), 0).otherwise(1), "created_date") \
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

first_phone_df = user_phones_df.withColumn("first_phone", 
                                           first("phone_number", ignorenulls=True).over(window_spec)) \
    .select("user_id", "first_phone").distinct()

7. Customers who haven't placed any orders

%sql
-- Method 1: LEFT JOIN
SELECT c.customer_id, c.customer_name
FROM customers c LEFT JOIN orders o ON c.customer_id = o.customer_id WHERE o.order_id IS NULL;

-- Method 2: NOT EXISTS
SELECT customer_id, customer_name
FROM customers c
WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id);

-- Method 3: NOT IN
SELECT customer_id, customer_name
FROM customers
WHERE customer_id NOT IN (SELECT DISTINCT customer_id FROM orders);

%python
# Method 1: Left Anti Join
customers_no_orders = customers_df.join(orders_df, "customer_id", "left_anti")

# Method 2: Using subtract
customer_ids = customers_df.select("customer_id")
order_customer_ids = orders_df.select("customer_id").distinct()
customers_no_orders = customer_ids.subtract(order_customer_ids) \
    .join(customers_df, "customer_id")

8. Products with sales greater than average

%sql
WITH product_sales AS (
    SELECT product_id, SUM(quantity * unit_price) as total_sales
    FROM order_items
    GROUP BY product_id),
avg_sales AS (
    SELECT AVG(total_sales) as avg_total_sales
    FROM product_sales)
SELECT p.product_id, p.product_name, ps.total_sales
FROM product_sales ps
JOIN products p ON ps.product_id = p.product_id
CROSS JOIN avg_sales a
WHERE ps.total_sales > a.avg_total_sales
ORDER BY ps.total_sales DESC;

%python
from pyspark.sql.functions import avg, sum, col

product_sales = order_items_df.groupBy("product_id") \
    .agg(sum(col("quantity") * col("unit_price")).alias("total_sales"))

avg_sales_value = product_sales.agg(avg("total_sales").alias("avg_sales")).collect()[0]["avg_sales"]

above_avg_products = product_sales.filter(col("total_sales") > avg_sales_value) \
    .join(products_df, "product_id")

9. Monthly revenue trend(running total)

%sql
WITH monthly_revenue AS (
    SELECT TRUNC(sale_date, 'MM') as sale_month,
           SUM(amount) as monthly_revenue FROM sales GROUP BY TRUNC(sale_date, 'MM'))
SELECT sale_month,
       monthly_revenue,
       SUM(monthly_revenue) OVER (ORDER BY sale_month) as cumulative_revenue,
       SUM(monthly_revenue) OVER (
           ORDER BY sale_month 
           ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) as rolling_12_month_revenue FROM monthly_revenue ORDER BY sale_month;

%python
from pyspark.sql.functions import sum as spark_sum, trunc, col

monthly_revenue = sales_df.withColumn("sale_month", trunc("sale_date", "month")) \
    .groupBy("sale_month") \
    .agg(spark_sum("amount").alias("monthly_revenue"))

window_spec = Window.orderBy("sale_month")
rolling_window = Window.orderBy("sale_month").rowsBetween(-11, 0)

monthly_trend = monthly_revenue.withColumn("cumulative_revenue", 
                                           spark_sum("monthly_revenue").over(window_spec)) \
    .withColumn("rolling_12_month", spark_sum("monthly_revenue").over(rolling_window))

10. Top 3 customers by spending in each category

%sql
WITH customer_category_spending AS (
    SELECT c.customer_id, p.category_id,
           SUM(oi.quantity * oi.unit_price) as total_spent
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
    GROUP BY c.customer_id, p.category_id)
SELECT customer_id, category_id, total_spent, rnk FROM (SELECT customer_id, category_id, total_spent,
           DENSE_RANK() OVER (PARTITION BY category_id ORDER BY total_spent DESC) as rnk FROM customer_category_spending) WHERE rnk <= 3 ORDER BY category_id, rnk;

%python
customer_category_spending = orders_df.join(order_items_df, "order_id") \
    .join(products_df, "product_id") \
    .groupBy("customer_id", "category_id") \
    .agg(spark_sum(col("quantity") * col("unit_price")).alias("total_spent"))

window_spec = Window.partitionBy("category_id").orderBy(col("total_spent").desc())
top3_customers = customer_category_spending.withColumn("rnk", dense_rank().over(window_spec)) \
    .filter(col("rnk") <= 3)

11. Employees with higher salary than their manager

%sql
-- Self join
SELECT e.employee_id, e.employee_name, e.salary as emp_salary,
       m.employee_id as manager_id, m.employee_name as manager_name, m.salary as manager_salary
FROM employees e
JOIN employees m ON e.manager_id = m.employee_id
WHERE e.salary > m.salary;

-- Using subquery
SELECT employee_id, employee_name, salary
FROM employees e
WHERE salary > (
    SELECT salary FROM employees WHERE employee_id = e.manager_id);

%python
# Self join
emp_df = employees_df.alias("e")
mgr_df = employees_df.alias("m")

higher_than_manager = emp_df.join(mgr_df, 
                                  col("e.manager_id") == col("m.employee_id"), "inner") \
    .filter(col("e.salary") > col("m.salary")) \
    .select(col("e.employee_id"), col("e.employee_name"), col("e.salary").alias("emp_salary"),
            col("m.employee_id").alias("manager_id"), col("m.employee_name").alias("manager_name"),
            col("m.salary").alias("manager_salary"))

12. Consecutive login days by user

%sql
WITH login_dates AS (
    SELECT DISTINCT user_id, TRUNC(login_timestamp) as login_date
    FROM logins),
consecutive_groups AS (
    SELECT user_id, login_date,
           login_date - ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date) as grp
    FROM login_dates),
streak_lengths AS (
    SELECT user_id, grp, 
           COUNT(*) as streak_length,
           MIN(login_date) as streak_start,
           MAX(login_date) as streak_end
    FROM consecutive_groups
    GROUP BY user_id, grp)
SELECT user_id, streak_length, streak_start, streak_end
FROM streak_lengths
WHERE streak_length >= 3  -- minimum consecutive days
ORDER BY user_id, streak_start;

%python
from pyspark.sql.functions import datediff, to_date, min as spark_min, max as spark_max

login_dates = logins_df.select(to_date("login_timestamp").alias("login_date"), "user_id").distinct()

window_spec = Window.partitionBy("user_id").orderBy("login_date")
consecutive_groups = login_dates.withColumn("rn", row_number().over(window_spec)) \
    .withColumn("grp", datediff(col("login_date"), col("rn")))

streak_lengths = consecutive_groups.groupBy("user_id", "grp") \
    .agg(spark_count("*").alias("streak_length"),
         spark_min("login_date").alias("streak_start"),
         spark_max("login_date").alias("streak_end")) \
    .filter(col("streak_length") >= 3)

13. cumulative product sales month over month

%sql
WITH product_monthly_sales AS (
    SELECT product_id, 
           TRUNC(sale_date, 'MM') as sale_month,
           SUM(quantity * unit_price) as monthly_sales
    FROM order_items
    JOIN orders ON order_items.order_id = orders.order_id
    GROUP BY product_id, TRUNC(sale_date, 'MM')
)
SELECT product_id, sale_month, monthly_sales,
       SUM(monthly_sales) OVER (PARTITION BY product_id ORDER BY sale_month) as cumulative_sales,
       LAG(monthly_sales) OVER (PARTITION BY product_id ORDER BY sale_month) as prev_month_sales,
       CASE 
           WHEN LAG(monthly_sales) OVER (PARTITION BY product_id ORDER BY sale_month) > 0 
           THEN ROUND((monthly_sales - LAG(monthly_sales) OVER (PARTITION BY product_id ORDER BY sale_month)) * 100.0 / 
                 LAG(monthly_sales) OVER (PARTITION BY product_id ORDER BY sale_month), 2)
           ELSE NULL 
       END as mom_growth_pct
FROM product_monthly_sales
ORDER BY product_id, sale_month;

%python
from pyspark.sql.functions import lag, round as spark_round

monthly_sales = order_items_df.join(orders_df, "order_id") \
    .withColumn("sale_month", trunc("sale_date", "month")) \
    .groupBy("product_id", "sale_month") \
    .agg(spark_sum(col("quantity") * col("unit_price")).alias("monthly_sales"))

window_spec = Window.partitionBy("product_id").orderBy("sale_month")
cumulative_sales = monthly_sales.withColumn("cumulative_sales", 
                                            spark_sum("monthly_sales").over(window_spec)) \
    .withColumn("prev_month_sales", lag("monthly_sales").over(window_spec)) \
    .withColumn("mom_growth_pct", 
                when(col("prev_month_sales") > 0,
                     spark_round((col("monthly_sales") - col("prev_month_sales")) * 100.0 / col("prev_month_sales"), 2)
                ).otherwise(None))

14. Department with more than 5 employees 

%sql
SELECT d.department_id, d.department_name, COUNT(e.employee_id) as employee_count
FROM departments d
JOIN employees e ON d.department_id = e.department_id
GROUP BY d.department_id, d.department_name
HAVING COUNT(e.employee_id) > 5
ORDER BY employee_count DESC;

%python
departments_more_than_5 = employees_df.groupBy("department_id") \
    .agg(spark_count("employee_id").alias("employee_count")) \
    .filter(col("employee_count") > 5) \
    .join(departments_df, "department_id") \
    .orderBy(col("employee_count").desc())

15. Second highest salary in the company

%sql
-- Method 1: Using DENSE_RANK
SELECT salary
FROM (
    SELECT salary, DENSE_RANK() OVER (ORDER BY salary DESC) as rnk
    FROM employees) WHERE rnk = 2;

-- Method 2: Using subquery
SELECT MAX(salary) as second_highest_salary FROM employees
WHERE salary < (SELECT MAX(salary) FROM employees);

-- Method 3: Using OFFSET (Oracle 12c+)
SELECT salary
FROM (SELECT DISTINCT salary FROM employees ORDER BY salary DESC)
OFFSET 1 ROWS FETCH NEXT 1 ROWS ONLY;

%python
# Method 1: Using dense_rank
second_highest = employees_df.select("salary").distinct() \
    .withColumn("rnk", dense_rank().over(Window.orderBy(col("salary").desc()))) \
    .filter(col("rnk") == 2) \
    .select("salary")

# Method 2: Using limit
second_highest = employees_df.select("salary").distinct() \
    .orderBy(col("salary").desc()) \
    .limit(2) \
    .orderBy(col("salary").asc()) \
    .limit(1)

16. Customers who spent more in 2024 than in 2023

%sql
WITH yearly_spending AS (
    SELECT customer_id,
           EXTRACT(YEAR FROM order_date) as order_year,
           SUM(order_amount) as total_spent
    FROM orders
    WHERE EXTRACT(YEAR FROM order_date) IN (2023, 2024)
    GROUP BY customer_id, EXTRACT(YEAR FROM order_date))
SELECT c.customer_id, c.customer_name,
       sp2023.total_spent as spent_2023,
       sp2024.total_spent as spent_2024,
       sp2024.total_spent - sp2023.total_spent as difference FROM yearly_spending sp2024
JOIN yearly_spending sp2023 ON sp2024.customer_id = sp2023.customer_id AND sp2023.order_year = 2023
JOIN customers c ON sp2024.customer_id = c.customer_id WHERE sp2024.order_year = 2024 AND sp2024.total_spent > sp2023.total_spent ORDER BY difference DESC;

%python
from pyspark.sql.functions import year, sum as spark_sum

yearly_spending = orders_df.filter(year("order_date").isin([2023, 2024])) \
    .withColumn("order_year", year("order_date")) \
    .groupBy("customer_id", "order_year") \
    .agg(spark_sum("order_amount").alias("total_spent"))

spending_2023 = yearly_spending.filter(col("order_year") == 2023) \
    .select(col("customer_id"), col("total_spent").alias("spent_2023"))
spending_2024 = yearly_spending.filter(col("order_year") == 2024) \
    .select(col("customer_id"), col("total_spent").alias("spent_2024"))

customers_more_2024 = spending_2024.join(spending_2023, "customer_id") \
    .filter(col("spent_2024") > col("spent_2023")) \
    .join(customers_df, "customer_id")

17. Users who made consecutive purchase

%sql
WITH user_purchase_dates AS (
    SELECT DISTINCT user_id, TRUNC(purchase_date) as purchase_date
    FROM purchases),
purchase_groups AS (
    SELECT user_id, purchase_date,
           purchase_date - ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY purchase_date) as grp
    FROM user_purchase_dates),
consecutive_days AS (
    SELECT user_id, grp, COUNT(*) as consecutive_count
    FROM purchase_groups
    GROUP BY user_id, grp
    HAVING COUNT(*) >= 2  -- At least 2 consecutive days)
SELECT DISTINCT user_id FROM consecutive_days;

%python
purchase_dates = purchases_df.select("user_id", 
                                     to_date("purchase_date").alias("purchase_date")).distinct()

window_spec = Window.partitionBy("user_id").orderBy("purchase_date")
purchase_groups = purchase_dates.withColumn("rn", row_number().over(window_spec)) \
    .withColumn("grp", datediff(col("purchase_date"), col("rn")))

consecutive_users = purchase_groups.groupBy("user_id", "grp") \
    .agg(spark_count("*").alias("consecutive_count")) \
    .filter(col("consecutive_count") >= 2) \
    .select("user_id").distinct()

18. Top 3 products in each category

%sql
-- Based on sales amount
WITH product_sales AS (
SELECT p.product_id, p.product_name, p.category_id, SUM(oi.quantity * oi.unit_price) as total_sales FROM products p
    JOIN order_items oi ON p.product_id = oi.product_id
    GROUP BY p.product_id, p.product_name, p.category_id)
SELECT category_id, product_id, product_name, total_sales, rnk FROM (
SELECT category_id, product_id, product_name, total_sales, DENSE_RANK() OVER (PARTITION BY category_id ORDER BY total_sales DESC) as rnk FROM product_sales) WHERE rnk <= 3 ORDER BY category_id, rnk;

%python
product_sales = products_df.join(order_items_df, "product_id") \
    .groupBy("category_id", "product_id", "product_name") \
    .agg(spark_sum(col("quantity") * col("unit_price")).alias("total_sales"))

window_spec = Window.partitionBy("category_id").orderBy(col("total_sales").desc())
top3_products = product_sales.withColumn("rnk", dense_rank().over(window_spec)) \
    .filter(col("rnk") <= 3)

19. Employees with salary in top 10% of their department

%sql
WITH dept_salary_percentile AS (
    SELECT employee_id, department_id, salary,
           PERCENT_RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) as pct_rank
    FROM employees)
SELECT employee_id, department_id, salary FROM dept_salary_percentile WHERE pct_rank <= 0.1 ORDER BY department_id, salary DESC;

-- Alternative using NTILE
SELECT employee_id, department_id, salary FROM (
SELECT employee_id, department_id, salary, NTILE(10) OVER (PARTITION BY department_id ORDER BY salary DESC) as decile FROM employees) WHERE decile = 1;

%python
from pyspark.sql.functions import percent_rank, ntile

# Method 1: Using percent_rank
window_spec = Window.partitionBy("department_id").orderBy(col("salary").desc())
top_10_pct = employees_df.withColumn("pct_rank", percent_rank().over(window_spec)) \
    .filter(col("pct_rank") <= 0.1)

# Method 2: Using ntile
top_10_pct_ntile = employees_df.withColumn("decile", ntile(10).over(window_spec)) \
    .filter(col("decile") == 1)

20. Orders with no items

%sql
-- Orders that exist but have no items
SELECT o.order_id, o.order_date, o.customer_id FROM orders o
LEFT JOIN order_items oi ON o.order_id = oi.order_id
WHERE oi.order_id IS NULL;

-- Using NOT EXISTS
SELECT order_id, order_date, customer_id
FROM orders o
WHERE NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.order_id);

%python
# Left anti join
orders_no_items = orders_df.join(order_items_df, "order_id", "left_anti")

# Alternative with count
orders_with_items = order_items_df.groupBy("order_id").count()
orders_no_items = orders_df.join(orders_with_items, "order_id", "left_anti")

21. Products never ordered

%sql
SELECT p.product_id, p.product_name, p.category_id
FROM products p
LEFT JOIN order_items oi ON p.product_id = oi.product_id
WHERE oi.product_id IS NULL;

-- Using NOT EXISTS
SELECT product_id, product_name
FROM products p
WHERE NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.product_id);

%python
products_never_ordered = products_df.join(order_items_df, "product_id", "left_anti")

# Alternative using subtract
all_product_ids = products_df.select("product_id")
ordered_product_ids = order_items_df.select("product_id").distinct()
never_ordered = all_product_ids.subtract(ordered_product_ids).join(products_df, "product_id")

22. Monthly active users

%sql
SELECT TRUNC(login_timestamp, 'MM') as month,
       COUNT(DISTINCT user_id) as active_users
FROM logins
GROUP BY TRUNC(login_timestamp, 'MM')
ORDER BY month;

-- With growth metrics
WITH mau AS (
    SELECT TRUNC(login_timestamp, 'MM') as month,
           COUNT(DISTINCT user_id) as active_users
    FROM logins
    GROUP BY TRUNC(login_timestamp, 'MM'))
SELECT month, active_users,
       LAG(active_users) OVER (ORDER BY month) as prev_month_users,
       ROUND((active_users - LAG(active_users) OVER (ORDER BY month)) * 100.0 / 
             LAG(active_users) OVER (ORDER BY month), 2) as growth_pct FROM mau ORDER BY month;

%python
from pyspark.sql.functions import month, year, countDistinct

mau = logins_df.withColumn("month", trunc("login_timestamp", "month")) \
    .groupBy("month") \
    .agg(countDistinct("user_id").alias("active_users")) \
    .orderBy("month")

window_spec = Window.orderBy("month")
mau_with_growth = mau.withColumn("prev_month_users", lag("active_users").over(window_spec)) \
    .withColumn("growth_pct", 
                spark_round((col("active_users") - col("prev_month_users")) * 100.0 / 
                           col("prev_month_users"), 2))

23. New customers in the last 30 days

%sql
-- Customers who made their first purchase in last 30 days
WITH first_purchase AS (
    SELECT customer_id, MIN(order_date) as first_order_date
    FROM orders
    GROUP BY customer_id
)
SELECT c.customer_id, c.customer_name, fp.first_order_date
FROM first_purchase fp
JOIN customers c ON fp.customer_id = c.customer_id
WHERE fp.first_order_date >= SYSDATE - 30
ORDER BY fp.first_order_date DESC;

-- Newly registered customers
SELECT customer_id, customer_name, registration_date
FROM customers
WHERE registration_date >= SYSDATE - 30
ORDER BY registration_date DESC;

%python
from pyspark.sql.functions import current_date, date_sub

# Customers with first order in last 30 days
first_purchase = orders_df.groupBy("customer_id") \
    .agg(spark_min("order_date").alias("first_order_date"))

new_customers_by_purchase = first_purchase \
    .filter(col("first_order_date") >= date_sub(current_date(), 30)) \
    .join(customers_df, "customer_id")

# Newly registered customers
newly_registered = customers_df \
    .filter(col("registration_date") >= date_sub(current_date(), 30))

24. Customers with more than 1 product in their first order

%sql
WITH first_orders AS (
    SELECT customer_id, MIN(order_id) KEEP (DENSE_RANK FIRST ORDER BY order_date) as first_order_id
    FROM orders
    GROUP BY customer_id),
first_order_items AS (
    SELECT fo.customer_id, fo.first_order_id, COUNT(oi.product_id) as product_count
    FROM first_orders fo
    JOIN order_items oi ON fo.first_order_id = oi.order_id
    GROUP BY fo.customer_id, fo.first_order_id
    HAVING COUNT(oi.product_id) > 1)
SELECT c.customer_id, c.customer_name, foi.product_count
FROM first_order_items foi
JOIN customers c ON foi.customer_id = c.customer_id
ORDER BY foi.product_count DESC;

%python
from pyspark.sql.functions import first, spark_count

window_spec = Window.partitionBy("customer_id").orderBy("order_date")
first_orders = orders_df.withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .select("customer_id", "order_id")

first_order_product_count = first_orders.join(order_items_df, "order_id") \
    .groupBy("customer_id") \
    .agg(spark_count("product_id").alias("product_count")) \
    .filter(col("product_count") > 1)

result = first_order_product_count.join(customers_df, "customer_id")

25. Revenue by month for each category

%sql
WITH category_monthly_revenue AS (
    SELECT p.category_id,
           TRUNC(o.order_date, 'MM') as order_month,
           SUM(oi.quantity * oi.unit_price) as revenue
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
    GROUP BY p.category_id, TRUNC(o.order_date, 'MM'))
SELECT c.category_name, cmr.order_month, cmr.revenue,
       SUM(cmr.revenue) OVER (PARTITION BY cmr.category_id ORDER BY cmr.order_month) as cumulative_revenue,
       RANK() OVER (PARTITION BY cmr.order_month ORDER BY cmr.revenue DESC) as monthly_rank FROM category_monthly_revenue cmr JOIN categories c ON cmr.category_id = c.category_id ORDER BY cmr.order_month, c.category_name;

-- Pivot version for comparison
SELECT *
FROM (
    SELECT p.category_id, TRUNC(o.order_date, 'MM') as order_month,
           SUM(oi.quantity * oi.unit_price) as revenue
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
    GROUP BY p.category_id, TRUNC(o.order_date, 'MM'))
PIVOT (SUM(revenue) FOR order_month IN (DATE '2024-01-01', DATE '2024-02-01', DATE '2024-03-01'));

%python
category_monthly_revenue = orders_df.join(order_items_df, "order_id") \
    .join(products_df, "product_id") \
    .withColumn("order_month", trunc("order_date", "month")) \
    .groupBy("category_id", "order_month") \
    .agg(spark_sum(col("quantity") * col("unit_price")).alias("revenue"))

window_spec = Window.partitionBy("category_id").orderBy("order_month")
rank_window = Window.partitionBy("order_month").orderBy(col("revenue").desc())

result = category_monthly_revenue \
    .withColumn("cumulative_revenue", spark_sum("revenue").over(window_spec)) \
    .withColumn("monthly_rank", rank().over(rank_window)) \
    .join(categories_df, "category_id")

# Pivot version
pivot_result = category_monthly_revenue.groupBy("category_id") \
    .pivot("order_month") \
    .agg(spark_sum("revenue"))