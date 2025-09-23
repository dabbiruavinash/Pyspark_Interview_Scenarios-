# Find top 3 customers who contributed the most to revenue each month.

%python
from pyspark.sql import Window
from pyspark.sql.functions import *

monthly_revenue = transactions.groupBy("customer_id" , date_trunc("month", "txn_date").alias("month")).agg(sum("amount").alias("total_spent"))
window_spec = Window.partitionBy("month").orderBy(desc("total_spent"))
ranked = monthly_revenue.withColumn("rnk", rank().over(window_spec))
result = ranked.filter(col("rnk") <= 3).orderBy("month", desc("total_spent"))
result.show()

%sql
with monthly_revenue as (
select customer_id, trunc(txn_date,'Month') as month, sum(amount) as total_spent from transaction group by customer_id, trunc(txn_date,'Month')),
ranked as (
select *, rank() over (partition by month order by total_spent desc) as rnk from monthly_revenue)
select month, customer_id, total_spent from ranked where rnk <= 3 order by month, total_spent desc;

---
# Find employees who earn more than their manager.

%python
employees.alias("e").join(employees.alias("m"), col("e.manager_id") == col("m.emp_id")).filter(col("e.salary") > col("m.salary").select("e.emp_id", "e.emp_name", "e.salary", col("m.emp_name").alias("manager"), col("m.salary").alias("manager_salary"))

%sql
select e.emp_id, e.emp_name, e.salary, m.emp_name as manager, m.salary as manager_salary from employees e join employees on e.manager_id = m.emp_id where e.salary > m.salary;

---
# Calculate customer churn (customers active in previous month but not in current month).

%python
from pyspark.sql.functions import *

monthly_customers = transactions.select("customer_id", date_trunc("month", "txn_date").alias("month")).distinct()

window_spec = Window.partitionBy("customer_id").order("month")
monthly_customer_lag = monthly_customers.withColumn("next_month", lead("month").over(window_spec))
churn = monthly_cusotmers_lag.filter(months_between("next_month, "month") > 1) | col("next_month").isNull()).filter(months_between(lit("2025-09-01"), "month") >= 1)
churn.show()

%sql
with monthly_customers as (
select distinct customer_id, trunc(txn_date, 'Month') as month from transactions),
churn as (
select prev.customer_id, prev.month as churn_month from monthly_customer prev left join monthly_cusotmers curr on prev.customer_id = curr.customer_id and curr.month = add_months(prev.month,1) where curr.customer_id is null)
select * from churn;

---
# Detect products that were never sold in consecutive months.

%python
monthly_sales = sales.groupBy("product_id", date_trunc("month","txn_date").alias("month")).agg(count("*").alias("sales_count"))

window_spec = Window.partitionBy("product_id").orderBy("month")
gaps = monthly_sales.withColumn("prev_month", lag("month").over(window_spec)).withColumn("month_gap", months_between("month","prev_month"))
result = gap.filter(col("prev_month").isNotNull()) & (col("month_gap") > 1))
result.show()

%sql
with monthly_sales as (
select product_id, trunc(txn_date, 'month') as month from sales group by product_id, trunc(txn_date, 'Month')),
gaps as (
select product_id, month, lag(month) over (partition by product_id order by month) as prev_month from monthly_sales)
select product_id, month from gaps where prev_month is not null and months_between(month, prev_month) > 1;

---
# Running total + YoY growth in same query

%python
yearly = transactions.groupBy(year("txn_date").alias("year")).agg(sum("amount").alias("total_revenue"))
window_spec = Window.orderBy("year")
result = yearly.withColumn("running_total", sum("total_revenue").over(window_spec)).withColumn("prev_year_revenue", lag("total_revenue").over(window_spec)).withColumn("yoy_growth", when(col("prev_year_revenue").isNull(), 0).otherwise(((col("total_revenue") - col("prev_year_revenue"))/ col("prev_year_revenue")) * 100))
result.show()

%sql
with yearly as (
select extract(year from txn_date) as year, sum(amount) as total_revenue from transactions group by extract(year from txn_date))
select year, total_revenue, sum(total_revenue) over (order by year) as running_total, lag(total_revenue) over (order by year) as prev_year_revenue, round(((total_revenue - lag(total_revenue) over (order by year)) / nullif(lag(total_revenue) over (order by year),0)) * 100, 2) as yoy_growth from yearly;

---
# Find the second order of each customer (by date)

%python
window_spec = Window.partitionBy("customer_id").orderBy("order_date")
ranked_orders = orders.withColumn("rn", row_number().over(window_spec))

result = ranked_orders.filter(col("rn") == 2)
result.show()

%sql
with ranked_orders as (
select customer_id, order_id, order_date, row_number() over (partition by customer_id order by order_date) as rn from orders)
select customer_id, order_id, order_date from ranked_orders where rn = 2;

---
# Find customers who placed orders in 3 consecutive months.

%python
monthly = transactions.select("customer_id", date_trunc("month", "txn_date").alias("month")).distinct()

window_spec = Window.partitionBy("customer_id").orderBy("month")
ranked = monthly.withColumn("rn", row_number().over(window_spec))

m1 = ranked.alias("m1")
m2 = ranked.alias("m2")
m3 = ranked.alias("m3")

result = m1.join(m2, (col("m1.customer_id") == col("m2.customer_id")) & (col("m2.rn") == col("m1.rn") + 1).join(m3, (col("m1.customer_id") == col("m3.customer_id")) & (col("m3.rn") == col("m1.rn") + 2)).select("m1.customer_id").distinct()

%sql
WITH monthly AS (
SELECT DISTINCT customer_id, TRUNC(txn_date, 'MONTH') AS month FROM transactions),
ranked AS (
SELECT customer_id, month, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY month) AS rn FROM monthly)
SELECT DISTINCT m1.customer_id FROM ranked m1
JOIN ranked m2 ON m1.customer_id = m2.customer_id AND m2.rn = m1.rn + 1
JOIN ranked m3 ON m1.customer_id = m3.customer_id AND m3.rn = m1.rn + 2;

---
# Find products with the highest sales in each category

%python
window_spec = Window.partitionBy("category").orderBy(desc("sales"))
ranked_products = products.withColumn("rnk", rank().over(window_spec))

result = ranked_products.filter(col("rnk") == 1)

%sql
SELECT product_id, category, sales FROM (
SELECT product_id, category, sales, RANK() OVER (PARTITION BY category ORDER BY sales DESC) AS rnk FROM products) ranked WHERE rnk = 1;

---
# Detect users with transactions from two different countries on the same day.

%python
result = transactions.groupBy("customer_id", "txn_date").agg(countDistinct("country").alias("distinct_countries")).filter(col("distinct_countries") > 1)

%sql
SELECT customer_id, txn_date FROM transactions GROUP BY customer_id, txn_date HAVING COUNT(DISTINCT country) > 1;

---

# Find customers who never placed an order

%python
result = customers.join(orders, "customer_id", "left_anti")

%sql
SELECT c.customer_id, c.name FROM customers c LEFT JOIN orders o ON c.customer_id = o.customer_id WHERE o.order_id IS NULL;

---
# Find top 2 products by revenue in each month

%python
monthly_sales = sales.groupBy("product_id", date_trunc("month", "txn_date").alias("month")).agg(sum("amount").alias("revenue"))

window_spec = Window.partitionBy("month").orderBy("desc("revenue"))
ranked = monthly_sales.withColumn("rnk", rank().over(window_spec))

result = ranked.filter(col("rnk") <= 2)

%sql
WITH monthly_sales AS (
SELECT product_id, TRUNC(txn_date, 'MONTH') AS month, SUM(amount) AS revenue FROM sales GROUP BY product_id, TRUNC(txn_date, 'MONTH')),
ranked AS (
SELECT *, RANK() OVER (PARTITION BY month ORDER BY revenue DESC) AS rnk FROM monthly_sales)
SELECT month, product_id, revenue FROM ranked WHERE rnk <= 2;

---

# Find employees whose salary is above company average but below department average

%python
from pyspark.sql.functions import avg

company_avg = employees.agg(avg("salary").alias("company_avg")).first()[0]
dept_avg = employees.groupBy("dept_id").agg(avg("salary").alias("dept_avg"))

result = employees.join(dept_avg, "dept_id").filter((col("salary") > company_avg) & (col("salary") < col("dept_avg")))
result.show()

%sql
SELECT e.emp_id, e.salary, e.dept_id FROM employees e
WHERE e.salary > (SELECT AVG(salary) FROM employees) AND e.salary < (
SELECT AVG(salary) FROM employees d WHERE d.dept_id = e.dept_id);
---

# Calculate order repeat rate (customers with >1 order /total customers)

%python
customer_orders = orders.groupBy("customer_id").agg(count("*").alias("order_count"))

total_customers = customer_orders.count()
repeat_customers = customer_orders.filter(col("order_count") > 1).count()

repeat_rate = (repeat_customers / total_customers) * 100

%sql
SELECT (COUNT(DISTINCT CASE WHEN order_count > 1 THEN customer_id END) * 100.0) /COUNT(DISTINCT customer_id) AS repeat_rate FROM (
SELECT customer_id, COUNT(*) AS order_count FROM orders GROUP BY customer_id) t;

---
# Find the first transaction of each customer and the difference from their last transaction

%python
from pyspark.sql.functions import datediff

result = transactions.groupBy("customer_id").agg(
    min("txn_date").alias("first_txn"),
    max("txn_date").alias("last_txn")).withColumn("duration", datediff(col("last_txn"), col("first_txn")))

%sql
SELECT customer_id,
       MIN(txn_date) AS first_txn,
       MAX(txn_date) AS last_txn,
       MAX(txn_date) - MIN(txn_date) AS duration FROM transactions GROUP BY customer_id;

---
# Identify the top 5% of customers by lifetime spend

%python
customer_spend = transactions.groupBy("customer_id").agg(sum("amount").alias("total_spent"))

window_spec = Window.orderBy(desc("total_spent"))
ranked = customer_spend.withColumn("pct_rank", percent_rank().over(window_spec))

result = ranked.filter(col("pct_rank") <= 0.05)
result.show()

%sql
WITH customer_spend AS (
    SELECT customer_id, SUM(amount) AS total_spent
    FROM transactions
    GROUP BY customer_id),
ranked AS (
SELECT customer_id, total_spent, PERCENT_RANK() OVER (ORDER BY total_spent DESC) AS pct_rank FROM customer_spend)
SELECT customer_id, total_spent FROM ranked WHERE pct_rank <= 0.05;

---
# Find customers who made transactions in every month of 2024

%python
result = transactions.filter(year("txn_date") == 2024).groupBy("customer_id").agg(countDistinct(month("txn_date")).alias("active_months")).filter(col("active_months") == 12)

%sql
SELECT customer_id FROM transactions WHERE EXTRACT(YEAR FROM txn_date) = 2024 GROUP BY customer_id HAVING COUNT(DISTINCT EXTRACT(MONTH FROM txn_date)) = 12;

---

# Detect revenue drop compared to previous month

%python
monthly = transactions.groupBy(
    date_trunc("month", "txn_date").alias("month")).agg(sum("amount").alias("revenue"))

window_spec = Window.orderBy("month")
result = monthly.withColumn("prev_revenue", lag("revenue").over(window_spec)).filter(col("revenue") < col("prev_revenue"))

%sql
WITH monthly AS (
    SELECT TRUNC(txn_date, 'MONTH') AS month, SUM(amount) AS revenue
    FROM transactions
    GROUP BY TRUNC(txn_date, 'MONTH'))
SELECT month, revenue, LAG(revenue) OVER (ORDER BY month) AS prev_revenue, revenue - LAG(revenue) OVER (ORDER BY month) AS diff FROM monthly WHERE revenue < LAG(revenue) OVER (ORDER BY month);

---

# Find users who never skipped a day of transactions

%python
days = transactions.groupBy("customer_id").agg(
    countDistinct("txn_date").alias("active_days"),
    datediff(max("txn_date"), min("txn_date")).alias("total_days_range")).withColumn("total_days", col("total_days_range") + 1)

result = days.filter(col("active_days") == col("total_days"))

%sql
WITH days AS (
SELECT customer_id, COUNT(DISTINCT txn_date) AS active_days, MAX(txn_date) - MIN(txn_date) + 1 AS total_days FROM transactions GROUP BY customer_id)
SELECT customer_id FROM days WHERE active_days = total_days;

----
# Detect customers who upgraded spending month-over-month (strictly increasing)

%python

monthly = transactions.groupBy(
    "customer_id", 
    date_trunc("month", "txn_date").alias("month")).agg(sum("amount").alias("revenue"))

window_spec = Window.partitionBy("customer_id").orderBy("month")
ranked = monthly.withColumn("prev_revenue", lag("revenue").over(window_spec))

# Check if revenue always increases
result = ranked.groupBy("customer_id").agg(
    min(when((col("revenue") > col("prev_revenue")) | col("prev_revenue").isNull(), 1).otherwise(0)).alias("always_increasing"),
    max(when(col("revenue") <= col("prev_revenue"), 1).otherwise(0)).alias("has_decrease")).filter((col("always_increasing") == 1) & (col("has_decrease") == 0))

%sql
WITH monthly AS (
    SELECT customer_id, TRUNC(txn_date, 'MONTH') AS month, SUM(amount) AS revenue
    FROM transactions
    GROUP BY customer_id, TRUNC(txn_date, 'MONTH')),
ranked AS (
SELECT customer_id, month, revenue, LAG(revenue) OVER (PARTITION BY customer_id ORDER BY month) AS prev_revenue FROM monthly)
SELECT DISTINCT customer_id FROM ranked GROUP BY customer_id
HAVING MIN(CASE WHEN revenue > prev_revenue OR prev_revenue IS NULL THEN 1 ELSE 0 END) = 1
AND MAX(CASE WHEN revenue <= prev_revenue THEN 1 ELSE 0 END) = 0;

---

# Find the most common sequence of two products bought together

%python
window_spec = Window.partitionBy("customer_id").orderBy("order_date")
ordered = orders.withColumn("rn", row_number().over(window_spec))

o1 = ordered.alias("o1")
o2 = ordered.alias("o2")

pairs = o1.join(o2, 
    (col("o1.customer_id") == col("o2.customer_id")) & 
    (col("o2.rn") == col("o1.rn") + 1)).select(
    "o1.customer_id",
    col("o1.product_id").alias("first_product"),
    col("o2.product_id").alias("second_product"))

result = pairs.groupBy("first_product", "second_product").agg(count("*").alias("pair_count")).orderBy(desc("pair_count")).limit(1)

%sql
WITH ordered AS (
    SELECT customer_id, product_id,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS rn
    FROM orders),
pairs AS (
SELECT o1.customer_id, o1.product_id AS first_product, o2.product_id AS second_product FROM ordered o1 JOIN ordered o2 ON o1.customer_id = o2.customer_id AND o2.rn = o1.rn + 1)
SELECT first_product, second_product, COUNT(*) AS pair_count FROM pairs GROUP BY first_product, second_product ORDER BY pair_count DESC FETCH FIRST 1 ROW ONLY;

----
# Identify employees who directly or indirectly report to a given manager

%python
# Note: PySpark requires specific recursive CTE handling or iterative approach
# This is a simplified version using iterative approach
def find_subordinates(manager_id, max_iterations=10):
    subordinates = employees.filter(col("manager_id") == manager_id)
    all_subordinates = subordinates
    
    for i in range(max_iterations):
        new_subordinates = employees.alias("e").join(
            subordinates.alias("s"), 
            col("e.manager_id") == col("s.emp_id")
        ).select("e.*")
        
        if new_subordinates.count() == 0:
            break
            
        all_subordinates = all_subordinates.union(new_subordinates)
        subordinates = new_subordinates
    
    return all_subordinates

result = find_subordinates(101)

%sql
WITH RECURSIVE subordinates AS (
    SELECT emp_id, emp_name, manager_id
    FROM employees
    WHERE manager_id = 101  -- given manager
    UNION ALL
    SELECT e.emp_id, e.emp_name, e.manager_id
    FROM employees e
    JOIN subordinates s ON e.manager_id = s.emp_id)
SELECT * FROM subordinates;

---
# Find the longest streak of consecutive login days per user

%python
numbered = logins.withColumn(
    "rn", 
    row_number().over(Window.partitionBy("user_id").orderBy("login_date")))

grouped = numbered.withColumn(
    "grp_date", 
    expr("date_sub(login_date, rn)"))

streaks = grouped.groupBy("user_id", "grp_date").agg(
    count("*").alias("streak_length"))

result = streaks.groupBy("user_id").agg(max("streak_length").alias("longest_streak"))

%sql
WITH numbered AS (
SELECT user_id, login_date, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date) AS rn FROM logins),
grouped AS (
SELECT user_id, login_date, login_date - rn AS grp FROM numbered)
SELECT user_id, COUNT(*) AS streak FROM grouped GROUP BY user_id, grp ORDER BY streak DESC FETCH FIRST 1 ROW ONLY;

----
# Find customers whose spend contributes to 80% of total revenue (Pareto analysis)

%python
customer_spend = transactions.groupBy("customer_id").agg(
    sum("amount").alias("total_spent"))

window_spec = Window.orderBy(desc("total_spent"))
ranked = customer_spend.withColumn(
    "grand_total", sum("total_spent").over(Window.partitionBy())).withColumn(
    "running_total", 
    sum("total_spent").over(window_spec))

result = ranked.filter(col("running_total") <= col("grand_total") * 0.8)

%sql
WITH ranked AS (
    SELECT customer_id, SUM(amount) AS total_spent,
           SUM(SUM(amount)) OVER () AS grand_total,
           SUM(SUM(amount)) OVER (ORDER BY SUM(amount) DESC) AS running_total FROM transactions GROUP BY customer_id)
SELECT customer_id, total_spent FROM ranked WHERE running_total <= 0.8 * grand_total;

---
# Detect orders placed using more than one payment method

%python
result = payments.groupBy("order_id").agg(countDistinct("payment_method").alias("payment_methods_count")).filter(col("payment_methods_count") > 1)

%sql
SELECT order_id FROM payments GROUP BY order_id HAVING COUNT(DISTINCT payment_method) > 1;

---
# Calculate rolling 3-month retention rate

%python
monthly = transactions.select(
    "customer_id", 
    date_trunc("month", "txn_date").alias("month")
).distinct()

# Create cohort table
cohort = monthly.alias("m1").join(
    monthly.alias("m2"),
    (col("m1.customer_id") == col("m2.customer_id")) &
    (col("m2.month").between(col("m1.month"), add_months(col("m1.month"), 2)))).select(
    "m1.customer_id",
    col("m1.month").alias("start_month"),
    col("m2.month").alias("active_month"))

result = cohort.groupBy("start_month").agg(
    countDistinct("customer_id").alias("cohort_size"),
    countDistinct(when(col("active_month") == add_months(col("start_month"), 2), "customer_id")).alias("retained_after_3_months")).withColumn(
    "retention_rate", 
    (col("retained_after_3_months") / col("cohort_size")) * 100)

%sql
WITH monthly AS (
    SELECT DISTINCT customer_id, TRUNC(txn_date, 'MONTH') AS month
    FROM transactions),
cohort AS (
    SELECT m1.customer_id, m1.month AS start_month, m2.month AS active_month
    FROM monthly m1
    JOIN monthly m2 ON m1.customer_id = m2.customer_id AND m2.month BETWEEN m1.month AND ADD_MONTHS(m1.month, 2))
SELECT start_month,
       COUNT(DISTINCT customer_id) AS cohort_size,
       COUNT(DISTINCT CASE WHEN active_month = ADD_MONTHS(start_month, 2) THEN customer_id END) AS retained_after_3_months,
       (COUNT(DISTINCT CASE WHEN active_month = ADD_MONTHS(start_month, 2) THEN customer_id END) * 100.0 /
        COUNT(DISTINCT customer_id)) AS retention_rate FROM cohort GROUP BY start_month;


