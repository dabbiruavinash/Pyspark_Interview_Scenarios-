# Top 3 Customers by Monthly Revenue

%sql
with monthly_revenue as (
select customer_id, trunc(txn_date, 'month') as month, sum(amount) as total_spent from transactions group by customer_id, trunc(txn_date, 'month')),
ranked as (
select monthly_revenue.*, rank() over (partition by month order by total_spent desc) as rank from monthly_revenue)
select month, customer_id, total_spent from ranked where rnk <= 3 order by month, total_spent desc;

%python
from pyspark.sql import Window
from pyspark.sql.functions import col, sum, date_trunc, rank

monthly_revenue_df = (transactions_df.withColumn("month", date_trunc("month", "txn_date")).groupBy("customer_id", "month").agg(sum("amount").alias("total_spent")))

window_spec = Window.partitionBy("month").orderBy(col("total_spent").desc())
ranked_df = monthly_revenue_df.withColumn("rnk", rank().over(window_spec))

result_df = ranked_df.filter(col("rnk") <= 3).orderBy("month", col("total_spent").desc())
result_df.show()

# Employees Earning More Than Their Manager

%sql
select e.emp_id, e.emp_name, e.salary, m.emp_name as manager_name, m.salary as manager_salary from employees e join employees m on e.manager_id = m.emp_id where e.salary > m.salary;

%python
employee_alias_df = employees_df.alias("e")
managers_alias_df = employees_df.alias("m")

result_df = (employees_alias_df.join(managers_alias_df, col("e.manager_id") == col("m.emp_id")
.select(col("e.emp_id"), col("e.emp_name"), col("e.salary"), col("m.emp_name").alias("manager_name"), col("m.salary").alias("manager_salary"))
.filter(col("e.salary") > col("m.salary")))
result_df.show()

# Customer Churn (Month-over-Month)

%sql
with monthly_customers as (
select distinct customer_id, trunc(txn_date, "month") as month from transactions),
churn as (
select prev.customer_id, prev.month as churn_month from monthly_customers prev left join monthly_customers curr on prev.customer_id = curr.customer_id and curr.month = add_months(prev.month,1) where curr.customer_id is null);

select * from churn;

%python
monthly_customers_df = (transactions_df.select("customer_id", date_trunc("month", "txn_date").alias("month"))
.distinct())

prev_month_df = monthly_customer_df.alias("prev")
curr_month_df = monthly_customers_df.alias("curr")

churn_df = (prev_month_df.join(curr_month_df, (col("prev.customer_id") == col("curr.customer_id")) & (col("curr,month") == add_months(col("prev.month"), 1)), "left_outer")
.filter(col("curr.customer_id").isNull())
.select(col("prev.customer_id"), col("prev.month").alias("churn_month")))
churn_df.show()

# Products Never Sold in Consecutive Months

%sql
with monthly_sales as (
select product_id, trunc(txn_date, 'month') as month from sales group by product_id, trunc(txn_date, 'month')),
gaps as (
select product_id, month, lag(month) over (partition by product_id order by month) as prev_month from monthly_sales)
select product_id , month from gaps where prev_month is not null and months_between(month, prev_month) > 1;

%python
from pyspark.sql.functions import lag, months_between
from pyspark.sql.window import Window

monthly_sales_df = (sales_df
.groupBy("product_id", date_trunc("month", "txn_date").alias("month"))
.count()
.select("product_id", "month"))

window_spec = Window.partitionBy("product_id").orderBy("month")
gaps_df = monthly_sales_df.withColumn("prev_month", lag("month").over(window_spec))

result_df = gaps_df.fitler(col("prev_month").isNotNull() & (months_between("month", "prev_month") > 1)).select("product_id", "month")
result_df.show()

# Running Total and YoY Growth

%sql
with yearly as (
select year(txn_date) as year, sum(amount) as total_revenue from transactions group by year(txn_date))
select year, total_revenue, sum(total_revenue) over (order by year) as running_total, lag(total_revenue) over (order by year) as prev_year_revenue ,
round(((total_revenue - lag(total_revenue) over (order by year))/ nullif(lag(total_revenue) over (order by year), 0)) * 100, 2) as yoy_growth from yearly order by year;

%python
from pyspark.sql.functions import year, round, lag, sum as spark_sum
from pyspark.sql.window import window

yearly_df = (transactions_df
    .withColumn("year", year("txn_date"))
    .groupBy("year")
    .agg(spark_sum("amount").alias("total_revenue")))

window_spec = Window.orderBy("year")
result_df = (yearly_df
    .withColumn("running_total", spark_sum("total_revenue").over(window_spec))
    .withColumn("prev_year_revenue", lag("total_revenue").over(window_spec))
    .withColumn("yoy_growth", 
                round(
                    (col("total_revenue") - col("prev_year_revenue")) 
                    / col("prev_year_revenue") * 100, 2)))
result_df.show()

# Second Order per Customer

%sql
WITH ranked_orders AS (
    SELECT 
        customer_id, 
        order_id, 
        order_date,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS order_seq FROM orders)
SELECT 
    customer_id, 
    order_id, 
    order_date FROM ranked_orders WHERE order_seq = 2;

%python
from pyspark.sql.window import Window

window_spec = Window.partitionBy("customer_id").orderBy("order_date")
ranked_orders_df = orders_df.withColumn("order_seq", row_number().over(window_spec))

result_df = ranked_orders_df.filter(col("order_seq") == 2)
result_df.show()

# Customers with Orders in 3 Consecutive Months

%sql
WITH monthly_orders AS (
SELECT DISTINCT customer_id, TRUNC(txn_date, 'MONTH') AS month FROM transactions),
ranked AS (
SELECT customer_id, month, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY month) AS rn FROM monthly_orders)
SELECT DISTINCT m1.customer_id FROM ranked m1
JOIN ranked m2 ON m1.customer_id = m2.customer_id AND m2.rn = m1.rn + 1
JOIN ranked m3 ON m1.customer_id = m3.customer_id AND m3.rn = m1.rn + 2;

%python
monthly_orders_df = (transactions_df
    .select("customer_id", date_trunc("month", "txn_date").alias("month"))
    .distinct()
)

window_spec = Window.partitionBy("customer_id").orderBy("month")
ranked_df = monthly_orders_df.withColumn("rn", row_number().over(window_spec))

m1_df = ranked_df.alias("m1")
m2_df = ranked_df.alias("m2")
m3_df = ranked_df.alias("m3")

result_df = (m1_df
    .join(m2_df, (col("m1.customer_id") == col("m2.customer_id")) & (col("m2.rn") == col("m1.rn") + 1))
    .join(m3_df, (col("m1.customer_id") == col("m3.customer_id")) & (col("m3.rn") == col("m1.rn") + 2))
    .select(col("m1.customer_id"))
    .distinct())
result_df.show()

# Highest Selling Product in Each Category

%sql
SELECT product_id, category, sales FROM (
SELECT product_id, category, sales, RANK() OVER (PARTITION BY category ORDER BY sales DESC) AS rnk FROM products) ranked WHERE rnk = 1;

%python
from pyspark.sql.window import Window

window_spec = Window.partitionBy("category").orderBy(col("sales").desc())
ranked_df = products_df.withColumn("rnk", rank().over(window_spec))

result_df = ranked_df.filter(col("rnk") == 1)
result_df.show()

# Users with Transactions in Multiple Countries Same Day

%sql
SELECT customer_id, txn_date FROM transactions GROUP BY customer_id, txn_date HAVING COUNT(DISTINCT country) > 1;

%python
result_df = (transactions_df
    .groupBy("customer_id", "txn_date")
    .agg(countDistinct("country").alias("unique_countries"))
    .filter(col("unique_countries") > 1)
    .select("customer_id", "txn_date"))
result_df.show()

# Customers Who Never Placed an Order

%sql
SELECT c.customer_id, c.name FROM customers c LEFT JOIN orders o ON c.customer_id = o.customer_id WHERE o.order_id IS NULL;

%python
result_df = (customers_df
    .join(orders_df, "customer_id", "left_outer")
    .filter(col("order_id").isNull())
    .select("customer_id", "name"))
result_df.show()

# Top 2 Products by Revenue Each Month

%sql
WITH monthly_sales AS (
SELECT product_id, TRUNC(txn_date, 'MONTH') AS month, SUM(amount) AS revenue FROM sales GROUP BY product_id, TRUNC(txn_date, 'MONTH')),
ranked AS (
SELECT  *, RANK() OVER (PARTITION BY month ORDER BY revenue DESC) AS rnk FROM monthly_sales)
SELECT month, product_id, revenue FROM ranked WHERE rnk <= 2 ORDER BY month, revenue DESC;

%python
from pyspark.sql.window import Window

monthly_sales_df = (sales_df
    .withColumn("month", date_trunc("month", "txn_date"))
    .groupBy("product_id", "month")
    .agg(sum("amount").alias("revenue")))

window_spec = Window.partitionBy("month").orderBy(col("revenue").desc())
ranked_df = monthly_sales_df.withColumn("rnk", rank().over(window_spec))

result_df = ranked_df.filter(col("rnk") <= 2).orderBy("month", col("revenue").desc())
result_df.show()

# Employees with Salary Above Company Avg but Below Dept Avg

%sql
SELECT 
    emp_id,
    salary,
    dept_id FROM employees e WHERE salary > (SELECT AVG(salary) FROM employees) AND salary < (SELECT AVG(salary) FROM employees d WHERE d.dept_id = e.dept_id);

%python
from pyspark.sql.functions import avg

overall_avg = employees_df.agg(avg("salary")).first()[0]

dept_avg_df = employees_df.groupBy("dept_id").agg(avg("salary").alias("dept_avg_salary"))

result_df = (employees_df
    .join(dept_avg_df, "dept_id")
    .filter((col("salary") > overall_avg) & (col("salary") < col("dept_avg_salary")))
    .select("emp_id", "salary", "dept_id"))
result_df.show()

# Order Repeat Rate

%sql
SELECT (COUNT(DISTINCT CASE WHEN order_count > 1 THEN customer_id END) * 100.0) / COUNT(DISTINCT customer_id) AS repeat_rate FROM (
SELECT customer_id, COUNT(*) AS order_count FROM orders GROUP BY customer_id) t;

%python
from pyspark.sql.functions import count, countDistinct, when

customer_orders_df = orders_df.groupBy("customer_id").agg(count("*").alias("order_count"))

total_customers = customer_orders_df.count()
repeat_customers = customer_orders_df.filter(col("order_count") > 1).count()

repeat_rate = (repeat_customers * 100.0) / total_customers
print(f"Repeat Rate: {repeat_rate:.2f}%")

# First Transaction and Duration to Last Transaction

%sql
SELECT 
    customer_id,
    MIN(txn_date) AS first_txn,
    MAX(txn_date) AS last_txn,
    MAX(txn_date) - MIN(txn_date) AS duration_days FROM transactions GROUP BY customer_id;

%python
from pyspark.sql.functions import min, max, datediff

result_df = (transactions_df
    .groupBy("customer_id")
    .agg(
        min("txn_date").alias("first_txn"),
        max("txn_date").alias("last_txn"),
        datediff(max("txn_date"), min("txn_date")).alias("duration_days")))
result_df.show()

# Top 5% Customers by Lifetime Spend

%sql
WITH customer_spend AS (
SELECT customer_id, SUM(amount) AS total_spent FROM transactions GROUP BY customer_id),
ranked AS (
SELECT customer_id, total_spent, PERCENT_RANK() OVER (ORDER BY total_spent DESC) AS pct_rank FROM customer_spend)
SELECT customer_id,total_spent FROM ranked WHERE pct_rank <= 0.05;

%python
from pyspark.sql.functions import percent_rank

customer_spend_df = transactions_df.groupBy("customer_id").agg(sum("amount").alias("total_spent"))

window_spec = Window.orderBy(col("total_spent").desc())
ranked_df = customer_spend_df.withColumn("pct_rank", percent_rank().over(window_spec))

result_df = ranked_df.filter(col("pct_rank") <= 0.05)
result_df.show()

# Customers Active Every Month in 2024

%sql
SELECT customer_id FROM transactions WHERE EXTRACT(YEAR FROM txn_date) = 2024 GROUP BY customer_id HAVING COUNT(DISTINCT EXTRACT(MONTH FROM txn_date)) = 12;

%python
from pyspark.sql.functions import year, month, countDistinct

result_df = (transactions_df
    .filter(year("txn_date") == 2024)
    .groupBy("customer_id")
    .agg(countDistinct(month("txn_date")).alias("active_months"))
    .filter(col("active_months") == 12)
    .select("customer_id"))

result_df.show()

#  Revenue Drop Compared to Previous Month

%sql
WITH monthly AS (
SELECT TRUNC(txn_date, 'MONTH') AS month, SUM(amount) AS revenue FROM transactions GROUP BY TRUNC(txn_date, 'MONTH'))
SELECT 
    month,
    revenue, LAG(revenue) OVER (ORDER BY month) AS prev_revenue, revenue - LAG(revenue) OVER (ORDER BY month) AS diff FROM monthly WHERE revenue < LAG(revenue) OVER (ORDER BY month);

%python
from pyspark.sql.window import Window

monthly_df = (transactions_df
    .withColumn("month", date_trunc("month", "txn_date"))
    .groupBy("month")
    .agg(sum("amount").alias("revenue")))

window_spec = Window.orderBy("month")
result_df = (monthly_df
    .withColumn("prev_revenue", lag("revenue").over(window_spec))
    .withColumn("diff", col("revenue") - col("prev_revenue"))
    .filter(col("revenue") < col("prev_revenue")))
result_df.show()

# Users Who Never Skipped a Day of Transactions

%sql
WITH days AS (
SELECT customer_id, COUNT(DISTINCT txn_date) AS active_days, MAX(txn_date) - MIN(txn_date) + 1 AS total_days FROM transactions GROUP BY customer_id)
SELECT customer_id FROM days WHERE active_days = total_days;

%python
from pyspark.sql.functions import countDistinct, datediff

result_df = (transactions_df
    .groupBy("customer_id")
    .agg(countDistinct("txn_date").alias("active_days"),(datediff(max("txn_date"), min("txn_date")) + 1).alias("total_days"))
    .filter(col("active_days") == col("total_days"))
    .select("customer_id"))
result_df.show()

# Customers with Strictly Increasing Monthly Spending

%sql
WITH monthly AS (
SELECT customer_id,TRUNC(txn_date, 'MONTH') AS month, SUM(amount) AS revenue FROM transactions GROUP BY customer_id, TRUNC(txn_date, 'MONTH')),
ranked AS (
SELECT customer_id, month, revenue, LAG(revenue) OVER (PARTITION BY customer_id ORDER BY month) AS prev_revenue FROM monthly)
SELECT DISTINCT customer_id FROM ranked GROUP BY customer_id HAVING MIN(CASE WHEN revenue > prev_revenue OR prev_revenue IS NULL THEN 1 ELSE 0 END) = 1 AND MAX(CASE WHEN revenue <= prev_revenue THEN 1 ELSE 0 END) = 0;

%python
from pyspark.sql.window import Window

monthly_df = (transactions_df
    .withColumn("month", date_trunc("month", "txn_date"))
    .groupBy("customer_id", "month")
    .agg(sum("amount").alias("revenue")))

window_spec = Window.partitionBy("customer_id").orderBy("month")
lagged_df = monthly_df.withColumn("prev_revenue", lag("revenue").over(window_spec))

flagged_df = lagged_df.withColumn("is_increasing", when((col("revenue") > col("prev_revenue")) | col("prev_revenue").isNull(), 1).otherwise(0))
.withColumn("has_decrease", when(col("revenue") <= col("prev_revenue"), 1).otherwise(0))

result_df = (flagged_df
    .groupBy("customer_id")
    .agg(min("is_increasing").alias("always_increasing"),max("has_decrease").alias("ever_decreased"))
    .filter((col("always_increasing") == 1) & (col("ever_decreased") == 0))
    .select("customer_id"))

result_df.show()

# Most Common Sequence of Two Products Bought Together

%sql
WITH ordered AS (
    SELECT 
        customer_id,
        product_id,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS seq FROM orders),
pairs AS (
    SELECT 
        o1.customer_id,
        o1.product_id AS first_product,
        o2.product_id AS second_product FROM ordered o1 JOIN ordered o2 ON o1.customer_id = o2.customer_id AND o2.seq = o1.seq + 1)
SELECT 
    first_product,
    second_product,
    COUNT(*) AS pair_count FROM pairs GROUP BY first_product, second_product ORDER BY pair_count DESC FETCH FIRST 1 ROW ONLY;

%python
from pyspark.sql.window import Window

ordered_df = orders_df.withColumn(
    "seq", row_number().over(Window.partitionBy("customer_id").orderBy("order_date"))
)

o1_df = ordered_df.alias("o1")
o2_df = ordered_df.alias("o2")

pairs_df = (o1_df
    .join(o2_df, 
          (col("o1.customer_id") == col("o2.customer_id")) & 
          (col("o2.seq") == col("o1.seq") + 1))
    .select(
        col("o1.product_id").alias("first_product"),
        col("o2.product_id").alias("second_product")))

result_df = (pairs_df
    .groupBy("first_product", "second_product")
    .count()
    .orderBy(col("count").desc())
    .limit(1))
result_df.show()

# Employees Reporting Directly or Indirectly to a Manager

%sql
WITH RECURSIVE subordinates AS (
SELECT emp_id,  emp_name, manager_id FROM employees WHERE manager_id = 101  -- given manager ID
UNION ALL
SELECT e.emp_id, e.emp_name, e.manager_id FROM employees e JOIN subordinates s ON e.manager_id = s.emp_id)

SELECT * FROM subordinates;

%python
def find_subordinates(manager_id, depth=0, max_depth=10):
    if depth > max_depth:
        return spark.createDataFrame([], employees_df.schema)
    
    direct_reports = employees_df.filter(col("manager_id") == manager_id)
    
    if direct_reports.count() == 0:
        return direct_reports
    
    child_reports = None
    for row in direct_reports.collect():
        child_df = find_subordinates(row.emp_id, depth + 1, max_depth)
        if child_reports is None:
            child_reports = child_df
        else:
            child_reports = child_reports.union(child_df)
    
    return direct_reports.union(child_reports) if child_reports else direct_reports

result_df = find_subordinates(101)
result_df.show()

# Longest Streak of Consecutive Login Days

%sql
WITH numbered AS (
SELECT user_id, login_date, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date) AS rn FROM logins),
grouped AS (
SELECT user_id, login_date, login_date - rn AS grp FROM numbered),
streaks AS (
SELECT user_id, grp, COUNT(*) AS streak_length FROM grouped GROUP BY user_id, grp)
SELECT user_id, MAX(streak_length) AS longest_streak FROM streaks GROUP BY user_id;

%python
from pyspark.sql.window import Window

numbered_df = logins_df.withColumn(
    "rn", row_number().over(Window.partitionBy("user_id").orderBy("login_date")))

grouped_df = numbered_df.withColumn("grp", expr("date_sub(login_date, rn)"))

streaks_df = (grouped_df
    .groupBy("user_id", "grp")
    .agg(count("*").alias("streak_length")))

result_df = streaks_df.groupBy("user_id").agg(max("streak_length").alias("longest_streak"))
result_df.show()

# Customers Contributing to 80% of Total Revenue

%sql
WITH ranked AS (
    SELECT 
        customer_id,
        SUM(amount) AS total_spent,
        SUM(SUM(amount)) OVER () AS grand_total,
        SUM(SUM(amount)) OVER (ORDER BY SUM(amount) DESC) AS running_total FROM transactions GROUP BY customer_id)
SELECT customer_id, total_spent FROM ranked WHERE running_total <= 0.8 * grand_total;

%python
customer_spend_df = transactions_df.groupBy("customer_id").agg(sum("amount").alias("total_spent"))

grand_total = customer_spend_df.agg(sum("total_spent").alias("grand_total")).first()[0]

window_spec = Window.orderBy(col("total_spent").desc())
ranked_df = customer_spend_df.withColumn(
    "running_total", sum("total_spent").over(window_spec))

result_df = ranked_df.filter(col("running_total") <= 0.8 * grand_total)
result_df.show()

# Orders with Multiple Payment Methods

%sql
SELECT order_id FROM payments GROUP BY order_id HAVING COUNT(DISTINCT payment_method) > 1;

%python
result_df = (payments_df
    .groupBy("order_id")
    .agg(countDistinct("payment_method").alias("payment_methods"))
    .filter(col("payment_methods") > 1)
    .select("order_id"))
result_df.show()

# Rolling 3-Month Retention Rate

%sql
WITH monthly AS (
SELECT DISTINCT customer_id, TRUNC(txn_date, 'MONTH') AS month FROM transactions),
cohort AS (
SELECT m1.customer_id, m1.month AS start_month, m2.month AS active_month FROM monthly m1 JOIN monthly m2 ON m1.customer_id = m2.customer_id AND m2.month BETWEEN m1.month AND ADD_MONTHS(m1.month, 2))
SELECT 
    start_month,
    COUNT(DISTINCT customer_id) AS cohort_size,
    COUNT(DISTINCT CASE WHEN active_month = ADD_MONTHS(start_month, 2) THEN customer_id END) AS retained_after_3_months,
    ROUND(
        COUNT(DISTINCT CASE WHEN active_month = ADD_MONTHS(start_month, 2) THEN customer_id END) * 100.0 /
        COUNT(DISTINCT customer_id), 2) AS retention_rate FROM cohort GROUP BY start_month ORDER BY start_month;