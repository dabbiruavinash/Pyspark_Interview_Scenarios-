# 5 HARD Pyspark and SQL Scenario-Based Questions

# Customer Retention Drop Analysis 
Scenario: Identify customers whose order frequency dropped month-over-month.
Table: orders
order_id | customer_id | order_date | order_value
Task: Find customers whose order count decreased compared to the previous month.

%sql
with monthly_orders as (
select customer_id, trunc(month(order_date)), count(*) as order_count from orders group by customer_id, trunc(month(order_date))),
monthly_comparison as (
select customer_id, order_month, order_count, lag(order_count) over (partition by customer_id order by order_month) as prev_month_count from monthly_orders)
select customer_id, order_month, order_count, prev_month_count 
case when order_count < prev_month_count then 'Decreased'
when order_count = prev_month_count then 'same' else 'Increased' end as trend from monthly_comparison
where prev_month_count is not null and order_count < prev_month_count order by customer_id, order_month;

%python
from pyspark.sql import Window
from pyspark.sql.functions import col, count, lag, date_trunc, when

monthly_orders = (orders
.withColumn("order_month", date_trunc("month", "order_date"))
.groupBy("customer_id", "order_month").agg(count("*").alias("order_count")))

window_spec = Window.partitionBy("customer_id").orderBy("order_month")

retention_drop = (monthly_orders
.withColumn("prev_month_count", lag("order_count").over(window_spec))
.filter(col("prev_month_count").isNotNull())
.filter(col("order_count") < col("prev_month_count"))
.select("customer_id", "order_month", "order_count", "prev_month_count"))

# Revenue Contribution with Ranking 
Scenario: Management wants to know which products contribute the top 80% of revenue.
Table: sales
order_id | product_id | order_date | revenue
Task: Return products contributing to cumulative 80% revenue, ordered by contribution.

%sql
with product_revenue as (
select product_id, sum(revenue) as total_revenue from sales group by product_id),
revenue_metrics as (
select product_id, total_revenue, sum(total_revenue) over (order by total_revenue desc) as running_total,
sum(total_revenue) over () as total_all from product_revenue)
select product_id, total_revenue, round((running_total/total_all) * 100, 2) as cumulative_percentage from revenue_metrics where running_total/ total_all <= 0.8 order by total_revenue desc;

%python
from pyspark.sql import Window
from pyspark.sql.functions import sum, round, desc, col

product_revenue = sales.groupBy("product_id").agg(sum("revenue").alias("total_revenue"))
window_spec = Window.orderBy(desc("total_revenue"))
revenue_contribution = (product_revenue
.withColumn("running_total", sum("total_revenue").over(window_spec.rowBetween(Window.unboundedPreceding,0)))
.withColumn("total_all", sum("total_revenue").over(Window.partitionBy()))
.withColumn("cumulative_percentage", round(col("running_total")/col("total_all") * 100, 2))
.filter(col("running_total")/col("total_all") <= 0.8).orderBy(desc("total_revenue")))

# Consecutive Activity Detection 
Scenario: Identify users who were active for 3 or more consecutive days.
Table: user_activity
user_id | activity_date
Task: Return user_id and the start & end date of each consecutive activity streak.

%sql
with activity_groups as (
select user_id, activity_date, activity_date - interval '1day' * row_number() over(partition by user_id order by activity_date) as grp from user_activity group by user_id, activity_date),
consective_days as (
select user_id, grp, min(activity_date) as streak_start, max(activity_date) as streak_end, count(*) as streak_length from activity_groups group by user_id, grp)
select user_id, streak_start, streak_end, streak_length from consecutive_days where streak_length >= 3 order by user_id, streak_start;

%python
from pyspark.sql import Window
from pyspark.sql.functions import col, date_sub, row_number, min, max, count

window_spec = Window.partitionBy("user_id").orderBy("activity_date")
activity_groups = (user_activity
.distinct()
.withColumn("rn", row_number().over(window_spec))
.withColumn("grp_date", date_sub("activity_date", col("rn"))))

consective_activity = (activity_groups
.groupBy("user_id", "grp_date")
.agg(
min("activity_date").alias("streak_start"),
max("activity_date").alias("streak_end"), count("*").alias("streak_length"))
.filter(col("streak_length") >= 3)
.select("user_id", "streak_start", "streak_end", "streak_length").orderBy("user_id", "streak_start"))

# Salary Anomaly Detection 💼
Scenario: Find employees earning more than their manager.
Table: employees
emp_id | emp_name | salary | manager_id
Task: Return employee name, salary, manager name, and manager salary.

%sql
select e.emp_name as employee_name,e.salary as employee_salary, m.emp_name as manager_name, m.salary as manager_salary from employees e inner join employees m on e.manager_id = m.emp_id where e.salary > m.salary order by (e.salary - m.salary) desc;

%python
emp_df = employees.alias("emp")
mgr_df = employees.alias("mgr")

salary_anomaly = (emp_df
    .join(mgr_df, emp_df.manager_id == mgr_df.emp_id, "inner")
    .filter(emp_df.salary > mgr_df.salary)
    .select(
        emp_df.emp_name.alias("employee_name"),
        emp_df.salary.alias("employee_salary"),
        mgr_df.emp_name.alias("manager_name"),
        mgr_df.salary.alias("manager_salary")).orderBy((emp_df.salary - mgr_df.salary).desc()))

# Cohort Retention Analysis 
Scenario: Measure how many users return in subsequent months after signup.
Tables:
 users
 user_id | signup_date

orders
order_id | user_id | order_date
Task: Build a monthly cohort retention table showing retention % for Month-0, Month-1, Month-2…

%sql
WITH user_cohorts AS (
  SELECT 
    user_id, DATE_TRUNC('month', signup_date) AS cohort_month FROM users),

monthly_activity AS (
  SELECT 
    u.user_id,
    u.cohort_month,
    DATE_TRUNC('month', o.order_date) AS activity_month,
    EXTRACT(MONTH FROM AGE(DATE_TRUNC('month', o.order_date), u.cohort_month)) AS months_since_signup FROM user_cohorts u LEFT JOIN orders o ON u.user_id = o.user_id AND o.order_date >= u.cohort_month),

cohort_sizes AS (
  SELECT 
    cohort_month,
    COUNT(DISTINCT user_id) AS cohort_size FROM user_cohorts GROUP BY cohort_month),

retention_data AS (
  SELECT 
    cohort_month,
    months_since_signup,
    COUNT(DISTINCT user_id) AS retained_users
  FROM monthly_activity WHERE months_since_signup IS NOT NULL GROUP BY cohort_month, months_since_signup)

SELECT 
  r.cohort_month,
  c.cohort_size,
  r.months_since_signup,
  r.retained_users,
  ROUND(r.retained_users * 100.0 / c.cohort_size, 2) AS retention_pct FROM retention_data r JOIN cohort_sizes c ON r.cohort_month = c.cohort_month ORDER BY r.cohort_month, r.months_since_signup;

%python
from pyspark.sql.functions import date_trunc, countDistinct, col, month, datediff, round

user_cohorts = users.select(
    "user_id", date_trunc("month", "signup_date").alias("cohort_month"))

cohort_sizes = user_cohorts.groupBy("cohort_month").agg(
    countDistinct("user_id").alias("cohort_size"))

monthly_activity = (user_cohorts
    .join(orders, ["user_id"], "left")
    .filter((orders.order_date >= user_cohorts.cohort_month) | (orders.order_date.isNull()))
    .withColumn("activity_month", date_trunc("month", "order_date"))
    .withColumn("months_since_signup", (datediff("activity_month", "cohort_month") / 30).cast("int")))

retention_data = (monthly_activity
    .filter(col("months_since_signup").isNotNull())
    .groupBy("cohort_month", "months_since_signup")
    .agg(countDistinct("user_id").alias("retained_users")))

retention_table = (retention_data
    .join(cohort_sizes, "cohort_month")
    .withColumn("retention_pct", round(col("retained_users") * 100 / col("cohort_size"), 2))
    .select("cohort_month", "cohort_size", "months_since_signup",  "retained_users", "retention_pct").orderBy("cohort_month", "months_since_signup"))

cohort_matrix = retention_table.groupBy("cohort_month", "cohort_size").pivot("months_since_signup").agg({"retention_pct": "first"})