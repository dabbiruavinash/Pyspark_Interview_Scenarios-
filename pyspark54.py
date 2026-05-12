Find the top N items per category.

%sql
select * from (
select row_number() over (partition by department order by salary desc) as rn from employees) ranked where rn <= 3;

%python
from pyspark.sql import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("department").orderBy(col("salary").desc())
result = df.withColumn("rn", row_number().over(window_spec)) \
           .filter(col("rn") <= 3) \
           .drop("rn")

Compare this month's numbers to the same month last year.

%sql
select curr.month, curr.revenue as this_year, prev.revenue as last_year, curr.revenue - prev.revenue as difference from monthly_revenue curr left join monthly_revenue prev on curr.month = prev.month and curr.year = prev.month + 1;

%python
from pyspark.sql.functions import col, add_months

df_alias = df.alias("a")
df_prior = df.alias("b")

result = df_alias.join(
    df_prior,
    add_months(col("b.month_date"), 12) == col("a.month_date"),
    "left").select(
    col("a.month_date"),
    col("a.revenue").alias("current_revenue"),
    col("b.revenue").alias("previous_year_revenue"))

Calculate a cumulative sum over time.

%sql
select date, daily_revenue, sum(daily_revenue) over (order by date) as running_total from revenue;

%python
SELECT 
    date, 
    daily_revenue,
    SUM(daily_revenue) OVER (ORDER BY date) AS running_total FROM revenue;

Identify records that appear more than once.

%sql
select email, count(*) as cnt from users group by email having count(*) > 1;

%python
from pyspark.sql.functions import count

result = df.groupBy("user_id", "email") \
           .agg(count("*").alias("cnt")) \
           .filter(col("cnt") > 1)

Compare each row with the previous one.

%sql
select date, revenue, lag(revenue) over (order by date) as prev_day, revenue - lag(revenue) over (order by date) as daily_change from daily_revenue;

%python
from pyspark.sql import Window
from pyspark.sql.functions import lag

window_spec = Window.orderBy("date")
result = df.withColumn("prev_day", lag("revenue").over(window_spec)) \
           .withColumn("daily_change", col("revenue") - col("prev_day"))

Count or sum based on conditions in one query.

%sql
select department, count(case when status = "active" then 1 end) as active_count, count(case when status = "churned" then 1 end) as churned_count from users group by department;

%python
from pyspark.sql.functions import count, when

result = df.groupBy("department").agg(
    count(when(col("status") == "active", 1)).alias("active_count"),
    count(when(col("status") == "churned", 1)).alias("churned_count"))

Find missing dates or IDs in a sequence.

%sql
select a.date + 1 as missing_date from logins a left join logins b on a.date + 1 = b.date where b.date is null;

%python
from pyspark.sql.functions import col, expr, datediff, lead
from pyspark.sql import Window

# Method 1: Self-join approach
df_alias = df.select(col("date").alias("date_a"))
df_joined = df_alias.join(
    df.select(col("date").alias("date_b")),
    expr("date_a + 1 = date_b"),
    "left")
result = df_joined.filter(col("date_b").isNull()) \
                   .select(expr("date_a + 1").alias("missing_date"))


Find users who came back within 7 days.

%sql
select a.user_id, a.login_date as first_visit, min(b.login_date) as return from logins a join logins b on a.user_id = b.user_id and b.login_date > a.login_date and b.login_date <= a.login_date + 7 group by a.user_id, a.login_date;

%python
from pyspark.sql.functions import col, min as spark_min, datediff, expr

df_alias = df.alias("a")
df_return = df.alias("b")

result = df_alias.join(
    df_return,
    (col("a.user_id") == col("b.user_id")) &
    (col("b.login_date") > col("a.login_date")) &
    (col("b.login_date") <= expr("a.login_date + interval 7 days")),
    "inner").groupBy(col("a.user_id"), col("a.login_date").alias("first_visit")).agg(spark_min("b.login_date").alias("return_visit"))

Make your query readable and debuggable.

%sql
with active_users as (
select user_id from logins where login_date >= '2026-01-01' group by user_id having count(*) >= 5),
user_orders as (
select user_id, sum(amount) as total_spent from orders group by user_id)
select a.user_id, u.total_spent from active_users a join users_orders u on a.user_id = u.user_id;

%python
# PySpark uses temporary views for CTE-like behavior
active_users = df_logins.filter(col("login_date") >= "2026-01-01") \
                        .groupBy("user_id") \
                        .agg(count("*").alias("login_count")) \
                        .filter(col("login_count") >= 5) \
                        .select("user_id")

user_orders = df_orders.groupBy("user_id") \
                       .agg(sum("amount").alias("total_spent"))

result = active_users.join(user_orders, on="user_id", how="inner")

Find the median salary without built-in functions

%sql
select avg(salary) as median_salary from (
select salary, row_number() over (order by salary) as rn , count(*) over () as total form employees) sub where rn in (floor ((total + 1)/2.0), ciel((total + 1)/ 2.0));

%python
from pyspark.sql import Window
from pyspark.sql.functions import row_number, avg

# Total count
total_count = df.count()
window_asc = Window.orderBy("salary")
window_desc = Window.orderBy(col("salary").desc())

result = df.withColumn("rn_asc", row_number().over(window_asc)) \
           .withColumn("rn_desc", row_number().over(window_desc)) \
           .filter(
               (col("rn_asc") == col("rn_desc")) |
               (col("rn_asc") == col("rn_desc") - 1) |
               (col("rn_asc") == col("rn_desc") + 1)) \
           .select(avg("salary").alias("median_salary"))