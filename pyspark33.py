# Famous Percentage

%python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

distinct_users = famous.select("user_id").union(famous.select("follower_id")).distinct()

follower_count = famous.groupBy("user_id").agg(F.count("follower_id").alias("followers"))

total_users = distinct_users.count()
result = follower_count.withColumn("famous_percentage", (F.col("followers") * 100) / total_users);

%sql
with distinct_users as (
select user_id as users from famous union select follower_id as users from famous),
follower_count as (
select user_id, count(follower_id) as followers from famous group by user_id)
select f.user_id, (f.followers * 100.0) / (select count(*) from distinct_users) as famous_percentage from follower_count f;

# Monthly Revenue Change

%python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

monthly_revenue = sf_transactions.groupBy(F.date_format("created_at", "yyyy-mm").alias("year_month")).agg(F.sum("value").alias("total_revenue"))

window_spec = Window.orderBy("year_month")
revenue_change = monthly_revenue.withColumn("previous_revenue", F.lag("total_revenue").over(window_spec)).withColumn("percentage_change", F.when(F.col("previous_revenue").isNull(), F.lit(None)).otherwise(F.round(((F.col("total_revneue") - F.col("previous_revenue"))/ F.col("previous_revenue").cast("float")) * 100 , 2))).select("year_month", "total_revenue", "percentage_change");

%sql
with monthlyRevenue as (
select to_char(created_at, 'YYYY-MM') as year_month, sum(value) as total_revenue from sf_transactions group by to_char(created_at, 'YYYY-MM')),
RevenueChange as (
select year_month, total_revenue, lag(total_revenue) over (order by year_month) as previous_revenue from monthlyRevenue)
select year_month, total_revenue, round(case when previous_revenue is NULL then NULL else ((total_revenue - previous_revenue)/previous_revenue) * 100 end, 2) as percentage_change from reveneuChange order by year_month;

# Mutual Friends

%python
kanl_friends = friends.join(users.filter(F.col("user_name") == "kanl").select("user_id"), "user_id").select("friend_id")

hans_friends = friends.join(users.filter(F.col("user_name") == "Hans").select("user_id"), "user_id").select("friend_id")

mutual_friends = kanl_friends.join(hans_friends, kanl_friends.friend_id == hans_friends.friend_id).join(users, kanl_friends.friend_id == users.user_id).select("user_id", "user_name")

%sql
with kanl_friends as (
select friend_id from friends where user_id = (select user_id from users where user_name = 'kanl')),
hans_friends as (
select friend_id from friends where user_id = (select user_id from users where user_name = 'Hans'))
select u.user_id, u.user_name from users u join kanl_friends kf on u.user_id = kf.friend_id join hans_friends hf on u.user_id = hf.friend_id;

# RMSE Calculation

%python
from pyspark.sql import functions as F

monthly_aggregates = uber_request_logs.groupBy(F.date_format("request_date", "yyyy-MM").alias("year_month")).agg(
    F.sum("distance_to_travel").alias("total_distance"),
    F.sum("monetary_cost").alias("total_cost")).withColumn("distance_per_dollar", F.col("total_distance") / F.col("total_cost"))

window_spec = Window.orderBy("year_month") 
naive_forecast = monthly_aggregates.withColumn("forecasted_value", F.lag("distance_per_dollar", 1).over(window_spec)).filter(F.col("forecasted_value").isNotNull())

rmse_result = naive_forecast.select(F.round(
        F.sqrt(F.avg(F.pow(F.col("distance_per_dollar") - F.col("forecasted_value"), 2))), 2).alias("rmse"))

%sql
WITH monthly_aggregates AS (
SELECT TO_CHAR(request_date, 'YYYY-MM') AS year_month, SUM(distance_to_travel) AS total_distance, SUM(monetary_cost) AS total_cost FROM uber_request_logs GROUP BY TO_CHAR(request_date, 'YYYY-MM')),
distance_per_dollar AS (
SELECT year_month, total_distance / total_cost AS distance_per_dollar FROM monthly_aggregates),
naive_forecast AS (
SELECT year_month, distance_per_dollar, LAG(distance_per_dollar, 1) OVER (ORDER BY year_month) AS forecasted_value FROM distance_per_dollar)
SELECT ROUND(SQRT(AVG(POWER((distance_per_dollar - forecasted_value), 2))), 2) AS rmse FROM naive_forecast WHERE forecasted_value IS NOT NULL;

# Budget per Employee

%python
budget_per_employee = ms_projects.join(ms_emp_projects,  ms_projects.id == ms_emp_projects.project_id).groupBy(ms_projects.id, ms_projects.title, ms_projects.budget
).agg(
    F.count("emp_id").alias("employee_count")).withColumn(
    "budget_per_employee",
    F.round(F.col("budget") / F.col("employee_count"), 0)).select(
    F.col("title").alias("project_title"), "budget_per_employee").orderBy(F.desc("budget_per_employee"))

%sql
SELECT p.title AS project_title, ROUND(p.budget / COUNT(e.emp_id), 0) AS budget_per_employee FROM ms_projects p JOIN ms_emp_projects e ON p.id = e.project_id GROUP BY p.id, p.title, p.budget ORDER BY budget_per_employee DESC;

# Total Available Beds by Nationality

%python
total_beds = airbnb_apartments.join(
    airbnb_hosts, 
    airbnb_apartments.host_id == airbnb_hosts.host_id).groupBy("nationality").agg(
    F.sum("n_beds").alias("total_available_beds")).orderBy(F.desc("total_available_beds"))

%sql
SELECT h.nationality, SUM(a.n_beds) AS total_available_beds FROM airbnb_apartments a JOIN airbnb_hosts h ON a.host_id = h.host_id GROUP BY h.nationality ORDER BY total_available_beds DESC;

# Q1 Fridays Analysis

%python
from pyspark.sql import functions as F

q1_fridays = user_purchases.filter(
    (F.dayofweek("date") == 6) &  # Friday (Spark: 1=Sunday, 6=Friday)
    (F.month("date").isin([1, 2, 3]))).withColumn("week_number", F.weekofyear("date"))

result = q1_fridays.groupBy("week_number").agg(F.round(F.avg("amount_spent"), 2).alias("avg_amount_spent")).orderBy("week_number")

%sql
WITH q1_fridays AS (
SELECT
        user_id,
        date,
        amount_spent,
        TO_CHAR(date, 'WW') AS week_number,
        TO_CHAR(date, 'Day') AS day_name FROM user_purchases WHERE TO_CHAR(date, 'Day') LIKE 'Friday%' AND EXTRACT(MONTH FROM date) IN (1, 2, 3))
SELECT
    week_number,
    ROUND(AVG(amount_spent), 2) AS avg_amount_spent FROM q1_fridays GROUP BY week_number ORDER BY week_number;

# Product Count Difference

%python
product_counts = car_launches.filter(
    F.col("year").isin([2019, 2020])).groupBy("company_name").agg(
    F.sum(F.when(F.col("year") == 2020, 1).otherwise(0)).alias("products_2020"),
    F.sum(F.when(F.col("year") == 2019, 1).otherwise(0)).alias("products_2019"))

result = product_counts.withColumn(
    "net_difference", 
    F.col("products_2020") - F.col("products_2019")).select("company_name", "net_difference").orderBy(F.desc("net_difference"))

%sql
WITH product_counts AS (
SELECT
        company_name,
        SUM(CASE WHEN year = 2020 THEN 1 ELSE 0 END) AS products_2020,
        SUM(CASE WHEN year = 2019 THEN 1 ELSE 0 END) AS products_2019 FROM car_launches WHERE year IN (2019, 2020) GROUP BY company_name)
SELECT
    company_name,
    (products_2020 - products_2019) AS net_difference FROM product_counts ORDER BY net_difference DESC;

# Top Genre by Oscar Wins

%python
from pyspark.sql.window import Window

winner_count = oscar_nominees.filter(F.col("winner") == 1).groupBy("nominee").agg(F.count("*").alias("total_wins"))

result = winner_count.join(nominee_information, 
    winner_count.nominee == nominee_information.name).orderBy([
    F.desc("total_wins"),
    F.asc("name")]).limit(1).select("top_genre")

%sql
WITH WinnerCount AS (
SELECT nominee, COUNT(*) AS total_wins FROM oscar_nominees WHERE winner = 1 GROUP BY nominee)
SELECT ni.top_genre FROM WinnerCount wc JOIN nominee_information ni ON wc.nominee = ni.name ORDER BY wc.total_wins DESC, ni.name ASC FETCH FIRST 1 ROWS ONLY;