# Revenue Growth Decomposition

%python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("RevenueDecomposition").getOrCreate()

# Read orders data
orders_df = spark.read.table("orders")  # or spark.read.csv(), etc.

monthly_metrics = orders_df.groupBy(
    date_trunc('month', col('order_date')).alias('month')).agg(
    countDistinct('order_id').alias('total_orders'),
    sum(col('quantity') * col('list_price')).alias('gross_revenue'),
    sum('discount_amount').alias('total_discount'),
    sum(col('quantity') * col('list_price') - col('discount_amount')).alias('net_revenue'))

window_spec = Window.orderBy('month')
result = monthly_metrics.withColumn(
    'mom_revenue_change',
    col('net_revenue') - lag('net_revenue', 1).over(window_spec)).orderBy('month')

result.show()

%sql
WITH monthly_metrics AS (
    SELECT 
        TRUNC(order_date, 'MONTH') AS month,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(quantity * list_price) AS gross_revenue,
        SUM(discount_amount) AS total_discount,
        SUM(quantity * list_price - discount_amount) AS net_revenue FROM orders GROUP BY TRUNC(order_date, 'MONTH'))
SELECT 
    month,
    total_orders,
    gross_revenue,
    total_discount,
    net_revenue,
    net_revenue - LAG(net_revenue) OVER (ORDER BY month) AS mom_revenue_change FROM monthly_metrics ORDER BY month;

# Silent Churn Detection

%python
from pyspark.sql.functions import datediff, current_date, when, max, col
from pyspark.sql.types import DataType

# calculate days since last activity
last_activity = user_activity_df.groupBy('user_id').agg(max('activity_date').alias('last_seen_date'))

result = last_activity.withColumn('days_since_last_activity', datediff(current_date(), col('last_seen_date')))
             .withColumn('churn_status', when(col('days_since_last_activity') <= 30, 'active')
             .when(col('days_since_last_activity') <= 90, 'at_risk')
             .otherwise('churned')).select('user_id', 'churn_status')
result.show()

%sql
with last_activity as (
select user_id, max(activity_date) as last_seen_date from user_activity group by user_id)
select user_id, case 
when last_seen_date >= sysdate - interval '30' day then 'active'
when last_seen_date >= sysdate - interval '90' day then 'at_risk' else 'Churned' end as churn_status from last_activity;

# Funnel Conversion with Time Constraint

from pyspark.sql.window import Window
from pyspark.sql.functions import lead, unix_timestamp, col

window_spec = Window.partitionBy('user_id').orderBy('event_time')

ordered_events = events_df.withColumn('next_event', lead('event_type').over(window_spec)).withColumn('next_event_time', lead('event_time').over(window_spec))

# calculate time difference in seconds (48 hours = 172800 seconds)
converted_users = ordered_events.filter((col('event_type') == 'signup') & (col('next_event') == 'purchase') & ((unix_timestamp(col('next_event_time')) - unix_timestamp(col('event_time'))) <= 172800)).select('user_id').distinct()

print(f"Converted users : {converted_users.count()}")

%sql
with ordered_events as (
select user_id, event_type, event_time, lead(event_type) over (partition by user_id order by event_time) as next_event,
lead(event_time) over (partition by user_id order by event_time) as next_event_time from events)
select count(distinct user_id) as converted_users from ordered_events where event_type = 'signup' and next_event = 'purchase' and next_event_time <= event_time + interval '48' hour;

# Revenue Spikes Detection After JOIN

from pyspark.sql.functions import countDistinct, sum

joined_df = order_df.join(promotions_df, on='order_id', how='left')

analysis = joined_df.agg(count('*').alias('joined_row_count'), countDistinct('order_id').alias('unique_orders'), sum('revenue').alias('joined_revenue'))

result = analysis.withColumn('revenue_inflated', col('joined_row_count') > col('unique_orders')).withColumn('inflation_percentage', (col('joined_row_count') - col('unique_orders')) / col('unique_orders') * 100)

result.show()

%sql
select count(*) as joined_row_count, count(distinct o.order_id) as unique_orders, sum(o.revenue) as joined_revenue, case when count(*) > count(distinct o.order_id) then 'YES' else 'NO' end as revenue_inflated from orders o left join promotions p on o.order_id = p.order_id;

# Customer Lifetime Value Ranking

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, sum as _sum

order_level_revenue = order_items_df.groupBy('order_id', 'customer_id').agg(_sum(col('quantity') * col('price')).alias('order_revenue'))
customer_ltv = order_level_revenue.groupBy('customer_id').agg(_sum('order_revenue').alias('lifetime_value'))
window_spec = Window.orderBy(col('lifetime_value').desc())
result = customer_ltv.withColumn('ltv_rank', rank().over(window_spec)).orderBy('ltv_rank')
result.show()

%sql
with order_level_revenue as (
select order_id, customer_id, sum(quantity * price) as order_revenue from order_items group by order_id, customer_id),
customer_ltv as (
select customer_id, sum(order_revenue) as lifetime_value from order_level_revenue group by customer_id)
select customer_id, lifetime_value, rank() over (order by lifetime_value desc) as ltv_rank from customer_ltv;

# Fair YoY Comparison

%python
from pyspark.sql.functions import year, dayofmonth, when, max as _max, sum as _sum

max_day_df = daily_revenue_df.filter(year(col('order_date')) == year(current_date())).agg(_max(dayofmonth('order_date')).alias('max_day_curr_year')).alias('max_day_curr_year'))
max_day = max_day_df.collect()[0]['max_day_curr_year']

result = daily_revenue_df.filter(dayofmonth(col('order_date')) <= max_day).groupBy(dayofmonth('order_date').alias('day')).agg(
_sum(when(year('order_date') == year(current_date()), col('revenue'))).alias('curr_year_rev'),
_sum(when(year('order_date') == year(current_date()) - 1, col('revenue'))).alias('prev_year_rev')).orderBy('day')
result.show()

%sql
with max_day as (
select max(extract(day from order_date)) as max_day_curr_year from daily_revenue where extract(year from order_date) = extract(year from sysdate)),
filtered as (
select extract(year from order_date) as year, extract(day from order_date) as day, revenue from daily_revenue)
select day, sum(case when year = extract(year from sysdate) then revenue end) as curr_year_rev,
sum(case when year = extract(year from sysdate) - 1 then revenue end) as prev_year_rev from filtered where day <= (select max_day_curr_year from max_day) group by day order by day;

# Rolling 30-Day Active Users

%python
from pyspark.sql.functions import date_add, col, countDistinct

# Get distinct dates
distinct_dates = user_activity_df.select('activity_date').distinct()

# Cross join approach for rolling window
result = distinct_dates.alias('d').join(
    user_activity_df.alias('u'),
    col('u.activity_date').between(
        date_add(col('d.activity_date'), -29),
        col('d.activity_date'))).groupBy(col('d.activity_date')).agg(countDistinct('u.user_id').alias('rolling_30d_users')).orderBy('d.activity_date')

result.show()

%sql
SELECT 
    d.activity_date,
    COUNT(DISTINCT u.user_id) AS rolling_30d_users FROM (
    SELECT DISTINCT activity_date FROM user_activity) d JOIN user_activity u 
    ON u.activity_date BETWEEN d.activity_date - INTERVAL '29' DAY AND d.activity_date GROUP BY d.activity_date ORDER BY d.activity_date;

# Detect KPI Gaming (Fake Active Users)

from pyspark.sql.functions import date_trunc, count, month

monthly_activity = activity_log_df.groupBy(
    'user_id',
    date_trunc('month', 'activity_date').alias('month')).agg(count('*').alias('activity_count'))

suspicious_users = monthly_activity.filter(col('activity_count') == 1).groupBy('month').agg(count('user_id').alias('suspicious_users')).orderBy('month')
suspicious_users.show()

%sql
WITH monthly_activity AS (
    SELECT 
        user_id,
        TRUNC(activity_date, 'MONTH') AS month,
        COUNT(*) AS activity_count FROM activity_log GROUP BY user_id, TRUNC(activity_date, 'MONTH'))
SELECT 
    month,
    COUNT(user_id) AS suspicious_users FROM monthly_activity WHERE activity_count = 1 GROUP BY month ORDER BY month;

# Revenue Decline Without Churn Increase

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import lag

monthly_spend = transactions_df.groupBy(
    'customer_id',
    date_trunc('month', 'transaction_date').alias('month')).agg(sum('revenue').alias('monthly_revenue'))

window_spec = Window.partitionBy('customer_id').orderBy('month')
with_lag = monthly_spend.withColumn(
    'prev_month_revenue',
    lag('monthly_revenue', 1).over(window_spec))

downgraded_customers = with_lag.filter(
    col('prev_month_revenue').isNotNull() &
    (col('monthly_revenue') < col('prev_month_revenue'))).groupBy('month').agg(
    countDistinct('customer_id').alias('downgraded_customers')).orderBy('month')

downgraded_customers.show()

%sql
WITH monthly_spend AS (
    SELECT 
        customer_id,
        TRUNC(transaction_date, 'MONTH') AS month,
        SUM(revenue) AS monthly_revenue FROM transactions GROUP BY customer_id, TRUNC(transaction_date, 'MONTH')),
with_lag AS (
    SELECT 
        customer_id,
        month,
        monthly_revenue,
        LAG(monthly_revenue) OVER (PARTITION BY customer_id ORDER BY month) AS prev_month_revenue FROM monthly_spend)
SELECT 
    month,
    COUNT(DISTINCT customer_id) AS downgraded_customers FROM with_lag WHERE prev_month_revenue IS NOT NULL
    AND monthly_revenue < prev_month_revenue GROUP BY month ORDER BY month;

# Assign Single Region Per Month

%python

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy(
    'user_id',
    date_trunc('month', 'effective_date')).orderBy(
    col('effective_date').desc())

ranked_regions = user_region_history_df.withColumn(
    'rn',
    row_number().over(window_spec))

result = ranked_regions.filter(
    col('rn') == 1).select(
    'user_id',
    date_trunc('month', 'effective_date').alias('month'),
    'region').orderBy('user_id', 'month')

result.show()

%sql

WITH ranked_regions AS (
    SELECT 
        user_id,
        region,
        TRUNC(effective_date, 'MONTH') AS month,
        ROW_NUMBER() OVER (
            PARTITION BY user_id, TRUNC(effective_date, 'MONTH')
            ORDER BY effective_date DESC) AS rn FROM user_region_history)

SELECT 
    user_id,
    month,
    region FROM ranked_regions WHERE rn = 1 ORDER BY user_id, month;
