# How would you identify daily active users (logged in at least once)?

%python
from pyspark.sql import functions as F

daily_active_users = df_logs.filter(F.col("login_timestamp").isNotNull()).groupBy(F.date_trunc("day", "login_timestamp").alias("date")).agg(F.countDistinct("user_id").alias("active_users"))
daily_active_users.show()

%sql
select trunc(login_timestamp) as login_date, count(distinct user_id) as active_users from user_logs where login_timestamp is not null group by trunc(login_timestamp);

# How do you fetch the second highest transaction per user without using LIMIT or TOP?

%python
from pyspark.sql.window import Window
window_spec = Window.partitionBy("user_id").orderBy(F.desc("transaction_amount"))
second_highest = df_transactions.withColumn("rank", F.dense_rank().over(window_spec)).filter(F.col("rank") == 2).drop("rank")
second_highest.show()

%sql
select user_id, transaction_amount from (
select user_id, transaction_amount, dense_rank() over (partition by user_id order by trnasaction_amount desc) as rank_num from transactions) where rank_num = 2;

# How can you detect missing records in a time-series dataset (e.g., hourly gaps)?

%python
from pyspark.sql.types import TimestampType
import pandas as pd

complete_hours = spark.createDataFrame(pd.date_range(start='2024-01-01', end='2024-01-31', freq='H'), TimestampType()).toDF("hour")
missing_hours = complete_hours.join(df_time_series, "hour", "left_anti")
missing_hours.show()

%sql
WITH all_hours AS (
SELECT TRUNC(SYSDATE) + (LEVEL-1)/24 as hour FROM dual CONNECT BY LEVEL <= 24*30)
SELECT a.hour FROM all_hours a LEFT JOIN time_series_data t ON a.hour = t.hour WHERE t.hour IS NULL;

# How would you find a user’s first purchase date and calculate days since then?

%python
from pyspark.sql.window import Window

window_spec = Window.partitionBy("user_id").orderBy("purchase_date")

user_first_purchase = df_purchases \
    .withColumn("first_purchase", F.min("purchase_date").over(window_spec)) \
    .withColumn("days_since_first", F.datediff(F.current_date(), "first_purchase")) \
    .select("user_id", "first_purchase", "days_since_first") \
    .distinct()

%sql
SELECT user_id, MIN(purchase_date) as first_purchase, SYSDATE - MIN(purchase_date) as days_since_first FROM purchases GROUP BY user_id;

# How can you detect schema changes in SCD Type 2 Delta tables?

%python
current_schema = spark.table("scd_table").schema
new_data_schema = df_new_data.schema

schema_changes = set(new_data_schema.fieldNames()) - set(current_schema.fieldNames())
if schema_changes:
    print(f"New columns detected: {schema_changes}")

%sql
-- Check for new columns in incoming data vs SCD table
SELECT column_name FROM user_tab_columns WHERE table_name = 'NEW_DATA_TABLE'
MINUS
SELECT column_name FROM user_tab_columns WHERE table_name = 'SCD_TABLE';

# How do you safely join product & transaction tables while excluding null foreign keys?

%python
safe_join = df_products.join(
    df_transactions.filter(F.col("product_id").isNotNull()),
    "product_id",
    "inner")

%sql
SELECT p.*, t.* FROM products p INNER JOIN transactions t ON p.product_id = t.product_id WHERE t.product_id IS NOT NULL;

# How would you identify users who upgraded to premium within 7 days of signup?

%python
upgraded_users = df_signups.alias("s").join(
    df_upgrades.alias("u"),
    (F.col("s.user_id") == F.col("u.user_id")) & 
    (F.datediff("u.upgrade_date", "s.signup_date") <= 7),
    "inner").select("s.user_id", "s.signup_date", "u.upgrade_date")

%sql
SELECT s.user_id, s.signup_date, u.upgrade_date FROM signups s INNER JOIN upgrades u ON s.user_id = u.user_id WHERE u.upgrade_date - s.signup_date <= 7;

# How do you calculate cumulative distinct product purchases per customer?

%python
from pyspark.sql.window import Window

window_spec = Window.partitionBy("customer_id").orderBy("purchase_date")

cumulative_products = df_purchases \
    .withColumn("distinct_products", 
                F.size(F.collect_set("product_id").over(window_spec.rowsBetween(Window.unboundedPreceding, 0))))

%sql
SELECT customer_id, purchase_date,
       COUNT(DISTINCT product_id) OVER (
           PARTITION BY customer_id 
           ORDER BY purchase_date 
           ROWS UNBOUNDED PRECEDING) as cumulative_distinct_products FROM purchases;

# How would you find customers spending above the average in their region?

%python
regional_avg = df_transactions.join(df_customers, "customer_id") \
    .groupBy("region") \
    .agg(F.avg("amount").alias("region_avg_spend"))

high_spenders = df_transactions.join(df_customers, "customer_id") \
    .join(regional_avg, "region") \
    .filter(F.col("amount") > F.col("region_avg_spend"))

%sql
WITH regional_avg AS (
    SELECT c.region, AVG(t.amount) as avg_spend
    FROM transactions t
    JOIN customers c ON t.customer_id = c.customer_id
    GROUP BY c.region)
SELECT t.customer_id, t.amount, c.region, r.avg_spend
FROM transactions t
JOIN customers c ON t.customer_id = c.customer_id
JOIN regional_avg r ON c.region = r.region
WHERE t.amount > r.avg_spend;

# How can you detect duplicates in ingestion tables across all columns?

%python
duplicates = df_ingestion.groupBy(df_ingestion.columns) \
    .count() \
    .filter(F.col("count") > 1) \
    .drop("count")

%sql
SELECT * FROM ingestion_table GROUP BY col1, col2, col3, ... -- all columnsHAVING COUNT(*) > 1;

# How do you calculate daily revenue growth % using LAG()?

%python
from pyspark.sql.window import Window

daily_revenue = df_transactions.groupBy(F.date_trunc("day", "transaction_date").alias("date")) \
    .agg(F.sum("amount").alias("daily_revenue"))

revenue_growth = daily_revenue.withColumn("prev_day_revenue", 
    F.lag("daily_revenue").over(Window.orderBy("date"))) \
    .withColumn("growth_pct", 
        ((F.col("daily_revenue") - F.col("prev_day_revenue")) / F.col("prev_day_revenue")) * 100)

%sql
SELECT transaction_date,
       daily_revenue,
       LAG(daily_revenue) OVER (ORDER BY transaction_date) as prev_day_revenue,
       ((daily_revenue - LAG(daily_revenue) OVER (ORDER BY transaction_date)) / 
        LAG(daily_revenue) OVER (ORDER BY transaction_date)) * 100 as growth_pct FROM (
SELECT TRUNC(transaction_date) as transaction_date, SUM(amount) as daily_revenue FROM transactions GROUP BY TRUNC(transaction_date));

# How would you spot products with 3 months of consecutive sales decline?

%python
monthly_sales = df_sales.groupBy("product_id", F.date_trunc("month", "sale_date").alias("month")) \
    .agg(F.sum("quantity").alias("monthly_sales")) \
    .orderBy("product_id", "month")

window_spec = Window.partitionBy("product_id").orderBy("month")

declining_products = monthly_sales \
    .withColumn("prev_sales", F.lag("monthly_sales").over(window_spec)) \
    .withColumn("is_decline", F.when(F.col("monthly_sales") < F.col("prev_sales"), 1).otherwise(0)) \
    .withColumn("consecutive_declines", 
                F.sum("is_decline").over(window_spec.rowsBetween(2, 0))) \
    .filter(F.col("consecutive_declines") == 3)

%sql
WITH monthly_sales AS (
    SELECT product_id, 
           TRUNC(sale_date, 'MONTH') as month,
           SUM(quantity) as monthly_sales
    FROM sales
    GROUP BY product_id, TRUNC(sale_date, 'MONTH')
),
sales_trend AS (
    SELECT product_id, month, monthly_sales,
           LAG(monthly_sales, 1) OVER (PARTITION BY product_id ORDER BY month) as prev_month1,
           LAG(monthly_sales, 2) OVER (PARTITION BY product_id ORDER BY month) as prev_month2,
           LAG(monthly_sales, 3) OVER (PARTITION BY product_id ORDER BY month) as prev_month3 FROM monthly_sales)
SELECT product_id FROM sales_trend WHERE monthly_sales < prev_month1 AND prev_month1 < prev_month2 AND prev_month2 < prev_month3;

# How do you find users with 3+ logins per week consistently over 2 months?

%python
weekly_logins = df_logs.groupBy("user_id", F.weekofyear("login_date").alias("week")) \
    .agg(F.count("*").alias("weekly_logins")) \
    .filter(F.col("weekly_logins") >= 3)

consistent_users = weekly_logins.groupBy("user_id") \
    .agg(F.count("*").alias("weeks_with_3_logins")) \
    .filter(F.col("weeks_with_3_logins") >= 8)  # 2 months ≈ 8 weeks

%sql
WITH weekly_logins AS (
SELECT user_id, TO_CHAR(login_date, 'IYYY-IW') as week, COUNT(*) as login_count FROM user_logs GROUP BY user_id, TO_CHAR(login_date, 'IYYY-IW') HAVING COUNT(*) >= 3)
SELECT user_id, COUNT(*) as consistent_weeks FROM weekly_logins GROUP BY user_id HAVING COUNT(*) >= 8;

# How would you rank users by login frequency this quarter?

%python
from pyspark.sql.window import Window

current_quarter_logins = df_logs.filter(
    F.quarter("login_date") == F.quarter(F.current_date()) &
    (F.year("login_date") == F.year(F.current_date())))

user_login_freq = current_quarter_logins.groupBy("user_id") \
    .agg(F.count("*").alias("login_count"))

ranked_users = user_login_freq.withColumn("rank", 
    F.dense_rank().over(Window.orderBy(F.desc("login_count"))))

%sql
SELECT user_id, COUNT(*) as login_count, DENSE_RANK() OVER (ORDER BY COUNT(*) DESC) as rank FROM user_logs WHERE EXTRACT(QUARTER FROM login_date) = EXTRACT(QUARTER FROM SYSDATE) AND EXTRACT(YEAR FROM login_date) = EXTRACT(YEAR FROM SYSDATE) GROUP BY user_id;

# How do you identify users buying the same product multiple times in a single day?

%python
daily_duplicate_purchases = df_purchases.groupBy("user_id", "product_id", F.date_trunc("day", "purchase_date").alias("purchase_day")).agg(F.count("*").alias("purchase_count")).filter(F.col("purchase_count") > 1)

%sql
SELECT user_id, product_id, TRUNC(purchase_date) as purchase_day, COUNT(*) as purchase_count FROM purchases GROUP BY user_id, product_id, TRUNC(purchase_date) HAVING COUNT(*) > 1;

# How would you delete late-arriving data for the current month safely?

%python
current_month = F.date_format(F.current_date(), "yyyy-MM")

df_current_month = spark.table("target_table").filter(F.date_format("load_date", "yyyy-MM") == current_month)
df_current_month.write.saveAsTable("backup_current_month")

spark.sql(f"DELETE FROM target_table WHERE date_format(load_date, 'yyyy-MM') = '{current_month}'")

df_new_data.write.insertInto("target_table")

%sql
-- Backup current month
CREATE TABLE backup_current_month AS
SELECT * FROM target_table 
WHERE EXTRACT(YEAR FROM load_date) = EXTRACT(YEAR FROM SYSDATE)
  AND EXTRACT(MONTH FROM load_date) = EXTRACT(MONTH FROM SYSDATE);

-- Delete current month
DELETE FROM target_table
WHERE EXTRACT(YEAR FROM load_date) = EXTRACT(YEAR FROM SYSDATE)
  AND EXTRACT(MONTH FROM load_date) = EXTRACT(MONTH FROM SYSDATE);

-- Insert new data
INSERT INTO target_table SELECT * FROM new_data;

# How do you determine the top 5 products by profit margin across all categories?

%python
product_margins = df_products.withColumn("profit_margin", 
    (F.col("selling_price") - F.col("cost_price")) / F.col("selling_price"))

top_products = product_margins.withColumn("rank", 
    F.dense_rank().over(Window.orderBy(F.desc("profit_margin")))) \
    .filter(F.col("rank") <= 5)

%sql
SELECT product_id, product_name, (selling_price - cost_price) / selling_price as profit_margin FROM (
SELECT product_id, product_name, selling_price, cost_price, DENSE_RANK() OVER (ORDER BY (selling_price - cost_price) / selling_price DESC) as rank_num FROM products) WHERE rank_num <= 5;

# How can you compare rolling 30-day revenue with the previous 30 days?

%python
daily_revenue = df_transactions.groupBy(F.date_trunc("day", "transaction_date").alias("date")) \
    .agg(F.sum("amount").alias("daily_revenue"))

rolling_comparison = daily_revenue \
    .withColumn("last_30_days", 
        F.sum("daily_revenue").over(Window.orderBy("date").rowsBetween(-29, 0))) \
    .withColumn("prev_30_days", 
        F.sum("daily_revenue").over(Window.orderBy("date").rowsBetween(-59, -30))) \
    .withColumn("growth_pct", 
        ((F.col("last_30_days") - F.col("prev_30_days")) / F.col("prev_30_days")) * 100)

%sql
SELECT date,
       daily_revenue,
       SUM(daily_revenue) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as last_30_days,
       SUM(daily_revenue) OVER (ORDER BY date ROWS BETWEEN 59 PRECEDING AND 30 PRECEDING) as prev_30_days,
       ((SUM(daily_revenue) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) -
         SUM(daily_revenue) OVER (ORDER BY date ROWS BETWEEN 59 PRECEDING AND 30 PRECEDING)) /
         SUM(daily_revenue) OVER (ORDER BY date ROWS BETWEEN 59 PRECEDING AND 30 PRECEDING)) * 100 as growth_pct FROM (
SELECT TRUNC(transaction_date) as date, SUM(amount) as daily_revenue FROM transactions GROUP BY TRUNC(transaction_date));

# How would you flag transactions that occurred outside business hours?

%python
flagged_transactions = df_transactions \
    .withColumn("transaction_hour", F.hour("transaction_time")) \
    .withColumn("is_business_hours", 
        F.when((F.col("transaction_hour") >= 9) & (F.col("transaction_hour") <= 17), "Yes")
         .otherwise("No"))

%sql
SELECT transaction_id, transaction_time, CASE WHEN EXTRACT(HOUR FROM transaction_time) BETWEEN 9 AND 17  THEN 'Yes' ELSE 'No' END as is_business_hours FROM transactions;
