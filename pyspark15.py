# Daily Active Users

%sql
select login_date, count(distinct user_id) as daily_active_users from user_logins where login_date = trunc(sysdate) - 1 group by login_date;

%python
daily_active_users = (user_logins
    .filter(col("login_date") == current_date() - 1)
    .groupBy("login_date")
    .agg(countDistinct("user_id").alias("daily_active_users")))

# Second Highest Transaction per User (without LIMIT/TOP)

%sql
ranked_transactions as (
select user_id, transaction_amount, dense_rank() over (partition by user_id order by transaction_amount desc) as rnk from transactions)
select user_id, transactions where rnk = 2;

%python
window_spec = Window.partitionBy("user_id").orderBy(col("transaction_amount").desc())
ranked_transactions = transactions.withColumn("rnk", dense_rank().over(window_spec))
second_highest = ranked_transactions.filter(col("rnk") == 2)

# Detect Missing Records in Time-Series

%sql
with all_hours as (
select trunc(sysdate) + (LEVEL - 1)/24 as hour from dual connect by dual <= 24)
select a.hour from all_hours a left join hourly_data h on a.hour = h.timestamp where h.timestamp is null;

%python
all_hours = spark.range(24).select((current_date() + (col("id") / 24)).cast("timestamp").alias("hour"))
missing_hours = all_hours.join(hourly_data, all_hours.hour == hourly_data.timestamp, "left_anti")

# First Purchase Date and Days Since

%sql
select user_id, min(purchase_date) as first_purchase_date, sysdate - min(purchase_date) as day_since_first_purchase from purchase group by user_id;

%python
user_first_purchase = (purchases
    .groupBy("user_id")
    .agg(min("purchase_date").alias("first_purchase_date"))
    .withColumn("days_since_first_purchase", datediff(current_date(), col("first_purchase_date"))))

# Detect Schema Changes in SCD Type 2

%sql
select column_name, data_type from user_tab_columns where table_name = "YOUR_TABLE'
minus
select column_name, data_type from user_tab_columns where table_name = 'YOUR_TABLE'
as of TIMESTAMP (SYSTIMESTAMP - INTERVAL '1' DAY);

%python
old_schema = spark.read.option("timestampAsOf", "2024-01-01").table("your_table").schema
new_schema = spark.read.table("your_table").schema
schema_changes = [field for field in new_schema if field not in old_schema]

# Safe Join Excluding Null Foreign Keys

%sql
select t*, p.* from transactions t inner join products p on t.product_id = p.product_id where t.product_id is not null;

%python
safe_join = (transactions.filter(col("product_id").isNotNull()).join(products, "product_id"))

# Users Upgraded to Premium within 7 Days

%sql
select u.user_id from users u join premium_upgrades p on u.user_id = p.user_id where p.upgrade_date <= u.signup_date + 7;

%pytjon
upgraded_users = (users.join(premium_upgrades, "user_id").filter(col("upgrade_date") <= date_add(col("signup_date"), 7)))

# Cumulative Distinct Product Purchases

%sql
SELECT user_id, purchase_date, COUNT(DISTINCT product_id) OVER (PARTITION BY user_id ORDER BY purchase_date) as cumulative_distinct_products FROM purchases;

%python
window_spec = Window.partitionBy("user_id").orderBy("purchase_date")
cumulative_products = (purchases.withColumn("distinct_products", countDistinct("product_id").over(window_spec.rowsBetween(Window.unboundedPreceding, 0))))

# Customers Spending Above Regional Average

%sql
with regional_avg as (
select region, avg(total_spent) as avg_spent from (
select c.region, c.customer_id, sum(t.amount) as total_spent from customers c join transactions t on c.customer_id = t.customer_id group by c.region, c.customer_id) group by region)
select c.customer_id, c.region, sum(t.amount) as total_spent, ra.avg_spent from customers c join transactions t on c.customer_id = t.customer_id join regional_avg ra on c.region = ra.region group by c.customer_id, c.region, ra.avg_spent having sum(t.amount) > ra.avg_spent;

%python
regional_avg = (customers
    .join(transactions, "customer_id")
    .groupBy("region", "customer_id")
    .agg(sum("amount").alias("total_spent"))
    .groupBy("region")
    .agg(avg("total_spent").alias("avg_spent")))

above_avg_customers = (customers
    .join(transactions, "customer_id")
    .groupBy("region", "customer_id")
    .agg(sum("amount").alias("total_spent"))
    .join(regional_avg, "region")
    .filter(col("total_spent") > col("avg_spent")))

# Detect Duplicates Across All Columns

%sql
select *, count(*) over (partition by column1, column2, column3) as duplicate_count from ingestion_table qualify count(*) over (partition by column1, column2, column3) > 1;

%python
all_columns = [col for col in ingestion_table.columns]
duplicates = (ingestion_table
    .groupBy(all_columns)
    .agg(count("*").alias("duplicate_count"))
    .filter(col("duplicate_count") > 1))

# Daily Revenue Growth Percentage

%sql
with daily_revenue as (
select trunc(sale_date) as day, sum(amount) as revenue from sales group by trunc(sale_date))
select day, revenue, lag(revenue) over (order by day) as prev_day_revenue, ((revenue-lag(revenue) over (order by day))/ lag(revenue) over (order by day)) * 100 as growth_pct from daily_revenue;

%python
daily_revenue = (sales
    .groupBy(date_trunc("day", "sale_date").alias("day"))
    .agg(sum("amount").alias("revenue"))
    .withColumn("prev_day_revenue", lag("revenue").over(Window.orderBy("day")))
    .withColumn("growth_pct", (col("revenue") - col("prev_day_revenue")) / col("prev_day_revenue") * 100))

# Products with 3 Months Consecutive Sales Decline

%sql
with monthly_sales as (
select product_id, trunc(sale_date, 'month') as month, sum(amount) as monthly_sales, lag(sum(amount)) over (partition by product_id order by trunc(sale_date, 'MONTH')) as prev_month_sales from sales group by product_id, trunc(sale_date, 'month'))
select product_id from monthly_sales 
qualify count(case when monthly_sales < prev_month_sales then 1 end) over (partition by product_id order by month rows between 2 preceding and current row) = 3;

%python
monthly_sales = (sales
    .groupBy("product_id", date_trunc("month", "sale_date").alias("month"))
    .agg(sum("amount").alias("monthly_sales"))
    .withColumn("prev_month_sales", lag("monthly_sales").over(Window.partitionBy("product_id").orderBy("month"))))

consecutive_decline = (monthly_sales
    .withColumn("is_decline", when(col("monthly_sales") < col("prev_month_sales"), 1).otherwise(0))
    .withColumn("consecutive_declines", 
        sum("is_decline").over(Window.partitionBy("product_id").orderBy("month").rowsBetween(2, 0)))
    .filter(col("consecutive_declines") == 3))

# Users with 3+ Logins per Week for 2 Months

%sql
with weekly_logins as (
select user_id, trunc(login_date, 'IW') as week_start, count(*) as weekly_login_count from user_logins group by user_id, trunc(login_date, 'IW'))
select user_id from weekly_logins where weekly_login_count >= 3 group by user_id having count(*) >= 8;

%python
weekly_logins = (user_logins
    .groupBy("user_id", date_trunc("week", "login_date").alias("week_start"))
    .agg(count("*").alias("weekly_login_count"))
    .filter(col("weekly_login_count") >= 3))

consistent_users = (weekly_logins
    .groupBy("user_id")
    .agg(count("*").alias("weeks_with_3_logins"))
    .filter(col("weeks_with_3_logins") >= 8))

# Rank Users by Login Frequency This Quarter

%sql
select user_id, login_count, dense_rank() over (order by login_count desc) as rank from (
select user_id, count(*) as login_count from user_logins where login_date >= trunc(sysdate, 'Q') group by user_id);

%python
quarter_start = date_trunc("quarter", current_date())
quarter_logins = (user_logins
    .filter(col("login_date") >= quarter_start)
    .groupBy("user_id")
    .agg(count("*").alias("login_count"))
    .withColumn("rank", dense_rank().over(Window.orderBy(col("login_count").desc()))))

# Users Buying Same Product Multiple Times in Single Day

%sql
select user_id, product_id, purchase_date, count(*) as purchase_count from purchases group by user_id, product_id, trunc(purchase_date) having count(*) > 1;

%python
same_day_purchases = (purchases
    .groupBy("user_id", "product_id", date_trunc("day", "purchase_date").alias("purchase_day"))
    .agg(count("*").alias("purchase_count"))
    .filter(col("purchase_count") > 1))

# Delete Late-Arriving Data for Current Month Safely

%sql
DELETE FROM your_table
WHERE load_date > TRUNC(SYSDATE, 'MONTH')
AND created_date < TRUNC(SYSDATE, 'MONTH')
AND created_date >= ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -1);

%python
current_month_start = date_trunc("month", current_date())
prev_month_start = add_months(current_month_start, -1)

late_arriving_data = (your_table
    .filter((col("load_date") > current_month_start) & 
            (col("created_date") < current_month_start) &
            (col("created_date") >= prev_month_start)))

# Top 5 Products by Profit Margin Across Categories

%sql
SELECT category, product_id, profit_margin FROM (
SELECT category, product_id,  (revenue - cost) / revenue as profit_margin, ROW_NUMBER() OVER (PARTITION BY category ORDER BY (revenue - cost) / revenue DESC) as rnk FROM products) WHERE rnk <= 5;

%python
profit_margin = (products
    .withColumn("profit_margin", (col("revenue") - col("cost")) / col("revenue"))
    .withColumn("rnk", row_number().over(Window.partitionBy("category").orderBy(col("profit_margin").desc())))
    .filter(col("rnk") <= 5))

# Compare Rolling 30-Day Revenue with Previous 30 Days

%sql
WITH daily_revenue AS (
    SELECT TRUNC(sale_date) as day, SUM(amount) as revenue
    FROM sales
    GROUP BY TRUNC(sale_date))
SELECT day, SUM(revenue) OVER (ORDER BY day RANGE BETWEEN 29 PRECEDING AND CURRENT ROW) as last_30_days, SUM(revenue) OVER (ORDER BY day RANGE BETWEEN 59 PRECEDING AND 30 PRECEDING) as previous_30_days FROM daily_revenue;

%python
daily_revenue = (sales
    .groupBy(date_trunc("day", "sale_date").alias("day"))
    .agg(sum("amount").alias("revenue")))

window_spec = Window.orderBy("day").rowsBetween(-29, 0)
prev_window_spec = Window.orderBy("day").rowsBetween(-59, -30)

revenue_comparison = (daily_revenue
    .withColumn("last_30_days", sum("revenue").over(window_spec))
    .withColumn("previous_30_days", sum("revenue").over(prev_window_spec)))

# Flag Transactions Outside Business Hours

%sql
SELECT transaction_id, transaction_time, CASE WHEN EXTRACT(HOUR FROM transaction_time) < 9 
OR EXTRACT(HOUR FROM transaction_time) >= 17
OR TO_CHAR(transaction_time, 'DY') IN ('SAT', 'SUN') THEN 'Outside Business Hours' ELSE 'Within Business Hours' END as business_hours_flag FROM transactions;

%python
business_hours_flag = (transactions
    .withColumn("transaction_hour", hour("transaction_time"))
    .withColumn("day_of_week", date_format("transaction_time", "E"))
    .withColumn("business_hours_flag",
     when((col("transaction_hour") < 9) | (col("transaction_hour") >= 17)  col("day_of_week").isin(["Sat", "Sun"]), "Outside Business Hours").otherwise("Within Business Hours")))

# Optimize Query with Broadcast Join Hint

%sql
SELECT /*+ USE_HASH(t p) */ t.*, p.product_name FROM transactions t JOIN products p ON t.product_id = p.product_id WHERE p.category = 'Electronics';

%python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10MB

optimized_join = (transactions
    .join(broadcast(products.filter(col("category") == "Electronics")), "product_id"))









