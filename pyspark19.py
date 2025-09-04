# First Purchase per Customer (ROW_NUMBER)

%python
from pyspark.sql import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("customer_id").orderBy("transaction_date")
df_with_rank = df.withColumn("rn", row_number().over(window_spec))
first_purchase_df = df_with_rank.filter("rn == 1").drop("rn")

# Highest Salary per Department (DENSE_RANK)

%python
from pyspark.sql.functions import dense_rank

window_spec = Window.partitionBy("department").orderBy(col("salary").desc())
df_with_rank = df.withColumn("dr", dense_rank().over(window_spec))
highest_salary_df = df_with_rank.filter("dr == 1").drop("dr")

# Consecutive Active Days (DATE logic + windowing)

%python
from pyspark.sql.functions import lag, datediff, col

window_spec = Window.partitionBy("user_id").orderBy("login_date")
df_with_lag = df.withColumn("prev_login", lag("login_date").over(window_spec))
consecutive_df = df_with_lag.withColumn("day_diff", datediff(col("login_date"), col("prev_login")))
consecutive_users = consecutive_df.filter("day_diff == 1").select("user_id").distinct()

# Running Total of Sales (cumulative SUM)

%python
from pyspark.sql.functions import sum

window_spec = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df_with_running_total = df.withColumn("running_total", sum("sales").over(window_spec))

# Customers with no orders (LEFT ANTI JOIN)

%python
customers_with_no_orders_df = customers_df.join(orders_df, "customer_id", "left_anti")

# Second Highest Transaction per User (DENSE_RANK)

%python
window_spec = Window.partitionBy("user_id").orderBy(col("transaction_amount").desc())
df_with_rank = df.withColumn("dr", dense_rank().over(window_spec))
second_highest_df = df_with_rank.filter("dr == 2").drop("dr")

# Users with exactly 2 purchase days (COUNT DISTINCT)

%python
from pyspark.sql.functions import countDistinct

result_df = df.groupBy("user_id").agg(countDistinct("purchase_date").alias("distinct_days"))
result_df = result_df.filter("distinct_days == 2")

# First and last login timestamps (MIN/MAX)

%python
from pyspark.sql.functions import min, max

result_df = df.groupBy("user_id").agg(min("login_ts").alias("first_login"), max("login_ts").alias("last_login"))

# Products never sold (LEFT ANTI JOIN)

%python
unsold_products_df = products_df.join(sales_df, "product_id", "left_anti")

# Revenue % contribution per product per day (Window + Ratio)

%python
from pyspark.sql.functions import sum

window_spec = Window.partitionBy("date")
daily_total_df = df.withColumn("daily_total_rev", sum("revenue").over(window_spec))
result_df = daily_total_df.withColumn("pct_contribution", (col("revenue") / col("daily_total_rev")) * 100)

# Latest 2 orders per customer (ROW_NUMBER + DESC Order)

%python
window_spec = Window.partitionBy("customer_id").orderBy(col("order_date").desc())
df_with_rank = df.withColumn("rn", row_number().over(window_spec))
latest_two_orders_df = df_with_rank.filter("rn <= 2").drop("rn")

# Transaction comparison + Strictly increasing detection (LAG + Boolean SUM)

%python
from pyspark.sql.functions import lag, when

window_spec = Window.partitionBy("user_id").orderBy("transaction_date")
df_with_prev = df.withColumn("prev_amount", lag("amount").over(window_spec))
df_with_flag = df_with_prev.withColumn("is_increasing", when(col("amount") > col("prev_amount"), 1).otherwise(0))
# For strictly increasing per user: Group by user and check if all flags are 1 (and no nulls from first transaction)
strictly_increasing_users = df_with_flag.groupBy("user_id").agg(
    (sum("is_increasing") == count("is_increasing") - 1).alias("is_strictly_increasing")).filter("is_strictly_increasing = true")

# Top-selling product per region (DENSE_RANK over grouped SUM)

%python
from pyspark.sql.functions import sum

sales_by_region_product = df.groupBy("region", "product").agg(sum("sales").alias("total_sales"))
window_spec = Window.partitionBy("region").orderBy(col("total_sales").desc())
ranked_df = sales_by_region_product.withColumn("rn", dense_rank().over(window_spec))
top_product_per_region_df = ranked_df.filter("rn == 1").drop("rn")

# Group books per user using collect_list and concat_ws

%python
from pyspark.sql.functions import collect_list, concat_ws

result_df = df.groupBy("user_id").agg(concat_ws(", ", collect_list("book_name")).alias("all_books"))

# Filter users with all NULL purchases

%python
from pyspark.sql.functions import count, when

result_df = df.groupBy("user_id").agg(
    count(when(col("purchase_amount").isNotNull(), 1)).alias("non_null_count")).filter("non_null_count == 0")

# Top region by average revenue per product using dense_rank

%python
avg_rev_df = df.groupBy("region", "product").agg(avg("revenue").alias("avg_rev"))
window_spec = Window.orderBy(col("avg_rev").desc())
ranked_df = avg_rev_df.withColumn("rn", dense_rank().over(window_spec))
top_region_df = ranked_df.filter("rn == 1")

# Flag transactions as High/Low using when / CASE WHEN

%python
from pyspark.sql.functions import when

result_df = df.withColumn("flag", when(col("amount") > 1000, "High").otherwise("Low"))

# Customers with exactly 2 distinct purchase amounts

%python
result_df = df.groupBy("customer_id").agg(countDistinct("purchase_amount").alias("distinct_amounts"))
result_df = result_df.filter("distinct_amounts == 2")

# First purchase (product + date) per customer using row_number

%python
window_spec = Window.partitionBy("customer_id").orderBy("purchase_date")
df_with_rank = df.withColumn("rn", row_number().over(window_spec))
first_purchase_df = df_with_rank.filter("rn == 1").select("customer_id", "product", "purchase_date")

# Repeat purchases: customers who bought the same product twice

%python
result_df = df.groupBy("customer_id", "product_id").count()
repeat_customers_df = result_df.filter("count >= 2")

# Second Purchase Date per Customer

%python
window_spec = Window.partitionBy("customer_id").orderBy("purchase_date")
df_with_rank = df.withColumn("rn", row_number().over(window_spec))
second_purchase_df = df_with_rank.filter("rn == 2").select("customer_id", "purchase_date")

# Running Average per Product (Smoothed trend)

%python
window_spec = Window.partitionBy("product_id").orderBy("date").rowsBetween(-2, Window.currentRow)
df_with_avg = df.withColumn("running_avg", avg("sales").over(window_spec))

# Consecutive Purchase Streaks

%python
# Step 1 & 2: Create a flag for a new streak
window_spec = Window.partitionBy("user_id").orderBy("purchase_date")
df_with_lag = df.withColumn("prev_date", lag("purchase_date").over(window_spec))
df_with_flag = df_with_lag.withColumn("new_streak_flag",
    when(datediff(col("purchase_date"), col("prev_date")) > 1, 1).otherwise(0))

# Step 3: Create a streak_id by summing the flags over time
window_spec2 = Window.partitionBy("user_id").orderBy("purchase_date")
df_with_streak_id = df_with_flag.withColumn("streak_id", sum("new_streak_flag").over(window_spec2))

# Step 4: Group by user_id and streak_id to count the streak length
streak_lengths_df = df_with_streak_id.groupBy("user_id", "streak_id").count()

# Top 2 Products by Region

%sql
sales_by_region_product = df.groupBy("region", "product").agg(sum("sales").alias("total_sales"))
window_spec = Window.partitionBy("region").orderBy(col("total_sales").desc())
ranked_df = sales_by_region_product.withColumn("rn", dense_rank().over(window_spec))
top_2_products_df = ranked_df.filter("rn <= 2")

# Exploding Multi-Item Transactions

%python
from pyspark.sql.functions import split, explode

df_with_array = df.withColumn("items_array", split(col("items"), ","))
exploded_df = df_with_array.select("transaction_id", explode("items_array").alias("single_item"))

# Fill Missing Dates with Zero Revenue

%python
from pyspark.sql.functions import sequence, lit, explode

# 1. Create all dates
min_date, max_date = df.agg(min("date"), max("date")).first()
all_dates_df = spark.sql(f"SELECT explode(sequence('{min_date}', '{max_date}')) AS date")

# 2. Get distinct products and cross join
distinct_products = df.select("product").distinct()
full_grid_df = all_dates_df.crossJoin(distinct_products)

# 3 & 4. Left join and fill nulls
result_df = full_grid_df.join(df, ["date", "product"], "left").na.fill({"revenue": 0})

# Churn Detection (Inactive in last 30 days)

%python
from pyspark.sql.functions import current_date, datediff, max

max_activity_df = df.groupBy("user_id").agg(max("activity_date").alias("last_active"))
churned_users_df = max_activity_df.withColumn("days_since_active", datediff(current_date(), col("last_active")))
churned_users_df = churned_users_df.filter("days_since_active > 30")

# Top spender per month

%python
from pyspark.sql.functions import month, year, sum

monthly_spend_df = df.withColumn("year", year("date")).withColumn("month", month("date")) \
                     .groupBy("year", "month", "customer_id").agg(sum("amount").alias("total_spend"))
window_spec = Window.partitionBy("year", "month").orderBy(col("total_spend").desc())
ranked_df = monthly_spend_df.withColumn("rn", row_number().over(window_spec))
top_spender_df = ranked_df.filter("rn == 1")

# Order-to-delivery duration

%python
from pyspark.sql.functions import datediff

result_df = df.withColumn("duration_days", datediff("delivery_date", "order_date"))

# Join orders with first product purchased

%python
first_purchase_df = ... # From Q1 logic
result_df = orders_df.join(first_purchase_df, ["customer_id", "order_date"], "inner")
# Alternatively, if first_purchase_df has the product, just select from it.

# Revenue contribution % per category

%python
window_spec = Window.partitionBy("date")
daily_total_df = df.withColumn("daily_total_rev", sum("revenue").over(window_spec))
result_df = daily_total_df.withColumn("pct_contribution", (col("revenue") / col("daily_total_rev")) * 100)
# If you want per category per day, first calculate category revenue per day, then find its percentage of the daily total.

# Repeat buyers within a week

%python
from pyspark.sql.functions import lag, datediff

window_spec = Window.partitionBy("customer_id").orderBy("purchase_date")
df_with_prev = df.withColumn("prev_purchase", lag("purchase_date").over(window_spec))
repeat_buyers_df = df_with_prev.filter(datediff("purchase_date", "prev_purchase") <= 7)





