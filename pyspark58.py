Write a query to calculate the total expenditure for each department in 2022. Additionally, for comparison purposes, return the average expense across all departments in 2022.

CREATE TABLE departments (
 id INTEGER PRIMARY KEY,
 name VARCHAR(100));

CREATE TABLE expenses (
 id INTEGER PRIMARY KEY,
 department_id INTEGER,
 amount FLOAT,
 date DATE,
 FOREIGN KEY (department_id) REFERENCES departments(id));

%sql
WITH department_totals AS (
    SELECT 
        d.id AS department_id,
        d.name AS department_name,
        COALESCE(SUM(e.amount), 0) AS total_expenditure
    FROM departments d
    LEFT JOIN expenses e 
        ON d.id = e.department_id 
        AND EXTRACT(YEAR FROM e.date) = 2022
    GROUP BY d.id, d.name
)
SELECT 
    department_id,
    department_name,
    total_expenditure,
    ROUND(AVG(total_expenditure) OVER (), 2) AS avg_expense_all_depts
FROM department_totals
ORDER BY department_id;

%python

from pyspark.sql import Window
from pyspark.sql.functions import col, sum, avg, coalesce, lit, year, round

# Filter expenses for 2022 and aggregate by department
expenses_2022 = expenses.filter(year("date") == 2022) \
                        .groupBy("department_id") \
                        .agg(sum("amount").alias("total_expenditure"))

# Join with departments (include departments with no expenses)
result = departments.join(expenses_2022, on="department_id", how="left") \
                    .fillna(0, subset=["total_expenditure"]) \
                    .select(
                        col("id").alias("department_id"),
                        col("name").alias("department_name"),
                        col("total_expenditure")
                    )

# Add average across all departments
result = result.withColumn("avg_expense_all_depts", 
                          round(avg("total_expenditure").over(Window.partitionBy()), 2))

result.orderBy("department_id").show()

------------------------

Write a query to identify customers who placed more than three transactions each in both 2019 and 2020.

CREATE TABLE users (
 id INTPRIMARYKEY,
 name VARCHAR(100));

CREATE TABLE transactions (
 id INTPRIMARYKEY,
 user_id INT,
 created_at DATETIME,
 product_id INT,
 quantity INT);

%sql
SELECT u.id, u.name
FROM users u
WHERE (
    SELECT COUNT(*)
    FROM transactions t
    WHERE t.user_id = u.id
    AND EXTRACT(YEAR FROM t.created_at) = 2019
) > 3
AND (
    SELECT COUNT(*)
    FROM transactions t
    WHERE t.user_id = u.id
    AND EXTRACT(YEAR FROM t.created_at) = 2020
) > 3
ORDER BY u.id;

%python
from pyspark.sql.functions import year, count, col

# Filter transactions for 2019 and 2020, add year column
transactions_filtered = transactions.filter(year("created_at").isin(2019, 2020)) \
                                     .withColumn("year", year("created_at"))

# Count transactions per user per year
user_year_counts = transactions_filtered.groupBy("user_id", "year") \
                                        .agg(count("*").alias("transaction_count"))

# Pivot to get 2019 and 2020 counts side by side
from pyspark.sql.functions import expr

pivot_counts = user_year_counts.groupBy("user_id") \
                               .pivot("year", [2019, 2020]) \
                               .agg(expr("first(transaction_count)")) \
                               .fillna(0)

# Filter users with >3 transactions in both years
qualified_users = pivot_counts.filter(
    (col("2019") > 3) & (col("2020") > 3)
).select("user_id")

# Join with users table to get names
result = users.join(qualified_users, users.id == qualified_users.user_id, "inner") \
              .select(users.id, users.name) \
              .orderBy("id")

result.show()

--------------------
Write a query to retrieve all user IDs in ascending order whose transactions have exactly a 10-second gap from one another.

CREATE TABLE bank_transaction(
 user_id INT,
 created_at DATETIME,
 transaction_value FLOAT);

%sql
WITH transaction_gaps AS (
    SELECT 
        user_id,
        created_at,
        LAG(created_at) OVER (PARTITION BY user_id ORDER BY created_at) AS prev_created_at,
        LEAD(created_at) OVER (PARTITION BY user_id ORDER BY created_at) AS next_created_at
    FROM bank_transaction
),
gap_calculation AS (
    SELECT 
        user_id,
        created_at,
        prev_created_at,
        next_created_at,
        EXTRACT(SECOND FROM (created_at - prev_created_at)) * 86400 AS gap_to_prev_seconds,
        CASE 
            WHEN prev_created_at IS NULL THEN 'FIRST'
            WHEN next_created_at IS NULL THEN 'LAST'
            ELSE 'MIDDLE'
        END AS position
    FROM transaction_gaps
)
SELECT DISTINCT user_id
FROM gap_calculation
WHERE user_id NOT IN (
    -- Exclude users who have any gap not equal to 10 seconds (excluding NULLs for first/last)
    SELECT user_id
    FROM gap_calculation
    WHERE gap_to_prev_seconds != 10
    AND prev_created_at IS NOT NULL
)
AND user_id IN (
    -- Include only users who have at least one gap (i.e., at least 2 transactions)
    SELECT user_id
    FROM bank_transaction
    GROUP BY user_id
    HAVING COUNT(*) >= 2
)
ORDER BY user_id;

%python
from pyspark.sql.functions import unix_timestamp, when, sum as spark_sum

window_spec = Window.partitionBy("user_id").orderBy("created_at")

# Calculate all gaps
df_with_gaps = bank_transaction.withColumn("prev_created_at", lag("created_at").over(window_spec)) \
                               .withColumn("unix_time", unix_timestamp("created_at")) \
                               .withColumn("prev_unix_time", unix_timestamp("prev_created_at"))

# Mark invalid gaps (where gap is not exactly 10 seconds)
df_with_validity = df_with_gaps.withColumn("invalid_gap", 
    when((col("prev_created_at").isNotNull()) & (col("unix_time") - col("prev_unix_time") != 10), 1)
    .otherwise(0)
)

# Aggregate to find users with no invalid gaps and at least 2 transactions
result = df_with_validity.groupBy("user_id") \
                         .agg(
                             spark_sum("invalid_gap").alias("invalid_gap_count"),
                             count("*").alias("total_txns")
                         ) \
                         .filter(
                             (col("invalid_gap_count") == 0) & 
                             (col("total_txns") >= 2)
                         ) \
                         .select("user_id") \
                         .orderBy("user_id")

result.show()

--------------------
Write query to return the 𝐜𝐮𝐬𝐭𝐨𝐦𝐞𝐫_𝐢𝐝 𝐚𝐧𝐝 𝐭𝐨𝐭𝐚𝐥 𝐚𝐦𝐨𝐮𝐧𝐭 of the customer who had the 𝐡𝐢𝐠𝐡𝐞𝐬𝐭 𝐭𝐨𝐭𝐚𝐥 𝐭𝐫𝐚𝐧𝐬𝐚𝐜𝐭𝐢𝐨𝐧 𝐚𝐦𝐨𝐮𝐧𝐭 for each month. 
The results should be sorted by year, month and total amount in descending order.

CREATE TABLE Customers (
 customer_id INTPRIMARYKEY,
 customer_name VARCHAR(100),
 sign_up_date DATE);

CREATE TABLE Transactions (
 transaction_id INTPRIMARYKEY,
 customer_id INT,
 transaction_date DATE,
 amount DECIMAL(10,2),
FOREIGNKEY (customer_id) REFERENCES Customers(customer_id));

%sql
WITH monthly_customer_totals AS (
    SELECT 
        EXTRACT(YEAR FROM t.transaction_date) AS year,
        EXTRACT(MONTH FROM t.transaction_date) AS month,
        t.customer_id,
        c.customer_name,
        SUM(t.amount) AS total_amount
    FROM Transactions t
    INNER JOIN Customers c ON t.customer_id = c.customer_id
    GROUP BY EXTRACT(YEAR FROM t.transaction_date), 
             EXTRACT(MONTH FROM t.transaction_date), 
             t.customer_id, 
             c.customer_name
),
ranked_months AS (
    SELECT 
        year,
        month,
        customer_id,
        customer_name,
        total_amount,
        ROW_NUMBER() OVER (PARTITION BY year, month ORDER BY total_amount DESC) AS rn
    FROM monthly_customer_totals
)
SELECT 
    year,
    month,
    customer_id,
    customer_name,
    total_amount
FROM ranked_months
WHERE rn = 1
ORDER BY year, month, total_amount DESC;

%python
from pyspark.sql import Window
from pyspark.sql.functions import col, sum, row_number, year, month, date_format, desc

# Calculate total amount per customer per month
monthly_customer_totals = transactions.join(customers, on="customer_id", how="inner") \
                                      .withColumn("year", year("transaction_date")) \
                                      .withColumn("month", month("transaction_date")) \
                                      .groupBy("year", "month", "customer_id", "customer_name") \
                                      .agg(sum("amount").alias("total_amount"))

# Rank customers within each month by total_amount
window_spec = Window.partitionBy("year", "month").orderBy(desc("total_amount"))

result = monthly_customer_totals.withColumn("rn", row_number().over(window_spec)) \
                                .filter(col("rn") == 1) \
                                .select("year", "month", "customer_id", "customer_name", "total_amount") \
                                .orderBy("year", "month", desc("total_amount"))

result.show()

-----------------------
Write a query to get the total three-day rolling average for deposits by day.

CREATE TABLE bank_transactions (
 user_id INT,
 created_at DATETIME,
 transaction_value FLOAT);

%sql
WITH daily_deposits AS (
    SELECT 
        TRUNC(created_at) AS deposit_date,
        SUM(transaction_value) AS daily_total
    FROM bank_transactions
    GROUP BY TRUNC(created_at)
)
SELECT 
    deposit_date,
    daily_total,
    ROUND(AVG(daily_total) OVER (ORDER BY deposit_date 
           ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS three_day_rolling_avg
FROM daily_deposits
ORDER BY deposit_date;

%python
from pyspark.sql import Window
from pyspark.sql.functions import col, sum, avg, round, to_date, coalesce, lit
from pyspark.sql.functions import row_number, datediff, date_add

# Method 1: Simple rolling average (only days with transactions)
daily_deposits = bank_transactions.withColumn("deposit_date", to_date("created_at")) \
                                  .groupBy("deposit_date") \
                                  .agg(sum("transaction_value").alias("daily_total")) \
                                  .orderBy("deposit_date")

window_spec = Window.orderBy("deposit_date").rowsBetween(-2, 0)

result = daily_deposits.withColumn("three_day_rolling_avg", 
                                   round(avg("daily_total").over(window_spec), 2))

result.show()
-----------------------

write a query to get the last transaction for each day. 
The output should include the id of the transaction, datetime of the transaction, and the transaction amount. Order the transactions by datetime.

CREATE TABLE bank_transactions (
 id INTEGER,
 created_at DATETIME,
 transaction_value FLOAT);

%sql
SELECT id, created_at, transaction_value
FROM (
    SELECT 
        id,
        created_at,
        transaction_value,
        ROW_NUMBER() OVER (PARTITION BY TRUNC(created_at) ORDER BY created_at DESC) AS rn
    FROM bank_transactions
)
WHERE rn = 1
ORDER BY created_at;

%python
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number, to_date, desc

# Add date column for partitioning
df_with_date = bank_transactions.withColumn("transaction_date", to_date("created_at"))

# Rank transactions within each day by datetime descending
window_spec = Window.partitionBy("transaction_date").orderBy(desc("created_at"))

result = df_with_date.withColumn("rn", row_number().over(window_spec)) \
                     .filter(col("rn") == 1) \
                     .select("id", "created_at", "transaction_value") \
                     .orderBy("created_at")

result.show()
---------------------
Write a query that outputs the name of each credit card and the difference in the number of issued cards between the month with the highest issuance cards and the lowest issuance. Arrange the results based on the largest disparity.

CREATE TABLE monthly_cards_issued (
 card_name VARCHAR(100),
 issued_amount INT,
 issue_month INT,
 issue_year INT );

%sql
WITH card_stats AS (
    SELECT 
        card_name,
        MAX(issued_amount) AS max_issuance,
        MIN(issued_amount) AS min_issuance,
        MAX(issued_amount) - MIN(issued_amount) AS disparity
    FROM monthly_cards_issued
    GROUP BY card_name
)
SELECT 
    card_name,
    max_issuance,
    min_issuance,
    disparity
FROM card_stats
ORDER BY disparity DESC, card_name;

%python
from pyspark.sql.functions import col, max, min, expr, desc

# Method 1: Simple aggregation
result = monthly_cards_issued.groupBy("card_name") \
                             .agg(
                                 max("issued_amount").alias("max_issuance"),
                                 min("issued_amount").alias("min_issuance")
                             ) \
                             .withColumn("disparity", col("max_issuance") - col("min_issuance")) \
                             .orderBy(desc("disparity"), "card_name")

result.show()

--------------------
Write a solution to find the daily active user count for a period of 30 days ending 2019-07-27 inclusively. A user was active on someday if they made at least one activity on that day.

𝐓𝐚𝐛𝐥𝐞: 𝐀𝐜𝐭𝐢𝐯𝐢𝐭𝐲
+---------------+---------+
| Column Name | Type |
+---------------+---------+
| user_id | int |
| session_id | int |
| activity_date | date |
| activity_type | enum |
+---------------+---------+

%sql
SELECT 
    activity_date AS day,
    COUNT(DISTINCT user_id) AS active_users
FROM Activity
WHERE activity_date BETWEEN DATE '2019-06-28' AND DATE '2019-07-27'
GROUP BY activity_date
ORDER BY activity_date;

%python
from pyspark.sql.functions import col, countDistinct, to_date, lit, date_add, coalesce
from pyspark.sql import SparkSession

# Method 1: Simple filter and group by
result = activity.filter(
    (col("activity_date") >= "2019-06-28") & 
    (col("activity_date") <= "2019-07-27")).groupBy(col("activity_date").alias("day")) \
 .agg(countDistinct("user_id").alias("active_users")) \
 .orderBy("day")

result.show(30)

--------------------

Find the total number of orders placed by each customer.

CREATE TABLE customer(
 custId INT PRIMARY KEY,
 customerName VARCHAR(100),
 custEmail VARCHAR(100),
 country VARCHAR(50));

CREATE TABLE orders(
 orderId INT PRIMARY KEY,
 orderDate DATE,
 custId INT,
 orderAmount INT);

CREATE TABLE order_items(
 itemId INT AUTO_INCREMENT PRIMARY KEY,
 orderId INT,
 productCode VARCHAR(50),
 qty INT,
 unitPrice DECIMAL(10,2));

%sql
SELECT 
    c.custId,
    c.customerName,
    c.custEmail,
    c.country,
    COUNT(o.orderId) AS total_orders
FROM customer c
LEFT JOIN orders o ON c.custId = o.custId
GROUP BY c.custId, c.customerName, c.custEmail, c.country
ORDER BY total_orders DESC, c.custId;

%python
from pyspark.sql.functions import col, count, sum as spark_sum, round, coalesce, lit

# Method 1: Simple aggregation (includes customers with zero orders)
order_counts = orders.groupBy("custId") \
                     .agg(count("orderId").alias("total_orders"))

result = customer.join(order_counts, on="custId", how="left") \
                 .fillna(0, subset=["total_orders"]) \
                 .select("custId", "customerName", "custEmail", "country", "total_orders") \
                 .orderBy(col("total_orders").desc(), "custId")

result.show()

----------------------
Given a Teams table with columns TeamID (integer) and Members (comma-separated string of names).
NOW write a query to calculate and display the total number of members in each team.

CREATE TABLE Teams (
 TeamID INT PRIMARY KEY,
 Members TEXT);

%sql
SELECT 
    TeamID,
    Members,
    CASE 
        WHEN Members IS NULL OR Members = '' THEN 0
        ELSE LENGTH(Members) - LENGTH(REPLACE(Members, ',', '')) + 1
    END AS total_members
FROM Teams
ORDER BY TeamID;

%python
from pyspark.sql.functions import col, split, size, when, length, regexp_replace

# Using split and size
result = teams.withColumn(
    "total_members",
    size(split(col("Members"), ","))
).select("TeamID", "Members", "total_members")

result.show()

---------------------
Write a SQL query to calculate total yearly revenue and Year-over-Year (YoY) growth from a transactions table.

CREATE TABLE transactions (
 customer_id INT,
 transaction_date DATE,
 amount NUMERIC);

%sql
WITH yearly_revenue AS (
    SELECT 
        EXTRACT(YEAR FROM transaction_date) AS year,
        SUM(amount) AS total_revenue
    FROM transactions
    GROUP BY EXTRACT(YEAR FROM transaction_date)
)
SELECT 
    year,
    total_revenue,
    LAG(total_revenue) OVER (ORDER BY year) AS prev_year_revenue,
    ROUND(
        (total_revenue - LAG(total_revenue) OVER (ORDER BY year)) / 
        LAG(total_revenue) OVER (ORDER BY year) * 100, 
        2
    ) AS yoy_growth_percent
FROM yearly_revenue
ORDER BY year;

%python
from pyspark.sql.functions import col, sum, year, lag, round, coalesce, lit, when

# Using window function
yearly_revenue = transactions.withColumn("year", year("transaction_date")) \
                             .groupBy("year") \
                             .agg(sum("amount").alias("total_revenue")) \
                             .orderBy("year")

window_spec = Window.orderBy("year")

result = yearly_revenue.withColumn("prev_year_revenue", lag("total_revenue").over(window_spec)) \
                       .withColumn("yoy_growth_percent",
                           when(
                               col("prev_year_revenue").isNotNull(),
                               round((col("total_revenue") - col("prev_year_revenue")) / 
                                     col("prev_year_revenue") * 100, 2)
                           ).otherwise(lit(None))
                       ) \
                       .select("year", "total_revenue", "yoy_growth_percent")

result.show()

---------------------
Write a SQL query to retrieve the first and last order for each customer from the orders table.

CREATE TABLE orders2 (
 order_id INT PRIMARY KEY,
 customer_id INT,
 order_date DATE);

%sql
WITH ranked_orders AS (
    SELECT 
        order_id,
        customer_id,
        order_date,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date ASC) AS rn_first,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn_last
    FROM orders2
)
SELECT 
    customer_id,
    MAX(CASE WHEN rn_first = 1 THEN order_id END) AS first_order_id,
    MAX(CASE WHEN rn_first = 1 THEN order_date END) AS first_order_date,
    MAX(CASE WHEN rn_last = 1 THEN order_id END) AS last_order_id,
    MAX(CASE WHEN rn_last = 1 THEN order_date END) AS last_order_date
FROM ranked_orders
GROUP BY customer_id
ORDER BY customer_id;

%python
from pyspark.sql import Window
from pyspark.sql.functions import col, first, last, row_number, min, max, collect_list, struct

# Method 1: Using row_number for first and last
window_first = Window.partitionBy("customer_id").orderBy("order_date", "order_id")
window_last = Window.partitionBy("customer_id").orderBy(col("order_date").desc(), col("order_id").desc())

df_with_rn = orders2.withColumn("rn_first", row_number().over(window_first)) \
                    .withColumn("rn_last", row_number().over(window_last))

first_orders = df_with_rn.filter(col("rn_first") == 1) \
                         .select("customer_id", 
                                 col("order_id").alias("first_order_id"),
                                 col("order_date").alias("first_order_date"))

last_orders = df_with_rn.filter(col("rn_last") == 1) \
                        .select("customer_id", 
                                col("order_id").alias("last_order_id"),
                                col("order_date").alias("last_order_date"))

result = first_orders.join(last_orders, on="customer_id", how="inner") \
                     .orderBy("customer_id")

result.show()

----------------------
Can you retrieve the most frequently ordered item(s) for each date?
If multiple items have the same highest count, include all of them.

CREATE TABLE orders1(
 order_date DATE,
 item VARCHAR(50));

%sql
WITH daily_item_counts AS (
    SELECT 
        order_date,
        item,
        COUNT(*) AS order_count
    FROM orders1
    GROUP BY order_date, item
),
ranked_items AS (
    SELECT 
        order_date,
        item,
        order_count,
        RANK() OVER (PARTITION BY order_date ORDER BY order_count DESC) AS rnk
    FROM daily_item_counts
)
SELECT 
    order_date,
    item,
    order_count
FROM ranked_items
WHERE rnk = 1
ORDER BY order_date, item;

%python
from pyspark.sql import Window
from pyspark.sql.functions import col, count, rank, desc

# Method 1: Using rank
daily_item_counts = orders1.groupBy("order_date", "item") \
                           .agg(count("*").alias("order_count"))

window_spec = Window.partitionBy("order_date").orderBy(desc("order_count"))

result = daily_item_counts.withColumn("rnk", rank().over(window_spec)) \
                          .filter(col("rnk") == 1) \
                          .select("order_date", "item", "order_count") \
                          .orderBy("order_date", "item")

result.show()

--------------------
Can you calculate the day-over-day change in sales?”

CREATE TABLE sales_data (
 sale_date DATE,
 total_sales INT);

%sql
SELECT 
    sale_date,
    total_sales,
    LAG(total_sales) OVER (ORDER BY sale_date) AS previous_day_sales,
    total_sales - LAG(total_sales) OVER (ORDER BY sale_date) AS absolute_change,
    ROUND(
        (total_sales - LAG(total_sales) OVER (ORDER BY sale_date)) / 
        LAG(total_sales) OVER (ORDER BY sale_date) * 100, 
        2
    ) AS percent_change,
    CASE 
        WHEN total_sales > LAG(total_sales) OVER (ORDER BY sale_date) THEN 'Increase'
        WHEN total_sales < LAG(total_sales) OVER (ORDER BY sale_date) THEN 'Decrease'
        WHEN total_sales = LAG(total_sales) OVER (ORDER BY sale_date) THEN 'No Change'
        ELSE 'N/A'
    END AS trend
FROM sales_data
ORDER BY sale_date;

%python
from pyspark.sql import Window
from pyspark.sql.functions import col, lag, round, when, datediff

# Method 1: Using lag window function
window_spec = Window.orderBy("sale_date")

result = sales_data.withColumn("previous_day_sales", lag("total_sales").over(window_spec)) \
                   .withColumn("absolute_change", 
                       col("total_sales") - col("previous_day_sales")
                   ) \
                   .withColumn("percent_change",
                       round(
                           (col("total_sales") - col("previous_day_sales")) / 
                           col("previous_day_sales") * 100, 
                           2
                       )
                   ) \
                   .withColumn("trend",
                       when(col("total_sales") > col("previous_day_sales"), "Increase")
                       .when(col("total_sales") < col("previous_day_sales"), "Decrease")
                       .when(col("total_sales") == col("previous_day_sales"), "No Change")
                       .otherwise("N/A")
                   ) \
                   .select("sale_date", "total_sales", "previous_day_sales", 
                           "absolute_change", "percent_change", "trend")

result.orderBy("sale_date").show()

-------------------
𝑾𝒉𝒊𝒄𝒉 𝒑𝒓𝒐𝒅𝒖𝒄𝒕 𝒉𝒂𝒔 𝒕𝒉𝒆 𝒉𝒊𝒈𝒉𝒆𝒔𝒕 𝒂𝒗𝒆𝒓𝒂𝒈𝒆 𝒑𝒓𝒊𝒄𝒆?

CREATE TABLE sales (
 Product VARCHAR(20),
 Quantity INT,
 Price DECIMAL(10,2),
 Date DATE);

%sql
SELECT 
    Product,
    AVG(Price) AS avg_price
FROM sales
GROUP BY Product
ORDER BY avg_price DESC
FETCH FIRST 1 ROW ONLY;

%python
from pyspark.sql.functions import col, avg, round, count, min, max, stddev
from pyspark.sql import Window

# Simple aggregation with order and limit
product_avg = sales.groupBy("Product") \
                   .agg(avg("Price").alias("avg_price")) \
                   .orderBy(col("avg_price").desc()) \
                   .limit(1)

result = product_avg.withColumn("avg_price", round(col("avg_price"), 2))
result.show()

------------------
Write a SQL query to report the card_name and issued_amount in the launch month. Order the results by issued_amount in descending order.

𝑻𝒂𝒃𝒍𝒆: monthly_cards_issued

𝒊𝒔𝒔𝒖𝒆_𝒎𝒐𝒏𝒕𝒉 | 𝒊𝒔𝒔𝒖𝒆_𝒚𝒆𝒂𝒓 | 𝒄𝒂𝒓𝒅_𝒏𝒂𝒎𝒆 | 𝒊𝒔𝒔𝒖𝒆𝒅_𝒂𝒎𝒐𝒖𝒏𝒕

%sql
WITH ranked_cards AS (
    SELECT 
        card_name,
        issued_amount,
        issue_month,
        issue_year,
        ROW_NUMBER() OVER (
            PARTITION BY card_name 
            ORDER BY issue_year, issue_month
        ) AS rn
    FROM monthly_cards_issued
)
SELECT 
    card_name,
    issued_amount
FROM ranked_cards
WHERE rn = 1
ORDER BY issued_amount DESC;

%python
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number, min, struct, first, desc

# Using row_number
window_spec = Window.partitionBy("card_name").orderBy("issue_year", "issue_month")

result = monthly_cards_issued.withColumn("rn", row_number().over(window_spec)) \
                             .filter(col("rn") == 1) \
                             .select("card_name", "issued_amount") \
                             .orderBy(desc("issued_amount"))

result.show()

------------------
Write a SQL query to report the product_name, year, and price for each sale.

𝑻𝒂𝒃𝒍𝒆: 𝑺𝒂𝒍𝒆𝒔
𝐬𝐚𝐥𝐞_𝐢𝐝 | 𝐩𝐫𝐨𝐝𝐮𝐜𝐭_𝐢𝐝 | 𝐲𝐞𝐚𝐫 | 𝐪𝐮𝐚𝐧𝐭𝐢𝐭𝐲 | 𝐩𝐫𝐢𝐜𝐞
(𝒔𝒂𝒍𝒆_𝒊𝒅, 𝒚𝒆𝒂𝒓) 𝒊𝒔 𝒕𝒉𝒆 𝒑𝒓𝒊𝒎𝒂𝒓𝒚 𝒌𝒆𝒚.
product_id references the Product table.

𝑻𝒂𝒃𝒍𝒆: 𝑷𝒓𝒐𝒅𝒖𝒄𝒕
𝒑𝒓𝒐𝒅𝒖𝒄𝒕_𝒊𝒅 | 𝒑𝒓𝒐𝒅𝒖𝒄𝒕_𝒏𝒂𝒎𝒆
𝒑𝒓𝒐𝒅𝒖𝒄𝒕_𝒊𝒅 𝒊𝒔 𝒕𝒉𝒆 𝒑𝒓𝒊𝒎𝒂𝒓𝒚 𝒌𝒆𝒚.

%sql
SELECT 
    p.product_name,
    s.year,
    s.price
FROM Sales s
INNER JOIN Product p ON s.product_id = p.product_id
ORDER BY s.year, p.product_name, s.price;

%python
from pyspark.sql.functions import col, sum as spark_sum, count, round

# Simple join
result = sales.join(product, on="product_id", how="inner") \
              .select("product_name", "year", "price") \
              .orderBy("year", "product_name", "price")

result.show()

-----------------------------
Calculate number of bank accounts for each salary category:
 • Low Salary → income < 20000
 • Average Salary → income between 20000 and 50000 (inclusive)
 • High Salary → income > 50000

𝑻𝒂𝒃𝒍𝒆: 𝑨𝒄𝒄𝒐𝒖𝒏𝒕𝒔
| 𝒂𝒄𝒄𝒐𝒖𝒏𝒕_𝒊𝒅 | 𝒊𝒏𝒄𝒐𝒎𝒆 |

%sql
SELECT 
    CASE 
        WHEN income < 20000 THEN 'Low Salary'
        WHEN income BETWEEN 20000 AND 50000 THEN 'Average Salary'
        WHEN income > 50000 THEN 'High Salary'
    END AS salary_category,
    COUNT(*) AS account_count
FROM Accounts
GROUP BY 
    CASE 
        WHEN income < 20000 THEN 'Low Salary'
        WHEN income BETWEEN 20000 AND 50000 THEN 'Average Salary'
        WHEN income > 50000 THEN 'High Salary'
    END
ORDER BY 
    CASE salary_category
        WHEN 'Low Salary' THEN 1
        WHEN 'Average Salary' THEN 2
        WHEN 'High Salary' THEN 3
    END;


%python
from pyspark.sql.functions import col, when, count, lit

# Using when and otherwise
result = accounts.withColumn(
    "salary_category",
    when(col("income") < 20000, "Low Salary")
    .when((col("income") >= 20000) & (col("income") <= 50000), "Average Salary")
    .when(col("income") > 50000, "High Salary")
    .otherwise("Unknown")
).groupBy("salary_category") \
 .agg(count("*").alias("account_count")) \
 .orderBy(
     when(col("salary_category") == "Low Salary", 1)
     .when(col("salary_category") == "Average Salary", 2)
     .when(col("salary_category") == "High Salary", 3)
 )

result.show()

--------------------------
Write a solution to find managers with at least five direct reports.

𝑻𝒂𝒃𝒍𝒆: Employee
+-------------+---------+
| 𝑪𝒐𝒍𝒖𝒎𝒏 𝑵𝒂𝒎𝒆 | 𝑻𝒚𝒑𝒆 |
+-------------+---------+
| id | int |
| name | varchar |
| department | varchar |
| managerId | int |
+-------------+---------+

%sql
SELECT 
    m.id AS manager_id,
    m.name AS manager_name,
    COUNT(e.id) AS direct_reports
FROM Employee e
INNER JOIN Employee m ON e.managerId = m.id
GROUP BY m.id, m.name
HAVING COUNT(e.id) >= 5
ORDER BY direct_reports DESC;

%python
from pyspark.sql.functions import col, count, desc

#  Group by managerId and join with employee table
# Count direct reports per manager
report_counts = employee.filter(col("managerId").isNotNull()) \
                        .groupBy("managerId") \
                        .agg(count("*").alias("direct_reports")) \
                        .filter(col("direct_reports") >= 5)


---------------------
Count the number of companies that posted duplicate job listings.
𝑪𝒐𝒍𝒖𝒎𝒏𝒔-𝑵𝒂𝒎𝒆: 𝐣𝐨𝐛_𝐢𝐝 | 𝐜𝐨𝐦𝐩𝐚𝐧𝐲_𝐢𝐝 | 𝐭𝐢𝐭𝐥𝐞 | 𝐝𝐞𝐬𝐜𝐫𝐢𝐩𝐭𝐢𝐨𝐧

%sql
SELECT COUNT(DISTINCT company_id) AS companies_with_duplicates
FROM (
    SELECT 
        company_id,
        title,
        description,
        COUNT(*) AS duplicate_count
    FROM job_listings
    GROUP BY company_id, title, description
    HAVING COUNT(*) > 1
);

%python
from pyspark.sql.functions import col, count, desc, sum as spark_sum

# Simple count of companies with duplicates
duplicate_companies = job_listings.groupBy("company_id", "title", "description") \
                                  .agg(count("*").alias("duplicate_count")) \
                                  .filter(col("duplicate_count") > 1) \
                                  .select("company_id") \
                                  .distinct()

result = duplicate_companies.agg(count("*").alias("companies_with_duplicates"))
result.show()

------------------
