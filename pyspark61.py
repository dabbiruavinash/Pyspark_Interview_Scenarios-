📌 Question 1: Find Customers Who Purchased in Consecutive Months 
Table: orders customer_id, order_date 
Requirement: Identify customers who placed orders in consecutive months.

%sql
WITH monthly_orders AS (
SELECT DISTINCT customer_id, DATE_TRUNC('month', order_date) AS order_month FROM orders),
consecutive_orders AS (
SELECT customer_id, order_month, LAG(order_month) OVER (PARTITION BY customer_id ORDER BY order_month) AS prev_month FROM monthly_orders)
SELECT customer_id FROM consecutive_orders WHERE order_month = prev_month + INTERVAL '1 month';

%pyspark
monthly_orders = orders.select("customer_id", date_trunc("month", "order_date").alias("order_month")).distinct()

window_spec = Window.partitionBy("customer_id").orderBy("order_month")
consecutive_orders = monthly_orders.withColumn("prev_month", lag("order_month").over(window_spec))

result_q1 = consecutive_orders.filter(expr("order_month = add_months(prev_month, 1)")).select("customer_id").distinct()

📌 Question 2: Find the Top 3 Customers by Revenue Each Month
Table: orders customer_id, amount, order_date

%sql
WITH customer_revenue AS (
SELECT DATE_TRUNC('month', order_date) AS month, customer_id, SUM(amount) AS revenue FROM orders GROUP BY 1, 2)

SELECT * FROM (
 SELECT *,
 DENSE_RANK() OVER (PARTITION BY month ORDER BY revenue DESC) AS rnk FROM customer_revenue) t WHERE rnk <= 3;

%pyspark
customer_revenue = orders.withColumn(
    "month", date_trunc("month", "order_date")).groupBy("month", "customer_id").agg(sum("amount").alias("revenue"))

window_spec = Window.partitionBy("month").orderBy(col("revenue").desc())
result_q2 = customer_revenue.withColumn("rnk", dense_rank().over(window_spec)) \
    .filter(col("rnk") <= 3)

📌 Question 3: Calculate Running Total Revenue
Table: sales sale_date, amount 
Requirement: Show cumulative revenue over time.

%sql
SELECT sale_date, amount, SUM(amount) OVER (ORDER BY sale_date) AS running_revenue FROM sales;

%pyspark
window_spec = Window.orderBy("sale_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
result_q3 = sales.withColumn("running_revenue", sum("amount").over(window_spec))

📌 Question 4: Find Users Who Have Not Logged In During the Last 30 Days
Tables: users user_id, logins user_id, login_date

%sql
SELECT u.user_id FROM users u LEFT JOIN logins l ON u.user_id = l.user_id GROUP BY u.user_id HAVING MAX(login_date) < CURRENT_DATE - INTERVAL '30 days' OR MAX(login_date) IS NULL;

%python
result_q4 = users.join(logins, "user_id", "left") \
    .groupBy("user_id") \
    .agg(max("login_date").alias("last_login")) \
    .filter(
        (col("last_login") < date_sub(current_date(), 30)) |
        col("last_login").isNull())

📌 Question 5: Detect Duplicate Transactions
Table: transactions transaction_id, customer_id, amount, transaction_date 
Requirement: Find duplicate transactions based on customer, amount, and date.

%sql
SELECT
 customer_id,
 amount,
 transaction_date,
 COUNT(*) AS duplicate_count FROM transactions GROUP BY customer_id, amount, transaction_date HAVING COUNT(*) > 1;

%python
result_q5 = transactions.groupBy("customer_id", "amount", "transaction_date") \
    .agg(count("*").alias("duplicate_count")) \
    .filter(col("duplicate_count") > 1)

📌 Question 6: Calculate Average Order Value by Month
Table: orders order_id, amount, order_date

%sql
SELECT DATE_TRUNC('month', order_date) AS month, ROUND(AVG(amount), 2) AS avg_order_value FROM orders GROUP BY DATE_TRUNC('month', order_date) ORDER BY month;

%python
result_q6 = orders.withColumn("month", date_trunc("month", "order_date")) \
    .groupBy("month") \
    .agg(round(avg("amount"), 2).alias("avg_order_value")) \
    .orderBy("month")

📌 Question 7: Find the Most Recent Order for Each Customer
Table: orders order_id, customer_id, order_date

%sql
WITH ranked_orders AS (
SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn FROM orders)
SELECT customer_id, order_id, order_date FROM ranked_orders WHERE rn = 1;

%python
window_spec = Window.partitionBy("customer_id").orderBy(col("order_date").desc())
result_q7 = orders.withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .select("customer_id", "order_id", "order_date")

📌 Question 8: Calculate Product Contribution to Total Revenue 
Table: sales product_id, amount 
Requirement: Find percentage contribution of each product.

%sql
SELECT
 product_id,
 SUM(amount) AS revenue,
 ROUND(
 100.0 * SUM(amount) / SUM(SUM(amount)) OVER (), 2) AS contribution_pct FROM sales GROUP BY product_id;

%python
result_q8 = sales.groupBy("product_id") \
    .agg(sum("amount").alias("revenue")) \
    .withColumn("contribution_pct", 
                round(100.0 * col("revenue") / sum("revenue").over(Window.partitionBy()), 2))

📌 Question 9: Find Customers with No Orders
Tables: customers customer_id, orders customer_id

%sql
SELECT c.customer_id FROM customers c LEFT JOIN orders o ON c.customer_id = o.customer_id WHERE o.customer_id IS NULL;

%python
result_q9 = customers.join(orders, "customer_id", "left") \
    .filter(col("orders.customer_id").isNull()) \
    .select("customers.customer_id")

📌 Question 10: Calculate 7-Day Moving Average Sales
Table: sales sale_date, amount

%sql
SELECT
 sale_date,
 amount,
 ROUND(
 AVG(amount) OVER (ORDER BY sale_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS moving_avg_7_days FROM sales;

%python
window_spec = Window.orderBy("sale_date").rowsBetween(-6, 0)
result_q10 = sales.withColumn("moving_avg_7_days", 
                              round(avg("amount").over(window_spec), 2))

📌 Question 11: Find the Second Highest Salary in Each Department 
Table: employees employee_id, department_id, salary

%sql
WITH ranked_salary AS (
 SELECT *,
 DENSE_RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) AS rnk FROM employees)
SELECT department_id, employee_id, salary FROM ranked_salary WHERE rnk = 2;

%python
window_spec = Window.partitionBy("department_id").orderBy(col("salary").desc())
result_q11 = employees.withColumn("rnk", dense_rank().over(window_spec)) \
    .filter(col("rnk") == 2) \
    .select("department_id", "employee_id", "salary")

📌 Question 12: Identify Users Who Purchased on Their First Visit
Tables: visits user_id, visit_date | orders user_id, order_date

%sql
WITH first_visit AS (
 SELECT user_id, MIN(visit_date) AS first_visit_date FROM visits GROUP BY user_id)
SELECT DISTINCT f.user_id FROM first_visit f JOIN orders o ON f.user_id = o.user_id AND f.first_visit_date = o.order_date;

%python
first_visit = visits.groupBy("user_id").agg(min("visit_date").alias("first_visit_date"))
result_q12 = first_visit.join(orders, 
    (first_visit.user_id == orders.user_id) & 
    (first_visit.first_visit_date == orders.order_date), "inner") \
    .select("first_visit.user_id").distinct()

📌 Question 13: Find Products Never Sold
Tables: products product_id, product_name | sales product_id

%sql
SELECT p.product_id, p.product_name FROM products p LEFT JOIN sales s ON p.product_id = s.product_id WHERE s.product_id IS NULL;

%python
result_q13 = products.join(sales, "product_id", "left") \
    .filter(col("sales.product_id").isNull()) \
    .select("products.product_id", "products.product_name")

📌 Question 14: Calculate Month-over-Month Revenue Growth
Table: orders order_date, revenue

%sql
WITH monthly_revenue AS (
SELECT DATE_TRUNC('month', order_date) AS month, SUM(revenue) AS total_revenue FROM orders GROUP BY 1)
SELECT month,
 total_revenue,
 LAG(total_revenue) OVER (ORDER BY month) AS previous_month, ROUND(100.0 * (total_revenue - LAG(total_revenue) OVER (ORDER BY month)) /
 LAG(total_revenue) OVER (ORDER BY month), 2 ) AS growth_pct FROM monthly_revenue;

%python
monthly_revenue = orders.withColumn("month", date_trunc("month", "order_date")) \
    .groupBy("month").agg(sum("revenue").alias("total_revenue"))

window_spec = Window.orderBy("month")
result_q14 = monthly_revenue.withColumn("previous_month", lag("total_revenue").over(window_spec)) \
    .withColumn("growth_pct", 
                round(100.0 * (col("total_revenue") - col("previous_month")) / col("previous_month"), 2))

📌 Question 15: Find Employees Earning More Than Department Average
Table: employees employee_id, department_id, salary

%sql
SELECT employee_id, department_id, salary FROM (SELECT *, AVG(salary) OVER (PARTITION BY department_id) AS dept_avg FROM employees) t WHERE salary > dept_avg;

%python
window_spec = Window.partitionBy("department_id")
result_q15 = employees.withColumn("dept_avg", avg("salary").over(window_spec)) \
    .filter(col("salary") > col("dept_avg")) \
    .select("employee_id", "department_id", "salary")

📌 Question 16: Find Longest Consecutive Login Streak 
Table: logins user_id, login_date

%sql
WITH cte AS (
 SELECT user_id,
 login_date,
 login_date - ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date) * INTERVAL '1 day' AS grp FROM logins)
SELECT user_id, COUNT(*) AS streak_days FROM cte GROUP BY user_id, grp ORDER BY streak_days DESC;

%python
window_spec = Window.partitionBy("user_id").orderBy("login_date")
result_q16 = logins.withColumn("rn", row_number().over(window_spec)) \
    .withColumn("grp", expr("login_date - INTERVAL rn DAYS")) \
    .groupBy("user_id", "grp") \
    .agg(count("*").alias("streak_days")) \
    .orderBy(col("streak_days").desc())

📌 Question 17: Find Peak Sales Day of Every Month
 Table: sales sale_date, amount

%sql
WITH daily_sales AS (
SELECT DATE(sale_date) AS sale_day, SUM(amount) AS revenue FROM sales GROUP BY DATE(sale_date)
SELECT * FROM (
 SELECT *,ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('month', sale_day) ORDER BY revenue DESC) rn FROM daily_sales) t WHERE rn = 1;

%python
daily_sales = sales.withColumn("sale_day", to_date("sale_date")) \
    .groupBy("sale_day").agg(sum("amount").alias("revenue"))

window_spec = Window.partitionBy(date_trunc("month", "sale_day")).orderBy(col("revenue").desc())
result_q17 = daily_sales.withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1)

📌 Question 18: Find Customers Who Ordered Every Month 
Table: orders customer_id, order_date

%sql
WITH customer_months AS (
 SELECT customer_id, COUNT(DISTINCT DATE_TRUNC('month', order_date)) AS months_active FROM orders GROUP BY customer_id),
total_months AS (
SELECT COUNT(DISTINCT DATE_TRUNC('month', order_date)) AS total_months FROM orders)
SELECT customer_id FROM customer_months c CROSS JOIN total_months t WHERE c.months_active = t.total_months;

%python
customer_months = orders.withColumn("month", date_trunc("month", "order_date")) \
    .groupBy("customer_id").agg(countDistinct("month").alias("months_active"))

total_months = orders.select(countDistinct(date_trunc("month", "order_date")).alias("total_months"))

result_q18 = customer_months.crossJoin(total_months) \
    .filter(col("months_active") == col("total_months")) \
    .select("customer_id")

📌 Question 19: Find Top Selling Product Category
 Tables: products product_id, category | sales product_id, quantity

%sql
SELECT category, SUM(quantity) AS total_sold FROM sales s JOIN products p ON s.product_id = p.product_id GROUP BY category ORDER BY total_sold DESC LIMIT 1;

%python
result_q19 = sales.join(products, "product_id") \
    .groupBy("category") \
    .agg(sum("quantity").alias("total_sold")) \
    .orderBy(col("total_sold").desc()) \
    .limit(1)

📌 Question 20: Calculate Median Salary 
Table: employees employee_id, salary

%sql
SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) AS median_salary FROM employees;

%python
result_q20 = employees.select(expr("percentile_approx(salary, 0.5)").alias("median_salary"))

📌 Question 21: Find the First Purchase Date for Every Customer
Table: orders order_id, customer_id, order_date

%sql
SELECT customer_id, MIN(order_date) AS first_purchase_date FROM orders GROUP BY customer_id;

%python
result_q21 = orders.groupBy("customer_id").agg(min("order_date").alias("first_purchase_date"))

📌 Question 22: Calculate Customer Lifetime Value CLV
Table: orders customer_id, amount 
Requirement: Total revenue generated by each customer.

%sql
SELECT customer_id, SUM(amount) AS lifetime_value FROM orders GROUP BY customer_id ORDER BY lifetime_value DESC;

%python
result_q22 = orders.groupBy("customer_id").agg(sum("amount").alias("lifetime_value")) \
    .orderBy(col("lifetime_value").desc())

📌 Question 23: Find the Top 5 Products Contributing 80% of Revenue
Table: sales product_id, amount

%sql
WITH product_revenue AS (
SELECT product_id, SUM(amount) AS revenue FROM sales GROUP BY product_id),
revenue_rank AS (
SELECT product_id, revenue,
 SUM(revenue) OVER (ORDER BY revenue DESC) AS running_revenue,
 SUM(revenue) OVER () AS total_revenue FROM product_revenue)

SELECT product_id, revenue, ROUND(100.0 * running_revenue / total_revenue, 2) AS cumulative_pct FROM revenue_rank WHERE running_revenue <= total_revenue * 0.80;

%python
product_revenue = sales.groupBy("product_id").agg(sum("amount").alias("revenue"))

window_spec = Window.orderBy(col("revenue").desc())
revenue_rank = product_revenue.withColumn("running_revenue", 
    sum("revenue").over(window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow))) \
    .withColumn("total_revenue", sum("revenue").over(Window.partitionBy()))

result_q23 = revenue_rank.withColumn("cumulative_pct", 
    round(100.0 * col("running_revenue") / col("total_revenue"), 2)) \
    .filter(col("running_revenue") <= col("total_revenue") * 0.8) \
    .select("product_id", "revenue", "cumulative_pct") \
    .orderBy(col("cumulative_pct").desc())

📌 Question 24: Find Customers Who Purchased More Than 3 Different Products
Table: orders customer_id, product_id

%sql
SELECT customer_id FROM orders GROUP BY customer_id HAVING COUNT(DISTINCT product_id) > 3;

%python
result_q24 = orders.groupBy("customer_id") \
    .agg(countDistinct("product_id").alias("product_count")) \
    .filter(col("product_count") > 3) \
    .select("customer_id")

📌 Question 25: Find the Highest Revenue Order for Each Customer 
Table: orders order_id, customer_id, amount

%sql
WITH ranked_orders AS (
 SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS rn FROM orders)
SELECT customer_id, order_id, amount FROM ranked_orders WHERE rn = 1;

%python
window_spec = Window.partitionBy("customer_id").orderBy(col("amount").desc())
result_q25 = orders.withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .select("customer_id", "order_id", "amount")

📌 Question 26: Calculate Average Time Between Orders 
Table: orders customer_id, order_date

%sql
WITH order_gap AS (
 SELECT 
 customer_id,
 order_date, LAG(order_date) OVER (PARTITION BY customer_id  ORDER BY order_date) AS previous_order FROM orders)
SELECT customer_id, AVG(order_date - previous_order) AS avg_days_between_orders FROM order_gap WHERE previous_order IS NOT NULL GROUP BY customer_id;

%python
window_spec = Window.partitionBy("customer_id").orderBy("order_date")
order_gap = orders.withColumn("previous_order", lag("order_date").over(window_spec))

result_q26 = order_gap.filter(col("previous_order").isNotNull()) \
    .withColumn("gap_days", datediff("order_date", "previous_order")) \
    .groupBy("customer_id") \
    .agg(avg("gap_days").alias("avg_days_between_orders"))

📌 Question 27: Find Users Who Abandoned Their Cart 
Tables: cart user_id, product_id, orders user_id 
Requirement: Users who added items to their cart but never completed a purchase.

%sql
SELECT DISTINCT c.user_id FROM cart c LEFT JOIN orders o ON c.user_id = o.user_id WHERE o.user_id IS NULL;

%python
result_q27 = cart.join(orders, "user_id", "left") \
    .filter(col("orders.user_id").isNull()) \
    .select("cart.user_id").distinct()

📌 Question 28: Find Revenue Generated by New vs Returning Customers 
Tables: users user_id, signup_date, orders user_id, amount, order_date

%sql
SELECT CASE 
 WHEN DATE_TRUNC('month', signup_date) = DATE_TRUNC('month', order_date) THEN 'New' ELSE 'Returning' END AS customer_type,
 SUM(amount) AS revenue FROM users u JOIN orders o  ON u.user_id = o.user_id GROUP BY customer_type;

%python
result_q28 = users.join(orders, "user_id") \
    .withColumn("customer_type", 
                when(date_trunc("month", "signup_date") == date_trunc("month", "order_date"), "New")
                .otherwise("Returning")) \
    .groupBy("customer_type") \
    .agg(sum("amount").alias("revenue"))

📌 Question 29: Find the Most Frequently Purchased Product Pair 
Table: order_items order_id, product_id

%sql
SELECT 
 a.product_id AS product_1,
 b.product_id AS product_2,
 COUNT(*) AS pair_count FROM order_items a JOIN order_items b ON a.order_id = b.order_id AND a.product_id < b.product_id GROUP BY a.product_id,  b.product_id ORDER BY pair_count DESC LIMIT 1;

%python
result_q29 = order_items.alias("a") \
    .join(order_items.alias("b"), 
          (col("a.order_id") == col("b.order_id")) & 
          (col("a.product_id") < col("b.product_id"))).groupBy(col("a.product_id").alias("product_1"), col("b.product_id").alias("product_2")) \
    .agg(count("*").alias("pair_count")) \
    .orderBy(col("pair_count").desc()) \
    .limit(1)

📌 Question 30: Calculate Revenue Contribution by Region 
Tables: customers customer_id, region, orders customer_id, amount

%sql
SELECT 
 c.region,
 SUM(o.amount) AS revenue,
 ROUND(100.0 * SUM(o.amount) / SUM(SUM(o.amount)) OVER (),  2) AS revenue_share FROM customers c JOIN orders o ON c.customer_id = o.customer_id GROUP BY c.region ORDER BY revenue DESC;

%python
result_q30 = customers.join(orders, "customer_id") \
    .groupBy("region") \
    .agg(sum("amount").alias("revenue")) \
    .withColumn("revenue_share", 
                round(100.0 * col("revenue") / sum("revenue").over(Window.partitionBy()), 2)) \
    .orderBy(col("revenue").desc())

📌 Question 31: Find Customers Who Increased Their Monthly Spending* 
Table: orders customer_id, amount, order_date 
Requirement: Return customers whose spending increased compared to the previous month.

%sql
WITH monthly_spend AS (
SELECT customer_id, DATE_TRUNC('month', order_date) AS month, SUM(amount) AS total_spend FROM orders GROUP BY customer_id, DATE_TRUNC('month', order_date))
SELECT customer_id, month,total_spend FROM (
SELECT *, LAG(total_spend) OVER (PARTITION BY customer_id ORDER BY month) AS prev_spend FROM monthly_spend) t WHERE total_spend > prev_spend;

%python
monthly_spend = orders.withColumn("month", date_trunc("month", "order_date")) \
    .groupBy("customer_id", "month") \
    .agg(sum("amount").alias("total_spend"))

window_spec = Window.partitionBy("customer_id").orderBy("month")
result_q31 = monthly_spend.withColumn("prev_spend", lag("total_spend").over(window_spec)) \
    .filter(col("total_spend") > col("prev_spend")) \
    .select("customer_id", "month", "total_spend")

📌 Question 32: Find Products Purchased Together Most Often 
Table: order_items order_id, product_id

%sql
SELECT 
 a.product_id AS product_1,
 b.product_id AS product_2,
 COUNT(*) AS frequency FROM order_items a JOIN order_items b 
 ON a.order_id = b.order_id AND a.product_id < b.product_id GROUP BY 1,2 ORDER BY frequency DESC LIMIT 10;

%python
result_q32 = order_items.alias("a") \
    .join(order_items.alias("b"), 
          (col("a.order_id") == col("b.order_id")) & 
          (col("a.product_id") < col("b.product_id"))) \
    .groupBy(col("a.product_id").alias("product_1"), 
             col("b.product_id").alias("product_2")) \
    .agg(count("*").alias("frequency")) \
    .orderBy(col("frequency").desc()) \
    .limit(10)

📌 Question 33: Find Users Active for 7 Consecutive Days 
Table: user_activity user_id, activity_date

%sql
WITH activity AS (
 SELECT 
 user_id,
 activity_date,
 activity_date - ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY activity_date) * INTERVAL '1 day' AS grp FROM user_activity)
SELECT  user_id FROM activity GROUP BY user_id, grp HAVING COUNT(*) >= 7;

%python
window_spec = Window.partitionBy("user_id").orderBy("activity_date")
result_q33 = user_activity.withColumn("rn", row_number().over(window_spec)) \
    .withColumn("grp", expr("activity_date - INTERVAL rn DAYS")) \
    .groupBy("user_id", "grp") \
    .agg(count("*").alias("days_active")) \
    .filter(col("days_active") >= 7) \
    .select("user_id").distinct()

📌 Question 34: Calculate Repeat Purchase Rate 
Table: orders customer_id, order_id

%sql
SELECT 
 ROUND(
 100.0 * 
 COUNT(CASE WHEN order_count > 1 THEN 1 END) / COUNT(*),  2) AS repeat_purchase_rate FROM (SELECT customer_id, COUNT(*) AS order_count FROM orders GROUP BY customer_id) t;

%python
customer_orders = orders.groupBy("customer_id").agg(count("*").alias("order_count"))
total_customers = customer_orders.count()

repeat_customers = customer_orders.filter(col("order_count") > 1).count()

result_q34 = spark.createDataFrame([(
    round(100.0 * repeat_customers / total_customers, 2))], ["repeat_purchase_rate"])

# Alternative using SQL expression:
result_q34_alt = customer_orders.select(
    round(100.0 * count(when(col("order_count") > 1, 1)) / count("*"), 2).alias("repeat_purchase_rate"))

📌 Question 35: Find the Longest Gap Between Two Orders 
Table: orders customer_id, order_date

%sql
WITH gaps AS (
 SELECT 
 customer_id,
 order_date,
 order_date - LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS gap_days FROM orders)
SELECT customer_id, MAX(gap_days) AS longest_gap FROM gaps GROUP BY customer_id;

%python
window_spec = Window.partitionBy("customer_id").orderBy("order_date")
gaps = orders.withColumn("gap_days", 
    datediff("order_date", lag("order_date").over(window_spec)))

result_q35 = gaps.filter(col("gap_days").isNotNull()) \
    .groupBy("customer_id") \
    .agg(max("gap_days").alias("longest_gap"))