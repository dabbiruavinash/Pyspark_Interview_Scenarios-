# Find duplicate records

%sql
SELECT column1, column2, COUNT(*) FROM your_table GROUP BY column1, column2 HAVING COUNT(*) > 1;

%python
result = df.groupBy("column1", "column2").agg(F.count("*").alias("count")).filter(F.col("count") > 1)

# Second highest salary

%sql
SELECT MAX(salary) AS SecondHighestSalary FROM Employee WHERE salary < (SELECT MAX(salary) FROM Employee);

%python
max_salary = employee.agg(F.max("salary")).first()[0]
result = employee.filter(F.col("salary") < max_salary).agg(F.max("salary").alias("SecondHighestSalary"))

# Employees without department

%sql
SELECT e.* FROM Employee e LEFT JOIN Department d ON e.department_id = d.department_id WHERE d.department_id IS NULL;

%python
result = employee.join(department, "department_id", "left_anti")

# Total revenue per product

%sql
SELECT product_id, SUM(quantity * price) AS total_revenue FROM Sales GROUP BY product_id;

%python
result = sales.groupBy("product_id").agg(F.sum(F.col("quantity") * F.col("price")).alias("total_revenue"))

# Top 3 highest-paid employees

%sql
SELECT * FROM Employee ORDER BY salary DESC FETCH FIRST 3 ROWS ONLY;

%python
result = employee.orderBy(F.desc("salary")).limit(3)

# Customers who purchased but never returned

%sql
SELECT DISTINCT c.customer_id FROM Customers c JOIN Orders o ON c.customer_id = o.customer_id WHERE c.customer_id NOT IN (SELECT customer_id FROM Returns);

%python
customers_with_orders = customers.join(orders, "customer_id")
result = customers_with_orders.join(returns, "customer_id", "left_anti").select("customer_id").distinct()

#  Count of orders per customer

%sql
SELECT customer_id, COUNT(*) AS order_count FROM Orders GROUP BY customer_id;

%python
result = orders.groupBy("customer_id").agg(F.count("*").alias("order_count"))

# Employees hired in 2023

%sql
SELECT * FROM Employee WHERE EXTRACT(YEAR FROM hire_date) = 2023;

%python
result = employee.filter(F.year("hire_date") == 2023)

# Average order value per customer

%sql
SELECT customer_id, AVG(total_amount) AS avg_order_value FROM Orders GROUP BY customer_id;

%python
result = orders.groupBy("customer_id").agg(F.avg("total_amount").alias("avg_order_value"))

# Latest order per customer

%sql
SELECT customer_id, MAX(order_date) AS latest_order_date FROM Orders GROUP BY customer_id;

%python
result = orders.groupBy("customer_id").agg(F.max("order_date").alias("latest_order_date"))

# Products never sold

%sql
SELECT p.product_id FROM Products p LEFT JOIN Sales s ON p.product_id = s.product_id WHERE s.product_id IS NULL;

%python
result = products.join(sales, "product_id", "left_anti")

# Most selling product

%sql
SELECT product_id, SUM(quantity) AS total_qty FROM Sales GROUP BY product_id ORDER BY total_qty DESC FETCH FIRST 1 ROW ONLY;

%python
result = sales.groupBy("product_id").agg(F.sum("quantity").alias("total_qty")).orderBy(F.desc("total_qty")).limit(1)

# Revenue and orders per region

%sql
SELECT region, SUM(total_amount) AS total_revenue, COUNT(*) AS order_count FROM Orders GROUP BY region;

%python
result = orders.groupBy("region").agg(F.sum("total_amount").alias("total_revenue"), F.count("*").alias("order_count"))

# Customers with >5 orders

%sql
SELECT COUNT(*) AS customer_count 
FROM (SELECT customer_id FROM Orders GROUP BY customer_id HAVING COUNT(*) > 5);

%python
customer_orders = orders.groupBy("customer_id").agg(F.count("*").alias("order_count"))
result = customer_orders.filter(F.col("order_count") > 5).agg(F.count("*").alias("customer_count"))

# Orders above average value

%sql
SELECT * FROM Orders 
WHERE total_amount > (SELECT AVG(total_amount) FROM Orders);

%python
avg_amount = orders.agg(F.avg("total_amount")).first()[0]
result = orders.filter(F.col("total_amount") > avg_amount)

# Employees hired on weekends

%sql
SELECT * FROM Employee WHERE TO_CHAR(hire_date, 'DY') IN ('SAT', 'SUN');

%python
result = employee.filter(F.dayofweek("hire_date").isin([1, 7]))  # 1=Sunday, 7=Saturday

# Employees with salary between 50k-100k

%sql
SELECT * FROM Employee WHERE salary BETWEEN 50000 AND 100000;

%python
result = employee.filter(F.col("salary").between(50000, 100000))

# Monthly sales revenue

%sql
SELECT TO_CHAR(date, 'YYYY-MM') AS month, SUM(amount) AS total_revenue, COUNT(order_id) AS order_count FROM Orders GROUP BY TO_CHAR(date, 'YYYY-MM');

%python
result = (orders.withColumn("month", F.date_format("date", "yyyy-MM"))
             .groupBy("month")
             .agg(F.sum("amount").alias("total_revenue"), F.count("order_id").alias("order_count")))

# Rank employees by salary

%sql
SELECT employee_id, department_id, salary, RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) AS salary_rk FROM Employee;

%python
from pyspark.sql.window import Window

window_spec = Window.partitionBy("department_id").orderBy(F.desc("salary"))
result = employee.withColumn("salary_rk", F.rank().over(window_spec))

# Customers ordering every month in 2023

%sql
SELECT customer_id FROM Orders WHERE EXTRACT(YEAR FROM order_date) = 2023 GROUP BY customer_id HAVING COUNT(DISTINCT EXTRACT(MONTH FROM order_date)) = 12;

%python
result = (orders.filter(F.year("order_date") == 2023)
          .groupBy("customer_id")
          .agg(F.countDistinct(F.month("order_date")).alias("month_count"))
          .filter(F.col("month_count") == 12)
          .select("customer_id"))

# 3-day moving average

%sql
SELECT order_date,AVG(total_amount) OVER (ORDER BY order_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg FROM Orders;

%python
window_spec = Window.orderBy("order_date").rowsBetween(-2, 0)
result = orders.withColumn("moving_avg", F.avg("total_amount").over(window_spec))

# First and last order dates

%sql
SELECT customer_id, MIN(order_date) AS first_order, MAX(order_date) AS last_order FROM Orders GROUP BY customer_id;

%python
result = orders.groupBy("customer_id").agg(F.min("order_date").alias("first_order"), F.max("order_date").alias("last_order"))

# Product sales distribution

%sql
WITH TotalRevenue AS (SELECT SUM(quantity * price) AS total FROM Sales)
SELECT s.product_id, SUM(s.quantity * s.price) AS revenue, SUM(s.quantity * s.price) * 100 / t.total AS revenue_pct FROM Sales s CROSS JOIN TotalRevenue t GROUP BY s.product_id, t.total;

%python
total_revenue = sales.agg(F.sum(F.col("quantity") * F.col("price")).alias("total")).first()[0]
result = (sales.groupBy("product_id")
              .agg(F.sum(F.col("quantity") * F.col("price")).alias("revenue"))
              .withColumn("revenue_pct", F.col("revenue") * 100 / total_revenue))

# Consecutive purchases

%sql
WITH cte AS (SELECT id, order_date,LAG(order_date) OVER (PARTITION BY id ORDER BY order_date) AS prev_order_date FROM Orders)
SELECT id, order_date, prev_order_date FROM cte WHERE order_date = prev_order_date + 1;

%python
window_spec = Window.partitionBy("id").orderBy("order_date")
result = (orders.withColumn("prev_order_date", F.lag("order_date").over(window_spec))
          .filter(F.datediff("order_date", "prev_order_date") == 1))

# Churned customers

%sql
SELECT customer_id FROM Orders GROUP BY customer_id HAVING MAX(order_date) < ADD_MONTHS(SYSDATE, -6);

%python
six_months_ago = F.current_date() - F.expr("INTERVAL 6 MONTHS")
result = orders.groupBy("customer_id").agg(F.max("order_date").alias("last_order"))
               .filter(F.col("last_order") < six_months_ago)

# Cumulative revenue

%sql
SELECT order_date,SUM(total_amount) OVER (ORDER BY order_date) AS cumulative_revenue FROM Orders;

%python
window_spec = Window.orderBy("order_date")
result = orders.withColumn("cumulative_revenue", F.sum("total_amount").over(window_spec))

# Top departments by average salary

%sql
SELECT department_id, AVG(salary) AS avg_salary FROM Employee GROUP BY department_id ORDER BY avg_salary DESC;

%python
result = employee.groupBy("department_id").agg(F.avg("salary").alias("avg_salary")).orderBy(F.desc("avg_salary"))

# Customers with above-average orders

%sql
WITH customer_orders AS (
    SELECT customer_id, COUNT(*) AS order_count 
    FROM Orders 
    GROUP BY customer_id)
SELECT * FROM customer_orders WHERE order_count > (SELECT AVG(order_count) FROM customer_orders);

%python
customer_orders = orders.groupBy("customer_id").agg(F.count("*").alias("order_count"))
avg_orders = customer_orders.agg(F.avg("order_count")).first()[0]
result = customer_orders.filter(F.col("order_count") > avg_orders)

# Revenue from first-time orders

%sql
WITH first_orders AS (
    SELECT customer_id, MIN(order_date) AS first_order_date 
    FROM Orders 
    GROUP BY customer_id)
SELECT SUM(o.total_amount) AS new_revenue 
FROM Orders o JOIN first_orders f ON o.customer_id = f.customer_id 
WHERE o.order_date = f.first_order_date;

%python
first_orders = orders.groupBy("customer_id").agg(F.min("order_date").alias("first_order_date"))
result = (orders.join(first_orders, ["customer_id"])
          .filter(F.col("order_date") == F.col("first_order_date"))
          .agg(F.sum("total_amount").alias("new_revenue")))

# Percentage of employees per department

%sql
SELECT department_id, COUNT(*) AS emp_count,COUNT(*) * 100.0 / (SELECT COUNT(*) FROM Employee) AS pct FROM Employee GROUP BY department_id;

%python
total_employees = employee.count()
result = (employee.groupBy("department_id")
          .agg(F.count("*").alias("emp_count"))
          .withColumn("pct", F.col("emp_count") * 100 / total_employees))

# Max salary difference per department

%sql
SELECT department_id, MAX(salary) - MIN(salary) AS salary_diff  FROM Employee GROUP BY department_id;

%python
result = employee.groupBy("department_id").agg((F.max("salary") - F.min("salary")).alias("salary_diff"))

# Pareto principle (80% revenue)

%sql
WITH sales_cte AS (
    SELECT product_id, SUM(quantity * price) AS revenue 
    FROM Sales 
    GROUP BY product_id),
total_revenue AS (SELECT SUM(revenue) AS total FROM sales_cte)
SELECT s.product_id, s.revenue
FROM sales_cte s, total_revenue t
WHERE (SELECT SUM(revenue) FROM sales_cte s2 WHERE s2.revenue >= s.revenue) <= t.total * 0.8;

%python
sales_cte = sales.groupBy("product_id").agg(F.sum(F.col("quantity") * F.col("price")).alias("revenue"))
total_revenue = sales_cte.agg(F.sum("revenue")).first()[0]

window_spec = Window.orderBy(F.desc("revenue"))
result = (sales_cte.withColumn("running_total", F.sum("revenue").over(window_spec))
          .filter(F.col("running_total") <= total_revenue * 0.8))

# Average time between purchases

%sql
WITH cte AS (
SELECT customer_id, order_date, LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS prev_date FROM Orders)
SELECT customer_id, AVG(order_date - prev_date) AS avg_gap_days FROM cte WHERE prev_date IS NOT NULL GROUP BY customer_id;

%python
window_spec = Window.partitionBy("customer_id").orderBy("order_date")
result = (orders.withColumn("prev_date", F.lag("order_date").over(window_spec))
          .filter(F.col("prev_date").isNotNull())
          .withColumn("gap_days", F.datediff("order_date", "prev_date"))
          .groupBy("customer_id")
          .agg(F.avg("gap_days").alias("avg_gap_days")))

# Last purchase per customer

%sql
WITH ranked_orders AS (
SELECT customer_id, order_id, total_amount,ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn FROM Orders)
SELECT customer_id, order_id, total_amount FROM ranked_orders WHERE rn = 1;

%python
window_spec = Window.partitionBy("customer_id").orderBy(F.desc("order_date"))
result = (orders.withColumn("rn", F.row_number().over(window_spec))
          .filter(F.col("rn") == 1)
          .select("customer_id", "order_id", "total_amount"))

# Year-over-year growth

%sql
SELECT EXTRACT(YEAR FROM order_date) AS year,
SUM(total_amount) AS revenue,
SUM(total_amount) - LAG(SUM(total_amount)) OVER (ORDER BY EXTRACT(YEAR FROM order_date)) AS yoy_growth 
FROM Orders GROUP BY EXTRACT(YEAR FROM order_date);

%python
yearly_revenue = (orders.withColumn("year", F.year("order_date"))
                  .groupBy("year")
                  .agg(F.sum("total_amount").alias("revenue")))

window_spec = Window.orderBy("year")
result = yearly_revenue.withColumn("yoy_growth", F.col("revenue") - F.lag("revenue").over(window_spec))

# Purchases above 90th percentile

%sql
WITH ranked_orders AS (
SELECT customer_id, order_id, total_amount,NTILE(10) OVER (PARTITION BY customer_id ORDER BY total_amount) AS decile FROM Orders)
SELECT customer_id, order_id, total_amount FROM ranked_orders WHERE decile = 10;

%python
window_spec = Window.partitionBy("customer_id").orderBy("total_amount")
result = orders.withColumn("decile", F.ntile(10).over(window_spec)).filter(F.col("decile") == 10)

# Longest gap between orders

%sql
WITH cte AS (
SELECT customer_id, order_date,LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS prev_order_date FROM Orders)
SELECT customer_id, MAX(order_date - prev_order_date) AS max_gap FROM cte  WHERE prev_order_date IS NOT NULL GROUP BY customer_id;

%python
window_spec = Window.partitionBy("customer_id").orderBy("order_date")
result = (orders.withColumn("prev_order_date", F.lag("order_date").over(window_spec))
          .filter(F.col("prev_order_date").isNotNull())
          .withColumn("gap_days", F.datediff("order_date", "prev_order_date"))
          .groupBy("customer_id")
          .agg(F.max("gap_days").alias("max_gap")))

# Customers below 10th percentile revenue

%sql
WITH cte AS (
    SELECT customer_id, SUM(total_amount) AS total_revenue 
    FROM Orders 
    GROUP BY customer_id)
SELECT customer_id, total_revenue FROM cte WHERE total_revenue < (SELECT PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY total_revenue) FROM cte);

%python
customer_revenue = orders.groupBy("customer_id").agg(F.sum("total_amount").alias("total_revenue"))
percentile_10 = customer_revenue.approxQuantile("total_revenue", [0.1], 0.01)[0]
result = customer_revenue.filter(F.col("total_revenue") < percentile_10)

# Products sold together

%sql
SELECT A.product_id AS product_A, B.product_id AS product_B, COUNT(*) AS count_together FROM Order_Details A 
JOIN Order_Details B ON A.order_id = B.order_id AND A.product_id < B.product_id 
GROUP BY A.product_id, B.product_id HAVING COUNT(*) > 10;

%python
order_details_a = order_details.alias("A")
order_details_b = order_details.alias("B")

result = (order_details_a.join(order_details_b, 
             (F.col("A.order_id") == F.col("B.order_id")) & 
             (F.col("A.product_id") < F.col("B.product_id")))
             .groupBy("A.product_id", "B.product_id")
             .agg(F.count("*").alias("count_together"))
             .filter(F.col("count_together") > 10))

# Gini coefficient

%sql
WITH income_cte AS (
SELECT salary, SUM(salary) OVER (ORDER BY salary) AS cum_income, COUNT(*) OVER() AS n, ROW_NUMBER() OVER (ORDER BY salary) AS r FROM Employee)
SELECT 1 - (2 * SUM((cum_income) / (SUM(salary) OVER ()) * (1.0 / n))) AS gini_coefficient FROM income_cte;

%python
window_spec = Window.orderBy("salary")
income_cte = (employee.withColumn("cum_income", F.sum("salary").over(window_spec))
              .withColumn("n", F.count("*").over(Window.partitionBy()))
              .withColumn("r", F.row_number().over(window_spec)))

total_income = income_cte.agg(F.sum("salary")).first()[0]
result = income_cte.agg(F.expr("1 - (2 * SUM(cum_income / total_income * (1.0 / n)))").alias("gini_coefficient"))

# Median sales day

%sql
WITH cte AS (
    SELECT order_date, SUM(total_amount) AS daily_rev 
    FROM Orders 
    GROUP BY order_date),
cum_cte AS (
SELECT order_date, daily_rev, 
SUM(daily_rev) OVER (ORDER BY order_date) AS cum_rev,
SUM(daily_rev) OVER() AS total_rev FROM cte)
SELECT order_date FROM cum_cte WHERE cum_rev >= total_rev / 2 ORDER BY order_date FETCH FIRST 1 ROW ONLY;

%python
daily_rev = orders.groupBy("order_date").agg(F.sum("total_amount").alias("daily_rev"))
total_rev = daily_rev.agg(F.sum("daily_rev")).first()[0]

window_spec = Window.orderBy("order_date")
result = (daily_rev.withColumn("cum_rev", F.sum("daily_rev").over(window_spec))
          .filter(F.col("cum_rev") >= total_rev / 2)
          .orderBy("order_date")
          .limit(1)
          .select("order_date"))

# Salary percentiles

%sql
SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary) AS p25,
       PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY salary) AS p50,
       PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salary) AS p75 FROM Employee;

%python
result = employee.select(
    F.expr("percentile_approx(salary, 0.25)").alias("p25"),
    F.expr("percentile_approx(salary, 0.50)").alias("p50"),
    F.expr("percentile_approx(salary, 0.75)").alias("p75"))

# Increasing order amounts

%sql
WITH cte AS (
SELECT customer_id, order_date, total_amount,
LAG(total_amount, 2) OVER (PARTITION BY customer_id ORDER BY order_date) AS amt_t_minus_2,
LAG(total_amount, 1) OVER (PARTITION BY customer_id ORDER BY order_date) AS amt_t_minus_1 FROM Orders)
SELECT customer_id, order_date, total_amount FROM cte WHERE amt_t_minus_2 < amt_t_minus_1 AND amt_t_minus_1 < total_amount;

%python
window_spec = Window.partitionBy("customer_id").orderBy("order_date")
result = (orders.withColumn("amt_t_minus_2", F.lag("total_amount", 2).over(window_spec))
          .withColumn("amt_t_minus_1", F.lag("total_amount", 1).over(window_spec))
          .filter((F.col("amt_t_minus_2") < F.col("amt_t_minus_1")) & 
                 (F.col("amt_t_minus_1") < F.col("total_amount"))))

# Conversion funnel

%sql
SELECT SUM(CASE WHEN stage = 'visit' THEN 1 ELSE 0 END) AS visits,
       SUM(CASE WHEN stage = 'sign_up' THEN 1 ELSE 0 END) AS sign_ups,
       SUM(CASE WHEN stage = 'purchase' THEN 1 ELSE 0 END) AS purchases FROM Funnel;

%python
result = funnel.agg(
    F.sum(F.when(F.col("stage") == "visit", 1).otherwise(0)).alias("visits"),
    F.sum(F.when(F.col("stage") == "sign_up", 1).otherwise(0)).alias("sign_ups"),
    F.sum(F.when(F.col("stage") == "purchase", 1).otherwise(0)).alias("purchases"))

# Top 10% customers revenue

%sql
WITH cte AS (
    SELECT customer_id, SUM(total_amount) AS revenue 
    FROM Orders 
    GROUP BY customer_id),
ranked AS (
SELECT *, NTILE(10) OVER (ORDER BY revenue DESC) AS decile FROM cte)
SELECT SUM(revenue) * 100.0 / (SELECT SUM(revenue) FROM cte) AS pct_top_10 FROM ranked WHERE decile = 1;

%python
customer_revenue = orders.groupBy("customer_id").agg(F.sum("total_amount").alias("revenue"))
total_revenue = customer_revenue.agg(F.sum("revenue")).first()[0]

window_spec = Window.orderBy(F.desc("revenue"))
result = (customer_revenue.withColumn("decile", F.ntile(10).over(window_spec))
          .filter(F.col("decile") == 1)
          .agg((F.sum("revenue") * 100 / total_revenue).alias("pct_top_10")))

# Weekly active users

%sql
SELECT EXTRACT(YEAR FROM login_date) AS year, EXTRACT(WEEK FROM login_date) AS week,COUNT(DISTINCT user_id) AS wau 
FROM Logins GROUP BY EXTRACT(YEAR FROM login_date), EXTRACT(WEEK FROM login_date);

%python
result = (logins.withColumn("year", F.year("login_date"))
          .withColumn("week", F.weekofyear("login_date"))
          .groupBy("year", "week")
          .agg(F.countDistinct("user_id").alias("wau")))

# Employees above department average

%sql
WITH dept_avg AS (
    SELECT department_id, AVG(salary) AS avg_salary 
    FROM Employee 
    GROUP BY department_id)
SELECT e.* 
FROM Employee e JOIN dept_avg d ON e.department_id = d.department_id 
WHERE e.salary > d.avg_salary;

%python
dept_avg = employee.groupBy("department_id").agg(F.avg("salary").alias("avg_salary"))
result = employee.join(dept_avg, "department_id").filter(F.col("salary") > F.col("avg_salary"))

# Time to first purchase

%sql
WITH first_purchase AS (
    SELECT user_id, MIN(purchase_date) AS first_purchase_date 
    FROM Purchases 
    GROUP BY user_id)
SELECT u.user_id, u.signup_date - f.first_purchase_date AS days_to_purchase 
FROM Users u JOIN first_purchase f ON u.user_id = f.user_id;

%python
first_purchase = purchases.groupBy("user_id").agg(F.min("purchase_date").alias("first_purchase_date"))
result = users.join(first_purchase, "user_id").withColumn("days_to_purchase", F.datediff("first_purchase_date", "signup_date"))

# Longest gap between orders

%sql
WITH cte AS (
SELECT customer_id, order_date,LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS prev_order_date FROM Orders)
SELECT customer_id, MAX(order_date - prev_order_date) AS max_gap FROM cte WHERE prev_order_date IS NOT NULL GROUP BY customer_id;

%python
window_spec = Window.partitionBy("customer_id").orderBy("order_date")
result = (orders.withColumn("prev_order_date", F.lag("order_date").over(window_spec))
          .filter(F.col("prev_order_date").isNotNull())
          .withColumn("gap_days", F.datediff("order_date", "prev_order_date"))
          .groupBy("customer_id")
          .agg(F.max("gap_days").alias("max_gap")))

# Customers below 10th percentile revenue

%sql
WITH cte AS (
    SELECT customer_id, SUM(total_amount) AS total_revenue 
    FROM Orders 
    GROUP BY customer_id)
SELECT customer_id, total_revenue FROM cte WHERE total_revenue < (SELECT PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY total_revenue) FROM cte);

%python
customer_revenue = orders.groupBy("customer_id").agg(F.sum("total_amount").alias("total_revenue"))
percentile_10 = customer_revenue.approxQuantile("total_revenue", [0.1], 0.01)[0]
result = customer_revenue.filter(F.col("total_revenue") < percentile_10)