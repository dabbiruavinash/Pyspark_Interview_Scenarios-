Find the job titles of workers who earn the highest salary.
If multiple workers have the same highest salary, include all their job titles

worker - worker_id, first_name, last_name, salary, joining_date, department 
title - worker_ref_id, worker_title, affected_from 

%sql
SELECT w.worker_title FROM title t
JOIN worker w ON t.worker_ref_id = w.worker_id WHERE w.salary = (SELECT MAX(salary) FROM worker);

%python
from pyspark.sql import functions as F

# Find max salary
max_salary = worker.agg(F.max("salary")).collect()[0][0]

# Join and filter
result = title.join(worker, title.worker_ref_id == worker.worker_id) \
    .filter(worker.salary == max_salary) \
    .select("worker_title")

result.show()
---
Find the details of each customer regardless of whether the customer made an order.
Return the first name, last name, city, and order details.

customer - id, first_name, last_name, city
orders - id, cust_id, order_details

%sql
SELECT c.first_name, c.last_name, c.city, o.order_details FROM customer c
LEFT JOIN orders o ON c.id = o.cust_id;

%python
result = customer.join(orders, customer.id == orders.cust_id, how="left") \
    .select("first_name", "last_name", "city", "order_details")

result.show()

---
Find the order details of purchases made by Jill and Eva.

customers - first_name, last_name
orders - id, cust_id, order_date, order_details, total_order_cost

%sql
SELECT o.order_details FROM orders o
JOIN customers c ON o.cust_id = c.id WHERE c.first_name IN ('Jill', 'Eva');

%python
result = orders.join(customers, orders.cust_id == customers.id) \
    .filter(customers.first_name.isin(["Jill", "Eva"])) \
    .select("order_details")

result.show()
---

Write a query to calculate the number of shipments per month

amazon_shipment - shipment_id, sub_id, weight, shipment_date

%sql
SELECT 
    TO_CHAR(shipment_date, 'YYYY-MM') AS month,
    COUNT(*) AS number_of_shipments FROM amazon_shipment GROUP BY TO_CHAR(shipment_date, 'YYYY-MM') ORDER BY month;

%python
from pyspark.sql import functions as F

result = amazon_shipment.withColumn("month", F.date_format("shipment_date", "yyyy-MM")) \
    .groupBy("month") \
    .agg(F.count("*").alias("number_of_shipments")) \
    .orderBy("month")

result.show()
---
Find the total cost of orders for each customer.

customers - id, first_name, last_name, city
orders - id, cust_id, order_date, order_details, total_order_cost

%sql
SELECT 
    c.first_name, 
    c.last_name, 
    COALESCE(SUM(o.total_order_cost), 0) AS total_cost FROM customers c
LEFT JOIN orders o ON c.id = o.cust_id
GROUP BY c.id, c.first_name, c.last_name;

%python
from pyspark.sql import functions as F

result = customers.join(orders, customers.id == orders.cust_id, how="left") \
    .groupBy("id", "first_name", "last_name") \
    .agg(F.coalesce(F.sum("total_order_cost"), F.lit(0)).alias("total_cost")) \
    .select("first_name", "last_name", "total_cost")

result.show()
---
Find the number of workers by department who joined on or after April 1, 2014

worker - worker_id, first_name, last_name, salary, joining_date, department

%sql
SELECT 
    department,
    COUNT(*) AS number_of_workers FROM worker WHERE joining_date >= DATE '2014-04-01' GROUP BY department ORDER BY department;

%python
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

result = worker.filter(F.col("joining_date") >= F.lit("2014-04-01")) \
    .groupBy("department") \
    .agg(F.count("*").alias("number_of_workers")) \
    .orderBy("department")

result.show()
---
Find the number of employees working in the Admin department.

worker - worker_id, first_name, last_name, salary, joining_date, department

%sql
SELECT COUNT(*) AS admin_employees_count FROM worker
WHERE department = 'Admin';

%python
result = worker.filter(worker.department == "Admin") \
    .agg(F.count("*").alias("admin_employees_count"))

result.show()
---
Write a query to find products that have not had any sales.

product - product_id, market_name
sales - product_id, units_sold, cost_in_dollars, date

%sql
SELECT p.product_id, p.market_name FROM product p
LEFT JOIN sales s ON p.product_id = s.product_id
WHERE s.product_id IS NULL;

%python
result = product.join(sales, product.product_id == sales.product_id, how="left") \
    .filter(sales.product_id.isNull()) \
    .select("product_id", "market_name")

result.show()
---
Find the lowest order cost of each customer.

customers - id, first_name, last_name, city
orders - id, cust_id, order_date, order_details, total_order_cost

%sql
SELECT 
    c.id,
    c.first_name,
    c.last_name,
    MIN(o.total_order_cost) AS lowest_order_cost FROM customers c
LEFT JOIN orders o ON c.id = o.cust_id
GROUP BY c.id, c.first_name, c.last_name;

%python
from pyspark.sql import functions as F

result = customers.join(orders, customers.id == orders.cust_id, how="left") \
    .groupBy("id", "first_name", "last_name") \
    .agg(F.min("total_order_cost").alias("lowest_order_cost"))

result.show()
---
For each week, find the total number of orders.

order_analysis - stage_of_order, week, quantity

%sql
SELECT 
    week,
    SUM(quantity) AS total_orders FROM order_analysis GROUP BY week ORDER BY week;

%python
from pyspark.sql import functions as F

result = order_analysis.groupBy("week") \
    .agg(F.sum("quantity").alias("total_orders")) \
    .orderBy("week")

result.show()
---
You are given a dataset containing sales information across different products and marketplaces.
Find the top 3 sellers in each product category for January.

sales_date - sellar_id, total_sales, product_category, market_place, month

%sql
SELECT * FROM (
    SELECT 
        sellar_id,
        product_category,
        total_sales, ROW_NUMBER() OVER (PARTITION BY product_category ORDER BY total_sales DESC) AS rank FROM sales_date WHERE month = 'January') WHERE rank <= 3;

%python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

window_spec = Window.partitionBy("product_category").orderBy(F.desc("total_sales"))

result = sales_date.filter(F.col("month") == "January") \
    .withColumn("rank", F.row_number().over(window_spec)) \
    .filter(F.col("rank") <= 3) \
    .drop("rank")

result.show()
---
Write a query to find the weight for each shipment's earliest shipment date.

amazon_shipment - shipment_id, sub_id, weight, shipment_date

%sql
SELECT 
    shipment_id,
    weight,
    MIN(shipment_date) AS earliest_shipment_date FROM amazon_shipment GROUP BY shipment_id, weight ORDER BY shipment_id;

%python
from pyspark.sql import functions as F

result = amazon_shipment.groupBy("shipment_id", "weight") \
    .agg(F.min("shipment_date").alias("earliest_shipment_date")) \
    .orderBy("shipment_id")

result.show()
---
Write a query to find all customer IDs (cust_id) that are violating the primary key constraint in the table.
A customer is violating the constraint if the same cust_id appears more than once.

dim_customer - cust_id, customer_name, city, dob, pincode

%sql
SELECT cust_id, COUNT(*) AS duplicate_count FROM dim_customer GROUP BY cust_id HAVING COUNT(*) > 1;

%python
from pyspark.sql import functions as F

result = dim_customer.groupBy("cust_id") \
    .agg(F.count("*").alias("duplicate_count")) \
    .filter(F.col("duplicate_count") > 1)

result.show()
---
Determine the total sales revenue generated by Samantha and Lisa.

sales_performance - salesperson, widget_sales, sales_revenue, id

%sql
SELECT 
    salesperson,
    SUM(sales_revenue) AS total_revenue FROM sales_performance WHERE salesperson IN ('Samantha', 'Lisa') GROUP BY salesperson;

%python
from pyspark.sql import functions as F

result = sales_performance.filter(F.col("salesperson").isin(["Samantha", "Lisa"])) \
    .groupBy("salesperson") \
    .agg(F.sum("sales_revenue").alias("total_revenue"))

result.show()
---
Find the time period during which the maximum number of users were online simultaneously.

user_session - user_id, session_start, session_end

%sql
WITH time_points AS (
    SELECT session_start AS time_point, 1 AS change FROM user_session
    UNION ALL
    SELECT session_end, -1 FROM user_session
),
cumulative_users AS (
    SELECT 
        time_point,
        SUM(change) OVER (ORDER BY time_point) AS concurrent_users
    FROM time_points
)
SELECT 
    time_point AS period_start,
    LEAD(time_point) OVER (ORDER BY time_point) AS period_end,
    concurrent_users
FROM cumulative_users
ORDER BY concurrent_users DESC
FETCH FIRST 1 ROW ONLY;

%python
from pyspark.sql import functions as F

# Alternative approach - create time intervals
user_session = user_session.withColumn("time_range", F.explode(
    F.sequence(F.col("session_start"), F.col("session_end"))
))

result = user_session.groupBy("time_range") \
    .agg(F.countDistinct("user_id").alias("concurrent_users")) \
    .orderBy(F.desc("concurrent_users")) \
    .limit(1)

result.show()
---
Calculate the total weight for each shipment and add it as a new column

amazon_shipment - shipment_id, sub_id, weight, shipment_date

%sql
SELECT 
    shipment_id,
    sub_id,
    weight,
    shipment_date,
    SUM(weight) OVER (PARTITION BY shipment_id) AS total_shipment_weight FROM amazon_shipment;

%python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

window_spec = Window.partitionBy("shipment_id")

result = amazon_shipment.withColumn(
    "total_shipment_weight", 
    F.sum("weight").over(window_spec))

result.show()
---
Find all workers whose worker_id is an even number.

worker - worker_id, first_name, last_name, salary, joining_date, department

%sql
SELECT * FROM worker WHERE MOD(worker_id, 2) = 0;

%python
from pyspark.sql import functions as F

result = worker.filter(F.col("worker_id") % 2 == 0)
result.show()
---
Find the latest login date for each user.

user_logins - user_id, login_date

%sql
SELECT 
    user_id,
    MAX(login_date) AS latest_login_date FROM user_logins GROUP BY user_id ORDER BY user_id;

%python
from pyspark.sql import functions as F

result = user_logins.groupBy("user_id") \
    .agg(F.max("login_date").alias("latest_login_date")) \
    .orderBy("user_id")

result.show()
---
Find the most recent login details for each employee.
Return all columns related to the employee's latest login.

worker_logins - worker_id, id, login_timestamp, ip_address, country, region, city, device_type

%sql
WITH ranked_logins AS (
    SELECT 
        wl.*,
        ROW_NUMBER() OVER (PARTITION BY wl.worker_id ORDER BY wl.login_timestamp DESC) AS rn
    FROM worker_logins wl)
SELECT worker_id, id, login_timestamp, ip_address, country, region, city, device_type FROM ranked_logins WHERE rn = 1;

%python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

window_spec = Window.partitionBy("worker_id").orderBy(F.desc("login_timestamp"))

result = worker_logins.withColumn("rn", F.row_number().over(window_spec)) \
    .filter(F.col("rn") == 1) \
    .drop("rn")

result.show()
---
Find the third heaviest package (weight) for each shipment.

amazon_shipment - shipment_id, sub_id, weight, shipment_date

%sql
WITH ranked_packages AS (
    SELECT 
        shipment_id,
        sub_id,
        weight,
        shipment_date,
        DENSE_RANK() OVER (PARTITION BY shipment_id ORDER BY weight DESC) AS weight_rank FROM amazon_shipment)
SELECT shipment_id, sub_id, weight, shipment_date FROM ranked_packages WHERE weight_rank = 3;

%python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

window_spec = Window.partitionBy("shipment_id").orderBy(F.desc("weight"))

result = amazon_shipment.withColumn("weight_rank", F.dense_rank().over(window_spec)) \
    .filter(F.col("weight_rank") == 3) \
    .drop("weight_rank")

result.show()
---
Identify returning active users.
A returning active user is defined as a user who made a second purchase within 1 to 7 days after their first purchase.

amazon_transactions - id, user, item, created_at, revenue

%sql
WITH first_purchase AS (
    SELECT 
        user,
        MIN(created_at) AS first_purchase_date FROM amazon_transactions GROUP BY user)
SELECT DISTINCT at.user
FROM amazon_transactions at
JOIN first_purchase fp ON at.user = fp.user
WHERE at.created_at > fp.first_purchase_date
    AND at.created_at <= fp.first_purchase_date + 7
    AND at.created_at > fp.first_purchase_date;  -- Ensures second purchase

%python
from pyspark.sql import functions as F

first_purchase = amazon_transactions.groupBy("user") \
    .agg(F.min("created_at").alias("first_purchase_date"))

result = amazon_transactions.join(first_purchase, "user") \
    .filter(
        (F.col("created_at") > F.col("first_purchase_date")) &
        (F.col("created_at") <= F.date_add(F.col("first_purchase_date"), 7)))
    .select("user")
    .distinct()

result.show()
---
Find the customer with the highest total order cost on a single day between 2019-02-01 and 2019-05-01.
If a customer placed multiple orders on the same day, sum their order costs for that day.

customer - id, first_name, last_name, city, address, phone_number
orders - id, cust_id, order_date, order_details

%sql
WITH daily_customer_costs AS (
    SELECT 
        o.cust_id,
        c.first_name,
        c.last_name,
        o.order_date,
        SUM(o.total_order_cost) AS daily_total FROM orders o JOIN customer c ON o.cust_id = c.id WHERE o.order_date BETWEEN DATE '2019-02-01' AND DATE '2019-05-01' GROUP BY o.cust_id, c.first_name, c.last_name, o.order_date)
SELECT first_name, last_name, order_date, daily_total FROM daily_customer_costs WHERE daily_total = (SELECT MAX(daily_total) FROM daily_customer_costs);

%python
from pyspark.sql import functions as F

daily_customer_costs = orders.join(customer, orders.cust_id == customer.id) \
    .filter(
        (F.col("order_date") >= F.lit("2019-02-01")) & 
        (F.col("order_date") <= F.lit("2019-05-01"))) \
    .groupBy("cust_id", "first_name", "last_name", "order_date") \
    .agg(F.sum("total_order_cost").alias("daily_total"))

max_daily_total = daily_customer_costs.agg(F.max("daily_total")).collect()[0][0]

result = daily_customer_costs.filter(F.col("daily_total") == max_daily_total) \
    .select("first_name", "last_name", "order_date", "daily_total")

result.show()
---
Calculate the total revenue from each customer in March 2019.

orders - id, cust_id, order_date, order_details, total_order_cost

%sql
SELECT 
    o.cust_id,
    SUM(o.total_order_cost) AS total_revenue FROM orders o
WHERE o.order_date BETWEEN DATE '2019-03-01' AND DATE '2019-03-31' GROUP BY o.cust_id ORDER BY o.cust_id;

%python
from pyspark.sql import functions as F

result = orders.filter(
    (F.col("order_date") >= F.lit("2019-03-01")) & 
    (F.col("order_date") <= F.lit("2019-03-31"))) \
.groupBy("cust_id") \
.agg(F.sum("total_order_cost").alias("total_revenue")) \
.orderBy("cust_id")

result.show()
---
Find the percentage of orders that are shippable.
An order is considered shippable if the customer's address is known (not NULL).

orders - id, cust_id, order_date, order_details, total_order_cost
customers - id, first_name, last_name, city, address

%sql
SELECT 
    ROUND(
        (COUNT(CASE WHEN c.address IS NOT NULL THEN 1 END) * 100.0) / COUNT(*),2) AS shippable_percentage FROM orders o LEFT JOIN customers c ON o.cust_id = c.id;

%python
from pyspark.sql import functions as F

total_orders = orders.count()
shippable_orders = orders.join(customers, orders.cust_id == customers.id, how="left") \
    .filter(F.col("address").isNotNull()) \
    .count()

shippable_percentage = (shippable_orders / total_orders) * 100
result = spark.createDataFrame([(round(shippable_percentage, 2),)], ["shippable_percentage"])
result.show()
---
Given a list of employees, find the manager(s) of the largest department (department with the highest number of
employees).
A manager is defined as an employee whose position contains the word "manager".
If multiple departments have the same maximum size, return all their managers.

az_employees - id, first_name, last_name, department_id, position

%sql
WITH dept_sizes AS (
    SELECT 
        department_id,
        COUNT(*) AS dept_size
    FROM az_employees
    GROUP BY department_id
),
max_size AS (
    SELECT MAX(dept_size) AS max_dept_size FROM dept_sizes
),
largest_depts AS (
    SELECT department_id
    FROM dept_sizes
    WHERE dept_size = (SELECT max_dept_size FROM max_size)
)
SELECT 
    e.id,
    e.first_name,
    e.last_name,
    e.department_id,
    e.position
FROM az_employees e
JOIN largest_depts ld ON e.department_id = ld.department_id
WHERE LOWER(e.position) LIKE '%manager%';

%python
from pyspark.sql import functions as F

dept_sizes = az_employees.groupBy("department_id") \
    .agg(F.count("*").alias("dept_size"))

max_size = dept_sizes.agg(F.max("dept_size")).collect()[0][0]

largest_depts = dept_sizes.filter(F.col("dept_size") == max_size) \
    .select("department_id")

result = az_employees.join(largest_depts, "department_id") \
    .filter(F.lower(F.col("position")).contains("manager")) \
    .select("id", "first_name", "last_name", "department_id", "position")

result.show()

---
Calculate the percentage of the total spend a customer spent on each order

customers - id, first_name, last_name, city, address, phone_number
orders - id, cust_id, order_date, order_details, total_order_cost

%sql
SELECT 
    o.id AS order_id,
    o.cust_id,
    c.first_name,
    c.last_name,
    o.total_order_cost,
    ROUND(100 * o.total_order_cost / SUM(o.total_order_cost) OVER (PARTITION BY o.cust_id), 2) AS percentage_of_total FROM orders o JOIN customers c ON o.cust_id = c.id ORDER BY o.cust_id, o.id;

%python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

window_spec = Window.partitionBy("cust_id")

result = orders.join(customers, orders.cust_id == customers.id) \
    .withColumn("customer_total", F.sum("total_order_cost").over(window_spec)) \
    .withColumn("percentage_of_total", 
                F.round(100 * F.col("total_order_cost") / F.col("customer_total"), 2)) \
    .select("id", "cust_id", "first_name", "last_name", "total_order_cost", "percentage_of_total") \
    .orderBy("cust_id", "id")

result.show()
---
Find the second highest salary from the worker table.

worker - worker_id, first_name, last_name, salary, joining_date, department

%sql
SELECT salary
FROM (
    SELECT salary, DENSE_RANK() OVER (ORDER BY salary DESC) AS rnk FROM worker) WHERE rnk = 2;

%python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

window_spec = Window.orderBy(F.desc("salary"))

result = worker.withColumn("rank", F.dense_rank().over(window_spec)) \
    .filter(F.col("rank") == 2) \
    .select("salary") \
    .distinct()

result.show()
---
Find the number of orders, the number of customers, and the total cost of orders for each city.
Only include cities that have at least 5 orders, and count all customers in each city even if they did not place an order

customers - id, first_name, last_name, city, address, phone_number
orders - id, cust_id, order_date, order_details, total_order_cost

%sql
WITH city_stats AS (
    SELECT 
        c.city,
        COUNT(DISTINCT o.id) AS number_of_orders,
        COUNT(DISTINCT c.id) AS number_of_customers,
        COALESCE(SUM(o.total_order_cost), 0) AS total_cost
    FROM customers c
    LEFT JOIN orders o ON c.id = o.cust_id
    GROUP BY c.city)
SELECT * FROM city_stats WHERE number_of_orders >= 5 ORDER BY city;

%python
from pyspark.sql import functions as F

city_stats = customers.join(orders, customers.id == orders.cust_id, how="left") \
    .groupBy("city") \
    .agg(
        F.countDistinct("orders.id").alias("number_of_orders"),
        F.countDistinct("customers.id").alias("number_of_customers"),
        F.coalesce(F.sum("total_order_cost"), F.lit(0)).alias("total_cost")) \
    .filter(F.col("number_of_orders") >= 5) \
    .orderBy("city")

city_stats.show()
---
Find the most expensive product in each category.

products - product_id, category, price

%sql
WITH ranked_products AS (
    SELECT 
        product_id,
        category,
        price,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) AS rn FROM products)
SELECT product_id, category, price FROM ranked_products WHERE rn = 1;

%python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

window_spec = Window.partitionBy("category").orderBy(F.desc("price"))

result = products.withColumn("rn", F.row_number().over(window_spec)) \
    .filter(F.col("rn") == 1) \
    .select("product_id", "category", "price") \
    .orderBy("category")

result.show()
---
Find all users who made exactly three purchases

amazon_transactions - id, user_id, item, created_at, revenue

%sql
SELECT user_id FROM amazon_transactions GROUP BY user_id HAVING COUNT(*) = 3;

%python
from pyspark.sql import functions as F

result = amazon_transactions.groupBy("user_id") \
    .agg(F.count("*").alias("purchase_count")) \
    .filter(F.col("purchase_count") == 3) \
    .select("user_id")

result.show()
---
Find the favorite customer — the one who has placed the most orders.

orders - id, cust_id, order_date, order_details, total_order_cost

%sql
WITH order_counts AS (
    SELECT 
        cust_id,
        COUNT(*) AS order_count FROM orders GROUP BY cust_id)
SELECT c.id, c.first_name, c.last_name, oc.order_count FROM customers c
JOIN order_counts oc ON c.id = oc.cust_id WHERE oc.order_count = (SELECT MAX(order_count) FROM order_counts);

%python
from pyspark.sql import functions as F

order_counts = orders.groupBy("cust_id") \
    .agg(F.count("*").alias("order_count"))

max_orders = order_counts.agg(F.max("order_count")).collect()[0][0]

result = customers.join(order_counts, customers.id == order_counts.cust_id) \
    .filter(F.col("order_count") == max_orders) \
    .select("id", "first_name", "last_name", "order_count")

result.show()
---
Find all customers who have never placed an order

customers - id, first_name, last_name, city, address
orders - id, cust_id, order_date, order_details, total_order_cost

%sql
SELECT c.id, c.first_name, c.last_name, c.city, c.address FROM customers c
LEFT JOIN orders o ON c.id = o.cust_id
WHERE o.cust_id IS NULL;

%python
result = customers.join(orders, customers.id == orders.cust_id, how="left") \
    .filter(orders.cust_id.isNull()) \
    .select("id", "first_name", "last_name", "city", "address")

result.show()
---
Find how many customers have never placed an order.

customers - id, first_name, last_name, city, address
orders - id, cust_id, order_date, order_details, total_order_cost

%sql
SELECT COUNT(*) AS customers_without_orders FROM customers c
LEFT JOIN orders o ON c.id = o.cust_id
WHERE o.cust_id IS NULL;

%python
from pyspark.sql import functions as F

result = customers.join(orders, customers.id == orders.cust_id, how="left") \
    .filter(orders.cust_id.isNull()) \
    .agg(F.count("*").alias("customers_without_orders"))

result.show()
---
Find all workers whose salary is less than twice the average salary across all workers.

worker - worker_id, first_name, last_name, salary, joining_date, department

%sql
SELECT worker_id, first_name, last_name, salary, department FROM worker
WHERE salary < 2 * (SELECT AVG(salary) FROM worker) ORDER BY salary DESC;

%python
from pyspark.sql import functions as F

avg_salary = worker.agg(F.avg("salary")).collect()[0][0]
threshold = 2 * avg_salary

result = worker.filter(F.col("salary") < threshold) \
    .select("worker_id", "first_name", "last_name", "salary", "department") \
    .orderBy(F.desc("salary"))

result.show()
---
For each caller, list their phone call history, including:
caller_number
callee_number
call_start
call_end
duration_in_seconds (difference between end and start)

phone_calls - call_id, caller_number, calle_number, call_start, call_end

%sql
SELECT 
    caller_number,
    calle_number,
    call_start,
    call_end,
    ROUND((call_end - call_start) * 86400, 0) AS duration_in_seconds FROM phone_calls ORDER BY caller_number, call_start;

%python
from pyspark.sql import functions as F

result = phone_calls.withColumn(
    "duration_in_seconds", 
    F.round(F.unix_timestamp("call_end") - F.unix_timestamp("call_start"), 0)) \
.select("caller_number", "calle_number", "call_start", "call_end", "duration_in_seconds") \
.orderBy("caller_number", "call_start")

result.show()
---
Find the worker(s) with the highest salary and the worker(s) with the lowest salary.

worker - worker_id, first_name, last_name, salary, joinnig_date, department

%sql
SELECT 'Highest' AS salary_type, worker_id, first_name, last_name, salary
FROM worker
WHERE salary = (SELECT MAX(salary) FROM worker)
UNION ALL
SELECT 'Lowest' AS salary_type, worker_id, first_name, last_name, salary
FROM worker
WHERE salary = (SELECT MIN(salary) FROM worker);

%python
from pyspark.sql import functions as F

max_salary = worker.agg(F.max("salary")).collect()[0][0]
min_salary = worker.agg(F.min("salary")).collect()[0][0]

highest = worker.filter(F.col("salary") == max_salary) \
    .withColumn("salary_type", F.lit("Highest"))

lowest = worker.filter(F.col("salary") == min_salary) \
    .withColumn("salary_type", F.lit("Lowest"))

result = highest.union(lowest) \
    .select("salary_type", "worker_id", "first_name", "last_name", "salary")

result.show()
---
Find all customers who have never placed an order.

customers - id, first_name, last_name, city, address
orders - id, cust_id, order_date, order_details, total_order_cost

%sql
SELECT c.id, c.first_name, c.last_name, c.city, c.address FROM customers c
LEFT JOIN orders o ON c.id = o.cust_id WHERE o.cust_id IS NULL;

%python
result = customers.join(orders, customers.id == orders.cust_id, how="left_anti") \
    .select("id", "first_name", "last_name", "city", "address")

result.show()
---
Identify returning active users — users who made a second purchase within 7 days of any other purchase.

amazon_transactions - id, user_id, item, created_at, revenue

%sql
SELECT DISTINCT at1.user_id FROM amazon_transactions at1
JOIN amazon_transactions at2 ON at1.user_id = at2.user_id WHERE at2.created_at > at1.created_at
    AND at2.created_at <= at1.created_at + 7;

%python
result = amazon_transactions.alias("t1") \
    .join(amazon_transactions.alias("t2"), F.col("t1.user_id") == F.col("t2.user_id")) \
    .filter(
        (F.col("t2.created_at") > F.col("t1.created_at")) &
        (F.col("t2.created_at") <= F.date_add(F.col("t1.created_at"), 7))) \
    .select("t1.user_id") \
    .distinct()

result.show()
---
Find the customer who placed the most orders.

orders - id, cust_id, order_date, order_details, total_order_cost

%sql
WITH order_counts AS (
    SELECT cust_id, COUNT(*) AS order_count
    FROM orders
    GROUP BY cust_id)
SELECT c.id, c.first_name, c.last_name, oc.order_count
FROM customers c
JOIN order_counts oc ON c.id = oc.cust_id
WHERE oc.order_count = (SELECT MAX(order_count) FROM order_counts);

%python
order_counts = orders.groupBy("cust_id") \
    .agg(F.count("*").alias("order_count"))

max_orders = order_counts.agg(F.max("order_count")).collect()[0][0]

result = customers.join(order_counts, customers.id == order_counts.cust_id) \
    .filter(F.col("order_count") == max_orders) \
    .select("id", "first_name", "last_name", "order_count")

result.show()
---
Return the first 50 records from the dataset.
Specifically return all columns, but limit the result to only the first 50 rows.

any_table - col1, col2, col3

%sql
SELECT * FROM any_table WHERE ROWNUM <= 50;

%python
result = any_table.limit(50)

result.show()
---
For each month in the dataset, calculate the percentage difference in total revenue compared to the previous month.

orders - id, cust_id, order_date, order_details, total_order_cost

%sql
WITH monthly_revenue AS (
    SELECT 
        TO_CHAR(order_date, 'YYYY-MM') AS month,
        SUM(total_order_cost) AS revenue FROM orders GROUP BY TO_CHAR(order_date, 'YYYY-MM'))
SELECT 
    month,
    revenue,
    LAG(revenue) OVER (ORDER BY month) AS prev_month_revenue,
    ROUND(100 * (revenue - LAG(revenue) OVER (ORDER BY month)) / 
          NULLIF(LAG(revenue) OVER (ORDER BY month), 0), 2) AS percentage_change FROM monthly_revenue ORDER BY month;

%python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

monthly_revenue = orders.withColumn("month", F.date_format("order_date", "yyyy-MM")) \
    .groupBy("month") \
    .agg(F.sum("total_order_cost").alias("revenue")) \
    .orderBy("month")

window_spec = Window.orderBy("month")

result = monthly_revenue.withColumn("prev_month_revenue", F.lag("revenue").over(window_spec)) \
    .withColumn("percentage_change", 
                F.when(F.col("prev_month_revenue").isNotNull(),
                       F.round(100 * (F.col("revenue") - F.col("prev_month_revenue")) / 
                               F.col("prev_month_revenue"), 2))
                .otherwise(F.lit(None))) \
    .select("month", "revenue", "prev_month_revenue", "percentage_change")

result.show()
---
Find how many users made additional purchases due to the marketing campaign’s success.
The campaign starts one day after the first purchase.
Users who only made one or multiple purchases on their first day do not count.
Users who later repurchased only the same products they purchased on the first day also do not count

marketing_campaign - user_id, created_at, product_id, quantity, price

%sql
WITH first_day_purchases AS (
    SELECT 
        user_id,
        MIN(created_at) AS first_day,
        LISTAGG(product_id, ',') WITHIN GROUP (ORDER BY product_id) AS first_day_products
    FROM marketing_campaign
    GROUP BY user_id
),
customer_purchases AS (
    SELECT DISTINCT
        mc.user_id,
        mc.product_id,
        mc.created_at
    FROM marketing_campaign mc
    JOIN first_day_purchases fdp ON mc.user_id = fdp.user_id
    WHERE mc.created_at > fdp.first_day
)
SELECT COUNT(DISTINCT cp.user_id) AS qualifying_users
FROM customer_purchases cp
LEFT JOIN first_day_purchases fdp ON cp.user_id = fdp.user_id
WHERE INSTR(fdp.first_day_products, cp.product_id) = 0;  -- New product not in first day

%python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

first_day = marketing_campaign.groupBy("user_id") \
    .agg(F.min("created_at").alias("first_day"))

# Get first day products for each user
first_day_products = marketing_campaign.join(first_day, "user_id") \
    .filter(F.col("created_at") == F.col("first_day")) \
    .groupBy("user_id") \
    .agg(F.collect_set("product_id").alias("first_day_products"))

# Get later purchases
later_purchases = marketing_campaign.join(first_day, "user_id") \
    .filter(F.col("created_at") > F.col("first_day")) \
    .select("user_id", "product_id") \
    .distinct()

# Filter out users who only bought first-day products
result = later_purchases.join(first_day_products, "user_id") \
    .filter(~F.array_contains(F.col("first_day_products"), F.col("product_id"))) \
    .select("user_id") \
    .distinct() \
    .agg(F.count("*").alias("qualifying_users"))

result.show()
---
For each month in the dataset, calculate:
total revenue
cumulative revenue up to that month

orders - id, cust_id, order_date, order_details, total_order_cost

%sql
WITH monthly_revenue AS (
    SELECT 
        TO_CHAR(order_date, 'YYYY-MM') AS month,
        SUM(total_order_cost) AS revenue FROM orders GROUP BY TO_CHAR(order_date, 'YYYY-MM'))
SELECT 
    month,
    revenue,
    SUM(revenue) OVER (ORDER BY month) AS cumulative_revenue FROM monthly_revenue ORDER BY month;

%python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

monthly_revenue = orders.withColumn("month", F.date_format("order_date", "yyyy-MM")) \
    .groupBy("month") \
    .agg(F.sum("total_order_cost").alias("revenue")) \
    .orderBy("month")

window_spec = Window.orderBy("month")

result = monthly_revenue.withColumn("cumulative_revenue", 
                                    F.sum("revenue").over(window_spec))

result.show()

---
Find the item with the highest total number of units sold.

sales - id, item, units, revenue, sale_date

%sql
SELECT item, SUM(units) AS total_units FROM sales GROUP BY item ORDER BY total_units DESC
FETCH FIRST 1 ROW ONLY;

%python
result = sales.groupBy("item") \
    .agg(F.sum("units").alias("total_units")) \
    .orderBy(F.desc("total_units")) \
    .limit(1)

result.show()
---
Find the player who had the longest consecutive winning streak.

game_result - game_id, player_id, result, game_date

%sql
WITH ranked_games AS (
    SELECT 
        player_id,
        game_date,
        result,
        ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY game_date) AS game_num,
        ROW_NUMBER() OVER (PARTITION BY player_id, result ORDER BY game_date) AS streak_group
    FROM game_result
),
streaks AS (
    SELECT 
        player_id,
        result,
        COUNT(*) AS streak_length
    FROM ranked_games
    WHERE result = 'W'
    GROUP BY player_id, game_num - streak_group)
SELECT player_id, MAX(streak_length) AS longest_win_streak FROM streaks GROUP BY player_id ORDER BY longest_win_streak DESC
FETCH FIRST 1 ROW ONLY;

%python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

window_player = Window.partitionBy("player_id").orderBy("game_date")
window_streak = Window.partitionBy("player_id", "result_group")

ranked = game_result.withColumn("game_num", F.row_number().over(window_player)) \
    .withColumn("streak_group", 
                F.row_number().over(Window.partitionBy("player_id", "result").orderBy("game_date"))) \
    .withColumn("streak_id", F.col("game_num") - F.col("streak_group"))

streaks = ranked.filter(F.col("result") == "W") \
    .groupBy("player_id", "streak_id") \
    .agg(F.count("*").alias("streak_length"))

result = streaks.groupBy("player_id") \
    .agg(F.max("streak_length").alias("longest_win_streak")) \
    .orderBy(F.desc("longest_win_streak")) \
    .limit(1)

result.show()
---
Calculate the market share of each product brand for each territory for products sold in the fourth quarter of 2021.

fct_customer_sales - cust_id, prod_sku_id, order_date, order_value, order_id
map_customer_territory - cust_id, territory_id
dim_product - prod_sku_id, prod_sku_name, prod_brand, market_name

%sql
WITH brand_sales AS (
    SELECT 
        cmt.territory_id,
        dp.prod_brand,
        SUM(fcs.order_value) AS brand_sales
    FROM fct_customer_sales fcs
    JOIN map_customer_territory cmt ON fcs.cust_id = cmt.cust_id
    JOIN dim_product dp ON fcs.prod_sku_id = dp.prod_sku_id
    WHERE fcs.order_date BETWEEN DATE '2021-10-01' AND DATE '2021-12-31'
    GROUP BY cmt.territory_id, dp.prod_brand
),
territory_total AS (
    SELECT 
        territory_id,
        SUM(brand_sales) AS total_sales
    FROM brand_sales
    GROUP BY territory_id)
SELECT 
    bs.territory_id,
    bs.prod_brand,
    ROUND(100 * bs.brand_sales / tt.total_sales, 2) AS market_share_percentage FROM brand_sales bs JOIN territory_total tt ON bs.territory_id = tt.territory_id
ORDER BY bs.territory_id, market_share_percentage DESC;

%python
brand_sales = fct_customer_sales.join(map_customer_territory, "cust_id") \
    .join(dim_product, "prod_sku_id") \
    .filter(
        (F.col("order_date") >= F.lit("2021-10-01")) &
        (F.col("order_date") <= F.lit("2021-12-31"))
    ) \
    .groupBy("territory_id", "prod_brand") \
    .agg(F.sum("order_value").alias("brand_sales"))

territory_total = brand_sales.groupBy("territory_id") \
    .agg(F.sum("brand_sales").alias("total_sales"))

result = brand_sales.join(territory_total, "territory_id") \
    .withColumn("market_share_percentage", 
                F.round(100 * F.col("brand_sales") / F.col("total_sales"), 2)) \
    .select("territory_id", "prod_brand", "market_share_percentage") \
    .orderBy("territory_id", F.desc("market_share_percentage"))

result.show()
---

Calculate the first‑day retention rate for users.
A user is considered retained if they return the day after their first activity.
Return:
retention_rate (percentage of users retained on day 1)

user_activity - user_id, activity_date, event

%sql
WITH first_activity AS (
    SELECT 
        user_id,
        MIN(activity_date) AS first_date
    FROM user_activity
    GROUP BY user_id
),
retention_check AS (
    SELECT DISTINCT
        fa.user_id,
        fa.first_date,
        CASE WHEN ua.activity_date = fa.first_date + 1 THEN 1 ELSE 0 END AS retained_day1
    FROM first_activity fa
    LEFT JOIN user_activity ua ON fa.user_id = ua.user_id)
SELECT ROUND(100 * SUM(retained_day1) / COUNT(DISTINCT user_id), 2) AS retention_rate FROM retention_check;

%python
first_activity = user_activity.groupBy("user_id") \
    .agg(F.min("activity_date").alias("first_date"))

retention_check = first_activity.join(user_activity, "user_id") \
    .withColumn("retained_day1", 
                F.when(F.col("activity_date") == F.date_add(F.col("first_date"), 1), 1)
                .otherwise(0)) \
    .select("user_id", "first_date", "retained_day1") \
    .distinct()

result = retention_check.agg(
    F.round(100 * F.sum("retained_day1") / F.countDistinct("user_id"), 2).alias("retention_rate"))

result.show()
---
Calculate the total revenue generated each day.

orders - id, order_date, total_order_cost

%sql
SELECT 
    order_date,
    SUM(total_order_cost) AS daily_revenue FROM orders GROUP BY order_date ORDER BY order_date;

%python
result = orders.groupBy("order_date") \
    .agg(F.sum("total_order_cost").alias("daily_revenue")) \
    .orderBy("order_date")

result.show()
---
For each calendar week, determine the number of orders that were:
Pending (placed but not shipped)
Shipped (shipped but not yet received)
Delivered/Received
Return a weekly report showing:
week start or week number
counts of orders by current status

orders_events - order_id, event_type, event_date

%sql
SELECT 
    TO_CHAR(event_date, 'IW') AS week_number,
    MIN(event_date) AS week_start,
    SUM(CASE WHEN event_type = 'Pending' THEN 1 ELSE 0 END) AS pending_orders,
    SUM(CASE WHEN event_type = 'Shipped' THEN 1 ELSE 0 END) AS shipped_orders,
    SUM(CASE WHEN event_type IN ('Delivered', 'Received') THEN 1 ELSE 0 END) AS delivered_orders
FROM orders_events
GROUP BY TO_CHAR(event_date, 'IW')
ORDER BY week_number;

%python
result = orders_events.withColumn("week_number", F.weekofyear("event_date")) \
    .groupBy("week_number") \
    .agg(
        F.min("event_date").alias("week_start"),
        F.sum(F.when(F.col("event_type") == "Pending", 1).otherwise(0)).alias("pending_orders"),
        F.sum(F.when(F.col("event_type") == "Shipped", 1).otherwise(0)).alias("shipped_orders"),
        F.sum(F.when(F.col("event_type").isin(["Delivered", "Received"]), 1).otherwise(0)).alias("delivered_orders")) \
    .orderBy("week_number")

result.show()
---
You're analyzing employee performance at a customer support center.
Rank employees by their average customer satisfaction score on resolved tickets and return the top 3 ranks (with ties
ranked consecutively — no gaps)

amazon_support_tickets - ticket_id, employee_id, employee_name, department, ticket_priority, resolution_time_minutes, customer_satisfaction, resolution_status;

%sql
SELECT employee_id, employee_name, department, avg_satisfaction
FROM (
    SELECT 
        employee_id,
        employee_name,
        department,
        AVG(customer_satisfaction) AS avg_satisfaction,
        DENSE_RANK() OVER (ORDER BY AVG(customer_satisfaction) DESC) AS rank FROM amazon_support_tickets WHERE resolution_status = 'Resolved' GROUP BY employee_id, employee_name, department) WHERE rank <= 3 ORDER BY rank, employee_id;

%python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

employee_avg = amazon_support_tickets.filter(F.col("resolution_status") == "Resolved") \
    .groupBy("employee_id", "employee_name", "department") \
    .agg(F.avg("customer_satisfaction").alias("avg_satisfaction"))

window_spec = Window.orderBy(F.desc("avg_satisfaction"))

result = employee_avg.withColumn("rank", F.dense_rank().over(window_spec)) \
    .filter(F.col("rank") <= 3) \
    .select("employee_id", "employee_name", "department", "avg_satisfaction", "rank") \
    .orderBy("rank", "employee_id")

result.show()









