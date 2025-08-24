# Customers who made purchases on exactly three different days

%sql
with purchase_summary as (
select customer_id, count(distinct purchase_date) as purchase_days from purchases where purchase_date >= add_months(current_date, -1) group by customer_id)
select customer_id from purchase_summary where purchase_days = 3;

%python
from pyspark.sql import functions as F
purchases_summary = (purchase.filter(F.col("purchase_date") >= F.add_months(F.current_date(), -1).groupBy("customer_id").agg(F.countDistinct("purchase_date").alias("purchase_days")))
result = purchase_summary.filter(F.col("purchase_days") == 3)

# Top 2 highest-selling products for each category

%sql
with ranked_sales as (
select p.category, s.product_id, sum(s.sale_amount) as total_sales, rank() over (partition by p.category order by sum(s.sale_amount) desc) as rank from sales s join products p on s.product_id = p.product_id group by p.category, s.product_id)
select category, product_id, total_sales from ranked_sales where rank <= 2;

%python
from pyspark.sql.window import Window
window_spec = Window.partitionBy("category").orderBy(F.desc("total_sales"))
ranked_sales = (sales.join(products, "product_id").groupBy("category", "product_id").agg(F.sum("sales_amount").alias("total_sales")).withColumn("rank", F.rank().over(window_spec)))
result = ranked_sales.filter(F.col("rank") <= 2)

# Sales anomalies (50% lower than average)

%sql
with product_stats as (
select product_id, avg(sale_amount) as avg_sales from sales group by product_id)
select s.product_id, s.sale_amount from sales s join product_stats ps on s.product_id = ps.product_id where s.sales_amount < 0.5 * ps.avg_sales;

%python
product_stats = sales.groupBy("product_id").agg(F.avg("sale_amount").alias("avg_sales"))
result = (sales.join(product_stats, "product_id").filter(F.col("sales_amount") < 0.5 * F.col("avg_sales")))

# Employees never managers, worked in multiple departments

%sql
with manager_list as (
select distinct manager_id from manager_id not null),
department_count as (
select employee_id, count(distinct department_id) as department_count from employees group by employee_id)
select e.employee_id, e.name from employees e join department_count dc on e.employee_id = dc.employee_id where e.employee_id not in (select manager_id from manager_list) and dc.department_count > 1;

%python
manager_list = (employees.filter(F.col("manager_id").isNotNull())
                .select("manager_id").distinct())

department_count = (employees.groupBy("employee_id")
                    .agg(F.countDistinct("department_id").alias("department_count")))

result = (employees.join(department_count, "employee_id")
          .join(manager_list, employees.employee_id == manager_list.manager_id, "left_anti")
          .filter(F.col("department_count") > 1))

# Median salary by department

%sql
with ranked_salaries as (
select department_id, salary, row_number() over(partition by department_id order by salary) as row_num, count(*) over (partition by department_id) as total_rows from employees)
select department_id, avg(salary) as median_salary from ranked_salaries where row_num in (Floor((total_rows + 1)/2), Ceil((total_rows + 1)/2)) group by department_id;

%python
from pyspark.sql.window import Window

window_spec = Window.partitionBy("department_id").orderBy("salary")

ranked_salaries = (employees.withColumn("row_num", F.row_number().over(window_spec))
                   .withColumn("total_rows", F.count("*").over(Window.partitionBy("department_id"))))

result = (ranked_salaries.filter((F.col("row_num") == F.floor((F.col("total_rows") + 1) / 2)) | 
                                (F.col("row_num") == F.ceil((F.col("total_rows") + 1) / 2)))
          .groupBy("department_id")
          .agg(F.avg("salary").alias("median_salary")))

# Customers who purchased from all categories

%sql
WITH categories_per_customer AS (
    SELECT customer_id, COUNT(DISTINCT p.category) AS customer_categories
    FROM purchases pu 
    JOIN products p ON pu.product_id = p.product_id 
    GROUP BY customer_id),
total_categories AS (
SELECT COUNT(DISTINCT category) AS total_categories FROM products)
SELECT customer_id FROM categories_per_customer, total_categories WHERE customer_categories = total_categories;

%python
categories_per_customer = (purchases.join(products, "product_id")
                           .groupBy("customer_id")
                           .agg(F.countDistinct("category").alias("customer_categories")))

total_categories = products.agg(F.countDistinct("category").alias("total_categories"))

result = categories_per_customer.crossJoin(total_categories).filter(
    F.col("customer_categories") == F.col("total_categories"))

# Cumulative sales exceeding daily average

%sql
WITH store_avg AS (
    SELECT store_id, AVG(sale_amount) AS avg_sales FROM sales GROUP BY store_id),
filtered_sales AS (
    SELECT s.store_id, s.sale_date, s.sale_amount 
    FROM sales s
    JOIN store_avg sa ON s.store_id = sa.store_id
    WHERE s.sale_amount > sa.avg_sales)
SELECT store_id, sale_date, SUM(sale_amount) OVER (PARTITION BY store_id ORDER BY sale_date) AS cumulative_sales FROM filtered_sales;

%python
store_avg = sales.groupBy("store_id").agg(F.avg("sale_amount").alias("avg_sales"))

filtered_sales = (sales.join(store_avg, "store_id")
                  .filter(F.col("sale_amount") > F.col("avg_sales")))

window_spec = Window.partitionBy("store_id").orderBy("sale_date")
result = filtered_sales.withColumn("cumulative_sales", F.sum("sale_amount").over(window_spec))

# Employees earning more than department average

%sql
WITH department_avg AS (
    SELECT department_id, AVG(salary) AS avg_salary
    FROM employees
    GROUP BY department_id)
SELECT e.employee_id, e.salary FROM employees e JOIN department_avg da ON e.department_id = da.department_id WHERE e.salary > da.avg_salary;

%python
department_avg = employees.groupBy("department_id").agg(F.avg("salary").alias("avg_salary"))

result = (employees.join(department_avg, "department_id")
          .filter(F.col("salary") > F.col("avg_salary")))

# Missing products and sales count

%sql
SELECT s.product_id, COUNT(*) AS times_sold FROM sales s LEFT JOIN products p ON s.product_id = p.product_id WHERE p.product_id IS NULL GROUP BY s.product_id;

%python
result = (sales.join(products, "product_id", "left_anti")
          .groupBy("product_id")
          .agg(F.count("*").alias("times_sold")))

# Suppliers with fast delivery for large quantities

%sql
SELECT supplier_id FROM deliveries WHERE quantity > 100 GROUP BY supplier_id HAVING AVG(delivery_date - order_date) < 2;

%python
result = (deliveries.filter(F.col("quantity") > 100)
          .withColumn("delivery_days", F.datediff("delivery_date", "order_date"))
          .groupBy("supplier_id")
          .agg(F.avg("delivery_days").alias("avg_delivery_days"))
          .filter(F.col("avg_delivery_days") < 2))

# Inactive customers with past purchases

%sql
WITH six_months_ago AS (
    SELECT customer_id
    FROM purchases
    WHERE purchase_date BETWEEN ADD_MONTHS(CURRENT_DATE, -12) AND ADD_MONTHS(CURRENT_DATE, -6)),
recent_purchases AS (
    SELECT customer_id
    FROM purchases
    WHERE purchase_date >= ADD_MONTHS(CURRENT_DATE, -6))
SELECT DISTINCT c.customer_id FROM customers c JOIN six_months_ago sm ON c.customer_id = sm.customer_id LEFT JOIN recent_purchases rp ON c.customer_id = rp.customer_id WHERE rp.customer_id IS NULL;

%python
six_months_ago = purchases.filter(
    (F.col("purchase_date") >= F.add_months(F.current_date(), -12)) &
    (F.col("purchase_date") <= F.add_months(F.current_date(), -6))).select("customer_id").distinct()

recent_purchases = purchases.filter(
    F.col("purchase_date") >= F.add_months(F.current_date(), -6)).select("customer_id").distinct()

result = (customers.join(six_months_ago, "customer_id")
          .join(recent_purchases, "customer_id", "left_anti"))

# Top 3 product combinations

%sql
WITH product_pairs AS (
    SELECT od1.product_id AS product1, od2.product_id AS product2, COUNT(*) AS pair_count
    FROM order_details od1
    JOIN order_details od2 ON od1.order_id = od2.order_id AND od1.product_id < od2.product_id
    GROUP BY od1.product_id, od2.product_id)
SELECT product1, product2, pair_count FROM product_pairs ORDER BY pair_count DESC FETCH FIRST 3 ROWS ONLY;

%python
order_details1 = order_details.alias("od1")
order_details2 = order_details.alias("od2")

product_pairs = (order_details1.join(order_details2, 
                  (F.col("od1.order_id") == F.col("od2.order_id")) & 
                  (F.col("od1.product_id") < F.col("od2.product_id")))
                .groupBy("od1.product_id", "od2.product_id")
                .agg(F.count("*").alias("pair_count")))

result = product_pairs.orderBy(F.desc("pair_count")).limit(3)

# 7-day moving average

%sql
SELECT product_id, sale_date,AVG(sale_amount) OVER (PARTITION BY product_id ORDER BY sale_date  ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg FROM sales;

%python
window_spec = Window.partitionBy("product_id").orderBy("sale_date").rowsBetween(-6, 0)
result = sales.withColumn("moving_avg", F.avg("sale_amount").over(window_spec))

# Store ranking by monthly sales

%sql
WITH monthly_sales AS (
    SELECT store_id, TRUNC(sale_date, 'MONTH') AS sale_month, SUM(sale_amount) AS total_sales
    FROM sales
    GROUP BY store_id, TRUNC(sale_date, 'MONTH'))
SELECT store_id, sale_month, total_sales,RANK() OVER (PARTITION BY sale_month ORDER BY total_sales DESC) AS rank FROM monthly_sales;

%python
monthly_sales = (sales.withColumn("sale_month", F.date_trunc("month", "sale_date"))
                 .groupBy("store_id", "sale_month")
                 .agg(F.sum("sale_amount").alias("total_sales")))

window_spec = Window.partitionBy("sale_month").orderBy(F.desc("total_sales"))
result = monthly_sales.withColumn("rank", F.rank().over(window_spec))

# Customers with >50% orders in last month

%sql
WITH order_stats AS (
SELECT customer_id, COUNT(*) AS total_orders, SUM(CASE WHEN order_date >= ADD_MONTHS(CURRENT_DATE, -1) THEN 1 ELSE 0 END) AS last_month_orders FROM orders GROUP BY customer_id)
SELECT customer_id FROM order_stats WHERE last_month_orders > 0.5 * total_orders;

%python
order_stats = (orders.groupBy("customer_id")
               .agg(F.count("*").alias("total_orders"),
                    F.sum(F.when(F.col("order_date") >= F.add_months(F.current_date(), -1), 1).otherwise(0)).alias("last_month_orders")))

result = order_stats.filter(F.col("last_month_orders") > 0.5 * F.col("total_orders"))