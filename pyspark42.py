Q1: Identify customers who made purchases on exactly three different days in the last month.

%python
from pyspark.sql.functions import current_date, expr, countDistinct

purchase_summary = purchase.filter(expr("purchase_date >= add_months(current_date, -1)")).groupBy("customer_id").agg(countDistinct("purchase_date").alias("purchase_days"))
result = purchase_summary.filter("purchase_days = 3")
result.select("customer_id").show()

%sql
with pruchase_summary as (
select customer_id, count(distinct purchase) as purchase_days from purchases where purchase_date >= add_months(sysdate, -1) group by customer_id)
select customer_id from purchase_summary where purchase_days = 3;

Q2: Find the top 2 highest-selling products for each category.

%python
from pyspark.sql.window import Window
from pyspark.sql.window import sum, desc, rank

ranked_sales = sales.join(products, "product_id").groupBy("category","product_id").agg(sum("sale_amount").alias("total_sales")).withColumn("rank", rank().over(Window.partitionBy("category").orderBy(desc("total_sales"))))
result = ranked_sales.filter("rank <= 2").select("category", "product_id", "total_sales")
result.show()

%sql
with ranked_sales as (
select p.category, s.product_id, sum(s.sale_amount) as total_sales, rank() over (partition by p.category order by sum(s.sale_amount) desc) as rank from sales s join products p on s.product_id = p.product_id group by p.category, s.product_id;
select category, product_id, total_sales from ranked_sales where rank <= 2;

Q3: Detect anomalies where sales for a product are 50% lower than the average for that product.

%python
product_stats = sales.groupBy("product_id").agg(avg("sale_amount").alias("avg_sales"))
result = sales.join(product_stats, "product_id").filter("sale_amount < 0.5 * avg_sales").select("product_id", "sale_amount")
result.show()

%sql
with product_stats as (
select product_id, avg(sale_amount) as avg_sales from sales group by product)
select s.product_id, s.sale_amount from sales s join product_stats ps on s.product_id = ps.product_id where s.sale_amount < 0.5 * ps.avg_sales;

Q4: Find employees who have never been a manager and have worked in more than one department.

%python
from pyspark.sql.functions import countDistinct

manager_list = employees.select("manager_id").distinct().filter("manager_id is not null")
department_count = employees.groupBy("employee_id").agg(countDistinct("department_id").alias("dept_count")

result = employees.join(department_count, "employee_id").join(manager_list, employees.employee_id == manager_list.manager_id, "left_anti").filter("dept_count >1").select("employee_id", "name")
result.show()

%sql
with manager_list as (
select distinct manager_id from employees where employees where manager_id is not null),
department_count as (
select employee_id, count(distinct department_id) as dept_count from employees group by employee_id)
select e.employee_id, e.name from employees e join department_count dc on e.employee_is = dc.employee_id where e.employee_id not in (select manager_id from manager_list) and dc.dept_count > 1;

Q5: Calculate the median salary in each department.

%python
from pyspark.sql.functions import row_number, count, floor, ceil
from pyspark.sql.window import Window

window_spec = Window.partitionBy("department_id").orderBy("salary")
ranked_salaries = employees.withColumn("row_num", row_number().over(window_spec)).withColumn("total_rows", count("*").over(Window.partitionBy("department_id")))
result = ranked_salaries.filter(expr("row_num in (FLOOR((total_rows + 1)/2), CEIL((total_rows + 1)/2))")).groupBy("department_id").agg(avg("salary").alias("medium_salary"))
result.show()

%sql
with ranked_salaries as (
select department_id, salary, row_number() over (partition by department_id order by salary) as row_num, count(*) over (partition by department_id) as total_rows from employees)
select department_id, avg(salary) as medium_salary from ranked_salaries where row_num in (FLOOR((total_rows + 1)/2), CEIL((total_rows + 1)/2)) group by department_id;

Q6: Identify customers who purchased products from all available categories.

%python
total_categories = products.select(countDistinct("category")).collect()[0][0]
categories_per_customer = purchase.join(products, "product_id").groupBy("customer_id").agg(countDistinct("category").alias("customer_categories"))
result = categories_per_customer.filter(f"customer_categories = {total_categories}").select("customer_id")
result.show()

%sql
with categories_per_customer as (
select customer_id, count(distinct p.category) as customer_categories from purchases pu join products p on pu.product_id = p.product_id group by customer_id),
total_categories as (
select customer_id, count(distinct category) as total_categories from products)
select customer_id from categories_per_customer, total_categories where customer_categories = total_categories;

Q7: Calculate the cumulative sales for each store, but only include dates where the daily sales exceeded the store's average daily sales.

%python
from pyspark.sql.window import Window

store_avg = sales.groupBy("store_id").agg(avg("sale_amount").alias(avg("sale_amount").alias("avg_sales"))
filtered_sales = sales.join(store_avg, "store_id").filter("sale_amount > avg_sales")

window_spec = Window.partitionBy("store_id").orderBy("sale_date")
result = filtered_sales.withColumn("cumulative_sales", sum("sale_amount").over(window_spec)).select("store_id", "sale_date", "cumulative_sales")
result.show()

%sql
with store_avg as (
select store_id, avg(sale_amount) as avg_sales from sales group by store_id),
filtered_sales as (
select s.store_id, s.sale_date, s.sale_amount from sales s join store_avg on s.store_id = sa.store_id where s.sale_amount > sa.avg_sales)
select store_id, sale_date,sum(sale_amount) over (partition by store_id order by sale_date) as cumulative_sales from filtered_sales;

Q8: List employees who earn more than their department average.

%python
department_avg = employees.groupBy("department_id").agg(avg("salary").alias("avg_salary"))
result = employees.join(department_avg, "department_id").filter("salary > avg_salary").select("employee_id", "salary")
result.show()

%sql
with department_avg as (
select department_id, avg(salary) as avg_salary from employees group by department_id)
select e.employee_id, e.salary from employees e join department_avg on e.department_id = da.department_id where e.salary > da.avg_salary;

Q9: Identify products that have been sold but have no record in the products table and also calculate how many times each missing product has been sold.

%python
result = sales.join(products, "product_id", "left_anti").groupBy("product_id").agg(count("*").alias("times_sold"))
result.show()

%sql
select s.product_id, count(*) as times_sold from sales s left join products p on s.product_id = p.product_id where p.product_id is null group by s.product_id;

Q10: Identify suppliers whose average delivery time is less than 2 days, but only consider deliveries with quantities greater than 100 units.

%python
from pyspark.sql.functions import datediff, avg, col

result = deliveries.filter("quantity > 100") \
.withColumn("delivery_time", datediff(col("delivery_date"), col("order_date"))) \
.groupBy("supplier_id") \
.agg(avg("delivery_time").alias("avg_delivery_time")) \
.filter("avg_delivery_time < 2") \
.select("supplier_id")
result.show()

%sql
select supplier_id from deliveries where quantity > 100 group by supplier_id having avg(delivery_date - order_date) < 2;

Q11: Find customers who made no purchases in the last 6 months but made at least one purchase in the 6 months prior to that.

from pyspark.sql.functions import expr

six_months_ago = purchase.filter(expr("purchase_date between add_months(current_date, -12) and add_months(current_date, -6)")).select("customer_id").distinct()

recent_purchases = purchase.filter(expr("purchase_date >= add_months(current_date, -6)")).select("customer_id").distinct()

result = customer.join(six_months_ago, "customer_id").join(recent_purchase, "customer_id", "left_anti").select("customer_id").distinct()
result.show()

%sql
with six_months_ago as (
select distinct customer_id from purchase where purchases where purchase_date between add_months(sysdate,-12) and add_months(sysdate,-6)),
recent_purchases as (
select distinct customer_id from purchases where purchase_date >=add_months(sysdate, -6))
select distinct c.customer_id from customers c join six_months_ago on c.customer_id = sm.customer_id left join recent_purchases rp on c.customer_id = rp.customer_id where rp.customer_id is null;

Q12: Find the top 3 most frequent product combinations bought together.

%python
from pyspark.sql.functions import count, col, desc

product_pairs = order_details.alias("od1").join(
    order_details.alias("od2"), 
    (col("od1.order_id") == col("od2.order_id")) & (col("od1.product_id") < col("od2.product_id"))).groupBy("od1.product_id", "od2.product_id").agg(count("*").alias("pair_count"))

result = product_pairs.orderBy(desc("pair_count")).limit(3)
result.show()

%sql
WITH product_pairs AS (
    SELECT
        od1.product_id AS product1,
        od2.product_id AS product2,
        COUNT(*) AS pair_count
    FROM order_details od1 JOIN order_details od2 ON od1.order_id = od2.order_id AND od1.product_id < od2.product_id GROUP BY od1.product_id, od2.product_id)
SELECT product1, product2, pair_count FROM product_pairs ORDER BY pair_count DESC FETCH FIRST 3 ROWS ONLY;

Q13: Calculate the moving average of sales for each product over a 7-day window.

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import avg

window_spec = Window.partitionBy("product_id").orderBy("sale_date").rowsBetween(-6, 0)
result = sales.withColumn("moving_avg", avg("sale_amount").over(window_spec)).select("product_id", "sale_date", "moving_avg")
result.show()

%sql
SELECT
    product_id,
    sale_date, AVG(sale_amount) OVER (PARTITION BY product_id ORDER BY sale_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg FROM sales;

Q14: Rank stores by their monthly sales performance.

%python
from pyspark.sql.functions import date_trunc, sum, desc, rank

monthly_sales = sales.withColumn("sale_month", date_trunc("month", "sale_date")) \
    .groupBy("store_id", "sale_month") \
    .agg(sum("sale_amount").alias("total_sales"))

window_spec = Window.partitionBy("sale_month").orderBy(desc("total_sales"))
result = monthly_sales.withColumn("rank", rank().over(window_spec)).select("store_id", "sale_month", "total_sales", "rank")
result.show()

%sql
WITH monthly_sales AS (
    SELECT
        store_id,
        TRUNC(sale_date, 'MONTH') AS sale_month,
        SUM(sale_amount) AS total_sales FROM sales GROUP BY store_id, TRUNC(sale_date, 'MONTH'))
SELECT
    store_id,
    sale_month,
    total_sales,
    RANK() OVER (PARTITION BY sale_month ORDER BY total_sales DESC) AS rank FROM monthly_sales;

Q15: Find customers who placed more than 50% of their orders in the last month.

%python
from pyspark.sql.functions import expr, sum as spark_sum, count

order_stats = orders.groupBy("customer_id") \
    .agg(
        count("*").alias("total_orders"),
        spark_sum(expr("CASE WHEN order_date >= add_months(current_date, -1) THEN 1 ELSE 0 END")).alias("last_month_orders"))

result = order_stats.filter("last_month_orders > 0.5 * total_orders").select("customer_id")
result.show()

%sql
WITH order_stats AS (
    SELECT
        customer_id,
        COUNT(*) AS total_orders,
        SUM(CASE WHEN order_date >= ADD_MONTHS(SYSDATE, -1) THEN 1 ELSE 0 END) AS last_month_orders FROM orders GROUP BY customer_id)
SELECT customer_id FROM order_stats WHERE last_month_orders > 0.5 * total_orders;