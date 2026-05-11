1. Consecutive Login Days (3 consecutive days)

%sql
select distinct user_id from (
select user_id, login_date, lag(login_date, 1) over (partition by user_id order by login_date) as prev1, lag(login_date,2) over (partition by user_id order by login_date) as prev2 from logins)
where login_date = prev1 + interval '1' day and prev1 = prev2 + interval '1' day;

%python
from pyspark.sql import Window
from pyspark.sql.functions import lag, col, datediff

window_spec = Window.partitionBy("user_id").orderBy("login_date")
df = df.withColumn("prev1", lag("login_date", 1).over(window_spec)) \ 
           .withColumn("prev2", lag("login_date", 2).over(window_spec))
result = df.where( (datediff(col("login_date"), col("prev1")) == 1) & (datediff(col("prev1"), col("prev2")) == 1)).select("user_id").distinct()

2. Second Highest Order per Customer

%sql
SELECT cust_id, amount FROM (
SELECT cust_id, amount, DENSE_RANK() OVER (PARTITION BY cust_id ORDER BY amount DESC) AS rnk FROM orders) WHERE rnk = 2;

%python
from pyspark.sql import Window
from pyspark.sql.functions import dense_rank

window_spec = Window.partitionBy("cust_id").orderBy(col("amount").desc())

result = df.withColumn("rnk", dense_rank().over(window_spec)) \
           .filter(col("rnk") == 2) \
           .select("cust_id", "amount")

result.show()

3. Employees Joined in Last 30 Days

%sql
SELECT emp_id, emp_name, join_date FROM emp WHERE join_date >= SYSDATE - 30;

%python
from pyspark.sql.functions import current_date, expr

result = df.filter(col("join_date") >= expr("current_date() - interval 30 days")) \
           .select("emp_id", "emp_name", "join_date")

result.show()

4. Repeated Orders Same Day

%sql
SELECT cust_id, order_date, COUNT(*) AS total_orders FROM orders GROUP BY cust_id, order_date HAVING COUNT(*) > 1;

%python
result = df.groupBy("cust_id", "order_date") \
           .count() \
           .filter(col("count") > 1) \
           .withColumnRenamed("count", "total_orders")

result.show()

5. Max Sale Day per Month

%sql
SELECT order_date, total_sales FROM (
SELECT order_date, total_sales, ROW_NUMBER() OVER (PARTITION BY TO_CHAR(order_date, 'YYYY-MM') ORDER BY total_sales DESC) AS rn FROM (
SELECT order_date, SUM(amount) AS total_sales FROM orders GROUP BY order_date)) WHERE rn = 1;

%python
from pyspark.sql import Window
from pyspark.sql.functions import sum, row_number, date_format

daily_sales = df.groupBy("order_date").agg(sum("amount").alias("total_sales"))

window_spec = Window.partitionBy(date_format("order_date", "yyyy-MM")) \
                    .orderBy(col("total_sales").desc())

result = daily_sales.withColumn("rn", row_number().over(window_spec)) \
                    .filter(col("rn") == 1) \
                    .select("order_date", "total_sales")

result.show()

6. Inactive Customers Last 6 Months

%sql
SELECT c.cust_id, c.name FROM customers c LEFT JOIN orders o ON c.cust_id = o.cust_id AND o.order_date >= SYSDATE - INTERVAL '6' MONTH WHERE o.order_id IS NULL;

%python
from pyspark.sql.functions import current_date, expr

six_months_ago = expr("current_date() - interval 6 months")

orders_recent = orders.filter(col("order_date") >= six_months_ago) \
                      .select("cust_id").distinct()

result = customers.join(orders_recent, on="cust_id", how="left_anti") \
                  .select("cust_id", "name")

result.show()

7. Highest Salary in Company

%sql
SELECT emp_id, emp_name, salary FROM emp WHERE salary = (SELECT MAX(salary) FROM emp);

%python
max_salary = df.agg({"salary": "max"}).collect()[0][0]

result = df.filter(col("salary") == max_salary) \
           .select("emp_id", "emp_name", "salary")

result.show()

8. Running Total of Sales by Order Date

%sql
SELECT order_id, order_date, amount, SUM(amount) OVER (ORDER BY order_date, order_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total FROM orders;

%python
from pyspark.sql import Window
from pyspark.sql.functions import sum

window_spec = Window.orderBy("order_date", "order_id") \
                    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = df.withColumn("running_total", sum("amount").over(window_spec)) \
           .select("order_id", "order_date", "amount", "running_total")

result.show()

9. First Order of Each Customer

%sql
SELECT cust_id, order_id, order_date FROM (
SELECT cust_id, order_id, order_date, ROW_NUMBER() OVER (PARTITION BY cust_id ORDER BY order_date, order_id) AS rn FROM orders) WHERE rn = 1;

%python
from pyspark.sql import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("cust_id").orderBy("order_date", "order_id")

result = df.withColumn("rn", row_number().over(window_spec)) \
           .filter(col("rn") == 1) \
           .select("cust_id", "order_id", "order_date")

result.show()

10. Employees Earning Same Salary

%sql
SELECT emp_id, emp_name, salary FROM emp WHERE salary IN (SELECT salary FROM emp GROUP BY salary HAVING COUNT(*) > 1);

%python
from pyspark.sql.functions import count

duplicate_salaries = df.groupBy("salary") \
                        .agg(count("*").alias("cnt")) \
                        .filter(col("cnt") > 1) \
                        .select("salary")

result = df.join(duplicate_salaries, on="salary", how="inner") \
           .select("emp_id", "emp_name", "salary")

result.show()