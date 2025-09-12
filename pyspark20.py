# Find the second highest salary from the Employee table

%pyspark
from pyspark.sql.window import Window
from pyspark.sql.functions import col, max, row_number

# Method 1: Using window function
window_spec = Window.orderBy(col("salary").desc())
employees_with_rank = employees.withColumn("rank", row_number().over(window_spec))
second_highest = employees_with_rank.filter(col("rank") == 2).select("salary")

# Method 2: Using subquery
max_salary = employees.agg(max("salary")).collect()[0][0]
second_highest = employees.filter(col("salary") < max_salary) \
                         .agg(max("salary").alias("SecondHighestSalary"))

%sql
SELECT MAX(salary) AS SecondHighestSalary FROM employees 
WHERE salary < (SELECT MAX(salary) FROM employees);

# Find duplicate records in a table

%pyspark
from pyspark.sql.functions import count

duplicates = employees.groupBy("name").agg(count("*").alias("count")) \
                     .filter(col("count") > 1)

%sql
SELECT name, COUNT(*) FROM employees GROUP BY name HAVING COUNT(*) > 1;

# Retrieve employees who earn more than their manager

%python
employees_alias = employees.alias("e")
managers_alias = employees.alias("m")

result = employees_alias.join(managers_alias, 
                              col("e.manager_id") == col("m.id")) \
                       .filter(col("e.salary") > col("m.salary")) \
                       .select(col("e.name").alias("Employee"), 
                               col("e.salary"), 
                               col("m.name").alias("Manager"), 
                               col("m.salary").alias("ManagerSalary"))

%sql
SELECT e.name AS Employee, e.salary, m.name AS Manager, m.salary AS ManagerSalary FROM employees e
JOIN employees m ON e.manager_id = m.id WHERE e.salary > m.salary;

# Count employees in each department having more than 5 employees

%python
from pyspark.sql.functions import count

dept_counts = employees.groupBy("department_id") \
                      .agg(count("*").alias("num_employees")) \
                      .filter(col("num_employees") > 5)

%sql
SELECT department_id, COUNT(*) AS num_employees FROM employees GROUP BY department_id HAVING COUNT(*) > 5;

# Find employees who joined in the last 6 months

%python
from pyspark.sql.functions import current_date, datediff, months_between

six_months_ago = current_date() - expr("INTERVAL 6 MONTHS")
recent_employees = employees.filter(col("join_date") >= six_months_ago)

%sql
SELECT * FROM employees WHERE join_date >= CURRENT_DATE - INTERVAL '6' MONTH;

# Get departments with no employees

%pyspark
departments_with_employees = employees.select("department_id").distinct()
departments_with_no_employees = departments.join(
    departments_with_employees, 
    departments.department_id == departments_with_employees.department_id, 
    "left_anti").select("department_name")

%sql
SELECT d.department_name FROM departments d LEFT JOIN employees e ON d.department_id = e.department_id WHERE e.id IS NULL;

# Write a query to find the median salary

%python
from pyspark.sql.functions import percentile_approx

median_salary = employee.select(percentile_approx("salary",0.5).alias("median_salary"))

%sql
select percentile_cont(0.5) within group (order by salary) as median_salary from employees;

# Running total of salaries by department

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import sum

window_spec = Window.partitionBy("department_id").orderBy("id")
employees_with_running_total = employees.withColumn(
    "running_total", 
    sum("salary").over(window_spec))

%sql
SELECT name, department_id, salary,SUM(salary) OVER (PARTITION BY department_id ORDER BY id) AS running_total  FROM employees;

# Find the longest consecutive streak of daily logins for each user

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, min, max, count, date_sub

window_spec = Window.partitionBy("user_id").orderBy("login_date")
login_dates = user_logins.withColumn(
    "grp", 
    date_sub("login_date", row_number().over(window_spec)))

streaks = login_dates.groupBy("user_id", "grp") \
                    .agg(
                        count("*").alias("streak_length"),
                        min("login_date").alias("start_date"),
                        max("login_date").alias("end_date")) \
                    .orderBy(col("streak_length").desc())

%sql
WITH login_dates AS (
SELECT user_id, login_date, login_date - ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date) AS grp FROM user_logins)
SELECT user_id, COUNT(*) AS streak_length, MIN(login_date) AS start_date, MAX(login_date) AS end_date FROM login_dates GROUP BY user_id, grp ORDER BY streak_length DESC;

# Recursive query to find the full reporting chain for each employee

%sql
WITH RECURSIVE reporting_chain AS (
    SELECT id, name, manager_id, 1 AS level 
    FROM employees 
    WHERE manager_id IS NULL
    UNION ALL
    SELECT e.id, e.name, e.manager_id, rc.level + 1 
    FROM employees e 
    JOIN reporting_chain rc ON e.manager_id = rc.id)
SELECT * FROM reporting_chain ORDER BY level, id;

# Write a query to find gaps in a sequence of numbers (missing IDs)

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, when

window_spec = Window.orderBy("id")
employees_with_prev = employees.withColumn("prev_id", lag("id").over(window_spec))
missing_ids = employees_with_prev.filter((col("id") - col("prev_id") > 1) | col("prev_id").isNull()).select((col("prev_id") + 1).alias("missing_id"))

%sql
SELECT (id + 1) AS missing_id FROM employees e1 WHERE NOT EXISTS (SELECT 1 FROM employees e2 WHERE e2.id = e1.id + 1) ORDER BY missing_id;

# Calculate cumulative distribution (CDF) of salaries

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import cume_dist

window_spec = Window.orderBy("salary")
employees_with_cdf = employees.withColumn("salary_cdf", cume_dist().over(window_spec))

%sql
SELECT name, salary,CUME_DIST() OVER (ORDER BY salary) AS salary_cdf FROM employees;

# Compare two tables and find rows with differences in any column

%python
# Assuming both tables have the same structure
differences = table1.join(table2, "id", "full_outer") \
                   .filter(
                       (table1.col1 != table2.col1) | 
                       (table1.col2 != table2.col2) | 
                       (table1.col3 != table2.col3) |
                       (table1.col1.isNull() & table2.col1.isNotNull()) | (table1.col1.isNotNull() & table2.col1.isNull()))

%sql
SELECT * FROM table1 t1 FULL OUTER JOIN table2 t2 ON t1.id = t2.id WHERE t1.col1 IS DISTINCT FROM t2.col1  OR t1.col2 IS DISTINCT FROM t2.col2 OR t1.col3 IS DISTINCT FROM t2.col3;

#  Write a query to rank employees based on salary with ties handled properly

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

window_spec = Window.orderBy(col("salary").desc())
employees_with_rank = employees.withColumn("salary_rank", rank().over(window_spec))

%sql
SELECT name, salary, RANK() OVER (ORDER BY salary DESC) AS salary_rank FROM employees;

# Find customers who have not made any purchase

%python
customers_without_sales = customers.join(sales, "customer_id", "left_anti")

%sql
SELECT c.customer_id, c.name FROM customers c LEFT JOIN sales s ON c.customer_id = s.customer_id WHERE s.sale_id IS NULL;

# Write a query to perform a conditional aggregation

%python
from pyspark.sql.functions import sum, when

gender_counts = employees.groupBy("department_id") \
                        .agg(
                            sum(when(col("gender") == "M", 1).otherwise(0)).alias("male_count"),
                            sum(when(col("gender") == "F", 1).otherwise(0)).alias("female_count"))

%sql
SELECT department_id,
COUNT(CASE WHEN gender = 'M' THEN 1 END) AS male_count,
COUNT(CASE WHEN gender = 'F' THEN 1 END) AS female_count FROM employees GROUP BY department_id;

# Write a query to calculate the difference between current row and previous row's salary

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import lag

window_spec = Window.orderBy("id")
employees_with_diff = employees.withColumn("salary_diff", col("salary") - lag("salary").over(window_spec))

%sql
SELECT name, salary, salary - LAG(salary) OVER (ORDER BY id) AS salary_diff FROM employees;

# Identify overlapping date ranges for bookings

%python
overlapping = bookings.alias("b1").join(
    bookings.alias("b2"),
    (col("b1.booking_id") != col("b2.booking_id")) &
    (col("b1.start_date") <= col("b2.end_date")) &
    (col("b1.end_date") >= col("b2.start_date"))).select(col("b1.booking_id"), col("b2.booking_id"))

%sql
SELECT b1.booking_id, b2.booking_id FROM bookings b1 JOIN bookings b2 ON b1.booking_id <> b2.booking_id
WHERE b1.start_date <= b2.end_date AND b1.end_date >= b2.start_date;

# Write a query to find employees with salary greater than average salary

%python
avg_salary = employees.agg(avg("salary")).collect()[0][0]
high_earners = employees.filter(col("salary") > avg_salary).orderBy(col("salary").desc())

%sql
SELECT name, salary FROM employees WHERE salary > (SELECT AVG(salary) FROM employees) ORDER BY salary DESC;

# Aggregate JSON data to list all employee names in a department as a JSON array

%python
from pyspark.sql.functions import collect_list, to_json

dept_employees = employees.groupBy("department_id") \
                          .agg(to_json(collect_list("name")).alias("employee_names"))

%sql
SELECT department_id, JSON_ARRAYAGG(name) AS employee_names FROM employees GROUP BY department_id;

# Find employees who have the same salary as their manager

%python
emp_alias = employees.alias("e")
mgr_alias = employees.alias("m")

same_salary = emp_alias.join(mgr_alias, 
                            col("e.manager_id") == col("m.id")) \
                     .filter(col("e.salary") == col("m.salary")) \
                     .select(col("e.name").alias("Employee"), 
                             col("e.salary"), 
                             col("m.name").alias("Manager"))

%sql
SELECT e.name AS Employee, e.salary, m.name AS Manager FROM employees e JOIN employees m ON e.manager_id = m.id WHERE e.salary = m.salary;

# Write a query to get the first and last purchase date for each customer

%python
from pyspark.sql.functions import min, max

purchase_dates = sales.groupBy("customer_id") \
                     .agg(min("purchase_date").alias("first_purchase"),
                          max("purchase_date").alias("last_purchase"))

%sql
SELECT customer_id, MIN(purchase_date) AS first_purchase, MAX(purchase_date) AS last_purchase FROM sales GROUP BY customer_id;

# Find departments with the highest average salary

%python
from pyspark.sql.functions import avg

avg_salaries = employees.groupBy("department_id") \
                       .agg(avg("salary").alias("avg_salary"))

max_avg_salary = avg_salaries.agg(max("avg_salary")).collect()[0][0]

depts_with_highest_avg = avg_salaries.filter(col("avg_salary") == max_avg_salary)

%sql
WITH avg_salaries AS (
SELECT department_id, AVG(salary) AS avg_salary FROM employees GROUP BY department_id)
SELECT * FROM avg_salaries WHERE avg_salary = (SELECT MAX(avg_salary) FROM avg_salaries);

# Write a query to find the number of employees in each job title

%python
job_title_counts = employees.groupBy("job_title").agg(count("*").alias("num_employees"))

%sql
SELECT job_title, COUNT(*) AS num_employees FROM employees GROUP BY job_title;

# Find employees who don't have a department assigned

%python
no_dept_employees = employees.filter(col("department_id").isNull())

%sql
SELECT * FROM employees WHERE department_id IS NULL;

# Write a query to find the difference in days between two dates in the same table

%python
from pyspark.sql.functions import datediff

projects_with_days_diff = projects.withColumn(
    "days_difference", 
    datediff(col("end_date"), col("start_date")))

%sql
SELECT id, end_date - start_date AS days_difference FROM projects;

# Calculate the moving average of salaries over the last 3 employees ordered by hire date

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import avg

window_spec = Window.orderBy("hire_date").rowsBetween(-2, 0)
employees_with_moving_avg = employees.withColumn(
    "moving_avg_salary", 
    avg("salary").over(window_spec))

%sql
SELECT name, hire_date, salary, AVG(salary) OVER (ORDER BY hire_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg_salary FROM employees;

# Find the most recent purchase per customer using window functions

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("customer_id").orderBy(col("purchase_date").desc())
recent_purchases = sales.withColumn("rn", row_number().over(window_spec)) \
                       .filter(col("rn") == 1) \
                       .drop("rn")

%sql
SELECT *FROM (
SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY purchase_date DESC) AS rn  FROM sales) sub WHERE rn = 1;

# Write a query to perform a self-join to find pairs of employees in the same department

%python
emp1 = employees.alias("e1")
emp2 = employees.alias("e2")

same_dept_pairs = emp1.join(emp2, 
                           (col("e1.department_id") == col("e2.department_id")) & 
                           (col("e1.id") < col("e2.id"))) \
                     .select(col("e1.name").alias("Employee1"), 
                             col("e2.name").alias("Employee2"), 
                             col("e1.department_id"))

%sql
SELECT e1.name AS Employee1, e2.name AS Employee2, e1.department_id FROM employees e1
JOIN employees e2 ON e1.department_id = e2.department_id AND e1.id < e2.id;

# Write a query to pivot rows into columns dynamically

%python
from pyspark.sql.functions import sum, when

pivoted_data = employees.groupBy("department_id") \
                       .pivot("job_title") \
                       .agg(count("*")) \
                       .fillna(0)

%sql
SELECT
    department_id,
    SUM(CASE WHEN job_title = 'Manager' THEN 1 ELSE 0 END) AS Managers,
    SUM(CASE WHEN job_title = 'Developer' THEN 1 ELSE 0 END) AS Developers,
    SUM(CASE WHEN job_title = 'Tester' THEN 1 ELSE 0 END) AS Testers FROM employees GROUP BY department_id;

# Find customers who made purchases in every category available

%python
total_categories = sales.select("category_id").distinct().count()

customer_categories = sales.groupBy("customer_id") \
                          .agg(countDistinct("category_id").alias("categories_count"))

all_categories_customers = customer_categories.filter(col("categories_count") == total_categories).select("customer_id")

%sql
SELECT customer_id FROM sales s GROUP BY customer_id HAVING COUNT(DISTINCT category_id) = (
SELECT COUNT(DISTINCT category_id) FROM sales);

# Identify employees who haven't received a salary raise in more than a year

%python
from pyspark.sql.functions import current_date, datediff, max

one_year_ago = current_date() - expr("INTERVAL 1 YEAR")

last_raises = salary_history.groupBy("employee_id") \
                           .agg(max("raise_date").alias("last_raise_date"))

no_recent_raise = last_raises.filter(col("last_raise_date") < one_year_ago) \
                            .join(employees, "employee_id") \
                            .select("id", "name")

%sql
SELECT e.name FROM employees e JOIN salary_history sh ON e.id = sh.employee_id GROUP BY e.id, e.name
HAVING MAX(sh.raise_date) < CURRENT_DATE - INTERVAL '1 year';

# Write a query to rank salespeople by monthly sales, resetting the rank every month

%python
from pyspark.sql.functions import date_trunc, sum, rank
from pyspark.sql.window import Window

monthly_sales = sales.groupBy(
    "salesperson_id", 
    date_trunc("month", "sale_date").alias("sale_month")).agg(sum("amount").alias("total_sales"))

window_spec = Window.partitionBy("sale_month").orderBy(col("total_sales").desc())
ranked_sales = monthly_sales.withColumn("monthly_rank", rank().over(window_spec))

%sql
SELECT salesperson_id, sale_month, total_sales, RANK() OVER (PARTITION BY sale_month ORDER BY total_sales DESC) AS monthly_rank FROM (
SELECT salesperson_id, TRUNC(sale_date, 'MONTH') AS sale_month, SUM(amount) AS total_sales FROM sales GROUP BY salesperson_id, TRUNC(sale_date, 'MONTH')) AS monthly_sales;

# Calculate the percentage change in sales compared to the previous month for each product

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import lag

monthly_sales = sales.groupBy(
    "product_id", 
    date_trunc("month", "sale_date").alias("sale_month")).agg(sum("amount").alias("total_sales"))

window_spec = Window.partitionBy("product_id").orderBy("sale_month")
sales_with_lag = monthly_sales.withColumn(
    "prev_month_sales", 
    lag("total_sales").over(window_spec)).withColumn(
    "pct_change", 
    ((col("total_sales") - col("prev_month_sales")) * 100.0) / col("prev_month_sales"))

%sql
SELECT product_id, sale_month, total_sales,
    (total_sales - LAG(total_sales) OVER (PARTITION BY product_id ORDER BY sale_month)) * 100.0 /
    LAG(total_sales) OVER (PARTITION BY product_id ORDER BY sale_month) AS pct_change FROM (
    SELECT product_id, TRUNC(sale_date, 'MONTH') AS sale_month, SUM(amount) AS total_sales FROM sales
    GROUP BY product_id, TRUNC(sale_date, 'MONTH')) monthly_sales;

# Find employees who earn more than the average salary across the company but less than the highest salary in their department

%python
from pyspark.sql.functions import avg, max

company_avg_salary = employees.agg(avg("salary")).collect()[0][0]

dept_max_salaries = employees.groupBy("department_id") \
                            .agg(max("salary").alias("dept_max_salary"))

qualified_employees = employees.join(dept_max_salaries, "department_id") \
                              .filter(
                                  (col("salary") > company_avg_salary) & 
                                  (col("salary") < col("dept_max_salary")))

%sql
SELECT *
FROM employees e
WHERE salary > (SELECT AVG(salary) FROM employees)
    AND salary < (
        SELECT MAX(salary)
        FROM employees
        WHERE department_id = e.department_id);

# Retrieve the last 5 orders for each customer

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("customer_id").orderBy(col("order_date").desc())
last_five_orders = orders.withColumn("rn", row_number().over(window_spec)) \
                        .filter(col("rn") <= 5) \
                        .drop("rn")

%sql
SELECT * FROM (
SELECT o.*, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn FROM orders o) sub WHERE rn <= 5;

# Find employees with no salary changes in the last 2 years

%python
from pyspark.sql.functions import current_date

two_years_ago = current_date() - expr("INTERVAL 2 YEARS")

employees_with_recent_changes = salary_history.filter(
    col("change_date") >= two_years_ago).select("employee_id").distinct()

employees_no_recent_changes = employees.join(
    employees_with_recent_changes, 
    employees.id == employees_with_recent_changes.employee_id, 
    "left_anti")

%sql
SELECT e.* FROM employees e LEFT JOIN salary_history sh ON e.id = sh.employee_id  AND sh.change_date >= CURRENT_DATE - INTERVAL '2 years' WHERE sh.employee_id IS NULL;

# Find the department with the lowest average salary

%python
avg_salaries = employees.groupBy("department_id") \
                       .agg(avg("salary").alias("avg_salary"))

min_avg_salary = avg_salaries.agg(min("avg_salary")).collect()[0][0]

dept_with_lowest_avg = avg_salaries.filter(col("avg_salary") == min_avg_salary)

%sql
SELECT department_id, AVG(salary) AS avg_salary FROM employees GROUP BY department_id ORDER BY avg_salary LIMIT 1;

# List employees whose names start and end with the same letter

%python
from pyspark.sql.functions import lower, substring, length

employees_same_letter = employees.filter(
    lower(substring(col("name"), 1, 1)) == 
    lower(substring(col("name"), -1, 1)))

%sql
SELECT * FROM employees WHERE LOWER(SUBSTR(name, 1, 1)) = LOWER(SUBSTR(name, -1, 1));

# Write a query to get the running total of sales per customer, ordered by sale date

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import sum

window_spec = Window.partitionBy("customer_id").orderBy("sale_date")
running_total = sales.withColumn(
    "running_total", 
    sum("amount").over(window_spec)).select("customer_id", "sale_date", "amount", "running_total")

%sql
SELECT customer_id, sale_date, amount, SUM(amount) OVER (PARTITION BY customer_id ORDER BY sale_date) AS running_total FROM sales;

# Find the department-wise salary percentile using window functions

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import percent_rank

window_spec = Window.partitionBy("department_id").orderBy("salary")
employees_with_percentile = employees.withColumn(
    "salary_percentile", 
    percent_rank().over(window_spec))

%sql
SELECT department_id, salary, PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY salary) OVER (PARTITION BY department_id) AS pct_90_salary FROM employees;

# Find employees who have worked for multiple departments over time

%python
from pyspark.sql.functions import countDistinct

multi_dept_employees = employee_department_history.groupBy("employee_id") \
                                                 .agg(countDistinct("department_id").alias("dept_count")) \
                                                 .filter(col("dept_count") > 1) \
                                                 .select("employee_id")

%sql
SELECT employee_id FROM employee_department_history GROUP BY employee_id HAVING COUNT(DISTINCT department_id) > 1;

# Use window function to find the difference between current row's sales and previous row's sales partitioned by product

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import lag

window_spec = Window.partitionBy("product_id").orderBy("sale_date")
sales_with_diff = sales.withColumn(
    "sales_diff", 
    col("amount") - lag("amount").over(window_spec))

%sql
SELECT product_id, sale_date, amount, amount - LAG(amount) OVER (PARTITION BY product_id ORDER BY sale_date) AS sales_diff FROM sales;

# Find average order value per month and product category

%python
from pyspark.sql.functions import date_trunc, avg

monthly_category_avg = orders.groupBy(
    date_trunc("month", "order_date").alias("order_month"), 
    "category_id").agg(avg("order_value").alias("avg_order_value"))

%sql
SELECT DATE_TRUNC('month', order_date) AS order_month, category_id,  AVG(order_value) AS avg_order_value FROM orders GROUP BY order_month, category_id;

# Write a query to create a running count of how many employees joined in each year

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import year, count, sum

yearly_hires = employees.withColumn("join_year", year("hire_date")) \
                       .groupBy("join_year") \
                       .agg(count("*").alias("yearly_hires"))

window_spec = Window.orderBy("join_year")
running_total = yearly_hires.withColumn(
    "running_total_hires", 
    sum("yearly_hires").over(window_spec))

%sql
SELECT join_year, COUNT(*) AS yearly_hires, SUM(COUNT(*)) OVER (ORDER BY join_year) AS running_total_hires FROM (
SELECT EXTRACT(YEAR FROM hire_date) AS join_year FROM employees) sub GROUP BY join_year ORDER BY join_year;

# Write a query to find the second most recent order date per customer

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("customer_id").orderBy(col("order_date").desc())
second_recent_orders = orders.withColumn("rn", row_number().over(window_spec)) \
                            .filter(col("rn") == 2) \
                            .select("customer_id", "order_date")

%sql
SELECT customer_id, order_date FROM (
SELECT customer_id, order_date, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn FROM orders) sub WHERE rn = 2;




