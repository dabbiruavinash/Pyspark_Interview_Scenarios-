# Employees with Salary > Manager Salary

%sql
SELECT e.emp_id, e.emp_name, m.emp_name as manager_name, e.salary, m.salary as manager_salary FROM employee e INNER JOIN employee m ON e.manager_id = m.emp_id WHERE e.salary > m.salary;

%python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Assuming employee_df has columns: emp_id, emp_name, manager_id, salary
employee_df.alias("e")\
    .join(employee_df.alias("m"), F.col("e.manager_id") == F.col("m.emp_id"))\
    .filter(F.col("e.salary") > F.col("m.salary"))\
    .select("e.emp_id", "e.emp_name", F.col("m.emp_name").alias("manager_name"), "e.salary", F.col("m.salary").alias("manager_salary"))\
    .show()

# Second Highest Salary by Department

%sql
SELECT * FROM (
SELECT emp.*, DENSE_RANK() OVER(PARTITION BY department_id ORDER BY salary DESC) as rank_num FROM emp) WHERE rank_num = 2;

%python
window_spec = Window.partitionBy("department_id").orderBy(F.desc("salary"))

emp_df.withColumn("rank_num", F.dense_rank().over(window_spec))\
      .filter(F.col("rank_num") == 2)\
      .drop("rank_num")\
      .show()

# Employees Not in Department Table

%sql
SELECT emp.*, dept.dept_id, dept.dept_name FROM emp LEFT JOIN dept ON emp.department_id = dept.dept_id WHERE dept.dept_id IS NULL;

%python
emp_df.join(dept_df, emp_df.department_id == dept_df.dept_id, "left_anti").show()

# Running Total/Cumulative Sum

%sql
SELECT product_id, cost, SUM(cost) OVER(ORDER BY cost ASC, product_id) as running_cost FROM product;

%python
window_spec = Window.orderBy("cost", "product_id")

product_df.withColumn("running_cost", F.sum("cost").over(window_spec)).show()

# Year-over-Year Growth

%sql
WITH cte AS (
SELECT category, EXTRACT(YEAR FROM order_date) AS year_order, SUM(sales) AS sales FROM order_a GROUP BY category, EXTRACT(YEAR FROM order_date)),
cte2 AS (
SELECT *,LAG(sales, 1, sales) OVER(PARTITION BY category ORDER BY year_order) as prev_year_sales FROM cte)
SELECT *, ROUND((sales - prev_year_sales) * 100 / prev_year_sales, 0) as YOY_sales FROM cte2;

%python
from pyspark.sql.window import Window

window_spec = Window.partitionBy("category").orderBy("year_order")

yoy_df = order_df.groupBy("category", F.year("order_date").alias("year_order"))\
                 .agg(F.sum("sales").alias("sales"))\
                 .withColumn("prev_year_sales", F.lag("sales", 1, "sales").over(window_spec))\
                 .withColumn("YOY_sales", F.round(((F.col("sales") - F.col("prev_year_sales")) * 100 / F.col("prev_year_sales")), 0))

yoy_df.show()

# Pivoting - Year-wise Sales by Category

%sql
SELECT EXTRACT(YEAR FROM order_date) as year_order,
       SUM(CASE WHEN category = 'Furniture' THEN sales ELSE 0 END) AS furniture_sales,
       SUM(CASE WHEN category = 'Office Supplies' THEN sales ELSE 0 END) AS OS_sales,
       SUM(CASE WHEN category = 'Technology' THEN sales ELSE 0 END) AS technology_sales FROM order_a GROUP BY EXTRACT(YEAR FROM order_date);

%python
pivot_df = order_df.groupBy(F.year("order_date").alias("year_order"))\
                   .pivot("category")\
                   .agg(F.sum("sales"))\
                   .fillna(0)

pivot_df.show()

# Median Salary by Company

%sql
SELECT company, AVG(salary) as median_salary FROM (
SELECT *, ROW_NUMBER() OVER(PARTITION BY company ORDER BY salary) as m, COUNT(1) OVER(PARTITION BY company) as total_count FROM median_salary)  WHERE m BETWEEN (total_count * 1.0)/2 AND (total_count * 1.0)/2 + 1 GROUP BY company;

%python
window_spec = Window.partitionBy("company").orderBy("salary")
count_spec = Window.partitionBy("company")

median_df = salary_df.withColumn("m", F.row_number().over(window_spec))\
                     .withColumn("total_count", F.count("*").over(count_spec))\
                     .filter((F.col("m") >= F.col("total_count")/2) & (F.col("m") <= F.col("total_count")/2 + 1))\
                     .groupBy("company")\
                     .agg(F.avg("salary").alias("median_salary"))

median_df.show()

# First Login Device for Each Player

%sql
SELECT * FROM (
SELECT *, RANK() OVER(PARTITION BY player_id ORDER BY event_date) as rank_num FROM activity) WHERE rank_num = 1;

%python
window_spec = Window.partitionBy("player_id").orderBy("event_date")

activity_df.withColumn("rank_num", F.rank().over(window_spec))\
           .filter(F.col("rank_num") == 1)\
           .drop("rank_num")\
           .show()

# Cumulative Games Played

%sql
SELECT *, SUM(games_played) OVER(PARTITION BY player_id ORDER BY event_date) as total_played FROM activity;

%python
window_spec = Window.partitionBy("player_id").orderBy("event_date")

activity_df.withColumn("total_played", F.sum("games_played").over(window_spec)).show()

# Remove Exact Duplicates

%sql
#Method 1
DELETE FROM table WHERE col1 IN (
SELECT id FROM table GROUP BY id HAVING (COUNT(col1) > 1));

#Method2
with cte as (
select emp_name, rank() over(partition by emp_name order by emp_name)m from emp) 
delete from cte where m > 1;

%python
#Method1
empl_df = empl_df.dropDuplicates()

#Method2
window_spec = Window.parititonBy("id").orderBy("salary")
empl_df = empl_df.withColumn("row_num", F.row_number().over(window_spec)).filter(F.col("row_num") == 1).drop("row_num")

# Find Mode (Most Frequent Value)

%sql
WITH Freq_cte AS (
SELECT id, COUNT(*) as freq FROM mode_table GROUP BY id)
SELECT * FROM Freq_cte WHERE freq = (SELECT MAX(freq) FROM Freq_cte);

%python
from pyspark.sql import functions as F

freq_df = mode_df.groupBy("id").agg(F.count("*").alias("freq"))
max_freq = freq_df.agg(F.max("freq").alias("max_freq")).first()["max_freq"]
result_df = freq_df.filter(F.col("freq") == max_freq)

# Employee Status Change (2020 to 2021)

%sql
SELECT e20.*, e21.*,CASE
WHEN e20.designation IS DISTINCT FROM e21.designation THEN 'Promoted'
WHEN e21.designation IS NULL THEN 'Resigned' ELSE 'New' END AS comment FROM emp_2020 e20 FULL OUTER JOIN emp_2021 e21 ON e20.emp_id = e21.emp_id WHERE COALESCE(e20.designation, 'XXX') != COALESCE(e21.designation, 'YYY');

%python
result_df =  emp_2020.alias("e20").join(emp_2021.alias("e21"), 
                    F.col("e20.emp_id") == F.col("e21.emp_id"), "full_outer")\
                   .withColumn("comment", F.when(F.col("e20.designation") != F.col("e21.designation"), "Promoted")
                   .when(F.col("e21.designation").isNull(), "Resigned")
                   .otherwise("New"))\
                   .filter(F.coalesce(F.col("e20.designation"), F.lit("XXX")) != F.coalesce(F.col("e21.designation"), F.lit("YYY")))


# Rank Duplicate Records

%sql
WITH cte_dup AS (
SELECT id FROM list_table GROUP BY id HAVING COUNT(1) > 1),
cte_rank AS (
SELECT id, RANK() OVER(ORDER BY id ASC) as m FROM cte_dup)
SELECT l.id, 'DUP' || CAST(cr.m AS VARCHAR2(2)) as output FROM list_table l LEFT JOIN cte_rank cr ON l.id = cr.id;

%python
cte_dup = list_df.groupBy("id").agg(F.count("*").alias("cnt")).filter(F.col("cnt") > 1).select("id")
window_spec = Window.orderBy("id")
cte_rank = cte_dup.withColumn("m", F.rank().over(window_spec))

result_df = list_df.join(cte_rank, "id", "left")\
    .withColumn("output", 
                F.when(F.col("m").isNotNull(), F.concat(F.lit("DUP"), F.col("m").cast("string")))
                 .otherwise(F.lit(None)))

# NTILE Function (Customer Groups)

%sql
WITH cte AS (
SELECT Customer_Name, region, SUM(sales) as total_Sales  FROM Order_A GROUP BY Customer_Name, region ORDER BY region, total_sales DESC FETCH FIRST 18 ROWS ONLY)
SELECT * FROM (
SELECT *, NTILE(4) OVER (PARTITION BY region ORDER BY total_Sales) as cust_groups  FROM cte) WHERE cust_groups = 1;

%python
cte = order_df.groupBy("Customer_Name", "region").agg(F.sum("sales").alias("total_Sales"))\
    .orderBy("region", F.desc("total_Sales"))\
    .limit(18)

window_spec = Window.partitionBy("region").orderBy("total_Sales")
result_df = cte.withColumn("cust_groups", F.ntile(4).over(window_spec))\
    .filter(F.col("cust_groups") == 1)

# Gold Medal Winners Only

%sql
SELECT gold as player_name, COUNT(1) as no_of_medals FROM events WHERE gold NOT IN (SELECT silver FROM events UNION ALL SELECT bronze FROM events) GROUP BY gold;

%python
silver_bronze = events_df.select("silver").unionAll(events_df.select("bronze"))
result_df = events_df.filter(~F.col("gold").isin([row["silver"] for row in silver_bronze.collect()]))\
    .groupBy("gold").agg(F.count("*").alias("no_of_medals"))\
    .withColumnRenamed("gold", "player_name")

# Business Days Between Dates

%sql
SELECT
    t.ticket_id,
    t.create_date,
    t.resolved_date,
    COUNT(*) AS business_days FROM tickets t
CROSS JOIN LATERAL (
    SELECT LEVEL - 1 as day_offset
    FROM DUAL
    CONNECT BY LEVEL <= t.resolved_date - t.create_date + 1) days
WHERE TO_CHAR(t.create_date + day_offset, 'D') NOT IN ('6', '7') -- Exclude weekends
AND NOT EXISTS (
    SELECT 1 FROM holidays h 
    WHERE h.holiday_date = t.create_date + day_offset) GROUP BY t.ticket_id, t.create_date, t.resolved_date;

%python
from pyspark.sql import functions as F

def calculate_business_days(create_date, resolved_date, holidays):
    from datetime import timedelta
    business_days = 0
    current_date = create_date
    while current_date <= resolved_date:
        if current_date.weekday() < 5 and current_date not in holidays:
            business_days += 1
        current_date += timedelta(days=1)
    return business_days

# UDF registration
calculate_business_days_udf = F.udf(calculate_business_days, IntegerType())

holidays_list = [row['holiday_date'] for row in holidays_df.collect()]
result_df = tickets_df.withColumn("business_days", 
    calculate_business_days_udf(F.col("create_date"), F.col("resolved_date"), F.lit(holidays_list)))

# People Inside Hospital

%sql
WITH cte AS (
SELECT emp_id, MAX(CASE WHEN action='in' THEN time END) as intime, MAX(CASE WHEN action = 'out' THEN time END) as outtime FROM hospital GROUP BY emp_id)
SELECT * FROM cte WHERE intime > outtime OR outtime IS NULL;

%python
cte = hospital_df.groupBy("emp_id").agg(
    F.max(F.when(F.col("action") == "in", F.col("time"))).alias("intime"),
    F.max(F.when(F.col("action") == "out", F.col("time"))).alias("outtime"))

result_df = cte.filter((F.col("intime") > F.col("outtime")) | F.col("outtime").isNull())

# Unique Salary in Department

%sql
WITH sal_dep AS (
SELECT dept_id, salary FROM emp_salary GROUP BY dept_id, salary HAVING COUNT(1) = 1)
SELECT es.* FROM emp_salary es INNER JOIN sal_dep sd ON es.dept_id = sd.dept_id AND es.salary = sd.salary;

%python
sal_dep = emp_salary_df.groupBy("dept_id", "salary").agg(F.count("*").alias("cnt")).filter(F.col("cnt") == 1)
result_df = emp_salary_df.join(sal_dep, ["dept_id", "salary"])

# Repeated Words in Content

%sql
SELECT word, COUNT(*) AS cnt_of_words
FROM (
    SELECT REGEXP_SUBSTR(content, '[^ ]+', 1, LEVEL) AS word
    FROM namaste_python
    CONNECT BY LEVEL <= REGEXP_COUNT(content, ' ') + 1
    AND PRIOR SYS_GUID() IS NOT NULL
    AND PRIOR id = id) WHERE word IS NOT NULL GROUP BY word HAVING COUNT(*) > 1 ORDER BY cnt_of_words DESC;

%python
from pyspark.sql.functions import explode, split

result_df = namaste_python_df\
    .withColumn("word", explode(split(F.col("content"), " ")))\
    .groupBy("word").agg(F.count("*").alias("cnt_of_words"))\
    .filter(F.col("cnt_of_words") > 1)\
    .orderBy(F.desc("cnt_of_words"))

# Highest Rated Movie by Genre with Stars

%sql
WITH cte AS (
SELECT m.genre, m.title, ROUND(AVG(r.rating), 0) as avg_rating, ROW_NUMBER() OVER(PARTITION BY m.genre ORDER BY AVG(r.rating) DESC) as rank_num FROM movies m INNER JOIN reviews r ON m.id = r.movie_id GROUP BY m.genre, m.title)
SELECT genre, title, avg_rating, RPAD('*', avg_rating, '*') as stars FROM cte WHERE rank_num = 1;

%python
cte = movies_df.join(reviews_df, "movie_id")\
    .groupBy("genre", "title").agg(F.avg("rating").alias("avg_rating"))\
    .withColumn("avg_rating", F.round(F.col("avg_rating"), 0))\
    .withColumn("rank_num", F.row_number().over(Window.partitionBy("genre").orderBy(F.desc("avg_rating"))))

result_df = cte.filter(F.col("rank_num") == 1)\
    .withColumn("stars", F.expr("repeat('*', cast(avg_rating as int))"))

# Highest and Lowest Salary by Department

%sql
WITH cte AS (
SELECT *, RANK() OVER(PARTITION BY dep_id ORDER BY salary DESC) as rank_desc, RANK() OVER(PARTITION BY dep_id ORDER BY salary ASC) as rank_asc FROM employee1)
SELECT dep_id, MAX(CASE WHEN rank_desc = 1 THEN emp_name END) as max_sal_emp, MIN(CASE WHEN rank_asc = 1 THEN emp_name END) as min_sal_emp FROM cte GROUP BY dep_id;

%python
window_desc = Window.partitionBy("dep_id").orderBy(F.desc("salary"))
window_asc = Window.partitionBy("dep_id").orderBy(F.asc("salary"))

cte = employee1_df\
    .withColumn("rank_desc", F.rank().over(window_desc))\
    .withColumn("rank_asc", F.rank().over(window_asc))

result_df = cte.groupBy("dep_id").agg(
    F.max(F.when(F.col("rank_desc") == 1, F.col("emp_name"))).alias("max_sal_emp"),
    F.min(F.when(F.col("rank_asc") == 1, F.col("emp_name"))).alias("min_sal_emp"))

# Names Not Common in Source and Target

%sql
SELECT COALESCE(s.id, t.id) as id,
       CASE 
           WHEN t.name IS NULL THEN 'new in source' 
           WHEN s.name IS NULL THEN 'new in target'
           ELSE 'mismatched'  END as comment FROM source s FULL OUTER JOIN target t ON s.id = t.id WHERE s.name != t.name OR s.name IS NULL OR t.name IS NULL;

%python
result_df = source_df.alias("s").join(target_df.alias("t"), "id", "full_outer")\
    .withColumn("comment", F.when(F.col("t.name").isNull(), "new in source")
    .when(F.col("s.name").isNull(), "new in target").otherwise("mismatched"))\
    .filter((F.col("s.name") != F.col("t.name")) | F.col("s.name").isNull() | F.col("t.name").isNull())\
    .select(F.coalesce(F.col("s.id"), F.col("t.id")).alias("id"), "comment")

# Final Flight Destination

%sql
SELECT o.cid, o.origin, d.destination as final_destination FROM flights o INNER JOIN flights d ON o.destination = d.origin;

%python
result_df = flights_df.alias("o").join(flights_df.alias("d"), 
                                      F.col("o.destination") == F.col("d.origin")).select("o.cid", "o.origin", F.col("d.destination").alias("final_destination"))

# New Customers by Month

%sql
SELECT order_date, COUNT(DISTINCT customer) as count_new_cust FROM (
SELECT *, RANK() OVER(PARTITION BY customer ORDER BY order_date) as rank_num FROM sales) WHERE rank_num = 1 GROUP BY order_date;

%python
window_spec = Window.partitionBy("customer").orderBy("order_date")
result_df = sales_df.withColumn("rank_num", F.rank().over(window_spec))\
    .filter(F.col("rank_num") == 1)\
    .groupBy("order_date").agg(F.countDistinct("customer").alias("count_new_cust"))

# Father and Mother Names for Children

%sql
SELECT r.c_id, p.name as child_name, 
       MAX(m.name) as mother_name, 
       MAX(f.name) as father_name
FROM relations r
LEFT JOIN people m ON r.p_id = m.id AND m.gender = 'F'
LEFT JOIN people f ON r.p_id = f.id AND f.gender = 'M'
INNER JOIN people p ON p.id = r.c_id
GROUP BY r.c_id, p.name;

%python
result_df = relations_df.alias("r")\
    .join(people_df.alias("p"), F.col("r.c_id") == F.col("p.id"))\
    .join(people_df.alias("m"), (F.col("r.p_id") == F.col("m.id")) & (F.col("m.gender") == "F"), "left")\
    .join(people_df.alias("f"), (F.col("r.p_id") == F.col("f.id")) & (F.col("f.gender") == "M"), "left")\
    .groupBy("r.c_id", F.col("p.name").alias("child_name"))\
    .agg(F.max("m.name").alias("mother_name"), F.max("f.name").alias("father_name"))

# Companies with Increasing Revenue

%sql
WITH cte AS (
SELECT *, revenue - LAG(revenue, 1, 0) OVER(PARTITION BY company ORDER BY year) as rev_diff FROM company_revenue)
SELECT company FROM cte WHERE company NOT IN (SELECT company FROM cte WHERE rev_diff < 0) GROUP BY company;

%python
window_spec = Window.partitionBy("company").orderBy("year")
cte = company_revenue_df\
    .withColumn("prev_revenue", F.lag("revenue", 1, 0).over(window_spec))\
    .withColumn("rev_diff", F.col("revenue") - F.col("prev_revenue"))

companies_with_decrease = cte.filter(F.col("rev_diff") < 0).select("company").distinct()
result_df = cte.filter(~F.col("company").isin([row["company"] for row in companies_with_decrease.collect()]))\
    .select("company").distinct()

# Adult-Child Pairing for Rides

%sql
WITH cte_adult AS (
    SELECT *, ROW_NUMBER() OVER(ORDER BY age DESC) as rank_num 
    FROM family
    WHERE type = 'Adult'),
cte_child AS (
    SELECT *, ROW_NUMBER() OVER(ORDER BY age ASC) as rank_num 
    FROM family
    WHERE type = 'Child')
SELECT a.person as adult, c.person as child, a.age as adult_age, c.age as child_age FROM cte_adult a LEFT JOIN cte_child c ON c.rank_num = a.rank_num;

%python
adults = family_df.filter(F.col("type") == "Adult")\
    .withColumn("rank_num", F.row_number().over(Window.orderBy(F.desc("age"))))

children = family_df.filter(F.col("type") == "Child")\
    .withColumn("rank_num", F.row_number().over(Window.orderBy(F.asc("age"))))

result_df = adults.alias("a").join(children.alias("c"), "rank_num", "left")\
    .select(F.col("a.person").alias("adult"), F.col("c.person").alias("child"),
            F.col("a.age").alias("adult_age"), F.col("c.age").alias("child_age"))

# Team Qualification Criteria

%sql
SELECT al.*, CASE WHEN Criteria1 = 'Y' AND Criteria2 = 'Y' AND SUM(CASE WHEN Criteria1 = 'Y' AND Criteria2 = 'Y' THEN 1 ELSE 0 END)  OVER (PARTITION BY teamID) >= 2  THEN 'Y' ELSE 'N' END as qualified_flag FROM Ameriprise_LLC al;

%python
window_spec = Window.partitionBy("teamID")

result_df = ameriprise_df\
    .withColumn("qualified_count", 
                F.sum(F.when((F.col("Criteria1") == "Y") & (F.col("Criteria2") == "Y"), 1).otherwise(0)).over(window_spec))\
    .withColumn("qualified_flag",
                F.when((F.col("Criteria1") == "Y") & (F.col("Criteria2") == "Y") & (F.col("qualified_count") >= 2), "Y").otherwise("N"))\
    .drop("qualified_count")

# Top 2 Salaried Employees by Department

%sql
SELECT * FROM (
SELECT *, DENSE_RANK() OVER(PARTITION BY department_id ORDER BY salary DESC) as dense_rn FROM emp) WHERE dense_rn <= 2;

%python
window_spec = Window.partitionBy("department_id").orderBy(F.desc("salary"))
result_df = empja_df.withColumn("dense_rn", F.dense_rank().over(window_spec)).filter(F.col("dense_rn") <= 2)

# Top 5 Products by Category and Sales

%sql
WITH cte AS (
    SELECT category, product_id, SUM(sales) as sales
    FROM order_a
    GROUP BY category, product_id)
SELECT * FROM (
SELECT *, ROW_NUMBER() OVER(PARTITION BY category ORDER BY sales DESC) as rank_num FROM cte) WHERE rank_num <= 5;

%python
cte = order_df.groupBy("category", "product_id").agg(F.sum("sales").alias("sales"))
window_spec = Window.partitionBy("category").orderBy(F.desc("sales"))
result_df = cte.withColumn("rank_num", F.row_number().over(window_spec)).filter(F.col("rank_num") <= 5)

# Cumulative Sales Year-wise

%sql
WITH cte AS (
SELECT category, EXTRACT(YEAR FROM order_date) AS year_order, SUM(sales) AS sales FROM order_a GROUP BY category, EXTRACT(YEAR FROM order_date))
SELECT *, SUM(sales) OVER(PARTITION BY category ORDER BY year_order) as cumulative_sales FROM cte;

%python
cte = order_df.groupBy("category", F.year("order_date").alias("year_order")).agg(F.sum("sales").alias("sales"))
window_spec = Window.partitionBy("category").orderBy("year_order")
result_df = cte.withColumn("cumulative_sales", F.sum("sales").over(window_spec))

# Pivoting - Year-wise Sales for Each Category

%sql
SELECT EXTRACT(YEAR FROM order_date) as year_order,
       SUM(CASE WHEN category = 'Furniture' THEN sales ELSE 0 END) AS furniture_sales,
       SUM(CASE WHEN category = 'Office Supplies' THEN sales ELSE 0 END) AS OS_sales,
       SUM(CASE WHEN category = 'Technology' THEN sales ELSE 0 END) AS technology_sales FROM order_a GROUP BY EXTRACT(YEAR FROM order_date);

%python
result_df = order_df.groupBy(F.year("order_date").alias("year_order"))\
    .pivot("category")\
    .agg(F.sum("sales"))\
    .fillna(0)\
    .withColumnRenamed("Furniture", "furniture_sales")\
    .withColumnRenamed("Office Supplies", "OS_sales")\
    .withColumnRenamed("Technology", "technology_sales")

# Game Interaction Categories

%sql
SELECT game_id,
    CASE
        WHEN COUNT(interaction_type) = 0 THEN 'No social interaction'
        WHEN COUNT(DISTINCT CASE WHEN interaction_type IS NOT NULL THEN user_id END) = 1
        THEN 'One sided interaction'
        WHEN COUNT(DISTINCT CASE WHEN interaction_type IS NOT NULL THEN user_id END) = 2 AND
        COUNT(DISTINCT CASE WHEN interaction_type = 'custom_typed' THEN user_id END) = 0
        THEN 'Both sided interaction without customer typed messages'
        WHEN COUNT(DISTINCT CASE WHEN interaction_type IS NOT NULL THEN user_id END) = 2 AND
        COUNT(DISTINCT CASE WHEN interaction_type = 'custom_typed' THEN user_id END) >= 1
        THEN 'Both sided interaction with customer typed message from at least one player' END AS game_type FROM user_interactions GROUP BY game_id;

%python
result_df = user_interactions_df.groupBy("game_id").agg(
    F.count("interaction_type").alias("interaction_count"),
    F.countDistinct(F.when(F.col("interaction_type").isNotNull(), F.col("user_id"))).alias("distinct_users"),
    F.countDistinct(F.when(F.col("interaction_type") == "custom_typed", F.col("user_id"))).alias("custom_typed_users")
).withColumn("game_type",
    F.when(F.col("interaction_count") == 0, "No social interaction")
     .when(F.col("distinct_users") == 1, "One sided interaction")
     .when((F.col("distinct_users") == 2) & (F.col("custom_typed_users") == 0), 
           "Both sided interaction without customer typed messages")
     .when((F.col("distinct_users") == 2) & (F.col("custom_typed_users") >= 1), 
           "Both sided interaction with customer typed message from at least one player")
     .otherwise("Unknown")).select("game_id", "game_type")

# Extract First, Middle, and Last Names

%sql
WITH cte AS (
    SELECT customer_name,
           LENGTH(customer_name) - LENGTH(REPLACE(customer_name, ' ', '')) as no_of_space,
           INSTR(customer_name, ' ', 1, 1) as first_space_position,
           INSTR(customer_name, ' ', 1, 2) as second_space_position FROM customers)
SELECT customer_name,
    CASE WHEN no_of_space = 0 THEN customer_name ELSE SUBSTR(customer_name, 1, first_space_position - 1) END as first_name,
    CASE WHEN no_of_space <= 1 THEN NULL ELSE SUBSTR(customer_name, first_space_position + 1, second_space_position - first_space_position - 1) END as middle_name,
    CASE WHEN no_of_space = 0 THEN NULL
         WHEN no_of_space = 1 THEN SUBSTR(customer_name, first_space_position + 1)
         WHEN no_of_space = 2 THEN SUBSTR(customer_name, second_space_position + 1) END as last_name FROM cte;

%python
from pyspark.sql.functions import split, size, col

result_df = customers_df.withColumn("name_parts", split(F.col("customer_name"), " "))\
    .withColumn("no_of_spaces", size(F.col("name_parts")) - 1)\
    .withColumn("first_name", 
                F.when(F.col("no_of_spaces") == 0, F.col("customer_name"))
                 .otherwise(F.col("name_parts")[0]))\
    .withColumn("middle_name",
                F.when(F.col("no_of_spaces") <= 1, F.lit(None))
                 .otherwise(F.col("name_parts")[1]))\
    .withColumn("last_name",
                F.when(F.col("no_of_spaces") == 0, F.lit(None))
                 .when(F.col("no_of_spaces") == 1, F.col("name_parts")[1])
                 .otherwise(F.col("name_parts")[2]))\
    .drop("name_parts", "no_of_spaces")

# Consecutive Rows with 100+ People

%sql
WITH cte AS (
SELECT *, ROW_NUMBER() OVER(ORDER BY visit_date) as rn, id - ROW_NUMBER() OVER(ORDER BY visit_date) as grp FROM stadium WHERE no_of_people >= 100)
SELECT id, visit_date, no_of_people FROM cte WHERE grp IN (
SELECT grp FROM cte GROUP BY grp HAVING COUNT(1) >= 3);

%python
window_spec = Window.orderBy("visit_date")
cte = stadium_df.filter(F.col("no_of_people") >= 100)\
    .withColumn("rn", F.row_number().over(window_spec))\
    .withColumn("grp", F.col("id") - F.col("rn"))

grp_counts = cte.groupBy("grp").agg(F.count("*").alias("cnt")).filter(F.col("cnt") >= 3)
result_df = cte.join(grp_counts, "grp").select("id", "visit_date", "no_of_people")

# Median Salary by Company

%sql
SELECT company, AVG(salary) as median_salary FROM (
SELECT *, ROW_NUMBER() OVER(PARTITION BY company ORDER BY salary) as m, COUNT(1) OVER(PARTITION BY company) as total_count FROM median_salary)  WHERE m BETWEEN (total_count * 1.0)/2 AND (total_count * 1.0)/2 + 1 GROUP BY company;

%python
window_spec = Window.partitionBy("company").orderBy("salary")
count_spec = Window.partitionBy("company")

result_df = median_salary_df\
    .withColumn("m", F.row_number().over(window_spec))\
    .withColumn("total_count", F.count("*").over(count_spec))\
    .filter((F.col("m") >= F.col("total_count")/2) & 
            (F.col("m") <= F.col("total_count")/2 + 1))\
    .groupBy("company")\
    .agg(F.avg("salary").alias("median_salary"))

# Pivot Players by City

%sql
SELECT
    MAX(CASE WHEN city = 'Bangalore' THEN name END) as Bangalore,
    MAX(CASE WHEN city = 'Mumbai' THEN name END) as Mumbai,
    MAX(CASE WHEN city = 'Delhi' THEN name END) as Delhi FROM (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY city ORDER BY name) as players_location FROM players_location) GROUP BY players_location ORDER BY players_location;

%python
window_spec = Window.partitionBy("city").orderBy("name")
ranked_df = players_location_df.withColumn("players_location", F.row_number().over(window_spec))

result_df = ranked_df.groupBy("players_location")\
    .pivot("city")\
    .agg(F.first("name"))\
    .select("players_location", "Bangalore", "Mumbai", "Delhi")\
    .orderBy("players_location")

# Second Most Recent Activity

%sql
WITH cte AS (
SELECT *,COUNT(1) OVER(PARTITION BY username) as total_activities, ROW_NUMBER() OVER(PARTITION BY username ORDER BY startDate DESC) as rn FROM UserActivity)
SELECT * FROM cte WHERE total_activities = 1 OR rn = 2;

%python
window_spec_count = Window.partitionBy("username")
window_spec_rank = Window.partitionBy("username").orderBy(F.desc("startDate"))

result_df = user_activity_df\
    .withColumn("total_activities", F.count("*").over(window_spec_count))\
    .withColumn("rn", F.row_number().over(window_spec_rank))\
    .filter((F.col("total_activities") == 1) | (F.col("rn") == 2))\
    .drop("total_activities", "rn")

# Total Sales by Year (Recursive)

%sql
WITH recursive_sales_cte (dates, max_date, product_id, average_daily_sales) AS (
    SELECT period_start as dates, 
           period_end as max_date,
           product_id,
           average_daily_sales
    FROM recursive_sales
    UNION ALL
    SELECT dates + INTERVAL '1' DAY, 
           max_date,
           product_id,
           average_daily_sales
    FROM recursive_sales_cte
    WHERE dates < max_date)
SELECT product_id, EXTRACT(YEAR FROM dates) as report_year,
       SUM(average_daily_sales) as total_amount
FROM recursive_sales_cte
GROUP BY product_id, EXTRACT(YEAR FROM dates)
ORDER BY product_id, report_year;

%python
from pyspark.sql.functions import datediff, explode, sequence

# Calculate number of days between period_start and period_end
sales_with_days = recursive_sales_df.withColumn(
    "num_days", datediff(F.col("period_end"), F.col("period_start")) + 1)

# Generate date sequence for each product
result_df = sales_with_days.withColumn(
    "date", explode(sequence(F.col("period_start"), F.col("period_end")))).withColumn("report_year", F.year("date"))\
.groupBy("product_id", "report_year")\
.agg(F.sum("average_daily_sales").alias("total_amount"))\
.orderBy("product_id", "report_year")

# Spending by Platform Type

%sql
WITH cte AS (
    SELECT spend_date, user_id, MAX(platform) as platform, SUM(amount) as amount
    FROM spending
    GROUP BY user_id, spend_date 
    HAVING COUNT(DISTINCT platform) = 1
    UNION ALL
    SELECT spend_date, user_id, 'Both' as platform, SUM(amount) as amount
    FROM spending
    GROUP BY user_id, spend_date 
    HAVING COUNT(DISTINCT platform) = 2)
SELECT spend_date, platform, SUM(amount) as total_amount, COUNT(DISTINCT user_id) as total_users FROM cte GROUP BY spend_date, platform ORDER BY spend_date, platform;

%python
# Single platform users
single_platform = spending_df.groupBy("spend_date", "user_id")\
    .agg(F.countDistinct("platform").alias("platform_count"), 
         F.sum("amount").alias("amount"))\
    .filter(F.col("platform_count") == 1)\
    .withColumn("platform", F.lit("single"))  # Will be updated with actual platform

# Both platform users
both_platform = spending_df.groupBy("spend_date", "user_id")\
    .agg(F.countDistinct("platform").alias("platform_count"), 
         F.sum("amount").alias("amount"))\
    .filter(F.col("platform_count") == 2)\
    .withColumn("platform", F.lit("Both"))

# Combine and get final result
result_df = single_platform.unionByName(both_platform)\
    .groupBy("spend_date", "platform")\
    .agg(F.sum("amount").alias("total_amount"), 
         F.countDistinct("user_id").alias("total_users"))\
    .orderBy("spend_date", "platform")

# First Login Device for Each Player

%sql
SELECT * FROM (
SELECT *, RANK() OVER(PARTITION BY player_id ORDER BY event_date) as rn FROM activity) WHERE rn = 1;

%python
window_spec = Window.partitionBy("player_id").orderBy("event_date")
result_df = activity_df.withColumn("rn", F.rank().over(window_spec))\
    .filter(F.col("rn") == 1)\
    .drop("rn")

# Cumulative Games Played

%sql
SELECT *, SUM(games_played) OVER(PARTITION BY player_id ORDER BY event_date) as total_played FROM activity;

%python
window_spec = Window.partitionBy("player_id").orderBy("event_date")
result_df = activity_df.withColumn("total_played", F.sum("games_played").over(window_spec))

# Second Item Favorite Brand Check

%sql
WITH ranked_orders AS (
SELECT *, RANK() OVER(PARTITION BY seller_id ORDER BY order_date ASC) as rn FROM orders)
SELECT u.user_id as seller_id, CASE WHEN i.item_brand = u.favorite_brand THEN 'Yes' ELSE 'No' END as item_fav_brand FROM users u
LEFT JOIN ranked_orders ro ON ro.seller_id = u.user_id AND rn = 2
LEFT JOIN items i ON i.item_id = ro.item_id;

%python
window_spec = Window.partitionBy("seller_id").orderBy("order_date")
ranked_orders = orders_df.withColumn("rn", F.rank().over(window_spec))

result_df = users_df.alias("u")\
    .join(ranked_orders.alias("ro"), 
          (F.col("u.user_id") == F.col("ro.seller_id")) & (F.col("ro.rn") == 2), 
          "left")\
    .join(items_df.alias("i"), F.col("ro.item_id") == F.col("i.item_id"), "left")\
    .select(F.col("u.user_id").alias("seller_id"),
            F.when(F.col("i.item_brand") == F.col("u.favorite_brand"), "Yes")
             .otherwise("No").alias("item_fav_brand"))

# Group Winner with Maximum Points

%sql
WITH player_scores AS (
    SELECT first_player as player_id, first_score as score FROM matches
    UNION ALL
    SELECT second_player as player_id, second_score as score FROM matches),
final_scores AS (
    SELECT p.group_id, ps.player_id, SUM(score) as score
    FROM player_scores ps
    INNER JOIN players p ON p.player_id = ps.player_id
    GROUP BY ps.player_id, p.group_id),
final_ranking AS (
SELECT *,RANK() OVER(PARTITION BY group_id ORDER BY score DESC, player_id ASC) as rn FROM final_scores)
SELECT * FROM final_ranking WHERE rn = 1;

%python
# Get all player scores
first_scores = matches_df.select(
    F.col("first_player").alias("player_id"),
    F.col("first_score").alias("score")
)
second_scores = matches_df.select(
    F.col("second_player").alias("player_id"),
    F.col("second_score").alias("score"))
all_scores = first_scores.union(second_scores)

# Calculate final scores
final_scores = all_scores.join(players_df, "player_id")\
    .groupBy("group_id", "player_id")\
    .agg(F.sum("score").alias("score"))

# Rank and get winners
window_spec = Window.partitionBy("group_id").orderBy(F.desc("score"), F.asc("player_id"))
result_df = final_scores.withColumn("rn", F.rank().over(window_spec))\
    .filter(F.col("rn") == 1)