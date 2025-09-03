# Find Duplicate Records

%sql
SELECT column1, column2, column3, COUNT(*) FROM table_name GROUP BY column1, column2, column3 HAVING COUNT(*) > 1;

%python
duplicates = (df
    .groupBy("column1", "column2", "column3")
    .agg(count("*").alias("count"))
    .filter(col("count") > 1))

# Top N Records per Group

%sql
SELECT * FROM (
SELECT column1, column2, category_column, value_column, ROW_NUMBER() OVER (PARTITION BY category_column ORDER BY value_column DESC) AS rank FROM table_name) ranked WHERE rank <= 5; 

%python
python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("CommonQueries").getOrCreate()

# Query 1: Find Duplicate Records
duplicates = (df
    .groupBy("column1", "column2", "column3")
    .agg(count("*").alias("count"))
    .filter(col("count") > 1))

# Query 2: Top N Records per Group
window_spec = Window.partitionBy("category_column").orderBy(col("value_column").desc())
top_n = (df
    .withColumn("rank", row_number().over(window_spec))
    .filter(col("rank") <= 5)  # Replace 5 with desired N
    .drop("rank"))

# Detect Missing Data

%sql
WITH all_ids AS (
SELECT MIN(id) + LEVEL - 1 AS expected_id FROM table_name CONNECT BY LEVEL <= (SELECT MAX(id) - MIN(id) + 1 FROM table_name))
SELECT a.expected_id AS missing_id FROM all_ids a LEFT JOIN table_name t ON a.expected_id = t.id WHERE t.id IS NULL;

%python
min_id = df.agg(min("id")).first()[0]
max_id = df.agg(max("id")).first()[0]

all_ids = spark.range(min_id, max_id + 1).toDF("expected_id")
missing_ids = (all_ids
    .join(df, all_ids.expected_id == df.id, "left_anti")
    .select(col("expected_id").alias("missing_id")))

# Find Second Highest Value

%sql
SELECT MAX(column_name) AS second_highest FROM table_name WHERE column_name < (SELECT MAX(column_name) FROM table_name);

%python
max_value = df.agg(max("column_name")).first()[0]
second_highest = (df.filter(col("column_name") < max_value).agg(max("column_name").alias("second_highest")))

# Pivot Data

%sql
SELECT category,
       SUM(CASE WHEN year = '2023' THEN value ELSE 0 END) AS "2023",
       SUM(CASE WHEN year = '2022' THEN value ELSE 0 END) AS "2022",
       SUM(CASE WHEN year = '2021' THEN value ELSE 0 END) AS "2021" FROM table_name GROUP BY category;

%python
pivoted_df = (df
    .groupBy("category")
    .pivot("year")
    .agg(sum("value").alias("total_value"))
    .fillna(0))

# Calculate Running Totals

%sql
SELECT column1, date_column, value_column, SUM(value_column) OVER (PARTITION BY category_column ORDER BY date_column) AS running_total FROM table_name;

%python
window_spec = Window.partitionBy("category_column").orderBy("date_column")
running_totals = df.withColumn("running_total", sum("value_column").over(window_spec))

# Retrieve Employees by Hierarchy

%sql
WITH EmployeeCTE (id, name, manager_id, level, path) AS (
    SELECT id, name, manager_id, 1 AS level, CAST(name AS VARCHAR2(4000)) AS path
    FROM employees
    WHERE manager_id IS NULL
    UNION ALL
    SELECT e.id, e.name, e.manager_id, c.level + 1, 
           c.path || ' -> ' || e.name
    FROM employees e
    INNER JOIN EmployeeCTE c ON e.manager_id = c.id)
SELECT id, name, manager_id, level, path FROM EmployeeCTE ORDER BY path;

# Complex Joins

%sql
SELECT t1.*, t2.*, t3.* FROM table1 t1 
INNER JOIN table2 t2 ON t1.id = t2.foreign_id
LEFT JOIN table3 t3 ON t2.id = t3.foreign_id WHERE t1.some_condition = 'value';

%python
complex_join = (df1
    .join(df2, df1.id == df2.foreign_id, "inner")
    .join(df3, df2.id == df3.foreign_id, "left")
    .filter(col("some_condition") == "value"))

# Percentage Contribution

%sql
SELECT category, region, sales_amount, ROUND((sales_amount * 100.0 / SUM(sales_amount) OVER()), 2) AS percentage_contribution FROM sales_data;

%python
total_sales = df.agg(sum("sales_amount")).first()[0]
percentage_contribution = (df
    .withColumn("percentage", round((col("sales_amount") * 100 / total_sales), 2)))

# Alternative with window function:
window_spec = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
percentage_window = (df
    .withColumn("total_sales", sum("sales_amount").over(window_spec))
    .withColumn("percentage", round((col("sales_amount") * 100 / col("total_sales")), 2))
    .drop("total_sales"))

# Identify Overlapping Ranges

%sql
SELECT t1.* FROM reservations t1 JOIN reservations t2 ON t1.id != t2.id WHERE t1.start_date < t2.end_date AND t1.end_date > t2.start_date AND t1.resource_id = t2.resource_id;

%python
overlapping = (df.alias("t1")
    .join(df.alias("t2"), 
          (col("t1.id") != col("t2.id")) & 
          (col("t1.start_date") < col("t2.end_date")) & 
          (col("t1.end_date") > col("t2.start_date")))
    .select("t1.*"))
