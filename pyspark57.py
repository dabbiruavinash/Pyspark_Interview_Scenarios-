How do you identify dates where non-paying customers generated more downloads than paying customers using Spark SQL on top of PySpark DataFrames?

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.getOrCreate()
user_data = [
(1, 101),
(2, 102),
(3, 103),
(4, 104),
(5, 105)]

account_data = [
(101, 'Yes'),
(102, 'No'),
(103, 'Yes'),
(104, 'No'),
(105, 'No')]

downloads_data = [
('2024-10-01', 1, 10),
('2024-10-01', 2, 15),
('2024-10-02', 1, 8),
('2024-10-02', 3, 12),
('2024-10-02', 4, 20),
('2024-10-03', 2, 25),
('2024-10-03', 5, 18)]

user_df = spark.createDataFrame(user_data, ['user_id', 'acc_id'])
account_df = spark.createDataFrame(account_data, ['acc_id', 'paying_customer'])
downloads_df = spark.createDataFrame(downloads_data, ['download_date', 'user_id', 'downloads'])



Create Temp Views
user_df.createOrReplaceTempView("ms_user_dimension")
account_df.createOrReplaceTempView("ms_acc_dimension")
downloads_df.createOrReplaceTempView("ms_download_facts")

result = spark.sql("""
select d.download_date as date, sum(case when a.paying_customer = 'No' then d.downloads else 0 end) as non_paying_downloads,  sum(case when a.paying_customer = 'Yes' then d.downloads else 0 end) as paying_downloads from ms_download_facts d join ms_user_dimension u on d.user_id = u.user_id join ms_acc_dimension a on u.acc_id = a.acc_id group by d.download_date having sum(case when a.paying_customer = 'No' then d.downloads else 0 end) > sum(case when a.paying_customer = 'Yes' then d.downloads else 0 end) order by d.download_date asc """)
result.show()


How do you identify users who started a session and placed orders on the same day using PySpark?

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

sessions_data = [
(1, 1, '2024-01-01 00:00:00'),
(2, 2, '2024-01-02 00:00:00'),
(3, 3, '2024-01-05 00:00:00'),
(4, 3, '2024-01-05 00:00:00'),
(5, 4, '2024-01-03 00:00:00'),
(6, 4, '2024-01-03 00:00:00'),
(7, 5, '2024-01-04 00:00:00'),
(8, 5, '2024-01-04 00:00:00'),
(9, 3, '2024-01-05 00:00:00'),
(10,5, '2024-01-04 00:00:00')]

orders_data = [
(1, 1, 152, '2024-01-01 00:00:00'),
(2, 2, 485, '2024-01-02 00:00:00'),
(3, 3, 398, '2024-01-05 00:00:00'),
(4, 3, 320, '2024-01-05 00:00:00'),
(5, 4, 156, '2024-01-03 00:00:00'),
(6, 4, 121, '2024-01-03 00:00:00'),
(7, 5, 238, '2024-01-04 00:00:00'),
(8, 5, 70, '2024-01-04 00:00:00'),
(9, 3, 152, '2024-01-05 00:00:00'),
(10,5, 171, '2024-01-04 00:00:00')]

%python
from pyspark.sql.function import *

sessions_df = session_df.withColumn("session_date", to_date("session_date"))
orders_df = spark.createDataFrame(orders_date, ["order_id", "user_id", "order_value", "order_date"]).withColumn("order_date", to_date("order_date"))
result_df = sessions_df.alias("s").join(order_df.alias("o"), (col("s.user_id") == col("o.user_id")) & (col("s.session_date") == col("o.order_date")), "inner").groupBy(col("s.user_id"), col("s.session_date")).agg(count("o.order_id").alias('total_orders"), sum("o.order_value").alias("total_order_value")).orderBy("user_id")
result_df.show()

Can you identify the highest-paid employee job titles using PySpark?

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.getOrCreate()
worker_data = [
(1, 'John', 'Doe', 80000, 'Engineering'),
(2, 'Jane', 'Smith', 120000, 'Marketing'),
(3, 'Alice', 'Brown', 120000, 'Sales'),
(4, 'Bob', 'Davis', 75000, 'Engineering'),
(5, 'Charlie', 'Miller', 95000, 'Sales')]

title_data = [
(1, 'Engineer'),
(2, 'Marketing Manager'),
(3, 'Sales Manager'),
(4, 'Junior Engineer'),
(5, 'Senior Salesperson')]

worker_df = spark.createDataFrame(worker_data, ["worker_id","first_name","last_name","salary","department"])

title_df = spark.createDataFrame(title_data,["worker_ref_id","worker_title"])

max_salary = worker_df.agg(max("salary").alias("max_sal"))
joined_df = worker_df.join(title_df, worker_df.worker_id == title_df.worker_ref_id, "inner")
result = joined_df.join(max_salary, joined_df.salary == max_salary.max_sal, "inner")
result.select("worker_id", "first_name", "salary", "worker_title").show(truncate = False)

How do you detect high-risk customers efficiently using optimized Spark transformations?

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

data = [
(1, 'C1', '2022-12-01 10:00:00'),
(2, 'C1', '2022-12-01 10:05:00'),
(3, 'C1', '2022-12-01 10:07:00'),
(4, 'C1', '2022-12-01 10:09:00'),
(5, 'C2', '2022-12-01 11:00:00'),
(6, 'C2', '2022-12-01 11:20:00'),
(7, 'C3', '2022-12-01 09:00:00'),
(8, 'C3', '2022-12-01 09:03:00'),
(9, 'C3', '2022-12-01 09:06:00'),
(10,'C3', '2022-12-01 09:09:00')
]
df = spark.createDataFrame(data, ["txn_id","customer_id","txn_time"])
df = df.withColumn("txn_time", to_timestamp("txn_time"))

%python
from pyspark.sql.window import Window

df_opt = df.repartition("customer_id")
df_opt = df_opt.withColumn("txn_unix", col("txn_time").cast("long"))
win = Window.partitionBy("customer_id").orderBy("txn_unix").rangeBetween(-600, 0)
df_flag = df_opt.withColumn("txn_count_10min", count("*").over(win))
result = df_flag.filter(col("txn_count_10min") > 3)
result.select("customer_id").distinct().show()

Can you detect invalid banking transactions based on business hours and holidays?

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

data=[(1051,'2022-12-03 10:15'),(1052,'2022-12-03 17:00'),(1053,'2022-12-04 10:00'),(1054,'2022-12-04 14:00'),(1055,'2022-12-05 08:59'),(1056,'2022-12-05 16:01'),(1057,'2022-12-06 09:00'),(1058,'2022-12-06 15:59'),(1059,'2022-12-07 12:00'),(1060,'2022-12-08 09:00'),(1061,'2022-12-09 10:00'),(1062,'2022-12-10 11:00'),(1063,'2022-12-10 17:30'),(1064,'2022-12-11 12:00'),(1065,'2022-12-12 08:59'),(1066,'2022-12-12 16:01'),(1067,'2022-12-25 10:00'),(1068,'2022-12-25 15:00'),(1069,'2022-12-26 09:00'),(1070,'2022-12-26 14:00'),(1071,'2022-12-26 16:30'),(1072,'2022-12-27 09:00'),(1073,'2022-12-28 08:30'),(1074,'2022-12-29 16:15'),(1075,'2022-12-30 14:00'),(1076,'2022-12-31 10:00')]

df=spark.createDataFrame(data,["transaction_id","time_stamp"]).withColumn("time_stamp",to_timestamp("time_stamp"))

%python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

df_filtered = df.filter(month("time_stamp") == 12) & (year("time_stamp") == 2022))
invalid_txns = df_filtered.filter(dayofweek").isin(1,7)) | (date_format("time_stamp", "HH:mm:ss") < "09:00:00") | (date_format("time_stamp", "HH:mm:ss") > "16:00:00") | ((dayofmonth("time_stamp").isin(25,26)) & (month("time_stamp") == 12)))
invalid_txns.select("transaction_id").show()

“Find days where receivers are more than senders”

data = [
 (0, 'A', 'X', 10),
 (1, 'A', 'Y', 6),
 (2, 'A', 'Z', 10),
 (3, 'A', 'P', 6),
 (4, 'A', 'Q', 1),
 (5, 'A', 'R', 6),
 (6, 'A', 'S', 8),
 (7, 'A', 'T', 1),
 (8, 'A', 'U', 1),
 (9, 'A', 'V', 3),
 (10, 'A', 'W', 2),
 (11, 'A', 'A', 8),
 (12, 'A', 'B', 9),
 (13, 'A', 'C', 5),
 (14, 'A', 'D', 2),
 (15, 'X', 'X', 6),
 (16, 'X', 'V', 8),
 (17, 'X', 'Z', 3),
 (18, 'X', 'Q', 10),
 (19, 'X', 'W', 10),
 (20, 'X', 'T', 7)
]

columns = ["id", "from_user", "to_user", "day"]

df = spark.read.format("parquet").option("inferSchema", True).load("path_to_data")

from pyspark.sql import functions as F

df = spark.createDataFrame(data, columns)
agg_df = df.groupBy("day").agg(F.countDistinct("to_user").alias("distinct_receivers"), F.countDistinct("from_user").alias("distinct_senders"))
filtered_days = agg_df.filter(F.col("distinct_receiver") > F.col("distinct_senders")).select("day")
result_df = df.join(filtered_day, on = "day", how = "inner")
result_df.show()

Calculate the cumulative (running) sales revenue for each product based on date.

data = [
 ("product1", "2023-12-01", 100),
 ("product2", "2023-12-02", 200),
 ("product1", "2023-12-03", 150),
 ("product2", "2023-12-04", 250),
 ("product1", "2023-12-05", 300),
 ("product2", "2023-12-06", 100)]

columns = ["product_id", "date", "sales"]

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, to_date
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame(data, columns)
df = df.withColumn("date", to_date("date")))
window_spec = Window.partitionBy("product_id").orderBy("date")
result_df = df.withColumn("cumulative_sales", sum("sales").over(window_spec))
result_df.show()

Find the number of rows for each review score earned by "Hotel Arena".

data = [
 ('Hotel Arena', '2024-01-01', 8.0),
 ('Hotel Arena', '2024-01-03', 9.0),
 ('Hotel Arena', '2024-01-03', 9.0), ← tie: same score, same date
 ('Hotel Arena', '2024-01-02', 6.0),
 ('Grand Palace', '2024-01-04', 8.0),
 ('Grand Palace', '2024-01-06', 9.0),]
columns = ["hotel_name", "review_date", "reviewer_score"]

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dense_rank, count
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("hotel_reviews").getOrCreate()

df = spark.createDataFrame(data, columns)
filtered_df = df.filter(col("hotel_name") == "Hotel Arena")
window_spec = Window.parititonBy("reveiwer_score").orderBy(col("review_date").desc())
df_ranked = filtered_df.withColumn("rank", dense_rank().over(window_spec))
latest_df = df_ranked.filtered(col("rank") == 1)
result = latest_df.groupBy("hotel_name", "reviewer_score").agg(count("*").alias("review_count").orderBy("reviewer_score")
result.show()


