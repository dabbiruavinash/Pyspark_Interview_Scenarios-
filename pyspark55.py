Find Orders From Last 7 Days

%sql
select * from orders where orderdate >= current_date - interval 7 day;

%python
from pyspark.sql.functions import current_date, expr

result = df.filter(col("order_date") >= expr("current_date() - interval 7 days"))
result.show()

Calculate total sales for the current month

%sql
select sum(amount) as total_sales from orders where order_date >= date_format(current_date, '%y-%m-%01') and order_date < date_format(current_date + interval 1 month, '%y-%m-01');

%python
from pyspark.sql.functions import sum, date_format, current_date

current_month = date_format(current_date(), "yyyy-MM")
result = df.filter(date_format("order_date", "yyyy-MM") == current_month) \
           .agg(sum("amount").alias("total_sales"))

result.show()

Calculate Monthly Sales Growth Compared To Previous Month

%sql
select month, total_sales, lag(total_sales) over (order by month) as previous_month_sales, total_sales - lag(total_sales) over (order by month) as sales_growth from (
select date_format(order_date, '%y-%m') as month, sum(amount) as total_sales from orders group by date_format(order_date, '%y-%m')t;

%python
from pyspark.sql import Window
from pyspark.sql.functions import sum, lag, col, date_format, round

monthly = df.groupBy(date_format("order_date", "yyyy-MM").alias("month")) \
            .agg(sum("amount").alias("total_sales"))

window_spec = Window.orderBy("month")
result = monthly.withColumn("prev_month_sales", lag("total_sales").over(window_spec)) \
                .withColumn("growth_percent", 
                    round((col("total_sales") - col("prev_month_sales")) / 
                          col("prev_month_sales") * 100, 2))

result.show()

Calculate total sales from beginning of current year till today

%sql
select sum(amount) as ytd_sales from orders where order_date >= date_format(current_date, '%y-01-01');

%python
from pyspark.sql.functions import sum, expr, current_date, trunc

result = df.filter(col("order_date") >= trunc(current_date(), "YYYY")) \
           .agg(sum("amount").alias("sales_ytd"))

result.show()

Find all orders placed on weekends

%sql
select * from orders where dayofweek(order_date) in (1,7);

%python
from pyspark.sql.functions import date_format

result = df.filter(date_format("order_date", "E").isin("Sat", "Sun"))
result.show()

Find all orders placed on weekdays

%sql
select * from orders where dayofweek(order_date) between 2 and 6;

%python
from pyspark.sql.functions import date_format

result = df.filter(~date_format("order_date", "E").isin("Sat", "Sun"))
result.show()

Compare total sales between weekends and weekdays

%sql
select case when dayofweek(order_date) in (1,7) then 'weekend' else 'weekday' end as day_type, sum(amount) as total_sales from orders group by case when dayofweek(order_date) in (1,7) then 'weekend' else 'weekday' end;

%python
from pyspark.sql.functions import when, sum, count, avg, round, date_format

result = df.withColumn("day_type", 
    when(date_format("order_date", "E").isin("Sat", "Sun"), "Weekend")
    .otherwise("Weekday")).groupBy("day_type").agg(
    sum("amount").alias("total_sales"),
    count("*").alias("order_count"),
    round(avg("amount"), 2).alias("avg_order_value"))

result.show()

Calculate total sales for each day

%sql
select date(order_date) as order_day, sum(amount) as daily_sales from orders group by date(order_date) order by order_day;

%python
result = df.groupBy("order_date").agg(sum("amount").alias("daily_sales"), count("*").alias("order_count")).orderBy("order_date")

result.show()

Compare current day sales with previous day sales

%sql
select order_day, total_sales, lag(total_sales) over (order by order_day) as previous_day_sales, total_sales - lag(total_sales) over (order by order_day) as sales_difference from (
select date(order_date) as order_day, sum(amount) as total_sales from orders group by date(order_date)t;

%python
from pyspark.sql import Window
from pyspark.sql.functions import sum, lag, round

daily = df.groupBy("order_date").agg(sum("amount").alias("daily_sales"))

window_spec = Window.orderBy("order_date")
result = daily.withColumn("prev_day_sales", lag("daily_sales").over(window_spec)) \
              .withColumn("sales_change", col("daily_sales") - col("prev_day_sales")) \
              .withColumn("change_percent", 
                  round((col("daily_sales") - col("prev_day_sales")) / 
                        col("prev_day_sales") * 100, 2))

result.show()

Find the month having highest total sales

%sql
select month, total_sales from (
select month, total_sales, row_number() over (order by total_sales) desc) as rn from (
select date_format(order_date, '%y-%m') as month, sum(amount) as total_sales from orders group by date_format(order_date, '%y-%m')x)t where rn = 1;

%python
from pyspark.sql.functions import sum, date_format, desc

result = df.groupBy(date_format("order_date", "yyyy-MM").alias("month")) \
           .agg(sum("amount").alias("total_sales")) \
           .orderBy(desc("total_sales")) \
           .limit(1)

result.show()