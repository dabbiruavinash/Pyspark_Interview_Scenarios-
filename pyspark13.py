# Write a Query to give me the date when the temperature is higher than the previous day.

Data Set: (id int, recorddate varchar(50), temperature int);

%sql
with cte as (
select *, lag(temperature) over (order by recorddate) as prev_temperature from weather)
select recorddate, temperature, prev_temperature from cte where temperature > prev_temperature;

%python
df = spark.read.table("weather")
window_specified = Window.orderBy(col("recorddate").asc()
df = df.withColumn("prev_temp", lag('temperature").over(window_specified))
result_df = df.filter(col("temperature") > col("prev_temp")).select("recorddate","temperature","prev_temp")
result_df.display()

# Write Query to find new and repeat customers for each day

Data Set: customerorders (order_id integer, customer_id integer, order_date date, order_amount integer);

%sql
select *, min(order_date) over(partition by customer_id) as first_order_date from customerorders)
select order_date, sum(case when order_date = first_order_date then 1 else 0 end) as new_customers, sum(case when order_date <> first_order_date then 1 else 0 end) as repeat_customers form cte group by order_date order by order_date;

%python
df = df.withColumn('first_order_date', min('order_date').over(Window.partitionBy('customer_id')))
df = df.withColumn('new', when(col('order_date') == col('first_order_date'),1).otherwise(0)).withColumn('repeat', when(col('order_date') != col('first_order_date'),1).otherwise(0))
df_result = df.groupBy('order_date').agg(sum('new').alias('Newcustomers'), sum('repeat').alias('Repeat_customers')).orderBy('order_date')
df_result.display()

# Task: Analyze call activity in Q4 2025 (Oct–Dec) using CTEs to: Calculate total call duration per customer.
Identify customers with high usage (total duration > 100 minutes). Rank customers by total call duration within each plan_type (prepaid/postpaid). Include customer_name and plan_type from the customers table.
Handle: Customers with no calls in Q4 2025. Missing or duplicate call records. NULL call durations.
Return: customer_id, customer_name, plan_type, total_call_duration, duration_rank (within plan_type), ordered by plan_type, duration_rank. Use minimal data (4 call logs, 3 customers), filter for Q4 2025.

Data : call_logs (call_id VARCHAR(50) ,customer_id INT,call_start_time DATE,call_duration_minutes INT);
customers (customer_id INT ,customer_name VARCHAR(100),plan_type VARCHAR(50));

%sql
with call_summary as (
select l.customer_id, sum(coalesce(l.call_duration_minutes,0)) as total_call_duration from (
select distinct call_id,customer_id,call_duration_minutes, call_start_time from call_logs where call_start_time >= '2025-10-01' and call_start_time < '2026-01-01') group by l.customer_id),
customer_base as (
select c.customer_id, c.customer_name, c.plan_type,coalesce(cs.total_call_duration,0) as total_call_duration from customer c left join call_summary cs on c.customer_id = cs.customer_id),
ranked_customers as (
select cb.*,dense_rank() over (partition by cb.plan_type order by cb.total_call_duration desc) as duration_rank from customer_base cb)
select customer_id, customer_name, plan_type, total_call_duration, duration_rank from ranked_customers where total_call_duration > 100 order by plan_type, duration_rank;

%python
call_logs_df = spark.read.table('call_logs')
customers_df = spark.read.table('customers')
q4_calls_df = calls_logs_df.filter((col("call_start_time") >= "2025-10-01") & (col("call_start_time") < "2026-01-01")).dropDuplicates(["call_id"])
call_summary_df = q4_calls_df.groupBy("cusotmer_id").agg(F.sum(F.coalesce('call_duration_minutes", F.lit(0))).alias("total_call_duration"))
customer_base_df = customer_df.join(call_summary_df, on = "customer_id", how = "left").withColumn("total_call_duration", F.coalesce("total_call_duration", F.lit(0)))
window_spec = Window.partitionBy("plan_type").orderBy(F.desc("total_call_duration"))
ranked_customer_df = customer_base_df.withColumn("duration_rank", F.dense_rank().over(window_spec))
final_df = ranked_customer_df.filter(col("total_call_duration") > 100).select("customer_id", "customer_name", "plan_type","total_call_duration","duration_rank").orderBy("plan_type","duration_rank")
final_df.show()

# Write a Query to identify companies where revenue never decreased year-over-year. 
Data Set : company_revenue (company varchar(100), year int, revenue int);

%sql
select *, revenue-lag(revenue, 1, revenue) over (partition by company order by year asc) flg from company_revenue),
cte2 as (
selct company, year, revenue, count(*) over (partition by company) as count, sum(case when flg >= 0 then 1 else 0 end) over (partition by company) as flg from cte)
select company from cte2 where count = flg group by company;

%python
df = spark.read.table('company_revenue')
window_spec = Window.partitionBy('company').orderBy(col('year').asc())
df = df.withColumn('revenue_diff', coalesce(col('revenue') - lag('revenue').over(window_spec), lit(0)))
df = df.withColumn('total_years', count(*).over(company_window)).withColumn('non_decreasing_years', sum(when(col('revenue_diff') > 0, 1).otherwise(0)).over(company_window))
result_df = df.withColumn(col('non_decreasing_years') == col('total_years')).select('company').distinct()
result_df.display()

# Write a Query to print highest and lowest sal emp in each department.
Data Set : employe (emp_name varchar(10),dep_id int,salary int);

%sql
with cte as (
select *, max(salary) over(partition by dep_id) as max_salary, min(salary) over(partition by dep_id) as min_salary from employee)
select dep_id, min(case when salary = max_salary then emp_name end) as emp_name_max_salary, min(case when salary = min_salary then emp_name end) as emp_name_min_salary from cte group by dep_id;

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import max,min,col

df = spark.read.table('employee')
window_spec = Window.partitionBy('dep_id')
df2 = df.withColumn('max_salary', max('salary').over(window_spec)).withColumn('min_salary', min('salary').over(window_spec))
max_salary_df = df2.filter(col('salary') == col('max_salary')).select('dep_id', col('emp_name').alias('emp_name_max_salary'))
min_salary_df = df2.filter(col('salary') == col('min_salary')).select('dep_id', col('emp_name').alias('emp_name_min_salary'))
result = max_salary_df.join(min_salary_df, on = 'dep_id', how = 'inner')
result.show()

# Write Solution to swap seat id of the every two consecutive students.
if the number of students are odd, the id of last student is not swapped.
Data set: seats (id INT, student VARCHAR(10));

%sql
select id, student, case when id % 2 = 0 the lag(id) over(order by id) when id % 2 = 1 and lead(id) over(order by id) is null then id else lead(id) over (order by id) end swap_id from seats;

%python
from pyspark.sql.functions import lead, lag, col, when
from pyspark.sql.window import Window

df = spark.read.table('seats')
window_spec = Window.orderBy('id')
result_df = df.withColumn('swap_id', when(col('id') % 2 == 0).over(window_spec)).when((col('id') % 2 == 1) & (lead('id').over(window_spec).isNull()), col('id')).otherwise(lead('id').over(window_spec)))
result_df.show()

# Find the total number of people present inside hospital.
Data set: hospital ( emp_id int,action varchar(10),time timestamp);

%sql
select emp_id, max(case when action = 'in' then time end) as hospital_in, max(case when action = 'out' then time end) as Hospital_out from hospital group by emp_id)
select count(*) as total_members from cte where hospital_in > hospital_out or hosptial_out is null;

%python
from pyspark.sql.functions import col, when, count, max

df = spark.read.table('hospital')
df2 = df.withColumn('hospital_in', when(col('action = 'in', col('time'))).withColumn('hospital_out', when (col('action') == 'out', col('time')))
df3 = df2.groupBy('emp_id').agg(max(col('hospital_in')).alias('in_time'), max(col('hospital_out')).alias('out_time'))
filtered_df = df3.filter((col('in_time') > col('out_time')) | col('out_time').isNull())
result_df = filtered_df.agg(count('*').alias('total_members'))
result_df.show()

# Continuing my series on converting SQL queries to PySpark, today I’m tackling window functions — essential for running calculations across partitions of data without collapsing rows.
Question : for each lift, which passengers can board together without exceeding the lift’s weight capacity.
Dataset: lift(id int primary key,capacity_kg int);
lift_passengers(passenger_id varchar(50),weight_kg int,lift_id int);

%sql
SELECT lift_id, capacity_kg, STRING_AGG(passenger_id, ',') WITHIN GROUP (ORDER BY passenger_id) AS passengers_name FROM (
SELECT *,SUM(weight_kg) OVER (PARTITION BY lift_id ORDER BY weight_kg ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_weight FROM lift_passengers lp JOIN lift l ON lp.lift_id = l.id) a WHERE a.running_weight <= a.capacity_kg GROUP BY lift_id, capacity_kg;

%python
passengers_df=spark.read.table(lift_passengers)
lift_df=spark.read.table(lift)
joined_df = passengers_df.join(lift_df, passengers_df.lift_id == lift_df.id, "inner").select("passenger_id", "weight_kg", "lift_id", "capacity_kg")
window_spec = Window.partitionBy("lift_id").orderBy("weight_kg").rowsBetween(Window.unboundedPreceding, Window.currentRow)
running_sum_df = joined_df.withColumn("running_weight", spark_sum("weight_kg").over(window_spec))
filtered_df = running_sum_df.filter(col("running_weight") <= col("capacity_kg"))
result_df = filtered_df.groupBy("lift_id", "capacity_kg").agg(array_join(expr("collect_list(passenger_id)"), ",").alias("passengers_name")).orderBy("lift_id")
result_df.show(truncate=False)

# Find the top seller–buyer combinations with the highest number of transactions.
Disqualify any seller who has ever acted as a buyer, and any buyer who has ever acted as a seller.
Dataset: transactions (transaction_id INT, customer_id INT, amount INT, tran_Date TIMESTAMP);

%sql
with cte1 as(
select transaction_id,amount,tran_date,customer_id seller_id, lead(customer_id,1)over(partition by amount, tran_date order by transaction_id asc) as buyer_id from transactions order by transaction_id),
trasactions_count as(
select seller_id,buyer_id, count(*) as trasactions from cte1 where  buyer_id is not null group by seller_id,buyer_id),
frauds as(
select seller_id as fraud_id from trasactions_count intersect select buyer_id from trasactions_count )
select * from trasactions where seller_id not in (select fraud_id from frauds) and  buyer_id not in (select fraud_id from frauds)

%python
df = spark.read.table('transactions')

window_specified = Window.partitionBy('amount','tran_date').orderBy('transaction_id')
buyer_seller_details = df.withColumn('seller_id', col('customer_id'))
.withColumn('buyer_id', lead('customer_id').over(window_specified))
.select('transaction_id', 'amount', tran_date', 'seller_id', 'buyer_id')
.filter(col('buyer_id').isNotNull())

Transactions_count = buyer_seller_details.groupBy('seller_id','buyer_id').agg(count('*').alias('no_of_transactions'))
frauds = Transactions_count.select('seller_id').intersect(Transactions_count.select('buyer_id')).withColumnRenamed('seller_id','fraud_id')
result = Transactions_count.join(frauds, Transactions_count.buyer_id == frauds.fraud_id, how = 'left_anti')
.join(frauds, Transactions_count.seller_id == fraud.fraud_id, how = 'left_anti')
result.show()

# Identify employees who share the same salary in the same department.
Dataset: emp_salary (emp_id INTEGER NOT NULL,name VARCHAR(20) NOT NULL,salary VARCHAR(30),dept_id INTEGER);

%sql
WITH cte AS (
SELECT dept_id, salary FROM emp_salary GROUP BY dept_id, salary HAVING COUNT(*) > 1)
SELECT e1.*FROM emp_salary e1 JOIN cte c ON c.dept_id = e1.dept_id AND e1.salary = c.salary ORDER BY dept_id;

%python
from pyspark.sql.functions import count

df_emp = spark.read.table('emp_salary')
sal_dep = df_emp.groupBy("dept_id", "salary").agg(count("*").alias("cnt")).filter("cnt > 1")
result = df_emp.join(sal_dep,(df_emp.dept_id == sal_dep.dept_id) & (df_emp.salary == sal_dep.salary),'inner')
result.orderBy("dept_id").show()

# How do you identify continuous periods where a balance remains unchanged across consecutive dates?

%sql
with cte as(
select *, row_number()over(order by txn_date) -dense_rank()over(partition by balance order by txn_date) as diff_flag from test )
select balance,min(txn_date) as s_date, max(txn_date) as e_date from cte group by diff_flag,balance

%python
from pyspark.sql.functions import min,max,row_number,dense_rank,col
from pyspark.sql.window import Window
window_row= Window.orderBy(col('txn_date'))
window_dense=Window.partitionBy(col('balance')).orderBy(col('txn_date'))
Grouped_Df=df.select('*',(row_number().over(window_row)-dense_rank().over(window_dense)).alias('diff_flag'))
resultDf=Grouped_Df.groupBy('balance', 'diff_flag').agg(min('txn_date').alias('s_date'), max('txn_date').alias('e_date')).select('balance','s_date','e_date')
resultDf.display()

# Transform monthly sales data of products across different regions into a pivoted format where each month becomes a separate column, displaying total sales per product.
Dataset: sales_data(product_id VARCHAR(10),region VARCHAR(20),month VARCHAR(10),sales_amount INT);

%sql
select product_id, sum(case when month='Jan' then sales_amount else 0 end) as Jan , sum(case when month='Feb' then sales_amount else 0 end) as Feb from sales group by product_id order by product_id;

%python
df.groupBy('product_id').pivot('month', ['Jan', 'Feb']).sum('sales_amount').fillna(0).orderBy('product_id').display()

# Given a sales transactions table, we need to find the total sales for each product and rank them based on their total sales, with the highest sales getting the highest rank. In case of a tie in sales, products should share the same rank.

Dataset: sales(transaction_id INT PRIMARY KEY,product_id INT,product_name VARCHAR(50),sales_amount DECIMAL(10, 2),transaction_date DATE);

%sql
WITH ranked_sales AS (
SELECT product_name, SUM(sales_amount) AS total_sales, DENSE_RANK() OVER (ORDER BY SUM(sales_amount) DESC) AS sales_rank FROM sales GROUP BY product_name)
SELECT * FROM ranked_sales;

%python
df = spark.read.table('sales')
df_grouped = df.groupBy("product_name").agg(sum("sales_amount").alias("total_sales"))
window_spec = Window.orderBy(col("total_sales").desc())
df_ranked = df_grouped.withColumn("sales_rank", dense_rank().over(window_spec))
df_ranked.show()

# We have two tables — lift and lift_passengers. Each lift has a weight limit, and each passenger has a weight. We want to:
 ✔️ Load passengers into lifts in ascending weight order
 ✔️ Only allow passengers until the total weight stays within the lift's capacity
 ✔️ Return the list of allowed passenger names per lift using string aggregation

Dataset : lift( id int primary key, capacity_kg int);
lift_passengers( passenger_id varchar(50), weight_kg int,lift_id int);

SELECT lift_id, capacity_kg, STRING_AGG(passenger_id, ',') WITHIN GROUP (ORDER BY passenger_id) AS passengers_name FROM (
SELECT *, SUM(weight_kg) OVER (PARTITION BY lift_id ORDER BY weight_kg ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_weight FROM lift_passengers lp JOIN lift l ON lp.lift_id = l.id) a WHERE running_weight <= capacity_kg GROUP BY lift_id, capacity_kg;

%python
from pyspark.sql.functions import sum as _sum, concat_ws, collect_list, col
from pyspark.sql.window import Window

df1 = spark.read.table('lift')
df2 = spark.read.table('lift_passengers')

window_spec = Window.partitionBy('lift_id').orderBy('weight_kg').rowsBetween(Window.unboundedPreceding, Window.currentRow)
joined_df = df2.join(df1, df2.lift_id == df1.id, 'inner').withColumn('running_weight', _sum('weight_kg').over(window_spec))
filtered_df = joined_df.filter(col('running_weight') <= col('capacity_kg'))
result = filtered_df.orderBy('passenger_id').groupBy('lift_id', 'capacity_kg').agg(concat_ws(',', collect_list('passenger_id')).alias('passengers_name'))
result.show()

# Find the number of gold medals per swimmer — but only for swimmers who never won silver or bronze.

Dataset : events (ID INT,EVENT VARCHAR(255),YEAR INT,GOLD VARCHAR(255),SILVER VARCHAR(255),BRONZE VARCHAR(255));

%sql
WITH cte AS (
SELECT gold FROM events eWHERE NOT EXISTS (
SELECT 1 FROM events s WHERE e.gold = s.silver OR e.gold = s.bronze))
SELECT gold, COUNT(*) AS no_of_gold_medals FROM cte GROUP BY gold;

%python
df=spark.read.table('events')
gold_df = df.select("GOLD")
silver_df = df.select("SILVER")
bronze_df = df.select("BRONZE")
non_gold_df = silver_df.union(bronze_df).withColumnRenamed("SILVER", "nonGold")
gold_only_df = gold_df.join(non_gold_df, gold_df.GOLD == non_gold_df.nonGold, "left_anti")
result = gold_only_df.groupBy("GOLD").agg(count("*").alias("no_of_gold_medals"))
result.show()

# Write a Query to find all the consecutive available seats order by seat id.
1 means free , 0 means occupied
Data Set : cinema (seat_id INT ,free int);

%sql
select seat_id, free, seat_id - row_number() over(order by seat_id asc) as flg from cinema where free=1)
select seat_id from (
select seat_id, count(*) over(partition by flg) as count from cte) t where t.count >=2;

%python
df = spark.read.table('cinema')
df = df.filter(col('free') == 1).select('seat_id', (col('seat_id') - row_number().over(Window.orderBy(col('seat_id').asc()))).alias('flag'))
result_df = df.select('seat_id', count(*).over(Window.partitionBy('flag')).alias('cnt'))
result_df.filter(col('cnt') >= 2).select('seat_id').display()

# Find out Delivery Partner wise delayed orders count(delay means - the order which took more than predicted time to deliver the order)

Data Set : swiggy_orderss (orderid INT ,custid INT,city VARCHAR(50),del_partner VARCHAR(50),order_time TIMESTAMP,deliver_time TIMESTAMP,predicted_time INT -- Predicted delivery time in minutes);

%sql
with cte as(
SELECT del_partner,predicted_time,TIMESTAMPDIFF(MINUTE,order_time, deliver_time) AS actual_time FROM swiggy_orderss),
distinct_partners as (
select distinct del_partner from cte),
delayed_orders as(
select del_partner,count(*) delayed_orders from cte where actual_time>predicted_time group by del_partner)
select dp.del_partner,coalesce(do.delayed_orders,0 ) as delayed_orders from delayed_orders do right join distinct_partners dp on do.del_partner=dp.del_partner order by dp.del_partner;

%python
df = spark.read.table('swiggy_orders')
df = (df.withColumn("diff_seconds", unix_timestamp(col("deliver_time")) - unix_timestamp(col("order_time"))).withColumn("actual_time", (col("diff_seconds")/60).cast("long"))).select('del_partner','predicted_time','actual_time')
df.cache()
df.show()
disintct_parterns = df.dropDuplicates(subset = ['del_partner']).select('del_partner')
filter_df = df.filter(col('actual_time') > col('predicated_time'))
result_df = filter_df.groupBy('del_partner').agg(count('*').alias('delayed_orders'))
result_df = disitnct_partners.alias('dp').join(result_df.alias('rf'), col('dp.del_partner') == col('rf.del_partner'), 'left').select(col('dp.del_partner'), coalesce('delayed_orders', lit(0)).alias('delayed_orders')).orderBy('del_partner')
result_df.show()
df.unpersist()

# Write a query to generate all possible unique match combinations between the teams.

Data set : teams (team_name VARCHAR(50) NOT NULL);

%sql
select t1.team_name as team , t2.team_name as opponent from teams t1 join teams t2 on t1.team_name<t2.team_name;

%python
df=spark.read.table('teams')
result_df=df.alias('t1').join(df.alias('t2'), col('t1.team_name')<col('t2.team_name') , 'inner').select(col('t1.team_name').alias('team'),col('t2.team_name').alias('opponent'))
result_df.display()

# Find the customers who are active the same have more than 1 trasaction in past 10 days. 

Data set : orders (ORDER_ID INT ,CUSTOMER_ID INT,ITEM_DESC VARCHAR(50),ORDER_DATE DATE);

%sql
with cte as(
select *, dateadd(current_date(),-10) as dateadd from orders where ORDER_DATE>dateadd(current_date(),-10))
select CUSTOMER_ID from cte group by CUSTOMER_ID having count(*)>1;

%python
df=spark.read.table('orders')
df=df.withColumn('date_add',date_add(current_date(),lit(-10)))
filter_df=df.filter(col('order_date')>col('date_add'))
result_df=filter_df.groupBy('customer_id').agg(count('*').alias('no_trasactions')).filter(col('no_trasactions')>1).select('customer_id')
result_df.display()
