1. Find 4 consecutive empty seats in a movie hall

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, sum as _sum, when

# Assuming df has columns: row_num, seat_num, status (0=empty, 1=occupied)
window_spec = Window.partitionBy("row_num").orderBy("seat_num")
df = df.withColumn("prev_status", lag("status", 3).over(window_spec)) \
       .withColumn("prev_status_2", lag("status", 2).over(window_spec)) \
       .withColumn("prev_status_3", lag("status", 1).over(window_spec))

df_filtered = df.filter(
    (col("status") == 0) &
    (col("prev_status") == 0) &
    (col("prev_status_2") == 0) &
    (col("prev_status_3") == 0))

2. Yearwise count of new cities for UDAAN

%python
from pyspark.sql.functions import year, countDistinct

df_grouped = df.groupBy(year("operation_start_date").alias("year")).agg(countDistinct("city").alias("new_cities_count"))

3. 3+ consecutive days with people ≥ 100

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, sum as _sum

window_spec = Window.orderBy("date")
df = df.withColumn("prev_day_people", lag("people", 1).over(window_spec)) \
       .withColumn("prev_day2_people", lag("people", 2).over(window_spec))

df_filtered = df.filter(
    (col("people") >= 100) &
    (col("prev_day_people") >= 100) &
    (col("prev_day2_people") >= 100))

4. 3rd highest salary per department or lowest if less than 3

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, count, when, min as _min

window_spec = Window.partitionBy("department").orderBy(col("salary").desc())
window_spec_count = Window.partitionBy("department")

df = df.withColumn("row_num", row_number().over(window_spec)) \
       .withColumn("emp_count", count("*").over(window_spec_count))

result = df.filter(
    ((col("emp_count") >= 3) & (col("row_num") == 3)) |
    ((col("emp_count") < 3) & (col("salary") == _min("salary").over(Window.partitionBy("department")))))

5. Median salary per company

%python
from pyspark.sql.functions import expr

result = df.groupBy("company") \
           .agg(expr("percentile_approx(salary, 0.5)").alias("median_salary"))

6. Pivot player names and associated cities

%python
pivoted_df = df.groupBy("player_id").pivot("city").agg({"player_name": "first"})

7. System on/off status with login/logout count

%python
from pyspark.sql.functions import count, when, col

result = df.groupBy("date", "system_status") \
           .agg(count("*").alias("count"),
                min("login_time").alias("login_time"),
                max("logout_time").alias("logout_time"))

8. Largest order by value per salesperson

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("salesperson_id").orderBy(col("order_value").desc())
result = df.withColumn("rank", row_number().over(window_spec)) \
           .filter(col("rank") == 1) \
           .drop("rank")

10. Percentage of students scoring >90 in any subject

%python
from pyspark.sql.functions import countDistinct, when, col

total_students = df.select(countDistinct("student_id")).collect()[0][0]
students_above_90 = df.filter(col("marks") > 90).select(countDistinct("student_id")).collect()[0][0]
percentage = (students_above_90 / total_students) * 100

11. Second highest and second lowest marks per subject

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_desc = Window.partitionBy("subject").orderBy(col("marks").desc())
window_asc = Window.partitionBy("subject").orderBy(col("marks").asc())

df_ranked = df.withColumn("rank_desc", row_number().over(window_desc)) \
              .withColumn("rank_asc", row_number().over(window_asc))

second_highest = df_ranked.filter(col("rank_desc") == 2).select("subject", "marks").withColumn("type", lit("second_highest"))
second_lowest = df_ranked.filter(col("rank_asc") == 2).select("subject", "marks").withColumn("type", lit("second_lowest"))
result = second_highest.union(second_lowest)

12. Marks increased/decreased from previous test

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import lag

window_spec = Window.partitionBy("student_id", "subject").orderBy("test_date")
df = df.withColumn("prev_marks", lag("marks", 1).over(window_spec)) \
       .withColumn("change", 
                   when(col("marks") > col("prev_marks"), "increased")
                   .when(col("marks") < col("prev_marks"), "decreased")
                   .otherwise("no change"))

13. Messages exchanged per person per day

%python
from pyspark.sql.functions import count

result = df.groupBy("date", "sender_id", "receiver_id") \
           .agg(count("*").alias("message_count"))

14. Products within customer budget (choose less costly on clash)

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("customer_id").orderBy("product_price")
result = df.filter(col("product_price") <= col("customer_budget")) \
           .withColumn("rank", row_number().over(window_spec)) \
           .filter(col("rank") == 1) \
           .drop("rank")

15. Companies with ≥2 users speaking English and German

%python
from pyspark.sql.functions import countDistinct

english_german_users = df.filter(col("language").isin(["English", "German"])) \
                         .groupBy("company", "user_id") \
                         .agg(countDistinct("language").alias("lang_count")) \
                         .filter(col("lang_count") == 2)

result = english_german_users.groupBy("company") \
                             .agg(countDistinct("user_id").alias("user_count")) \
                             .filter(col("user_count") >= 2)

16. Cities where covid cases increasing continuously

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import lag

window_spec = Window.partitionBy("city").orderBy("date")
df = df.withColumn("prev_cases", lag("cases", 1).over(window_spec)) \
       .withColumn("increase_flag", when(col("cases") > col("prev_cases"), 1).otherwise(0))

# Assuming we want 3+ consecutive days
result = df.groupBy("city") \
           .agg(_sum("increase_flag").alias("consecutive_increase_days")) \
           .filter(col("consecutive_increase_days") >= 3)

17. Students with same marks in physics and chemistry

%python
physics = df.filter(col("subject") == "physics").select("student_id", "marks").withColumnRenamed("marks", "physics_marks")
chemistry = df.filter(col("subject") == "chemistry").select("student_id", "marks").withColumnRenamed("marks", "chemistry_marks")

result = physics.join(chemistry, "student_id") \
                .filter(col("physics_marks") == col("chemistry_marks"))

18. Total active users each day

%pythonfrom pyspark.sql.functions import countDistinct

result = df.groupBy("date") \
           .agg(countDistinct("user_id").alias("active_users"))

19. Total active users each week

%python
from pyspark.sql.functions import weekofyear, countDistinct

result = df.groupBy(weekofyear("date").alias("week_number")) \
           .agg(countDistinct("user_id").alias("active_users"))

20. Users who made purchase same day

%python
from pyspark.sql.functions import countDistinct

result = df.groupBy("date") \
           .agg(countDistinct("user_id").alias("same_day_purchasers"))

21. Percentage of paid users by country

%python
from pyspark.sql.functions import when, count, lit

result = df.withColumn("country_group", 
                       when(col("country").isin(["India", "USA"]), col("country"))
                       .otherwise("Others")) \
           .groupBy("country_group") \
           .agg((count(when(col("is_paid") == True, 1)) / count("*") * 100).alias("paid_percentage"))

22. App installs with next-day purchases

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, lag

window_spec = Window.partitionBy("user_id").orderBy("date")
df = df.withColumn("prev_activity", lag("activity", 1).over(window_spec)) \
       .withColumn("prev_date", lag("date", 1).over(window_spec))

result = df.filter(
    (col("prev_activity") == "install") &
    (col("activity") == "purchase") &
    (datediff(col("date"), col("prev_date")) == 1)).groupBy("prev_date").count()

23. Total charges as per billing rate

%python
from pyspark.sql.functions import sum

result = df.withColumn("total_charge", col("usage") * col("rate_per_unit")) \
           .agg(sum("total_charge").alias("total_charges"))

24. Second most recent activity or only activity

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, count

window_spec = Window.partitionBy("user_id").orderBy(col("activity_date").desc())
window_count = Window.partitionBy("user_id")

df = df.withColumn("activity_rank", row_number().over(window_spec)) \
       .withColumn("activity_count", count("*").over(window_count))

result = df.filter(
    (col("activity_count") == 1) |
    (col("activity_rank") == 2))

25. Monthwise churned customers

%python
from pyspark.sql.functions import month, countDistinct

churned_users = df.filter(col("status") == "churned")
result = churned_users.groupBy(month("churn_date").alias("month")) \
                      .agg(countDistinct("customer_id").alias("churned_count"))

26. Monthwise returning customers

%python
from pyspark.sql.functions import month, countDistinct

returning_users = df.filter(col("is_returning") == True)
result = returning_users.groupBy(month("return_date").alias("month")) \
                        .agg(countDistinct("customer_id").alias("returning_count"))

27. Recommendation system based on product pairs

%python
from pyspark.sql.functions import collect_list

# Assuming df has columns: transaction_id, product_id
product_pairs = df.alias("a").join(df.alias("b"), "transaction_id") \
                  .filter(col("a.product_id") < col("b.product_id")) \
                  .groupBy(col("a.product_id").alias("product1"), 
                          col("b.product_id").alias("product2")) \
                  .count() \
                  .orderBy(col("count").desc())

28. Fraction of users who accessed Amazon Music and upgraded to Prime within 30 days

%python
from pyspark.sql.functions import datediff, countDistinct

music_users = df.filter(col("activity") == "amazon_music_access")
prime_upgrades = df.filter(col("activity") == "prime_upgrade")

joined = music_users.alias("m").join(prime_upgrades.alias("p"), "user_id") \
                   .filter(datediff(col("p.upgrade_date"), col("m.access_date")) <= 30)

total_music_users = music_users.select(countDistinct("user_id")).collect()[0][0]
upgraded_users = joined.select(countDistinct("user_id")).collect()[0][0]
fraction = round(upgraded_users / total_music_users, 2)

29. Total sales by year

%python
from pyspark.sql.functions import year, sum

result = df.groupBy(year("sale_date").alias("year")) \
           .agg(sum("sale_amount").alias("total_sales"))

30. from pyspark.sql.functions import year, sum

%python
result = df.groupBy(year("sale_date").alias("year")) \
           .agg(sum("sale_amount").alias("total_sales"))

31. Users by device type (mobile/desktop/both)

%python
from pyspark.sql.functions import countDistinct, sum, when

result = df.groupBy("date") \
           .agg(
               countDistinct(when(col("device") == "mobile", col("user_id"))).alias("mobile_only"),
               countDistinct(when(col("device") == "desktop", col("user_id"))).alias("desktop_only"),
               countDistinct(when((col("device") == "mobile") | (col("device") == "desktop"), col("user_id"))).alias("both"),
               sum(when(col("device") == "mobile", col("amount"))).alias("mobile_spent"),
               sum(when(col("device") == "desktop", col("amount"))).alias("desktop_spent"))

32. Seller's second item brand vs favorite brand

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, when, lit

window_spec = Window.partitionBy("seller_id").orderBy("sale_date")
df = df.withColumn("sale_rank", row_number().over(window_spec)) \
       .withColumn("is_favorite", 
                   when(col("sale_rank") == 2, 
                        when(col("brand") == col("favorite_brand"), "yes", "no"))
                   .otherwise(lit("N/A")))

33. Winner in each group (max points, tie-break by lowest player_id)

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, sum as _sum

window_spec = Window.partitionBy("group_id").orderBy(col("total_points").desc(), col("player_id"))
df_with_points = df.groupBy("group_id", "player_id") \
                   .agg(_sum("points").alias("total_points"))

result = df_with_points.withColumn("rank", row_number().over(window_spec)) \
                       .filter(col("rank") == 1)

34. Department-wise median salary

%python
from pyspark.sql.functions import expr

result = df.groupBy("department") \
           .agg(expr("percentile_approx(salary, 0.5)").alias("median_salary"))

35. Cancellation rate for unbanned users

%python
from pyspark.sql.functions import when, avg, round

unbanned_requests = df.filter(
    (col("client_banned") == "No") & 
    (col("driver_banned") == "No") &
    (col("request_date").between("2013-10-01", "2013-10-03")))

result = unbanned_requests.groupBy("request_date") \
           .agg(round(avg(when(col("status") == "cancelled", 1).otherwise(0)), 2).alias("cancellation_rate"))

36. Manager ID and average salary for employees with conditions

%python
result = df.filter(col("salary") > 5000) \
           .groupBy("manager_id") \
           .agg(avg("salary").alias("avg_salary")) \
           .filter(col("avg_salary") > 10000)

37. Persons with friends having total score > 100

%python
from pyspark.sql.functions import count, sum

# Assuming df has: person_id, friend_id, marks
friends_agg = df.groupBy("person_id") \
                .agg(count("friend_id").alias("friend_count"),
                     sum("marks").alias("total_friend_marks"))

result = friends_agg.filter(col("total_friend_marks") > 100) \
                    .join(df.select("person_id", "name").distinct(), "person_id")

38. Top 20% products contributing to 80% of sales (Pareto analysis)

%python
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as _sum, percent_rank

window_spec = Window.orderBy(col("total_sales").desc())
df_product_sales = df.groupBy("product_id") \
                     .agg(_sum("sales").alias("total_sales")) \
                     .withColumn("sales_percent_rank", percent_rank().over(window_spec)) \
                     .withColumn("cumulative_sales", _sum("total_sales").over(window_spec))

total_sales = df.select(_sum("sales")).collect()[0][0]
result = df_product_sales.filter(col("cumulative_sales") <= total_sales * 0.8) \
                         .filter(col("sales_percent_rank") <= 0.2)

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# There are 3 rows in a movie hall each with 10 seats in each row.Write a SQL query to find 4 consecutive empty seats.

%sql
with cte1 as (
select *, left(seat, 1) as row_id, cast(substring(seat,2,2) as int) as seat_id from movie),
cte2 as (
select *, max(occupancy) over(partition by row_id order by seat_id rows between current row and 3 following) as is_4_empty,
count(occupancy) over (partition by row_id order by seat_id rows between current row and 3 following ) as cnt from cte1),
cte3 as (
select * from cte2 where is_4_empty = 0 and cnt = 4)
select c2.* from cte2 c2 join cte3 c3 on c2.row_id = c3.row_id and c2.seat_id between c3.seat_id and c3.seat_id + 3;

# Business City table has data from the day UDAAN has started operations. Write a SQL query to identify the yearwise count of new cities where UDAAN started their operations

create table business_city (
business_date date,
city_id int);

%sql
with cte as (
select datepart(year, business_date) as business_year, city_id from business_city)
select c1.business_year, count(distinct case when c2.city_id is null then c1.city_id end) as no_of_new_cities from cte c1 left join cte c2 on c1.business_year > c2.business_year and c1.city_id = c2.city_id group by c1.business_year;

#Write a SQL query to display the records which have 3 or more consecutive rows with the amount of people more than 100 (inclusive) each day

create table stadium (
id int,
visit_date date,
no_of_people int);

%sql
with cte as (
select *, row_number() over (order by visit_date) as rwn, id - row_number() over (order by visit_date) as grp from stadium where no_of_people >= 100)
select id, visit_date, no_of_people from cte where grp in (select grp from cte group by grp having count(1) >= 3);

# Write a SQL query to find details of employees with 3rd highest salary in each department. In case there are less than 3 employees in a department then return employee details with lowest salary in that department.

Solution:

CREATE TABLE [emp](
 [emp_id] [int] NULL,
 [emp_name] [varchar](50) NULL,
 [salary] [int] NULL,
 [manager_id] [int] NULL,
 [emp_age] [int] NULL,
 [dep_id] [int] NULL,
 [dep_name] [varchar](20) NULL,
 [gender] [varchar](10) NULL) ;

%sql
with cte as (
select emp_id , emp_name, dep_id, salary, dep_name, rank() over(partition by dep_id order by salary desc) as rn, 
count(1) over (partition by dep_id) as cnt from emp)
select * from cte where rn = 3 or (cnt < 3 and rn = cnt);

# Write a SQL query to find the median salary of each company

create table employee (
emp_id int,
company varchar(10),
salary int);

%sql
select company, avg(salary) as medium_salary from (select *, row_number() over (partition by company order by salary) as rn,
count(*) over (partition by company) as total_cnt from employee) a where rn between total_cnt * 1.0/2 and total_cnt * 1.0 / 2 + 1 group by company;

# Write a SQL query to pivot the player name and the cities that they are associated with

create table players_location (
name varchar(20),
city varchar(20));

%sql
with cte as (
select name, city, row_number() over (partition by city order by name asc) as rwn from players_location group by name, city)
select max(case when city = 'Bangalore' then name else null end) as Bangalore,
max(case when city = 'Mumbai' then name else null end) as Mumbai,
max(case when city = 'Delhi' then name else null end) as Delhi from cte group by rwn order by rwn;

# Write a SQL query to check whether the system is on or off and find login, logout time along with the count.

%sql
with cte as (
select *, lag(status, 1, status) over (order by event_time) as prev_status from event_status),
cte1 as (
select *, sum(case when status = 'on' and prev_status = 'off' then 1 else 0 end) over (order by event_time) as group_key from cte)
select min(event_time) as login, max(event_time) as logout, count(group_key) - 1 as on_count from cte1 group by group_key;

# Write a SQL query to find the largest order by value for each salesperson and display other details.

%sql
select a.order_number, a.order_date, a.cust_id, a.salesperson_id, a.amount from int_orders a left join int_orders b on a.salesperson_id = b.salesperson_id group by a.order_number, a.order_date, a.cust_id, a.salesperson_id, a.amount having a.amount >= max(b.amount) order by a.order_number asc;

# Write a SQL query to get the list of students who scored above the average marks in each subject

%sql
with avg_cte as (
select subject, avg(marks) as average_marks from students group by subject)
select studentname from students s join avg_cte on s.subject = ac.subject where s.mark > ac.average_marks;

#Write a SQL query to get the percentage of students who score more than 90 in any subject amongst the total students

%sql
select count(distinct case when marks > 90 then studentID end) * 1.0/ count(distinct studentname) * 100 as perc from students;

#  Write a SQL query to get the second highest and second lowest marks for each subject

%sql
select subject, sum(case when rnk_highest = 2 then marks else null end) as second_highest_marks,
sum(case when rnk_lowest = 2 then marks else null end) as second_lowest_marks from (
select distinct subject, marks, rank() over (partition by subject order by marks desc) as rnk_highest, rank() over (partition by subject order by marks) as rnk_lowest from students) a group by subject;

#  Write a SQL query to find each student and test, identify if their marks increased or decreased from the previous test

%sql
select *, case when marks > prev_marks then 'inc' else 'desc' end as marks_statys from (
select *, lag(marks, 1) over (partition by studentid order by testdate, subject) as prev_marks from students) a;

# Write a SQL query to find total number of messages exchanged between each person per day

%sql
select sms_date, p1, p2, sum(sums_no) as total_sums from (
select sms_date 
case when sender < receiver then sender else receiver end as p1,
case when sender > receiver then sender else receiver end as p2, sms_no from subscriber) a group by sms_date, p1, p2;

# Write a SQL query to find how many products falls into customer budget along with list of products. In case of clash choose the less costly product.

%sql
with cte as (select *, sum(cost) over (order by cost) as running_sum from products)
select customer_id, budget , count(distinct product_id) as no_of_products, group_concat(product_id, ',') as list_of_products from customer_budget cb left join cte c on c.running_sum < cb.budget group by customer_id, budget;

# Write a SQL query to find companies who have atleast 2 users who speaks English and German, both the languages

%sql
select company_id, count(1) from (select company_id, user_id, count(1) from company_users where language in ('English', 'German') group by company_id, user_id having count(1) = 2) a group by company_id having count(1) > 2;

# Write a SQL query to find cities where the covid cases are increasing continuosly

%sql
with cte as (select city, rank() over(partition by city order by cases) as rnk_increase_cases, rank() over (partition by city order by days) as rnk_increase_days, rank() over (partition by city order by days) as diff from covid)
select city from cte group by city having count(distinct diff) = 1 and max(diff) = 0;

# Write a SQL query to find students with same marks in physics and chemistry

%sql
with cte as (
select student_id, count(distinct subject) as subject_count, count(distinct marks) as marks_count from exams group by student_id having count(distinct subject) > 1and count(distinct marks) = 1)
select student_id from cte;

# Write a SQL query to find 3 or more consecutive empty seats

%sql
with cte as (select seat_no, is_empty, lag(is_empty,1) over (orderby seat_no asc) as prev_1,
lag(is_empty,2) over(order by seat_no asc) as prev_2,
lead(is_empty,1) over (order by seat_no asc) as next_1,
lead(is_empty, 2) over (order by seat_no asc) as next_2 from bms)
select * from cte where is_empty = 'Y' and prev_1 = 'Y' and prev_2= 'Y' or (is_empty = 'Y' and prev_1 = 'Y' and next_1 = 'Y') or (is_empty = 'Y' and next_1 = 'Y' and next_2 = 'Y');

# Write a SQL query to find total active users each day

%sql
select event_date, count(distinct user_id) as total_active_users from activity group by event_date;

# Write a SQL query to find total active users each week

%sql
select datepart(week, event_date) as week_number, count(distinct user_id) as total_active_users from activity group by datepart(week, event_date);

# Write a SQL query to find datewise total number of users who made the purchase same day

%sql
select event_date, count(new_user) as no_of_users  from (select user_id, event_date, case when count(distinct event_name)=2 then user_id 
else null end as new_user from activity group by user_id, event_date) a group by event_date;

# Write a SQL query to find percentage of paid users in India, USA, and any other country should be tagged as others

%sql
with cte as (select case when country in ('India', 'USA') then country else 'others' end as new_country, count(distinct user_id) as user_cnt from activity
where event_name = 'app-purchase' group by case when country in ('India', 'USA') then country else 'others' end), 
total as (select sum(user_cnt) as total_users from cte)
select new_country, user_cnt * 1.0/total_users * 100 as perc_users from cte, total;

# Write a SQL query to find among all the users who installed the app on a given day, how many did in app purchased on the very next day, day wise result

%sql
with cte as (select *, lag(event_name,1) over(partition by user_id order by event_date) as prev_event_name, lag(event_date,1) over(partition by user_id order by event_date) as prev_event_date from activity)
select event_date, count(distinct user_id) as cnt_users from cte where event_name = 'app-purchase' and prev_event_name = 'app-installed' and datediff(day, prev_event_date, event_date)=1 group by event_date;

# Write a SQL query to find total charges as per billing rate

%sql
with cte as(
select *, lead(dateadd(day, -1, bill_date),1, '9999-12-31') over(partition by emp_name order by bill_date) as next_billed_date from billings)
select hw.emp_name, sum(cte.bill_rate * hw.bill_hrs) as totalpayhours from cte join HoursWorked hw on hw.emp_name = cte.emp_name
and hw.work_date between cte.bill_date and cte.next_billed_date group by hw.emp_name;

# Write a SQL query to get the second most recent activity, if there is only one activity for a user then return that activity only.

%sql
with cte as (select *, 
count(1) over (partition by username) as total_activities, rank() over(partition by username order by startDate desc) as rnk from UserActivity)
select * from cte where rnk = 2 or total_activities = 1;

# Write a SQL query to find number of churned customers monthwise

%sql
SELECT strftime('%m', last_month.order_date) AS month_date, COUNT(DISTINCT last_month.cust_id) AS churned_customers FROM transactions AS last_month LEFT JOIN transactions AS this_month
 ON this_month.cust_id = last_month.cust_id
 AND strftime('%Y-%m', last_month.order_date) = strftime('%Y-%m', date(this_month.order_date, '-1 month')) WHERE this_month.cust_id IS NULL GROUP BY strftime('%m', last_month.order_date);

# Write a SQL query to find number of returning customers monthwise

%sql
SELECT
 strftime('%m', this_month.order_date) AS month_date,
 COUNT(DISTINCT last_month.cust_id) AS returning_customers FROM transactions AS this_month LEFT JOIN transactions AS last_month ON this_month.cust_id = last_month.cust_id AND strftime('%Y-%m', last_month.order_date) = strftime('%Y-%m', date(this_month.order_date, '-1 month')) GROUP BY strftime('%m', this_month.order_date) ORDER BY month_date;

# Write a SQL query to create a recommendation system based on product pairs most commonly purchased together.

%sql
select pr1.name + ' ' + pr2.name as pair, count(1) as purchase_freq 
from orders o1
inner join orders o2 on o1.order_id = o2.order_id
inner join products pr1 on pr1.id = o1.product_id
inner join products pr2 on pr2.id = o2.product_id where o1.product_id < o2.product_id group by pr1.name, pr2.name;

# Write a SQL query to return the fraction of users, rounded to two decimal places, who accessed Amazon music and upgraded to prime membership within the first 30 days of signing up

%sql
select 
count(distinct u.user_id) as total_users,
count(distinct case when datediff(day, u.join_date, e.access_date) <= 30 then u.user_id end) as users_access_prime_within_30_days,
CAST(1.0 * count(distinct case when datediff(day, u.join_date, e.access_date) <= 30 then u.user_id end) / count(distinct u.user_id) * 100 AS decimal(10,2)) as percent_users_access_prime_within_30_days from users u left join events e  on u.user_id = e.user_id and e.type = 'P' where u.user_id  in (select user_id from events where type = 'Music');

# Write a SQL query to find the total sales by year

%sql
with cte as (
select min(period_start) as min_dates, max(period_end) as max_dates from sales
union all
select dateadd(day, 1, min_dates), max_dates from cte where min_dates < max_dates)
select product_id, year(min_dates) as report_year, sum(average_daily_sales) as total_amount
from cte join sales on min_dates between period_start and period_end group by product_id, year(min_dates) order by product_id, year(min_dates) option(maxrecursion 1000);

# Write a SQL query to find the total number of users and the total amount spent using mobile only, desktop only and both mobile and desktop together for each date.

%sql
with cte as (select spend_date, user_id, max(platform) as platform, sum(amount) as amount from spending group by user_id, spend_date having count(distinct platform) = 1
union all
select spend_date,user_id, 'both' as platform, sum(amount) as amount from spending group by user_id, spend_date having count(distinct platform) = 2
union all
select distinct spend_date, null as user_id, 'both' as platform, 0 as amount from spending)
select spend_date, platform, sum(amount) as total_amount, count(distinct user_id) as total_users from cte group by spend_date, platform order by spend_date, platform desc;

# Write a SQL query to find for each seller, whether the brand of the second item (by date) they sold is their favorite brand.If a seller sold less than 2 items, report the answer for that seller as "no. o/p"

%sql
with rank_order as (select *, rank() over(partition by seller_id order by order_date asc) as rnk from orders)
select u.user_id as seller_id, case when item_brand = favorite_brand then 'Yes' else 'No' end as item_fav_brand from users u left join rank_order ro on u.user_id = ro.seller_id and ro.rnk = 2 left join items i on ro.item_id = i.item_id;

# Write a SQL query to find the winner in each group.The winner in each group is the player who scored the maximum total points within the group. In case of a tie, the lowest player_id wins.

%sql
with player_score as (select first_player as player, first_score as score from matches group by first_player, first_score
union all
select second_player, second_score from matches group by second_player, second_score), 
cte as (
select p.group_id, p.player_id, max(ps.score) as score from players p join player_score ps on p.player_id = ps.player group by p.group_id, p.player_id),
cte1 as (
select *, rank() over(partition by group_id order by score desc, player_id asc) as rnk from cte)
select group_id, player_id, score from cte1 where rnk = 1

# Write a SQL query to get department wise median salary.

%sql
with cte as (select *, row_number() over(partition by department_id order by salary asc) as rnk, count(*) over(partition by department_id) as count from emp)
select department_id, avg(salary) as median_salary from cte where rnk in ((count+1)/2, (count + 2)/2) group by department_id;

# Write a SQL query to find the cancellation rate of requests with unbanned users (both client and driver must not be banned) each day between "2013-10-01" and "2013-10-03". Round cancellation rate to 2 decimal points.

The cancellation rate is computed by dividing the number of cancelled (by client or driver) requests with unbanned users by the total number of requests with unbanned users on that day.

%sql
select request_at, count(case when status in ("cancelled_by_client","cancelled_by_driver") then 1 else null end) as cancelled_trip_count, count(1) as total_trips, 1.0 * count(case when status in ("cancelled_by_client","cancelled_by_driver") then 1 else null end)/count(1) * 100 as cancelled_percent from Trips t join Users u on t.client_id = u.users_id join Users d on t.driver_id = d.users_id where u.banned = "No" and d.banned = "No" group by request_at;

# Write an SQL to find manager id and average salary for employees working under him. Consider only those employees whole salary is greater than 5000 and show only manager ids with employees average salaries greater than 10000.

%sql
select manager_id, avg(salary) as avg_salary from emp where salary > 5000 group by manager_id having avg(salary) > 10000;

# Write a SQL query to find PersonID, Name, number of friends, sum of marks of person who have friends with total score greater then 100

%sql
with score as (
select f.pid, sum(p.score) as total_friend_score, count(f.pid) as no_of_friend from friend f join person p on f.id = p.PersonID group by f.pid haivng sum(p.score) > 100) 
select s.*, p.Name as person_n from score s join person p on s.pid = p.PersonID;

# Let's consider we have a super store data where we have sales for last 3 years and we have around 2000 products.Write a SQL query to find our which of the 20% products are top products which are contributing for 80% of the sales.

%sql
with product_price as (
select product_id, sum(sales) as product_sales from orders group by product_id),
calculated_sales as (
select product_id, product_sales, sum(product_sales) over (order by product_sales desc rows between unbounded preceding and 0 preceding as running_sales , 0.8 * sum(product_sales) over () as total_sales from product_price)
select * from calculated_sales where running_sales <= total_sales;
