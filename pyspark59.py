# Amazon
A table named “famous” has two columns called user id and follower id. It represents each user ID has a particular follower ID. These follower IDs are also users of Amazon Then, find the famous percentage of each user. 
Famous Percentage = number of followers a user has / total number of users on the platform.
CREATE TABLE famous (user_id INT, follower_id INT);

%sql
with distinct_users as (
select user_id as users from famous union select follower_id as users from famous),
follower_count as (
select user_id, count(follower_id) as followers from famouse group by user_id)
select f.user_id, (f.followers * 100.0/ (select count(*) from distinct_users) as famous_percentage from follower_count f;

---
Given a table 'sf_transactions' of purchases by date, calculate the month-over-month percentage change in revenue. The output should include the year-month date (YYYY-MM) and percentage change, rounded to the 2nd decimal point, and sorted from the beginning of the year to the end of the year. The percentage change column will be populated from the 2nd month forward and calculated as ((this month’s revenue — last month’s revenue) / last month’s revenue)*100.

CREATE TABLE sf_transactions(id INT, created_at datetime, value INT, purchase_id INT);

%sql
with monthlyrevenue as (
select format(created_at, 'yyyy-MM') as year_month, sum(value) as total_revenue from sf_transactions group by format(created_at, 'yyyy-MM')),
revenuechange as (
select year_month, total_revenue, lag(total_revenue) over (order by year_month) as previous_revenue from monthlyrevenue)
select year_month, total_revenue, round(case when previous_revenue is null then null else ((total_revenue - previous_revenue) / cast(previous_revenue as float)) * 100 end, 2) as percentage_change order by year_month;

---
You are analyzing a social network dataset at Google. Your task is to find mutual friends between two users, Karl and Hans. There is only one user named Karl and one named Hans in the dataset.

The output should contain 'user_id' and 'user_name' columns.
CREATE TABLE users(user_id INT, user_name varchar(30));
CREATE TABLE friends(user_id INT, friend_id INT);

%sql
with karl_friends as (
select friend_id from friends where user_id = (select user_id from users where user_name = 'karl')),
hans_friends as (
select friend_id from friends where user_id = (select user_id from users where user_name = 'Hans'))
select u.user_id, u.user_name from users u join karl_friends kf on u.user_id = kf.friend_id join hans_friends hf on u.user_id = hf.friend_id;

---
#Walmart
Some forecasting methods are extremely simple and surprisingly effective. Naïve forecast is one of them. To create a naïve forecast for "distance per dollar" (defined as distance_to_travel/monetary_cost), first sum the "distance to travel" and "monetary cost" values monthly. This gives the actual value for the current month. For the forecasted value, use the previous month's value. After obtaining both actual and forecasted values, calculate the root mean squared error (RMSE) using the formula RMSE = sqrt(mean(square(actual - forecast))). Report the RMSE rounded to two decimal places.

CREATE TABLE uber_request_logs(request_id int, request_date datetime, request_status varchar(10), distance_to_travel float, monetary_cost float, driver_to_client_distance float);

with monthly_aggregates as (
select convert(char(7), request_date, 120) as year_month, sum(distance_to_travel) as total_distance, sum(monetary_cost) as total_cost from uber_request_logs group by convert(char(7), request_date, 120)),
distance_per_dollar as (
select year_month, total_distance/total_cost as distance_per_dollar from monthly_aggregates),
naive_forecast as (
select year_month, distance_per_dollar, lag(distance_per_dollar,1) over (order by year_month) as forecasted_value from naive_forecast where forecasted_value is not null;

---
#meta
Given a list of projects and employees mapped to each project, calculate by the amount of project budget allocated to each employee. The output should include the project title and the project budget rounded to the closest integer. Order your list by projects with the highest budget per employee first.

CREATE TABLE ms_projects(id int, title varchar(15), budget int);
CREATE TABLE ms_emp_projects(emp_id int, project_id int);

%sql
select p.title as project_tilte, round(p.budget / count(e.emp_id), 0) as budget_per_employee from ms_projects p join ms_emp_projects e on p.id = e.product_id group by p.id, p.title, p.budget order by budget_per_employee desc;

---
# apple
Find the total number of available beds per hosts' nationality.
Output the nationality along with the corresponding total number of available beds. Sort records by the total available beds in descending order.
CREATE TABLE airbnb_apartments(host_id int,apartment_id varchar(5),apartment_type varchar(10),n_beds int,n_bedrooms int,country varchar(20),city varchar(20));
CREATE TABLE airbnb_hosts(host_id int,nationality varchar(15),gender varchar(5),age int);

%sql
select h.nationality, sum(a.n_beds) as total_available_beds from airbnb_apartments a join airbnb_hosts h on a.host_id = h.host_id group by h.nationality order by total_available_beds desc;

---
# Linkedln
IBM is working on a new feature to analyze user purchasing behavior for all Fridays in the first quarter of the year. For each Friday separately, calculate the average amount users have spent per order. The output should contain the week number of that Friday and average amount spent.

CREATE TABLE user_purchases(user_id int, date date, amount_spent float, day_name varchar(15));

%sql
with q1_fridays as (
select user_id, date, amount_spent, datepart(week, date) as week_number, day_name from user_purchases where day_name = 'Friday' and DATAPART(month, date) in (1,2,3))
select week_number, round(avg(amount_spent),2) as avg_amount_spent from q1_fridays group by week_number order by week_number;
---
# American Express
You are given a table of product launches by company by year. Write a query to count the net difference between the number of products companies launched in 2020 with the number of products companies launched in the previous year. Output the name of the companies and a net difference of net products released for 2020 compared to the previous year.

CREATE TABLE car_launches(year int, company_name varchar(15), product_name varchar(30));

%sql
with product_counts as (
select company_name, sum(case when year = 2020 then 1 else 0 end) as products_2020, sum(case when year = 2019 then 1 else 0 end) as products_2019 from car_launches where year in (2019, 2020) group by company_name)
select company_name, (products_2020 - product_2019) as net_difference from product_counts order by net_difference desc;

---
# IBM
Find the genre of the person with the most number of oscar winnings.
If there are more than one person with the same number of oscar wins, return the first one in alphabetic order based on their name. Use the names as keys when joining the tables.
CREATE TABLE nominee_information(name varchar(20), amg_person_id varchar(10), top_genre varchar(10), birthday datetime, id int);
CREATE TABLE oscar_nominees(year int, category varchar(30), nominee varchar(20), movie varchar(30), winner int, id int);

with WinnerCount as (
select o.nominee, count(*) as total_wins from oscar_nominees o where o.winner = 1 group by o.nominee)
select top 1 from winnerCount wc join nominee_information ni on wc.nominee = ni.name order by wc.total_wins desc, ni.name asc;

---
#AIrBnB
Write a query that'll identify returning active users. A returning active user is a user that has made a second purchase within 7 days of any other of their purchases. Output a list of user_ids of these returning active users.
CREATE TABLE amazon_transactions(id int, user_id int, item varchar(15), created_at datetime, revenue int);

%sql
select disitnct a.user_id from amazon_transactions a join amazon_transactions b on a.user_id = b.user_id and a.created_at < b.created_at and datediff(day, a.created_at, b.created_at) <= 7;

---
# Tesla
Find the number of transactions that occurred for each product. Output the product name along with the corresponding number of transactions and order records by the product id in ascending order. You can ignore products without transactions.
CREATE TABLE excel_sql_inventory_data (product_id INT,product_name VARCHAR(50),product_type VARCHAR(50),unit VARCHAR(20),price_unit FLOAT,wholesale FLOAT,current_inventory INT);
CREATE TABLE excel_sql_transaction_data (transaction_id INT PRIMARY KEY,time DATETIME,product_id INT);

%sql
select inv.product_name, count(trans.transaction_id) as transaction_count from excel_sql_inventory_data as inv join excel_sql_transaction_data as trans on inv.product_id = trans.product_id group by inv.product_id ,inv.product_name order by inv.product_id asc;

---
# Nvidia
Write a query that calculates the difference between the highest salaries found in the marketing and engineering departments. Output just the absolute difference in salaries.
CREATE TABLE db_employee (id INT,first_name VARCHAR(50),last_name VARCHAR(50),salary INT,department_id INT);
CREATE TABLE db_dept (id INT,department VARCHAR(50));

%sql
select abs(max(case when d.department = 'marketing' then e.salary end) - max(case when d.department = 'engineering' then e.salary end) from db_employee e join db_dept on e.department_id = d.id;

---
#JpMorganChase
Find the number of rows for each review score earned by 'Hotel Arena'. Output the hotel name (which should be 'Hotel Arena'), review score along with the corresponding number of rows with that score for the specified hotel.
CREATE TABLE hotel_reviews (hotel_address VARCHAR(255),additional_number_of_scoring INT,review_date DATETIME,average_score FLOAT,hotel_name VARCHAR(100),reviewer_nationality VARCHAR(100),negative_review TEXT,review_total_negative_word_counts INT,total_number_of_reviews INT,positive_review TEXT,review_total_positive_word_counts INT,total_number_of_reviews_reviewer_has_given INT,reviewer_score FLOAT,tags VARCHAR(255),days_since_review VARCHAR(50),lat FLOAT,lng FLOAT);

%sql
select hotel_name, reviewer_score, count(*) as score_count from hotel_reviews where hotel_name = 'Hotel Arena' group by hotel_name, reviewer_score order by reviewer_score;

---
#Deloitte
What is the total sales revenue of Samantha and Lisa?
CREATE TABLE sales_performance (salesperson VARCHAR(50),widget_sales INT,sales_revenue INT,id INT PRIMARY KEY);

%sql
select sum(sales_revenue) as total_sales_revenue from sales_performance where salesperson in ('Samantha', 'Lisa');

---
#Coforge
Find all records from days when the number of distinct users receiving emails was greater than the number of distinct users sending emails.
CREATE TABLE google_gmail_emails (id INT PRIMARY KEY,from_user VARCHAR(50),to_user VARCHAR(50),day INT);

%sql
with distinct_counts as (
select day, count(distinct to_user) as distinct_receivers, count(distinct from_user) as distinct_senders from google_emails group by day)
select g.id, g.from_user, g.to_user, g.day from google_gamil_emails g join distinct_counts dc on g.day = dc.day where dc.distinct_receivers > dc.distinct_senders;

---
# cognizant
Bank of Ireland has requested that you detect invalid transactions in December 2022. An invalid transaction is one that occurs outside of the bank's normal business hours. The following are the hours of operation for all branches:
CREATE TABLE boi_transactions (transaction_id INT PRIMARY KEY,time_stamp DATETIME NOT NULL);

%sql
select transaction_id from boi_transactions where month(time_stamp) = 12 and year(time_stamp) = 2022 and (
datepart(weekday, timestamp) in (1,7) or cast(time_stamp as time) < '09:00:00' or cast(time_stamp as time) > '16:00:00' or (datepart(day, time_stamp) in (25,26) and month(time_stamp) = 12));

---
#CGI
You’re given a table of Uber rides that contains the mileage and the purpose for the business expense. You’re asked to find business purposes that generate the most miles driven for passengers that use Uber for their business transportation. Find the top 3 business purpose categories by total mileage.
CREATE TABLE my_uber_drives (start_date DATETIME,end_date DATETIME,category VARCHAR(50),start VARCHAR(50),stop VARCHAR(50),miles FLOAT,purpose VARCHAR(50));

%sql
select top 3 purpose, sum(miles) as total_miles from my_uber_drivers where category = 'Business' group by purpose order by total_miles desc;
---
# Tiger Analytics
You have been asked to find the job titles of the highest-paid employees.
Your output should include the highest-paid title or multiple titles with the same salary.

CREATE TABLE worker(worker_id INT PRIMARY KEY,first_name VARCHAR(50),last_name VARCHAR(50),salary INT,joining_date DATETIME,department VARCHAR(50));
CREATE TABLE title(worker_ref_id INT,worker_title VARCHAR(50),affected_from DATETIME);

%sql
select t.worker_title from title t join worker w on t.worker_ref_id = w.worker_id where w.salary = (select max(salary) from worker):
---
# capgemini
Identify users who started a session and placed an order on the same day. For these users, calculate the total number of orders and the total order value for that day. Your output should include the user, the session date, the total number of orders, and the total order value for that day.
CREATE TABLE sessions(session_id INT PRIMARY KEY,user_id INT,session_date DATETIME);
CREATE TABLE order_summary (order_id INT PRIMARY KEY,user_id INT,order_value INT,order_date DATETIME);

%sql
select s.user_id as user_id, s.session_date, count(o.order_id) as total_orders, sum(o.order_value) as total_order_value from sessions s join order_summary o on s.user_id = o.user_id and convert(date, s.session_date) = convert(date, o.order_date) group by s.user_id, s.session_date having count(o.order_id) > 0;
---
# Infosys
Write a query that returns the number of unique users per client per month
CREATE TABLE fact_events (id INT PRIMARY KEY,time_id DATETIME,user_id VARCHAR(20),customer_id VARCHAR(50),client_id VARCHAR(20),event_type VARCHAR(50),event_id INT);

%sql
select client_id, format(time_id, 'yyyy-MM') as month_year, count(Distinct user_id) as unique_users from fact_events group by client_id, format(time_id, 'yyyy-MM') order by client_id, month_year;
---
#PhonePe
Find the total number of downloads for paying and non-paying users by date. Include only records where non-paying customers have more downloads than paying customers. The output should be sorted by earliest date first and contain 3 columns date, non-paying downloads, paying downloads. 
CREATE TABLE ms_user_dimension (user_id INT PRIMARY KEY,acc_id INT);
CREATE TABLE ms_download_facts (date DATETIME,user_id INT,downloads INT);

%sql
select convert(date, d.date) as [date],
sum(case when a.paying_customer = 'No' then d.downloads else 0 end) as non_paying_downloads, sum(case when a.paying_customer = 'Yes' then d.download else 0 end) as paying_downloads from ms_download_facts d join ms_user_dimension u on d.user_id = u.user_id join ms_acc_dimension a on u.acc_id = a.acc_id group by convert(date, d.date) having sum(case when a.paying_customer = 'No' then d.downloads else 0 end) > sum(case when a.paying_customer = 'Yes' then d.downloads else 0 end) order by date asc;
---
#TCS
Find managers with at least 7 direct reporting employees. In situations where user is reporting to himself/herself, count that also.
Output first names of managers.
CREATE TABLE employees (id INT PRIMARY KEY,first_name VARCHAR(50),last_name VARCHAR(50),age INT,sex VARCHAR(10),employee_title VARCHAR(50),department VARCHAR(50),salary INT,target INT,bonus INT,email VARCHAR(100),city VARCHAR(50),address VARCHAR(255),manager_id INT);

%sql
select m.first_name as manager_first_name from employees e join employees m on e.manager_id = m.id group by m.first_name having count(e.id) >= 7;
---
#Wipro
Write a query that compares each employee's salary to their manager's and the average department salary (excluding the manager's salary). Display the department, employee ID, employee's salary, manager's salary, and department average salary. Order by department, then by employee salary (highest to lowest).
CREATE TABLE employee_o (id INT PRIMARY KEY,first_name VARCHAR(50),last_name VARCHAR(50),age INT,gender VARCHAR(10),employee_title VARCHAR(50),department VARCHAR(50),salary INT,manager_id INT);

%sql
with departmentavgsalary as (
select department, round(avg(case when id != manager_id then salary end), 0) as avg_salary from employee_o group by department),
managerSalary as (
select e.id as employee_id, e.department,e.salary as employee_salary, m.salary as manager_salary from employee_o e left join employee_o on e.manager_id = m.id and e.id != e.manager_id)
select e.department, e.id as employee_id, e.salary as employee_salary, m.manager_salary, d.avg_salary as department_avg_salary from employee_o e join ManagerSalary m on e.id = m.employee_id join DepartmentAvgSalary d on e.department = d.department order by e.department, e.salary desc;

---
# Visa
Find products which are exclusive to only Amazon and therefore not sold at Top Shop and Macy's. Your output should include the product name, brand name, price, and rating.
CREATE TABLE innerwear_amazon_com (product_name VARCHAR(255),mrp VARCHAR(50),price VARCHAR(50),pdp_url VARCHAR(255),brand_name VARCHAR(100),product_category VARCHAR(100),retailer VARCHAR(100),description VARCHAR(255),rating FLOAT,review_count INT,style_attributes VARCHAR(255),total_sizes VARCHAR(50),available_size VARCHAR(50),color VARCHAR(50));
CREATE TABLE innerwear_macys_com (product_name VARCHAR(255),mrp VARCHAR(50),price VARCHAR(50),pdp_url VARCHAR(255),brand_name VARCHAR(100),product_category VARCHAR(100),retailer VARCHAR(100),description VARCHAR(255),rating FLOAT,review_count FLOAT,style_attributes VARCHAR(255),total_sizes VARCHAR(50),available_size VARCHAR(50),color VARCHAR(50));
CREATE TABLE innerwear_topshop_com (product_name VARCHAR(255),mrp VARCHAR(50),price VARCHAR(50),pdp_url VARCHAR(255),brand_name VARCHAR(100),product_category VARCHAR(100),retailer VARCHAR(100),description VARCHAR(255),rating FLOAT,review_count FLOAT,style_attributes VARCHAR(255),total_sizes VARCHAR(50),available_size VARCHAR(50),color VARCHAR(50));

%sql
select a.product_name, a.brand_name, a.price, a.rating from innnerwear_amazon_com a left join innerwear_macys_com m on a.product_name = m.product_name and a.mrp = m.mrp left join innerwear_topshop_com t on a.product_name = t.product_name and a.mrp = t.mrp where m.product_name is null and t.product_name is null order by a.product_name;
---
# Accenture
American Express is reviewing their customers' transactions, and you have been tasked with locating the customer who has the third highest total transaction amount. The output should include the customer's id, as well as their first name and last name. For ranking the customers, use type of ranking with no gaps between subsequent ranks.
CREATE TABLE customers (id INT,first_name VARCHAR(50),last_name VARCHAR(50),city VARCHAR(100),address VARCHAR(200),phone_number VARCHAR(20));
CREATE TABLE card_orders (order_id INT,cust_id INT,order_date DATETIME,order_details VARCHAR(255),total_order_cost INT);

%sql
with customertransactiontotals as (
select c.id as customer_id, c.first_name, c.last_name, sum(co.total_order_cost) as total_transaction_amount from customers c join card_orders co on c.id = co.cust_id group by c.id, c.first_name, c.last_name),
RankedTransactions as (
select customer_id, first_name, last_name, total_transaction_amount, rank() over (order by total_transaction_amount desc) as transaction_rank from customerTransactionTotals)
select customer_id, first_name, last_name from rankedTransactions where transaction_rank = 3;
---
# Sony
Consider all LinkedIn users who, at some point, worked at Microsoft. For how many of them was Google their next employer right after Microsoft (no employers in between)?
CREATE TABLE linkedin_users (user_id INT,employer VARCHAR(255),position VARCHAR(255),start_date DATETIME,end_date DATETIME);

%sql
with cte_ls as (
select user_id, employer, start_date, end_date, lead(employer) over (partition by user_id order by start_date) as next_employer from linkedln_users)
select user_id from cte_ls where employer = 'Microsoft' and next_employer = 'Google';

---
# cocaCola
You are given a day worth of scheduled departure and arrival times of trains at one train station. One platform can only accommodate one train from the beginning of the minute it's scheduled to arrive until the end of the minute it's scheduled to depart. Find the minimum number of platforms necessary to accommodate the entire scheduled traffic.
CREATE TABLE train_arrivals (train_id INT, arrival_time DATETIME);
CREATE TABLE train_departures (train_id INT, departure_time DATETIME);

%sql
with trainTimes as (
select arrival_time as event_time, 1 as event_type from train_arrivals union all select departure_time as event_time, -1 as event_type from train_departures)
select max(platform_needed) as min_platforms from (
select event_time, sum(event_type) over (order by event_time) as platform_needed from trainTimes) as platformCount;
---
Find the highest salary among salaries that appears only once.
CREATE TABLE employee(id INT,first_name VARCHAR(50),last_name VARCHAR(50),age INT,sex VARCHAR(1),employee_title VARCHAR(50),department VARCHAR(50),salary INT,target INT,bonus INT,email VARCHAR(100),city VARCHAR(50),address VARCHAR(100),manager_id INT);

%sql
with salarycounts as (
select salary, count(*) as salary_count from employee group by salary)
select max(salary) as highest_unique_salary from salarycoutns where salary_count = 1;
