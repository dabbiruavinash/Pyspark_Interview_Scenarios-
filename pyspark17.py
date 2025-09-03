# Find the second highest salary

%sql
select max(salary) as secondhighestsalary from employees where salary < (select max(salary) from employees);

%python
max_salary = employees.agg(max("Salary")).first()[0]
second_highest = (employees
    .filter(col("Salary") < max_salary)
    .agg(max("Salary").alias("SecondHighestSalary")))

# Find employees hired in the last 6 months

%sql
select * from employees where hiredate >= add_months(trunc(sysdate), -6);

%python
six_months_ago = current_date() - expr("INTERVAL 6 MONTHS")
recent_hires = employees.filter(col("HireDate") >= six_months_ago)

# Retrieve top 3 employees by salary in each department

%sql
SELECT DepartmentID, EmployeeID, Salary FROM (
SELECT DepartmentID, EmployeeID, Salary, ROW_NUMBER() OVER (PARTITION BY DepartmentID ORDER BY Salary DESC) AS rn FROM Employees) WHERE rn <= 3;

%python
window_spec = Window.partitionBy("DepartmentID").orderBy(col("Salary").desc())
top_3_per_dept = (employees
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") <= 3)
    .select("DepartmentID", "EmployeeID", "Salary"))

# Find employees who never placed an order

%sql
SELECT e.EmployeeID, e.EmployeeName FROM Employees e LEFT JOIN Orders o ON e.EmployeeID = o.EmployeeID WHERE o.EmployeeID IS NULL;

%python
employees_no_orders = (employees
    .join(orders, employees.EmployeeID == orders.EmployeeID, "left_anti")
    .select("EmployeeID", "EmployeeName"))

# Calculate cumulative sales by month

%sql
SELECT TO_CHAR(OrderDate, 'YYYY-MM') AS OrderMonth, SUM(Sales) OVER (ORDER BY TO_CHAR(OrderDate, 'YYYY-MM')) AS CumulativeSales FROM Orders;

%python
monthly_sales = (orders
    .withColumn("OrderMonth", date_format("OrderDate", "yyyy-MM"))
    .groupBy("OrderMonth")
    .agg(sum("Sales").alias("MonthlySales"))
    .orderBy("OrderMonth"))

# Get department-wise average salary, rounded to 2 decimals

%sql
SELECT DepartmentID, ROUND(AVG(Salary), 2) AS AvgSalary FROM Employees GROUP BY DepartmentID;

%python
dept_avg_salary = (employees.groupBy("DepartmentID").agg(round(avg("Salary"), 2).alias("AvgSalary")))

# Find customers who placed more than 5 orders

%sql
SELECT CustomerID, COUNT(*) AS OrderCount FROM Orders GROUP BY CustomerID HAVING COUNT(*) > 5;

%python
frequent_customers = (orders
    .groupBy("CustomerID")
    .agg(count("*").alias("OrderCount"))
    .filter(col("OrderCount") > 5))

# Find the longest trip distance from Trips table

%sql
SELECT TripID, Distance FROM Trips WHERE Distance = (SELECT MAX(Distance) FROM Trips);

%python
max_distance = trips.agg(max("Distance")).first()[0]
longest_trip = trips.filter(col("Distance") == max_distance)

# Find all weekends orders

%sql
SELECT * FROM Orders WHERE TO_CHAR(OrderDate, 'DY') IN ('SAT', 'SUN');

%python
weekend_orders = orders.filter(dayofweek("OrderDate").isin([1, 7]))

# Find employees with salary above department average

%sql
SELECT e.EmployeeID, e.Salary, e.DepartmentID FROM Employees e JOIN (
SELECT DepartmentID, AVG(Salary) AS DeptAvg FROM Employees GROUP BY DepartmentID) d ON e.DepartmentID = d.DepartmentID WHERE e.Salary > d.DeptAvg;

%python
dept_avg = (employees
    .groupBy("DepartmentID")
    .agg(avg("Salary").alias("DeptAvg")))

above_avg_employees = (employees
    .join(dept_avg, "DepartmentID")
    .filter(col("Salary") > col("DeptAvg"))
    .select("EmployeeID", "Salary", "DepartmentID"))

# Find duplicate employee names

%sql
SELECT EmployeeName, COUNT(*) AS CountOfName FROM Employees GROUP BY EmployeeName HAVING COUNT(*) > 1;

%python
duplicate_names = (employees
    .groupBy("EmployeeName")
    .agg(count("*").alias("CountOfName"))
    .filter(col("CountOfName") > 1))

# Rank products by sales amount

%sql
SELECT ProductID, SUM(Sales) AS TotalSales, RANK() OVER (ORDER BY SUM(Sales) DESC) AS SalesRank FROM Orders GROUP BY ProductID;

%python
product_sales = (orders
    .groupBy("ProductID")
    .agg(sum("Sales").alias("TotalSales")))

window_spec = Window.orderBy(col("TotalSales").desc())
ranked_products = product_sales.withColumn("SalesRank", rank().over(window_spec))

# Find the most frequent customer city

%sql
SELECT City, COUNT(*) AS OrderCount FROM Customers GROUP BY City ORDER BY OrderCount DESC FETCH FIRST 1 ROW ONLY;

%python
city_orders = (customers
    .groupBy("City")
    .agg(count("*").alias("OrderCount"))
    .orderBy(col("OrderCount").desc()))

most_frequent_city = city_orders.limit(1)

# Calculate variance and standard deviation of salary

%sql
SELECT VARIANCE(Salary) AS SalaryVariance, STDDEV(Salary) AS SalaryStdDev FROM Employees;

%python
salary_stats = employees.agg(
    variance("Salary").alias("SalaryVariance"),
    stddev("Salary").alias("SalaryStdDev"))
# Show only first 3 characters of employee names

%sql
SELECT SUBSTR(EmployeeName, 1, 3) AS ShortName FROM Employees;

%python
short_names = employees.withColumn("ShortName", substring("EmployeeName", 1, 3))
